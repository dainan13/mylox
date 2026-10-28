"""In-memory schema timeline.

Every table keeps a list of :class:`SchemaVersion` ordered by binlog position::

    (db, table) -> [ v@mysql-bin.000001:120 (bootstrap),
                     v@mysql-bin.000001:900 (ddl_sqlglot ADD COLUMN c),
                     v@mysql-bin.000002:40  (ddl_sqlglot DROP COLUMN c) ]

Looking up the schema for a row event is then a binary search over a handful of
entries - no PostgreSQL round trip per row, which was the single biggest
performance problem of the original implementation.

Replaying a DDL that is already in the timeline replaces the version at that
exact position (instead of stacking a duplicate), which makes the whole DDL
path idempotent across restarts.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterator, List, Optional, Tuple

from ..utils.positions import BinlogPosition
from .model import TableSchema, schemas_equal


class PutOutcome(str, Enum):
    ADDED = "added"
    REPLACED = "replaced"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class SchemaVersion:
    position: BinlogPosition
    schema: TableSchema
    source: str
    ddl: Optional[str] = None
    gtid: Optional[str] = None
    history_id: Optional[int] = None

    @property
    def db(self) -> str:
        return self.schema.db

    @property
    def table(self) -> str:
        return self.schema.table


@dataclass
class TableTimeline:
    """All known versions of a single ``db.table``."""

    db: str
    table: str
    versions: List[SchemaVersion] = field(default_factory=list)
    #: Position at which the table was dropped/renamed away.
    dropped_at: Optional[BinlogPosition] = None

    # -- queries ------------------------------------------------------
    @property
    def name(self) -> str:
        return "%s.%s" % (self.db, self.table)

    @property
    def latest(self) -> Optional[SchemaVersion]:
        if not self.versions:
            return None
        return self.versions[-1]

    def is_dropped_at(self, position: Optional[BinlogPosition]) -> bool:
        if self.dropped_at is None:
            return False
        if position is None:
            return True
        return position >= self.dropped_at

    def _index_leq(self, position: BinlogPosition) -> int:
        """Index of the last version with ``version.position <= position``."""
        keys = [version.position for version in self.versions]
        return bisect.bisect_right(keys, position) - 1

    def version_at(self, position: BinlogPosition) -> Optional[SchemaVersion]:
        index = self._index_leq(position)
        return self.versions[index] if index >= 0 else None

    def schema_at(self, position: Optional[BinlogPosition]) -> Optional[TableSchema]:
        if not self.versions:
            return None
        if position is None:
            return None if self.dropped_at is not None else self.versions[-1].schema
        if self.is_dropped_at(position):
            return None
        version = self.version_at(position)
        return version.schema if version is not None else None

    def version_exactly_at(self, position: BinlogPosition) -> Optional[SchemaVersion]:
        for version in reversed(self.versions):
            if version.position == position:
                return version
            if version.position < position:
                break
        return None

    def schema_before(self, position: BinlogPosition) -> Optional[TableSchema]:
        """Schema in effect strictly *before* ``position``.

        Used to compute the base for an ``ALTER`` replay: when the same DDL is
        seen twice the version written at ``position`` is replaced, and the base
        is looked up at ``position`` (exclusive), so the diff is applied to the
        same starting point both times.
        """
        if not self.versions or self.is_dropped_at(position):
            return None
        index = bisect.bisect_left([version.position for version in self.versions], position) - 1
        return self.versions[index].schema if index >= 0 else None

    # -- mutation -----------------------------------------------------
    def put(self, version: SchemaVersion) -> PutOutcome:
        """Insert ``version`` keeping the list ordered; replace same position."""
        if self.versions:
            keys = [item.position for item in self.versions]
            index = bisect.bisect_left(keys, version.position)
        else:
            index = 0

        if index < len(self.versions) and self.versions[index].position == version.position:
            existing = self.versions[index]
            if schemas_equal(existing.schema, version.schema) and existing.source == version.source:
                self.versions[index] = version
                return PutOutcome.UNCHANGED
            self.versions[index] = version
            return PutOutcome.REPLACED

        self.versions.insert(index, version)
        if self.dropped_at is not None and version.position >= self.dropped_at:
            self.dropped_at = None
        return PutOutcome.ADDED

    def mark_dropped(self, position: BinlogPosition) -> None:
        if self.dropped_at is None or position > self.dropped_at:
            self.dropped_at = position

    def __bool__(self) -> bool:
        return bool(self.versions) and self.dropped_at is None

    def __len__(self) -> int:
        return len(self.versions)


class SchemaRegistry:
    """Timeline registry keyed by ``(db, table)``.

    Not thread safe: the engine drives it from a single thread.
    """

    def __init__(self) -> None:
        self._timelines: Dict[Tuple[str, str], TableTimeline] = {}
        self.baseline: Optional[BinlogPosition] = None

    # -- introspection ------------------------------------------------
    def __contains__(self, key: object) -> bool:
        if isinstance(key, tuple) and len(key) == 2:
            return (str(key[0]), str(key[1])) in self._timelines
        return False

    def __len__(self) -> int:
        return len(self._timelines)

    def tables(self) -> List[Tuple[str, str]]:
        return list(self._timelines.keys())

    def timeline(self, db: str, table: str) -> Optional[TableTimeline]:
        return self._timelines.get((db, table))

    def ensure_timeline(self, db: str, table: str) -> TableTimeline:
        timeline = self._timelines.get((db, table))
        if timeline is None:
            timeline = TableTimeline(db=db, table=table)
            self._timelines[(db, table)] = timeline
        return timeline

    def iter_versions(self) -> Iterator[SchemaVersion]:
        for timeline in self._timelines.values():
            for version in timeline.versions:
                yield version

    # -- lookups ------------------------------------------------------
    def schema_at(self, db: str, table: str, position: Optional[BinlogPosition]) -> Optional[TableSchema]:
        timeline = self._timelines.get((db, table))
        if timeline is None:
            return None
        return timeline.schema_at(position)

    def latest_schema(self, db: str, table: str) -> Optional[TableSchema]:
        timeline = self._timelines.get((db, table))
        if timeline is None or timeline.is_dropped_at(None):
            return None
        latest = timeline.latest
        return latest.schema if latest is not None else None

    def schema_before(self, db: str, table: str, position: BinlogPosition) -> Optional[TableSchema]:
        timeline = self._timelines.get((db, table))
        if timeline is None:
            return None
        return timeline.schema_before(position)

    def has_version_at(self, db: str, table: str, position: BinlogPosition) -> bool:
        timeline = self._timelines.get((db, table))
        if timeline is None:
            return False
        return timeline.version_exactly_at(position) is not None

    # -- mutation -----------------------------------------------------
    def put(
        self,
        schema: TableSchema,
        position: BinlogPosition,
        source: str,
        ddl: Optional[str] = None,
        gtid: Optional[str] = None,
        history_id: Optional[int] = None,
    ) -> Tuple[SchemaVersion, PutOutcome]:
        timeline = self.ensure_timeline(schema.db, schema.table)
        version = SchemaVersion(
            position=position,
            schema=schema,
            source=source,
            ddl=ddl,
            gtid=gtid,
            history_id=history_id,
        )
        outcome = timeline.put(version)
        return version, outcome

    def drop(self, db: str, table: str, position: BinlogPosition) -> bool:
        timeline = self._timelines.get((db, table))
        if timeline is None:
            timeline = self.ensure_timeline(db, table)
            timeline.mark_dropped(position)
            return True
        if timeline.is_dropped_at(position):
            return False
        timeline.mark_dropped(position)
        return True

    def drop_database(self, db: str, position: BinlogPosition) -> List[Tuple[str, str]]:
        dropped = []
        for (candidate_db, table), timeline in self._timelines.items():
            if candidate_db != db:
                continue
            if not timeline.is_dropped_at(position):
                timeline.mark_dropped(position)
                dropped.append((candidate_db, table))
        return dropped

    def rename(
        self,
        old_db: str,
        old_table: str,
        new_db: str,
        new_table: str,
        position: BinlogPosition,
        source: str,
        ddl: Optional[str] = None,
        gtid: Optional[str] = None,
    ) -> Optional[Tuple[SchemaVersion, PutOutcome]]:
        """Move a table, keeping its schema for the new name from ``position``."""
        schema = self.schema_before(old_db, old_table, position)
        if schema is None:
            return None
        moved = schema.renamed(new_db, new_table)
        result = self.put(moved, position, source=source, ddl=ddl, gtid=gtid)
        self.drop(old_db, old_table, position)
        return result

    def mark_baseline(self, position: BinlogPosition) -> None:
        self.baseline = position

    def clear(self) -> None:
        self._timelines.clear()
        self.baseline = None

    def snapshot(self) -> List[SchemaVersion]:
        """Every live (non-dropped) version, latest first per table."""
        result: List[SchemaVersion] = []
        for timeline in self._timelines.values():
            latest = timeline.latest
            if latest is not None and not timeline.is_dropped_at(None):
                result.append(latest)
        return result
