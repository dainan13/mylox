"""In-memory doubles used by the unit tests.

The fakes subclass the real ``pymysqlreplication`` event classes so that the
engine's ``isinstance`` dispatch is exercised exactly as in production, without
needing a MySQL server.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from pymysqlreplication.event import GtidEvent, HeartbeatLogEvent, QueryEvent, XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from mylox.schema.model import ColumnSchema, TableSchema
from mylox.state.models import Checkpoint, SchemaHistoryRecord
from mylox.utils.positions import BinlogPosition


# ---------------------------------------------------------------------------
# binlog events
# ---------------------------------------------------------------------------
class FakeGtid(GtidEvent):
    def __init__(self, gtid: str) -> None:
        self._gtid = gtid

    @property
    def gtid(self) -> str:  # type: ignore[override]
        return self._gtid


class FakeHeartbeat(HeartbeatLogEvent):
    def __init__(self) -> None:
        pass


class FakeQuery(QueryEvent):
    def __init__(self, schema: str, query: str) -> None:
        self.schema = schema
        self.query = query


class FakeXid(XidEvent):
    def __init__(self) -> None:
        pass


class _FakeRows:
    _default_bitmap_attr = "columns_present_bitmap"

    def __init__(self, db: str, table: str, rows: Sequence[Dict[str, Any]], columns: int = 0, bitmap: Any = None, bitmap2: Any = None) -> None:
        self.schema = db
        self.table = table
        self._rows = list(rows)
        self.timestamp = 1_700_000_000
        self.number_of_columns = columns or None
        self.columns_present_bitmap = bitmap
        self.columns_present_bitmap2 = bitmap2

    @property
    def rows(self):
        return self._rows


class FakeWriteRows(_FakeRows, WriteRowsEvent):
    pass


class FakeUpdateRows(_FakeRows, UpdateRowsEvent):
    pass


class FakeDeleteRows(_FakeRows, DeleteRowsEvent):
    pass


class FakeStream:
    """Iterable stream of ``(event, log_file, log_pos)`` triples."""

    def __init__(self, events: Iterable[Tuple[Any, str, int]], log_file: str = "mysql-bin.000001", log_pos: int = 0) -> None:
        self._events = list(events)
        self.log_file = log_file
        self.log_pos = log_pos
        self.closed = False

    def __iter__(self):
        for event, log_file, log_pos in self._events:
            self.log_file = log_file
            self.log_pos = log_pos
            yield event

    def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# PostgreSQL state store
# ---------------------------------------------------------------------------
class FakeCursor:
    def __init__(self, store: "FakeStateStore") -> None:
        self.store = store

    def execute(self, sql: str, params: Any = None) -> None:
        self.store.statements.append((" ".join(sql.split()), params))

    def executemany(self, sql: str, params: Any) -> None:
        self.store.statements.append((" ".join(sql.split()), list(params)))

    def fetchall(self):  # pragma: no cover - not used by the engine
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeStateStore:
    """In-memory ``PgStateStore`` replacement."""

    def __init__(self, checkpoints: Optional[Iterable[Checkpoint]] = None, history: Optional[Sequence[SchemaHistoryRecord]] = None) -> None:
        self.checkpoints: Dict[str, Checkpoint] = {c.pipeline: c for c in checkpoints or []}
        self.history: List[SchemaHistoryRecord] = list(history or [])
        self.statements: List[Tuple[str, Any]] = []
        self.pending_updates: List[Tuple[str, Any, Any, Any]] = []
        self.pending_identity: Tuple[Any, Any] = (None, None)
        self.commits = 0
        self.rollbacks = 0
        self.reconnects = 0
        self.history_reads = 0
        self.closed = False
        self.fail_commit = False

    # -- checkpoint API ------------------------------------------------
    def get_checkpoints(self, pipelines: Iterable[str]) -> Dict[str, Checkpoint]:
        # Production returns freshly built objects, never the store's own
        # instances, so the engine cannot observe uncommitted mutations.
        return {name: self.checkpoints[name].copy() for name in pipelines if name in self.checkpoints}

    def mark_initialized(self, pipelines, position, gtid_set, server_uuid, server_id) -> None:
        for name in pipelines:
            checkpoint = self.checkpoints.setdefault(name, Checkpoint(pipeline=name))
            checkpoint.is_initialized = True
            if position is not None:
                checkpoint.log_file = position.file
                checkpoint.log_pos = position.pos
            checkpoint.gtid_set = checkpoint.gtid_set.copy()
            checkpoint.gtid_set.update(gtid_set)
            checkpoint.server_uuid = server_uuid
            checkpoint.server_id = server_id
        self.commits += 1

    def advance_checkpoints(self, updates, server_uuid, server_id) -> None:
        # Staged, exactly like an UPDATE inside an open transaction: nothing is
        # visible until commit() succeeds.
        self.pending_updates.extend(updates)
        self.pending_identity = (server_uuid, server_id)

    # -- transaction API -----------------------------------------------
    @contextmanager
    def cursor(self, row_factory=None):
        yield FakeCursor(self)

    def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError("commit failed")
        from mylox.gtid import GtidSet

        for name, log_file, log_pos, gtid_text in self.pending_updates:
            checkpoint = self.checkpoints.setdefault(name, Checkpoint(pipeline=name))
            checkpoint.log_file = log_file
            checkpoint.log_pos = log_pos
            if gtid_text:
                checkpoint.gtid_set = GtidSet(gtid_text)
            checkpoint.server_uuid, checkpoint.server_id = self.pending_identity
        self.pending_updates = []
        self.commits += 1

    def rollback(self) -> None:
        self.pending_updates = []
        self.rollbacks += 1

    def rollback_if_needed(self) -> None:
        self.rollbacks += 1

    def is_idle(self) -> bool:
        return False

    def reconnect(self) -> None:
        self.reconnects += 1

    def close(self) -> None:
        self.closed = True

    # -- schema history -------------------------------------------------
    def record_schema_versions(self, records) -> int:
        """Mirrors the real ``ON CONFLICT DO NOTHING`` unique index."""
        written = 0
        existing = {
            (
                record.namespace,
                record.db_name,
                record.table_name,
                record.position.file if record.position else None,
                record.position.pos if record.position else None,
                record.source,
            )
            for record in self.history
        }
        for record in records:
            key = (
                record.namespace,
                record.db_name,
                record.table_name,
                record.position.file if record.position else None,
                record.position.pos if record.position else None,
                record.source,
            )
            if key in existing:
                continue
            existing.add(key)
            self.history.append(record)
            written += 1
        return written

    def load_schema_history(self, db, table, before=None, limit=2000):
        self.history_reads += 1
        rows = [r for r in self.history if r.db_name == db and r.table_name == table]
        if before is not None:
            rows = [r for r in rows if r.position is None or r.position < before]
        return rows[:limit]

    def load_latest_schemas(self):
        latest = {}
        for record in self.history:
            latest[(record.db_name, record.table_name)] = record
        return latest


# ---------------------------------------------------------------------------
# MySQL metadata loader
# ---------------------------------------------------------------------------
class FakeLoader:
    def __init__(
        self,
        schemas: Optional[Sequence[TableSchema]] = None,
        server_uuid: str = "uuid-1",
        server_id: int = 11,
        position: Optional[BinlogPosition] = None,
        gtid_set: str = "",
        pause_succeeds: bool = True,
    ) -> None:
        self.schemas = list(schemas or [])
        self.server_uuid = server_uuid
        self.server_id = server_id
        self.position_value = position or BinlogPosition("mysql-bin.000001", 100)
        self.gtid_set = gtid_set
        self.pause_succeeds = pause_succeeds
        self.load_all_calls = 0
        self.load_tables_calls: List[Sequence[Tuple[Optional[str], str]]] = []
        self.paused = 0
        self.resumed = 0
        #: When set, load_tables answers from this mapping instead.
        self.refresh_schemas: Dict[Tuple[str, str], TableSchema] = {}

    def server_info(self):
        return {"server_uuid": self.server_uuid, "server_id": self.server_id}

    def master_status(self):
        return {
            "log_file": self.position_value.file if self.position_value else None,
            "log_pos": self.position_value.pos if self.position_value else None,
            "gtid_set": self.gtid_set,
        }

    def position(self):
        return self.position_value

    def load_all(self):
        self.load_all_calls += 1
        return list(self.schemas)

    def load_tables(self, refs):
        self.load_tables_calls.append(list(refs))
        result = []
        for db, table in refs:
            schema = self.refresh_schemas.get((db, table))
            if schema is not None:
                result.append(schema)
                continue
            for candidate in self.schemas:
                if candidate.db == db and candidate.table == table:
                    result.append(candidate)
        return result

    def pause_replication(self) -> bool:
        self.paused += 1
        return self.pause_succeeds

    def resume_replication(self) -> bool:
        self.resumed += 1
        return True


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def make_schema(db: str, table: str, columns: Sequence[str], primary_keys: Sequence[str] = (), types: Optional[Dict[str, str]] = None) -> TableSchema:
    types = types or {}
    return TableSchema(
        db=db,
        table=table,
        columns=tuple(
            ColumnSchema(name=name, column_type=types.get(name, "int"), ordinal=index)
            for index, name in enumerate(columns)
        ),
        primary_keys=tuple(primary_keys),
    )


def make_handler(record: Optional[List[Any]] = None, fail_for: Optional[Dict[str, int]] = None):
    from mylox.engine.handler import CDCHandler

    class Handler(CDCHandler):
        def __init__(self) -> None:
            self.calls = record if record is not None else []
            self.failures: Dict[str, int] = dict(fail_for or {})
            self.init_calls: List[Any] = []
            self.failed: List[Any] = []
            self.warnings: List[Any] = []

        def handle_row(self, change, pipeline, cur):
            remaining = self.failures.get(pipeline, 0)
            if remaining > 0:
                self.failures[pipeline] = remaining - 1
                raise RuntimeError("boom in %s" % pipeline)
            self.calls.append((change, pipeline))

        def on_init(self, pipelines, ctx):
            self.init_calls.append((sorted(pipelines), ctx))

        def on_pipeline_failed(self, pipeline, error, change):
            self.failed.append((pipeline, str(error)))

        def on_schema_warning(self, message, detail):
            self.warnings.append((message, detail))

    return Handler()
