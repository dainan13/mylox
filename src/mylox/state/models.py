"""Value objects persisted in PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

from ..gtid import GtidSet
from ..schema.model import TableSchema
from ..utils.positions import BinlogPosition


@dataclass
class Checkpoint:
    """One pipeline's durable position."""

    pipeline: str
    log_file: Optional[str] = None
    log_pos: Optional[int] = None
    gtid_set: GtidSet = field(default_factory=GtidSet)
    server_uuid: Optional[str] = None
    server_id: Optional[int] = None
    is_initialized: bool = False

    @property
    def position(self) -> Optional[BinlogPosition]:
        return BinlogPosition.maybe(self.log_file, self.log_pos)

    @property
    def gtid_text(self) -> str:
        return str(self.gtid_set)

    def copy(self) -> "Checkpoint":
        return Checkpoint(
            pipeline=self.pipeline,
            log_file=self.log_file,
            log_pos=self.log_pos,
            gtid_set=self.gtid_set.copy(),
            server_uuid=self.server_uuid,
            server_id=self.server_id,
            is_initialized=self.is_initialized,
        )

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "Checkpoint":
        raw_gtid = row.get("gtid_set")
        gtid_set = GtidSet()
        if raw_gtid:
            try:
                gtid_set = GtidSet(raw_gtid)
            except Exception:
                # Never let a legacy/corrupt checkpoint prevent startup; the
                # position fallback still protects us.
                gtid_set = GtidSet()
        return cls(
            pipeline=row["pipeline"],
            log_file=row.get("log_file"),
            log_pos=row.get("log_pos"),
            gtid_set=gtid_set,
            server_uuid=row.get("server_uuid"),
            server_id=row.get("server_id"),
            is_initialized=bool(row.get("is_initialized")),
        )


@dataclass
class SchemaHistoryRecord:
    """One row of ``mylox_schema_history``."""

    db_name: str
    table_name: str
    schema: TableSchema
    position: Optional[BinlogPosition] = None
    gtid: Optional[str] = None
    source: str = "bootstrap"
    ddl: Optional[str] = None
    namespace: Optional[str] = None
    record_id: Optional[int] = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any], namespace: Optional[str] = None) -> "SchemaHistoryRecord":
        columns = row.get("columns") or []
        primary_keys = row.get("primary_keys") or []
        schema = TableSchema.from_history(
            db=row["db_name"], table=row["table_name"], columns=columns, primary_keys=primary_keys
        )
        position = BinlogPosition.maybe(row.get("log_file"), row.get("log_pos"))
        return cls(
            db_name=row["db_name"],
            table_name=row["table_name"],
            schema=schema,
            position=position,
            gtid=row.get("gtid_set"),
            source=row.get("source") or "history",
            ddl=row.get("ddl_text"),
            namespace=row.get("namespace", namespace),
            record_id=row.get("id"),
        )
