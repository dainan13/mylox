"""Business facing API: the handler a user implements.

``handle_row`` is invoked **once per pipeline** and wrapped in a PostgreSQL
savepoint, so a bug in one pipeline only rolls back that pipeline's writes::

    class MyHandler(CDCHandler):
        def handle_row(self, change, pipeline, cur):
            cur.execute("INSERT INTO ... VALUES (...)", (...) )

        def on_init(self, pipelines, ctx):
            with ctx.pg_cursor() as cur:
                ...
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple


class ChangeAction(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass(frozen=True)
class RowChange:
    """One row level change, already cleaned of absent columns."""

    db: str
    table: str
    action: ChangeAction
    timestamp: Optional[int] = None
    primary_keys: Tuple[str, ...] = ()
    #: Column values keyed by column name; only columns present in the row
    #: image are included.
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    #: ``UPDATE`` only: the columns whose value actually changed.
    modify: Optional[Dict[str, Any]] = None
    #: Raw binlog coordinates of the event.
    log_file: Optional[str] = None
    log_pos: Optional[int] = None
    gtid: Optional[str] = None

    @property
    def full_name(self) -> str:
        return "%s.%s" % (self.db, self.table)

    def to_dict(self) -> Dict[str, Any]:
        """Mapping shaped view of this change (handy for JSON logging or a generic sink)."""
        payload: Dict[str, Any] = {
            "db": self.db,
            "table": self.table,
            "action": self.action.value,
            "timestamp": self.timestamp,
            "primary_keys": list(self.primary_keys),
        }
        if self.before is not None:
            payload["before"] = self.before
        if self.after is not None:
            payload["after"] = self.after
        if self.modify is not None:
            payload["modify"] = self.modify
        return payload

    def key_values(self) -> Dict[str, Any]:
        """Primary key values, taken from ``before`` when available."""
        source = self.before or self.after or {}
        return {key: source.get(key) for key in self.primary_keys}


class CDCHandler(ABC):
    """Base class for the business logic."""

    @abstractmethod
    def handle_row(self, change: RowChange, pipeline: str, cur: Any) -> None:
        """Handle one change for one pipeline.

        ``cur`` is a cursor on the shared PostgreSQL transaction.  Do **not**
        commit or roll back: the engine owns the transaction and rolls back to
        a savepoint when this method raises.
        """

    def on_init(self, pipelines: Iterable[str], ctx: Any) -> None:
        """Initialise the given pipelines (full load / bootstrap logic)."""

    def on_pipeline_failed(self, pipeline: str, error: BaseException, change: Optional[RowChange] = None) -> None:
        """Called when ``handle_row`` raised and the pipeline was dropped."""

    def on_schema_warning(self, message: str, detail: Mapping[str, Any]) -> None:
        """Called when the schema model could not be updated with certainty."""


class PrintHandler(CDCHandler):
    """Minimal handler used by examples and smoke tests."""

    def handle_row(self, change: RowChange, pipeline: str, cur: Any) -> None:
        import json

        print("[%s] %s %s %s" % (pipeline, change.action.value, change.full_name, json.dumps(change.to_dict(), default=str, ensure_ascii=False)))
