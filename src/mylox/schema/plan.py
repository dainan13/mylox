"""Declarative description of a parsed DDL statement.

The sqlglot interpreter (:mod:`mylox.schema.ddl`) turns a SQL string into a
:class:`DDLPlan`; :mod:`mylox.schema.evolve` applies the plan's operations to a
:class:`~mylox.schema.model.TableSchema`.  Keeping both sides free of sqlglot
types makes the evolution logic trivially unit-testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Sequence, Tuple

from .model import ColumnSchema


class DDLKind(str, Enum):
    CREATE_TABLE = "create_table"
    ALTER_TABLE = "alter_table"
    DROP_TABLE = "drop_table"
    RENAME_TABLE = "rename_table"
    DROP_DATABASE = "drop_database"
    TRUNCATE = "truncate"
    IGNORE = "ignore"
    UNKNOWN = "unknown"


class OpKind(str, Enum):
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    MODIFY_COLUMN = "modify_column"
    RENAME_COLUMN = "rename_column"
    ADD_PRIMARY_KEY = "add_primary_key"
    DROP_PRIMARY_KEY = "drop_primary_key"


@dataclass(frozen=True)
class ColumnPosition:
    """``FIRST`` / ``AFTER col`` positioning used by MySQL."""

    first: bool = False
    after: Optional[str] = None

    @classmethod
    def first_position(cls) -> "ColumnPosition":
        return cls(first=True)

    @classmethod
    def after_column(cls, name: str) -> "ColumnPosition":
        return cls(False, name)


@dataclass(frozen=True)
class AlterOp:
    kind: OpKind
    column: Optional[ColumnSchema] = None
    name: Optional[str] = None
    new_name: Optional[str] = None
    position: Optional[ColumnPosition] = None
    primary_keys: Tuple[str, ...] = ()
    ignore_if_missing: bool = False

    @classmethod
    def add_column(cls, column: ColumnSchema, position: Optional[ColumnPosition] = None, ignore_if_exists: bool = False) -> "AlterOp":
        return cls(
            OpKind.ADD_COLUMN,
            column=column,
            name=column.name,
            position=position,
            ignore_if_missing=ignore_if_exists,
        )

    @classmethod
    def drop_column(cls, name: str, ignore_if_missing: bool = False) -> "AlterOp":
        return cls(OpKind.DROP_COLUMN, name=name, ignore_if_missing=ignore_if_missing)

    @classmethod
    def modify_column(
        cls,
        column: ColumnSchema,
        previous_name: Optional[str] = None,
        position: Optional[ColumnPosition] = None,
    ) -> "AlterOp":
        """``MODIFY COLUMN`` (same name) or ``CHANGE COLUMN old new`` (rename)."""
        return cls(
            OpKind.MODIFY_COLUMN,
            column=column,
            name=previous_name or column.name,
            new_name=column.name,
            position=position,
        )

    @classmethod
    def rename_column(cls, name: str, new_name: str, ignore_if_missing: bool = False) -> "AlterOp":
        return cls(OpKind.RENAME_COLUMN, name=name, new_name=new_name, ignore_if_missing=ignore_if_missing)

    @classmethod
    def add_primary_key(cls, columns: Sequence[str]) -> "AlterOp":
        return cls(OpKind.ADD_PRIMARY_KEY, primary_keys=tuple(columns))

    @classmethod
    def drop_primary_key(cls) -> "AlterOp":
        return cls(OpKind.DROP_PRIMARY_KEY)


@dataclass
class DDLChange:
    """One table (or database) affected by a DDL statement."""

    kind: DDLKind
    db: Optional[str] = None
    table: Optional[str] = None
    new_db: Optional[str] = None
    new_table: Optional[str] = None
    #: Fully parsed schema, only for ``CREATE TABLE``.
    columns: Optional[Tuple[ColumnSchema, ...]] = None
    primary_keys: Tuple[str, ...] = ()
    ops: Tuple[AlterOp, ...] = ()
    #: ``CREATE TABLE ... LIKE other`` source.
    like: Optional[Tuple[Optional[str], str]] = None
    database: Optional[str] = None
    needs_fallback: bool = False
    reason: Optional[str] = None

    @property
    def target(self) -> str:
        if self.table is None:
            return str(self.database or self.db or "?")
        return "%s.%s" % (self.db or "?", self.table)


@dataclass
class DDLPlan:
    statement: str
    default_db: Optional[str] = None
    changes: List[DDLChange] = field(default_factory=list)
    is_ddl: bool = True
    error: Optional[str] = None
    #: Table references salvaged from the raw text (used when sqlglot could not
    #: describe the statement).
    salvaged_refs: Tuple[Tuple[Optional[str], str], ...] = ()

    @property
    def needs_fallback(self) -> bool:
        if self.error:
            return True
        return any(change.needs_fallback for change in self.changes)

    @property
    def ignored(self) -> bool:
        """``True`` when the statement cannot change any column layout."""
        if self.error:
            return False
        return all(change.kind in (DDLKind.IGNORE, DDLKind.TRUNCATE) for change in self.changes)

    def __bool__(self) -> bool:
        return bool(self.changes)
