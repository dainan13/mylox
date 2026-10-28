"""Immutable schema model.

The model is intentionally small: mylox only needs to know *which columns a
table has, in which order, and which of them form the primary key*.  Column
order is important - when the binlog carries no column names the payload is
mapped by ordinal position.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class ColumnSchema:
    name: str
    column_type: str = ""
    nullable: bool = True
    default: Optional[str] = None
    extra: str = ""
    ordinal: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "column_type": self.column_type,
            "nullable": self.nullable,
            "default": self.default,
            "extra": self.extra,
            "ordinal": self.ordinal,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ColumnSchema":
        return cls(
            name=str(data["name"]),
            column_type=str(data.get("column_type") or ""),
            nullable=bool(data.get("nullable", True)),
            default=data.get("default"),
            extra=str(data.get("extra") or ""),
            ordinal=int(data.get("ordinal") or 0),
        )

    @classmethod
    def from_name(cls, name: str) -> "ColumnSchema":
        """Build a column from a bare name (tolerant parsing of a hand-written row)."""
        return cls(name=name)


def coerce_column(value: object) -> ColumnSchema:
    if isinstance(value, ColumnSchema):
        return value
    if isinstance(value, str):
        return ColumnSchema.from_name(value)
    if isinstance(value, Mapping):
        return ColumnSchema.from_dict(value)
    raise TypeError("cannot build a ColumnSchema from %r" % (value,))


@dataclass(frozen=True)
class TableSchema:
    db: str
    table: str
    columns: Tuple[ColumnSchema, ...]
    primary_keys: Tuple[str, ...] = ()

    # -- lookups ------------------------------------------------------
    @property
    def name(self) -> str:
        return "%s.%s" % (self.db, self.table)

    @property
    def column_names(self) -> Tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def has_column(self, name: str) -> bool:
        return any(column.name == name for column in self.columns)

    def index_of(self, name: str) -> Optional[int]:
        for index, column in enumerate(self.columns):
            if column.name == name:
                return index
        return None

    def column(self, name: str) -> Optional[ColumnSchema]:
        for candidate in self.columns:
            if candidate.name == name:
                return candidate
        return None

    # -- transformations ----------------------------------------------
    def with_columns(
        self,
        columns: Sequence[ColumnSchema],
        primary_keys: Optional[Iterable[str]] = None,
    ) -> "TableSchema":
        ordered = reindex(columns)
        pks = tuple(primary_keys) if primary_keys is not None else self.primary_keys
        pks = tuple(pk for pk in pks if any(column.name == pk for column in ordered))
        return replace(self, columns=ordered, primary_keys=pks)

    def renamed(self, db: str, table: str) -> "TableSchema":
        return replace(self, db=db, table=table)

    # -- serialisation ------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "db": self.db,
            "table": self.table,
            "columns": [column.to_dict() for column in self.columns],
            "primary_keys": list(self.primary_keys),
        }

    @classmethod
    def from_history(
        cls,
        db: str,
        table: str,
        columns: Sequence[object],
        primary_keys: Optional[Iterable[object]] = None,
    ) -> "TableSchema":
        """Rebuild a schema from a ``mylox_schema_history`` row.

        Columns are normally dictionaries; a plain list of names is tolerated so
        that a hand-edited row cannot crash the loader at startup.
        """
        return cls(
            db=db,
            table=table,
            columns=reindex([coerce_column(column) for column in columns]),
            primary_keys=tuple(str(pk) for pk in (primary_keys or ())),
        )


def reindex(columns: Sequence[ColumnSchema]) -> Tuple[ColumnSchema, ...]:
    """Renumber ``ordinal`` so it always matches the tuple position."""
    result: List[ColumnSchema] = []
    for position, column in enumerate(columns):
        result.append(column if column.ordinal == position else replace(column, ordinal=position))
    return tuple(result)


#: Wraparound mask per MySQL unsigned integer type.
_UNSIGNED_MASKS = {
    "tinyint": 0xFF,
    "smallint": 0xFFFF,
    "mediumint": 0xFFFFFF,
    "int": 0xFFFFFFFF,
    "integer": 0xFFFFFFFF,
    "bigint": 0xFFFFFFFFFFFFFFFF,
}


@lru_cache(maxsize=1024)
def unsigned_columns(schema: TableSchema) -> Tuple[Tuple[str, int], ...]:
    """``(column, mask)`` for every ``UNSIGNED`` integer column of a table.

    MySQL only puts column signedness in the binlog when
    ``binlog_row_metadata=FULL``; with the default (``MINIMAL``) or on MySQL 5.7
    (the variable does not exist) the reader decodes unsigned values as signed,
    so ``bigint unsigned`` 18446744073709551615 arrives as ``-1``.  mylox knows
    the real type from ``information_schema`` and repairs the value instead of
    passing the corruption downstream.  A negative value in an ``UNSIGNED``
    column can only come from that wraparound, so the mask is always safe.

    Cached on the (hashable, immutable) schema instance: the lookup runs for
    every row image, but only the first one pays for the scan.
    """
    columns = []
    for column in schema.columns:
        lowered = (column.column_type or "").lower()
        if "unsigned" not in lowered:
            continue
        base = re.split(r"[\s(\[]", lowered, maxsplit=1)[0]
        mask = _UNSIGNED_MASKS.get(base)
        if mask is not None:
            columns.append((column.name, mask))
    return tuple(columns)


def fix_unsigned_values(schema: TableSchema, values: Dict[str, Any]) -> Dict[str, Any]:
    """Turn sign-wrapped unsigned integers back into their real value."""
    for name, mask in unsigned_columns(schema):
        value = values.get(name)
        if isinstance(value, int) and not isinstance(value, bool) and value < 0:
            values[name] = value & mask
    return values


def schemas_equal(left: Optional[TableSchema], right: Optional[TableSchema]) -> bool:
    """Structural comparison ignoring bookkeeping fields."""
    if left is None or right is None:
        return left is right
    return (
        left.db == right.db
        and left.table == right.table
        and left.columns == right.columns
        and left.primary_keys == right.primary_keys
    )
