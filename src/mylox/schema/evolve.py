"""Apply :class:`~mylox.schema.plan.AlterOp` sequences to a table schema.

Pure functions, no I/O: every failure is expressed with
:class:`~mylox.errors.IncompleteDDL`, which the manager turns into a fallback
``information_schema`` query.

Column *order* is preserved exactly as MySQL does it, because it is part of the
contract: when the binlog carries no column names the row image is mapped by
ordinal position.
"""

from __future__ import annotations

from typing import List, Optional, Sequence

from ..errors import IncompleteDDL
from .model import ColumnSchema, TableSchema, reindex
from .plan import AlterOp, ColumnPosition, OpKind


def _index_of(columns: Sequence[ColumnSchema], name: str) -> Optional[int]:
    for index, column in enumerate(columns):
        if column.name == name:
            return index
    return None


def _insert_index(columns: Sequence[ColumnSchema], position: Optional[ColumnPosition]) -> int:
    if position is None:
        return len(columns)
    if position.first:
        return 0
    anchor = position.after
    if anchor is None:
        return len(columns)
    index = _index_of(columns, anchor)
    if index is None:
        raise IncompleteDDL("cannot position a column after unknown column %r" % (anchor,))
    return index + 1


def apply_alter_ops(base: TableSchema, ops: Sequence[AlterOp]) -> TableSchema:
    """Return the schema produced by applying ``ops`` to ``base``.

    Raises :class:`IncompleteDDL` when an operation cannot be applied safely.
    """
    columns: List[ColumnSchema] = list(base.columns)
    primary_keys: List[str] = list(base.primary_keys)
    changed_any = False

    for op in ops:
        if op.kind is OpKind.ADD_COLUMN:
            column = op.column
            if column is None:
                raise IncompleteDDL("ADD COLUMN without a definition")
            existing = _index_of(columns, column.name)
            if existing is not None:
                if op.ignore_if_missing:
                    continue
                raise IncompleteDDL("column %r already exists in %s" % (column.name, base.name))
            index = _insert_index(columns, op.position)
            columns.insert(index, column)
            changed_any = True

        elif op.kind is OpKind.DROP_COLUMN:
            index = _index_of(columns, op.name or "")
            if index is None:
                if op.ignore_if_missing:
                    continue
                raise IncompleteDDL("cannot drop unknown column %r from %s" % (op.name, base.name))
            columns.pop(index)
            primary_keys = [pk for pk in primary_keys if pk != op.name]
            changed_any = True

        elif op.kind is OpKind.MODIFY_COLUMN:
            new_column = op.column
            if new_column is None:
                raise IncompleteDDL("MODIFY COLUMN without a definition")
            index = _index_of(columns, op.name or new_column.name)
            if index is None:
                raise IncompleteDDL("cannot modify unknown column %r of %s" % (op.name, base.name))
            old_name = columns[index].name
            columns[index] = new_column
            if old_name != new_column.name:
                primary_keys = [new_column.name if pk == old_name else pk for pk in primary_keys]
            if op.position is not None:
                moved = columns.pop(index)
                columns.insert(_insert_index(columns, op.position), moved)
            changed_any = True

        elif op.kind is OpKind.RENAME_COLUMN:
            index = _index_of(columns, op.name or "")
            if index is None:
                if op.ignore_if_missing:
                    continue
                raise IncompleteDDL("cannot rename unknown column %r of %s" % (op.name, base.name))
            if op.new_name is None:
                raise IncompleteDDL("RENAME COLUMN without a new name")
            if _index_of(columns, op.new_name) is not None:
                raise IncompleteDDL("cannot rename %r to %r: target already exists" % (op.name, op.new_name))
            old_name = columns[index].name
            columns[index] = ColumnSchema(
                name=op.new_name,
                column_type=columns[index].column_type,
                nullable=columns[index].nullable,
                default=columns[index].default,
                extra=columns[index].extra,
                ordinal=index,
            )
            primary_keys = [op.new_name if pk == old_name else pk for pk in primary_keys]
            changed_any = True

        elif op.kind is OpKind.ADD_PRIMARY_KEY:
            missing = [name for name in op.primary_keys if _index_of(columns, name) is None]
            if missing:
                raise IncompleteDDL("primary key references unknown columns %s of %s" % (missing, base.name))
            primary_keys = list(op.primary_keys)
            changed_any = True

        elif op.kind is OpKind.DROP_PRIMARY_KEY:
            primary_keys = []
            changed_any = True

        else:  # pragma: no cover - guarded by the parser
            raise IncompleteDDL("unsupported ALTER operation %r" % (op.kind,))

    if not changed_any:
        return base

    # Primary keys must reference existing columns; MySQL guarantees it, so a
    # violation means our model is out of sync.
    for pk in primary_keys:
        if _index_of(columns, pk) is None:
            raise IncompleteDDL("primary key column %r vanished from %s" % (pk, base.name))

    return base.with_columns(columns, primary_keys)


def build_created_table(
    db: str, table: str, columns: Sequence[ColumnSchema], primary_keys: Sequence[str]
) -> TableSchema:
    """Validate and build the schema of a freshly created table."""
    if not columns:
        raise IncompleteDDL("CREATE TABLE %s.%s without columns" % (db, table))
    schema = TableSchema(db=db, table=table, columns=reindex(columns), primary_keys=tuple(primary_keys))
    for pk in schema.primary_keys:
        if not schema.has_column(pk):
            raise IncompleteDDL("primary key column %r is not defined in %s" % (pk, schema.name))
    return schema
