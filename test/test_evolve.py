"""Applying ALTER operations to a schema (pure logic)."""

from __future__ import annotations

import pytest

from mylox.errors import IncompleteDDL
from mylox.schema.evolve import apply_alter_ops, build_created_table
from mylox.schema.model import ColumnSchema, TableSchema
from mylox.schema.plan import AlterOp, ColumnPosition


def base_schema():
    return TableSchema(
        db="d",
        table="t",
        columns=tuple(
            ColumnSchema(name=name, column_type="int", ordinal=index)
            for index, name in enumerate(["id", "a", "b"])
        ),
        primary_keys=("id",),
    )


def names(schema):
    return schema.column_names


def test_add_column_appends_by_default():
    result = apply_alter_ops(base_schema(), [AlterOp.add_column(ColumnSchema(name="c", ordinal=0))])
    assert names(result) == ("id", "a", "b", "c")
    assert [column.ordinal for column in result.columns] == [0, 1, 2, 3]


def test_add_column_honours_first_and_after():
    ops = [
        AlterOp.add_column(ColumnSchema(name="first"), ColumnPosition.first_position()),
        AlterOp.add_column(ColumnSchema(name="mid"), ColumnPosition.after_column("a")),
    ]
    assert names(apply_alter_ops(base_schema(), ops)) == ("first", "id", "a", "mid", "b")


def test_add_existing_column_is_drift_unless_if_not_exists():
    with pytest.raises(IncompleteDDL):
        apply_alter_ops(base_schema(), [AlterOp.add_column(ColumnSchema(name="a"))])
    result = apply_alter_ops(base_schema(), [AlterOp.add_column(ColumnSchema(name="a"), ignore_if_exists=True)])
    assert names(result) == ("id", "a", "b")


def test_drop_column_and_primary_key():
    result = apply_alter_ops(base_schema(), [AlterOp.drop_column("id")])
    assert names(result) == ("a", "b")
    assert result.primary_keys == ()
    with pytest.raises(IncompleteDDL):
        apply_alter_ops(base_schema(), [AlterOp.drop_column("nope")])


def test_modify_column_type():
    modified = ColumnSchema(name="a", column_type="varchar(32)", nullable=False)
    result = apply_alter_ops(base_schema(), [AlterOp.modify_column(modified)])
    assert result.column("a").column_type == "varchar(32)"
    assert result.column("a").nullable is False


def test_change_column_renames_and_keeps_position():
    renamed = ColumnSchema(name="a2", column_type="bigint")
    result = apply_alter_ops(base_schema(), [AlterOp.modify_column(renamed, previous_name="a")])
    assert names(result) == ("id", "a2", "b")


def test_rename_column_updates_primary_key():
    result = apply_alter_ops(base_schema(), [AlterOp.rename_column("id", "id2")])
    assert names(result) == ("id2", "a", "b")
    assert result.primary_keys == ("id2",)


def test_rename_column_to_existing_is_rejected():
    with pytest.raises(IncompleteDDL):
        apply_alter_ops(base_schema(), [AlterOp.rename_column("a", "b")])


def test_rename_unknown_column_is_drift():
    with pytest.raises(IncompleteDDL):
        apply_alter_ops(base_schema(), [AlterOp.rename_column("zzz", "yyy")])


def test_primary_key_operations():
    result = apply_alter_ops(base_schema(), [AlterOp.add_primary_key(["a", "b"])])
    assert result.primary_keys == ("a", "b")
    result = apply_alter_ops(result, [AlterOp.drop_primary_key()])
    assert result.primary_keys == ()
    with pytest.raises(IncompleteDDL):
        apply_alter_ops(base_schema(), [AlterOp.add_primary_key(["a", "missing"])])


def test_move_column_with_modify():
    moved = ColumnSchema(name="b", column_type="int")
    result = apply_alter_ops(base_schema(), [AlterOp.modify_column(moved, position=ColumnPosition.first_position())])
    assert names(result) == ("b", "id", "a")


def test_empty_ops_returns_same_object():
    schema = base_schema()
    assert apply_alter_ops(schema, []) is schema


def test_build_created_table_validates():
    schema = build_created_table("d", "t", [ColumnSchema(name="id")], ["id"])
    assert schema.primary_keys == ("id",)
    with pytest.raises(IncompleteDDL):
        build_created_table("d", "t", [], [])
    with pytest.raises(IncompleteDDL):
        build_created_table("d", "t", [ColumnSchema(name="id")], ["other"])
