"""Schema timeline lookups (evaluation issue P0-3)."""

from __future__ import annotations

from mylox.schema.model import ColumnSchema, TableSchema
from mylox.schema.registry import PutOutcome, SchemaRegistry
from mylox.utils.positions import BinlogPosition

P1 = BinlogPosition("mysql-bin.000001", 100)
P2 = BinlogPosition("mysql-bin.000001", 200)
P3 = BinlogPosition("mysql-bin.000002", 50)


def schema(*names, pks=()):
    return TableSchema(
        db="d",
        table="t",
        columns=tuple(ColumnSchema(name=name, ordinal=index) for index, name in enumerate(names)),
        primary_keys=tuple(pks),
    )


def test_lookup_between_versions():
    registry = SchemaRegistry()
    registry.put(schema("a"), P1, source="bootstrap")
    registry.put(schema("a", "b"), P2, source="ddl_sqlglot")

    assert registry.schema_at("d", "t", BinlogPosition("mysql-bin.000001", 150)).column_names == ("a",)
    assert registry.schema_at("d", "t", P2).column_names == ("a", "b")
    assert registry.schema_at("d", "t", P3).column_names == ("a", "b")
    # before the first version nothing is known
    assert registry.schema_at("d", "t", BinlogPosition("mysql-bin.000001", 1)) is None


def test_put_replaces_same_position_idempotently():
    registry = SchemaRegistry()
    _, outcome = registry.put(schema("a"), P1, source="ddl_sqlglot")
    assert outcome is PutOutcome.ADDED
    _, outcome = registry.put(schema("a"), P1, source="ddl_sqlglot")
    assert outcome is PutOutcome.UNCHANGED
    assert len(registry.timeline("d", "t").versions) == 1

    _, outcome = registry.put(schema("a", "b"), P1, source="ddl_sqlglot")
    assert outcome is PutOutcome.REPLACED
    assert len(registry.timeline("d", "t").versions) == 1
    assert registry.latest_schema("d", "t").column_names == ("a", "b")


def test_schema_before_is_strict():
    registry = SchemaRegistry()
    registry.put(schema("a"), P1, source="bootstrap")
    registry.put(schema("a", "b"), P2, source="ddl_sqlglot")
    assert registry.schema_before("d", "t", P2).column_names == ("a",)
    assert registry.schema_before("d", "t", P1) is None


def test_drop_hides_later_lookups():
    registry = SchemaRegistry()
    registry.put(schema("a"), P1, source="bootstrap")
    registry.drop("d", "t", P2)
    assert registry.schema_at("d", "t", P2) is None
    assert registry.schema_at("d", "t", P3) is None
    assert registry.schema_at("d", "t", P1).column_names == ("a",)
    assert registry.latest_schema("d", "t") is None
    assert registry.snapshot() == []


def test_recreate_after_drop_clears_the_marker():
    registry = SchemaRegistry()
    registry.put(schema("a"), P1, source="bootstrap")
    registry.drop("d", "t", P2)
    registry.put(schema("a", "b"), P3, source="ddl_sqlglot")
    assert registry.latest_schema("d", "t").column_names == ("a", "b")


def test_rename_moves_the_schema_and_keeps_history():
    registry = SchemaRegistry()
    registry.put(schema("a", "b", pks=("a",)), P1, source="bootstrap")
    result = registry.rename("d", "t", "d", "t2", P2, source="ddl_sqlglot")
    assert result is not None
    moved = registry.latest_schema("d", "t2")
    assert moved.column_names == ("a", "b")
    assert moved.primary_keys == ("a",)
    assert registry.schema_at("d", "t", P2) is None
    assert registry.schema_at("d", "t", P1) is not None


def test_drop_database():
    registry = SchemaRegistry()
    registry.put(schema("a"), P1, source="bootstrap")
    registry.put(schema("b").renamed("other", "t"), P1, source="bootstrap")
    dropped = registry.drop_database("d", P2)
    assert dropped == [("d", "t")]
    assert registry.latest_schema("other", "t") is not None


def test_baseline_and_clear():
    registry = SchemaRegistry()
    registry.mark_baseline(P1)
    assert registry.baseline == P1
    registry.put(schema("a"), P1, source="bootstrap")
    assert len(registry) == 1
    registry.clear()
    assert len(registry) == 0 and registry.baseline is None
