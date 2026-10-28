"""Hybrid schema acquisition: bootstrap, sqlglot path, fallback path."""

from __future__ import annotations

import pytest

from fakes import FakeLoader, FakeStateStore, make_schema
from mylox.config import EngineConfig
from mylox.constants import SOURCE_BOOTSTRAP, SOURCE_DDL_FALLBACK, SOURCE_DDL_FALLBACK_APPROX, SOURCE_DDL_SQLGLOT
from mylox.filtering import TableFilter
from mylox.schema.ddl import SqlglotDDLParser
from mylox.schema.manager import SchemaManager
from mylox.schema.registry import SchemaRegistry
from mylox.utils.positions import BinlogPosition

P = BinlogPosition("mysql-bin.000001", 100)
P2 = BinlogPosition("mysql-bin.000001", 200)
P3 = BinlogPosition("mysql-bin.000001", 300)


def build_manager(schemas=None, config=None, store=None, loader=None):
    registry = SchemaRegistry()
    store = store if store is not None else FakeStateStore()
    if loader is None:
        default = [make_schema("d", "t", ["id", "a"], ["id"])]
        loader = FakeLoader(schemas=default if schemas is None else schemas)
    config = config or EngineConfig(namespace="ns")
    table_filter = TableFilter(config.filter_rules, mode=config.filter_mode)
    manager = SchemaManager(registry, loader, SqlglotDDLParser(), store, table_filter, config)
    manager.bind_namespace("ns")
    return manager, registry, store, loader


def test_bootstrap_records_history_once():
    manager, registry, store, loader = build_manager()
    assert manager.bootstrap(P, gtid="uuid:1") == 1
    assert loader.load_all_calls == 1
    assert len(store.history) == 1
    assert store.history[0].source == SOURCE_BOOTSTRAP
    assert store.history[0].position == P

    # a second bootstrap with an identical schema must not add a row
    assert manager.bootstrap(P2, gtid="uuid:2") == 0
    assert len(store.history) == 1
    assert registry.latest_schema("d", "t").column_names == ("id", "a")


def test_ddl_sqlglot_path_updates_memory_and_history():
    manager, registry, store, loader = build_manager()
    manager.bootstrap(P)

    report = manager.handle_ddl("ALTER TABLE d.t ADD COLUMN b VARCHAR(10) AFTER a", P2, default_db="d")
    assert report.mode == "sqlglot"
    assert report.changed is True
    assert registry.schema_at("d", "t", P2).column_names == ("id", "a", "b")
    sources = [record.source for record in store.history]
    assert sources[-1] == SOURCE_DDL_SQLGLOT
    assert store.history[-1].position == P2
    # no extra information_schema round trip for a parsable DDL
    assert loader.load_tables_calls == []


def test_ddl_replay_is_idempotent():
    manager, registry, store, _ = build_manager()
    manager.bootstrap(P)
    statement = "ALTER TABLE d.t ADD COLUMN b VARCHAR(10)"
    manager.handle_ddl(statement, P2, default_db="d")
    first = [record for record in store.history if record.position == P2]
    manager.handle_ddl(statement, P2, default_db="d")
    assert registry.schema_at("d", "t", P2).column_names == ("id", "a", "b")
    assert len([record for record in store.history if record.position == P2]) == len(first) == 1


def test_ddl_fallback_on_parse_failure():
    loader = FakeLoader(schemas=[make_schema("d", "t", ["id", "a"], ["id"])])
    manager, registry, store, _ = build_manager(loader=loader)
    manager.bootstrap(P)
    # the fallback answers with the live schema
    loader.refresh_schemas[("d", "t")] = make_schema("d", "t", ["id", "a", "z"], ["id"])

    report = manager.handle_ddl(
        "ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (10))", P2, default_db="d"
    )
    assert report.mode == "fallback"
    assert loader.load_tables_calls[-1] == [("d", "t")]
    assert registry.schema_at("d", "t", P2).column_names == ("id", "a", "z")
    assert store.history[-1].source == SOURCE_DDL_FALLBACK


def test_ddl_fallback_is_marked_approximate_before_the_baseline():
    loader = FakeLoader(schemas=[make_schema("d", "t", ["id"], ["id"])])
    manager, registry, store, _ = build_manager(loader=loader)
    manager.bootstrap(P2)
    loader.refresh_schemas[("d", "t")] = make_schema("d", "t", ["id", "a"], ["id"])

    report = manager.handle_ddl("ALTER TABLE t ADD PARTITION (PARTITION p VALUES LESS THAN (1))", P, default_db="d")
    assert report.mode == "fallback"
    assert store.history[-1].source == SOURCE_DDL_FALLBACK_APPROX


def test_drop_table_removes_the_schema():
    manager, registry, store, _ = build_manager()
    manager.bootstrap(P)
    manager.handle_ddl("DROP TABLE d.t", P2, default_db="d")
    assert registry.schema_at("d", "t", P2) is None
    assert registry.schema_at("d", "t", P) is not None


def test_rename_table_moves_the_schema():
    manager, registry, store, _ = build_manager()
    manager.bootstrap(P)
    report = manager.handle_ddl("RENAME TABLE d.t TO d.t2", P2, default_db="d")
    assert report.mode == "sqlglot"
    assert registry.schema_at("d", "t2", P2).column_names == ("id", "a")
    assert registry.schema_at("d", "t", P2) is None


def test_create_table_like_uses_the_registry():
    manager, registry, store, loader = build_manager()
    manager.bootstrap(P)
    report = manager.handle_ddl("CREATE TABLE d.t_copy LIKE d.t", P2, default_db="d")
    assert report.mode == "sqlglot"
    assert registry.schema_at("d", "t_copy", P2).column_names == ("id", "a")
    assert loader.load_tables_calls == []


def test_alter_without_base_falls_back_to_history():
    """A DDL for a table that existed before the bootstrap snapshot."""
    store = FakeStateStore()
    manager, registry, _, loader = build_manager(store=store)
    manager.bootstrap(P2)  # baseline P2, table d.t known only from P2 onwards

    # a recorded version older than the baseline, only reachable through PG
    from mylox.state.models import SchemaHistoryRecord

    store.history.append(
        SchemaHistoryRecord(
            db_name="d",
            table_name="t",
            schema=make_schema("d", "t", ["id"], ["id"]),
            position=P,
            source=SOURCE_BOOTSTRAP,
            namespace="ns",
        )
    )
    position = BinlogPosition("mysql-bin.000001", 150)
    report = manager.handle_ddl("ALTER TABLE d.t ADD COLUMN old_col INT", position, default_db="d")
    assert report.mode == "sqlglot"
    assert registry.schema_at("d", "t", position).column_names == ("id", "old_col")
    # and the in-memory version predating the baseline was loaded from history
    assert registry.schema_at("d", "t", P).column_names == ("id",)


def test_schema_for_loads_history_lazily_once():
    store = FakeStateStore()
    manager, registry, _, _ = build_manager(store=store)
    manager.bootstrap(P2)
    from mylox.state.models import SchemaHistoryRecord

    store.history.append(
        SchemaHistoryRecord(
            db_name="d",
            table_name="t",
            schema=make_schema("d", "t", ["id"], ["id"]),
            position=P,
            source=SOURCE_BOOTSTRAP,
            namespace="ns",
        )
    )
    history_size = len(store.history)
    older = BinlogPosition("mysql-bin.000001", 120)
    assert manager.schema_for("d", "t", older).column_names == ("id",)
    assert manager.schema_for("d", "t", older).column_names == ("id",)
    assert len(store.history) == history_size  # reading must not write


def test_unknown_table_raises():
    from mylox.errors import SchemaNotFound

    manager, _, _, _ = build_manager()
    manager.bootstrap(P)
    with pytest.raises(SchemaNotFound):
        manager.schema_for("d", "nope", P)
    assert manager.schema_for("d", "nope", P, required=False) is None


def test_filtered_tables_never_enter_the_registry():
    config = EngineConfig(namespace="ns", filter_rules=["d.t", "!keep"])
    manager, registry, store, loader = build_manager(
        schemas=[make_schema("d", "t", ["id"]), make_schema("d", "other", ["id"])], config=config
    )
    manager.bootstrap(P)
    assert registry.latest_schema("d", "t") is None
    assert registry.latest_schema("d", "other") is not None


def test_fallback_disabled_reports_error():
    config = EngineConfig(namespace="ns", schema_fallback_enabled=False)
    manager, _, store, _ = build_manager(config=config)
    manager.bootstrap(P)
    report = manager.handle_ddl("ALTER TABLE t ADD PARTITION (PARTITION p VALUES LESS THAN (1))", P2, default_db="d")
    assert report.mode == "error"
    assert store.history[-1].source == SOURCE_BOOTSTRAP


def test_pre_baseline_refresh_is_refused_for_row_decoding():
    """A live information_schema snapshot must not be stamped at an old position."""
    loader = FakeLoader(schemas=[make_schema("d", "t", ["id"], ["id"])])
    manager, registry, store, _ = build_manager(loader=loader)
    manager.bootstrap(P2)  # baseline P2
    loader.refresh_schemas[("d", "t")] = make_schema("d", "t", ["id", "z"], ["id"])

    assert manager.refresh_tables_and_record([("d", "t")], P) == 0
    assert registry.schema_at("d", "t", P) is None
    assert len(store.history) == 1  # only the bootstrap row

    # the DDL fallback path accepts the approximation and labels it as such
    assert manager.refresh_tables_and_record([("d", "t")], P, allow_pre_baseline=True) == 1
    assert registry.schema_at("d", "t", P).column_names == ("id", "z")
    assert store.history[-1].source == SOURCE_DDL_FALLBACK_APPROX


def test_rename_replay_at_the_same_position_is_a_noop():
    manager, registry, store, loader = build_manager()
    manager.bootstrap(P)
    statement = "RENAME TABLE d.t TO d.t2"
    first = manager.handle_ddl(statement, P2, default_db="d")
    assert first.mode == "sqlglot"
    history_rows = len(store.history)

    second = manager.handle_ddl(statement, P2, default_db="d")
    assert second.mode == "sqlglot"
    assert second.changed is False
    assert len(store.history) == history_rows
    assert loader.load_tables_calls == []


def test_empty_source_is_not_reported_as_a_filter_mistake(caplog):
    """An empty source must not look like a mis-written filter rule."""
    loader = FakeLoader(schemas=[])
    loader.count_base_tables = lambda: 0
    manager, registry, store, loader = build_manager(loader=loader)

    with caplog.at_level("INFO", logger="mylox.schema.manager"):
        manager.bootstrap(P)

    messages = [record.getMessage() for record in caplog.records]
    assert any("no user tables yet" in message for message in messages)
    assert not any("no table matched the filter rules" in message for message in messages)


def test_all_tables_filtered_out_is_reported(caplog):
    """Rules that exclude everything are a loud, actionable warning."""
    # the loader applies the rules, so "everything filtered out" reaches the
    # manager as an empty list while the source does have tables
    loader = FakeLoader(schemas=[])
    loader.count_base_tables = lambda: 5
    manager, registry, store, loader = build_manager(loader=loader)

    with caplog.at_level("WARNING", logger="mylox.schema.manager"):
        manager.bootstrap(P)

    messages = [record.getMessage() for record in caplog.records]
    assert any("no table matched the filter rules" in message for message in messages)
