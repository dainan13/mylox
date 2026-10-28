"""Checkpoint / schema history value objects and the generated DDL."""

from __future__ import annotations

import pytest

from fakes import make_schema
from mylox.state.models import Checkpoint, SchemaHistoryRecord
from mylox.state.store import (
    CHECKPOINT_DDL,
    SCHEMA_HISTORY_DDL,
    SCHEMA_HISTORY_INDEXES,
    SCHEMA_HISTORY_MIGRATIONS,
)
from mylox.utils.positions import BinlogPosition


def test_checkpoint_from_row_parses_the_executed_gtid_set():
    checkpoint = Checkpoint.from_row(
        {
            "pipeline": "p1",
            "server_uuid": "uuid",
            "server_id": 3,
            "log_file": "mysql-bin.000001",
            "log_pos": 120,
            "gtid_set": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
            "is_initialized": True,
        }
    )
    assert checkpoint.position == BinlogPosition("mysql-bin.000001", 120)
    assert "3e11fa47-71ca-11e1-9e33-c80aa9429562:5" in checkpoint.gtid_set
    assert checkpoint.is_initialized is True


@pytest.mark.parametrize("raw", [None, "", "uuid:1", "garbage", "not-a-uuid:3"])
def test_checkpoint_tolerates_broken_legacy_gtid_values(raw):
    checkpoint = Checkpoint.from_row({"pipeline": "p1", "gtid_set": raw, "is_initialized": True})
    assert isinstance(checkpoint.gtid_set.is_empty(), bool)
    assert checkpoint.position is None


def test_checkpoint_copy_is_independent():
    original = Checkpoint(pipeline="p1", log_file="f", log_pos=1)
    original.gtid_set.add("3e11fa47-71ca-11e1-9e33-c80aa9429562:1")
    clone = original.copy()
    clone.gtid_set.add("3e11fa47-71ca-11e1-9e33-c80aa9429562:2")
    assert original.gtid_text != clone.gtid_text


def test_history_record_accepts_column_dicts_and_bare_names():
    record = SchemaHistoryRecord.from_row(
        {
            "id": 7,
            "db_name": "d",
            "table_name": "t",
            "columns": [{"name": "id", "column_type": "int", "nullable": False, "ordinal": 0}],
            "primary_keys": ["id"],
            "log_file": "mysql-bin.000001",
            "log_pos": 15,
            "source": "ddl_sqlglot",
            "ddl_text": "ALTER TABLE t ADD COLUMN id INT",
            "namespace": "ns",
        }
    )
    assert record.schema.column_names == ("id",)
    assert record.schema.column("id").nullable is False
    assert record.position == BinlogPosition("mysql-bin.000001", 15)
    assert record.record_id == 7

    # a hand-written row may carry bare column names; that must not crash the loader
    plain = SchemaHistoryRecord.from_row(
        {"db_name": "d", "table_name": "t", "columns": ["a", "b"], "primary_keys": ["a"]}
    )
    assert plain.schema.column_names == ("a", "b")
    assert plain.schema.primary_keys == ("a",)
    assert plain.source == "history"


def test_schema_history_ddl_covers_every_column_mylox_writes():
    for column in (
        "namespace",
        "log_file",
        "log_pos",
        "gtid_set",
        "db_name",
        "table_name",
        "columns",
        "primary_keys",
        "source",
        "ddl_text",
    ):
        assert column in SCHEMA_HISTORY_DDL
    assert "pipeline VARCHAR(64) PRIMARY KEY" in CHECKPOINT_DDL
    assert "cdc_pipeline" not in CHECKPOINT_DDL
    assert "gtid_set" in CHECKPOINT_DDL
    # the unique index is what makes re-recording a version a no-op
    assert any("UNIQUE" in statement for statement in SCHEMA_HISTORY_INDEXES)
    assert any("namespace, db_name, table_name, id" in statement for statement in SCHEMA_HISTORY_INDEXES)
    # tables created by an older mylox build must be upgradable in place
    assert all("IF NOT EXISTS" in statement for statement in SCHEMA_HISTORY_MIGRATIONS[:3])


def test_history_record_roundtrip_with_fake_schema():
    schema = make_schema("d", "t", ["id", "a"], ["id"])
    record = SchemaHistoryRecord(db_name="d", table_name="t", schema=schema, namespace="ns")
    restored = SchemaHistoryRecord.from_row(
        {
            "db_name": record.db_name,
            "table_name": record.table_name,
            "columns": [column.to_dict() for column in record.schema.columns],
            "primary_keys": list(record.schema.primary_keys),
        }
    )
    assert restored.schema.column_names == ("id", "a")
    assert restored.schema.primary_keys == ("id",)


class _RecordingCursor:
    def __init__(self, rows=None):
        self.sql = None
        self.params = None
        self.rows = rows or []

    def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = params

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _RecordingConnection:
    closed = False

    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, row_factory=None):
        return self._cursor


def _store_for_sql(namespace="ns"):
    from mylox.config import PostgresSettings
    from mylox.state.store import PgStateStore

    cursor = _RecordingCursor()
    store = PgStateStore.__new__(PgStateStore)
    store.settings = PostgresSettings(dbname="dw", user="u")
    store.namespace = namespace
    store._meta_conn = _RecordingConnection(cursor)
    store._tx_conn = None
    return store, cursor


def test_history_query_is_scoped_to_the_namespace():
    store, cursor = _store_for_sql()
    store.load_schema_history("d", "t")
    assert cursor.sql.startswith("SELECT id, namespace, log_file, log_pos, gtid_set, db_name, table_name")
    assert "WHERE namespace = %s AND db_name = %s AND table_name = %s" in cursor.sql
    assert "ORDER BY id ASC" in cursor.sql  # insertion order, not log_file
    assert cursor.params == ("ns", "d", "t", 2000)


def test_state_tables_use_the_mylox_prefix():
    from mylox.state.store import CHECKPOINT_TABLE, SCHEMA_HISTORY_TABLE

    assert CHECKPOINT_TABLE == "mylox_checkpoint"
    assert SCHEMA_HISTORY_TABLE == "mylox_schema_history"
    assert "CREATE TABLE IF NOT EXISTS mylox_checkpoint" in CHECKPOINT_DDL
    assert "CREATE TABLE IF NOT EXISTS mylox_schema_history" in SCHEMA_HISTORY_DDL
    # no query may still point at the pre-rename table names
    for statement in SCHEMA_HISTORY_MIGRATIONS + SCHEMA_HISTORY_INDEXES:
        assert "mylox_" in statement, statement
    assert "idx_mylox_schema_hist_lookup" in SCHEMA_HISTORY_INDEXES[0]
    assert "uq_mylox_schema_hist_version" in SCHEMA_HISTORY_INDEXES[1]
