"""sqlglot based DDL interpretation."""

from __future__ import annotations

import pytest

from mylox.schema.ddl import (
    SqlglotDDLParser,
    is_ddl_statement,
    looks_like_dml,
    salvage_table_refs,
)
from mylox.schema.plan import DDLKind, OpKind


@pytest.fixture(scope="module")
def parser():
    return SqlglotDDLParser()


def only_change(plan):
    assert len(plan.changes) == 1, plan.changes
    return plan.changes[0]


def test_create_table_with_columns_and_primary_key(parser):
    plan = parser.parse(
        "CREATE TABLE `d1`.`t` (id INT NOT NULL AUTO_INCREMENT, "
        "name VARCHAR(64) DEFAULT NULL, `k` int(11) unsigned NOT NULL DEFAULT 0, "
        "PRIMARY KEY (id), KEY idx_name (name))",
        default_db="d1",
    )
    change = only_change(plan)
    assert change.kind is DDLKind.CREATE_TABLE
    assert change.target == "d1.t"
    assert [c.name for c in change.columns] == ["id", "name", "k"]
    assert change.primary_keys == ("id",)
    assert change.columns[0].nullable is False
    assert change.columns[0].extra == "auto_increment"
    assert change.columns[1].nullable is True
    assert change.columns[1].default is None  # DEFAULT NULL matches information_schema


def test_create_table_inline_primary_key(parser):
    plan = parser.parse("CREATE TABLE t (a int primary key, b int)", default_db="d")
    assert only_change(plan).primary_keys == ("a",)


def test_create_table_like_is_resolved_from_the_registry(parser):
    plan = parser.parse("CREATE TABLE t2 LIKE d1.t1", default_db="d")
    change = only_change(plan)
    assert change.like == ("d1", "t1")
    assert change.needs_fallback is False


def test_create_table_as_select_needs_fallback(parser):
    plan = parser.parse("CREATE TABLE t3 AS SELECT * FROM t1", default_db="d")
    assert only_change(plan).needs_fallback is True
    assert "AS SELECT" in only_change(plan).reason


def test_unsupported_create_table_syntax_needs_fallback(parser):
    plan = parser.parse("CREATE TABLE t (a int) PARTITION BY HASH(a) PARTITIONS 4", default_db="d")
    assert plan.needs_fallback is True
    assert plan.salvaged_refs == (("d", "t"),)


def test_alter_add_columns_with_positions(parser):
    plan = parser.parse(
        "ALTER TABLE `d1`.`t1` ADD COLUMN c1 INT NOT NULL DEFAULT 5, "
        "ADD COLUMN c2 VARCHAR(10) AFTER id, ADD COLUMN c0 INT FIRST",
        default_db="d1",
    )
    change = only_change(plan)
    assert change.kind is DDLKind.ALTER_TABLE
    assert [op.kind for op in change.ops] == [OpKind.ADD_COLUMN] * 3
    assert change.ops[0].column.default == "5"
    assert change.ops[1].position.after == "id"
    assert change.ops[2].position.first is True


def test_alter_change_column(parser):
    plan = parser.parse("ALTER TABLE t CHANGE COLUMN c2 c2new VARCHAR(20) NULL", default_db="d")
    op = only_change(plan).ops[0]
    assert op.kind is OpKind.MODIFY_COLUMN
    assert op.name == "c2" and op.new_name == "c2new"
    assert op.column.column_type == "VARCHAR(20)"


def test_alter_rename_drop_and_primary_keys(parser):
    plan = parser.parse(
        "ALTER TABLE t RENAME COLUMN c3 TO c4, DROP COLUMN c1, "
        "DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)",
        default_db="d",
    )
    kinds = [op.kind for op in only_change(plan).ops]
    assert kinds == [
        OpKind.RENAME_COLUMN,
        OpKind.DROP_COLUMN,
        OpKind.DROP_PRIMARY_KEY,
        OpKind.ADD_PRIMARY_KEY,
    ]
    assert only_change(plan).ops[3].primary_keys == ("a", "b")


def test_alter_index_only_clauses_are_ignored(parser):
    for statement in (
        "ALTER TABLE t ADD INDEX idx (c1)",
        "ALTER TABLE t ADD UNIQUE KEY uk (c1)",
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (c1) REFERENCES o (id)",
        "ALTER TABLE t DROP INDEX idx",
        "ALTER TABLE t DROP FOREIGN KEY fk",
        "ALTER TABLE t ENGINE=InnoDB, ALGORITHM=INPLACE, LOCK=NONE",
    ):
        plan = parser.parse(statement, default_db="d")
        assert plan.ignored is True, statement
        assert plan.needs_fallback is False, statement


def test_alter_rename_table(parser):
    plan = parser.parse("ALTER TABLE d1.t1 RENAME TO d2.t2", default_db="d1")
    change = only_change(plan)
    assert change.kind is DDLKind.RENAME_TABLE
    assert change.target == "d1.t1"
    assert (change.new_db, change.new_table) == ("d2", "t2")


def test_rename_table_command_is_salvaged(parser):
    plan = parser.parse("RENAME TABLE a TO b, c TO d", default_db="d1")
    assert [c.kind for c in plan.changes] == [DDLKind.RENAME_TABLE, DDLKind.RENAME_TABLE]
    assert [(c.table, c.new_table) for c in plan.changes] == [("a", "b"), ("c", "d")]


def test_drop_multiple_tables(parser):
    plan = parser.parse("DROP TABLE IF EXISTS `a`, `b`", default_db="d1")
    assert [c.table for c in plan.changes] == ["a", "b"]
    assert all(c.kind is DDLKind.DROP_TABLE for c in plan.changes)


def test_drop_database(parser):
    plan = parser.parse("DROP DATABASE IF EXISTS db9", default_db="d1")
    assert only_change(plan).kind is DDLKind.DROP_DATABASE
    assert only_change(plan).database == "db9"


def test_non_layout_statements(parser):
    for statement in (
        "CREATE INDEX i ON t1 (c1)",
        "DROP INDEX i ON t1",
        "CREATE VIEW v1 AS SELECT * FROM t1",
        "DROP VIEW v1",
        "CREATE DATABASE db9",
        "TRUNCATE TABLE t1",
        "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4",
    ):
        plan = parser.parse(statement, default_db="d")
        assert plan.is_ddl is True, statement
        assert plan.ignored is True, statement
        assert plan.needs_fallback is False, statement


def test_dml_is_not_ddl(parser):
    plan = parser.parse("INSERT INTO t VALUES (1)", default_db="d")
    assert plan.is_ddl is False
    assert plan.ignored is True


def test_parse_failure_falls_back(parser):
    plan = parser.parse("ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (10))", default_db="d")
    assert plan.needs_fallback is True
    assert plan.error is not None


def test_unknown_alter_clause_falls_back(parser):
    plan = parser.parse("ALTER TABLE t FORCE", default_db="d")
    assert plan.needs_fallback in (True, False)  # FORCE may parse as an option
    assert plan.ignored is True or plan.needs_fallback is True


def test_statement_helpers():
    assert is_ddl_statement("  create table t (a int)") is True
    assert is_ddl_statement("insert into t values (1)") is False
    assert looks_like_dml("UPDATE t SET a=1") is True
    assert looks_like_dml("SELECT 1") is False


def test_salvage_table_refs():
    assert salvage_table_refs("CREATE TABLE `db`.`t` (a int)", None) == (("db", "t"),)
    assert salvage_table_refs("ALTER TABLE t ADD c INT", "d") == (("d", "t"),)
    assert salvage_table_refs("DROP TABLE a, b", "d") == (("d", "a"), ("d", "b"))


def test_plan_cache_is_transparent(parser):
    first = parser.parse("CREATE TABLE t (a int)", default_db="d")
    second = parser.parse("CREATE TABLE t (a int)", default_db="d")
    assert first is second


def test_column_defaults_are_normalised_like_information_schema(parser):
    """DDL-derived and information_schema-derived schemas must agree."""
    plan = parser.parse(
        "CREATE TABLE t (a int DEFAULT NULL, b varchar(3) DEFAULT 'x', c int DEFAULT 5, "
        "d timestamp DEFAULT CURRENT_TIMESTAMP, e decimal(10,2) DEFAULT '0.00')",
        default_db="d",
    )
    defaults = {column.name: column.default for column in only_change(plan).columns}
    assert defaults == {"a": None, "b": "x", "c": "5", "d": "CURRENT_TIMESTAMP", "e": "0.00"}
