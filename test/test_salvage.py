"""Raw DDL text salvage helpers."""

from __future__ import annotations

from mylox.schema.salvage import (
    is_ddl_statement,
    looks_like_dml,
    parse_rename_table,
    salvage_table_refs,
    split_ref,
    unquote,
)


def test_is_ddl_statement_and_looks_like_dml():
    assert is_ddl_statement("  create table t (a int)") is True
    assert is_ddl_statement("TRUNCATE t") is True
    assert is_ddl_statement("insert into t values (1)") is False
    assert is_ddl_statement("") is False
    assert looks_like_dml("UPDATE t SET a = 1") is True
    assert looks_like_dml("REPLACE INTO t VALUES (1)") is True
    assert looks_like_dml("SELECT 1") is False


def test_split_ref_and_unquote():
    assert split_ref("`db`.`t`") == ("db", "t")
    assert split_ref("t") == (None, "t")
    assert unquote("`x`") == "x"
    assert unquote(None) is None


def test_salvage_table_refs_for_each_supported_statement():
    assert salvage_table_refs("CREATE TABLE `db`.`t` (a int)", None) == (("db", "t"),)
    assert salvage_table_refs("ALTER TABLE t ADD c INT", "d") == (("d", "t"),)
    assert salvage_table_refs("DROP TABLE a, b", "d") == (("d", "a"), ("d", "b"))
    assert salvage_table_refs("TRUNCATE TABLE d.t", None) == (("d", "t"),)
    assert salvage_table_refs("CREATE INDEX i ON d.t (c)", None) == (("d", "t"),)
    # database level statements do not name a table
    assert salvage_table_refs("DROP DATABASE d", None) == ()


def test_salvage_of_an_unrecognised_statement_uses_backticked_tokens():
    assert salvage_table_refs("SOMETHING WEIRD ON `d`.`t`", None) == (("d", "t"),)


def test_parse_rename_table():
    assert parse_rename_table("RENAME TABLE a TO b", "d") == [("d", "a", "d", "b")]
    assert parse_rename_table("RENAME TABLE d.a TO e.b, c TO d2", "d") == [
        ("d", "a", "e", "b"),
        ("d", "c", "d", "d2"),
    ]
    assert parse_rename_table("ALTER TABLE a RENAME TO b", "d") is None
    assert parse_rename_table("RENAME TABLE", "d") is None
