"""SQL construction of the information_schema loader (no database required)."""

from __future__ import annotations

from mylox.config import MySQLSettings
from mylox.filtering import TableFilter
from mylox.schema.loader import (
    _COLUMNS_SQL,
    _PRIMARY_KEYS_SQL,
    InformationSchemaLoader,
    _table_filter_sql,
)


def test_table_filter_sql_uses_the_right_alias():
    columns_filter = _table_filter_sql("c", 2)
    assert columns_filter == "((c.TABLE_SCHEMA = %s AND c.TABLE_NAME = %s) OR (c.TABLE_SCHEMA = %s AND c.TABLE_NAME = %s))"
    pk_filter = _table_filter_sql("s", 1)
    assert pk_filter == "((s.TABLE_SCHEMA = %s AND s.TABLE_NAME = %s))"


def test_columns_query_joins_and_stays_unambiguous():
    # the joined query has both `c` and `t` aliases, so the table predicate must
    # be qualified with the same alias it is appended with
    assert "JOIN information_schema.TABLES t" in _COLUMNS_SQL
    assert "c.TABLE_SCHEMA NOT IN" in _COLUMNS_SQL
    assert "s.TABLE_SCHEMA NOT IN" in _PRIMARY_KEYS_SQL
    assert "STATISTICS" in _PRIMARY_KEYS_SQL and "SEQ_IN_INDEX" in _PRIMARY_KEYS_SQL


def test_loader_requires_settings_and_exposes_a_filter():
    settings = MySQLSettings(host="replica", user="cdc")
    # gitignore semantics: ignore everything, then re-include `db.*`
    loader = InformationSchemaLoader(settings, TableFilter(["*", "!db.*"]))
    assert loader.settings is settings
    assert loader.table_filter.is_allowed("db", "t") is True
    assert loader.table_filter.is_allowed("other", "t") is False
