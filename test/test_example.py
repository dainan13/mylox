"""Keeps ``example/mirror_to_pg.py`` honest without touching a database.

The example lives outside ``src``; it is loaded from its path so that a syntax
error, a broken type mapping or a bad identifier helper fails the test suite.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "example" / "mirror_to_pg.py"


@pytest.fixture(scope="module")
def example():
    spec = importlib.util.spec_from_file_location("mylox_example_mirror", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_example_imports(example):
    assert hasattr(example, "MirrorHandler")
    assert hasattr(example, "main")


def test_type_mapping(example):
    mapping = {
        # PostgreSQL rejects length modifiers on the integer family
        "int(11)": "integer",
        "int(10) unsigned": "integer",
        "bigint(20)": "bigint",
        "smallint(5)": "smallint",
        "mediumint(8)": "integer",
        "tinyint(1)": "boolean",
        "varchar(64)": "text",
        "float": "real",
        "double": "double precision",
        # ... but keeps them on numeric / temporal
        "decimal(10,2)": "numeric(10,2)",
        "datetime(3)": "timestamptz",
        "time(6)": "time",
        "date": "date",
        "json": "jsonb",
        "longblob": "bytea",
    }
    for source, expected in mapping.items():
        assert example.pg_type(source) == expected, source
    # unknown types must not raise, they degrade to text
    assert example.pg_type("geometry") == "text"


def test_identifier_quoting(example):
    assert example.quote_ident("a") == '"a"'
    assert example.quote_ident('we"ird') == '"we""ird"'


def test_sink_table_name_is_stable_and_safe(example):
    assert example.sink_table_name("orders", "shop", "t_order") == "mirror_orders_shop_t_order"
    # a 64 char MySQL identifier survives; an over-long combination is hashed
    long_name = example.sink_table_name("p" * 40, "d" * 40, "t" * 40)
    assert len(long_name) <= 60
    assert example.sink_table_name("p" * 40, "d" * 40, "t" * 40) == long_name
    assert "-" not in example.sink_table_name("a-b", "c.d", "e")


def test_sink_ddl_declares_the_primary_key(example):
    from fakes import make_schema

    schema = make_schema("shop", "t_order", ["id", "amount"], ["id"], types={"amount": "decimal(10,2)"})
    ddl = example.sink_ddl("mirror_x", schema)
    assert ddl.startswith('CREATE TABLE IF NOT EXISTS "mirror_x" (')
    assert '"id" integer NOT NULL' in ddl
    assert '"amount" numeric(10,2)' in ddl
    assert 'PRIMARY KEY ("id")' in ddl


def test_handle_row_builds_idempotent_statements(example):
    """Drive the handler with a fake cursor and check the generated SQL."""
    from mylox.engine import ChangeAction, RowChange

    class Cursor:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params=None):
            self.calls.append((" ".join(sql.split()), params))

    handler = example.MirrorHandler()
    cursor = Cursor()

    handler.handle_row(
        RowChange(
            db="shop",
            table="t_order",
            action=ChangeAction.INSERT,
            primary_keys=("id",),
            after={"id": 1, "amount": "10.00"},
        ),
        "orders",
        cursor,
    )
    handler.handle_row(
        RowChange(
            db="shop",
            table="t_order",
            action=ChangeAction.UPDATE,
            primary_keys=("id",),
            before={"id": 1, "amount": "10.00"},
            modify={"amount": "12.00"},
        ),
        "orders",
        cursor,
    )
    handler.handle_row(
        RowChange(
            db="shop",
            table="t_order",
            action=ChangeAction.DELETE,
            primary_keys=("id",),
            before={"id": 1, "amount": "12.00"},
        ),
        "orders",
        cursor,
    )
    # a table without a primary key cannot be mirrored idempotently
    handler.handle_row(
        RowChange(db="shop", table="no_pk", action=ChangeAction.INSERT, after={"a": 1}),
        "orders",
        cursor,
    )

    assert len(cursor.calls) == 3
    insert_sql, insert_params = cursor.calls[0]
    assert insert_sql.startswith('INSERT INTO "mirror_orders_shop_t_order" ("id", "amount") VALUES (%s, %s)')
    assert "ON CONFLICT (\"id\") DO UPDATE SET" in insert_sql
    assert insert_params == (1, "10.00")

    update_sql, update_params = cursor.calls[1]
    assert update_sql == 'UPDATE "mirror_orders_shop_t_order" SET "amount" = %s WHERE "id" = %s'
    assert update_params == ("12.00", 1)

    delete_sql, delete_params = cursor.calls[2]
    assert delete_sql == 'DELETE FROM "mirror_orders_shop_t_order" WHERE "id" = %s'
    assert delete_params == (1,)
