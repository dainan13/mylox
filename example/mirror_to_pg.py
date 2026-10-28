#!/usr/bin/env python
"""Example: mirror filtered MySQL tables into PostgreSQL, one sink table per source table.

Run it::

    export MYLOX_MYSQL_HOST=192.168.13.61 MYLOX_MYSQL_PORT=3307 \\
           MYLOX_MYSQL_USER=mylox MYLOX_MYSQL_PASSWORD='...'      # 从库 = binlog 源
    export MYLOX_PG_HOST=192.168.13.61 MYLOX_PG_PORT=5432 \\
           MYLOX_PG_DBNAME=mylox MYLOX_PG_USER=mylox MYLOX_PG_PASSWORD='...'

    python example/mirror_to_pg.py --pipelines orders \\
        --rule 'shop.*' --rule '!shop.tmp_*'

What it demonstrates, in the order you would write it yourself:

1. settings + ``EngineConfig`` read from ``MYLOX_*`` environment variables;
2. :meth:`CDCHandler.on_init` - the frozen-anchor full load, using a streaming
   MySQL cursor (no OOM) and a dedicated autocommit PostgreSQL connection;
3. :meth:`CDCHandler.handle_row` - the INSERT / UPDATE / DELETE fan-out, applied
   idempotently through the source primary key (``change.modify`` carries only
   the columns that really changed);
4. the sink DDL derived from the schema model, including a MySQL -> PostgreSQL
   type mapping;
5. the failure hooks (``on_pipeline_failed`` / ``on_schema_warning``).

Sink tables are **derived data**: every write is an upsert keyed by the source
primary key, so re-delivering a transaction (at-least-once) is harmless.

Note the pipeline dimension: mylox calls ``handle_row`` once per pipeline, so
the sink name includes the pipeline.  A real "wide table" handler usually
writes pipeline-specific aggregation SQL instead.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from mylox import (  # noqa: E402
    CDCEngine,
    CDCHandler,
    EngineConfig,
    MySQLSettings,
    PostgresSettings,
    RowChange,
    configure_logging,
    env_settings,
    get_logger,
)

logger = get_logger("example")

#: Simplified MySQL -> PostgreSQL type mapping.  Extend it for your own schema:
#: only the sink tables are created from it, the source is never touched.
_TYPE_MAP = (
    (r"^tinyint\(1\)", "boolean"),
    (r"^tinyint\b", "smallint"),
    (r"^smallint\b", "smallint"),
    (r"^mediumint\b", "integer"),  # MySQL only: no PostgreSQL equivalent
    (r"^int\b", "integer"),
    (r"^bigint\b", "bigint"),
    (r"^(decimal|numeric)\b", "numeric"),
    (r"^float\b", "real"),
    (r"^(double|real)\b", "double precision"),
    (r"^(varchar|char|text|tinytext|mediumtext|longtext|enum|set)\b", "text"),
    (r"^json\b", "jsonb"),
    (r"^(tinyblob|blob|mediumblob|longblob|binary|varbinary)\b", "bytea"),
    (r"^datetime\b", "timestamptz"),
    (r"^timestamp\b", "timestamptz"),
    (r"^date\b", "date"),
    (r"^time\b", "time"),
)

#: PostgreSQL does not accept a length/precision modifier on these types
#: (``integer(11)`` is a syntax error), while ``numeric(10,2)``,
#: ``timestamptz(3)`` and ``time(6)`` do accept one.
_NO_LENGTH = {
    "integer",
    "bigint",
    "smallint",
    "real",
    "double precision",
    "boolean",
    "text",
    "jsonb",
    "bytea",
    "date",
    "timestamptz",
    "time",
}


def pg_type(column_type: str) -> str:
    """Best-effort MySQL -> PostgreSQL type, keeping length/precision."""
    lowered = (column_type or "").strip().lower()
    if lowered.endswith("unsigned"):
        lowered = lowered[: -len("unsigned")].strip()
    match = re.match(r"^[a-z]+\(([\d,\s]+)\)", lowered)
    length = "(%s)" % match.group(1) if match else ""
    for pattern, target in _TYPE_MAP:
        if re.match(pattern, lowered):
            if target in _NO_LENGTH:
                return target
            return "%s%s" % (target, length)
    logger.warning("unknown MySQL type %r, falling back to text", column_type)
    return "text"


def quote_ident(name: str) -> str:
    return '"%s"' % str(name).replace('"', '""')


def sink_table_name(pipeline: str, db: str, table: str) -> str:
    """``mirror_<pipeline>_<db>_<table>``, sanitised and length-capped."""
    raw = "mirror_%s_%s_%s" % (pipeline, db, table)
    safe = re.sub(r"[^0-9A-Za-z_]", "_", raw).lower()
    if len(safe) <= 60:
        return safe
    return "%s_%s" % (safe[:50], hashlib.md5(raw.encode("utf-8")).hexdigest()[:8])


def sink_ddl(target: str, schema) -> str:
    columns = []
    for column in schema.columns:
        not_null = " NOT NULL" if column.name in schema.primary_keys else ""
        columns.append("%s %s%s" % (quote_ident(column.name), pg_type(column.column_type), not_null))
    columns.append("PRIMARY KEY (%s)" % ", ".join(quote_ident(pk) for pk in schema.primary_keys))
    return "CREATE TABLE IF NOT EXISTS %s (%s)" % (quote_ident(target), ", ".join(columns))


class MirrorHandler(CDCHandler):
    """Upsert every change into a per-pipeline, per-source-table sink table."""

    # -- full load at the frozen anchor ---------------------------------
    def on_init(self, pipelines, ctx) -> None:
        for pipeline in sorted(pipelines):
            for schema in ctx.all_schemas():
                if not schema.primary_keys:
                    logger.warning("skipping %s: no primary key, cannot mirror idempotently", schema.name)
                    continue
                target = sink_table_name(pipeline, schema.db, schema.table)

                with ctx.pg_cursor() as cur:  # dedicated autocommit connection
                    cur.execute(sink_ddl(target, schema))

                with ctx.mysql_cursor(streaming=True) as cur:  # SSDictCursor: no OOM
                    cur.execute(
                        "SELECT * FROM %s.%s" % (quote_ident(schema.db), quote_ident(schema.table))
                    )
                    rows = list(cur)

                columns = list(schema.column_names)
                statement = (
                    "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s"
                    % (
                        quote_ident(target),
                        ", ".join(quote_ident(name) for name in columns),
                        ", ".join(["%s"] * len(columns)),
                        ", ".join(quote_ident(pk) for pk in schema.primary_keys),
                        ", ".join(
                            "%s = EXCLUDED.%s" % (quote_ident(name), quote_ident(name)) for name in columns
                        ),
                    )
                )
                with ctx.pg_cursor() as cur:
                    for row in rows:
                        cur.execute(statement, tuple(row.get(name) for name in columns))
                logger.info("[%s] initialized %s -> %s (%d rows)", pipeline, schema.name, target, len(rows))

    # -- incremental ----------------------------------------------------
    def handle_row(self, change: RowChange, pipeline: str, cur) -> None:
        primary_keys = list(change.primary_keys)
        if not primary_keys:
            return
        target = quote_ident(sink_table_name(pipeline, change.db, change.table))
        where = " AND ".join("%s = %%s" % quote_ident(pk) for pk in primary_keys)
        keys = tuple(change.key_values().get(pk) for pk in primary_keys)

        if change.action.value == "DELETE":
            cur.execute("DELETE FROM %s WHERE %s" % (target, where), keys)
            return

        if change.action.value == "UPDATE":
            changed = change.modify or {}
            if not changed:
                return
            assignments = ", ".join("%s = %%s" % quote_ident(name) for name in changed)
            cur.execute(
                "UPDATE %s SET %s WHERE %s" % (target, assignments, where),
                tuple(changed.values()) + keys,
            )
            return

        values = change.after or {}
        columns = [name for name in values]
        cur.execute(
            "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s"
            % (
                target,
                ", ".join(quote_ident(name) for name in columns),
                ", ".join(["%s"] * len(columns)),
                ", ".join(quote_ident(pk) for pk in primary_keys),
                ", ".join("%s = EXCLUDED.%s" % (quote_ident(name), quote_ident(name)) for name in columns),
            ),
            tuple(values[name] for name in columns),
        )

    # -- hooks ----------------------------------------------------------
    def on_pipeline_failed(self, pipeline: str, error: BaseException, change) -> None:
        logger.error("[%s] pipeline parked: %s", pipeline, error)

    def on_schema_warning(self, message: str, detail) -> None:
        logger.warning("schema warning: %s %s", message, detail)


def settings_from_env(prefix: str) -> dict:
    values = env_settings(prefix)
    if not values.get("host"):
        raise SystemExit("%s_HOST is required" % prefix)
    values.setdefault("user", "root")
    return values


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="mylox example: MySQL -> PostgreSQL mirror")
    parser.add_argument("--pipelines", nargs="+", default=["orders"], help="pipeline names")
    parser.add_argument("--rule", action="append", default=[], help="table filter rule (repeatable)")
    parser.add_argument("--namespace", default=None, help="schema history namespace")
    parser.add_argument("--server-id", type=int, default=None, help="binlog client server_id")
    parser.add_argument("--no-pause", action="store_true", help="do not STOP REPLICA for the anchor")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    mysql = MySQLSettings.from_dict(settings_from_env("MYLOX_MYSQL"))
    pg_values = settings_from_env("MYLOX_PG")
    if not pg_values.get("dbname"):
        raise SystemExit("MYLOX_PG_DBNAME is required")

    engine = CDCEngine(
        mysql,
        PostgresSettings.from_dict(pg_values),
        args.pipelines,
        MirrorHandler(),
        EngineConfig(
            namespace=args.namespace,
            filter_rules=args.rule,
            server_id=args.server_id,
            pause_slave_on_init=not args.no_pause,
        ),
    )
    engine.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
