"""Read table schemas and server state from the MySQL replica.

``information_schema`` is the authoritative source for two situations:

1. the **initial** snapshot taken at the initialization anchor, and
2. the **fallback** whenever a DDL could not be interpreted locally.

Every method opens and closes its own short lived connection: the engine's
long-running binlog connection is never used for metadata queries, so a slow
metadata query can never stall the stream.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pymysql
import pymysql.cursors

from ..config import MySQLSettings
from ..constants import SYSTEM_SCHEMAS
from ..errors import MyloxError
from ..filtering import TableFilter
from ..logging_utils import get_logger
from ..utils.positions import BinlogPosition
from .model import ColumnSchema, TableSchema

logger = get_logger(__name__)

_COLUMNS_SQL = """
    SELECT c.TABLE_SCHEMA AS db_name,
           c.TABLE_NAME   AS table_name,
           c.COLUMN_NAME  AS column_name,
           c.COLUMN_TYPE  AS column_type,
           c.IS_NULLABLE  AS is_nullable,
           c.COLUMN_DEFAULT AS column_default,
           c.EXTRA        AS extra,
           c.ORDINAL_POSITION AS ordinal_position
    FROM information_schema.COLUMNS c
    JOIN information_schema.TABLES t
      ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
    WHERE t.TABLE_TYPE = 'BASE TABLE'
      AND c.TABLE_SCHEMA NOT IN ({system_schemas})
"""

_PRIMARY_KEYS_SQL = """
    SELECT s.TABLE_SCHEMA AS db_name,
           s.TABLE_NAME   AS table_name,
           s.COLUMN_NAME  AS column_name,
           s.SEQ_IN_INDEX AS seq_in_index
    FROM information_schema.STATISTICS s
    WHERE s.INDEX_NAME = 'PRIMARY'
      AND s.TABLE_SCHEMA NOT IN ({system_schemas})
"""


def _placeholders(count: int) -> str:
    return ", ".join(["%s"] * count)


def _table_filter_sql(alias: str, count: int) -> str:
    """``(alias.TABLE_SCHEMA = %s AND alias.TABLE_NAME = %s) OR ...``

    The alias must match the query it is appended to (``c`` for the COLUMNS
    join, ``s`` for STATISTICS) - an unqualified ``TABLE_SCHEMA`` is ambiguous
    in the joined COLUMNS query.
    """
    clause = "(%s.TABLE_SCHEMA = %%s AND %s.TABLE_NAME = %%s)" % (alias, alias)
    return "(" + " OR ".join([clause] * count) + ")"


class InformationSchemaLoader:
    """Short-lived, dict-cursor connections to the replica."""

    def __init__(self, settings: MySQLSettings, table_filter: Optional[TableFilter] = None) -> None:
        if not isinstance(settings, MySQLSettings):
            raise MyloxError("InformationSchemaLoader requires MySQLSettings")
        self.settings = settings
        self.table_filter = table_filter or TableFilter()

    # -- connection handling ------------------------------------------
    def connect(self) -> pymysql.connections.Connection:
        return pymysql.connect(**self.settings.cursor_kwargs(pymysql.cursors.DictCursor))

    def _system_schema_sql(self) -> Tuple[str, Tuple[Any, ...]]:
        return _placeholders(len(SYSTEM_SCHEMAS)), tuple(SYSTEM_SCHEMAS)

    # -- schema queries -----------------------------------------------
    def load_all(self) -> List[TableSchema]:
        """Every filtered base table of the whole server."""
        with self.connect() as conn, conn.cursor() as cursor:
            columns = self._fetch_columns(cursor)
            primary_keys = self._fetch_primary_keys(cursor)
        return self._assemble(columns, primary_keys, allowed_refs=None)

    def load_tables(self, refs: Iterable[Tuple[Optional[str], str]]) -> List[TableSchema]:
        """Schemas for specific ``(db, table)`` pairs, one round trip per query."""
        wanted: List[Tuple[str, str]] = []
        seen = set()
        for db, table in refs:
            if not db or not table:
                continue
            if (db, table) in seen:
                continue
            seen.add((db, table))
            wanted.append((db, table))
        if not wanted:
            return []

        params: List[Any] = []
        for db, table in wanted:
            params.extend([db, table])

        system_sql, system_params = self._system_schema_sql()
        with self.connect() as conn, conn.cursor() as cursor:
            cursor.execute(
                _COLUMNS_SQL.format(system_schemas=system_sql) + " AND " + _table_filter_sql("c", len(wanted)),
                system_params + tuple(params),
            )
            columns = cursor.fetchall()

            cursor.execute(
                _PRIMARY_KEYS_SQL.format(system_schemas=system_sql)
                + " AND "
                + _table_filter_sql("s", len(wanted)),
                system_params + tuple(params),
            )
            primary_keys = cursor.fetchall()

        allowed = {ref for ref in wanted}
        return self._assemble(
            columns,
            primary_keys,
            allowed_refs=allowed,
            skip_filter=True,
        )

    def count_base_tables(self) -> int:
        """How many user base tables exist, before the filter is applied."""
        system_sql, system_params = self._system_schema_sql()
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM information_schema.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN (%s)" % system_sql,
                system_params,
            )
            row = cur.fetchone() or {}
            return int(row.get("n") or 0)

    # -- server state -------------------------------------------------
    def server_info(self) -> Dict[str, Any]:
        """``server_uuid`` / ``server_id`` of the replica (5.6+)."""
        try:
            with self.connect() as conn, conn.cursor() as cursor:
                cursor.execute("SELECT @@server_uuid AS server_uuid, @@server_id AS server_id")
                row = cursor.fetchone() or {}
                return {"server_uuid": row.get("server_uuid"), "server_id": row.get("server_id")}
        except Exception as exc:
            logger.warning("Could not read @@server_uuid, falling back to @@server_id only: %s", exc)
        with self.connect() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT @@server_id AS server_id")
            row = cursor.fetchone() or {}
            return {"server_uuid": None, "server_id": row.get("server_id")}

    def master_status(self) -> Dict[str, Any]:
        """Current binlog coordinates of the replica's own binlog.

        ``SHOW MASTER STATUS`` was renamed to ``SHOW BINARY LOG STATUS`` in
        MySQL 8.4, so both spellings are attempted.
        """
        last_error: Optional[BaseException] = None
        for statement in ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"):
            try:
                with self.connect() as conn, conn.cursor() as cursor:
                    cursor.execute(statement)
                    row = cursor.fetchone()
                if row:
                    return {
                        "log_file": row.get("File"),
                        "log_pos": row.get("Position"),
                        "gtid_set": row.get("Executed_Gtid_Set") or "",
                    }
                return {}
            except Exception as exc:  # pragma: no cover - server specific
                last_error = exc
                continue
        if last_error is not None:
            raise MyloxError("could not read the replica binlog coordinates: %s" % (last_error,))
        return {}

    def position(self) -> Optional[BinlogPosition]:
        status = self.master_status()
        return BinlogPosition.maybe(status.get("log_file"), status.get("log_pos"))

    # -- replication control ------------------------------------------
    def pause_replication(self) -> bool:
        return self._try_statements(("STOP REPLICA", "STOP SLAVE"), "paused")

    def resume_replication(self) -> bool:
        return self._try_statements(("START REPLICA", "START SLAVE"), "resumed")

    def _try_statements(self, statements: Sequence[str], action: str) -> bool:
        errors: List[str] = []
        for statement in statements:
            try:
                with self.connect() as conn, conn.cursor() as cursor:
                    cursor.execute(statement)
                logger.info("Replica replication %s via %r", action, statement)
                return True
            except Exception as exc:
                errors.append("%s -> %s" % (statement, exc))
        logger.warning("Could not %s replica replication: %s", action.rstrip("d"), "; ".join(errors))
        return False

    # -- assembly -----------------------------------------------------
    def _fetch_columns(self, cursor) -> List[Dict[str, Any]]:
        system_sql, system_params = self._system_schema_sql()
        cursor.execute(_COLUMNS_SQL.format(system_schemas=system_sql), system_params)
        return list(cursor.fetchall())

    def _fetch_primary_keys(self, cursor) -> List[Dict[str, Any]]:
        system_sql, system_params = self._system_schema_sql()
        cursor.execute(_PRIMARY_KEYS_SQL.format(system_schemas=system_sql), system_params)
        return list(cursor.fetchall())

    def _assemble(
        self,
        column_rows: Sequence[Dict[str, Any]],
        pk_rows: Sequence[Dict[str, Any]],
        allowed_refs: Optional[set] = None,
        skip_filter: bool = False,
    ) -> List[TableSchema]:
        grouped: Dict[Tuple[str, str], List[ColumnSchema]] = {}
        for row in column_rows:
            db = row["db_name"]
            table = row["table_name"]
            if allowed_refs is not None and (db, table) not in allowed_refs:
                continue
            if not skip_filter and not self.table_filter.is_allowed(db, table):
                continue
            grouped.setdefault((db, table), []).append(
                ColumnSchema(
                    name=row["column_name"],
                    column_type=row.get("column_type") or "",
                    nullable=(row.get("is_nullable") or "YES").upper() == "YES",
                    default=row.get("column_default"),
                    extra=row.get("extra") or "",
                    ordinal=int(row.get("ordinal_position") or len(grouped.get((db, table), []))),
                )
            )

        pks: Dict[Tuple[str, str], List[Tuple[int, str]]] = {}
        for row in pk_rows:
            key = (row["db_name"], row["table_name"])
            if allowed_refs is not None and key not in allowed_refs:
                continue
            if not skip_filter and not self.table_filter.is_allowed(*key):
                continue
            pks.setdefault(key, []).append((int(row.get("seq_in_index") or 0), row["column_name"]))

        schemas: List[TableSchema] = []
        for (db, table), columns in grouped.items():
            ordered = sorted(columns, key=lambda column: column.ordinal)
            primary_keys = tuple(name for _, name in sorted(pks.get((db, table), [])))
            schemas.append(
                TableSchema(db=db, table=table, columns=tuple(ordered), primary_keys=primary_keys)
            )
        schemas.sort(key=lambda schema: (schema.db, schema.table))
        return schemas
