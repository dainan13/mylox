"""Context handed to :meth:`mylox.CDCHandler.on_init`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import psycopg
import pymysql
import pymysql.cursors

from ..config import MySQLSettings, PostgresSettings
from ..schema.manager import SchemaManager
from ..utils.positions import BinlogPosition


@dataclass
class InitContext:
    """Connections and metadata available while initializing pipelines.

    ``slave_connection(streaming=True)`` uses ``SSDictCursor`` so a full table
    scan does not buffer the whole result set in memory.
    """

    pipelines: Any
    anchor: Optional[BinlogPosition]
    server_uuid: Optional[str] = None
    server_id: Optional[int] = None
    mysql_settings: Optional[MySQLSettings] = None
    pg_settings: Optional[PostgresSettings] = None
    schema_manager: Optional[SchemaManager] = None
    filter: Any = None
    engine: Any = None

    # -- MySQL ---------------------------------------------------------
    def slave_connection(self, streaming: bool = False) -> pymysql.connections.Connection:
        if self.mysql_settings is None:
            raise RuntimeError("no MySQL settings available in this context")
        cursor_class = pymysql.cursors.SSDictCursor if streaming else pymysql.cursors.DictCursor
        return pymysql.connect(**self.mysql_settings.cursor_kwargs(cursor_class))

    def mysql_cursor(self, streaming: bool = False):
        """Context manager yielding a cursor on a fresh MySQL connection."""
        return _CursorScope(self.slave_connection(streaming))

    # -- PostgreSQL ----------------------------------------------------
    def pg_connection(self, autocommit: bool = True) -> psycopg.Connection:
        if self.pg_settings is None:
            raise RuntimeError("no PostgreSQL settings available in this context")
        return psycopg.connect(**self.pg_settings.connect_kwargs(autocommit=autocommit))

    def pg_cursor(self, autocommit: bool = True):
        """Context manager yielding a cursor on a dedicated PG connection.

        The connection is committed (or rolled back on error) and closed on
        exit, so a bulk load cannot leak into the engine's transaction.  Using
        ``autocommit=True`` also avoids the psycopg v3 context-manager pitfall
        where an exception triggers an implicit ``ROLLBACK`` that would undo the
        DDL executed during initialization.
        """
        return _PgCursorScope(self.pg_connection(autocommit=autocommit))

    # -- helpers -------------------------------------------------------
    def table_schema(self, db: str, table: str):
        if self.schema_manager is None:
            return None
        return self.schema_manager.schema_for(db, table, self.anchor, required=False)

    def all_schemas(self):
        if self.schema_manager is None:
            return []
        return [version.schema for version in self.schema_manager.registry.snapshot()]


class _CursorScope:
    def __init__(self, connection: pymysql.connections.Connection) -> None:
        self.connection = connection
        self.cursor = None

    def __enter__(self):
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc, tb):
        try:
            self.connection.close()
        finally:
            self.cursor = None
        return False


class _PgCursorScope:
    def __init__(self, connection: psycopg.Connection) -> None:
        self.connection = connection
        self.cursor = None

    def __enter__(self):
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None and not self.connection.closed:
                self.connection.commit()
        except Exception:  # pragma: no cover - defensive
            pass
        finally:
            try:
                self.connection.close()
            except Exception:  # pragma: no cover - defensive
                pass
            self.cursor = None
        return False
