"""PostgreSQL persistence: checkpoints and schema history.

Two connections are used on purpose:

``transaction connection`` (``autocommit=False``)
    Owns the *business* transaction.  Every pipeline write and its checkpoint
    update happen in the same PostgreSQL transaction, so a crash can never
    leave business data committed without a matching position (or the other way
    round).

``metadata connection`` (``autocommit=True``)
    Writes ``mylox_schema_history``.  The original implementation wrote schema
    history through the business connection and called ``commit()`` - which
    silently committed half-finished business data whenever a DDL was
    interleaved with a running transaction.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ..config import PostgresSettings
from ..errors import StateStoreError
from ..gtid import GtidSet
from ..logging_utils import get_logger
from ..utils.positions import BinlogPosition
from .models import Checkpoint, SchemaHistoryRecord

logger = get_logger(__name__)

#: Name of the checkpoint table (one row per pipeline).
CHECKPOINT_TABLE = "mylox_checkpoint"
#: Name of the schema history table (one row per schema version).
SCHEMA_HISTORY_TABLE = "mylox_schema_history"

CHECKPOINT_DDL = """
CREATE TABLE IF NOT EXISTS mylox_checkpoint (
    pipeline VARCHAR(64) PRIMARY KEY,
    server_uuid VARCHAR(64),
    server_id BIGINT,
    log_file VARCHAR(128),
    log_pos BIGINT,
    gtid_set TEXT,
    is_initialized BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

SCHEMA_HISTORY_DDL = """
CREATE TABLE IF NOT EXISTS mylox_schema_history (
    id BIGSERIAL PRIMARY KEY,
    namespace VARCHAR(64),
    log_file VARCHAR(128),
    log_pos BIGINT,
    gtid_set TEXT,
    db_name VARCHAR(64),
    table_name VARCHAR(64),
    columns JSONB,
    primary_keys JSONB,
    source VARCHAR(32),
    ddl_text TEXT,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

#: Safety net for tables created by an older mylox that lacked a column;
#: ``ADD COLUMN IF NOT EXISTS`` makes it a no-op on a fresh database.
SCHEMA_HISTORY_MIGRATIONS = (
    "ALTER TABLE mylox_schema_history ADD COLUMN IF NOT EXISTS namespace VARCHAR(64)",
    "ALTER TABLE mylox_schema_history ADD COLUMN IF NOT EXISTS source VARCHAR(32)",
    "ALTER TABLE mylox_schema_history ADD COLUMN IF NOT EXISTS ddl_text TEXT",
    "ALTER TABLE mylox_checkpoint ALTER COLUMN log_file TYPE VARCHAR(128)",
    "ALTER TABLE mylox_schema_history ALTER COLUMN log_file TYPE VARCHAR(128)",
)

SCHEMA_HISTORY_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_mylox_schema_hist_lookup "
    "ON mylox_schema_history (namespace, db_name, table_name, id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_mylox_schema_hist_version "
    "ON mylox_schema_history (namespace, db_name, table_name, log_file, log_pos, source)",
)


class PgStateStore:
    """Durable state for the CDC engine."""

    def __init__(self, settings: PostgresSettings, namespace: str = "default") -> None:
        self.settings = settings
        self.namespace = namespace
        self._tx_conn: Optional[psycopg.Connection] = None
        self._meta_conn: Optional[psycopg.Connection] = None
        self._connect()
        self.ensure_tables()

    # -- connections ---------------------------------------------------
    @property
    def transaction_connection(self) -> psycopg.Connection:
        if self._tx_conn is None or self._tx_conn.closed:
            self._tx_conn = psycopg.connect(**self.settings.connect_kwargs(autocommit=False))
        return self._tx_conn

    @property
    def metadata_connection(self) -> psycopg.Connection:
        if self._meta_conn is None or self._meta_conn.closed:
            self._meta_conn = psycopg.connect(**self.settings.connect_kwargs(autocommit=True))
        return self._meta_conn

    def _connect(self) -> None:
        self._tx_conn = psycopg.connect(**self.settings.connect_kwargs(autocommit=False))
        self._meta_conn = psycopg.connect(**self.settings.connect_kwargs(autocommit=True))

    def reconnect(self) -> None:
        """Drop and rebuild both connections (used by the engine's retry loop)."""
        self.close()
        self._connect()

    def close(self) -> None:
        for conn in (self._tx_conn, self._meta_conn):
            try:
                if conn is not None and not conn.closed:
                    conn.close()
            except Exception:  # pragma: no cover - best effort
                pass
        self._tx_conn = None
        self._meta_conn = None

    # -- schema management --------------------------------------------
    def ensure_tables(self) -> None:
        try:
            with self.metadata_connection.cursor() as cur:
                cur.execute(CHECKPOINT_DDL)
                cur.execute(SCHEMA_HISTORY_DDL)
                for statement in SCHEMA_HISTORY_MIGRATIONS:
                    try:
                        cur.execute(statement)
                    except psycopg.Error as exc:  # pragma: no cover - old PG
                        logger.debug("migration statement skipped (%s): %s", exc, statement)
                for statement in SCHEMA_HISTORY_INDEXES:
                    cur.execute(statement)
            self._rename_legacy_checkpoint_column()
        except psycopg.Error as exc:
            raise StateStoreError("could not create the mylox state tables: %s" % (exc,)) from exc

    def _rename_legacy_checkpoint_column(self) -> None:
        """Rename ``cdc_pipeline`` to ``pipeline`` on tables created before the rename.

        ``ALTER TABLE ... RENAME COLUMN`` has no ``IF EXISTS`` form, so the
        column list is inspected first; the whole thing is a no-op on a fresh
        database and idempotent afterwards.
        """
        with self.metadata_connection.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = %s
                """,
                (CHECKPOINT_TABLE,),
            )
            columns = {row[0] for row in cur.fetchall()}
            if "cdc_pipeline" not in columns or "pipeline" in columns:
                return
            cur.execute(
                "ALTER TABLE %s RENAME COLUMN cdc_pipeline TO pipeline" % CHECKPOINT_TABLE
            )
        logger.warning(
            "renamed %s.cdc_pipeline to pipeline (one-off migration of a table created by an older mylox)",
            CHECKPOINT_TABLE,
        )

    # -- business transaction -----------------------------------------
    @contextmanager
    def cursor(self, row_factory: Any = None) -> Iterator[Any]:
        """Cursor on the business transaction connection."""
        conn = self.transaction_connection
        if row_factory is not None:
            with conn.cursor(row_factory=row_factory) as cur:
                yield cur
        else:
            with conn.cursor() as cur:
                yield cur

    def commit(self) -> None:
        self.transaction_connection.commit()

    def rollback(self) -> None:
        """Roll back, never raising: this runs on error paths and shutdown."""
        conn = self._tx_conn
        if conn is None or conn.closed:
            return
        try:
            conn.rollback()
        except Exception as exc:  # pragma: no cover - connection already gone
            logger.debug("rollback failed: %s", exc)

    def is_idle(self) -> bool:
        conn = self._tx_conn
        if conn is None or conn.closed:
            return True
        try:
            return conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        except Exception:  # pragma: no cover - connection already gone
            return True

    def rollback_if_needed(self) -> None:
        if not self.is_idle():
            self.rollback()

    # -- checkpoints ---------------------------------------------------
    def get_checkpoints(self, pipelines: Iterable[str]) -> Dict[str, Checkpoint]:
        names = sorted(set(pipelines))
        if not names:
            return {}
        with self.metadata_connection.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT pipeline, server_uuid, server_id, log_file, log_pos, gtid_set, is_initialized
                FROM mylox_checkpoint
                WHERE pipeline = ANY(%s)
                """,
                (names,),
            )
            return {row["pipeline"]: Checkpoint.from_row(row) for row in cur.fetchall()}

    def mark_initialized(
        self,
        pipelines: Iterable[str],
        position: Optional[BinlogPosition],
        gtid_set: GtidSet,
        server_uuid: Optional[str],
        server_id: Optional[int],
    ) -> None:
        rows = [
            (
                name,
                server_uuid,
                server_id,
                position.file if position else None,
                position.pos if position else None,
                str(gtid_set) if gtid_set else None,
            )
            for name in sorted(set(pipelines))
        ]
        if not rows:
            return
        with self.transaction_connection.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO mylox_checkpoint
                    (pipeline, server_uuid, server_id, log_file, log_pos, gtid_set, is_initialized, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                ON CONFLICT (pipeline) DO UPDATE SET
                    server_uuid = EXCLUDED.server_uuid,
                    server_id = EXCLUDED.server_id,
                    log_file = EXCLUDED.log_file,
                    log_pos = EXCLUDED.log_pos,
                    gtid_set = EXCLUDED.gtid_set,
                    is_initialized = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                """,
                rows,
            )
        self.transaction_connection.commit()

    def advance_checkpoints(
        self,
        updates: Sequence[Tuple[str, Optional[str], Optional[int], Optional[str]]],
        server_uuid: Optional[str],
        server_id: Optional[int],
    ) -> None:
        """Update checkpoint rows inside the *current* transaction.

        Two details matter:

        * Each pipeline carries its own executed GTID set, so the rows cannot
          share a single ``gtid_set`` value: writing the union would make a
          lagging pipeline skip transactions it never consumed.
        * It is an ``UPSERT``: a pipeline whose ``is_initialized`` write failed
          earlier still gets a durable checkpoint instead of silently never
          persisting one.
        """
        if not updates:
            return
        rows = [
            (pipeline, server_uuid, server_id, log_file, log_pos, gtid_set)
            for pipeline, log_file, log_pos, gtid_set in updates
        ]
        with self.transaction_connection.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO mylox_checkpoint
                    (pipeline, server_uuid, server_id, log_file, log_pos, gtid_set,
                     is_initialized, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                ON CONFLICT (pipeline) DO UPDATE SET
                    server_uuid = EXCLUDED.server_uuid,
                    server_id = EXCLUDED.server_id,
                    log_file = EXCLUDED.log_file,
                    log_pos = EXCLUDED.log_pos,
                    gtid_set = EXCLUDED.gtid_set,
                    updated_at = CURRENT_TIMESTAMP
                """,
                rows,
            )

    # -- schema history ------------------------------------------------
    def record_schema_versions(self, records: Sequence[SchemaHistoryRecord]) -> int:
        if not records:
            return 0
        rows = []
        for record in records:
            rows.append(
                (
                    record.namespace or self.namespace,
                    record.position.file if record.position else None,
                    record.position.pos if record.position else None,
                    record.gtid,
                    record.db_name,
                    record.table_name,
                    Jsonb([column.to_dict() for column in record.schema.columns]),
                    Jsonb(list(record.schema.primary_keys)),
                    record.source,
                    record.ddl,
                )
            )
        try:
            with self.metadata_connection.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO mylox_schema_history
                        (namespace, log_file, log_pos, gtid_set, db_name, table_name,
                         columns, primary_keys, source, ddl_text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (namespace, db_name, table_name, log_file, log_pos, source) DO NOTHING
                    """,
                    rows,
                )
                # executemany cumulates the affected rows, so `DO NOTHING`
                # entries are not counted.
                return max(cur.rowcount, 0)
        except psycopg.Error as exc:
            raise StateStoreError("could not write schema history: %s" % (exc,)) from exc

    def load_schema_history(
        self,
        db: str,
        table: str,
        before: Optional[BinlogPosition] = None,
        limit: int = 2000,
    ) -> List[SchemaHistoryRecord]:
        """History rows for one table, oldest first.

        Ordering by ``id`` (the insertion order) instead of ``log_file`` avoids
        the string-comparison bug of ``mysql-bin.999999`` vs
        ``mysql-bin.1000000``.
        """
        with self.metadata_connection.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, namespace, log_file, log_pos, gtid_set, db_name, table_name,
                       columns, primary_keys, source, ddl_text
                FROM mylox_schema_history
                WHERE namespace = %s AND db_name = %s AND table_name = %s
                ORDER BY id ASC
                LIMIT %s
                """,
                (self.namespace, db, table, limit),
            )
            records = [SchemaHistoryRecord.from_row(row, self.namespace) for row in cur.fetchall()]
        if before is not None:
            records = [record for record in records if record.position is None or record.position < before]
        return records

    def load_latest_schemas(self) -> Dict[Tuple[str, str], SchemaHistoryRecord]:
        """Newest history row per table (used to skip redundant bootstraps)."""
        with self.metadata_connection.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (db_name, table_name)
                       id, namespace, log_file, log_pos, gtid_set, db_name, table_name,
                       columns, primary_keys, source, ddl_text
                FROM mylox_schema_history
                WHERE namespace = %s
                ORDER BY db_name, table_name, id DESC
                """,
                (self.namespace,),
            )
            return {
                (row["db_name"], row["table_name"]): SchemaHistoryRecord.from_row(row, self.namespace)
                for row in cur.fetchall()
            }
