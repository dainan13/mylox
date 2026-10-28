"""Shared constants for the mylox CDC library."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# mylox_schema_history.source values
# ---------------------------------------------------------------------------
SOURCE_BOOTSTRAP = "bootstrap"
"""Initial schema snapshot pulled from the replica's ``information_schema``."""

SOURCE_DDL_SQLGLOT = "ddl_sqlglot"
"""Schema evolved locally by parsing the DDL with sqlglot."""

SOURCE_DDL_FALLBACK = "ddl_fallback"
"""Schema re-read from the replica's ``information_schema`` after a DDL that
sqlglot could not interpret, or whose interpretation looked incomplete."""

SOURCE_DDL_FALLBACK_APPROX = "ddl_fallback_approx"
"""Same as :data:`SOURCE_DDL_FALLBACK`, but the ``information_schema`` snapshot
was taken *after* the DDL position (i.e. while replaying history), so the
result is an approximation."""

SOURCE_HISTORY = "history"
"""A version replayed from ``mylox_schema_history``."""

# ---------------------------------------------------------------------------
# Statement handling
# ---------------------------------------------------------------------------
#: Leading keywords that make a ``QueryEvent`` interesting for schema tracking.
DDL_KEYWORDS = (
    "CREATE",
    "ALTER",
    "DROP",
    "RENAME",
    "TRUNCATE",
    "COMMENT",
)

#: Statements that never change a table's column layout and can be skipped.
NOOP_STATEMENT_PREFIXES = (
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE SAVEPOINT",
    "ROLLBACK TO SAVEPOINT",
    "XA ",
    "SET ",
    "USE ",
    "GRANT",
    "REVOKE",
    "FLUSH",
    "ANALYZE",
    "OPTIMIZE",
    "REPAIR",
    "CHECK ",
    "KILL",
    "SHOW ",
    "DESCRIBE",
    "EXPLAIN",
)

#: MySQL schemas that never carry business tables.
SYSTEM_SCHEMAS = (
    "information_schema",
    "performance_schema",
    "mysql",
    "sys",
)

# ---------------------------------------------------------------------------
# Engine defaults
# ---------------------------------------------------------------------------
DEFAULT_SLAVE_HEARTBEAT_SECONDS = 1.0
DEFAULT_RECONNECT_INITIAL_BACKOFF = 1.0
DEFAULT_RECONNECT_MAX_BACKOFF = 30.0
DEFAULT_SERVER_ID_MIN = 1
DEFAULT_SERVER_ID_MAX = 2**32 - 1

#: Savepoint name template used to isolate a single pipeline inside the shared
#: PostgreSQL transaction.
SAVEPOINT_TEMPLATE = "mylox_sp_{index}"
