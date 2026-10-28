"""mylox - MySQL (GTID) to PostgreSQL change data capture.

Guarantees:

* **no shadow tables** - the schema is obtained from a hybrid of
  ``information_schema`` snapshots and local sqlglot DDL interpretation;
* a **multi-pipeline engine** with per-pipeline checkpoints, isolation and
  catch-up;
* atomic checkpoint + business writes in one PostgreSQL transaction.

Minimal usage::

    from mylox import CDCEngine, CDCHandler, EngineConfig, MySQLSettings, PostgresSettings

    class Handler(CDCHandler):
        def handle_row(self, change, pipeline, cur):
            cur.execute("INSERT INTO mirror VALUES (%s, %s)", (change.full_name, pipeline))

    engine = CDCEngine(
        MySQLSettings(host="replica", user="cdc", password="..."),
        PostgresSettings(dbname="dw", user="cdc", password="..."),
        pipelines=["orders", "inventory"],
        handler=Handler(),
        config=EngineConfig(filter_rules=["*", "!mysql.*"]),
    )
    engine.run()
"""

from __future__ import annotations

from .config import (
    EngineConfig,
    MySQLSettings,
    PostgresSettings,
    coerce_mysql_settings,
    coerce_postgres_settings,
    env_settings,
)
from .constants import (
    SOURCE_BOOTSTRAP,
    SOURCE_DDL_FALLBACK,
    SOURCE_DDL_FALLBACK_APPROX,
    SOURCE_DDL_SQLGLOT,
)
from .errors import (
    ConfigurationError,
    FatalEngineError,
    IncompleteDDL,
    MyloxError,
    ReaderError,
    SchemaNotFound,
    StateStoreError,
)
from .filtering import FilterMode, TableFilter
from .gtid import GtidSet, intersection, union
from .logging_utils import configure_logging, get_logger
from .utils.positions import BinlogPosition, is_pos_greater_or_equal
from .utils.serialize import stringify_bytes

# Subpackages are imported last: they depend on the modules above.
from .engine import (  # noqa: E402  isort:skip
    CDCEngine,
    CDCHandler,
    ChangeAction,
    InitContext,
    PipelineSet,
    PipelineState,
    PrintHandler,
    RowChange,
)
from .schema import (  # noqa: E402  isort:skip
    DDLKind,
    DDLReport,
    InformationSchemaLoader,
    SchemaManager,
    SchemaRegistry,
    SqlglotDDLParser,
    TableSchema,
    ColumnSchema,
)
from .state import Checkpoint, PgStateStore, SchemaHistoryRecord  # noqa: E402  isort:skip

def _resolve_version() -> str:
    try:  # the installed distribution is the single source of truth
        from importlib.metadata import version

        return version("mylox")
    except Exception:  # pragma: no cover - running from a bare source checkout
        return "0.8.0"


__version__ = _resolve_version()

__all__ = [
    "BinlogPosition",
    "CDCEngine",
    "CDCHandler",
    "ChangeAction",
    "Checkpoint",
    "ColumnSchema",
    "ConfigurationError",
    "DDLKind",
    "DDLReport",
    "EngineConfig",
    "FatalEngineError",
    "FilterMode",
    "GtidSet",
    "IncompleteDDL",
    "InformationSchemaLoader",
    "InitContext",
    "MySQLSettings",
    "MyloxError",
    "PgStateStore",
    "PipelineSet",
    "PipelineState",
    "PostgresSettings",
    "PrintHandler",
    "ReaderError",
    "RowChange",
    "SOURCE_BOOTSTRAP",
    "SOURCE_DDL_FALLBACK",
    "SOURCE_DDL_FALLBACK_APPROX",
    "SOURCE_DDL_SQLGLOT",
    "SchemaHistoryRecord",
    "SchemaManager",
    "SchemaNotFound",
    "SchemaRegistry",
    "SqlglotDDLParser",
    "StateStoreError",
    "TableFilter",
    "TableSchema",
    "__version__",
    "coerce_mysql_settings",
    "coerce_postgres_settings",
    "env_settings",
    "configure_logging",
    "get_logger",
    "intersection",
    "is_pos_greater_or_equal",
    "stringify_bytes",
    "union",
]
