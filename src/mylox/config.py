"""Typed connection settings and engine configuration.

Plain dictionaries keep working (``MySQLSettings.from_dict``), but dataclasses
make defaults, validation and the differences between the three connection
flavours (control cursor, streaming cursor, binlog stream) explicit.
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional

from .constants import (
    DEFAULT_RECONNECT_INITIAL_BACKOFF,
    DEFAULT_RECONNECT_MAX_BACKOFF,
    DEFAULT_SERVER_ID_MAX,
    DEFAULT_SERVER_ID_MIN,
    DEFAULT_SLAVE_HEARTBEAT_SECONDS,
)
from .errors import ConfigurationError
from .filtering import FilterMode

# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

#: Keys that mylox manages itself and therefore strips from ``extra``.
_RESERVED_MYSQL_KEYS = {
    "host",
    "port",
    "user",
    "password",
    "passwd",
    "charset",
    "connect_timeout",
    "cursorclass",
    "db",
    "database",
}


@dataclass
class MySQLSettings:
    """Connection settings for the MySQL **replica** (binlog + metadata)."""

    host: str
    user: str
    password: str = ""
    port: int = 3306
    charset: str = "utf8mb4"
    connect_timeout: int = 10
    database: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.host:
            raise ConfigurationError("MySQL host must not be empty")
        if not self.user:
            raise ConfigurationError("MySQL user must not be empty")
        self.extra = {
            key: value for key, value in (self.extra or {}).items() if key not in _RESERVED_MYSQL_KEYS
        }

    @classmethod
    def from_dict(cls, settings: Mapping[str, Any]) -> "MySQLSettings":
        known = {
            "host",
            "user",
            "password",
            "passwd",
            "port",
            "charset",
            "connect_timeout",
            "database",
            "db",
            "extra",
        }
        data = dict(settings)
        if "passwd" in data and "password" not in data:
            data["password"] = data["passwd"]
        if "db" in data and "database" not in data:
            data["database"] = data["db"]
        extra = dict(data.pop("extra", {}) or {})
        kwargs = {key: value for key, value in data.items() if key in known}
        for key, value in data.items():
            if key not in known:
                extra[key] = value
        kwargs.pop("passwd", None)
        kwargs.pop("db", None)
        kwargs["extra"] = extra
        for key in ("port", "connect_timeout"):
            if kwargs.get(key) is not None:
                kwargs[key] = _as_int(key, kwargs[key])
        return cls(**kwargs)

    def _base_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "charset": self.charset,
            "connect_timeout": self.connect_timeout,
        }
        if self.database:
            kwargs["db"] = self.database
        kwargs.update(self.extra)
        return kwargs

    def cursor_kwargs(self, cursor_class: Any = None, autocommit: bool = True) -> Dict[str, Any]:
        """Keyword arguments for a short-lived, dict-cursor connection.

        ``autocommit=True`` matters: metadata queries and
        ``STOP/START REPLICA`` must never sit inside an open transaction that is
        rolled back when the connection closes.
        """
        kwargs = self._base_kwargs()
        kwargs["autocommit"] = autocommit
        if cursor_class is not None:
            kwargs["cursorclass"] = cursor_class
        return kwargs

    def stream_settings(self) -> Dict[str, Any]:
        """Keyword arguments for :class:`BinLogStreamReader`.

        A fresh copy is returned on every call because
        ``BinLogStreamReader`` mutates the mapping it is given.
        """
        return self._base_kwargs()

    def as_dict(self) -> Dict[str, Any]:
        return self._base_kwargs()


@dataclass
class PostgresSettings:
    """Connection settings for the PostgreSQL state/business database."""

    dbname: str
    user: str
    password: str = ""
    host: str = "127.0.0.1"
    port: int = 5432
    connect_timeout: int = 10
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.dbname:
            raise ConfigurationError("PostgreSQL dbname must not be empty")
        if not self.user:
            raise ConfigurationError("PostgreSQL user must not be empty")
        self.extra = {
            key: value
            for key, value in (self.extra or {}).items()
            if key not in {"dbname", "user", "password", "host", "port", "connect_timeout"}
        }

    @classmethod
    def from_dict(cls, settings: Mapping[str, Any]) -> "PostgresSettings":
        known = {
            "dbname",
            "user",
            "password",
            "host",
            "port",
            "connect_timeout",
            "extra",
        }
        data = dict(settings)
        extra = dict(data.pop("extra", {}) or {})
        kwargs = {key: value for key, value in data.items() if key in known}
        for key, value in data.items():
            if key not in known:
                extra[key] = value
        kwargs["extra"] = extra
        for key in ("port", "connect_timeout"):
            if kwargs.get(key) is not None:
                kwargs[key] = _as_int(key, kwargs[key])
        return cls(**kwargs)

    def connect_kwargs(self, autocommit: bool = False) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "connect_timeout": self.connect_timeout,
            "autocommit": autocommit,
        }
        kwargs.update(self.extra)
        return kwargs


def _as_int(name: str, value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("%s must be an integer, got %r" % (name, value)) from exc


def coerce_mysql_settings(value: object) -> MySQLSettings:
    if isinstance(value, MySQLSettings):
        return value
    if isinstance(value, Mapping):
        return MySQLSettings.from_dict(value)
    raise ConfigurationError("expected MySQLSettings or a mapping, got %r" % (type(value),))


def coerce_postgres_settings(value: object) -> PostgresSettings:
    if isinstance(value, PostgresSettings):
        return value
    if isinstance(value, Mapping):
        return PostgresSettings.from_dict(value)
    raise ConfigurationError("expected PostgresSettings or a mapping, got %r" % (type(value),))


# ---------------------------------------------------------------------------
# Engine configuration
# ---------------------------------------------------------------------------

ON_PIPELINE_ERROR_POLICIES = ("drop", "raise")
ON_STATEMENT_DML_POLICIES = ("fail", "warn", "ignore")
ON_SCHEMA_ERROR_POLICIES = ("drop", "skip")


@dataclass
class EngineConfig:
    """Runtime knobs of :class:`mylox.CDCEngine`."""

    #: Namespace stored alongside schema history.  Defaults to the PostgreSQL
    #: database name, matching the old "shadow namespace" isolation.
    namespace: Optional[str] = None

    #: Table routing rules (see :class:`mylox.TableFilter`).
    #:
    #: With the default ``gitignore`` mode a plain pattern **excludes** and a
    #: leading ``!`` re-includes, so "only the ``shop`` schema" is
    #: ``["*", "!shop.*"]``.  ``filter_mode="include"`` flips it: a plain
    #: pattern includes, ``!`` excludes (``["shop.*", "!shop.tmp_*"]``).
    filter_rules: List[str] = field(default_factory=list)
    filter_mode: str = FilterMode.GITIGNORE.value

    #: Replication client id.  ``None`` means "pick a random free id", which
    #: avoids two CDC instances evicting each other (the old code hardcoded
    #: ``99999``).
    server_id: Optional[int] = None

    #: Heartbeat interval; keeps the socket busy so Ctrl+C is honoured even
    #: when no business write happens.
    slave_heartbeat_seconds: float = DEFAULT_SLAVE_HEARTBEAT_SECONDS

    #: ``STOP REPLICA`` the source before taking the initialization anchor.
    pause_slave_on_init: bool = True
    #: When the pause fails, abort instead of continuing with an unreliable
    #: anchor.
    strict_pause: bool = False

    #: What to do when a handler fails for one pipeline: drop that pipeline and
    #: keep serving the others, or abort the process.
    on_pipeline_error: str = "drop"

    #: What to do when a row event cannot be decoded because no trustworthy
    #: schema is available for its position (missing history, unresolved
    #: drift):
    #:
    #: ``drop`` - park the affected pipelines (their checkpoint stops moving, so
    #:            nothing is silently lost and they can catch up after the
    #:            cause is fixed) and keep serving the others
    #: ``skip`` - log an error, block the transaction's checkpoint advance and
    #:            carry on; the row is effectively lost once another transaction
    #:            moves the checkpoint past it
    on_schema_error: str = "drop"

    #: Reconnect the binlog reader after transient failures.
    reconnect: bool = True
    reconnect_initial_backoff: float = DEFAULT_RECONNECT_INITIAL_BACKOFF
    reconnect_max_backoff: float = DEFAULT_RECONNECT_MAX_BACKOFF
    reconnect_max_attempts: Optional[int] = None

    #: Fall back to ``information_schema`` when a DDL cannot be interpreted.
    schema_fallback_enabled: bool = True

    #: Pull the initial schema snapshot at startup and record it in
    #: ``mylox_schema_history``.
    bootstrap_schema: bool = True

    #: Record a schema history row for every DDL (both paths).  Disabling it is
    #: only useful for tests.
    record_schema_history: bool = True

    #: What to do when a ``QueryEvent`` carries DML, which means the source is
    #: logging at least some statements in statement format.  mylox only
    #: understands row events, so those rows would be lost silently:
    #:
    #: ``fail``   - raise immediately (default: silent data loss is worse)
    #: ``warn``   - log a warning once and keep going
    #: ``ignore`` - say nothing
    on_statement_dml: str = "fail"

    #: Ask the binlog reader to swallow charset decode errors instead of
    #: raising them.  Without it a single BLOB/BINARY value holding non-UTF-8
    #: bytes makes the driver raise while decoding a row, and the engine can
    #: never move past that event (it reconnects and fails again forever).
    #:
    #: The trade-off is fidelity: the driver then decodes with
    #: ``errors="ignore"``, so binary columns reach the handler as *lossy text*.
    #: mylox warns at bootstrap for every table that has binary columns;
    #: set this to ``False`` if you prefer a hard failure over a lossy value.
    ignore_decode_errors: bool = True

    #: Extra keyword arguments forwarded to ``BinLogStreamReader``.
    stream_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.filter_mode not in {mode.value for mode in FilterMode}:
            raise ConfigurationError(
                "filter_mode must be one of %s" % (sorted(mode.value for mode in FilterMode),)
            )
        if self.on_statement_dml not in ON_STATEMENT_DML_POLICIES:
            raise ConfigurationError(
                "on_statement_dml must be one of %s" % (list(ON_STATEMENT_DML_POLICIES),)
            )
        if self.on_schema_error not in ON_SCHEMA_ERROR_POLICIES:
            raise ConfigurationError(
                "on_schema_error must be one of %s" % (list(ON_SCHEMA_ERROR_POLICIES),)
            )
        if self.on_pipeline_error not in ON_PIPELINE_ERROR_POLICIES:
            raise ConfigurationError(
                "on_pipeline_error must be one of %s" % (list(ON_PIPELINE_ERROR_POLICIES),)
            )
        if self.server_id is not None and not (
            DEFAULT_SERVER_ID_MIN <= int(self.server_id) <= DEFAULT_SERVER_ID_MAX
        ):
            raise ConfigurationError(
                "server_id must be within [%d, %d]" % (DEFAULT_SERVER_ID_MIN, DEFAULT_SERVER_ID_MAX)
            )
        if self.slave_heartbeat_seconds is not None and self.slave_heartbeat_seconds < 0:
            raise ConfigurationError("slave_heartbeat_seconds must be >= 0")

    # -- helpers ------------------------------------------------------
    def resolve_namespace(self, pg_settings: PostgresSettings) -> str:
        return self.namespace or pg_settings.dbname or "default"

    def resolve_server_id(self) -> int:
        if self.server_id:
            return int(self.server_id)
        return random.randint(DEFAULT_SERVER_ID_MIN, DEFAULT_SERVER_ID_MAX)

def env_settings(prefix: str) -> Dict[str, Any]:
    """Read ``<PREFIX>_*`` environment variables into a settings mapping.

    Used by the integration tests and handy for container runs::

        MYLOX_MYSQL_HOST=... MYLOX_MYSQL_USER=... mylox-run
    """
    mapping: Dict[str, Any] = {}
    for key, value in os.environ.items():
        if not key.startswith(prefix + "_"):
            continue
        name = key[len(prefix) + 1 :].lower()
        mapping[name] = value
    return mapping
