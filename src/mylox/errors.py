"""Exception hierarchy for mylox."""

from __future__ import annotations


class MyloxError(Exception):
    """Base class for every error raised by mylox."""


class ConfigurationError(MyloxError, ValueError):
    """Raised when engine configuration is missing or inconsistent."""


class SchemaNotFound(MyloxError, LookupError):
    """Raised when no schema version is known for a table/position pair."""


class IncompleteDDL(MyloxError):
    """Raised when a DDL statement could not be applied to the schema model.

    The caller is expected to fall back to querying ``information_schema``.
    """


class StateStoreError(MyloxError):
    """Raised when the PostgreSQL state store cannot be read or written."""


class ReaderError(MyloxError):
    """Raised when the binlog reader cannot be created or started."""


class FatalEngineError(MyloxError):
    """Raised for conditions that must never be retried (e.g. GTID mismatch)."""
