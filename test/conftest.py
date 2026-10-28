"""Shared pytest fixtures.

The integration tests need a **master/replica pair**, because that is what mylox
is built for and because a real replica is read-only:

* schema and DML are executed on the **master**;
* the engine streams the binlog of the **replica** and reads its
  ``information_schema``.

Environment::

    # replica (binlog source)
    MYLOX_TEST_MYSQL_HOST=192.168.13.61
    MYLOX_TEST_MYSQL_PORT=3307
    MYLOX_TEST_MYSQL_USER=mylox
    MYLOX_TEST_MYSQL_PASSWORD=...

    # optional: a *second* replica of the same master, for the failover tests
    MYLOX_TEST_MYSQL_SECOND_PORT=3308

    # master (write target); host/user/password default to the replica's
    MYLOX_TEST_MYSQL_MASTER_PORT=3306

    MYLOX_TEST_PG_HOST=192.168.13.61
    MYLOX_TEST_PG_PORT=5432
    MYLOX_TEST_PG_USER=mylox
    MYLOX_TEST_PG_PASSWORD=...
    MYLOX_TEST_PG_DATABASE=mylox

    # opt-in: exercises STOP/START REPLICA on the real replica
    MYLOX_TEST_ALLOW_REPLICA_CONTROL=1
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))


def _env(prefix: str, key: str, default=None):
    return os.environ.get("%s_%s" % (prefix, key), default)


_SLAVE = "MYLOX_TEST_MYSQL"
_MASTER = "MYLOX_TEST_MYSQL_MASTER"
_SECOND = "MYLOX_TEST_MYSQL_SECOND"


def mysql_slave_settings_from_env():
    """Settings of the **replica**: the binlog and metadata source."""
    from mylox import MySQLSettings

    if not _env(_SLAVE, "HOST"):
        return None
    return MySQLSettings(
        host=_env(_SLAVE, "HOST"),
        port=int(_env(_SLAVE, "PORT", 3307)),
        user=_env(_SLAVE, "USER", "root"),
        password=_env(_SLAVE, "PASSWORD", ""),
        charset=_env(_SLAVE, "CHARSET", "utf8mb4"),
    )


def mysql_master_settings_from_env():
    """Settings of the **master**: where the tests create schemas and DML."""
    from mylox import MySQLSettings

    host = _env(_MASTER, "HOST") or _env(_SLAVE, "HOST")
    if not host:
        return None
    return MySQLSettings(
        host=host,
        port=int(_env(_MASTER, "PORT", 3306)),
        user=_env(_MASTER, "USER") or _env(_SLAVE, "USER", "root"),
        password=_env(_MASTER, "PASSWORD") or _env(_SLAVE, "PASSWORD", ""),
        charset=_env(_MASTER, "CHARSET") or _env(_SLAVE, "CHARSET", "utf8mb4"),
    )


def mysql_second_settings_from_env():
    """Settings of a **second replica** of the same master (failover tests)."""
    from mylox import MySQLSettings

    if not (_env(_SECOND, "HOST") or _env(_SECOND, "PORT")):
        return None
    return MySQLSettings(
        host=_env(_SECOND, "HOST") or _env(_SLAVE, "HOST"),
        port=int(_env(_SECOND, "PORT", 3308)),
        user=_env(_SECOND, "USER") or _env(_SLAVE, "USER", "root"),
        password=_env(_SECOND, "PASSWORD") or _env(_SLAVE, "PASSWORD", ""),
        charset=_env(_SECOND, "CHARSET") or _env(_SLAVE, "CHARSET", "utf8mb4"),
    )


def pg_settings_from_env():
    from mylox import PostgresSettings

    if not _env("MYLOX_TEST_PG", "HOST"):
        return None
    return PostgresSettings(
        host=_env("MYLOX_TEST_PG", "HOST"),
        port=int(_env("MYLOX_TEST_PG", "PORT", 5432)),
        user=_env("MYLOX_TEST_PG", "USER", "postgres"),
        password=_env("MYLOX_TEST_PG", "PASSWORD", ""),
        dbname=_env("MYLOX_TEST_PG", "DATABASE", "postgres"),
    )


def replica_control_allowed() -> bool:
    return _env("MYLOX_TEST", "ALLOW_REPLICA_CONTROL", "0") not in ("0", "", "false", "False")


requires_mysql = pytest.mark.skipif(
    mysql_slave_settings_from_env() is None or mysql_master_settings_from_env() is None,
    reason="MYLOX_TEST_MYSQL_HOST is not set",
)
requires_pg = pytest.mark.skipif(
    pg_settings_from_env() is None, reason="MYLOX_TEST_PG_HOST is not set"
)


@pytest.fixture(scope="session")
def mysql_settings():
    """The **replica** connection (binlog + information_schema)."""
    settings = mysql_slave_settings_from_env()
    if settings is None:
        pytest.skip("MYLOX_TEST_MYSQL_HOST is not set")
    return settings


@pytest.fixture(scope="session")
def mysql_master_settings():
    """The **master** connection (schema + DML under test)."""
    settings = mysql_master_settings_from_env()
    if settings is None:
        pytest.skip("MYLOX_TEST_MYSQL_HOST is not set")
    return settings


@pytest.fixture(scope="session")
def mysql_second_settings():
    """The second replica, when one is configured."""
    settings = mysql_second_settings_from_env()
    if settings is None:
        pytest.skip("MYLOX_TEST_MYSQL_SECOND_PORT is not set")
    return settings


@pytest.fixture(scope="session")
def pg_settings():
    settings = pg_settings_from_env()
    if settings is None:
        pytest.skip("MYLOX_TEST_PG_HOST is not set")
    return settings
