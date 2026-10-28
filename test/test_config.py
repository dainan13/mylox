"""Configuration coercion and validation."""

from __future__ import annotations

import pytest

from mylox.config import (
    EngineConfig,
    MySQLSettings,
    PostgresSettings,
    coerce_mysql_settings,
    coerce_postgres_settings,
    env_settings,
)
from mylox.errors import ConfigurationError
from mylox.filtering import FilterMode


def test_mysql_settings_from_dict_accepts_legacy_keys_and_coerces_ints():
    settings = MySQLSettings.from_dict(
        {"host": "h", "user": "u", "passwd": "p", "port": "3307", "connect_timeout": "3", "read_timeout": 30}
    )
    assert settings.password == "p"
    assert settings.port == 3307
    assert settings.connect_timeout == 3
    assert settings.extra == {"read_timeout": 30}
    with pytest.raises(ConfigurationError):
        MySQLSettings.from_dict({"host": "h", "user": "u", "port": "nope"})


def test_mysql_settings_reject_empty_credentials():
    with pytest.raises(ConfigurationError):
        MySQLSettings(host="", user="u")
    with pytest.raises(ConfigurationError):
        MySQLSettings(host="h", user="")


def test_mysql_connection_kwargs_flavours():
    settings = MySQLSettings(host="h", user="u", password="p", database="meta")
    base = settings.stream_settings()
    assert "cursorclass" not in base and base["db"] == "meta"
    sentinel = object()
    assert settings.cursor_kwargs(sentinel)["cursorclass"] is sentinel
    assert settings.cursor_kwargs()["autocommit"] is True
    # BinLogStreamReader mutates the mapping it is given
    base["charset"] = "latin1"
    assert settings.stream_settings()["charset"] == "utf8mb4"


def test_postgres_settings_from_dict_keeps_extra():
    settings = PostgresSettings.from_dict(
        {"dbname": "dw", "user": "u", "port": "5433", "sslmode": "require"}
    )
    assert settings.port == 5433
    assert settings.extra == {"sslmode": "require"}
    assert settings.connect_kwargs(autocommit=True)["autocommit"] is True


def test_coercers():
    assert isinstance(coerce_mysql_settings({"host": "h", "user": "u"}), MySQLSettings)
    assert isinstance(coerce_postgres_settings({"dbname": "d", "user": "u"}), PostgresSettings)
    existing = MySQLSettings(host="h", user="u")
    assert coerce_mysql_settings(existing) is existing
    with pytest.raises(ConfigurationError):
        coerce_postgres_settings(42)


def test_engine_config_resolves_namespace_and_server_id():
    pg = PostgresSettings(dbname="dw", user="u")
    assert EngineConfig().resolve_namespace(pg) == "dw"
    assert EngineConfig(namespace="custom").resolve_namespace(pg) == "custom"
    assert 1 <= EngineConfig().resolve_server_id() <= 2**32 - 1
    assert EngineConfig(server_id=42).resolve_server_id() == 42


def test_engine_config_validation():
    with pytest.raises(ConfigurationError):
        EngineConfig(filter_mode="nonsense")
    with pytest.raises(ConfigurationError):
        EngineConfig(on_pipeline_error="explode")
    with pytest.raises(ConfigurationError):
        EngineConfig(server_id=0)
    with pytest.raises(ConfigurationError):
        EngineConfig(slave_heartbeat_seconds=-1)
    assert EngineConfig(filter_mode=FilterMode.INCLUDE).filter_mode == "include"


def test_env_settings(monkeypatch):
    monkeypatch.setenv("MYLOX_MYSQL_HOST", "replica")
    monkeypatch.setenv("MYLOX_MYSQL_USER", "cdc")
    monkeypatch.setenv("OTHER_HOST", "ignored")
    mapping = env_settings("MYLOX_MYSQL")
    assert mapping == {"host": "replica", "user": "cdc"}
