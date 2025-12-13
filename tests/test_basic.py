"""Basic tests for cortex-utils."""

import pytest

from cortex_utils import __version__
from cortex_utils.config import Config, PostgresConfig


def test_version() -> None:
    """Test that version is defined."""
    assert __version__ == "0.1.0"


def test_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test loading config from environment variables."""
    monkeypatch.setenv("POSTGRES_HOST", "testhost")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test")
    monkeypatch.setenv("QUEUE_RETENTION_DAYS", "14")

    config = Config.from_env()

    assert config.postgres.host == "testhost"
    assert config.postgres.port == 5433
    assert config.postgres.database == "testdb"
    assert config.postgres.user == "testuser"
    assert config.postgres.password == "testpass"
    assert config.discord_webhook_url == "https://discord.test"
    assert config.queue_retention_days == 14


def test_postgres_dsn() -> None:
    """Test PostgresConfig DSN generation."""
    pg = PostgresConfig(
        host="localhost",
        port=5432,
        database="cortex",
        user="cortex",
        password="secret",
    )

    assert "host=localhost" in pg.dsn
    assert "port=5432" in pg.dsn
    assert "dbname=cortex" in pg.dsn
    assert "user=cortex" in pg.dsn
    assert "password=secret" in pg.dsn
