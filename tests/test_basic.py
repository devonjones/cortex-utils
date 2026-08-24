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


def test_importing_the_cli_does_not_configure_global_logging() -> None:
    """structlog.configure() is global. A library that calls it at import time
    decides how its consumer logs -- and this one routed to stdout, which is
    where the CLI's own output goes, so `queue stats` piped to a parser got log
    lines interleaved with the data.

    Reported by a downstream consumer who lost a day to the same shape.
    """
    import subprocess
    import sys

    # A fresh interpreter: structlog is process-global, so anything already
    # imported in this one would mask the answer.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import structlog, cortex_utils.cli; print(structlog.is_configured())",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "False", result.stdout


def test_running_the_cli_configures_logging_to_stderr() -> None:
    """Import must not configure it -- but running the command must, or the
    CLI has no logging at all, which the import test alone cannot tell apart.

    And to stderr: stdout is where the command's own output goes, so a log line
    there corrupts anything parsing it.
    """
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "cortex_utils.cli", "--help"],
        capture_output=True,
        text=True,
    )
    assert "Cortex operational utilities" in result.stdout

    # A real subcommand, not --help: click short-circuits --help before the
    # group callback body runs, so it would report "not configured" whether or
    # not the wiring works.
    probe = (
        "import structlog\n"
        "from click.testing import CliRunner\n"
        "from cortex_utils.cli import main\n"
        "print('before:', structlog.is_configured())\n"
        "CliRunner().invoke(main, ['queue', 'stats'])\n"
        "print('after:', structlog.is_configured())\n"
        "print('stream:', type(structlog.get_config()['logger_factory']._file).__name__)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    ).stdout
    assert "before: False" in out, out
    assert "after: True" in out, out
    assert "_LazyStderr" in out, f"logs must go to stderr, not the command's output: {out}"


def test_importing_the_queue_writes_nothing_to_stdout() -> None:
    """The rule a library owes a consumer whose stdout is a protocol."""
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c", "import cortex_utils.queue"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout == "", f"wrote to stdout on import: {result.stdout!r}"
