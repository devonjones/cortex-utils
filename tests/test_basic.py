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

    # Assert on the DESTINATION, not on the type of the sink. Checking the
    # class name passes if _LazyStderr.write is changed to write to stdout --
    # verified: that mutant leaves the whole suite green while the real CLI goes
    # back to interleaving log lines into `queue stats` output.
    probe = (
        "import structlog\n"
        "from click.testing import CliRunner\n"
        "from cortex_utils.cli import main\n"
        "print('before:', structlog.is_configured())\n"
        "CliRunner().invoke(main, ['queue', 'stats'])\n"
        "print('after:', structlog.is_configured())\n"
        "from cortex_utils.log import get_logger\n"
        "get_logger().warning('a-log-line-that-must-not-be-on-stdout')\n"
    )
    out2, err2 = _run(probe)
    assert "before: False" in out2, out2
    assert "after: True" in out2, out2
    assert "a-log-line-that-must-not-be-on-stdout" not in out2, (
        f"the CLI put a log line on stdout, where its own output goes: {out2!r}"
    )
    assert "a-log-line-that-must-not-be-on-stdout" in err2, err2


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


def _run(code: str) -> tuple[str, str]:
    """A fresh interpreter: structlog config is process-global, so anything
    this test process already imported would mask the answer."""
    import subprocess
    import sys

    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    return r.stdout, r.stderr


def test_library_logs_never_reach_an_unconfigured_consumers_stdout() -> None:
    """The bug a downstream consumer reported and we reproduced: their
    claim-drain stdout is TSV, one claimed row per line, and a library log line
    was parsed as a claimed row.

    structlog's own default is stdout. That is fine for an application and
    wrong for a library -- the corruption reads as data rather than as an error.
    """
    out, err = _run(
        "from cortex_utils.log import get_logger\n"
        "get_logger().info('library message')\n"
        "print('ROW\\tone\\ttwo')\n"
    )
    assert out == "ROW\tone\ttwo\n", f"library log leaked into the protocol: {out!r}"
    assert "library message" in err


def test_a_configured_consumer_keeps_their_own_configuration() -> None:
    """We do not call structlog.configure() -- that is global state, and a
    library deciding how its consumer logs is the same mistake one layer up.
    The stderr default applies only when they have not chosen."""
    out, _err = _run(
        "import io, structlog\n"
        "sink = io.StringIO()\n"
        "structlog.configure(processors=[structlog.processors.JSONRenderer()],\n"
        "                    logger_factory=structlog.PrintLoggerFactory(file=sink))\n"
        "from cortex_utils.log import get_logger\n"
        "get_logger().info('library message', k=1)\n"
        "print('SINK:' + sink.getvalue().strip())\n"
    )
    assert '"event": "library message"' in out, out
    assert out.startswith("SINK:{"), f"their renderer was not used: {out!r}"


def test_the_choice_is_made_at_log_time_not_import_time() -> None:
    """Modules bind their logger at import, which is before a consumer has had
    the chance to configure. Deciding then would lock in the wrong answer for
    every consumer that configures after importing us -- which is all of them."""
    out, _err = _run(
        "import io, structlog\n"
        "from cortex_utils.log import get_logger\n"
        "log = get_logger()                      # bound BEFORE configuring\n"
        "log.info('early')                       # and USED before, which is\n"
        "                                        # what makes memoising visible\n"
        "sink = io.StringIO()\n"
        "structlog.configure(processors=[structlog.processors.JSONRenderer()],\n"
        "                    logger_factory=structlog.PrintLoggerFactory(file=sink))\n"
        "log.info('late', k=1)\n"
        "print('SINK:' + sink.getvalue().strip())\n"
    )
    assert '"event": "late"' in out, f"import-time binding ignored later config: {out!r}"


def test_library_logs_carry_their_own_namespace_not_root() -> None:
    """structlog forwards *args to the logger factory, so get_logger(None)
    hands stdlib.LoggerFactory a non-empty args whose [0] is None ->
    logging.getLogger(None) -> the ROOT logger.

    Consequence: every module logs as "root", and
    logging.getLogger("cortex_utils").setLevel(...) stops silencing us. That is
    this library reaching past the consumer's configuration -- the same fault
    as logging to their stdout, which is what this module exists to fix.
    """
    out, _err = _run(
        "import json, logging, sys\n"
        "from cortex_utils.logging import configure_logging\n"
        "configure_logging('svc')\n"
        "import cortex_utils.queue.ops as ops\n"
        "ops.log.info('probe')\n"
    )
    # configure_logging renders JSON through stdlib to stderr; capture either.
    combined = (
        out
        + _run(
            "import logging\n"
            "from cortex_utils.logging import configure_logging\n"
            "configure_logging('svc')\n"
            "logging.getLogger('cortex_utils').setLevel(logging.CRITICAL)\n"
            "import cortex_utils.queue.ops as ops\n"
            "ops.log.info('should-be-silenced')\n"
        )[1]
    )
    assert "should-be-silenced" not in combined, (
        "silencing the cortex_utils namespace did not silence us -- we are logging as root"
    )


def test_the_log_stream_is_resolved_per_write_not_at_import() -> None:
    """_LazyStderr exists because binding sys.stderr when the logger is built
    leaves a harness that replaces the stream holding a closed file -- it broke
    121 tests the first time. Asserting only that logs reach stderr does not
    show that: an import-time binding passes too, right up until something
    redirects."""
    import contextlib
    import io

    from cortex_utils.log import get_logger

    log = get_logger()
    captured = io.StringIO()
    with contextlib.redirect_stderr(captured):
        log.warning("redirected-marker")

    assert "redirected-marker" in captured.getvalue(), (
        "the stream was captured when the logger was built, not resolved per write"
    )


def test_the_conftest_fixture_restores_a_configuration_it_found() -> None:
    """conftest's fixture has two branches and this suite only reaches one.

    Nothing here configures structlog at entry, so every test takes the
    reset_defaults() path and the restore path never runs. That asymmetry is
    how an untested branch ships, and it is not hypothetical: a consumer running
    this suite inside their own structlog setup would have it reset out from
    under them -- the library reaching past its consumer's configuration, which
    is the fault the whole log module exists to avoid.

    Driven directly rather than through capture_logs(), because capture_logs
    turns out to restore correctly when structlog WAS configured -- it only
    leaks from the unconfigured state. So no test using it can reach this
    branch, which is exactly why it needs driving by hand.
    """
    import io

    import structlog

    from tests.conftest import structlog_configuration_restored

    sink = io.StringIO()
    structlog.configure(
        processors=[structlog.processors.JSONRenderer()],
        logger_factory=structlog.PrintLoggerFactory(file=sink),
        cache_logger_on_first_use=False,
    )
    original = dict(structlog.get_config())
    try:
        with structlog_configuration_restored():
            # A test mutates it, the way any test touching structlog might.
            structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=io.StringIO()))
            assert structlog.get_config()["logger_factory"] is not original["logger_factory"]

        assert structlog.get_config()["logger_factory"] is original["logger_factory"], (
            "the caller's configuration was not restored -- their logging is "
            "now going somewhere they did not choose"
        )
        from cortex_utils.log import get_logger

        get_logger("probe").info("restored", k=1)
        assert "restored" in sink.getvalue()
    finally:
        structlog.reset_defaults()
