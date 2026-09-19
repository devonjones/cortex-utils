"""The alerter's three defaults, all of which used to fail toward silence.

Population: every distinct string literal passed to log.error / log.critical /
log.exception across postmark, triage, utils, gateway, actions and reflex,
collected by AST walk on 2026-09-19 -- 67 messages, no length or content
filter. These are what the code CAN report, not what it has reported; see the
separate live figure below.

    containers   5 of 11 running cortex containers were unwatched, including
                 cortex-gateway, cortex-actions-router, cortex-postgres and
                 cortex-alerter itself
    patterns      3 of 67 matched a tuned pattern
    gate         15 of 67 were recognised as errors at all. The severity of a
                 cortex log line lives in a structlog "level" field, and the
                 indicator list read the message TEXT only -- so "Config reload
                 failed", "Cannot start without Docker connection" and 50 other
                 real failures were invisible before any pattern was consulted
    reachable     0. None of the 3 pattern-matchers was among the 15, and the
                 daemon gates on is_error_line() before it classifies, so
                 nothing in this population could reach the channel at all

An earlier revision of this docstring said "2 of 40 ... the other 38 were
correctly identified as errors by is_error_line() and then dropped for want of
a tuned pattern". The shape of that claim was wrong, not just the counts: most
of those messages were never identified as errors in the first place, so the
catch-all alone would have rescued 15 of 67 rather than all of them. Both
halves were needed.

That revision also said 66, from a walk that silently dropped literals under
ten characters and so excluded "Peer down". The stated method did not
reproduce the stated number, which is the more serious half of the error --
hence "no length or content filter" above. Re-derive rather than trust it.

The live figure looks like it disagrees and does not. Of the 8 error-level
lines Hades emitted in the 14 days to 2026-09-19 the text-only gate caught 8,
but six of those read "Pattern detection failed for <id>" and bare lowercase
"failed" is not an indicator -- they matched on "Traceback" in the serialised
exception payload. On the event text alone, 2 of 8 would have passed. The
coverage rested on those six carrying exc_info.

All three were allowlists. The property that matters is which way FORGETTING
fails: silently under an allowlist, noisily under a denylist.
"""

from __future__ import annotations

from unittest import mock

import pytest

from cortex_utils.alerter.classifier import (
    Severity,
    classify,
    is_client_sql_fault,
    is_error_line,
)
from cortex_utils.alerter.daemon import DENYLISTED_CONTAINERS, discover_containers


class TestContainerDiscovery:
    def test_a_new_cortex_service_is_watched_without_anyone_editing_a_list(self) -> None:
        """The whole point: adding a service must not silently add a blind spot."""
        running = ["cortex-gmail-sync", "cortex-brand-new-service", "unrelated-nginx"]
        assert "cortex-brand-new-service" in discover_containers(running)

    def test_non_cortex_containers_are_not_watched(self) -> None:
        assert discover_containers(["nginx", "postgres", "traefik"]) == []

    def test_the_alerter_does_not_watch_itself(self) -> None:
        """Self-observation is not a health check.

        A dead alerter reads nothing, including its own logs, so watching
        itself would imply coverage it cannot provide. Noticing its death
        needs an EXTERNAL check.
        """
        assert "cortex-alerter" not in discover_containers(["cortex-alerter", "cortex-gateway"])
        assert "cortex-alerter" in DENYLISTED_CONTAINERS

    def test_the_five_containers_that_were_unwatched_are_now_watched(self) -> None:
        """Regression guard on the measured gap, by name."""
        running = [
            "cortex-gateway",
            "cortex-actions-router",
            "cortex-postgres",
            "cortex-teach",
            "cortex-alerter",
        ]
        watched = discover_containers(running)
        for name in running:
            if name == "cortex-alerter":
                continue
            assert name in watched, f"{name} was unwatched before and still is"


class TestClassifierFallsThroughToNoise:
    # Real messages harvested from cortex service code; every one of these was
    # dropped silently before this change.
    @pytest.mark.parametrize(
        "message",
        [
            "ERROR Gmail batch modify failed: HTTP 429 - Too Many Requests",
            "ERROR Failed to create required label: Cortex/Uncategorized",
            "CRITICAL Missing required environment variables: POSTGRES_PASSWORD",
            "ERROR Failed to get Gmail labels for 18f2a: connection reset",
        ],
    )
    def test_an_unrecognised_error_still_alerts(self, message: str) -> None:
        assert is_error_line(message), "precondition: this looks like an error"
        result = classify("cortex-gateway", message)
        assert result is not None, "an error nobody anticipated is still an error"
        assert result.severity is Severity.WARNING

    def test_unclassified_errors_dedupe_per_message(self) -> None:
        """A service failing in a loop must be one alert, not thousands."""
        a = classify("cortex-gateway", "ERROR Failed to widget: boom")
        b = classify("cortex-gateway", "ERROR Failed to widget: boom")
        c = classify("cortex-gateway", "ERROR Something entirely different")
        assert a and b and c
        assert a.error_key == b.error_key
        assert a.error_key != c.error_key

    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            ("ERROR LLM error processing abc: timeout", Severity.HIGH),
            # Caught by a tuned pattern at CRITICAL, which is better than the
            # WARNING fallback -- this was in my first draft of the
            # unclassified list and the classifier was already right about it.
            ("ERROR Failed to get/create label Cortex/KS: HttpError 403", Severity.CRITICAL),
        ],
    )
    def test_a_tuned_pattern_still_wins(self, message: str, expected: Severity) -> None:
        """The specific patterns are enrichment now, not a gate -- but they win."""
        result = classify("cortex-gateway", message)
        assert result is not None
        assert result.severity is expected
        assert result.title != "Unclassified Error"


class TestPostgresClientFaultsAreNotOurErrors:
    """A database rejecting bad SQL is working correctly.

    Measured over 24h of real logs: 21 error lines, ALL from cortex-postgres,
    and 17 were failed ad-hoc queries typed by agents that evening. Alerting on
    those turns the notifications channel into a feed of our own typos.
    """

    @pytest.mark.parametrize(
        "message",
        [
            'ERROR:  column "nope_no_such_column" does not exist at character 8',
            "ERROR:  aggregate functions are not allowed in GROUP BY at character 72",
            'ERROR:  unterminated quoted identifier at or near """ at character 1',
            "ERROR:  ORDER BY position 2 is not in select list at character 40",
            "ERROR:  syntax error at or near SELECT",
        ],
    )
    def test_an_agent_typo_does_not_alert(self, message: str) -> None:
        assert is_client_sql_fault("cortex-postgres", message)
        assert classify("cortex-postgres", message) is None

    @pytest.mark.parametrize(
        "message",
        [
            "ERROR:  could not map dynamic shared memory segment",
            "FATAL:  the database system is in recovery mode",
            "PANIC:  could not write to file pg_wal/xlogtemp: No space left on device",
        ],
    )
    def test_real_server_trouble_still_alerts(self, message: str) -> None:
        assert not is_client_sql_fault("cortex-postgres", message)
        assert classify("cortex-postgres", message) is not None

    def test_the_filter_is_scoped_to_postgres(self) -> None:
        """The same words from an application are NOT a client fault."""
        msg = 'ERROR:  column "x" does not exist'
        assert not is_client_sql_fault("cortex-gateway", msg)
        assert classify("cortex-gateway", msg) is not None


class TestPatternsMustBePrecise:
    """A tuned pattern is NOT gated on is_error_line(), so it must be precise.

    The patterns run before the generic gate, because they encode domain
    knowledge it lacks -- "History expired for historyId 12345" is a real
    failure containing none of ERROR, Failed or Traceback. The price is that an
    over-broad pattern classifies lines that are not errors at all.

    One did. `\\b5\\d{2}\\b` matches ANY three-digit number from 500-599
    anywhere in a line, so routine Postgres bookkeeping MATCHED the API server
    error pattern 122 times in 24 hours. It never alerted -- cortex-postgres
    was unwatched and the daemon gates on is_error_line() first -- so the
    number counts matches, not pings. The same pattern reads a traceback frame
    at capture_worker.py:517 as an HTTP status: a source line number.
    """

    @pytest.mark.parametrize(
        "line",
        [
            "LOG:  checkpoint complete: wrote 571 buffers (3.5%); 0 WAL file(s) added",
            "LOG:  checkpoint starting: time, 530 buffers",
            'File "/app/capture_worker.py", line 517, in _process',
            "INFO Processed 512 emails in this batch",
        ],
    )
    def test_a_bare_500_series_integer_is_not_an_http_status(self, line: str) -> None:
        result = classify("cortex-postgres", line)
        assert result is None or result.title != "API Server Error", (
            f"a 5xx-looking integer was read as an HTTP status: {line!r}"
        )

    @pytest.mark.parametrize(
        "line",
        [
            "ERROR HttpError 503 Service Unavailable",
            "ERROR Gmail API returned HTTP 500",
            "ERROR request failed status_code=502",
            "ERROR 504 Gateway Timeout from upstream",
        ],
    )
    def test_a_real_http_5xx_still_alerts(self, line: str) -> None:
        result = classify("cortex-gmail-sync", line)
        assert result is not None
        assert result.title == "API Server Error", f"missed a real 5xx: {line!r}"


class TestCriticalPatternsDoNotMatchRoutineTraffic:
    """Two CRITICAL patterns matched healthy traffic, found by measurement.

    Both are the same defect as the 5xx one: a substring match where a token
    match was meant. Both are CRITICAL, and the history one has a ZERO
    cooldown, so every match pings the channel.

    Neither fired in production, but only because the daemon's is_error_line()
    gate drops INFO lines before classify() sees them. That is accidental
    protection, not design -- the patterns are reached directly by any other
    caller, and the gate is not what makes them correct.
    """

    @pytest.mark.parametrize(
        "line",
        [
            # A Gmail history ID is a plain integer that routinely CONTAINS
            # "404". 9 of these in 24h of healthy Pub/Sub traffic.
            '{"event": "Notification for a@b.com, historyId=79564045", "level": "info"}',
            '{"event": "Notification for a@b.com, historyId=40412345", "level": "info"}',
            '{"event": "sync complete", "historyId": 404999, "level": "info"}',
        ],
    )
    def test_a_history_id_containing_404_is_not_an_expiry(self, line: str) -> None:
        result = classify("cortex-gmail-sync", line)
        assert result is None or result.title != "Gmail History Expired", (
            f"routine notification read as history expiry: {line[:70]!r}"
        )

    @pytest.mark.parametrize(
        "line",
        [
            # Unanchored OOM matches inside ordinary words. This one is real:
            # a Gmail label called Cortex/Automated/Zoom.
            "add_label=['Cortex/Automated/Zoom'] remove_label=None",
            "INFO joining room 12345",
            "INFO bloom filter rebuilt",
        ],
    )
    def test_a_word_containing_oom_is_not_an_oom_kill(self, line: str) -> None:
        result = classify("cortex-triage-worker", line)
        assert result is None or result.title != "Out of Memory", (
            f"ordinary word read as an OOM kill: {line[:70]!r}"
        )

    @pytest.mark.parametrize(
        ("line", "title"),
        [
            ("CRITICAL History expired, full resync required", "Gmail History Expired"),
            (
                "ERROR HttpError 404 fetching history for devon: historyId too old",
                "Gmail History Expired",
            ),
            ("ERROR MemoryError: cannot allocate 4GiB", "Out of Memory"),
            ("ERROR container exited with exit code 137", "Out of Memory"),
        ],
    )
    def test_the_real_conditions_still_alert(self, line: str, title: str) -> None:
        result = classify("cortex-gmail-sync", line)
        assert result is not None, f"missed a real condition: {line!r}"
        assert result.title == title


class TestRegexesStillMatchTheRealCondition:
    """The re-anchoring must not have silenced the failures it was tuning for.

    Every anchoring fix in this PR narrows a pattern, and a narrowed pattern
    fails SILENTLY -- a false negative is an alert that never arrives, with
    nothing in any log to say so. The sibling class above proves these patterns
    no longer match routine traffic; on its own that is satisfied by a pattern
    that matches nothing at all.

    Each case below is the real failure the alternation exists for, so deleting
    that alternation fails a test rather than going quiet.
    """

    @pytest.mark.parametrize(
        "line,expect_title",
        [
            # The three new history alternations. The OLD pattern was
            # historyId.*404, which matched any history ID containing "404" as
            # a substring; these require 404 to be an HTTP status.
            (
                "HttpError 404 when requesting history for startHistoryId 123",
                "Gmail History Expired",
            ),
            (
                "historyId 79564045 rejected: HttpError 404 Not Found",
                "Gmail History Expired",
            ),
            (
                "startHistoryId 12345 returned 404",
                "Gmail History Expired",
            ),
            # \bOOM\b and \bOut of memory\b -- word-bounded, but still present.
            ("Container killed: OOM", "Out of Memory"),
            ("Out of memory: Killed process 1234 (python)", "Out of Memory"),
            # HttpError 5\d{2} keeps the HTTP context it always had.
            ("HttpError 503 Service Unavailable", "API Server Error"),
        ],
    )
    def test_the_real_failure_still_classifies(self, line, expect_title):
        result = classify("cortex-gmail-sync", line)
        assert result is not None, f"pattern went silent on a real failure: {line!r}"
        assert result.title == expect_title


class TestStructlogLevelIsAnErrorSignal:
    """Cortex services put the severity in a field, not in the message text.

    ``{"event": "Missing required environment variables: ...", "level":
    "error"}`` contains no word from _ERROR_INDICATORS, so a text-only gate
    dropped it -- the alerter could not see a service reporting it cannot
    start. Measured 2026-09-19 over 14 days of Hades logs: all 8 error-level
    lines were caught, but six of them only via "Traceback" in the serialised
    exception payload -- bare lowercase "failed" is not an indicator. On the
    event text alone, 2 of 8 would have passed.
    """

    def test_structlog_error_level_is_an_error(self):
        line = '{"event": "Missing required environment variables: PG_PASS", "level": "error"}'
        assert is_error_line(line)

    def test_structlog_critical_level_is_an_error(self):
        assert is_error_line('{"event":"Shutting down","level":"critical"}')

    def test_structlog_warning_level_is_not(self):
        line = '{"event": "DuckDB API error for x: HTTP 404", "level": "warning"}'
        assert not is_error_line(line)

    def test_structlog_info_level_is_not(self):
        assert not is_error_line('{"event": "Processed 10 emails", "level": "info"}')

    def test_the_startup_abort_now_reaches_the_catch_all(self):
        """The whole point: it is classified, not dropped."""
        line = '{"event": "Missing required environment variables: PG_PASS", "level": "error"}'
        result = classify("cortex-parse-worker", line)
        assert result is not None
        assert result.title == "Unclassified Error"


class TestDiscoveryIsWiredIn:
    """discover_containers() is pure and tested above; this is the wiring.

    The pure function can be perfect while the daemon never calls it. Each test
    here kills a mutant that survived the whole suite: both `return []`
    fallbacks rewritten to a stale hardcoded list, `if self._discover:` turned
    off, the "watching NOTHING" alarm deleted, and `not containers` replaced by
    True so an explicit -c list is silently discarded.
    """

    @staticmethod
    def _daemon(containers=None):
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            return AlerterDaemon("https://discord.test/webhook", containers=containers)

    def test_discovery_before_docker_connect_watches_nothing(self):
        """NOT a stale hardcoded list. An alerter that cannot see Docker says so."""
        d = self._daemon()
        d.docker_client = None
        assert d._discover_running_containers() == []

    def test_docker_failure_watches_nothing(self):
        d = self._daemon()
        d.docker_client = mock.Mock()
        d.docker_client.containers.list.side_effect = RuntimeError("socket gone")
        assert d._discover_running_containers() == []

    def test_discovery_returns_live_cortex_containers_minus_denylist(self):
        d = self._daemon()
        d.docker_client = mock.Mock()

        def named(n):  # Mock(name=...) names the mock, it does not set .name
            m = mock.Mock()
            m.name = n
            return m

        d.docker_client.containers.list.return_value = [
            named(n) for n in ["cortex-gateway", "cortex-alerter", "traefik", "cortex-teach"]
        ]
        assert d._discover_running_containers() == ["cortex-gateway", "cortex-teach"]

    def _run_once(self, daemon, discovered):
        """Drive run() to completion without booting the daemon."""
        daemon._stop_event.set()  # run() returns at the final wait()
        with (
            mock.patch.object(daemon, "_connect_docker", return_value=True),
            mock.patch.object(daemon, "_tail_container"),
            mock.patch.object(daemon, "_discover_running_containers", return_value=discovered),
            mock.patch("cortex_utils.alerter.daemon.schedule"),
        ):
            daemon.run()
        return daemon.containers

    def test_run_discovers_when_no_explicit_list(self):
        d = self._daemon()
        assert self._run_once(d, ["cortex-gateway"]) == ["cortex-gateway"]

    def test_an_explicit_container_list_is_not_discarded(self):
        """-c wins. Discovery is the DEFAULT, not an override."""
        d = self._daemon(containers=["cortex-only-this"])
        assert d._discover is False
        assert self._run_once(d, ["cortex-gateway"]) == ["cortex-only-this"]

    def test_watching_nothing_is_reported_loudly(self, capsys):
        """structlog renders to stderr, so caplog does not see this one."""
        d = self._daemon()
        self._run_once(d, [])
        assert "NOTHING" in capsys.readouterr().err
