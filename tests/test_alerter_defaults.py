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

import json
from unittest import mock

import docker
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


class TestTheDedupKeyCollapsesRealTraffic:
    """The catch-all's survivability rests entirely on this key collapsing.

    It did not. Hashing `log_line[:200]` hashed a NONCE: structlog renders a
    per-emit "timestamp" field that lands inside the window for any event under
    ~137 characters, and Postgres prefixes its own clock. Measured 2026-09-19
    over 7 days of all 11 cortex containers: 42 unclassified errors produced 42
    distinct keys -- a collapse rate of zero. With normalisation, 25.

    The old test guarded only OVER-collapse. Under-collapse -- the live
    behaviour -- was unasserted, because its fixture fed a string with no
    timestamp, which production never emits.
    """

    @staticmethod
    def _line(event, ts="2026-09-19T01:00:00.111111Z"):
        return json.dumps(
            {
                "event": event,
                "service": "triage-worker",
                "logger": "__main__",
                "level": "error",
                "timestamp": ts,
            }
        )

    def test_the_same_fault_one_second_later_is_the_same_key(self):
        a = classify("cortex-triage-worker", self._line("Config reload failed"))
        b = classify(
            "cortex-triage-worker",
            self._line("Config reload failed", "2026-09-19T01:00:01.222222Z"),
        )
        assert a.error_key == b.error_key

    def test_the_same_fault_on_a_different_item_is_the_same_key(self):
        """Six real 'Pattern detection failed for <gmail-id>' lines are ONE fault."""
        a = classify(
            "cortex-triage-worker", self._line("Pattern detection failed for 1a06430fa5df4c3e")
        )
        b = classify(
            "cortex-triage-worker", self._line("Pattern detection failed for 1a05dcab473eb3dc")
        )
        assert a.error_key == b.error_key

    def test_postgres_clock_and_pid_do_not_split_one_fault(self):
        def mk(ts, pid):
            return (
                f"{ts} UTC [{pid}] ERROR:  duplicate key value violates "
                'unique constraint "unique_pattern"'
            )

        a = classify("cortex-postgres", mk("2026-09-14 16:23:14.923", 42))
        b = classify("cortex-postgres", mk("2026-09-14 16:24:02.117", 8891))
        assert a.error_key == b.error_key

    def test_a_loop_of_five_hundred_emissions_is_one_key(self):
        """The property the comment claims. It was 500 keys."""
        keys = {
            classify(
                "cortex-triage-worker", self._line("Worker crashed", f"2026-09-19T01:00:{n:02d}.0Z")
            ).error_key
            for n in range(60)
        }
        assert len(keys) == 1, f"a repeating fault produced {len(keys)} keys"

    def test_genuinely_different_faults_stay_apart(self):
        """Normalisation must not over-collapse into one useless bucket."""
        keys = {
            classify("cortex-triage-worker", self._line(e)).error_key
            for e in (
                "Config reload failed",
                "Gmail batch modify failed",
                "Cannot start without Docker connection",
            )
        }
        assert len(keys) == 3


class TestWarningsNeverReachTheChannel:
    """The whole noise-safety claim of this PR, and nothing held it.

    084c7fe made WARNING the primary path for every untriaged error in the
    estate. If that branch ever pings Discord, the alerter becomes the flood it
    was written to avoid -- and 'the WARNING branch also calls send_embed'
    survived the full suite before this test existed.
    """

    @staticmethod
    def _daemon():
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon("https://discord.test/hook", containers=["cortex-x"])
        d.discord = mock.Mock()
        return d

    def test_an_unclassified_error_is_counted_and_not_sent(self):
        d = self._daemon()
        line = json.dumps({"event": "Config reload failed", "level": "error"})
        assert classify("cortex-x", line).severity is Severity.WARNING
        d._process_log_line("cortex-x", line)
        d.discord.send_embed.assert_not_called()
        d.discord.send.assert_not_called()

    def test_it_really_was_counted(self):
        """Otherwise 'not sent' is satisfied by doing nothing at all."""
        d = self._daemon()
        d.rate_limiter = mock.Mock()
        d._process_log_line(
            "cortex-x", json.dumps({"event": "Config reload failed", "level": "error"})
        )
        d.rate_limiter.increment_warning.assert_called_once()
        d.rate_limiter.should_alert.assert_not_called()

    def test_a_critical_still_does_reach_the_channel(self):
        """The control: WARNING silence must not be silence for everything."""
        d = self._daemon()
        line = "psycopg2.OperationalError: could not connect to server"
        assert classify("cortex-x", line).severity is Severity.CRITICAL
        d._process_log_line("cortex-x", line)
        assert d.discord.send_embed.called, "a real critical must still alert"


class TestTheDailySummaryTruncatesLoudly:
    """A summary that silently drops entries fails toward silence.

    That is this PR's own bug class, one layer down: the 20-line cap and the
    "... and N more" tail were both unasserted, so a cap that stopped saying it
    had capped would ship green.
    """

    @staticmethod
    def _daemon():
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon("https://discord.test/hook", containers=["cortex-x"])
        d.discord = mock.Mock()
        return d

    def _summary_of(self, n, sample="something went wrong"):
        """Drives a REAL RateLimiter.

        The first version of this stubbed it, which hid the defect the summary
        actually had: the message was discarded at increment_warning(), so the
        rendered line was a digest and a count and no error text at all. A
        mocked rate limiter cannot show that, because the test supplies the
        dict the real one would have failed to populate.
        """
        d = self._daemon()
        for i in range(n):
            d.rate_limiter.increment_warning(f"cortex-x:unclassified:{i:04x}", sample)
        d._send_daily_summary()
        return d.discord.send_embed.call_args.kwargs["description"]

    def test_it_caps_at_twenty_and_says_how_many_it_dropped(self):
        body = self._summary_of(51)
        assert body.count("\n- ") == 20, "the cap must hold"
        assert "... and 31 more" in body, "a silent truncation is the bug this PR is about"

    def test_a_short_summary_has_no_truncation_notice(self):
        body = self._summary_of(3)
        assert body.count("\n- ") == 3
        assert "more" not in body

    def test_an_empty_day_still_reports(self):
        """Silence and 'nothing happened' must be distinguishable."""
        d = self._daemon()
        d._send_daily_summary()
        assert "No warnings" in d.discord.send_embed.call_args.kwargs["description"]


class TestClientFaultFilterNeverSuppressesCorruption:
    """The filter drops OUR typos. It must never drop the database's distress.

    A review suggested widening it to cover the 29 postgres lines a week that
    still reach the channel. Declined, and locked down here instead: those lines
    are not the typo class. `item order invariant violated for index` is btree
    corruption -- the cortex-rj2b failure that returned 85 rows where a seqscan
    returned 112, a 24% silent row loss that went undetected precisely because
    it raised no alarm anyone was listening for.

    Suppressing it to quieten the channel would rebuild that blindness on
    purpose.
    """

    @pytest.mark.parametrize(
        "line",
        [
            'ERROR:  item order invariant violated for index "unique_email_mapping"',
            "ERROR:  invalid collation version change",
            'ERROR:  duplicate key value violates unique constraint "unique_pattern"',
            'ERROR:  no partition of relation "queue" found for row',
            "ERROR:  could not map dynamic shared memory segment",
            "ERROR:  refusing to run: this migration drops the constraint",
        ],
    )
    def test_the_databases_own_distress_is_never_filtered(self, line):
        assert not is_client_sql_fault("cortex-postgres", line), (
            "this is the database in trouble, not a caller's bad SQL"
        )

    @pytest.mark.parametrize(
        "line",
        [
            'ERROR:  column "foo" does not exist at character 8',
            'ERROR:  relation "nosuch" does not exist',
            'ERROR:  syntax error at or near "slect"',
            "ERROR:  unterminated quoted identifier at or near",
            "ERROR:  aggregate functions are not allowed in GROUP BY",
            "ERROR:  ORDER BY position 2 is not in select list",
            # The one alternation this PR added that had no test.
            'ERROR:  invalid input syntax for type integer: "abc"',
        ],
    )
    def test_our_own_typos_are_filtered(self, line):
        assert is_client_sql_fault("cortex-postgres", line)

    def test_only_postgres(self):
        """The filter is scoped by container; a worker saying this is our bug."""
        line = 'ERROR:  relation "nosuch" does not exist'
        assert is_client_sql_fault("cortex-postgres", line)
        assert not is_client_sql_fault("cortex-triage-worker", line)


class TestTheSummarySaysWhatFailed:
    """The PR's declared landing place must carry the error, not just a tally.

    084c7fe made the daily summary the destination for every untriaged error in
    the estate. It rendered them as

        - **Unclassified:8A416200** (cortex-gateway): 2

    -- container, count, opaque digest, no message. The text was discarded at
    `increment_warning()`, which took only the key. `.title()` then case-mangled
    the hex, so the string printed was not even the key you could grep for.

    A summary nobody can act on is the silence this PR exists to end, wearing
    the fix's clothes.
    """

    @staticmethod
    def _summary(container, event, times=1):
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon("https://discord.test/hook", containers=[container])
        d.discord = mock.Mock()
        for i in range(times):
            d._process_log_line(
                container,
                json.dumps(
                    {"event": event, "level": "error", "timestamp": f"2026-09-19T0{i}:00:00Z"}
                ),
            )
        d._send_daily_summary()
        return d.discord.send_embed.call_args.kwargs["description"]

    def test_the_message_reaches_the_summary(self):
        body = self._summary("cortex-gateway", "Missing required environment variables: PG_PASS")
        assert "Missing required environment variables: PG_PASS" in body

    def test_the_count_is_still_there(self):
        body = self._summary("cortex-gateway", "Config reload failed", times=3)
        assert "): 3" in body
        assert "Config reload failed" in body

    def test_the_digest_is_left_greppable(self):
        """.title() on a hex digest prints a string that matches no key."""
        body = self._summary("cortex-gateway", "Config reload failed")
        digest = classify(
            "cortex-gateway",
            json.dumps(
                {
                    "event": "Config reload failed",
                    "level": "error",
                    "timestamp": "2026-09-19T00:00:00Z",
                }
            ),
        ).error_key.rsplit(":", 1)[1]
        assert digest == digest.lower(), "precondition: keys are lowercase hex"
        assert digest in body, "the printed key must be the real one"

    def test_the_sample_represents_the_bucket_not_one_arrival(self):
        """Six ids collapse to one key, so the sample must be the normalised form."""
        body = self._summary(
            "cortex-triage-worker", "Pattern detection failed for 1a06430fa5df4c3e"
        )
        assert "Pattern detection failed for <>" in body


class TestDedupSourceSurvivesAnythingDockerEmits:
    """An exception here does not fail one line, it drops a container.

    `_tail_container` wraps the whole tail loop in `except Exception`, sleeps
    30s and restarts with `since=now` -- so a raise inside classify silently
    discards every line that container emitted in between. Both isinstance
    guards and the `errors="replace"` on the hash are load-bearing on input
    Docker can really produce, and none of them was tested.
    """

    @pytest.mark.parametrize(
        "line",
        [
            "null",
            "[]",
            '"just a string"',
            "123456",
            '{"event": {"nested": "object"}}',
            '{"event": null}',
            '{"event": 5}',
            '{"event": "truncated by the log driver...',
            "",
            "\udcff a lone surrogate",
            "a NUL\x00byte",
            "plain text, not JSON at all",
        ],
    )
    def test_it_returns_a_string_and_does_not_raise(self, line):
        from cortex_utils.alerter.classifier import _dedup_source

        assert isinstance(_dedup_source(line), str)

    def test_a_surrogate_can_still_be_hashed(self):
        """errors='replace' on the encode, or classify() raises on real input."""
        assert classify("cortex-x", "ERROR \udcff broke") is not None

    def test_a_very_long_line_is_bounded(self):
        from cortex_utils.alerter.classifier import _dedup_source

        assert len(_dedup_source("ERROR " + "x" * 100_000)) <= 200


class TestAFailedSummaryDoesNotDeleteTheDay:
    """The summary used to clear the day BEFORE it sent, and ignore the result.

    `reset_warning_counts()` ran 38 lines above the send, and the send's bool
    was discarded -- so one Discord 5xx, timeout or 429 destroyed every warning
    accumulated that day, unrecoverably. Worse in a way that is easy to miss:
    DiscordClient logs that failure into cortex-alerter, the one container
    DENYLISTED_CONTAINERS excludes, so the alerter's report that it could not
    report went to the one log nothing watches.
    """

    @staticmethod
    def _daemon(delivered):
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon("https://discord.test/hook", containers=["cortex-x"])
        d.discord = mock.Mock()
        d.discord.send_embed.return_value = delivered
        for i in range(5):
            d._process_log_line(
                "cortex-x",
                json.dumps(
                    {
                        "event": f"fault number {i}",
                        "level": "error",
                        "timestamp": f"2026-09-19T0{i}:00:00Z",
                    }
                ),
            )
        return d

    def test_a_failed_send_keeps_the_warnings(self):
        d = self._daemon(delivered=False)
        assert len(d.rate_limiter.get_warning_counts()) == 5
        d._send_daily_summary()
        assert len(d.rate_limiter.get_warning_counts()) == 5, (
            "a 5xx must not delete the day -- the next summary carries them instead"
        )

    def test_a_successful_send_does_clear(self):
        """The control: keeping them forever is its own bug."""
        d = self._daemon(delivered=True)
        d._send_daily_summary()
        assert d.rate_limiter.get_warning_counts() == {}

    def test_a_failed_send_is_reported(self):
        d = self._daemon(delivered=False)
        with mock.patch("cortex_utils.alerter.daemon.log") as logger:
            d._send_daily_summary()
        assert logger.error.called, "an undelivered summary must be loud somewhere"

    def test_an_empty_day_also_respects_delivery(self):
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon("https://discord.test/hook", containers=["cortex-x"])
        d.discord = mock.Mock()
        d.discord.send_embed.return_value = False
        d._send_daily_summary()  # must not raise on the no-warnings path
        assert d.discord.send_embed.called


class TestTheSummaryIsActuallyScheduled:
    """A summary that never fires is the same outcome as one with no content.

    Every run() test patches the whole `schedule` module, so removing the
    scheduling entirely, or scheduling a different job, both shipped green.
    """

    @staticmethod
    def _run_once(daemon):
        daemon._stop_event.set()
        with (
            mock.patch.object(daemon, "_connect_docker", return_value=True),
            mock.patch.object(daemon, "_tail_container"),
            mock.patch.object(daemon, "_discover_running_containers", return_value=["cortex-x"]),
            mock.patch("cortex_utils.alerter.daemon.schedule") as sched,
        ):
            daemon.run()
        return sched

    def _daemon(self, hour=6):
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon(
                "https://discord.test/hook", containers=["cortex-x"], summary_hour=hour
            )
        d.discord = mock.Mock()
        return d

    def test_it_schedules_the_summary_at_the_configured_hour(self):
        d = self._daemon(hour=6)
        sched = self._run_once(d)
        sched.every.return_value.day.at.assert_called_once_with("06:00")

    def test_the_hour_is_honoured(self):
        d = self._daemon(hour=23)
        sched = self._run_once(d)
        sched.every.return_value.day.at.assert_called_once_with("23:00")

    def test_it_schedules_the_summary_itself_not_some_other_job(self):
        """`.do(lambda: None)` shipped green before this."""
        d = self._daemon()
        sched = self._run_once(d)
        sched.every.return_value.day.at.return_value.do.assert_called_once_with(
            d._send_daily_summary
        )


class TestOneRaiseDoesNotKillTheScheduler:
    """`schedule.run_pending()` does not catch, and this is a daemon thread.

    One exception out of a job killed the thread permanently: every later
    summary silently never sent, the process still looking healthy, and
    warning_counts filling with nothing draining it. Failing toward silence,
    which is the bug this whole PR is about.
    """

    def test_the_loop_survives_a_raising_job(self):
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon("https://discord.test/hook", containers=["cortex-x"])
        calls = []

        def boom():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("the summary blew up")
            d._stop_event.set()

        with (
            mock.patch("cortex_utils.alerter.daemon.schedule.run_pending", side_effect=boom),
            mock.patch("cortex_utils.alerter.daemon.time.sleep"),
        ):
            d._schedule_loop()

        assert len(calls) == 2, "the loop must run again after a job raised"


class TestTheTailerDelivers:
    """Ingestion: the stage every other stage rests on, and the one with no tests.

    Rounds 1-4 fixed the gate, the dedup key, the summary's content and its
    delivery. All four assume a line arrives here. `grep -rn "_tail_container"
    tests/` returned three hits before this class and all three were
    `mock.patch.object` -- the function was never executed, so ten mutations of
    it shipped green, including `self._process_log_line(...)` replaced by
    `pass`. The alerter could ingest NOTHING and 591 tests agreed it was fine.

    The failure-recovery cases are not hypothetical: six cortex containers were
    recreated by a redeploy on 2026-09-16, and a recreation is exactly the
    NotFound window. A tailer thread that returns is indistinguishable from one
    quietly watching, because they are all daemon threads with no supervision.
    """

    @staticmethod
    def _daemon():
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon("https://discord.test/hook", containers=["cortex-x"])
        d.discord = mock.Mock()
        return d

    @staticmethod
    def _client(d, lines, raise_first=None):
        """A docker client whose SECOND tail attempt stops the loop.

        Stopping from inside logs() rather than after it means the loop
        terminates whatever the mutant does -- including a mutant that removes
        the handler which would otherwise have ended it.
        """
        calls = []
        container = mock.Mock()

        def logs(**kw):
            calls.append(kw)
            if len(calls) >= 2:
                d._stop_event.set()
                return iter(())
            if raise_first is not None:
                raise raise_first
            return iter(lines)

        container.logs.side_effect = logs
        client = mock.Mock()
        client.containers.get.return_value = container
        client.logs_calls = calls
        return client

    def test_every_line_reaches_the_classifier_under_its_own_container_name(self):
        d = self._daemon()
        d.docker_client = self._client(
            d, [b'{"event": "boom", "level": "error"}\n', b"  second line  \n"]
        )
        seen = []
        d._process_log_line = lambda c, line: seen.append((c, line))
        with mock.patch("cortex_utils.alerter.daemon.time.sleep"):
            d._tail_container("cortex-x")
        assert seen == [
            ("cortex-x", '{"event": "boom", "level": "error"}'),
            ("cortex-x", "second line"),
        ]

    def test_undecodable_bytes_do_not_drop_the_line(self):
        d = self._daemon()
        d.docker_client = self._client(d, [b"ERROR \xff broke\n"])
        seen = []
        d._process_log_line = lambda c, line: seen.append(line)
        with mock.patch("cortex_utils.alerter.daemon.time.sleep"):
            d._tail_container("cortex-x")
        assert len(seen) == 1

    def test_it_follows_a_live_stream_rather_than_reading_the_backlog(self):
        d = self._daemon()
        d.docker_client = self._client(d, [])
        with mock.patch("cortex_utils.alerter.daemon.time.sleep"):
            d._tail_container("cortex-x")
        kw = d.docker_client.logs_calls[0]
        assert kw["stream"] is True and kw["follow"] is True
        assert "since" in kw, "without since, every restart replays the whole history"

    @pytest.mark.parametrize(
        "exc",
        [
            docker.errors.NotFound("container recreated by a redeploy"),
            docker.errors.APIError("docker restarted"),
            RuntimeError("classify raised on a hostile line"),
        ],
    )
    def test_a_failure_does_not_end_the_watch(self, exc):
        d = self._daemon()
        d.docker_client = self._client(d, [b"x\n"], raise_first=exc)
        with mock.patch("cortex_utils.alerter.daemon.time.sleep"):
            d._tail_container("cortex-x")
        assert len(d.docker_client.logs_calls) == 2, (
            f"{type(exc).__name__} left the container permanently unwatched"
        )

    def test_the_reconnect_resumes_from_now_and_so_drops_the_gap(self):
        """PINS A KNOWN LOSS rather than asserting it is correct.

        `since=datetime.now()` is re-evaluated on every pass of the while loop,
        so a reconnect resumes from the moment it reconnects and whatever the
        container emitted during the outage is gone. Measured on real traffic:
        cortex-triage-worker logs ~2.1 lines/s, so a 30s catch-all sleep drops
        ~63 lines, and cortex-labeling-worker logged two `Gmail batch modify
        failed` errors 35 seconds apart on 2026-09-14 -- inside one window.

        This asserts the CURRENT behaviour so that changing it is a deliberate
        act with a failing test, not an accident. The redesign is cortex-okcx.
        """
        d = self._daemon()
        d.docker_client = self._client(d, [b"x\n"], raise_first=RuntimeError("boom"))
        with mock.patch("cortex_utils.alerter.daemon.time.sleep"):
            d._tail_container("cortex-x")
        first, second = d.docker_client.logs_calls[0], d.docker_client.logs_calls[1]
        assert second["since"] > first["since"], (
            "the reconnect moved `since` forward, which is the documented loss"
        )


class TestConnectDocker:
    def test_a_docker_failure_is_reported_as_failure(self):
        d = TestTheTailerDelivers._daemon()
        with mock.patch(
            "cortex_utils.alerter.daemon.docker.from_env",
            side_effect=docker.errors.DockerException("no socket"),
        ):
            assert d._connect_docker() is False

    def test_run_refuses_to_half_start_without_docker(self):
        d = TestTheTailerDelivers._daemon()
        with (
            mock.patch.object(d, "_connect_docker", return_value=False),
            mock.patch.object(d, "_tail_container") as tail,
            mock.patch("cortex_utils.alerter.daemon.schedule"),
        ):
            d.run()
        assert not tail.called
        assert not d.discord.send.called


class TestRunStartsATailerPerContainer:
    """`_tail_container` is patched in every run() test and was never asserted on.

    So `for container_name in self.containers:` replaced by `for ... in []:`
    started no tailer threads at all and the suite stayed green -- the alerter
    would discover its containers, log that it was monitoring them, send its
    startup notice, and watch nothing.
    """

    @staticmethod
    def _run_with(containers, discovered=None):
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon("https://discord.test/hook", containers=containers)
        d.discord = mock.Mock()
        d._stop_event.set()
        with (
            mock.patch.object(d, "_connect_docker", return_value=True),
            mock.patch.object(d, "_tail_container") as tail,
            mock.patch.object(d, "_discover_running_containers", return_value=discovered or []),
            mock.patch("cortex_utils.alerter.daemon.schedule"),
        ):
            d.run()
        return tail

    def test_one_tailer_per_watched_container(self):
        tail = self._run_with(["cortex-a", "cortex-b", "cortex-c"])
        assert sorted(c.args[0] for c in tail.call_args_list) == [
            "cortex-a",
            "cortex-b",
            "cortex-c",
        ]

    def test_discovered_containers_are_tailed_too(self):
        """Discovery is pointless if nothing tails what it found."""
        tail = self._run_with(None, discovered=["cortex-gateway", "cortex-teach"])
        assert sorted(c.args[0] for c in tail.call_args_list) == [
            "cortex-gateway",
            "cortex-teach",
        ]

    def test_watching_nothing_starts_nothing(self):
        tail = self._run_with(None, discovered=[])
        assert not tail.called


_CRIT = (
    b'{"event": "History expired for historyId 12345", "service": "gmail-sync",'
    b' "level": "error", "timestamp": "2026-09-19T01:00:00Z"}\n'
)


def _warn(n):
    return (
        b'{"event": "Pattern detection failed for 1a0643' + str(n).encode() + b'fa5df4c3e",'
        b' "service": "triage-worker", "level": "error",'
        b' "timestamp": "2026-09-19T0' + str(n).encode() + b':00:00Z"}\n'
    )


def _drain(container, frames, ping_critical=True):
    """One real line, all the way from a fake Docker socket to a Discord payload.

    Real _tail_container, real classify, real _dedup_source, real RateLimiter,
    real _send_daily_summary. Only DiscordClient is recorded. This is the one
    test that would have failed on rounds 1, 3, 4 and 5's defects at once --
    every stage had its own tests and nobody had ever run them together.
    """
    from cortex_utils.alerter.daemon import AlerterDaemon

    with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
        d = AlerterDaemon(
            "https://discord.test/hook", containers=[container], ping_critical=ping_critical
        )
    d.discord = mock.Mock()
    d.discord.send_embed.return_value = True
    calls = []
    box = mock.Mock()

    def logs(**kw):
        calls.append(kw)
        if len(calls) >= 2:
            d._stop_event.set()
            return iter(())
        return iter(frames)

    box.logs.side_effect = logs
    d.docker_client = mock.Mock()
    d.docker_client.containers.get.return_value = box
    with mock.patch("cortex_utils.alerter.daemon.time.sleep"):
        d._tail_container(container)
    return d


class TestOneRealLineReachesDiscord:
    """The stages agree about the same line, or they do not. Nothing checked."""

    def test_a_critical_line_produces_exactly_one_alert_naming_its_container(self):
        d = _drain("cortex-gmail-sync", [_CRIT])
        assert d.discord.send_embed.call_count == 1
        kw = d.discord.send_embed.call_args.kwargs
        fields = {f["name"]: f["value"] for f in kw["fields"]}
        assert fields["Container"] == "cortex-gmail-sync"
        assert "History expired" in fields["Log"]
        assert kw["title"].startswith("CRITICAL:")

    @pytest.mark.parametrize("ping_critical", [True, False])
    def test_ping_critical_reaches_the_payload(self, ping_critical):
        """`ping=True` hardcoded shipped green -- an operator's @here setting ignored."""
        d = _drain("cortex-gmail-sync", [_CRIT], ping_critical=ping_critical)
        assert d.discord.send_embed.call_args.kwargs["ping"] is ping_critical

    def test_a_warning_reaches_nobody_until_the_summary_then_says_what_failed(self):
        d = _drain("cortex-triage-worker", [_warn(1), _warn(2), _warn(3)])
        assert d.discord.send_embed.call_count == 0, "a WARNING must not ping the channel"
        assert list(d.rate_limiter.get_warning_counts().values()) == [3], (
            "three ids, one fault, one key"
        )
        d._send_daily_summary()
        desc = d.discord.send_embed.call_args.kwargs["description"]
        assert "Pattern detection failed" in desc, "the summary must say what failed"
        assert d.rate_limiter.get_warning_counts() == {}, "a delivered summary clears the day"


class TestTheTestAlertNamesWhatItWouldWatch:
    """`cortex alerter test` runs BEFORE run(), so self.containers is still empty.

    Replacing `containers or DEFAULT_CONTAINERS` with `containers or []` made
    this render an empty Containers field where it used to name six. Discord's
    schema requires a non-empty field value, and an operator running the test
    learns nothing from a blank list.
    """

    @staticmethod
    def _fields(daemon):
        daemon.send_test_alert()
        return {f["name"]: f["value"] for f in daemon.discord.send_embed.call_args.kwargs["fields"]}

    @staticmethod
    def _daemon(containers=None):
        from cortex_utils.alerter.daemon import AlerterDaemon

        with mock.patch("cortex_utils.alerter.daemon.DiscordClient"):
            d = AlerterDaemon("https://discord.test/hook", containers=containers)
        d.discord = mock.Mock()
        return d

    def test_it_discovers_rather_than_reporting_an_empty_list(self):
        d = self._daemon()
        with (
            mock.patch.object(d, "_connect_docker", return_value=True),
            mock.patch.object(
                d, "_discover_running_containers", return_value=["cortex-a", "cortex-b"]
            ),
        ):
            assert self._fields(d)["Containers"] == "cortex-a, cortex-b"

    def test_the_field_is_never_empty_even_when_docker_is_down(self):
        """A webhook test must still work when Docker does not."""
        d = self._daemon()
        with mock.patch.object(d, "_connect_docker", return_value=False):
            value = self._fields(d)["Containers"]
        assert value, "Discord rejects an empty field value"
        assert "none discovered" in value

    def test_an_explicit_list_is_reported_as_given(self):
        d = self._daemon(containers=["cortex-only"])
        assert self._fields(d)["Containers"] == "cortex-only"
