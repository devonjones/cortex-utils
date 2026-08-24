"""Tests for queue retry/backoff helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cortex_utils.queue.retry import (
    DEFAULT_BASE_SECONDS,
    DEFAULT_CAP_SECONDS,
    compute_backoff_delay,
    fail_or_retry,
    ready_predicate,
)

# This module exists to test the legacy entry points, so their own deprecation
# warnings are expected here and only drown out real ones. pytest.warns still
# fires under an ignore filter, so the test that asserts them still works.
pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def test_backoff_doubles_each_attempt() -> None:
    delays = [
        compute_backoff_delay(n, base_seconds=30, cap_seconds=10_000, jitter_ratio=0.0)
        for n in range(1, 6)
    ]
    assert delays == [30, 60, 120, 240, 480]


def test_backoff_caps() -> None:
    delay = compute_backoff_delay(20, base_seconds=30, cap_seconds=900, jitter_ratio=0.0)
    assert delay == 900


def test_backoff_jitter_within_range() -> None:
    for _ in range(50):
        delay = compute_backoff_delay(3, base_seconds=30, cap_seconds=10_000, jitter_ratio=0.2)
        # exp_delay = 120; jitter spread = 24 -> [96, 144]
        assert 96 <= delay <= 144


def test_backoff_minimum_attempts_floor() -> None:
    delay = compute_backoff_delay(0, base_seconds=30, jitter_ratio=0.0)
    assert delay == 30


def test_backoff_returns_at_least_one_second() -> None:
    # Pathological config but should not return 0
    delay = compute_backoff_delay(1, base_seconds=1, cap_seconds=1, jitter_ratio=0.99)
    assert delay >= 1


def test_ready_predicate_default() -> None:
    assert (
        ready_predicate() == "(next_attempt_at IS NULL OR next_attempt_at <= statement_timestamp())"
    )


def test_ready_predicate_custom_column() -> None:
    assert ready_predicate("retry_at") == "(retry_at IS NULL OR retry_at <= statement_timestamp())"


def test_defaults() -> None:
    assert DEFAULT_BASE_SECONDS == 30
    assert DEFAULT_CAP_SECONDS == 900


def test_ready_predicate_rejects_invalid_column() -> None:
    with pytest.raises(ValueError, match="Invalid column name"):
        ready_predicate("col; DROP TABLE queue")


# --- fail_or_retry tests ---


def _mock_conn(fetchone_return):
    """Build a mock psycopg2 connection with a cursor context manager."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = fetchone_return
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cur


def test_fail_or_retry_retries_when_under_max() -> None:
    conn, cur = _mock_conn((1, "processing"))
    result = fail_or_retry(conn, job_id=42, error="boom", max_attempts=5, jitter_ratio=0.0)
    assert result == "retrying"
    calls = cur.execute.call_args_list
    assert "FOR UPDATE" in calls[0][0][0]
    assert "pending" in calls[1][0][0]
    conn.commit.assert_not_called()


def test_fail_or_retry_fails_when_at_max() -> None:
    conn, cur = _mock_conn((4, "processing"))
    result = fail_or_retry(conn, job_id=42, error="boom", max_attempts=5, jitter_ratio=0.0)
    assert result == "failed"
    calls = cur.execute.call_args_list
    assert "failed" in calls[1][0][0]
    conn.commit.assert_not_called()


def test_fail_or_retry_missing_job() -> None:
    conn, cur = _mock_conn(None)
    result = fail_or_retry(conn, job_id=999, error="boom", max_attempts=5)
    assert result == "failed"
    assert cur.execute.call_count == 1
    conn.commit.assert_not_called()


def test_fail_or_retry_null_attempts() -> None:
    conn, cur = _mock_conn((None, "processing"))
    result = fail_or_retry(conn, job_id=1, error="boom", max_attempts=5, jitter_ratio=0.0)
    assert result == "retrying"
    update_args = cur.execute.call_args_list[1][0][1]
    assert update_args[0] == 1


def test_fail_or_retry_truncates_error() -> None:
    conn, cur = _mock_conn((0, "processing"))
    long_error = "x" * 2000
    fail_or_retry(conn, job_id=1, error=long_error, max_attempts=5, error_max_chars=100)
    update_args = cur.execute.call_args_list[1][0][1]
    assert len(update_args[1]) == 100


def test_fail_or_retry_coerces_non_string_error() -> None:
    conn, cur = _mock_conn((0, "processing"))
    fail_or_retry(conn, job_id=1, error=ValueError("oops"), max_attempts=5)
    update_args = cur.execute.call_args_list[1][0][1]
    assert update_args[1] == "oops"


def test_fail_or_retry_skips_already_terminal_failed() -> None:
    conn, cur = _mock_conn((3, "failed"))
    result = fail_or_retry(conn, job_id=1, error="boom", max_attempts=5)
    assert result == "failed"
    assert cur.execute.call_count == 1


def test_fail_or_retry_skips_already_terminal_completed() -> None:
    conn, cur = _mock_conn((1, "completed"))
    result = fail_or_retry(conn, job_id=1, error="boom", max_attempts=5)
    assert result == "failed"
    assert cur.execute.call_count == 1


def test_retrying_releases_the_claim_token_with_the_claim() -> None:
    """A row put back on the pending pile must not still carry the token of the
    worker that failed it.

    Otherwise that worker -- which may simply be slow rather than dead -- can
    call complete() with its old token after the next claimant has picked the
    row up, match, and retire a job someone else is mid-flight on. Worse than
    carrying no token at all, because the token affirmatively vouches for the
    wrong claimant. The same defect was fixed in all six raw reset sites across
    postmark and triage; this is the library's own copy of it.
    """
    conn, cur = _mock_conn((1, "processing"))
    assert fail_or_retry(conn, job_id=42, error="boom", max_attempts=5) == "retrying"
    retry_sql = cur.execute.call_args_list[1][0][0]
    assert "claimed_by = NULL" in retry_sql, retry_sql


def test_exhausting_retries_keeps_the_token_as_a_record() -> None:
    """The other branch deliberately does NOT clear it: a 'failed' row is not
    claimable, so the token is inert there and worth keeping as a record of who
    last held the job. Pinned so the asymmetry reads as a decision rather than
    an oversight someone later "fixes" into a lost diagnostic.
    """
    conn, cur = _mock_conn((4, "processing"))
    assert fail_or_retry(conn, job_id=42, error="boom", max_attempts=5) == "failed"
    failed_sql = cur.execute.call_args_list[1][0][0]
    assert "claimed_by" not in failed_sql, failed_sql


def test_both_legacy_entry_points_warn() -> None:
    """The warning is the port checklist. All five cortex workers import these
    two names, so turning the deprecation on lights up exactly the call sites
    the port has to visit -- at no cost, and without breaking anyone today.

    Worth pinning because both are easy to lose: the docstrings said DEPRECATED
    for months while the functions warned about nothing, and a docstring is not
    something a consumer's CI can see.
    """
    conn, _ = _mock_conn((1, "processing"))
    with pytest.warns(DeprecationWarning, match="ops.fail_or_retry"):
        fail_or_retry(conn, job_id=1, error="boom", max_attempts=5)
    with pytest.warns(DeprecationWarning, match="claim"):
        ready_predicate()
