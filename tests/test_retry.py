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
    assert ready_predicate() == "(next_attempt_at IS NULL OR next_attempt_at <= NOW())"


def test_ready_predicate_custom_column() -> None:
    assert ready_predicate("retry_at") == "(retry_at IS NULL OR retry_at <= NOW())"


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
    conn, cur = _mock_conn((1,))
    result = fail_or_retry(conn, job_id=42, error="boom", max_attempts=5, jitter_ratio=0.0)
    assert result == "retrying"
    # Should have issued SELECT FOR UPDATE then UPDATE with status='pending'
    calls = cur.execute.call_args_list
    assert "FOR UPDATE" in calls[0][0][0]
    assert "pending" in calls[1][0][0]
    conn.commit.assert_not_called()


def test_fail_or_retry_fails_when_at_max() -> None:
    conn, cur = _mock_conn((4,))
    result = fail_or_retry(conn, job_id=42, error="boom", max_attempts=5, jitter_ratio=0.0)
    assert result == "failed"
    calls = cur.execute.call_args_list
    assert "failed" in calls[1][0][0]
    conn.commit.assert_not_called()


def test_fail_or_retry_missing_job() -> None:
    conn, cur = _mock_conn(None)
    result = fail_or_retry(conn, job_id=999, error="boom", max_attempts=5)
    assert result == "failed"
    conn.commit.assert_not_called()


def test_fail_or_retry_null_attempts() -> None:
    conn, cur = _mock_conn((None,))
    result = fail_or_retry(conn, job_id=1, error="boom", max_attempts=5, jitter_ratio=0.0)
    assert result == "retrying"
    # attempts should be treated as 0, so next_attempts=1 < 5 -> retry
    update_args = cur.execute.call_args_list[1][0][1]
    assert update_args[0] == 1  # next_attempts


def test_fail_or_retry_truncates_error() -> None:
    conn, cur = _mock_conn((0,))
    long_error = "x" * 2000
    fail_or_retry(conn, job_id=1, error=long_error, max_attempts=5, error_max_chars=100)
    # The UPDATE call should have the truncated error
    update_args = cur.execute.call_args_list[1][0][1]
    assert len(update_args[1]) == 100
