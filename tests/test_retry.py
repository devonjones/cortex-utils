"""Tests for queue retry/backoff helpers."""

from __future__ import annotations

from cortex_utils.queue.retry import (
    DEFAULT_BASE_SECONDS,
    DEFAULT_CAP_SECONDS,
    compute_backoff_delay,
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
