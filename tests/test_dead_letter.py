"""Tests for dead-letter retry.

`retry_job` had never worked: `payload` is JSONB, `get_job` hands it back as a
Python dict, and psycopg2 has no adapter for dict, so the old raw INSERT raised
on every non-dry-run call. Routing it through `enqueue()` fixed that by
accident. These pin the fixed behaviour so it cannot be reverted as a no-op
refactor.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import psycopg2
import pytest

from cortex_utils.queue.dead_letter import DeadLetterManager

JOB = {
    "id": 5,
    "queue_name": "triage",
    "payload": {"gmail_id": "abc"},
    "attempts": 3,
    "last_error": "boom",
}


class FakeCursor:
    def __init__(self, fetchone: Any = None):
        self.executed: list[tuple[str, Any]] = []
        self._fetchone = fetchone
        self.rowcount = 1
        self.raise_on: str | None = None

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        if self.raise_on and self.raise_on in sql:
            raise psycopg2.errors.UniqueViolation("boom")

    def fetchone(self) -> Any:
        return self._fetchone

    def fetchall(self) -> Any:
        return []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _manager(fetchone: Any = (99,)):
    cur = FakeCursor(fetchone=fetchone)
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur
    mgr = DeadLetterManager(conn)
    mgr.get_job = lambda job_id: dict(JOB)  # type: ignore[method-assign]
    return mgr, conn


def test_retry_wraps_the_payload_so_psycopg2_can_adapt_it() -> None:
    """The bug that made this path fail every time it ran.

    A bare dict reaches psycopg2 as an unadaptable type; it must arrive wrapped.
    """
    mgr, conn = _manager()
    assert mgr.retry_job(5) is True
    insert = [p for s, p in conn.cur.executed if "INSERT INTO queue" in s][0]
    payload = insert[1]
    assert not isinstance(payload, dict), "a raw dict raises: can't adapt type 'dict'"
    assert payload.adapted == JOB["payload"]


def test_retry_deletes_the_dead_letter_row_in_the_same_transaction() -> None:
    """Committing the re-enqueue separately would let a crash leave the job both
    queued and still in dead_letter."""
    mgr, conn = _manager()
    mgr.retry_job(5)
    statements = [s for s, _ in conn.cur.executed]
    assert any("INSERT INTO queue" in s for s in statements)
    assert any("DELETE FROM dead_letter" in s for s in statements)
    assert conn.commit.call_count == 1, "one commit covering both halves"


def test_retry_reenqueues_onto_the_original_queue() -> None:
    mgr, conn = _manager()
    mgr.retry_job(5)
    insert = [p for s, p in conn.cur.executed if "INSERT INTO queue" in s][0]
    assert insert[0] == "triage"


def test_dry_run_touches_nothing() -> None:
    mgr, conn = _manager()
    assert mgr.retry_job(5, dry_run=True) is True
    assert not conn.cur.executed
    conn.commit.assert_not_called()


def test_a_missing_job_is_not_reported_as_retried() -> None:
    mgr, conn = _manager()
    mgr.get_job = lambda job_id: None  # type: ignore[method-assign]
    assert mgr.retry_job(5) is False


def test_one_bad_job_does_not_poison_the_rest_of_the_batch() -> None:
    """Without the per-job guard the shared connection stays aborted and every
    remaining job fails for a reason unrelated to it."""
    mgr, conn = _manager()
    mgr.list_jobs = lambda **kw: [dict(JOB, id=1), dict(JOB, id=2)]  # type: ignore[method-assign]
    calls: list[int] = []

    def flaky(job_id: int, dry_run: bool = False) -> bool:
        calls.append(job_id)
        if job_id == 1:
            raise psycopg2.errors.UniqueViolation("boom")
        return True

    mgr.retry_job = flaky  # type: ignore[method-assign]
    assert mgr.retry_jobs() == 1
    assert calls == [1, 2], "the second job must still be attempted"
    conn.rollback.assert_called_once()


def test_a_non_database_error_is_not_swallowed() -> None:
    """The guard is scoped to psycopg2 errors; a bug in our own code must surface."""
    mgr, conn = _manager()
    mgr.list_jobs = lambda **kw: [dict(JOB, id=1)]  # type: ignore[method-assign]

    def broken(job_id: int, dry_run: bool = False) -> bool:
        raise ValueError("programming error")

    mgr.retry_job = broken  # type: ignore[method-assign]
    with pytest.raises(ValueError):
        mgr.retry_jobs()


def test_a_row_that_vanished_mid_retry_is_not_reported_as_retried() -> None:
    """A concurrent retry or purge can take it while we work. Claiming success
    would also leave the re-enqueue above as a duplicate."""
    mgr, conn = _manager()
    conn.cur.rowcount = 0
    assert mgr.retry_job(5) is False
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
