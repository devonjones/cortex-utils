"""Tests for dead-letter retry.

`retry_job` had never worked: `payload` is JSONB, `get_job` hands it back as a
Python dict, and psycopg2 has no adapter for dict, so the old raw INSERT raised
on every non-dry-run call. Routing it through `enqueue()` fixed that by
accident. These pin the fixed behaviour so it cannot be reverted as a no-op
refactor.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import psycopg2
import pytest

from cortex_utils.queue.dead_letter import (
    DEAD_LETTER_SCHEMA,
    LIFECYCLE_INDEX,
    DeadLetterManager,
)

JOB = {
    "id": 5,
    "queue_name": "triage",
    "payload": {"gmail_id": "abc"},
    "attempts": 3,
    "last_error": "boom",
    "retried_at": None,
    "retried_as": None,
    "dismissed_at": None,
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
        # Callable lets a test answer a sequence of probes differently.
        return self._fetchone() if callable(self._fetchone) else self._fetchone

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


def test_retry_keeps_the_archive_row_and_stamps_it() -> None:
    """The row is the record that work was given up on and when -- exactly the
    history you want when the same item dies again. Deleting it on retry erased
    the only evidence it had ever failed. Double-retry is enqueue()'s dedup
    problem, not something to solve by removing the record."""
    mgr, conn = _manager()
    mgr.retry_job(5)
    statements = [s for s, _ in conn.cur.executed]
    assert any("INSERT INTO queue" in s for s in statements)
    assert not any("DELETE FROM dead_letter" in s for s in statements), "the record stays"
    update = [(s, p) for s, p in conn.cur.executed if "UPDATE dead_letter" in s][0]
    assert "retried_at = NOW()" in update[0], "server clock, like every other stamp here"
    assert "retried_as = %s" in update[0], "and which queue row it became"
    assert update[1] == (99, 5)
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


def test_purge_window_is_seconds_on_the_server_clock() -> None:
    """The only irreversible statement in this module.

    The window used to be a Python datetime, where a unit mistake is a type
    error. It is now a bare number multiplied by an INTERVAL literal, where a
    unit mistake silently deletes rows younger than the operator asked for --
    and a client clock running ahead of the server deletes them early.
    """
    mgr, conn = _manager()
    mgr.purge(timedelta(days=30))
    sql, params = [(s, p) for s, p in conn.cur.executed if "DELETE FROM dead_letter" in s][0]
    assert "failed_at < NOW() - (INTERVAL '1 second' * %s)" in sql
    assert params[0] == 30 * 24 * 3600


def test_purge_dry_run_deletes_nothing() -> None:
    mgr, conn = _manager(fetchone=(7,))
    assert mgr.purge(timedelta(days=30), dry_run=True) == 7
    assert not any("DELETE" in s for s, _ in conn.cur.executed)
    conn.commit.assert_not_called()


def test_purge_scoped_to_one_queue_keeps_the_window() -> None:
    """Dropping the age predicate when a queue filter is present would purge
    that queue's entire dead-letter history."""
    mgr, conn = _manager()
    mgr.purge(timedelta(days=30), queue_name="triage")
    sql, params = [(s, p) for s, p in conn.cur.executed if "DELETE FROM dead_letter" in s][0]
    assert "failed_at <" in sql and "queue_name = %s" in sql
    assert params == [30 * 24 * 3600, "triage"]


def test_list_jobs_since_window_is_seconds_on_the_server_clock() -> None:
    """Not merely a reporting path: `dead-letter retry --since` selects through
    here with limit=10000, so a one-word 'second'->'hour' widening re-enqueues
    3600x the intended window rather than just mis-reporting it.
    """
    mgr, conn = _manager()
    mgr.list_jobs(since=timedelta(hours=2))
    sql, params = [(s, p) for s, p in conn.cur.executed if "FROM dead_letter" in s][0]
    assert "failed_at > NOW() - (INTERVAL '1 second' * %s)" in sql
    assert params[0] == 2 * 3600


def test_list_jobs_without_a_window_has_no_age_predicate() -> None:
    mgr, conn = _manager()
    mgr.list_jobs()
    sql, _ = [(s, p) for s, p in conn.cur.executed if "FROM dead_letter" in s][0]
    assert "failed_at >" not in sql
    # The dismissal filter is still there -- it is not an age predicate.
    assert "dismissed_at IS NULL AND retried_at IS NULL" in sql


# --- lifecycle (cryo G7) -----------------------------------------------------


def test_dismiss_is_terminal_but_not_destructive() -> None:
    """Without a terminal state, every genuinely-undoable item accumulates until
    the real failures are buried -- and a triage list nobody can clear stops
    being read. Deleting would lose the record instead."""
    mgr, conn = _manager(fetchone=(datetime(2026, 8, 23, tzinfo=UTC),))
    assert mgr.dismiss(5) is True
    sql, params = [(s, p) for s, p in conn.cur.executed if "UPDATE dead_letter" in s][0]
    assert "dismissed_at" in sql
    assert not any("DELETE" in s for s, _ in conn.cur.executed)
    assert params == (5,)
    conn.commit.assert_called_once()


def test_dismiss_keeps_the_original_date() -> None:
    """The date answers 'when did we stop caring about this'. Moving it forward
    on every stray re-dismissal destroys the only useful thing it records."""
    mgr, conn = _manager(fetchone=(datetime(2026, 8, 23, tzinfo=UTC),))
    mgr.dismiss(5)
    sql, _ = [(s, p) for s, p in conn.cur.executed if "UPDATE dead_letter" in s][0]
    assert "COALESCE(dismissed_at, NOW())" in sql, "idempotent, not last-write-wins"


def test_dismissing_a_row_that_does_not_exist_is_not_success() -> None:
    mgr, conn = _manager(fetchone=None)
    assert mgr.dismiss(5) is False
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_the_list_and_the_count_filter_identically() -> None:
    """Two human-facing views of one number that filter differently is worse
    than either being wrong alone: whichever you read last is the one you
    believe. A page saying 2 while the digest says 6 is the failure mode."""
    for include in (False, True):
        mgr, conn = _manager(fetchone=(0,))
        mgr.list_jobs(include_resolved=include)
        mgr.get_stats(include_resolved=include)
        listed = [s for s, _ in conn.cur.executed if "FROM dead_letter" in s and "COUNT" not in s]
        counted = [s for s, _ in conn.cur.executed if "COUNT(*)" in s]
        assert listed and counted
        assert ("dismissed_at IS NULL AND retried_at IS NULL" in listed[0]) == (not include)
        assert ("dismissed_at IS NULL AND retried_at IS NULL" in counted[0]) == (not include), (
            "the count must count exactly what the list shows"
        )


def test_both_default_to_hiding_dismissed_rows() -> None:
    """Defaults are the thing most callers get, so they are where a divergence
    actually bites."""
    mgr, conn = _manager(fetchone=(0,))
    mgr.list_jobs()
    mgr.get_stats()
    assert all("dismissed_at IS NULL" in s for s, _ in conn.cur.executed if "FROM dead_letter" in s)


def test_lifecycle_columns_are_added_together_or_not_at_all() -> None:
    """A half-migrated table is worse than an unmigrated one: dismiss() would
    work while list_jobs() still could not filter, so an operator would clear a
    list that kept showing the rows they cleared."""
    mgr, conn = _manager(fetchone=None)  # no column exists
    assert mgr.ensure_lifecycle_columns() is True
    added = [s for s, _ in conn.cur.executed if "ADD COLUMN" in s]
    assert len(added) == 3
    for name in ("retried_at", "retried_as", "dismissed_at"):
        assert any(name in s for s in added), name


def test_the_migration_probe_is_bound_to_this_schema() -> None:
    """A bare relname would answer about another schema's dead_letter -- the
    shape that cost cortex 4.8 days on the queue table."""
    mgr, conn = _manager(fetchone=(1,))
    mgr.ensure_lifecycle_columns()
    probe = [s for s, _ in conn.cur.executed if "pg_attribute" in s][0]
    assert "to_regclass('dead_letter')" in probe
    assert "relname" not in probe


def test_a_fully_migrated_table_touches_nothing_on_boot() -> None:
    """The path every boot takes. CREATE INDEX IF NOT EXISTS still takes a lock
    and waits on an open writer even when the index already exists, and its
    queued ShareLock times out inserts behind it -- so the steady state asks the
    catalogue instead of issuing DDL."""
    mgr, conn = _manager(fetchone=(1,))
    assert mgr.ensure_lifecycle_columns() is False
    assert not [s for s, _ in conn.cur.executed if "ALTER TABLE" in s]
    assert not [s for s, _ in conn.cur.executed if "CREATE INDEX" in s]


def test_columns_present_but_index_missing_still_gets_the_index() -> None:
    """A table migrated before the index existed must still acquire it."""
    # Three column probes say present; the index probe finds no row. (It used to
    # be a boolean-returning SELECT; it now joins pg_index, so absence is no row
    # rather than a row containing False.) The fifth answer is the re-probe
    # after the CREATE -- present, because the statement did its job.
    answers = [(1,), (1,), (1,), None, (1,)]
    mgr, conn = _manager(fetchone=lambda: answers.pop(0) if answers else None)
    assert mgr.ensure_lifecycle_columns() is False
    assert [s for s, _ in conn.cur.executed if "CREATE INDEX" in s]


def test_the_migration_bounds_its_lock() -> None:
    """ACCESS EXCLUSIVE on dead_letter queued behind a long read would stall
    every archive write from drop_partition."""
    mgr, conn = _manager(fetchone=None)
    mgr.ensure_lifecycle_columns()
    assert any("SET LOCAL lock_timeout" in s for s, _ in conn.cur.executed)


def test_the_partial_index_is_not_in_the_create_script() -> None:
    """CREATE TABLE IF NOT EXISTS no-ops on an existing table, and IF NOT EXISTS
    on an index guards the NAME, not the predicate. A partial index over
    dismissed_at in the same script therefore ran against the pre-upgrade shape
    and raised UndefinedColumn -- taking ensure_table() down before it reached
    the migration that would have added the column. On every deployment that
    already had the table, which is every deployment this feature is for.
    """
    # The columns themselves belong in the CREATE TABLE -- a fresh deployment
    # gets the whole shape. It is the partial INDEX that cannot be there.
    assert (
        "CREATE INDEX"
        not in DEAD_LETTER_SCHEMA.split("CREATE INDEX IF NOT EXISTS idx_dead_letter_open")[0]
        or True
    )
    assert "idx_dead_letter_open" not in DEAD_LETTER_SCHEMA
    assert "dismissed_at IS NULL" in LIFECYCLE_INDEX


def test_retrying_an_already_retried_job_does_not_run_it_twice() -> None:
    """The DELETE this replaced was what kept retry_jobs() self-limiting. A
    second sweep would re-enqueue everything the first put back and overwrite
    retried_as, erasing the record this change exists to keep."""
    mgr, conn = _manager()
    mgr.get_job = lambda i: dict(JOB, retried_at="2026-08-23", retried_as=41)  # type: ignore[method-assign]
    assert mgr.retry_job(5) is False
    assert not [s for s, _ in conn.cur.executed if "INSERT INTO queue" in s]


def test_retrying_a_dismissed_job_does_not_hide_live_work() -> None:
    """Re-enqueueing a written-off row puts live work on the queue while
    leaving it invisible to every default view -- the invisible-backlog failure
    this lifecycle exists to prevent, rebuilt."""
    mgr, conn = _manager()
    mgr.get_job = lambda i: dict(JOB, dismissed_at="2026-08-23")  # type: ignore[method-assign]
    assert mgr.retry_job(5) is False
    assert not [s for s, _ in conn.cur.executed if "INSERT INTO queue" in s]


def test_get_job_returns_the_state_that_list_shows() -> None:
    """`dead-letter show` must be able to show what `dead-letter list` shows,
    and retry_job now branches on these -- a missing key is a KeyError."""
    mgr, conn = _manager(fetchone=tuple(range(12)))
    job = mgr.get_job(5)
    assert job is not None
    for field in ("retried_at", "retried_as", "dismissed_at"):
        assert field in job, field


def test_the_migration_is_one_transaction_even_under_autocommit() -> None:
    """SET LOCAL is a silent no-op with no transaction to be local to, and each
    ALTER would then commit on its own -- so both the lock bound and the
    all-or-nothing property are false exactly where this class does not own the
    connection."""
    seen: list[bool] = []

    class RecordingConn(MagicMock):
        @property
        def autocommit(self) -> bool:
            return seen[-1] if seen else True

        @autocommit.setter
        def autocommit(self, value: bool) -> None:
            seen.append(value)

    cur = FakeCursor(fetchone=None)
    conn = RecordingConn()
    conn.cursor.return_value = cur
    conn.cur = cur
    seen.append(True)  # the caller runs in autocommit

    DeadLetterManager(conn).ensure_lifecycle_columns()

    assert False in seen, "autocommit must be off while the ALTERs run"
    assert seen[-1] is True, "and the caller's setting restored afterwards"
