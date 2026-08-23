"""Tests for the one-time queue migration.

Two defects live here, both of the family this PR is about: a decision made
from something other than the database's own answer.

`is_queue_partitioned` matched `pg_class.relname` with no schema binding -- the
exact shape that let cryo's `queue` masquerade as cortex's and produced a
4.8-day silent ingestion outage. Here the consequence is worse than a missed
partition: the migration reports `already_partitioned` and skips entirely.

And the partition window was anchored on the newest existing *row* rather than
on today, so a queue whose last insert predates the migration gets a range that
ends before go-live -- every partition created, none covering NOW().
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from cortex_utils.queue.migrate import (
    drop_old_queue_table,
    is_queue_partitioned,
    migrate_to_partitioned,
)

SERVER_TODAY = date(2026, 3, 1)


class FakeCursor:
    def __init__(self, answers: dict[str, Any] | None = None):
        self.executed: list[tuple[str, Any]] = []
        self.answers = answers or {}
        self._next: Any = None
        self.rowcount = 0

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        self._next = None
        if "CURRENT_DATE" in sql:
            self._next = (SERVER_TODAY,)
            return
        if "to_regclass('queue_old')" in sql and "to_regclass('queue_old')" not in self.answers:
            self._next = (1,)
            return
        for needle, value in self.answers.items():
            if needle in sql:
                self._next = value
                return

    def fetchone(self) -> Any:
        return self._next

    def fetchall(self) -> Any:
        return []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _conn(answers: dict[str, Any] | None = None):
    cur = FakeCursor(answers)
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur
    return conn


def _sql(conn, needle: str) -> str:
    hits = [s for s, _ in conn.cur.executed if needle in s]
    assert hits, f"no statement matching {needle!r}"
    return hits[0]


def test_partitioned_check_binds_the_parent_by_search_path() -> None:
    """The outage shape: `relname = 'queue'` matches a queue in ANY schema, so
    cryo being migrated would make cortex's migration report success and no-op."""
    conn = _conn()
    is_queue_partitioned(conn)
    sql = _sql(conn, "pg_partitioned_table")
    assert "to_regclass('queue')" in sql
    assert "relname = 'queue'" not in sql


def test_queue_old_check_binds_by_search_path() -> None:
    """A schema-blind guard gating a DROP that resolves via search_path: the
    guard could answer about another schema's table and let the DROP through."""
    conn = _conn({"COUNT(*)": (0,)})
    drop_old_queue_table(conn, dry_run=True)
    sql = _sql(conn, "queue_old")
    assert "to_regclass('queue_old')" in sql
    assert "relname = 'queue_old'" not in sql


def _dry_run(max_date: date, min_date: date | None = None) -> dict[str, Any]:
    conn = _conn()
    result = {"total_rows": 5, "min_date": min_date or max_date, "max_date": max_date}
    import cortex_utils.queue.migrate as m

    original = m.analyze_existing_queue
    m.analyze_existing_queue = lambda c: {**result, "status_counts": {}}  # type: ignore[assignment]
    try:
        return migrate_to_partitioned(conn, days_ahead=7, dry_run=True)
    finally:
        m.analyze_existing_queue = original  # type: ignore[assignment]


def test_the_window_always_reaches_past_today() -> None:
    """A quiet queue whose newest row is old would otherwise get a range ending
    before go-live: partitions created, none covering NOW(), success reported."""
    out = _dry_run(max_date=SERVER_TODAY - timedelta(days=60))
    last = out["partition_range"].split(" to ")[1]
    assert last == f"queue_{(SERVER_TODAY + timedelta(days=7)).strftime('%Y_%m_%d')}"


def test_a_busy_queue_still_extends_past_its_newest_row() -> None:
    """The clamp must not cut the window short in the other direction: rows
    dated ahead of today (a clock skew, a backfill) still need partitions."""
    ahead = SERVER_TODAY + timedelta(days=3)
    out = _dry_run(max_date=ahead, min_date=SERVER_TODAY)
    last = out["partition_range"].split(" to ")[1]
    assert last == f"queue_{(ahead + timedelta(days=7)).strftime('%Y_%m_%d')}"


def test_an_empty_queue_is_anchored_on_the_server_clock() -> None:
    conn = _conn()
    import cortex_utils.queue.migrate as m

    original = m.analyze_existing_queue
    m.analyze_existing_queue = lambda c: {  # type: ignore[assignment]
        "total_rows": 0,
        "min_date": None,
        "max_date": None,
        "status_counts": {},
    }
    try:
        out = migrate_to_partitioned(conn, days_ahead=7, dry_run=True)
    finally:
        m.analyze_existing_queue = original  # type: ignore[assignment]

    assert out["partition_range"].startswith(f"queue_{SERVER_TODAY.strftime('%Y_%m_%d')}")
    assert any("CURRENT_DATE" in s for s, _ in conn.cur.executed), (
        "the anchor must be the server's date, not this process's"
    )


def test_a_preview_does_not_drop_the_migration_rollback_path() -> None:
    """queue_old is the migration's only way back. The guard above returns
    early on a missing table, so nothing was executing this branch at all --
    turning `if dry_run:` into a no-op silently destroys the rollback path."""
    conn = _conn({"COUNT(*)": (12,)})
    assert drop_old_queue_table(conn, dry_run=True) is True
    assert not any("DROP TABLE" in s for s, _ in conn.cur.executed)
    conn.commit.assert_not_called()


def test_dropping_queue_old_for_real_commits() -> None:
    conn = _conn()
    assert drop_old_queue_table(conn, dry_run=False) is True
    assert any("DROP TABLE queue_old" in s for s, _ in conn.cur.executed)
    conn.commit.assert_called_once()


def test_a_missing_queue_old_is_not_reported_as_dropped() -> None:
    conn = _conn({"to_regclass('queue_old')": None})
    assert drop_old_queue_table(conn) is False
    assert not any("DROP TABLE" in s for s, _ in conn.cur.executed)


def _real_run(answers: dict[str, Any]) -> Any:
    """migrate_to_partitioned with dry_run=False. Nothing exercised this path."""
    conn = _conn(answers)
    import cortex_utils.queue.migrate as m

    original = m.analyze_existing_queue
    answers.setdefault("COUNT(*) FROM queue_new", (4,))  # the verification step
    m.analyze_existing_queue = lambda c: {  # type: ignore[assignment]
        "total_rows": 4,
        "min_date": SERVER_TODAY,
        "max_date": SERVER_TODAY,
        "status_counts": {},
    }
    try:
        m.migrate_to_partitioned(conn, days_ahead=1, dry_run=False)
    finally:
        m.analyze_existing_queue = original  # type: ignore[assignment]
    return conn


def test_the_sequence_is_reseeded_by_asking_the_table_which_one_it_uses() -> None:
    """queue_new is BIGSERIAL, so it owns queue_new_id_seq -- and ALTER TABLE
    ... RENAME TO does not rename an owned sequence. Naming queue_id_seq here
    advanced the sequence now owned by queue_old while the live table kept
    handing out ids from 1. Nothing raises: partitioning forces created_at into
    the primary key, so the collisions are legal and the migration returns
    success with id no longer unique -- which complete(), release() and
    fail_or_retry() all rely on when they address a row by id.
    """
    conn = _real_run({"pg_get_serial_sequence": ("public.queue_new_id_seq",), "MAX(id)": (41,)})

    setval = [(s, p) for s, p in conn.cur.executed if "setval" in s][0]
    assert "pg_get_serial_sequence" in _sql(conn, "pg_get_serial_sequence")
    assert setval[1] == ("public.queue_new_id_seq", 42), (
        "must reseed the sequence the table reports, not a hardcoded name"
    )
    assert "queue_id_seq" not in setval[0], "the name must be bound, not interpolated"


def test_a_table_with_no_owned_sequence_fails_loudly() -> None:
    """Returning success with an unreseeded sequence is the silent version of
    the same bug."""
    with pytest.raises(RuntimeError, match="no owned sequence"):
        _real_run({"pg_get_serial_sequence": (None,), "MAX(id)": (41,)})
