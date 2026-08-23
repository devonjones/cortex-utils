"""Tests for reading the queue.

The SQL itself was verified against the live two-schema database (both report
their own partition headroom independently); these pin the properties that make
the API worth having, which is where regressions would be silent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from cortex_utils.queue.inspect import (
    Failure,
    QueueHealth,
    failures,
    health,
    resubmit,
    stuck,
)
from cortex_utils.queue.ops import SELF_HEALED_MARKER, QueueError

NOW = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)

HEALTH_ROW = (
    [
        {
            "queue_name": "triage",
            "ready": 2,
            "deferred": 4,
            "processing": 1,
            "failed": 3,
            "oldest_ready_age_s": 900.0,
        }
    ],
    7,
    True,
    5,
    0,
    NOW,
)


class FakeCursor:
    def __init__(self, fetchone: Any = None, fetchall: Any = ()):
        self.executed: list[tuple[str, Any]] = []
        self.rowcount = 1
        self._fetchone = fetchone
        self._fetchall = list(fetchall)

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> Any:
        return self._fetchone

    def fetchall(self) -> Any:
        return self._fetchall

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _conn(**kw: Any):
    cur = FakeCursor(**kw)
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur
    return conn


# --- health -----------------------------------------------------------------


def test_health_is_one_round_trip() -> None:
    """A card needing five queries will either be slow or be written wrong."""
    conn = _conn(fetchone=HEALTH_ROW)
    health(conn)
    assert len(conn.cur.executed) == 1


def test_health_reports_ready_and_deferred_separately() -> None:
    """Collapsing them is actively misleading: six rows all backing off for an
    hour read as a backlog when they are a retry storm."""
    result = health(_conn(fetchone=HEALTH_ROW))
    depth = result.depths[0]
    assert (depth.ready, depth.deferred) == (2, 4)


def test_health_distinguishes_ready_from_merely_pending_in_sql() -> None:
    conn = _conn(fetchone=HEALTH_ROW)
    health(conn)
    sql = conn.cur.executed[0][0]
    assert "next_attempt_at IS NULL OR next_attempt_at <= NOW()" in sql
    assert "next_attempt_at > NOW()" in sql, "deferred must be counted apart"


def test_health_reports_how_far_behind_the_oldest_ready_work_is() -> None:
    """Counts cannot separate 'four arrived this minute' from 'four stuck since
    Tuesday', and those need different responses."""
    assert health(_conn(fetchone=HEALTH_ROW)).depths[0].oldest_ready_age_s == 900.0


def test_health_binds_the_queue_through_search_path() -> None:
    """A bare relname lookup reports healthy off the other schema's partitions."""
    conn = _conn(fetchone=HEALTH_ROW)
    health(conn)
    sql = conn.cur.executed[0][0]
    assert "to_regclass('queue')" in sql
    assert "relname = 'queue'" not in sql


def test_health_counts_self_healed_partitions() -> None:
    """Non-zero means maintenance is dead. A log line is the channel that
    already failed to surface a two-day outage; this is countable."""
    conn = _conn(fetchone=HEALTH_ROW)
    health(conn)
    sql, params = conn.cur.executed[0]
    assert "obj_description" in sql
    assert params == {"marker": SELF_HEALED_MARKER}


def test_health_takes_its_timestamp_from_the_server() -> None:
    conn = _conn(fetchone=HEALTH_ROW)
    assert health(conn).server_time == NOW
    assert "NOW()" in conn.cur.executed[0][0]


@pytest.mark.parametrize(
    "headroom,healed,expected",
    [(5, 0, True), (0, 0, False), (5, 2, False), (None, 0, False)],
)
def test_is_healthy_covers_both_ways_the_queue_dies(headroom, healed, expected) -> None:
    """Out of partitions, or limping on write-path self-heals."""
    h = QueueHealth(
        depths=[],
        dead_letter=0,
        partitioned=True,
        partition_headroom_days=headroom,
        self_healed_partitions=healed,
        server_time=NOW,
    )
    assert h.is_healthy is expected


# --- stuck ------------------------------------------------------------------


def test_stuck_measures_the_window_on_the_server() -> None:
    conn = _conn(fetchall=[])
    stuck(conn, visibility_timeout_min=9)
    sql, params = conn.cur.executed[0]
    assert "claimed_at < NOW() - (INTERVAL '1 minute' * %s)" in sql
    assert params[0] == 9


def test_stuck_reports_who_holds_the_claim() -> None:
    """The difference between 'a worker is chewing on it' and 'a worker died
    holding it'."""
    conn = _conn(fetchall=[(1, "triage", "worker-a", NOW, 120.0, 2)])
    job = stuck(conn)[0]
    assert job.claimed_by == "worker-a"
    assert job.stuck_for_s == 120.0


# --- failures ---------------------------------------------------------------


def test_failures_return_the_error_text_intact() -> None:
    """Fourteen rows all reading the same error is what proved an outage was
    infrastructure rather than fourteen content failures. Summarising loses that.
    """
    text = "visibility timeout, attempts exhausted"
    conn = _conn(fetchall=[(1, "triage", {"a": 1}, 3, 3, text, NOW)])
    got = failures(conn)[0]
    assert isinstance(got, Failure)
    assert got.last_error == text


def test_failures_can_be_scoped_to_one_queue() -> None:
    conn = _conn(fetchall=[])
    failures(conn, limit=10, queue_name="triage")
    _, params = conn.cur.executed[0]
    assert params == ("triage", "triage", 10)


def test_failures_are_newest_first() -> None:
    conn = _conn(fetchall=[])
    failures(conn)
    assert "ORDER BY created_at DESC" in conn.cur.executed[0][0]


# --- resubmit ---------------------------------------------------------------


# The failed row's own created_at, deliberately not today's: a partitioned row
# outlives the day it was made, and the cancel must carry the row's own value.
ROW_CREATED_AT = datetime(2026, 2, 27, 9, 30, tzinfo=UTC)


def _resubmit_conn(new_id: int | None = 99):
    """A failed row exists; the enqueue returns new_id."""
    cur = FakeCursor()
    answers = [
        ("triage", {"gmail_id": "abc"}, 0, ROW_CREATED_AT),
        (new_id,) if new_id else None,
    ]

    def fetchone() -> Any:
        return answers.pop(0) if answers else None

    cur.fetchone = fetchone  # type: ignore[method-assign]
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur
    return conn


def test_resubmit_creates_a_new_row_rather_than_reviving_the_old_one() -> None:
    """The obvious implementation is wrong: a revived row keeps its created_at,
    so it stays in an old partition that retention drops on age -- it would be
    on a clock nobody intended and could vanish mid-flight.
    """
    conn = _resubmit_conn()
    assert resubmit(conn, 7) == 99
    sql = "\n".join(s for s, _ in conn.cur.executed)
    assert "INSERT INTO queue" in sql, "must enqueue fresh, landing in today's partition"
    assert "SET status = 'pending'" not in sql, "must not flip the failed row back"


def test_resubmit_cancels_rather_than_deletes_the_original() -> None:
    """A failure list whose entries vanish when someone retries them defeats
    the purpose."""
    conn = _resubmit_conn()
    resubmit(conn, 7)
    sql = "\n".join(s for s, _ in conn.cur.executed)
    assert "status = 'cancelled'" in sql
    assert "DELETE" not in sql


def test_resubmit_lands_both_halves_in_one_transaction() -> None:
    """A crash between them would leave the work re-queued and the original
    still showing failed."""
    conn = _resubmit_conn()
    resubmit(conn, 7)
    assert conn.commit.call_count == 1


def test_resubmit_refuses_a_row_that_is_not_failed() -> None:
    cur = FakeCursor(fetchone=None)
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur
    with pytest.raises(QueueError):
        resubmit(conn, 7)


def test_resubmit_records_a_dedup_in_the_original() -> None:
    conn = _resubmit_conn(new_id=None)
    assert resubmit(conn, 7, dedup_key="gmail_id") is None
    note = [p for s, p in conn.cur.executed if "cancelled" in s][0]
    assert "deduped" in note[0]


def test_resubmit_cancels_by_the_whole_primary_key() -> None:
    """(id, created_at) is the declared key, and this reads then writes -- the
    same shape fail_or_retry carries created_at for. Addressing by id alone
    would let a row in another partition take the cancel."""
    conn = _resubmit_conn()
    resubmit(conn, 5)
    sql, params = [(s, p) for s, p in conn.cur.executed if "SET status = 'cancelled'" in s][0]
    assert "WHERE id = %s AND created_at = %s" in sql
    assert params[1:] == (5, ROW_CREATED_AT)
    select = [s for s, _ in conn.cur.executed if "FOR UPDATE" in s][0]
    assert "created_at" in select, "the value must come from the locked row"


def test_a_cancel_that_hit_no_row_rolls_back_the_new_one() -> None:
    """We hold the row under FOR UPDATE, so a zero here is a bug, not a race --
    and reporting success would leave the work queued twice."""
    conn = _resubmit_conn()
    conn.cur.rowcount = 0
    with pytest.raises(QueueError, match="rolled back"):
        resubmit(conn, 5)
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_every_scalar_lands_in_its_own_field() -> None:
    """Three of the five fields were never read off a health() return, so the
    SELECT-list-to-unpack correspondence was free to drift. HEALTH_ROW already
    carries distinct values; asserting all of them costs one line."""
    got = health(_conn(fetchone=HEALTH_ROW))
    assert (got.dead_letter, got.partition_headroom_days, got.self_healed_partitions) == (
        7,
        5,
        0,
    )
    assert got.server_time == NOW
    assert got.depths[0].queue_name == "triage"


def test_resubmit_carries_the_original_priority_and_the_dedup_key() -> None:
    """Dropping either from the enqueue call is invisible: the job still
    re-queues, just at the wrong priority or without the duplicate suppression
    the caller asked for."""
    conn = _resubmit_conn()
    resubmit(conn, 5, dedup_key="gmail_id")
    insert = [(s, p) for s, p in conn.cur.executed if "INSERT INTO queue" in s][0]
    assert insert[1][0] == "triage"
    assert 0 in insert[1], "the original priority"
    assert any("gmail_id" in str(p) for _, p in conn.cur.executed), "the dedup key"
