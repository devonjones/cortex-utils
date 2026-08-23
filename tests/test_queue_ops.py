"""Tests for the shared queue primitives.

Three properties carry the weight here, because they are exactly what drifted
when cortex and cryo each kept their own copy:

- expiry never consumes an attempt (cryo's did; it cost four healthy videos)
- every report is claim-token matched (cortex had no token at all)
- dedup is success, not failure (callers branch on it)

The suite has no live Postgres, so these drive a mock cursor and assert on the
SQL and parameters issued. Behaviour against a real server is covered by cryo's
selfcheck container and, for cortex, by the workers once migrated.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import psycopg2
import pytest

from cortex_utils.queue.ops import (
    QueueError,
    claim,
    complete,
    enqueue,
    fail_or_retry,
    release,
)


def _conn(fetchone=None, fetchall=(), rowcount=1):
    """Connection whose cursor records SQL and returns canned rows."""
    executed: list[tuple[str, object]] = []
    cur = MagicMock()
    cur.execute.side_effect = lambda sql, params=None: executed.append((sql, params))
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = list(fetchall)
    cur.rowcount = rowcount
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.executed = executed
    return conn


def _sql(conn) -> str:
    return "\n".join(s for s, _ in conn.executed)


# --- expiry must not consume an attempt ------------------------------------


def test_stale_recovery_does_not_consume_an_attempt() -> None:
    """The property cryo lacked: an outage costs latency, never work.

    A worker killed by an expired token has proven nothing about the job.
    """
    conn = _conn(fetchall=[])
    claim(conn, "triage")
    sql = _sql(conn)
    reset = sql.split("retire_exhausted")[0]
    assert "status = 'pending'" in reset
    assert "attempts" not in reset.split("SET")[1].split("WHERE")[0], (
        "stale recovery must not touch attempts"
    )


def test_claim_retires_only_rows_that_already_spent_their_attempts() -> None:
    conn = _conn(fetchall=[])
    claim(conn, "triage")
    sql = _sql(conn)
    assert "attempts >= max_attempts" in sql  # retire
    assert "attempts < max_attempts" in sql  # recover


# --- claim-token matching ---------------------------------------------------


@pytest.mark.parametrize("call", [complete, lambda c, j, w: release(c, j, 30, w)])
def test_reports_are_claim_token_matched(call) -> None:
    """A worker that stalled past its timeout must not report on a re-claimed row."""
    conn = _conn(rowcount=1)
    call(conn, 7, "worker-a")
    sql = _sql(conn)
    assert "claimed_by IS NOT DISTINCT FROM" in sql
    assert "status = 'processing'" in sql


def test_complete_returns_false_when_the_claim_moved_on() -> None:
    conn = _conn(rowcount=0)
    assert complete(conn, 7, "worker-a") is False


def test_release_returns_false_when_the_claim_moved_on() -> None:
    conn = _conn(rowcount=0)
    assert release(conn, 7, 30, "worker-a") is False


def test_fail_or_retry_reports_stale_rather_than_charging_another_worker() -> None:
    conn = _conn(fetchone=None)
    assert fail_or_retry(conn, 7, "boom", "worker-a") == "stale"
    conn.rollback.assert_called_once()


# --- release vs fail_or_retry ----------------------------------------------


def test_release_defers_without_touching_attempts() -> None:
    conn = _conn(rowcount=1)
    release(conn, 7, 120, "worker-a")
    sql = _sql(conn)
    assert "next_attempt_at" in sql
    assert "attempts" not in sql, "release must not spend the attempt budget"


def test_fail_or_retry_charges_an_attempt_and_reschedules() -> None:
    conn = _conn(fetchone=(0, 3))
    assert fail_or_retry(conn, 7, "boom", "worker-a") == "pending"
    update = [(s, p) for s, p in conn.executed if "SET status = 'pending'" in s]
    assert len(update) == 1
    sql, params = update[0]
    assert params[0] == 1, "attempts must go 0 -> 1"
    assert params[-1] == 7, "must update the job it was given"
    assert "next_attempt_at" in sql


def test_fail_or_retry_retires_on_the_last_attempt() -> None:
    conn = _conn(fetchone=(2, 3))
    assert fail_or_retry(conn, 7, "boom", "worker-a") == "failed"
    assert "status = 'failed'" in _sql(conn)


# --- enqueue ----------------------------------------------------------------


def test_enqueue_returns_the_new_id() -> None:
    conn = _conn(fetchone=(42,))
    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 42


def test_dedup_returns_none_not_an_error() -> None:
    """None means the work is already covered; callers branch on it."""
    conn = _conn(fetchone=None)
    assert enqueue(conn, "triage", {"gmail_id": "abc"}, dedup_key="gmail_id") is None


def test_dedup_serialises_producers_on_an_advisory_lock() -> None:
    """Unique indexes cannot backstop this: created_at is in every unique key."""
    conn = _conn(fetchone=(42,))
    enqueue(conn, "triage", {"gmail_id": "abc"}, dedup_key="gmail_id")
    assert "pg_advisory_xact_lock" in _sql(conn)


def test_dedup_key_absent_from_payload_is_rejected() -> None:
    conn = _conn(fetchone=(42,))
    with pytest.raises(QueueError):
        enqueue(conn, "triage", {"other": 1}, dedup_key="gmail_id")


# --- partition self-heal ----------------------------------------------------


def _conn_raising_once(exc: Exception):
    """Raise on the first INSERT, succeed afterwards."""
    state = {"raised": False}
    executed: list[str] = []
    cur = MagicMock()

    def _execute(sql, params=None):
        executed.append(sql)
        if "INSERT INTO queue" in sql and not state["raised"]:
            state["raised"] = True
            raise exc

    cur.execute.side_effect = _execute
    cur.fetchone.return_value = (99,)
    cur.rowcount = 1
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.executed = executed
    return conn


def test_missing_partition_is_created_and_the_insert_retried_once() -> None:
    exc = psycopg2.errors.CheckViolation('no partition of relation "queue" found for row')
    conn = _conn_raising_once(exc)
    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 99
    sql = "\n".join(conn.executed)
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert f"queue_{date.today().strftime('%Y_%m_%d')}" in sql
    assert sql.count("INSERT INTO queue") == 2, "exactly one retry"


def test_other_check_violations_are_not_mistaken_for_a_missing_partition() -> None:
    """queue_new_valid_status raises the same SQLSTATE.

    Creating a partition for a bad status value would invent work and misreport
    the cause.
    """
    exc = psycopg2.errors.CheckViolation(
        'new row for relation "queue" violates check constraint "queue_new_valid_status"'
    )
    conn = _conn_raising_once(exc)
    with pytest.raises(psycopg2.errors.CheckViolation):
        enqueue(conn, "triage", {"gmail_id": "abc"})
    assert "CREATE TABLE" not in "\n".join(conn.executed)
