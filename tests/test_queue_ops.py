"""Tests for the shared queue primitives.

Three properties carry the weight, because they are exactly what drifted when
cortex and cryo each kept their own copy:

- expiry never consumes an attempt (cryo's did; it cost four healthy videos)
- every report is claim-token matched (cortex had no token at all)
- dedup is success, not failure (callers branch on it)

These assert on the parameters passed to the driver wherever possible rather
than on SQL substrings. A round of mutation testing found substring assertions
survived 23 of 35 mutations -- they pin the text of a statement, not what it
does. The suite has no live Postgres, so the concurrency guarantees themselves
(advisory-lock serialisation, SKIP LOCKED, the partition-creation race) are
argued in review and not covered here; ops.py has no live-server coverage today.
"""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import psycopg2
import pytest

from cortex_utils.queue.ops import (
    QueueError,
    claim,
    complete,
    enqueue,
    ensure_claim_token_column,
    fail_or_retry,
    has_claim_token_column,
    release,
)

WORKER = "worker-a"


class FakeCursor:
    """Records (sql, params) and replays canned results."""

    def __init__(self, fetchone: Any = None, fetchall: Any = (), rowcount: int = 1):
        self.executed: list[tuple[str, Any]] = []
        self._fetchone = fetchone
        self._fetchall = list(fetchall)
        self.rowcount = rowcount
        self.raise_on: str | None = None
        self.error: Exception | None = None

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        if self.raise_on and self.raise_on in sql and self.error:
            raise self.error

    def fetchone(self) -> Any:
        return self._fetchone() if callable(self._fetchone) else self._fetchone

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


def _params_of(conn, needle: str) -> Any:
    """Params of the one executed statement containing `needle`."""
    hits = [p for s, p in conn.cur.executed if needle in s]
    assert len(hits) == 1, f"expected exactly one statement matching {needle!r}"
    return hits[0]


# --- expiry must not consume an attempt ------------------------------------


def test_stale_recovery_does_not_consume_an_attempt() -> None:
    """The property cryo lacked: an outage costs latency, never work."""
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER)
    sql = conn.cur.executed[0][0]
    reset = sql.split("retire_exhausted")[0]
    body = reset.split("SET", 1)[1].split("WHERE", 1)[0]
    assert "attempts" not in body, "stale recovery must not write attempts"
    assert "status = 'pending'" in body


def test_claim_recovers_under_budget_and_retires_only_exhausted() -> None:
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER)
    sql = conn.cur.executed[0][0]
    reset = sql.split("retire_exhausted")[0]
    retire = sql.split("retire_exhausted")[1].split("claimable")[0]
    assert "attempts < max_attempts" in reset
    assert "attempts >= max_attempts" in retire
    assert "status = 'failed'" in retire, "exhausted rows must be retired, not completed"


def test_claim_orders_by_priority_and_skips_locked() -> None:
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER)
    sql = conn.cur.executed[0][0]
    assert "ORDER BY priority DESC, created_at" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    # Partitioned tables key on (id, created_at); joining on id alone is wrong.
    assert "q.created_at = c.created_at" in sql


def test_claim_returns_the_worker_facing_job_shape() -> None:
    conn = _conn(fetchall=[(7, "triage", {"gmail_id": "abc"}, 2, 5)])
    jobs = claim(conn, "triage", WORKER)
    assert jobs == [
        {
            "id": 7,
            "queue_name": "triage",
            "payload": {"gmail_id": "abc"},
            "attempts": 2,
            "priority": 5,
        }
    ]


def test_claim_passes_the_worker_as_the_token() -> None:
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER, limit=3, visibility_timeout_min=9)
    params = _params_of(conn, "reset_stale")
    assert params["w"] == WORKER
    assert params["lim"] == 3
    assert params["vis"] == 9


# --- claim tokens -----------------------------------------------------------


def test_worker_is_required_on_claim() -> None:
    """A shared default would make every claimant anonymous and the token moot."""
    conn = _conn(fetchall=[])
    with pytest.raises(QueueError):
        claim(conn, "triage", "")


@pytest.mark.parametrize(
    "call,needle",
    [
        (lambda c: complete(c, 7, WORKER), "completed"),
        (lambda c: release(c, 7, 30, WORKER), "next_attempt_at"),
    ],
)
def test_reports_match_the_claim_token(call, needle: str) -> None:
    conn = _conn(rowcount=1)
    call(conn)
    sql, params = [(s, p) for s, p in conn.cur.executed if needle in s][0]
    assert "claimed_by = %s" in sql
    assert WORKER in params, "the worker token must be bound into the statement"
    assert "status = 'processing'" in sql


def test_fail_or_retry_matches_the_claim_token() -> None:
    """The report that actually spends the budget must be token-matched too."""
    conn = _conn(fetchone=(0, 3))
    fail_or_retry(conn, 7, "boom", WORKER)
    sql, params = conn.cur.executed[0]
    assert "SELECT attempts" in sql
    assert "claimed_by = %s" in sql
    assert params == (7, WORKER)


def test_complete_returns_false_when_the_claim_moved_on() -> None:
    assert complete(_conn(rowcount=0), 7, WORKER) is False


def test_release_returns_false_when_the_claim_moved_on() -> None:
    assert release(_conn(rowcount=0), 7, 30, WORKER) is False


def test_complete_returns_true_when_the_claim_held() -> None:
    assert complete(_conn(rowcount=1), 7, WORKER) is True


def test_release_returns_true_when_the_claim_held() -> None:
    assert release(_conn(rowcount=1), 7, 30, WORKER) is True


def test_fail_or_retry_reports_stale_rather_than_charging_another_worker() -> None:
    conn = _conn(fetchone=None)
    assert fail_or_retry(conn, 7, "boom", WORKER) == "stale"


# --- release vs fail_or_retry ----------------------------------------------


def test_release_defers_and_clears_the_token_without_charging() -> None:
    conn = _conn(rowcount=1)
    release(conn, 7, 120, WORKER)
    sql, params = conn.cur.executed[0]
    assert "attempts" not in sql, "release must not spend the attempt budget"
    assert "claimed_by = NULL" in sql, "a released row must not keep our token"
    assert params == (120, 7, WORKER)


def test_fail_or_retry_charges_an_attempt_and_backs_off() -> None:
    conn = _conn(fetchone=(0, 3))
    assert fail_or_retry(conn, 7, "boom", WORKER) == "pending"
    params = _params_of(conn, "SET status = 'pending'")
    assert params[0] == 1, "attempts must go 0 -> 1"
    assert params[1] == "boom"
    assert params[2] > 0, "a retry must be delayed"
    assert params[3] == 7


def test_fail_or_retry_retires_on_the_last_attempt() -> None:
    conn = _conn(fetchone=(2, 3))
    assert fail_or_retry(conn, 7, "boom", WORKER) == "failed"
    params = _params_of(conn, "SET status = 'failed'")
    assert params == (3, "boom", 7)


def test_fail_or_retry_truncates_a_huge_error() -> None:
    conn = _conn(fetchone=(0, 3))
    fail_or_retry(conn, 7, "x" * 9000, WORKER)
    assert len(_params_of(conn, "SET status = 'pending'")[1]) == 2000


# --- enqueue ----------------------------------------------------------------


def test_enqueue_returns_the_new_id() -> None:
    conn = _conn(fetchone=(42,))
    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 42
    assert _params_of(conn, "INSERT INTO queue")[0] == "triage"


def test_dedup_returns_none_not_an_error() -> None:
    """None means the work is already covered; callers branch on it."""
    conn = _conn(fetchone=None)
    assert enqueue(conn, "triage", {"gmail_id": "abc"}, dedup_key="gmail_id") is None


def test_dedup_checks_the_named_field_for_the_right_value() -> None:
    conn = _conn(fetchone=(42,))
    enqueue(conn, "triage", {"gmail_id": "abc"}, dedup_key="gmail_id")
    sql, params = [(s, p) for s, p in conn.cur.executed if "NOT EXISTS" in s][0]
    assert "payload->>%s" in sql, "the field must be bound, not spliced"
    assert params[-2:] == ("gmail_id", "abc")


def test_dedup_lock_key_separates_queues_and_fields() -> None:
    """Keying on the value alone would serialise unrelated queues."""
    conn = _conn(fetchone=(42,))
    enqueue(conn, "triage", {"gmail_id": "abc"}, dedup_key="gmail_id")
    key = _params_of(conn, "pg_advisory_xact_lock")[0]
    assert key == "triage:gmail_id:abc"


def test_dedup_key_absent_from_payload_is_rejected() -> None:
    with pytest.raises(QueueError):
        enqueue(_conn(fetchone=(42,)), "triage", {"other": 1}, dedup_key="gmail_id")


def test_dedup_key_must_be_an_identifier() -> None:
    """It reaches the SQL text, so anything else is an injection point."""
    payload = {"x'; DROP TABLE queue; --": 1}
    with pytest.raises(QueueError):
        enqueue(_conn(fetchone=(42,)), "triage", payload, dedup_key="x'; DROP TABLE queue; --")


@pytest.mark.parametrize("value", [True, 1.5, {"a": 1}, ["a"], None])
def test_dedup_value_types_that_would_silently_never_match_are_rejected(value) -> None:
    """Python str() and Postgres jsonb ->> disagree for these.

    bool gives "True" vs "true"; a mismatch would make dedup never match and
    double-queue instead of erroring.
    """
    with pytest.raises(QueueError):
        enqueue(_conn(fetchone=(42,)), "triage", {"k": value}, dedup_key="k")


def test_dedup_accepts_int_values() -> None:
    conn = _conn(fetchone=(42,))
    assert enqueue(conn, "triage", {"k": 7}, dedup_key="k") == 42
    assert _params_of(conn, "pg_advisory_xact_lock")[0] == "triage:k:7"


# --- partition self-heal ----------------------------------------------------


class _Violation(psycopg2.errors.CheckViolation):
    """A real CheckViolation whose diag we can set.

    psycopg2 exposes Diagnostics.constraint_name read-only, but the except
    clause under test matches on the exception type, so the double must still
    genuinely be a CheckViolation.
    """

    def __init__(self, message: str, constraint_name: str | None):
        super().__init__(message)
        self._constraint_name = constraint_name

    @property
    def diag(self) -> Any:  # type: ignore[override]
        return SimpleNamespace(constraint_name=self._constraint_name)


def _missing_partition() -> _Violation:
    return _Violation('no partition of relation "queue" found for row', None)


def _conn_failing_first_insert(exc: Exception):
    """Raise on the first INSERT, succeed afterwards."""
    state = {"raised": False}
    cur = FakeCursor(fetchone=(99,))
    real_execute = cur.execute

    def execute(sql: str, params: Any = None) -> None:
        real_execute(sql, params)
        if "INSERT INTO queue" in sql and not state["raised"]:
            state["raised"] = True
            raise exc

    cur.execute = execute  # type: ignore[method-assign]
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur
    return conn


def test_missing_partition_is_created_and_the_insert_retried_once() -> None:
    conn = _conn_failing_first_insert(_missing_partition())
    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 99
    sql = "\n".join(s for s, _ in conn.cur.executed)
    assert f"queue_{date.today().strftime('%Y_%m_%d')}" in sql
    assert sql.count("INSERT INTO queue") == 2, "exactly one retry"
    conn.rollback.assert_called()


def test_a_named_check_constraint_is_not_treated_as_a_missing_partition() -> None:
    """queue_new_valid_status raises the same SQLSTATE.

    Creating a partition for a bad status would invent work and misreport the
    cause. A genuine violation names its constraint; a routing failure does not.
    """
    exc = _Violation("violates check constraint", "queue_new_valid_status")
    conn = _conn_failing_first_insert(exc)
    with pytest.raises(psycopg2.errors.CheckViolation):
        enqueue(conn, "triage", {"gmail_id": "abc"})
    assert "CREATE TABLE" not in "\n".join(s for s, _ in conn.cur.executed)


# --- transaction hygiene ----------------------------------------------------


def test_a_failed_statement_rolls_back_before_raising() -> None:
    """Otherwise the caller's NEXT statement fails complaining about this one."""
    conn = _conn(fetchall=[])
    conn.cur.raise_on = "reset_stale"
    conn.cur.error = psycopg2.OperationalError("boom")
    with pytest.raises(psycopg2.OperationalError):
        claim(conn, "triage", WORKER)
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


# --- schema migration -------------------------------------------------------


def test_claim_token_column_is_not_re_added_when_present() -> None:
    """The ALTER takes ACCESS EXCLUSIVE on a hot partitioned parent."""
    conn = _conn(fetchone=(1,))
    assert ensure_claim_token_column(conn) is False
    assert not [s for s, _ in conn.cur.executed if "ALTER TABLE" in s]


def test_claim_token_column_is_added_when_absent_under_a_lock_timeout() -> None:
    conn = _conn(fetchone=None)
    assert ensure_claim_token_column(conn) is True
    sql = "\n".join(s for s, _ in conn.cur.executed)
    assert "ADD COLUMN IF NOT EXISTS claimed_by" in sql
    assert "lock_timeout" in sql, "a boot must fail fast, not wedge the pipeline"


# --- package surface --------------------------------------------------------


def test_package_fail_or_retry_is_the_claim_token_aware_one() -> None:
    """Importing claim+fail_or_retry together must not pair new with legacy."""
    from cortex_utils import queue as pkg

    assert pkg.fail_or_retry is fail_or_retry


# --- the transaction actually completes -------------------------------------


@pytest.mark.parametrize(
    "call",
    [
        lambda c: enqueue(c, "triage", {"gmail_id": "abc"}),
        lambda c: claim(c, "triage", WORKER),
        lambda c: complete(c, 7, WORKER),
        lambda c: release(c, 7, 30, WORKER),
        lambda c: ensure_claim_token_column(c),
    ],
)
def test_every_operation_commits(call) -> None:
    """Deleting conn.commit() must not pass.

    Only the rollback half was pinned before, so a module that never committed
    anything looked healthy: every write silently rolled back.
    """
    conn = _conn(fetchone=(1,), fetchall=[])
    call(conn)
    conn.commit.assert_called()


def test_fail_or_retry_commits_its_report() -> None:
    conn = _conn(fetchone=(0, 3))
    fail_or_retry(conn, 7, "boom", WORKER)
    conn.commit.assert_called()


def test_read_only_precheck_does_not_leave_a_transaction_open() -> None:
    """psycopg2 opens a transaction even for a SELECT."""
    conn = _conn(fetchone=(1,))
    has_claim_token_column(conn)
    assert conn.commit.called or conn.rollback.called


# --- clauses that consume the asserted parameters ---------------------------


def test_claim_writes_the_token_and_honours_its_limits() -> None:
    """A param can be bound and then not used; assert the clause too."""
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER, limit=3, visibility_timeout_min=9)
    sql = conn.cur.executed[0][0]
    assert "claimed_by = %(w)s" in sql, "the token must actually be written"
    assert "LIMIT %(lim)s" in sql, "limit must be bound, not hardcoded"
    assert "INTERVAL '1 minute' * %(vis)s" in sql, "timeout must be bound"


def test_claim_only_takes_pending_rows_that_are_ready() -> None:
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER)
    claimable = conn.cur.executed[0][0].split("claimable AS")[1]
    assert "status = 'pending'" in claimable
    assert "next_attempt_at <= statement_timestamp()" in claimable, "deferral must be honoured"
    assert "FOR UPDATE SKIP LOCKED" in claimable


def test_stale_recovery_is_scoped_to_this_queue() -> None:
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER)
    sql = conn.cur.executed[0][0]
    reset = sql.split("retire_exhausted")[0]
    retire = sql.split("retire_exhausted")[1].split("claimable")[0]
    assert "queue_name = %(q)s" in reset, "must not recover another queue's rows"
    assert "queue_name = %(q)s" in retire


def test_dedup_predicate_is_scoped_to_queue_and_live_rows() -> None:
    """Dropping queue_name would make dedup global: one enqueue suppressing the
    same id in every other queue, silently, while returning success."""
    conn = _conn(fetchone=(42,))
    enqueue(conn, "triage", {"gmail_id": "abc"}, dedup_key="gmail_id")
    where = [s for s, _ in conn.cur.executed if "NOT EXISTS" in s][0].split("NOT EXISTS")[1]
    assert "queue_name = %s" in where
    assert (
        "status IN ('pending', 'processing')" in where
    ), "completed rows must not suppress a replay"


@pytest.mark.parametrize("dedup_key", [None, "gmail_id"])
def test_priority_is_carried_on_both_insert_paths(dedup_key) -> None:
    """CLAUDE.md fixes -100 for backfill; hardcoding 0 would erase that."""
    conn = _conn(fetchone=(42,))
    enqueue(conn, "triage", {"gmail_id": "abc"}, priority=-100, dedup_key=dedup_key)
    params = _params_of(conn, "INSERT INTO queue")
    assert -100 in params


def test_release_defers_forward_not_backward() -> None:
    """A negative interval would make the row instantly ready: a hot loop."""
    conn = _conn(rowcount=1)
    release(conn, 7, 120, WORKER)
    sql, params = conn.cur.executed[0]
    assert "clock_timestamp() + (INTERVAL '1 second' * %s)" in sql
    assert params[0] == 120


def test_backoff_grows_with_attempts() -> None:
    """Asserting non-zero allowed a constant; the point is that it backs off."""
    first = _conn(fetchone=(0, 9))
    fail_or_retry(first, 7, "boom", WORKER)
    later = _conn(fetchone=(5, 9))
    fail_or_retry(later, 7, "boom", WORKER)
    assert (
        _params_of(later, "SET status = 'pending'")[2]
        > _params_of(first, "SET status = 'pending'")[2]
    )


def test_partition_covers_exactly_one_day() -> None:
    conn = _conn_failing_first_insert(_missing_partition())
    enqueue(conn, "triage", {"gmail_id": "abc"})
    ddl = [s for s, _ in conn.cur.executed if "CREATE TABLE" in s][0]
    today = date.today()
    assert f"FROM ('{today}') TO ('{today + timedelta(days=1)}')" in ddl


def test_migration_precheck_looks_for_the_right_column() -> None:
    conn = _conn(fetchone=(1,))
    has_claim_token_column(conn)
    sql = conn.cur.executed[0][0]
    assert "attname = 'claimed_by'" in sql
    assert "to_regclass('queue')" in sql, "must resolve through search_path"


def test_migration_lock_timeout_is_transaction_scoped() -> None:
    """A bare SET would leak the timeout into the whole session."""
    conn = _conn(fetchone=None)
    ensure_claim_token_column(conn)
    assert any("SET LOCAL lock_timeout" in s for s, _ in conn.cur.executed)


def test_terminal_failure_clears_the_retry_schedule() -> None:
    conn = _conn(fetchone=(2, 3))
    fail_or_retry(conn, 7, "boom", WORKER)
    assert (
        "next_attempt_at = NULL"
        in [s for s, _ in conn.cur.executed if "SET status = 'failed'" in s][0]
    )


def test_complete_stamps_completed_at() -> None:
    conn = _conn(rowcount=1)
    complete(conn, 7, WORKER)
    assert "completed_at = NOW()" in conn.cur.executed[0][0]


def test_empty_dedup_key_is_rejected_not_silently_ignored() -> None:
    """'' is falsy but not None: it used to skip validation and still take the
    dedup branch, producing payload->>'' = NULL, which never matches."""
    with pytest.raises(QueueError):
        enqueue(_conn(fetchone=(42,)), "triage", {"": 1}, dedup_key="")
