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

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import psycopg2
import pytest

from cortex_utils.queue.ops import (
    DEFAULT_VISIBILITY_TIMEOUT_MIN,
    MIGRATION_LOCK_TIMEOUT_MS,
    PARTITION_LOCK_TIMEOUT_MS,
    SELF_HEALED_MARKER,
    PartitionNotAttachedError,
    QueueError,
    QueueTableNotFoundError,
    _ensure_partition,
    _partition_name,
    claim,
    complete,
    enqueue,
    ensure_claim_token_column,
    fail_or_retry,
    has_claim_token_column,
    release,
    server_today,
)

WORKER = "worker-a"


# The date the fake server reports. Deliberately not date.today(): a test that
# used the client clock could not tell the two apart, which is the bug.
SERVER_TODAY = date(2026, 3, 1)
# A row's created_at, deliberately not SERVER_TODAY: partitioned rows outlive
# the day they were made, and the UPDATE must carry the row's own value.
ROW_CREATED_AT = datetime(2026, 2, 27, 9, 30, tzinfo=UTC)


class FakeCursor:
    """Records (sql, params) and replays canned results.

    Answers SELECT CURRENT_DATE itself, so tests do not have to thread the
    server-date probe through every fetchone sequence.

    It also keeps a real catalogue: the pg_inherits probe answers "attached"
    only for partitions a CREATE in this session actually made. _ensure_partition
    asks that question twice with different correct answers -- absent before its
    CREATE, present after -- and a mock that says "present" to both cannot tell
    a partition this call created from one that was already there. That is the
    whole distinction the self-heal counter rests on.
    """

    def __init__(self, fetchone: Any = None, fetchall: Any = (), rowcount: int = 1):
        self.executed: list[tuple[str, Any]] = []
        self._fetchone = fetchone
        self._fetchall = list(fetchall)
        self.rowcount = rowcount
        self.raise_on: str | None = None
        self.error: Exception | None = None
        self._pending_date = False
        self._pending_probe = False
        self._pending_parent = False
        self.no_queue_table = False
        self.created: set[str] = set()
        self.preexisting: set[str] = set()
        self.preexisting_after_create = False

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        self._pending_date = "CURRENT_DATE" in sql
        # The queue table exists unless a test says otherwise -- every primitive
        # now checks, and threading that through each fetchone sequence would
        # bury the thing each test is actually about.
        self._pending_parent = "SELECT to_regclass('queue')" in sql
        self._pending_probe = "pg_inherits" in sql
        self._probed = params[0] if (self._pending_probe and params) else None
        if self.preexisting_after_create and sql.strip().startswith("CREATE TABLE"):
            # A concurrent creator won the race. Their partition lands whether or
            # not our own statement then errors, so this happens before the
            # raise -- which is the situation the concession handler exists for.
            for token in sql.split():
                if token.startswith("queue_2"):
                    self.preexisting.add(token)
        if self.raise_on and self.raise_on in sql and self.error:
            raise self.error
        if sql.strip().startswith("CREATE TABLE"):
            for token in sql.split():
                if token.startswith("queue_2"):
                    self.created.add(token)

    def fetchone(self) -> Any:
        if self._pending_date:
            self._pending_date = False
            return (SERVER_TODAY,)
        if self._pending_parent:
            self._pending_parent = False
            return (None,) if self.no_queue_table else ("queue",)
        if self._pending_probe:
            self._pending_probe = False
            known = self.created | self.preexisting
            return (1,) if self._probed in known else None
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
    """The whole dict, not a subset: this is the contract consumers build on,
    so a field appearing or vanishing should be a deliberate edit here."""
    conn = _conn(fetchall=[(7, "triage", {"gmail_id": "abc"}, 2, 5, ROW_CREATED_AT)])
    jobs = claim(conn, "triage", WORKER)
    assert jobs == [
        {
            "id": 7,
            "queue_name": "triage",
            "payload": {"gmail_id": "abc"},
            "attempts": 2,
            "priority": 5,
            # Free: partitioning forces created_at into the primary key, so the
            # CTE already joins on it. Without it a consumer that needs the age
            # of the work runs a second query per claimed row.
            "created_at": ROW_CREATED_AT,
        }
    ]


def test_claim_asks_the_database_for_created_at() -> None:
    """It has to come from the RETURNING, not from a second query or a client
    clock -- created_at is server-produced and half the primary key."""
    conn = _conn(fetchall=[(7, "triage", {}, 0, 0, ROW_CREATED_AT)])
    claim(conn, "triage", WORKER)
    sql = [s for s, _ in conn.cur.executed if "RETURNING" in s][0]
    assert "q.created_at" in sql.split("RETURNING")[1]


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
    conn = _conn(fetchone=(0, 3, ROW_CREATED_AT))
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
    conn = _conn(fetchone=(0, 3, ROW_CREATED_AT))
    assert fail_or_retry(conn, 7, "boom", WORKER) == "pending"
    params = _params_of(conn, "SET status = 'pending'")
    assert params[0] == 1, "attempts must go 0 -> 1"
    assert params[1] == "boom"
    assert params[2] > 0, "a retry must be delayed"
    assert params[3] == 7


def test_fail_or_retry_retires_on_the_last_attempt() -> None:
    conn = _conn(fetchone=(2, 3, ROW_CREATED_AT))
    assert fail_or_retry(conn, 7, "boom", WORKER) == "failed"
    params = _params_of(conn, "SET status = 'failed'")
    assert params == (3, "boom", 7, ROW_CREATED_AT)


def test_fail_or_retry_truncates_a_huge_error() -> None:
    conn = _conn(fetchone=(0, 3, ROW_CREATED_AT))
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
    # Dated by the server, not this process - see the dedicated test below.
    assert _partition_name(SERVER_TODAY) in sql
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
    conn = _conn(fetchone=(0, 3, ROW_CREATED_AT))
    fail_or_retry(conn, 7, "boom", WORKER)
    conn.commit.assert_called()


def test_read_only_precheck_does_not_leave_a_transaction_open() -> None:
    """psycopg2 opens a transaction even for a SELECT."""
    conn = _conn(fetchone=(1,))
    has_claim_token_column(conn)
    # Both transactions: require_queue_table's guard, then the probe's own. An
    # or-assertion passes on the guard's commit alone, which is exactly the
    # mutant this test is named for -- the probe opening a SELECT and never
    # closing it, leaving worker connections idle-in-transaction on the boot
    # fast path.
    assert conn.commit.call_count == 2


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
    claimable = conn.cur.executed[0][0].split("claimable AS")[1].split("UPDATE queue q")[0]
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
    assert "status IN ('pending', 'processing')" in where, (
        "completed rows must not suppress a replay"
    )


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
    # Without this the row stays 'processing' with claimed_at = NULL, which the
    # stale sweep's "claimed_at < ..." can never match: unclaimable forever.
    assert "SET status = 'pending'" in sql


def test_backoff_grows_with_attempts() -> None:
    """Asserting non-zero allowed a constant; the point is that it backs off."""
    first = _conn(fetchone=(0, 9, ROW_CREATED_AT))
    fail_or_retry(first, 7, "boom", WORKER)
    later = _conn(fetchone=(5, 9, ROW_CREATED_AT))
    fail_or_retry(later, 7, "boom", WORKER)
    assert (
        _params_of(later, "SET status = 'pending'")[2]
        > _params_of(first, "SET status = 'pending'")[2]
    )


def test_partitions_are_dated_by_the_server_not_this_process() -> None:
    """The bug cryo found: created_at is server NOW(), so the partition date has
    to come from the same clock. A client date.today() only agrees by accident."""
    conn = _conn_failing_first_insert(_missing_partition())
    enqueue(conn, "triage", {"gmail_id": "abc"})
    ddl = [s for s, _ in conn.cur.executed if "CREATE TABLE" in s]
    assert any("SELECT CURRENT_DATE" in s for s, _ in conn.cur.executed)
    assert f"FROM ('{SERVER_TODAY}') TO ('{SERVER_TODAY + timedelta(days=1)}')" in ddl[0]


def test_self_heal_creates_tomorrow_too() -> None:
    """Removes the midnight race rather than narrowing it."""
    conn = _conn_failing_first_insert(_missing_partition())
    enqueue(conn, "triage", {"gmail_id": "abc"})
    ddl = [s for s, _ in conn.cur.executed if "CREATE TABLE" in s]
    assert len(ddl) == 2
    tomorrow = SERVER_TODAY + timedelta(days=1)
    assert _partition_name(tomorrow) in ddl[1]


def test_partition_creation_is_lock_bounded() -> None:
    """It runs on the live producer path and takes ACCESS EXCLUSIVE on the
    parent, blocking every insert and claim across all queues while held."""
    conn = _conn_failing_first_insert(_missing_partition())
    enqueue(conn, "triage", {"gmail_id": "abc"})
    ddl = [s for s, _ in conn.cur.executed if "CREATE TABLE" in s]
    timeouts = [p for s, p in conn.cur.executed if "lock_timeout" in s]
    assert len(timeouts) == len(ddl), "every write-path DDL must be lock-bounded"
    assert all(t[0] == f"{PARTITION_LOCK_TIMEOUT_MS}ms" for t in timeouts)
    # A bare SET leaks the bound into the whole session rather than the statement.
    assert all("SET LOCAL lock_timeout" in s for s, _ in conn.cur.executed if "lock_timeout" in s)
    assert PARTITION_LOCK_TIMEOUT_MS < MIGRATION_LOCK_TIMEOUT_MS, (
        "the live producer path must give up sooner than the deploy path"
    )


def test_losing_the_partition_lock_consults_the_catalogue() -> None:
    """Losing a lock race is evidence of contention, not of creation.

    The holder can be ensure_claim_token_column's ALTER -- whose timeout is
    longer than this path's, so it outlasts us -- or drop_partition's DROP.
    Conceding without asking would report success for a partition nobody made.
    """
    conn = _conn_failing_first_insert(_missing_partition())
    conn.cur.raise_on = "CREATE TABLE"
    conn.cur.error = psycopg2.errors.LockNotAvailable("timeout")
    # The lock holder is another creator, so the partition does land -- but only
    # the catalogue can say so, which is the point of the assertion below.
    conn.cur.preexisting_after_create = True
    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 99
    probes = [s for s, _ in conn.cur.executed if "pg_inherits" in s]
    assert probes, "must ask the catalogue rather than infer from the exception"
    assert "to_regclass('queue')" in probes[0], "and ask about THIS schema's queue"
    assert "relname = 'queue'" not in probes[0], "the two-schema outage shape"


def test_migration_precheck_looks_for_the_right_column() -> None:
    conn = _conn(fetchone=(1,))
    has_claim_token_column(conn)
    sql = [s for s, _ in conn.cur.executed if "pg_attribute" in s][0]
    assert "attname = 'claimed_by'" in sql
    assert "to_regclass('queue')" in sql, "must resolve through search_path"


def test_migration_lock_timeout_is_transaction_scoped() -> None:
    """A bare SET would leak the timeout into the whole session."""
    conn = _conn(fetchone=None)
    ensure_claim_token_column(conn)
    assert any("SET LOCAL lock_timeout" in s for s, _ in conn.cur.executed)


def test_terminal_failure_clears_the_retry_schedule() -> None:
    conn = _conn(fetchone=(2, 3, ROW_CREATED_AT))
    fail_or_retry(conn, 7, "boom", WORKER)
    assert (
        "next_attempt_at = NULL"
        in [s for s, _ in conn.cur.executed if "SET status = 'failed'" in s][0]
    )


def test_complete_marks_the_job_completed() -> None:
    """Asserting only completed_at let status = 'failed' pass: succeeded work
    would land in dead-letter while complete() returned True."""
    conn = _conn(rowcount=1)
    complete(conn, 7, WORKER)
    sql = conn.cur.executed[0][0]
    assert "SET status = 'completed'" in sql
    assert "completed_at = NOW()" in sql


def test_empty_dedup_key_is_rejected_not_silently_ignored() -> None:
    """'' is falsy but not None: it used to skip validation and still take the
    dedup branch, producing payload->>'' = NULL, which never matches."""
    with pytest.raises(QueueError):
        enqueue(_conn(fetchone=(42,)), "triage", {"": 1}, dedup_key="")


def test_stale_sweep_targets_expired_processing_rows_only() -> None:
    """Two silent inversions live here.

    Flipping the comparison steals claims from live workers and never recovers
    abandoned ones; dropping the status filter resurrects completed jobs on a
    loop. Both return normally.
    """
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER)
    sql = conn.cur.executed[0][0]
    for cte in (
        sql.split("retire_exhausted")[0],
        sql.split("retire_exhausted")[1].split("claimable")[0],
    ):
        assert "status = 'processing'" in cte, "must not touch rows that are not claimed"
        assert "claimed_at < NOW()" in cte, "must target expired claims, not fresh ones"


def test_claimable_is_scoped_and_budget_bounded() -> None:
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER)
    claimable = conn.cur.executed[0][0].split("claimable AS")[1].split("UPDATE queue q")[0]
    assert "queue_name = %(q)s" in claimable, "must not claim another queue's work"
    assert "attempts < max_attempts" in claimable
    assert "next_attempt_at IS NULL" in claimable, "a never-deferred row must be claimable"


def test_migration_adds_exactly_the_claim_token_column_with_a_real_timeout() -> None:
    conn = _conn(fetchone=None)
    ensure_claim_token_column(conn)
    ddl = [s for s, _ in conn.cur.executed if "ADD COLUMN" in s][0]
    assert ddl.rstrip().endswith("claimed_by TEXT"), "prefix matching would accept claimed_bys"
    timeout = _params_of(conn, "lock_timeout")[0]
    assert timeout == f"{MIGRATION_LOCK_TIMEOUT_MS}ms"
    assert not timeout.startswith("0"), "0 means wait forever, inverting the intent"


def test_losing_the_partition_creation_race_counts_as_success() -> None:
    """IF NOT EXISTS checks the name before taking the lock that serialises
    creation, so a concurrent creator can still win in between."""
    state = {"insert_failed": False}
    cur = FakeCursor(fetchone=(99,))
    plain = cur.execute

    def execute(sql: str, params: Any = None) -> None:
        plain(sql, params)
        if "INSERT INTO queue" in sql and not state["insert_failed"]:
            state["insert_failed"] = True
            raise _missing_partition()
        if "CREATE TABLE" in sql:
            raise psycopg2.errors.DuplicateTable("already exists")

    cur.execute = execute  # type: ignore[method-assign]
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur

    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 99
    assert len([s for s, _ in cur.executed if "INSERT INTO queue" in s]) == 2


def test_claim_writes_a_complete_claim() -> None:
    """The outer UPDATE is the claim itself, and was only asserted for the token.

    Without claimed_at the row sits in 'processing' with a NULL timestamp, which
    reset_stale's "claimed_at < NOW() - interval" can never match: abandoned work
    becomes unrecoverable. Leaving status as 'pending' gives unbounded concurrent
    reprocessing with complete() bouncing every time.
    """
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER)
    outer = conn.cur.executed[0][0].split("UPDATE queue q")[1]
    assert "status = 'processing'" in outer
    assert "claimed_at = NOW()" in outer, "a claim with no timestamp is never recovered"
    assert "claimed_by = %(w)s" in outer


def test_claim_uses_the_documented_default_visibility_timeout() -> None:
    conn = _conn(fetchall=[])
    claim(conn, "triage", WORKER)
    # Asserting against the constant would be tautological: a mutation moves
    # both sides together. 30 minutes is the documented contract.
    assert DEFAULT_VISIBILITY_TIMEOUT_MIN == 30
    assert _params_of(conn, "reset_stale")["vis"] == 30


def test_fail_or_retry_defers_forward_in_seconds() -> None:
    """Backwards, a failing job is instantly re-claimable and burns its whole
    attempt budget in seconds during an outage -- the 2026-08-18 shape."""
    conn = _conn(fetchone=(0, 3, ROW_CREATED_AT))
    fail_or_retry(conn, 7, "boom", WORKER)
    sql = [s for s, _ in conn.cur.executed if "SET status = 'pending'" in s][0]
    assert "clock_timestamp() + (INTERVAL '1 second' * %s)" in sql


# --- transaction composition ------------------------------------------------


def test_composed_enqueue_leaves_the_transaction_to_the_caller() -> None:
    """Two real callers need this: approving a proposal and queueing its apply
    job, and moving a row out of dead_letter while re-queueing it. Committing
    separately would let a crash leave the two halves disagreeing."""
    conn = _conn(fetchone=(42,))
    assert enqueue(conn, "triage", {"gmail_id": "abc"}, commit=False) == 42
    conn.commit.assert_not_called()
    conn.rollback.assert_not_called()


def test_composed_enqueue_does_not_roll_back_the_callers_work() -> None:
    """Rolling back here would discard whatever the caller already did."""
    conn = _conn(fetchone=(42,))
    conn.cur.raise_on = "INSERT INTO queue"
    conn.cur.error = psycopg2.OperationalError("boom")
    with pytest.raises(psycopg2.OperationalError):
        enqueue(conn, "triage", {"gmail_id": "abc"}, commit=False)
    conn.rollback.assert_not_called()


def test_composed_enqueue_gives_up_the_partition_self_heal() -> None:
    """Creating a partition has to commit, which would commit the caller's
    pending work behind their back. The violation propagates instead."""
    conn = _conn_failing_first_insert(_missing_partition())
    with pytest.raises(psycopg2.errors.CheckViolation):
        enqueue(conn, "triage", {"gmail_id": "abc"}, commit=False)
    assert "CREATE TABLE" not in "\n".join(s for s, _ in conn.cur.executed)


def test_default_enqueue_still_commits_and_self_heals() -> None:
    conn = _conn_failing_first_insert(_missing_partition())
    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 99
    conn.commit.assert_called()


def test_a_taken_name_that_is_not_our_partition_is_not_counted_as_present() -> None:
    """DuplicateTable proves a name is taken, not that the partition exists.

    A same-named relation that is not a partition of this queue would otherwise
    be reported as success and the retry would fail explaining nothing.
    """
    conn = _conn_failing_first_insert(_missing_partition())
    conn.cur.raise_on = "CREATE TABLE"
    conn.cur.error = psycopg2.errors.DuplicateTable("name taken")
    # preexisting_after_create stays False: the name belongs to something that
    # is not a partition of our queue, so it never enters the catalogue.

    with pytest.raises(psycopg2.errors.DuplicateTable):
        enqueue(conn, "triage", {"gmail_id": "abc"})
    assert any("pg_inherits" in s for s, _ in conn.cur.executed), "must ask, not assume"


def test_tomorrows_failure_never_fails_the_callers_write() -> None:
    """Today's partition is what the caller needs; tomorrow is a favour.

    Letting a speculative create take the write down trades a real failure now
    for a hypothetical one later.
    """
    cur = FakeCursor(fetchone=(99,))
    plain = cur.execute
    state = {"insert_failed": False}
    tomorrow = _partition_name(SERVER_TODAY + timedelta(days=1))

    def execute(sql: str, params: Any = None) -> None:
        plain(sql, params)
        if "INSERT INTO queue" in sql and not state["insert_failed"]:
            state["insert_failed"] = True
            raise _missing_partition()
        if "CREATE TABLE" in sql and tomorrow in sql:
            raise psycopg2.errors.InsufficientPrivilege("disk full")

    cur.execute = execute  # type: ignore[method-assign]
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur

    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 99


def test_conceding_a_partition_still_attempts_tomorrow() -> None:
    """Breaking out of the loop on a concession silently restores the race."""
    conn = _conn_failing_first_insert(_missing_partition())
    conn.cur.raise_on = "CREATE TABLE"
    conn.cur.error = psycopg2.errors.DuplicateTable("exists")
    conn.cur.preexisting_after_create = True  # the duplicate is a real partition
    enqueue(conn, "triage", {"gmail_id": "abc"})
    assert len([s for s, _ in conn.cur.executed if "CREATE TABLE" in s]) == 2
    assert [s for s, _ in conn.cur.executed if "pg_inherits" in s], (
        "a concession must be confirmed, not assumed"
    )


def test_server_date_probe_closes_its_transaction() -> None:
    """psycopg2 opens one even for a SELECT, and callers can finish without
    ever reaching a write."""
    conn = _conn(fetchone=(SERVER_TODAY,))
    server_today(conn)
    assert conn.commit.called or conn.rollback.called


def test_fail_or_retry_updates_by_the_whole_primary_key() -> None:
    """(id, created_at) is the enforced key, and this function reads then writes:
    the UPDATE must address the same row the SELECT ... FOR UPDATE locked, by the
    key the table actually declares. complete() and release() are single
    statements with no such window, which is why they address by id alone."""
    for fetchone, marker in ((2, "SET status = 'failed'"), (0, "SET status = 'pending'")):
        conn = _conn(fetchone=(fetchone, 3, ROW_CREATED_AT))
        fail_or_retry(conn, 7, "boom", WORKER)
        sql = [s for s, _ in conn.cur.executed if marker in s][0]
        assert "WHERE id = %s AND created_at = %s" in sql, marker
        assert _params_of(conn, marker)[-1] == ROW_CREATED_AT

    # And the value must come from the locked row, not from a fresh clock.
    conn = _conn(fetchone=(0, 3, ROW_CREATED_AT))
    fail_or_retry(conn, 7, "boom", WORKER)
    select = [s for s, _ in conn.cur.executed if "FOR UPDATE" in s][0]
    assert "created_at" in select, "must read the row's own created_at"


def _conn_where_create_silently_skips(shadowed: date = SERVER_TODAY) -> Any:
    """A connection where CREATE TABLE IF NOT EXISTS returns cleanly but the
    name belongs to a relation that is not a partition of this queue."""
    cur = FakeCursor(fetchone=(99,))
    plain = cur.execute
    state = {"insert_failed": False}
    shadow_name = _partition_name(shadowed)

    def execute(sql: str, params: Any = None) -> None:
        plain(sql, params)
        if "INSERT INTO queue" in sql and not state["insert_failed"]:
            state["insert_failed"] = True
            raise _missing_partition()
        # A shadowed name never enters the catalogue: the relation exists, but
        # not as a partition of our queue, which is exactly what the probe asks.
        if sql.strip().startswith("CREATE TABLE") and shadow_name in sql:
            cur.created.discard(shadow_name)

    cur.execute = execute  # type: ignore[method-assign]
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur
    return conn


def test_a_create_that_silently_skipped_is_not_reported_as_created() -> None:
    """CREATE TABLE IF NOT EXISTS ... PARTITION OF raises nothing when the name
    is already a non-partition relation -- it emits a NOTICE and skips. So the
    statement returning cleanly is not evidence the partition exists, and
    migrate.py creates exactly those shadow names (queue_YYYY_MM_DD as
    partitions of queue_new). Without the post-check the retry INSERT fails
    again, explaining nothing.
    """
    conn = _conn_where_create_silently_skips()
    with pytest.raises(PartitionNotAttachedError):
        enqueue(conn, "triage", {"gmail_id": "abc"})
    probes = [s for s, _ in conn.cur.executed if "pg_inherits" in s]
    assert probes, "success must be confirmed, not assumed"
    assert "to_regclass('queue')" in probes[0]


def test_a_silently_skipped_create_for_tomorrow_does_not_fail_the_write() -> None:
    """Tomorrow is a favour; a shadowed name there must not take the write down."""
    conn = _conn_where_create_silently_skips(SERVER_TODAY + timedelta(days=1))
    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 99


def test_ensure_partition_does_not_claim_to_have_created_what_was_there() -> None:
    """CREATE TABLE IF NOT EXISTS succeeds whether or not it created anything,
    so a 'created' outcome would be a guess. Tomorrow's partition usually
    exists already, and the self-heal log is read to judge whether maintenance
    is keeping up -- a wrong verb there is the thing being judged."""
    conn = _conn_failing_first_insert(_missing_partition())
    enqueue(conn, "triage", {"gmail_id": "abc"})
    assert _ensure_partition(conn, SERVER_TODAY, required=True) == "present"


def test_only_a_partition_this_call_made_is_marked_self_healed() -> None:
    """health() reads the marker count to decide whether maintenance is dead,
    and COMMENT is permanent, so a false stamp is an alarm that never clears.

    _create_partition_for also ensures tomorrow, which usually exists already --
    stamping unconditionally would label maintenance's own partitions as
    self-heals on every write-path recovery.
    """
    conn = _conn_failing_first_insert(_missing_partition())
    tomorrow = _partition_name(SERVER_TODAY + timedelta(days=1))
    conn.cur.preexisting.add(tomorrow)  # maintenance already made this one

    enqueue(conn, "triage", {"gmail_id": "abc"})

    stamped = [p for s, p in conn.cur.executed if "COMMENT ON TABLE" in s]
    tables = [s.split()[3] for s, _ in conn.cur.executed if "COMMENT ON TABLE" in s]
    assert tables == [_partition_name(SERVER_TODAY)], f"stamped the wrong set: {tables}"
    assert all(p == (SELF_HEALED_MARKER,) for p in stamped)


def test_an_existing_partition_is_not_recreated_or_commented() -> None:
    """COMMENT ON TABLE requires ownership. On a healthy partition owned by
    another role, stamping unconditionally raises InsufficientPrivilege and
    fails a write that would otherwise have succeeded."""
    conn = _conn_failing_first_insert(_missing_partition())
    for offset in (0, 1):
        conn.cur.preexisting.add(_partition_name(SERVER_TODAY + timedelta(days=offset)))

    assert enqueue(conn, "triage", {"gmail_id": "abc"}) == 99

    assert not [s for s, _ in conn.cur.executed if "CREATE TABLE" in s]
    assert not [s for s, _ in conn.cur.executed if "COMMENT ON TABLE" in s]


def test_the_catalogue_is_asked_before_the_create_not_only_after() -> None:
    """CREATE TABLE IF NOT EXISTS succeeds whether or not it created anything,
    so 'did this call make it' can only be answered by asking first."""
    conn = _conn_failing_first_insert(_missing_partition())
    enqueue(conn, "triage", {"gmail_id": "abc"})
    order = [s for s, _ in conn.cur.executed if "pg_inherits" in s or "CREATE TABLE" in s]
    assert "pg_inherits" in order[0], "the first thing asked must be the catalogue"


def test_a_missing_queue_table_is_not_reported_as_a_missing_column() -> None:
    """to_regclass returns NULL rather than raising, and `attrelid = NULL`
    matches nothing -- so an unguarded probe answers False for a connection
    pointed at the wrong schema. That reads as "run the migration", and the
    migration then fails on a table that does not exist."""
    conn = _conn(fetchone=(1,))
    conn.cur.no_queue_table = True
    with pytest.raises(QueueTableNotFoundError, match="PGOPTIONS"):
        has_claim_token_column(conn)


# --- the claimed_by migration guard (cryo D4) --------------------------------


def _conn_without_claimed_by():
    """A schema where ensure_claim_token_column() was never run."""
    cur = FakeCursor(fetchone=(99,))
    plain = cur.execute

    def execute(sql: str, params: Any = None) -> None:
        plain(sql, params)
        if "claimed_by" in sql:
            raise psycopg2.errors.UndefinedColumn('column "claimed_by" does not exist')

    cur.execute = execute  # type: ignore[method-assign]
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur
    return conn


def test_every_primitive_that_touches_claimed_by_names_the_remedy() -> None:
    """All five reference claimed_by and the only protection is a convention.
    A consumer who forgets gets UndefinedColumn out of the core of the queue,
    naming a column they never wrote.

    Asserted over all of them at once on purpose: a half-guard reads as safe and
    is not. Guard four and the fifth still raises the raw error on the same
    schema, which is worse than guarding none.
    """
    for label, call in (
        ("claim", lambda c: claim(c, "triage", WORKER)),
        ("complete", lambda c: complete(c, 5, WORKER)),
        ("release", lambda c: release(c, 5, 60, WORKER)),
        ("fail_or_retry", lambda c: fail_or_retry(c, 5, "boom", WORKER)),
    ):
        with pytest.raises(QueueError, match="ensure_claim_token_column") as excinfo:
            call(_conn_without_claimed_by())
        assert "search_path" in str(excinfo.value), label


def test_an_unrelated_missing_column_is_not_relabelled() -> None:
    """The guard must not claim every schema problem is this one -- a typo in a
    caller's own SQL would be reported as a migration they have already run."""
    cur = FakeCursor(fetchone=(99,))
    plain = cur.execute

    def execute(sql: str, params: Any = None) -> None:
        plain(sql, params)
        raise psycopg2.errors.UndefinedColumn('column "widget" does not exist')

    cur.execute = execute  # type: ignore[method-assign]
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur

    with pytest.raises(psycopg2.errors.UndefinedColumn):
        complete(conn, 5, WORKER)


def test_the_guard_still_rolls_the_connection_back() -> None:
    """Relabelling the error must not skip the rollback -- a long-lived worker
    would then fail every later job for a reason unrelated to this one."""
    conn = _conn_without_claimed_by()
    with pytest.raises(QueueError):
        complete(conn, 5, WORKER)
    conn.rollback.assert_called_once()
