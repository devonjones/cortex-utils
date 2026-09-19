"""Tests for queue partition management.

Regression cover for the two-schema bug: catalog lookups that matched
pg_class.relname = 'queue' also matched a same-named table in another schema, so
partition_exists() reported True for partitions belonging to the *other* queue and
the real ones were never created. See partitions.py module docstring.

These assert the SQL shape, not real catalog resolution: the suite has no live
Postgres, so the actual two-schema case cannot be exercised here. That gap is
tracked in cortex-jst7, whose acceptance criteria require a regression test
covering it against a real server.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock

import psycopg2
import pytest

from cortex_utils.queue.ops import QueueError
from cortex_utils.queue.partitions import (
    PartitionError,
    PartitionManager,
    PartitionNotAttachedError,
    QueueTableNotFoundError,
)

Row = tuple[object, ...]

# What "SELECT to_regclass('queue')" returns when the parent does and does not resolve.
PARENT_OK: Row = ("queue",)
PARENT_MISSING: Row = (None,)


# The date the fake server reports, deliberately not date.today(): partition
# dates must come from the server clock that produces created_at, so a test
# using the client clock could not tell a regression from correct behaviour.
SERVER_TODAY = date(2026, 3, 1)


def _is_meta(sql: str) -> bool:
    """True for probe statements that carry no lookup logic of their own."""
    stripped = sql.strip()
    return (
        stripped.startswith("SELECT to_regclass")
        or stripped.startswith("SHOW search_path")
        or stripped.startswith("SELECT CURRENT_DATE")
    )


def _name(day: date) -> str:
    return f"queue_{day.strftime('%Y_%m_%d')}"


def _manager_capturing_sql(
    rows: list[Row | None] | None = None,
    parent: Row = PARENT_OK,
    fetchall: list[Row] | None = None,
) -> tuple[PartitionManager, list[str]]:
    """PartitionManager whose executed SQL is captured instead of run.

    Bound parameters are appended to the statement so name assertions can see
    them wherever the query passes the partition name as %s.

    `parent` answers the _require_parent probe wherever it occurs; `rows` answers
    the real lookups in order. Keeping them apart means a test pins only the
    ordering that carries meaning (was the partition there before the CREATE, is
    it there after) and not how many times the guard happens to probe -- memoising
    that probe is an obvious refactor and must not fail these tests.
    """
    executed: list[str] = []
    pending = list(rows) if rows else []
    answered_meta: list[Row] = []

    def _execute(sql: str, *args: object) -> None:
        executed.append(sql + repr(args))
        if sql.strip().startswith("SELECT to_regclass"):
            answered_meta.append(parent)
        elif sql.strip().startswith("SHOW search_path"):
            answered_meta.append(("public",))
        elif sql.strip().startswith("SELECT CURRENT_DATE"):
            # (date, zone, source) -- server_today() reads all three in one
            # round trip so the TimeZone-override check costs no extra trip.
            answered_meta.append((SERVER_TODAY, "UTC", "default"))

    def _fetchone() -> Row | None:
        if answered_meta:
            return answered_meta.pop(0)
        return pending.pop(0) if pending else None

    cur = MagicMock()
    cur.execute.side_effect = _execute
    cur.fetchone.side_effect = _fetchone
    cur.fetchall.return_value = fetchall if fetchall is not None else []
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    return PartitionManager(conn), executed


@pytest.fixture
def stub_dead_letter_ddl(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """maintain() ensures dead_letter before archiving into it. That is real DDL
    and belongs in the live suite (test_partitions_live.py proves it against a
    schema whose table has been dropped); driving it against a MagicMock only
    tests the mock. Stubbed so these stay focused on create/drop ordering and
    the window arithmetic -- via monkeypatch, so the module global is restored.

    Returns the call log, so a test can still assert the call happened.
    """
    import cortex_utils.queue.partitions as mod

    calls: list[str] = []

    class _Stub:
        def __init__(self, conn: Any) -> None:
            pass

        def ensure_table(self) -> None:
            calls.append("ensure_table")

    monkeypatch.setattr(mod, "DeadLetterManager", _Stub)
    return calls


def test_lookups_bind_parent_by_search_path_not_bare_name() -> None:
    """Every catalog lookup must resolve the parent via to_regclass('queue').

    Asserted over every non-probe statement, not just the last one. The probe
    only happens to run first; reorder it and executed[-1] becomes the probe's own
    SQL, which satisfies both assertions on its own -- letting all three lookups
    revert to the outage form with the suite still green. This is the test that
    stands between that bug and a repeat, so it must not depend on statement order.
    """
    for call in (
        lambda m: m.partition_exists(date(2026, 8, 10)),
        lambda m: m.list_partitions(),
        lambda m: m.is_table_partitioned(),
    ):
        manager, executed = _manager_capturing_sql()
        call(manager)
        lookups = [sql for sql in executed if not _is_meta(sql)]
        assert lookups, "expected a catalog query"
        for sql in lookups:
            assert "to_regclass('queue')" in sql, f"parent not bound by search_path: {sql}"
            # The exact shape that caused the outage: any schema's queue would match.
            assert "relname = 'queue'" not in sql, f"matches queue in any schema: {sql}"


def test_every_lookup_raises_when_queue_not_on_search_path() -> None:
    """A misconfigured search_path must not read as an ordinary empty result.

    Returning False/[] sends `partitions drop` down "Partition does not exist" and
    the CLI's other commands down "run migrate-queue first", both exiting 0 -- the
    green no-op that let the real outage run for 4.8 days. The contract must not
    vary by method: `partitions drop` never calls is_table_partitioned().
    """
    for call in (
        lambda m: m.partition_exists(date(2026, 8, 10)),
        lambda m: m.list_partitions(),
        lambda m: m.is_table_partitioned(),
    ):
        manager, _ = _manager_capturing_sql(parent=PARENT_MISSING)
        with pytest.raises(QueueTableNotFoundError):
            call(manager)


def test_partition_exists_returns_false_when_no_row() -> None:
    manager, _ = _manager_capturing_sql(rows=[None])
    assert manager.partition_exists(date(2026, 8, 10)) is False


def test_partition_exists_returns_true_when_row_found() -> None:
    manager, _ = _manager_capturing_sql(rows=[(1,)])
    assert manager.partition_exists(date(2026, 8, 10)) is True


def test_list_partitions_maps_rows_to_dicts() -> None:
    manager, _ = _manager_capturing_sql(fetchall=[("queue_2026_08_10", "8192 bytes", 8192)])
    assert manager.list_partitions() == [
        {"name": "queue_2026_08_10", "size": "8192 bytes", "size_bytes": 8192}
    ]


def test_is_table_partitioned_true_when_parent_is_partitioned() -> None:
    manager, _ = _manager_capturing_sql(rows=[("r",)])
    assert manager.is_table_partitioned() is True


def test_is_table_partitioned_false_when_parent_is_plain_table() -> None:
    """Queue resolves but has no partition strategy -- the migrate-queue case."""
    manager, _ = _manager_capturing_sql(rows=[None])
    assert manager.is_table_partitioned() is False


def test_partition_name_matches_created_partition() -> None:
    """create_partition and partition_exists must agree on the name."""
    # not there before the CREATE, there after.
    manager, executed = _manager_capturing_sql(rows=[None, (1,)])
    manager.create_partition(date(2026, 8, 6))
    created = [s for s in executed if "CREATE TABLE" in s]
    assert len(created) == 1
    # Bounds are the single day, half-open.
    assert "queue_2026_08_06" in created[0]
    assert "FROM ('2026-08-06') TO ('2026-08-07')" in created[0]
    # The post-check must re-ask about the partition just created. Counting calls
    # is not enough: partition_exists(next_date) is an easy off-by-one, since
    # next_date is in scope at that point and differs by exactly one day.
    lookups = [sql for sql in executed if not _is_meta(sql)]
    assert "queue_2026_08_06" in lookups[-1], f"post-check asked the wrong thing: {lookups[-1]}"


def test_create_partition_tolerates_concurrent_creation() -> None:
    """The other maintenance job may create the partition between check and CREATE."""
    manager, executed = _manager_capturing_sql(rows=[None, (1,)])
    manager.create_partition(date(2026, 8, 6))
    assert "IF NOT EXISTS" in [s for s in executed if "CREATE TABLE" in s][0]


def test_create_partition_raises_when_name_is_shadowed() -> None:
    """IF NOT EXISTS skips on any same-named relation, not just a partition of queue.

    migrate.py creates queue_YYYY_MM_DD tables under queue_new, so an interrupted
    migration leaves a shadow that would absorb the CREATE and leave the day
    uncovered -- silently, which is the failure this module exists to prevent.
    """
    # not there before the CREATE, still not there after.
    manager, _ = _manager_capturing_sql(rows=[None, None])
    with pytest.raises(PartitionNotAttachedError):
        manager.create_partition(date(2026, 8, 6))


def test_create_future_partitions_continues_past_an_unusable_date() -> None:
    """One shadowed date must not starve the dates after it.

    Raising straight out of the loop would let a shadow on today suppress
    tomorrow's partition too -- the guard causing the very gap it exists to catch.
    """
    # day 0: absent, then still absent after CREATE (shadowed).
    # day 1: absent, then present after CREATE (fine).
    manager, executed = _manager_capturing_sql(rows=[None, None, None, (1,)])
    with pytest.raises(PartitionNotAttachedError):
        manager.create_future_partitions(days_ahead=1)
    assert len([s for s in executed if "CREATE TABLE" in s]) == 2


def test_partition_errors_share_a_base() -> None:
    """Callers should be able to catch the category without an explicit tuple."""
    assert issubclass(QueueTableNotFoundError, PartitionError)
    assert issubclass(PartitionNotAttachedError, PartitionError)
    # And one level further up, which is the level that actually matters to a
    # consumer: enqueue() raises PartitionNotAttachedError, so a worker loop
    # catching QueueError -- the documented way to catch queue failures -- must
    # catch it. Pinning only the two leaves leaves this base free to revert.
    assert issubclass(PartitionError, QueueError)


def test_create_future_partitions_raises_the_first_failure() -> None:
    """With several unusable dates the raised error is the first, not the last."""
    # both days: absent, then still absent after CREATE.
    manager, _ = _manager_capturing_sql(rows=[None, None, None, None])
    with pytest.raises(PartitionNotAttachedError) as excinfo:
        manager.create_future_partitions(days_ahead=1)
    # The server's date, not this process's: the two agree only by coincidence.
    assert f"queue_{SERVER_TODAY.strftime('%Y_%m_%d')}" in str(excinfo.value)


def test_create_future_partitions_returns_count_when_all_succeed() -> None:
    manager, _ = _manager_capturing_sql(rows=[None, (1,), None, (1,)])
    assert manager.create_future_partitions(days_ahead=1) == 2


def test_dry_run_executes_no_ddl() -> None:
    """dry_run must inspect only -- a dry run that creates tables is worse than none."""
    manager, executed = _manager_capturing_sql(rows=[None])
    assert manager.create_partition(date(2026, 8, 6), dry_run=True) is True
    assert not [sql for sql in executed if "CREATE TABLE" in sql]


def test_create_future_partitions_forwards_dry_run() -> None:
    manager, executed = _manager_capturing_sql(rows=[None, None])
    manager.create_future_partitions(days_ahead=1, dry_run=True)
    assert not [sql for sql in executed if "CREATE TABLE" in sql]


def test_create_future_partitions_does_not_count_existing_partitions() -> None:
    """An already-present date is skipped, not counted as created."""
    # day 0 already there; day 1 absent, then present after its CREATE.
    manager, _ = _manager_capturing_sql(rows=[(1,), None, (1,)])
    assert manager.create_future_partitions(days_ahead=1) == 1


def test_retention_cutoff_uses_the_server_clock() -> None:
    """The destructive path. A client clock running ahead of the server pushes
    the cutoff forward and drops a partition still holding live rows."""
    manager, executed = _manager_capturing_sql(fetchall=[])
    manager.drop_old_partitions(retention_days=7)
    assert any("SELECT CURRENT_DATE" in sql for sql in executed), (
        "the retention cutoff must come from the server, not this process"
    )


def test_retention_drops_only_partitions_older_than_the_window() -> None:
    """Asserting the clock source alone left the arithmetic free: a sign flip or
    an off-by-N still drops live partitions, silently and irreversibly.

    SERVER_TODAY is 2026-03-01, so a 7-day window retains 02-23 onward and
    retires anything older.
    """
    parts = [
        {"name": _name(SERVER_TODAY - timedelta(days=d)), "size": "0", "size_bytes": 0}
        for d in (10, 8, 7, 6, 0)
    ]
    manager, _ = _manager_capturing_sql()
    manager.list_partitions = lambda: parts  # type: ignore[method-assign]
    dropped: list[date] = []
    manager.drop_partition = (  # type: ignore[method-assign]
        lambda d, **kw: dropped.append(d) or {"dropped_rows": 0, "archived_failed": 0}
    )

    manager.drop_old_partitions(retention_days=7)

    assert dropped == [SERVER_TODAY - timedelta(days=10), SERVER_TODAY - timedelta(days=8)]
    assert SERVER_TODAY not in dropped, "today must never be dropped"


def test_a_dry_run_drop_does_not_hold_the_partition_lock() -> None:
    """drop_partition takes SHARE ROW EXCLUSIVE before counting. On the real
    path the DROP commits and releases it; a preview reaches no commit, so
    without an explicit rollback drop_old_partitions accumulates one lock per
    expired partition and holds them for the rest of the connection. claim()'s
    stale-reset and retirement UPDATEs are not date-qualified, so a *preview*
    would block the claim path pipeline-wide.
    """
    manager, executed = _manager_capturing_sql(rows=[(1,)])  # the partition exists

    manager.drop_partition(SERVER_TODAY - timedelta(days=30), dry_run=True)

    assert any("LOCK TABLE" in sql for sql in executed), "the lock is what needs releasing"
    assert not any("DROP TABLE" in sql for sql in executed), "a preview drops nothing"
    manager.conn.commit.assert_not_called()
    manager.conn.rollback.assert_called_once()


def test_skipping_a_partition_with_active_jobs_releases_its_lock() -> None:
    """The twin of the dry-run leak, and the worse one: it needs no --dry-run
    flag, and the nightly `partitions maintain` cron is the caller. The comment
    on the dry-run rollback claims this branch already does the right thing --
    nothing was asserting that it does."""
    manager, executed = _manager_capturing_sql(
        rows=[(1,)],  # the partition exists
        fetchall=[("pending", 3)],  # ...and still holds live work
    )

    result = manager.drop_partition(SERVER_TODAY - timedelta(days=30))

    assert result["skipped_active"] == 3
    assert not any("DROP TABLE" in sql for sql in executed), "live jobs must survive"
    manager.conn.rollback.assert_called_once()
    manager.conn.commit.assert_not_called()


def test_failed_jobs_are_archived_before_the_partition_is_dropped() -> None:
    """The whole reason retention is allowed to DROP: failed jobs move to
    dead_letter first. Nothing was executing this body -- round 4 pinned the two
    branches that exit *before* it. Disabling the archive leaves the DROP in
    place and every failed job in that partition permanently gone, on the
    nightly `partitions maintain` cron, with nothing raised.
    """
    manager, executed = _manager_capturing_sql(
        rows=[(1,)],  # the partition exists
        fetchall=[("failed", 4), ("completed", 9)],
    )

    result = manager.drop_partition(SERVER_TODAY - timedelta(days=30))

    archive = [sql for sql in executed if "INSERT INTO dead_letter" in sql]
    assert archive, "failed jobs must be archived before the drop"
    drop_at = next(i for i, sql in enumerate(executed) if "DROP TABLE" in sql)
    assert executed.index(archive[0]) < drop_at, "archive must precede the drop"
    assert "WHERE status = 'failed'" in archive[0]
    assert result["dropped_rows"] == 13


def test_archiving_can_be_turned_off_but_then_nothing_is_archived() -> None:
    manager, executed = _manager_capturing_sql(rows=[(1,)], fetchall=[("failed", 4)])
    manager.drop_partition(SERVER_TODAY - timedelta(days=30), archive_failed=False)
    assert not [sql for sql in executed if "INSERT INTO dead_letter" in sql]


def test_forcing_a_drop_reenqueues_live_jobs_before_dropping() -> None:
    """force=True is the only way live work survives a drop -- it is re-enqueued
    with a fresh created_at so it lands in today's partition."""
    manager, executed = _manager_capturing_sql(
        rows=[(1,)], fetchall=[("pending", 2), ("processing", 1)]
    )

    result = manager.drop_partition(SERVER_TODAY - timedelta(days=30), force=True)

    requeue = [sql for sql in executed if "INSERT INTO queue" in sql]
    assert requeue, "live jobs must be re-enqueued, not dropped"
    assert "WHERE status IN ('pending', 'processing')" in requeue[0]
    assert "NOW()" in requeue[0], "fresh created_at, or it lands back in this partition"
    assert executed.index(requeue[0]) < next(
        i for i, sql in enumerate(executed) if "DROP TABLE" in sql
    )
    assert "skipped_active" not in result


def test_a_skipped_partition_is_not_counted_as_dropped() -> None:
    """The wedged-queue signal: a partition kept back because it still holds
    live work must show up as skipped, not folded into the dropped count."""
    manager, _ = _manager_capturing_sql()
    manager.list_partitions = lambda: [  # type: ignore[method-assign]
        {"name": _name(SERVER_TODAY - timedelta(days=d)), "size": "0", "size_bytes": 0}
        for d in (10, 9)
    ]
    manager.drop_partition = lambda d, **kw: (  # type: ignore[method-assign]
        {"skipped_active": 3, "dropped_rows": 0, "archived_failed": 0}
        if d == SERVER_TODAY - timedelta(days=10)
        else {"dropped_rows": 5, "archived_failed": 1}
    )

    totals = manager.drop_old_partitions(retention_days=7)

    assert totals["partitions_skipped"] == 1
    assert totals["partitions_dropped"] == 1
    assert totals["rows_dropped"] == 5


def test_a_concurrent_creator_does_not_abort_the_maintenance_run() -> None:
    """IF NOT EXISTS narrows the window between the check and the CREATE but
    does not close it -- the name check is not atomic with the creation. Two
    maintenance jobs share this database, and an unhandled DuplicateTable would
    abort the transaction and take the rest of maintain() down with it.
    """
    manager, executed = _manager_capturing_sql(rows=[None, (1,)])
    raised = {"once": False}
    plain = manager.conn.cursor.return_value.__enter__.return_value.execute.side_effect

    def execute(sql: str, *args: object) -> None:
        plain(sql, *args)
        if "CREATE TABLE" in sql and not raised["once"]:
            raised["once"] = True
            raise psycopg2.errors.DuplicateTable("beaten to it")

    manager.conn.cursor.return_value.__enter__.return_value.execute.side_effect = execute

    assert manager.create_partition(date(2026, 8, 6)) is True
    manager.conn.rollback.assert_called_once()
    assert [sql for sql in executed if "pg_inherits" in sql], (
        "losing the race is not proof the partition exists -- ask the catalogue"
    )


def test_a_shadowed_name_still_raises_when_the_create_is_lost() -> None:
    """Same race, but the winner was not a partition of queue. Conceding on the
    exception alone would report success for a day with no partition."""
    manager, _ = _manager_capturing_sql(rows=[None, None])
    plain = manager.conn.cursor.return_value.__enter__.return_value.execute.side_effect

    def execute(sql: str, *args: object) -> None:
        plain(sql, *args)
        if "CREATE TABLE" in sql:
            raise psycopg2.errors.DuplicateTable("name taken")

    manager.conn.cursor.return_value.__enter__.return_value.execute.side_effect = execute

    with pytest.raises(PartitionNotAttachedError):
        manager.create_partition(date(2026, 8, 6))


def test_maintain_forwards_dry_run_to_both_halves() -> None:
    """The cron entrypoint, and it had no tests: every mutation in it survived,
    including flipping this flag on the drop half -- which makes
    `partitions maintain --dry-run` drop partitions for real. Silent and
    irreversible, in the one function the nightly job actually calls.
    """
    manager, _ = _manager_capturing_sql()
    seen: dict[str, bool] = {}
    manager.create_future_partitions = (  # type: ignore[method-assign]
        lambda days_ahead, dry_run: seen.__setitem__("create", dry_run) or 0
    )
    manager.drop_old_partitions = lambda **kw: (  # type: ignore[method-assign]
        seen.__setitem__("drop", kw["dry_run"]) or {"partitions_dropped": 0}
    )

    result = manager.maintain(dry_run=True)

    assert seen == {"create": True, "drop": True}, "a preview must preview both halves"
    assert result["dry_run"] is True, "and must say so, or the log reads as a real run"


def test_maintain_creates_before_it_drops(stub_dead_letter_ddl: list[str]) -> None:
    """Drop-then-create leaves a window with no partition for today: the write
    path self-heals, but only after an insert has already failed."""
    manager, _ = _manager_capturing_sql()
    order: list[str] = []
    manager.create_future_partitions = lambda **kw: order.append("create") or 2  # type: ignore[method-assign]
    manager.drop_old_partitions = lambda **kw: order.append("drop") or {}  # type: ignore[method-assign]

    manager.maintain()

    assert order == ["create", "drop"]
    assert stub_dead_letter_ddl == ["ensure_table"], (
        "maintain() must ensure dead_letter before archiving into it -- it runs "
        "from cron on a host that may never boot a service"
    )


def test_maintain_reports_both_halves_and_its_own_mode(stub_dead_letter_ddl: list[str]) -> None:
    """The cron's only output. Losing a key here makes a run that dropped
    nothing indistinguishable from one that dropped everything."""
    manager, _ = _manager_capturing_sql()
    manager.create_future_partitions = lambda **kw: 3  # type: ignore[method-assign]
    manager.drop_old_partitions = lambda **kw: {  # type: ignore[method-assign]
        "partitions_dropped": 2,
        "partitions_skipped": 1,
        "rows_dropped": 40,
    }

    result = manager.maintain(retention_days=7, days_ahead=3)

    assert result == {
        "partitions_created": 3,
        "partitions_dropped": 2,
        "partitions_skipped": 1,
        "rows_dropped": 40,
        "dry_run": False,
    }


def test_maintain_passes_its_windows_through(stub_dead_letter_ddl: list[str]) -> None:
    """retention_days and days_ahead swapped would retire three days of live
    partitions and create a week of empty ones."""
    manager, _ = _manager_capturing_sql()
    got: dict[str, Any] = {}
    manager.create_future_partitions = lambda **kw: got.update(create=kw) or 0  # type: ignore[method-assign]
    manager.drop_old_partitions = lambda **kw: got.update(drop=kw) or {}  # type: ignore[method-assign]

    manager.maintain(retention_days=14, days_ahead=5)

    assert got["create"]["days_ahead"] == 5
    assert got["drop"]["retention_days"] == 14
    assert got["drop"]["archive_failed"] is True, "the cron must not drop failed jobs unarchived"


def test_a_python_side_failure_under_the_lock_rolls_back() -> None:
    """A mock cannot show the lock being released -- see test_ops_live.py for
    that. What it can pin is that the handler runs at all, and that it rolls
    back rather than only re-raising.

    Deliberately a Python-side exception, not a psycopg2 one: a database error
    aborts the transaction server-side and releases the lock on its own, so
    simulating one would assert the case that never needed fixing.
    """
    manager, executed = _manager_capturing_sql(rows=[(1,)], fetchall=[("failed", 2)])
    cur = manager.conn.cursor.return_value.__enter__.return_value
    plain = cur.execute.side_effect

    def boom(sql: str, *args: object) -> None:
        plain(sql, *args)
        if "INSERT INTO dead_letter" in sql:
            raise RuntimeError("a bug in our own code, not the server's")

    cur.execute.side_effect = boom

    with pytest.raises(RuntimeError):
        manager.drop_partition(SERVER_TODAY - timedelta(days=30))

    assert any("LOCK TABLE" in sql for sql in executed)
    manager.conn.rollback.assert_called_once()
    manager.conn.commit.assert_not_called()
