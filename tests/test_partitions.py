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

from datetime import date
from unittest.mock import MagicMock

import pytest

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
            answered_meta.append((SERVER_TODAY,))

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
