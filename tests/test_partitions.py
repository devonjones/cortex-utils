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

from cortex_utils.queue.partitions import PartitionManager, QueueTableNotFoundError


def _manager_capturing_sql(
    fetchone: object = None, fetchall: object = None
) -> tuple[PartitionManager, list[str]]:
    """PartitionManager whose executed SQL is captured instead of run.

    Bound parameters are appended to the statement so name assertions can see
    them wherever the query passes the partition name as %s. `fetchone`/`fetchall`
    set what the cursor returns, so callers can drive the found and not-found
    branches -- defaulting to not-found would leave the found paths unasserted.
    """
    executed: list[str] = []
    cur = MagicMock()
    cur.execute.side_effect = lambda sql, *a: executed.append(sql + repr(a))
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall if fetchall is not None else []
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    return PartitionManager(conn), executed


def test_lookups_bind_parent_by_search_path_not_bare_name() -> None:
    """Every catalog lookup must resolve the parent via to_regclass('queue')."""
    for call in (
        lambda m: m.partition_exists(date(2026, 8, 10)),
        lambda m: m.list_partitions(),
        lambda m: m.is_table_partitioned(),
    ):
        manager, executed = _manager_capturing_sql(fetchone=("queue",))
        call(manager)
        assert executed, "expected a catalog query"
        sql = executed[-1]
        assert "to_regclass('queue')" in sql, f"parent not bound by search_path: {sql}"
        # The exact shape that caused the outage: any schema's queue would match.
        assert "relname = 'queue'" not in sql, f"matches queue in any schema: {sql}"


def test_partition_exists_returns_false_when_no_row() -> None:
    manager, _ = _manager_capturing_sql()
    assert manager.partition_exists(date(2026, 8, 10)) is False


def test_partition_exists_returns_true_when_row_found() -> None:
    manager, _ = _manager_capturing_sql(fetchone=(1,))
    assert manager.partition_exists(date(2026, 8, 10)) is True


def test_list_partitions_maps_rows_to_dicts() -> None:
    manager, _ = _manager_capturing_sql(fetchall=[("queue_2026_08_10", "8192 bytes", 8192)])
    assert manager.list_partitions() == [
        {"name": "queue_2026_08_10", "size": "8192 bytes", "size_bytes": 8192}
    ]


def test_is_table_partitioned_true_when_parent_resolves() -> None:
    manager, _ = _manager_capturing_sql(fetchone=("queue",))
    assert manager.is_table_partitioned() is True


def test_is_table_partitioned_raises_when_queue_not_on_search_path() -> None:
    """A misconfigured search_path must not read as "not partitioned".

    Returning False sends the CLI down "run migrate-queue first" and exits 0 --
    the green no-op that let the real outage run for 4.8 days.
    """
    manager, _ = _manager_capturing_sql(fetchone=(None,))
    with pytest.raises(QueueTableNotFoundError):
        manager.is_table_partitioned()


def test_partition_name_matches_created_partition() -> None:
    """create_partition and partition_exists must agree on the name."""
    manager, executed = _manager_capturing_sql()
    manager.partition_exists(date(2026, 8, 6))
    manager.create_partition(date(2026, 8, 6))
    assert "queue_2026_08_06" in executed[0]
    assert "queue_2026_08_06" in executed[-1]
    # Bounds are the single day, half-open.
    assert "FROM ('2026-08-06') TO ('2026-08-07')" in executed[-1]


def test_create_partition_tolerates_concurrent_creation() -> None:
    """The other maintenance job may create the partition between check and CREATE."""
    manager, executed = _manager_capturing_sql()
    manager.create_partition(date(2026, 8, 6))
    assert "IF NOT EXISTS" in executed[-1]
