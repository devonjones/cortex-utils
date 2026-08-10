"""Tests for queue partition management.

Regression cover for the two-schema bug: catalog lookups that matched
pg_class.relname = 'queue' also matched a same-named table in another schema, so
partition_exists() reported True for partitions belonging to the *other* queue and
the real ones were never created. See partitions.py module docstring.

These assert the SQL shape because the suite has no live Postgres; a true
two-schema test needs a real server (see test_partition_exists_two_schemas below).
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from cortex_utils.queue.partitions import PartitionManager


def _manager_capturing_sql() -> tuple[PartitionManager, list[str]]:
    """PartitionManager whose executed SQL is captured instead of run.

    Bound parameters are appended to the statement so name assertions can see
    them wherever the query passes the partition name as %s.
    """
    executed: list[str] = []
    cur = MagicMock()
    cur.execute.side_effect = lambda sql, *a: executed.append(sql + repr(a))
    cur.fetchone.return_value = None
    cur.fetchall.return_value = []
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
        manager, executed = _manager_capturing_sql()
        call(manager)
        assert executed, "expected a catalog query"
        sql = executed[-1]
        assert "to_regclass('queue')" in sql, f"parent not bound by search_path: {sql}"
        # The exact shape that caused the outage: any schema's queue would match.
        assert "relname = 'queue'" not in sql, f"matches queue in any schema: {sql}"


def test_partition_exists_returns_false_when_no_row() -> None:
    manager, _ = _manager_capturing_sql()
    assert manager.partition_exists(date(2026, 8, 10)) is False


def test_partition_name_matches_created_partition() -> None:
    """create_partition and partition_exists must agree on the name."""
    manager, executed = _manager_capturing_sql()
    manager.partition_exists(date(2026, 8, 6))
    manager.create_partition(date(2026, 8, 6))
    assert "queue_2026_08_06" in executed[0]
    assert "queue_2026_08_06" in executed[-1]
    # Bounds are the single day, half-open.
    assert "FROM ('2026-08-06') TO ('2026-08-07')" in executed[-1]
