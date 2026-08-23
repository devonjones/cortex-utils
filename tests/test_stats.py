"""Tests for queue stats.

These exist because the clock fix changed a parameter's *meaning*: it used to be
a Python datetime cutoff, where a unit mistake is a type error and fails loudly,
and it is now a bare count multiplied by an INTERVAL literal inside a SQL
string, where a unit mistake is silent and returns plausible numbers.

get_stale_jobs exists to detect crashed workers, so a silently-wrong window is a
monitor that lies -- which is worse than no monitor.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

from cortex_utils.queue.stats import get_queue_stats, get_stale_jobs


class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchall(self) -> list[Any]:
        return []

    def fetchone(self) -> Any:
        # SELECT NOW() is the only single-row read in this module.
        return (datetime(2026, 3, 1, tzinfo=UTC),)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _conn():
    cur = FakeCursor()
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.cur = cur
    return conn


def _sql_with(conn, needle: str) -> tuple[str, Any]:
    hits = [(s, p) for s, p in conn.cur.executed if needle in s]
    assert hits, f"no statement matching {needle!r}"
    return hits[0]


def test_history_window_is_measured_in_hours_on_the_server() -> None:
    conn = _conn()
    get_queue_stats(conn, history_hours=6)
    sql, params = _sql_with(conn, "COALESCE(completed_at, created_at)")
    assert "NOW() - (INTERVAL '1 hour' * %s)" in sql, "hours, and the server's clock"
    assert params == (6,)


def test_stale_window_is_measured_in_minutes_on_the_server() -> None:
    """An hour/minute swap here silently widens the window 60x."""
    conn = _conn()
    get_stale_jobs(conn, stale_minutes=30)
    sql, params = _sql_with(conn, "status = 'processing'")
    assert "NOW() - (INTERVAL '1 minute' * %s)" in sql
    assert params == (30,)


def test_stale_window_looks_backwards() -> None:
    """Negating the interval selects jobs claimed in the future: always empty,
    so the monitor reports healthy forever."""
    conn = _conn()
    get_stale_jobs(conn, stale_minutes=30)
    sql, _ = _sql_with(conn, "status = 'processing'")
    assert "claimed_at < NOW() -" in sql


def test_stale_query_filters_at_all() -> None:
    """Dropping the predicate returns every processing row as stale."""
    conn = _conn()
    get_stale_jobs(conn, stale_minutes=30)
    sql, _ = _sql_with(conn, "status = 'processing'")
    assert "claimed_at <" in sql


def test_history_query_filters_at_all() -> None:
    conn = _conn()
    get_queue_stats(conn, history_hours=24)
    sql, _ = _sql_with(conn, "COALESCE(completed_at, created_at)")
    assert ">" in sql.split("COALESCE(completed_at, created_at)")[1][:60]


def test_report_timestamp_comes_from_the_server() -> None:
    """Every other timestamp in the document is server-produced; stamping it
    with this process's clock would put two clocks in one report."""
    conn = _conn()
    get_queue_stats(conn)
    assert any("SELECT NOW()" in s for s, _ in conn.cur.executed)
