"""Live-Postgres tests for the parts of retention that a mock cannot judge.

test_partitions.py is honest about being entirely MagicMock-based: it asserts
the SQL was *written*, never that Postgres *does* it. That is fine for the
cutoff arithmetic, and it caught an off-by-one. It is not fine for the one
promise retention makes about data -- that failed work is preserved before its
partition goes away -- because a mock will happily report a rowcount for an
INSERT that matched nothing.

Proof it was not covered: changing the archive step's filter to
`WHERE status = 'failed' AND attempts > 999`, so retention destroys failed jobs
instead of preserving them, left all 470 tests green.

Same harness as test_ops_live.py; skipped unless CORTEX_TEST_DSN is set.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from cortex_utils.queue.dead_letter import DeadLetterManager  # noqa: E402
from cortex_utils.queue.partitions import PartitionManager  # noqa: E402
from cortex_utils.queue.schema import ensure_queue_schema  # noqa: E402

DSN = os.environ.get("CORTEX_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not DSN, reason="set CORTEX_TEST_DSN to a throwaway Postgres to run these"
)

SCHEMA = "t_part_live"


@pytest.fixture
def conn():
    setup = psycopg2.connect(DSN)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {SCHEMA}")
    setup.close()

    c = psycopg2.connect(DSN)
    with c.cursor() as cur:
        cur.execute(f"SET search_path = {SCHEMA}")
    c.commit()
    ensure_queue_schema(c)
    try:
        yield c
    finally:
        try:
            c.cancel()
            c.close()
        except Exception:  # noqa: BLE001 -- teardown must not mask the failure
            pass
        tear = psycopg2.connect(DSN)
        tear.autocommit = True
        with tear.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        tear.close()


def _old_partition_with(conn, statuses: list[str], days_old: int = 10) -> date:
    """Build a real partition `days_old` days back and put one row per status in it."""
    day = date.today() - timedelta(days=days_old)
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE queue_{day:%Y_%m_%d} PARTITION OF queue "
            f"FOR VALUES FROM ('{day}') TO ('{day + timedelta(days=1)}')"
        )
        for status in statuses:
            cur.execute(
                "INSERT INTO queue (queue_name, payload, status, created_at, "
                "attempts, max_attempts, last_error) VALUES "
                "('q', %s::jsonb, %s, %s, 2, 3, 'it broke')",
                ('{"keep": "me"}', status, day),
            )
    conn.commit()
    return day


def test_failed_jobs_are_in_dead_letter_before_their_partition_goes_away(conn) -> None:
    """The whole point of archive-before-drop. A DROP is irreversible and there
    is no other copy of the payload, so if the archive INSERT matches nothing
    the work is simply gone -- and the operator's only clue is a count they were
    not watching.
    """
    day = _old_partition_with(conn, ["failed", "completed"])

    result = PartitionManager(conn).drop_partition(day, archive_failed=True)

    assert result["archived_failed"] == 1, result
    with conn.cursor() as cur:
        cur.execute("SELECT payload, last_error FROM dead_letter")
        rows = cur.fetchall()
    conn.commit()
    assert len(rows) == 1, f"failed job was destroyed with its partition: {rows}"
    assert rows[0][0] == {"keep": "me"}, "archived a row but lost the payload"
    assert rows[0][1] == "it broke", "archived a row but lost why it failed"


def test_a_partition_holding_live_work_is_kept_back(conn) -> None:
    """The other half: retention must not drop a partition still holding work
    nobody has finished with.
    """
    day = _old_partition_with(conn, ["pending"])

    result = PartitionManager(conn).drop_partition(day, archive_failed=True)

    assert result.get("skipped_active"), result
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM queue")
        assert cur.fetchone()[0] == 1, "dropped a partition holding a pending job"
    conn.commit()


def test_maintain_works_on_a_schema_that_never_booted_a_service(conn) -> None:
    """maintain() runs from cron, on a host that may never call
    ensure_queue_schema(). Its archive step writes to dead_letter, so it has to
    ensure that table itself -- otherwise the first failed row raises
    UndefinedTable, and because creation runs before dropping, partitions keep
    accumulating while retention silently stops.
    """
    _old_partition_with(conn, ["failed"])
    with conn.cursor() as cur:
        cur.execute("DROP TABLE dead_letter")
    conn.commit()

    result = PartitionManager(conn).maintain(retention_days=7, days_ahead=1)

    assert result["failed_archived"] == 1, result
    assert DeadLetterManager(conn).list_jobs(), "the failed row was not preserved"


def test_a_forced_move_does_not_promote_a_backfill_ahead_of_real_time_mail(
    conn,
) -> None:
    """force=True re-enqueues live rows into today's partition. It used to build
    that INSERT from a fixed column list that omitted priority and
    next_attempt_at, so every moved row came out at priority 0 with no backoff.

    A backfill is enqueued at -100 precisely so it cannot get in front of
    real-time mail. Force-dropping an old backfill partition -- the thing an
    operator does when retention is wedged, i.e. when the queue is already
    unhappy -- promoted the whole backlog to the front.
    """
    day = date.today() - timedelta(days=10)
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE queue_{day:%Y_%m_%d} PARTITION OF queue "
            f"FOR VALUES FROM ('{day}') TO ('{day + timedelta(days=1)}')"
        )
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, priority, created_at, "
            "attempts, max_attempts, next_attempt_at) VALUES "
            "('q', '{}'::jsonb, 'pending', -100, %s, 2, 3, %s)",
            (day, date.today() + timedelta(days=1)),
        )
    conn.commit()

    PartitionManager(conn).drop_partition(day, force=True)

    with conn.cursor() as cur:
        cur.execute("SELECT priority, next_attempt_at, attempts FROM queue")
        priority, next_attempt_at, attempts = cur.fetchone()
    conn.commit()
    assert priority == -100, (
        f"backfill came back at priority {priority} -- it now outranks real-time mail"
    )
    assert next_attempt_at is not None, "a job waiting out a backoff became ready early"
    assert attempts == 0, "a relocated job should arrive with a full budget"


def test_health_reports_how_old_the_oldest_partition_is(conn) -> None:
    """The number that goes wrong when retention silently stops.

    A partition holding a stuck row is kept back under the default
    force=False, and the skip count appears only in maintain()'s return value
    and log. Nothing an operator polls said anything about it, so the failure
    shape was: worker down, rows freeze, retention stops dropping, and the first
    signal is disk usage. This climbs past retention_days and keeps climbing.
    """
    from cortex_utils.queue.inspect import health

    assert health(conn).oldest_partition_age_days <= 0, (
        "a fresh schema has only today's and future partitions"
    )

    _old_partition_with(conn, ["pending"], days_old=30)

    assert health(conn).oldest_partition_age_days == 30
