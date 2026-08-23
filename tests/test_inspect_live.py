"""Live-Postgres tests for the health statement.

Every finding on this module's first review round survived the mock suite, and
the reason was structural rather than careless: `tests/test_inspect.py` asserts
on substrings of `_HEALTH_SQL` and never executes it. A test that greps SQL
cannot tell `MAX(upper bound)` from contiguous coverage, cannot tell `numeric`
from `float8`, and cannot tell an inverted FILTER from a correct one -- the
mock hands back whatever type and value the code asked for.

So these run the real statement against a real Postgres. They are skipped
unless one is reachable, which keeps `uv run pytest` fast and offline for
everyone else:

    docker run -d --rm --name pgtest -e POSTGRES_PASSWORD=x -p 55432:5432 postgres:16
    CORTEX_TEST_DSN="host=127.0.0.1 port=55432 user=postgres password=x dbname=postgres" \\
        uv run pytest tests/test_inspect_live.py

Each test builds its own schema and drops it, so they neither share state nor
touch anything real.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from cortex_utils.queue.add_retry_columns import has_next_attempt_at_column  # noqa: E402
from cortex_utils.queue.inspect import failures, health, resubmit, stuck  # noqa: E402
from cortex_utils.queue.ops import (  # noqa: E402
    SELF_HEALED_MARKER,
    QueueError,
    QueueTableNotFoundError,
    enqueue,
)

DSN = os.environ.get("CORTEX_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not DSN, reason="set CORTEX_TEST_DSN to a throwaway Postgres to run these"
)

QUEUE_DDL = """
CREATE TABLE queue (
    id BIGSERIAL,
    queue_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    last_error TEXT,
    claimed_at TIMESTAMPTZ,
    claimed_by TEXT,
    next_attempt_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE dead_letter (
    id BIGSERIAL PRIMARY KEY,
    original_id BIGINT NOT NULL,
    queue_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INT NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_from_partition TEXT NOT NULL DEFAULT 'x'
);
"""


@pytest.fixture
def conn():
    """A connection whose search_path is a schema of this test's own."""
    c = psycopg2.connect(DSN)
    c.autocommit = True
    schema = "t_inspect"
    with c.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path = {schema}")
        cur.execute(QUEUE_DDL)
    c.autocommit = False
    try:
        yield c
    finally:
        c.rollback()
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            cur.execute("DROP SCHEMA IF EXISTS other CASCADE")
        c.close()


def _server_today(conn) -> date:
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT_DATE")
        return cur.fetchone()[0]


def _partition(conn, day: date, self_healed: bool = False) -> str:
    name = f"queue_{day.strftime('%Y_%m_%d')}"
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {name} PARTITION OF queue "
            f"FOR VALUES FROM ('{day}') TO ('{day + timedelta(days=1)}')"
        )
        if self_healed:
            cur.execute(f"COMMENT ON TABLE {name} IS %s", (SELF_HEALED_MARKER,))
    conn.commit()
    return name


# --- headroom ----------------------------------------------------------------


def test_headroom_counts_contiguous_days_not_the_furthest_bound(conn) -> None:
    """A partition for today and one for +7 with a gap between is not seven
    days of headroom: the insert on the first uncovered day raises
    CheckViolation. MAX(bound) overstates in the direction that lets the queue
    break unannounced."""
    today = _server_today(conn)
    _partition(conn, today)
    _partition(conn, today + timedelta(days=7))

    assert health(conn).partition_headroom_days == 0

    # And prove the claim the number is about: tomorrow really does fail.
    with pytest.raises(psycopg2.errors.CheckViolation):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO queue (queue_name, payload, created_at) "
                "VALUES ('t', '{}'::jsonb, CURRENT_DATE + 1)"
            )
    conn.rollback()


def test_headroom_counts_the_whole_contiguous_run(conn) -> None:
    today = _server_today(conn)
    for offset in range(4):
        _partition(conn, today + timedelta(days=offset))
    assert health(conn).partition_headroom_days == 3


def test_headroom_is_none_when_today_is_uncovered(conn) -> None:
    """Not zero. Zero means tomorrow fails; None means writes fail right now,
    and a monitor should be able to tell those apart."""
    today = _server_today(conn)
    _partition(conn, today + timedelta(days=1))
    assert health(conn).partition_headroom_days is None


def test_no_partitions_at_all_reports_none_rather_than_erroring(conn) -> None:
    assert health(conn).partition_headroom_days is None
    assert health(conn).is_healthy is False


def test_yesterdays_partition_does_not_extend_todays_run(conn) -> None:
    """Retention drops old partitions, so a run must be anchored at today."""
    today = _server_today(conn)
    _partition(conn, today - timedelta(days=1))
    _partition(conn, today)
    assert health(conn).partition_headroom_days == 0


def test_is_healthy_trips_while_something_can_still_be_done(conn) -> None:
    """headroom 0 means tomorrow's writes already fail. An alarm that first
    fires at 0 fires after the problem, not before it."""
    today = _server_today(conn)
    _partition(conn, today)
    assert health(conn).is_healthy is False, "only today covered is not healthy"

    _partition(conn, today + timedelta(days=1))
    assert health(conn).partition_headroom_days == 1
    assert health(conn).is_healthy is True


# --- depths ------------------------------------------------------------------


def test_ready_and_deferred_are_counted_apart(conn) -> None:
    """Collapsed into one number, rows all backing off read as a working queue
    with a backlog rather than a queue in retry storm."""
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO queue (queue_name, payload) VALUES ('t', '{}'::jsonb)")
        cur.execute(
            "INSERT INTO queue (queue_name, payload, next_attempt_at) "
            "VALUES ('t', '{}'::jsonb, NOW() + INTERVAL '1 hour')"
        )
        cur.execute(
            "INSERT INTO queue (queue_name, payload, next_attempt_at) "
            "VALUES ('t', '{}'::jsonb, NOW() - INTERVAL '1 hour')"
        )
    conn.commit()

    depth = health(conn).depths[0]
    assert depth.ready == 2, "NULL and past-due next_attempt_at are both ready"
    assert depth.deferred == 1


def test_status_counts_land_in_their_own_fields(conn) -> None:
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        for status, n in (("pending", 2), ("processing", 3), ("failed", 4), ("completed", 5)):
            for _ in range(n):
                cur.execute(
                    "INSERT INTO queue (queue_name, payload, status) VALUES ('t', '{}'::jsonb, %s)",
                    (status,),
                )
    conn.commit()

    depth = health(conn).depths[0]
    assert (depth.ready, depth.processing, depth.failed) == (2, 3, 4)


def test_oldest_ready_age_ignores_deferred_rows(conn) -> None:
    """A week-old row still backing off is not a week of queue latency."""
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, created_at, next_attempt_at) "
            "VALUES ('t', '{}'::jsonb, NOW() - INTERVAL '6 hours', NOW() + INTERVAL '1 hour')"
        )
        cur.execute(
            "INSERT INTO queue (queue_name, payload, created_at) "
            "VALUES ('t', '{}'::jsonb, NOW() - INTERVAL '60 seconds')"
        )
    conn.commit()

    age = health(conn).depths[0].oldest_ready_age_s
    assert 55 <= age <= 120, f"took the deferred row's age: {age}"
    assert isinstance(age, float), "arithmetic on this must not raise on a Decimal"


def test_oldest_ready_age_is_the_oldest_not_the_newest(conn) -> None:
    """MIN, not MAX. With MAX a queue backed up for days reports ~1 second the
    moment one new row arrives -- it collapses to zero exactly when the backlog
    is worst, and this is the only latency signal health() emits."""
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        for minutes in (1, 240, 90):
            cur.execute(
                "INSERT INTO queue (queue_name, payload, created_at) "
                "VALUES ('t', '{}'::jsonb, NOW() - (INTERVAL '1 minute' * %s))",
                (minutes,),
            )
    conn.commit()

    age = health(conn).depths[0].oldest_ready_age_s
    assert 14000 <= age <= 14700, f"reported the newest row's age: {age}"


def test_an_unpartitioned_queue_is_not_reported_as_broken(conn) -> None:
    """A plain queue is a supported state -- this package ships
    migrate_to_partitioned() -- and its inserts never fail for want of a
    partition. Reporting it with the same headroom as a queue whose partitions
    ran out is a monitor that cries forever about something that works."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE queue")
        cur.execute(
            "CREATE TABLE queue (id BIGSERIAL PRIMARY KEY, queue_name TEXT NOT NULL, "
            "payload JSONB NOT NULL, status TEXT NOT NULL DEFAULT 'pending', "
            "priority INT NOT NULL DEFAULT 0, attempts INT NOT NULL DEFAULT 0, "
            "max_attempts INT NOT NULL DEFAULT 3, last_error TEXT, "
            "claimed_at TIMESTAMPTZ, claimed_by TEXT, next_attempt_at TIMESTAMPTZ, "
            "completed_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        )
        cur.execute("INSERT INTO queue (queue_name, payload) VALUES ('t', '{}'::jsonb)")
    conn.commit()

    got = health(conn)
    assert got.partitioned is False
    assert got.is_healthy is True, "inserts work; there is no headroom to run out of"
    assert got.depths[0].ready == 1


def test_a_partitioned_queue_says_so(conn) -> None:
    _partition(conn, _server_today(conn))
    assert health(conn).partitioned is True


def test_an_empty_queue_reports_no_depths_rather_than_failing(conn) -> None:
    _partition(conn, _server_today(conn))
    assert health(conn).depths == []


# --- self-heal counter -------------------------------------------------------


def test_only_marked_partitions_count_as_self_healed(conn) -> None:
    """health() reads this to decide whether maintenance is dead, so counting
    every partition would alarm permanently and counting none never would."""
    today = _server_today(conn)
    _partition(conn, today, self_healed=True)
    _partition(conn, today + timedelta(days=1))
    _partition(conn, today + timedelta(days=2))

    assert health(conn).self_healed_partitions == 1
    assert health(conn).is_healthy is False, "a self-heal means maintenance is behind"


def test_the_marker_the_write_path_writes_is_the_one_health_reads(conn) -> None:
    """Both sides import the same constant, so a unit test comparing them is a
    tautology. This lets enqueue() write it and health() read it back."""
    # No partitions at all, so the insert raises CheckViolation and the write
    # path recovers -- creating today's and, speculatively, tomorrow's.
    enqueue(conn, "t", {"a": 1})

    got = health(conn)
    assert got.self_healed_partitions == 2
    assert got.partition_headroom_days == 1
    assert got.is_healthy is False, "a self-heal means maintenance is behind"
    assert got.depths[0].ready == 1, "and the row it was recovering for landed"


def test_a_partition_maintenance_made_is_not_counted_as_a_self_heal(conn) -> None:
    """The counter decides whether maintenance is dead. Marking a partition
    that maintenance created makes it alarm forever, and COMMENT is permanent."""
    today = _server_today(conn)
    _partition(conn, today + timedelta(days=1))  # maintenance got ahead
    enqueue(conn, "t", {"a": 1})  # ...but not to today, so the write path heals it

    got = health(conn)
    assert got.self_healed_partitions == 1, "only the one enqueue actually created"
    assert got.partition_headroom_days == 1


def test_a_foreign_schemas_partitions_are_not_counted(conn) -> None:
    """The outage shape. Everything binds to_regclass('queue') under the active
    search_path, so another schema's queue must be invisible here."""
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS other CASCADE")
        cur.execute("CREATE SCHEMA other")
        cur.execute("SET search_path = other")
        cur.execute(QUEUE_DDL)
        for offset in range(4):
            day = today + timedelta(days=offset)
            name = f"queue_{day.strftime('%Y_%m_%d')}"
            cur.execute(
                f"CREATE TABLE {name} PARTITION OF queue "
                f"FOR VALUES FROM ('{day}') TO ('{day + timedelta(days=1)}')"
            )
            cur.execute(f"COMMENT ON TABLE {name} IS %s", (SELF_HEALED_MARKER,))
        cur.execute("SET search_path = t_inspect")
    conn.commit()

    # A different date, so a bare-relname lookup that matched both schemas would
    # visibly extend this schema's headroom rather than agreeing by coincidence.
    assert health(conn).partition_headroom_days == 0, "counted the other schema's partitions"
    assert health(conn).self_healed_partitions == 0, "counted the other schema's markers"


# --- dead letter -------------------------------------------------------------


def test_dead_letter_count_is_the_dead_letter_count(conn) -> None:
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO queue (queue_name, payload) VALUES ('t', '{}'::jsonb)")
        for _ in range(3):
            cur.execute(
                "INSERT INTO dead_letter (original_id, queue_name, payload, attempts, created_at) "
                "VALUES (1, 't', '{}'::jsonb, 3, NOW())"
            )
    conn.commit()

    got = health(conn)
    assert got.dead_letter == 3, "must not pick up a neighbouring scalar"
    assert got.depths[0].ready == 1


# --- stuck and failures ------------------------------------------------------


def test_stuck_reports_rows_past_the_visibility_window(conn) -> None:
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, claimed_by, claimed_at) "
            "VALUES ('t', '{}'::jsonb, 'processing', 'w1', NOW() - INTERVAL '90 minutes')"
        )
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, claimed_by, claimed_at) "
            "VALUES ('t', '{}'::jsonb, 'processing', 'w2', NOW() - INTERVAL '2 minutes')"
        )
    conn.commit()

    rows = stuck(conn, visibility_timeout_min=30)
    assert [r.claimed_by for r in rows] == ["w1"], "a worker still working is not stuck"
    # Arithmetic on this must not raise: EXTRACT(EPOCH ...) is numeric, and
    # Decimal / float is a TypeError where the annotation promises float.
    assert 50 < rows[0].stuck_for_s / 60.0 < 100


def test_stuck_ignores_rows_nobody_holds(conn) -> None:
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO queue (queue_name, payload) VALUES ('t', '{}'::jsonb)")
    conn.commit()
    assert stuck(conn, visibility_timeout_min=30) == []


def test_failures_returns_the_error_text_intact(conn) -> None:
    """Fourteen rows reading the identical error is the signal that the cause
    was infrastructure. That uniformity is only visible in the raw text."""
    today = _server_today(conn)
    _partition(conn, today)
    long_error = "visibility timeout, attempts exhausted. " + ("detail " * 60)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, last_error) "
            "VALUES ('t', '{}'::jsonb, 'failed', %s)",
            (long_error,),
        )
        cur.execute("INSERT INTO queue (queue_name, payload) VALUES ('t', '{}'::jsonb)")
    conn.commit()

    rows = failures(conn)
    assert len(rows) == 1, "only failed rows"
    assert rows[0].last_error == long_error, "not truncated on the way out"


def test_failures_can_be_scoped_to_one_queue(conn) -> None:
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        for name in ("triage", "parse"):
            cur.execute(
                "INSERT INTO queue (queue_name, payload, status) "
                "VALUES (%s, '{}'::jsonb, 'failed')",
                (name,),
            )
    conn.commit()
    assert [r.queue_name for r in failures(conn, queue_name="triage")] == ["triage"]


def test_resubmit_requeues_and_cancels_in_one_transaction(conn) -> None:
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, last_error) "
            "VALUES ('t', '{\"a\": 1}'::jsonb, 'failed', 'boom') RETURNING id"
        )
        failed_id = cur.fetchone()[0]
    conn.commit()

    new_id = resubmit(conn, failed_id)

    with conn.cursor() as cur:
        cur.execute("SELECT status, last_error FROM queue WHERE id = %s", (failed_id,))
        status, error = cur.fetchone()
        cur.execute("SELECT status, payload FROM queue WHERE id = %s", (new_id,))
        new_status, payload = cur.fetchone()

    assert status == "cancelled", "the record of what happened is the point"
    assert f"resubmitted as {new_id}" in error
    assert (new_status, payload) == ("pending", {"a": 1})


def test_resubmit_keeps_the_original_priority(conn) -> None:
    """Backfill runs at -100 so a replay never blocks live mail. Silently
    promoting a resubmitted backfill row to 0 preempts real-time work."""
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, priority) "
            "VALUES ('t', '{}'::jsonb, 'failed', -100) RETURNING id"
        )
        failed_id = cur.fetchone()[0]
    conn.commit()

    new_id = resubmit(conn, failed_id)

    with conn.cursor() as cur:
        cur.execute("SELECT priority FROM queue WHERE id = %s", (new_id,))
        assert cur.fetchone()[0] == -100, "a backfill replay must not jump the queue"


def test_resubmit_refuses_a_row_that_is_not_failed(conn) -> None:
    """Duplicating a processing job would run it twice and cancel it under a
    live worker."""
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, claimed_by) "
            "VALUES ('t', '{}'::jsonb, 'processing', 'w1') RETURNING id"
        )
        live_id = cur.fetchone()[0]
    conn.commit()

    with pytest.raises(QueueError):
        resubmit(conn, live_id)
    conn.rollback()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM queue")
        assert cur.fetchone()[0] == 1, "nothing was queued"


def test_stuck_lists_the_worst_offenders_first(conn) -> None:
    """With a LIMIT, the order decides which rows you can see at all. Reversed,
    a queue with more stuck jobs than the limit shows the 50 *least* stuck and
    hides every one worth looking at."""
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        for worker, hours in (("recent", 1), ("oldest", 9), ("middle", 5)):
            cur.execute(
                "INSERT INTO queue (queue_name, payload, status, claimed_by, claimed_at) "
                "VALUES ('t', '{}'::jsonb, 'processing', %s, NOW() - (INTERVAL '1 hour' * %s))",
                (worker, hours),
            )
    conn.commit()

    assert [r.claimed_by for r in stuck(conn, visibility_timeout_min=30)] == [
        "oldest",
        "middle",
        "recent",
    ]


def test_stuck_respects_its_limit(conn) -> None:
    today = _server_today(conn)
    _partition(conn, today)
    with conn.cursor() as cur:
        for i in range(5):
            cur.execute(
                "INSERT INTO queue (queue_name, payload, status, claimed_by, claimed_at) "
                "VALUES ('t', '{}'::jsonb, 'processing', %s, NOW() - (INTERVAL '1 hour' * %s))",
                (f"w{i}", i + 2),
            )
    conn.commit()

    rows = stuck(conn, visibility_timeout_min=30, limit=2)
    assert [r.claimed_by for r in rows] == ["w4", "w3"], "the limit must keep the worst"


def test_the_retry_migration_probe_refuses_a_missing_queue_table(conn) -> None:
    """to_regclass returns NULL rather than raising and `attrelid = NULL` matches
    nothing, so an unguarded probe answers "column missing" for a table that does
    not exist -- and the CLI prints that as an actionable migration which then
    fails on --execute. This is the misconfigured-PGOPTIONS case exactly."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE queue")
    conn.commit()

    with pytest.raises(QueueTableNotFoundError, match="PGOPTIONS"):
        has_next_attempt_at_column(conn)


def test_the_retry_migration_probe_answers_normally_when_the_table_exists(conn) -> None:
    assert has_next_attempt_at_column(conn) is True
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE queue DROP COLUMN next_attempt_at")
    conn.commit()
    assert has_next_attempt_at_column(conn) is False, "a real missing column still reads False"
