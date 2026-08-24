"""Live-Postgres tests for the canonical queue DDL.

A schema module is exactly the thing a mock cannot check: the whole claim is
that the shape this package emits is the shape its own queries need, and only a
server can hold both at once.
"""

from __future__ import annotations

import os

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from cortex_utils.queue.ops import (  # noqa: E402
    QueueError,
    claim,
    complete,
    enqueue,
    fail_or_retry,
    release,
)
from cortex_utils.queue.schema import (  # noqa: E402
    REQUIRED_COLUMNS,
    ensure_queue_table,
    missing_columns,
    queue_ddl,
)

DSN = os.environ.get("CORTEX_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not DSN, reason="set CORTEX_TEST_DSN to a throwaway Postgres to run these"
)


@pytest.fixture
def conn():
    c = psycopg2.connect(DSN)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS t_schema CASCADE")
        cur.execute("DROP SCHEMA IF EXISTS other CASCADE")
        cur.execute("CREATE SCHEMA t_schema")
        cur.execute("SET search_path = t_schema")
    c.autocommit = False
    try:
        yield c
    finally:
        c.rollback()
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS t_schema CASCADE")
            cur.execute("DROP SCHEMA IF EXISTS other CASCADE")
        c.close()


def _today_partition(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE queue_today PARTITION OF queue "
            "FOR VALUES FROM (CURRENT_DATE) TO (CURRENT_DATE + 1)"
        )
    conn.commit()


# --- the claim this module exists to make -----------------------------------


def test_the_primitives_run_against_the_shape_this_module_emits(conn) -> None:
    """The whole point. Every primitive against a table built only from the
    canonical DDL -- no test fixture filling gaps, no migration bolted on.

    This is what six hand-written copies could not tell anyone: the shape the
    package emits and the shape its own queries need are the same shape.
    """
    assert ensure_queue_table(conn) == "created"
    _today_partition(conn)

    job_id = enqueue(conn, "q", {"n": 1}, priority=5)
    assert job_id is not None

    claimed = claim(conn, "q", "worker-a")
    assert [j["id"] for j in claimed] == [job_id]
    assert claimed[0]["priority"] == 5

    assert release(conn, job_id, 0, "worker-a") is True
    again = claim(conn, "q", "worker-a")
    assert [j["id"] for j in again] == [job_id]

    assert fail_or_retry(conn, job_id, "boom", "worker-a") == "pending"
    assert claim(conn, "q", "worker-a") == [], (
        "a retried job is deferred by next_attempt_at, so it is not claimable yet"
    )

    # Bring it forward the way a caller would, rather than waiting out the
    # backoff, and finish the cycle.
    with conn.cursor() as cur:
        cur.execute("UPDATE queue SET next_attempt_at = NOW() - INTERVAL '1 second'")
    conn.commit()
    assert [j["id"] for j in claim(conn, "q", "worker-a")] == [job_id]
    assert complete(conn, job_id, "worker-a") is True

    with conn.cursor() as cur:
        cur.execute("SELECT status, attempts, claimed_by FROM queue WHERE id = %s", (job_id,))
        status, attempts, claimed_by = cur.fetchone()
    assert status == "completed"
    assert attempts == 1, "the release must not have charged one; only fail_or_retry does"
    assert claimed_by == "worker-a", "the token records who finished it"


def test_dedup_works_on_the_emitted_shape(conn) -> None:
    ensure_queue_table(conn)
    _today_partition(conn)
    assert enqueue(conn, "q", {"vid": "x"}, dedup_key="vid") is not None
    assert enqueue(conn, "q", {"vid": "x"}, dedup_key="vid") is None


def test_every_required_column_is_actually_created(conn) -> None:
    ensure_queue_table(conn)
    assert missing_columns(conn) == []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT attname FROM pg_attribute WHERE attrelid = to_regclass('queue') "
            "AND attnum > 0 AND NOT attisdropped"
        )
        present = {r[0] for r in cur.fetchall()}
    assert set(REQUIRED_COLUMNS) <= present


# --- idempotence and drift ---------------------------------------------------


def test_safe_on_every_boot(conn) -> None:
    assert ensure_queue_table(conn) == "created"
    assert ensure_queue_table(conn) == "present"
    assert ensure_queue_table(conn) == "present"


def test_an_existing_table_missing_a_column_is_reported_not_altered(conn) -> None:
    """Adding it silently would take ACCESS EXCLUSIVE on a live queue, and this
    package cannot tell a table that predates a column from one a consumer
    deliberately shapes differently. Naming the columns is the useful act."""
    with conn.cursor() as cur:
        cur.execute(queue_ddl().replace("    claimed_by TEXT,\n", ""))
    conn.commit()

    assert missing_columns(conn) == ["claimed_by"]
    with pytest.raises(QueueError, match="claimed_by") as excinfo:
        ensure_queue_table(conn)
    # Name the remedy. cryo D4 added exactly this to the other door onto this
    # failure; an operator hitting this one first should not get less.
    assert "ensure_claim_token_column" in str(excinfo.value)
    assert "add_retry_columns" in str(excinfo.value)

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('queue')")
    conn.rollback()


def test_extra_columns_are_not_drift(conn) -> None:
    """A consumer composing on top is the point; only absence is a problem."""
    ensure_queue_table(conn)
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE queue ADD COLUMN cryo_ref TEXT")
    conn.commit()

    assert missing_columns(conn) == []
    assert ensure_queue_table(conn) == "present"


def test_it_answers_about_this_schemas_queue(conn) -> None:
    """A bare relname lookup would find another schema's queue and report the
    shape as fine -- the defect that cost cortex 4.8 days of email."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA other")
        cur.execute("SET search_path = other")
        cur.execute(queue_ddl())
        cur.execute("SET search_path = t_schema")
    conn.commit()

    assert missing_columns(conn) == list(REQUIRED_COLUMNS), (
        "this schema has no queue at all, so every column is missing"
    )


# --- indexes -----------------------------------------------------------------


def test_the_indexes_the_claim_query_needs_are_created(conn) -> None:
    ensure_queue_table(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 't_schema' AND tablename = 'queue'"
        )
        names = {r[0] for r in cur.fetchall()}
    assert "idx_queue_claim" in names
    assert "idx_queue_stale" in names
    # Deliberately NOT idx_queue_processing: migrate.py already creates one
    # under that name with a different column list, and _ensure_indexes reads a
    # name that resolves as "it is there" -- so reusing it would mean the
    # canonical index is never created on any migrated deployment.
    assert "idx_queue_processing" not in names


def test_a_boot_with_the_indexes_already_there_issues_no_ddl(conn) -> None:
    """CREATE INDEX IF NOT EXISTS still takes a lock and waits on an open
    writer even when the index exists, and this runs on every boot."""
    ensure_queue_table(conn)
    _today_partition(conn)

    other = psycopg2.connect(DSN)
    try:
        other.autocommit = True
        with other.cursor() as cur:
            cur.execute("SET search_path = t_schema")
        other.autocommit = False
        # A real write, holding ROW EXCLUSIVE. SELECT ... FOR UPDATE only takes
        # ROW SHARE, which does NOT conflict with CREATE INDEX's SHARE -- so a
        # test using one proves nothing, and this one did until it was mutated.
        with other.cursor() as cur:
            cur.execute("INSERT INTO queue (queue_name, payload) VALUES ('t', '{}'::jsonb)")

        # Session-level, not SET LOCAL: _ensure_indexes opens its own
        # transaction, so a LOCAL bound never reaches it -- and without a bound
        # a regression here HANGS the suite instead of failing it. Verified:
        # removing the catalogue probe makes this block indefinitely.
        conn.rollback()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '2000ms'")
        conn.autocommit = False

        assert ensure_queue_table(conn) == "present", "must not wait on the writer"
    finally:
        other.rollback()
        other.close()


# --- the name is ours, but check anyway --------------------------------------


def test_a_table_name_that_is_not_a_name_is_refused() -> None:
    for bad in ("queue; DROP TABLE x", "queue--", "public.queue", ""):
        with pytest.raises(QueueError):
            queue_ddl(bad)


def test_the_canonical_index_is_created_on_a_migrated_deployment(conn) -> None:
    """migrate.py already creates idx_queue_pending and idx_queue_processing
    with different column lists. _ensure_indexes reads a name that resolves as
    "it is there", so reusing either name would mean the canonical index is
    silently never created on exactly the deployments that have been around
    longest -- this module's own subject matter, inside the module.
    """
    ensure_queue_table(conn)
    with conn.cursor() as cur:
        cur.execute("DROP INDEX idx_queue_claim")
        cur.execute("DROP INDEX idx_queue_stale")
        # The legacy shapes, exactly as migrate.py leaves them.
        cur.execute(
            "CREATE INDEX idx_queue_pending ON queue (queue_name, status, created_at) "
            "WHERE status = 'pending'"
        )
        cur.execute(
            "CREATE INDEX idx_queue_processing ON queue (queue_name, claimed_at) "
            "WHERE status = 'processing'"
        )
    conn.commit()

    assert ensure_queue_table(conn) == "present"

    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 't_schema' AND tablename = 'queue'"
        )
        names = {r[0] for r in cur.fetchall()}
    assert "idx_queue_claim" in names, "the legacy names must not mask the canonical ones"
    assert "idx_queue_stale" in names


def test_the_claim_index_can_serve_the_claim_order(conn) -> None:
    """Column order is the whole value of this index. A range predicate between
    the equality prefix and the sort keys stops it serving the ORDER BY, which
    EXPLAIN reports as a Sort node -- invisible to any test that only checks
    the index exists."""
    ensure_queue_table(conn)
    _today_partition(conn)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, priority) "
            "SELECT 'q', '{}'::jsonb, (g % 5) FROM generate_series(1, 3000) g"
        )
        cur.execute("ANALYZE queue")
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute(
            "EXPLAIN (COSTS OFF) SELECT id FROM queue "
            "WHERE queue_name = 'q' AND status = 'pending' "
            "ORDER BY priority DESC, created_at LIMIT 10"
        )
        plan = "\n".join(r[0] for r in cur.fetchall())
    conn.rollback()

    # A partition inherits the parent's index under its own generated name, so
    # assert the column signature rather than ours.
    assert "queue_name_status_priority_created_at" in plan, plan
    assert "Sort" not in plan, f"the index cannot serve the order:\n{plan}"


def test_an_unpartitioned_queue_is_reported_rather_than_left_to_fail_later(conn) -> None:
    """Legal and supported -- migrate_to_partitioned() exists for it -- but
    partitions.py would otherwise fail several frames from the thing that could
    have mentioned it."""
    with conn.cursor() as cur:
        cur.execute(queue_ddl().replace(") PARTITION BY RANGE (created_at)", ")"))
    conn.commit()

    import structlog

    with structlog.testing.capture_logs() as logs:
        assert ensure_queue_table(conn) == "present"
    assert any("not partitioned" in entry["event"] for entry in logs), (
        f"a boot that finds an unpartitioned queue must say so: {logs}"
    )
    assert missing_columns(conn) == []


def test_the_stale_index_can_serve_the_recovery_scan(conn) -> None:
    """claim()'s stale-recovery pass filters on queue_name as well as status,
    so leading with status alone leaves queue_name as a Filter -- 86ms against
    1.2ms on a real table, and invisible to any test that only checks the index
    exists."""
    ensure_queue_table(conn)
    _today_partition(conn)
    with conn.cursor() as cur:
        # The real recovery shape: many rows in flight, a few actually stalled.
        # With everything equally stale the claimed_at predicate selects nothing
        # and the planner reasonably filters instead of seeking.
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, claimed_at, claimed_by) "
            "SELECT 'q', '{}'::jsonb, 'processing', NOW() - INTERVAL '1 minute', 'w' "
            "FROM generate_series(1, 5000)"
        )
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, claimed_at, claimed_by) "
            "SELECT 'q', '{}'::jsonb, 'processing', NOW() - INTERVAL '2 hours', 'w' "
            "FROM generate_series(1, 5)"
        )
        cur.execute("ANALYZE queue")
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute(
            "EXPLAIN (COSTS OFF) SELECT id FROM queue "
            "WHERE queue_name = 'q' AND status = 'processing' "
            "AND claimed_at < NOW() - INTERVAL '30 minutes'"
        )
        plan = "\n".join(r[0] for r in cur.fetchall())
    conn.rollback()

    assert "queue_name_status_claimed_at" in plan, plan
    assert "Filter: " not in plan, f"queue_name should be an Index Cond, not a Filter:\n{plan}"


@pytest.mark.parametrize(
    ("kind", "ddl"),
    [
        ("view", "CREATE VIEW queue AS SELECT 1 AS id"),
        ("materialized view", "CREATE MATERIALIZED VIEW queue AS SELECT 1 AS id"),
        ("sequence", "CREATE SEQUENCE queue"),
    ],
)
def test_something_that_is_not_a_table_is_named_as_such(conn, kind, ddl) -> None:
    """to_regclass resolving is not proof the thing is a table -- the same
    argument this module makes about index names, one relation kind over.

    Before this check: a matview with the right column names returned
    "present", a view raised a raw WrongObjectType from inside _ensure_indexes,
    and a sequence was reported as a table missing every column it never had.
    Each is a wrong diagnosis of a search_path problem.
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

    with pytest.raises(QueueError, match=kind):
        ensure_queue_table(conn)
    conn.rollback()


def test_a_matview_with_the_right_columns_is_still_not_a_queue(conn) -> None:
    """The contrived case is the one that used to pass silently."""
    with conn.cursor() as cur:
        cur.execute(
            "CREATE MATERIALIZED VIEW queue AS SELECT "
            + ", ".join(f"NULL::text AS {name}" for name in REQUIRED_COLUMNS)
        )
    conn.commit()

    with pytest.raises(QueueError, match="materialized view"):
        ensure_queue_table(conn)
    conn.rollback()


def test_both_entry_points_agree_about_what_a_queue_is(conn) -> None:
    """require_queue_table() promises the contract does not vary by entry
    point. A matview resolves through to_regclass and carries pg_attribute
    rows, so without a relkind check it would accept one -- and
    has_claim_token_column() would then answer True about a matview, a wrong
    branch reported as success.
    """
    from cortex_utils.queue.ops import (
        QueueTableNotFoundError,
        has_claim_token_column,
        require_queue_table,
    )

    with conn.cursor() as cur:
        cur.execute(
            "CREATE MATERIALIZED VIEW queue AS SELECT "
            + ", ".join(f"NULL::text AS {name}" for name in REQUIRED_COLUMNS)
        )
    conn.commit()

    with pytest.raises(QueueError, match="materialized view"):
        missing_columns(conn)
    conn.rollback()
    with pytest.raises(QueueTableNotFoundError):
        require_queue_table(conn)
    conn.rollback()
    with pytest.raises(QueueTableNotFoundError):
        has_claim_token_column(conn)
    conn.rollback()
