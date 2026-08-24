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
    with pytest.raises(QueueError, match="claimed_by"):
        ensure_queue_table(conn)

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
    assert "idx_queue_claimable" in names
    assert "idx_queue_processing" in names


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
