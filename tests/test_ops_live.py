"""Live-Postgres tests for the three concurrency claims in ops.py.

The Known Limitations section names exactly these as unassertable against a
mock, and it is right: `SKIP LOCKED` not double-claiming, advisory-lock
serialisation of same-key producers, and the partition-creation race all live
in the database's behaviour rather than in ours. A mock returns whatever the
code asked it for, so a test that stubs the cursor confirms the SQL was
*written*, never that Postgres *does* it.

These use two real connections and race them, following the harness
`test_inspect_live.py` established. Skipped unless a throwaway Postgres is
reachable, so the default suite stays fast and offline:

    docker run -d --rm --name pgtest -e POSTGRES_PASSWORD=x -p 55432:5432 postgres:16
    CORTEX_TEST_DSN="host=127.0.0.1 port=55432 user=postgres password=x dbname=postgres" \
        uv run pytest tests/test_ops_live.py

Contributed from cryo's integration (cryo-64); cryo runs an equivalent suite as
`jobs/selfcheck_queue.sh` against a container it starts itself.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from cortex_utils.queue.ops import claim, enqueue  # noqa: E402

DSN = os.environ.get("CORTEX_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not DSN, reason="set CORTEX_TEST_DSN to a throwaway Postgres to run these"
)

SCHEMA = "t_ops_live"

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
"""


def _fresh(autocommit: bool = False):
    c = psycopg2.connect(DSN)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute(f"SET search_path = {SCHEMA}")
    c.autocommit = autocommit
    return c


@pytest.fixture
def conns():
    """TWO connections on one schema — one is not enough to race anything."""
    setup = psycopg2.connect(DSN)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {SCHEMA}")
        cur.execute(f"SET search_path = {SCHEMA}")
        cur.execute(QUEUE_DDL)
    with setup.cursor() as cur:
        cur.execute("SELECT CURRENT_DATE")
        day = cur.fetchone()[0]
        cur.execute(
            f"CREATE TABLE queue_{day:%Y_%m_%d} PARTITION OF queue "
            f"FOR VALUES FROM ('{day}') TO ('{day + timedelta(days=1)}')"
        )
    a, b = _fresh(), _fresh()
    try:
        yield a, b
    finally:
        # cancel() is what matters, and it is not belt-and-braces: close()
        # alone hangs just as long as rollback() would -- both wait on a thread
        # still inside execute() on that connection, measured at >20s. cancel()
        # returns immediately and the thread gets QueryCanceled. A teardown that
        # hangs turns a failing test into a hung runner, which nobody reads as a
        # test result.
        for c in (a, b):
            try:
                c.cancel()
                c.close()
            except Exception:  # noqa: BLE001 -- teardown must not mask the failure
                pass
        with setup.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        setup.close()


def test_skip_locked_never_double_claims(conns):
    """A claim must SKIP rows another transaction holds, not wait for them.

    Racing this properly matters. An earlier version of this test called
    claim() on both connections in sequence and passed even with SKIP LOCKED
    removed — because claim() commits internally, so the first connection had
    already released its locks before the second ran. It proved nothing.

    So the contention is created directly: connection A locks three rows in an
    OPEN transaction, and B then claims with a short statement_timeout. With
    SKIP LOCKED, B steps over them and returns the rest. Without it, B queues
    behind A's locks and the timeout fires — which is the assertion.
    """
    a, b = conns
    ids = [enqueue(a, "q", {"n": i}) for i in range(6)]
    a.commit()

    with a.cursor() as cur:  # held open, deliberately, for the whole test
        cur.execute(
            "SELECT id FROM queue WHERE id = ANY(%s) ORDER BY id LIMIT 3 FOR UPDATE",
            (ids,),
        )
        locked = {r[0] for r in cur.fetchall()}
    assert len(locked) == 3

    with b.cursor() as cur:
        # without this, a regression HANGS the suite instead of failing it
        cur.execute("SET LOCAL statement_timeout = '3s'")
    got = claim(b, "q", "worker-b", limit=6)
    b.commit()
    a.rollback()

    claimed = {r["id"] for r in got}
    assert claimed, "B must claim the rows A is not holding"
    assert not (claimed & locked), f"claimed a row A had locked: {claimed & locked}"
    assert claimed == set(ids) - locked


def test_advisory_lock_serialises_same_key_producers(conns):
    """Concurrent same-key enqueues must produce exactly one row.

    The partial unique index cannot backstop this: partitioning forces
    created_at into every unique key, so two producers in different
    transactions get different timestamps and BOTH inserts satisfy it. The
    advisory xact lock is the only thing between that and a double-queued job.

    This must be genuinely concurrent. An earlier version committed A before B
    ran, so B's NOT EXISTS saw the committed row and deduped without ever
    needing the lock — it passed with the lock removed. Here B runs in a
    thread WHILE A holds the lock uncommitted, which is the only arrangement
    that can tell the two apart.
    """
    import threading

    a, b = conns
    first = enqueue(a, "q", {"vid": "same"}, dedup_key="vid", commit=False)
    assert first is not None

    result: dict[str, object] = {}

    def producer_b() -> None:
        try:
            result["id"] = enqueue(b, "q", {"vid": "same"}, dedup_key="vid")
        except Exception as exc:  # noqa: BLE001 — reported, not swallowed
            result["error"] = exc

    with b.cursor() as cur:
        # Without this, a regression where the lock is gone and B instead blocks
        # on something else hangs the suite rather than failing it -- the same
        # guard the SKIP LOCKED test above already has.
        cur.execute("SET LOCAL statement_timeout = '10s'")

    t = threading.Thread(target=producer_b, daemon=True)
    t.start()
    t.join(timeout=2)
    # B must still be blocked on the advisory lock A holds. If it finished, it
    # never waited — which is what happens when the lock is gone.
    assert t.is_alive(), "B did not block on the advisory lock A holds"

    a.commit()  # releases the xact lock; B proceeds and sees the row
    t.join(timeout=10)
    assert not t.is_alive(), "B never completed after A committed"
    assert "error" not in result, f"B failed: {result.get('error')}"
    assert result["id"] is None, "the second producer must dedup, not insert"

    with a.cursor() as cur:
        cur.execute("SELECT count(*) FROM queue WHERE payload->>'vid' = 'same'")
        assert cur.fetchone()[0] == 1, "exactly one row may exist for a dedup key"


def test_missing_partition_self_heals_once_under_concurrency(conns):
    """Two producers hitting a missing partition: both land, one partition.

    This does NOT reproduce the DuplicateTable interleaving, and saying so
    matters more than the test. Two attempts failed: forcing it with an
    uncommitted CREATE on A blocks B on the parent's ACCESS EXCLUSIVE at the
    INSERT, before it can reach CheckViolation at all; and four producers behind
    a barrier came back clean 12 times out of 12, with one reporting `created`
    and the rest `present` because _ensure_partition's pre-check had already
    seen the winner's committed partition.

    What it does cover is still un-mockable and worth having: two real
    connections, a missing partition, both rows landing, exactly one partition
    created. See Known Limitations for the branch that remains unreached.
    """
    a, b = conns
    with a.cursor() as cur:
        cur.execute("SELECT CURRENT_DATE")
        today = cur.fetchone()[0]
        cur.execute(f"DROP TABLE queue_{today:%Y_%m_%d}")
    a.commit()

    id_a = enqueue(a, "q", {"n": "a"})
    id_b = enqueue(b, "q", {"n": "b"})
    assert id_a and id_b, "both enqueues must survive a missing partition"

    with a.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s",
            (SCHEMA, f"queue_{today:%Y_%m_%d}"),
        )
        assert cur.fetchone()[0] == 1, "exactly one partition, created once"
        cur.execute("SELECT count(*) FROM queue")
        assert cur.fetchone()[0] == 2, "both rows must be readable"
