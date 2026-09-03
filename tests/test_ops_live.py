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
from datetime import datetime, timedelta

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from cortex_utils.queue.ops import claim, complete, enqueue  # noqa: E402
from cortex_utils.queue.schema import queue_ddl  # noqa: E402

DSN = os.environ.get("CORTEX_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not DSN, reason="set CORTEX_TEST_DSN to a throwaway Postgres to run these"
)

SCHEMA = "t_ops_live"

QUEUE_DDL = queue_ddl()


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

    # The mock tests feed the RETURNING tuple in themselves, in the code's own
    # expected order, and the SQL-grep test only proves a column is requested --
    # neither can see a POSITION swap. Swapping attempts and created_at in the
    # RETURNING list passes all 348 while putting a datetime in `attempts`,
    # which fail_or_retry does arithmetic on. Only a real cursor can catch it.
    for row in got:
        assert isinstance(row["id"], int)
        assert isinstance(row["attempts"], int), f"attempts is {type(row['attempts'])}"
        assert isinstance(row["priority"], int)
        assert isinstance(row["created_at"], datetime)
        assert isinstance(row["payload"], dict)
        assert row["queue_name"] == "q"

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


class _FailingCursor:
    """A real cursor that raises OUR exception on one statement.

    A psycopg2 error would abort the transaction server-side and release the
    lock by itself -- the case that never needed fixing. This raises before the
    statement reaches the server, which is the case that leaks.
    """

    def __init__(self, real, trigger: str):
        self._real = real
        self._trigger = trigger

    def execute(self, sql, params=None):
        if self._trigger in sql:
            raise RuntimeError("a bug in our own code, not the server's")
        return self._real.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return self._real.__exit__(*a)


class _FailingConn:
    def __init__(self, real, trigger: str):
        self._real = real
        self._trigger = trigger

    def cursor(self, *a, **k):
        return _FailingCursor(self._real.cursor(*a, **k), self._trigger)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_drop_partition_releases_the_lock_when_our_own_code_raises(conns):
    """The behaviour the rollback actually buys, end to end on a real server.

    Without it the backend sits `idle in transaction` holding SHARE ROW
    EXCLUSIVE on the partition, and the next writer gets LockNotAvailable.
    """
    from cortex_utils.queue.partitions import PartitionManager

    a, b = conns
    with a.cursor() as cur:
        cur.execute("SELECT CURRENT_DATE")
        today = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status) VALUES ('q', '{}'::jsonb, 'failed')"
        )
    a.commit()
    partition = f"queue_{today:%Y_%m_%d}"

    # Fail between the LOCK and the commit, in our code rather than the server's.
    manager = PartitionManager(_FailingConn(a, "INSERT INTO dead_letter"))
    with pytest.raises(RuntimeError):
        manager.drop_partition(today)

    # The partition must be free: another session can take ACCESS EXCLUSIVE.
    with b.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '3s'")
        cur.execute(f"LOCK TABLE {partition} IN ACCESS EXCLUSIVE MODE")
    b.rollback()

    with a.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
            "WHERE c.relname = %s AND l.mode = 'ShareRowExclusiveLock'",
            (partition,),
        )
        assert cur.fetchone()[0] == 0, "the lock outlived the failure"


def test_a_worker_that_dies_on_the_last_attempt_is_retired_not_left_processing(
    conns,
) -> None:
    """claim()'s recovery branch retires a row whose attempts are spent instead
    of handing it back out. Nothing pinned that branch, and disabling it left
    all 470 other tests green.

    The cost of it not firing is not one stuck row. The row stays 'processing'
    forever, so: it never becomes 'failed', so failures() never lists it and no
    operator sees it; it is never archived to dead_letter, so the payload is
    only in the partition; and drop_partition skips any partition holding a
    processing row, so retention stops dropping that day -- permanently, and
    silently. One dead branch, three modules of consequence.

    Recovery only runs when someone claims on that queue, so a paused or dead
    queue never self-heals either.
    """
    a, _ = conns
    with a.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, claimed_at, attempts, "
            "max_attempts) VALUES ('q', '{}'::jsonb, 'processing', "
            "NOW() - INTERVAL '40 minutes', 3, 3) RETURNING id"
        )
        job_id = cur.fetchone()[0]
    a.commit()

    # The claim finds nothing to hand out -- the point is what it did on the way.
    assert claim(a, "q", worker="w1") == []

    with a.cursor() as cur:
        cur.execute("SELECT status, last_error FROM queue WHERE id = %s", (job_id,))
        status, last_error = cur.fetchone()
    a.commit()
    assert status == "failed", (
        f"row left {status!r} with its attempts spent -- it will never surface in "
        "failures(), never reach dead_letter, and will pin its partition against "
        "retention for good"
    )
    assert last_error, "retired without saying why"


def test_a_per_connection_timezone_override_is_reported(conns, capsys) -> None:
    """Partition routing rests on every connection agreeing about what day it
    is. CURRENT_DATE and the FROM/TO bounds on a TIMESTAMPTZ column are both
    TimeZone-dependent, so two connections that disagree put each day's boundary
    rows in the wrong partition -- and drop_old_partitions compares a
    name-derived date against a differently-framed cutoff, so it drops silently
    rather than failing loudly.

    PGOPTIONS is already the schema-selection knob here (`-c search_path=cryo`)
    and carries `-c timezone=` too, so this is one env var away. The invariant
    used to live only in a docstring.

    A warning rather than an error: a deployment where every connection sets the
    same zone this way is consistent. What one connection cannot check is
    whether the others agree, so this reports the thing that makes disagreement
    possible and leaves the judgement to whoever set it.
    """
    from cortex_utils.queue.ops import server_today

    # capsys, not caplog: this package logs through its own stderr logger rather
    # than the stdlib root, precisely so importing it does not reconfigure a
    # consumer's logging. caplog would see nothing and the test would pass while
    # asserting nothing.
    a, _ = conns
    server_today(a)
    assert "TimeZone" not in capsys.readouterr().err, "a server-inherited zone is not a finding"

    with a.cursor() as cur:
        cur.execute("SET TIME ZONE 'America/New_York'")
    server_today(a)
    assert "TimeZone is overridden per connection" in capsys.readouterr().err
    a.rollback()

    # PGOPTIONS is what the docstring is actually about, and it lands as
    # source='client', not 'session'. Covering only the SET TIME ZONE shape
    # meant 'client' could be deleted from the predicate with the suite green --
    # the named cause untested, the incidental one pinned.
    opts = psycopg2.connect(DSN, options="-c timezone=America/New_York")
    try:
        capsys.readouterr()
        server_today(opts)
        assert "TimeZone is overridden per connection" in capsys.readouterr().err
    finally:
        opts.close()

    # And the one that is NOT a finding: a zone every connection inherits from
    # the server is consistent by construction.
    b = psycopg2.connect(DSN)
    try:
        capsys.readouterr()
        server_today(b)
        assert "TimeZone" not in capsys.readouterr().err
    finally:
        b.close()


def test_complete_can_leave_the_transaction_to_the_caller(conns) -> None:
    """A consumer whose job writes its own rows needs the completion to land
    with them or not at all.

    Committing inside complete() would let the work commit and the completion
    roll back, or the reverse -- the job then either runs twice or is lost.
    enqueue() has offered this contract for the same reason; complete() did not,
    so postmark's attachment worker kept hand-writing its completion inside the
    transaction that inserts the attachment row.
    """
    a, _ = conns
    with a.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status, claimed_at, claimed_by) "
            "VALUES ('q', '{}'::jsonb, 'processing', NOW(), 'w1') RETURNING id"
        )
        job_id = cur.fetchone()[0]
    a.commit()

    assert complete(a, job_id, "w1", commit=False) is True
    a.rollback()

    with a.cursor() as cur:
        cur.execute("SELECT status FROM queue WHERE id = %s", (job_id,))
        assert cur.fetchone()[0] == "processing", (
            "commit=False must leave the completion to the caller -- rolling "
            "back has to take it with it"
        )
    a.commit()

    assert complete(a, job_id, "w1", commit=False) is True
    a.commit()
    with a.cursor() as cur:
        cur.execute("SELECT status FROM queue WHERE id = %s", (job_id,))
        assert cur.fetchone()[0] == "completed"
    a.commit()
