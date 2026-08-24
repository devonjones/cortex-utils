"""Live-Postgres tests for the dead-letter lifecycle.

This module is mostly DDL and SQL, and the mock suite asserts statement text a
real server rejects: `'5000s'` passes the lock-bound test, and the first version
of the migration raised `UndefinedColumn` on every existing database while all
27 unit tests stayed green. The upgrade path is the one thing this feature is
for, so it gets tested against a real server.

Skipped unless CORTEX_TEST_DSN points at a throwaway Postgres; CI starts one.
"""

from __future__ import annotations

import contextlib
import os
from datetime import timedelta

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from cortex_utils.queue.dead_letter import DeadLetterManager  # noqa: E402
from cortex_utils.queue.ops import QueueError  # noqa: E402
from cortex_utils.queue.schema import queue_ddl  # noqa: E402

DSN = os.environ.get("CORTEX_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not DSN, reason="set CORTEX_TEST_DSN to a throwaway Postgres to run these"
)

# The table exactly as it shipped before the lifecycle columns existed.
LEGACY_SCHEMA = """
CREATE TABLE dead_letter (
    id BIGSERIAL PRIMARY KEY,
    original_id BIGINT NOT NULL,
    queue_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INT NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    archived_from_partition TEXT NOT NULL
);
CREATE INDEX idx_dead_letter_queue ON dead_letter(queue_name, failed_at DESC);
"""

QUEUE_SCHEMA = queue_ddl()


def _fresh(legacy: bool):
    c = psycopg2.connect(DSN)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS t_dl CASCADE")
        cur.execute("CREATE SCHEMA t_dl")
        cur.execute("SET search_path = t_dl")
        cur.execute(QUEUE_SCHEMA)
        cur.execute(
            "CREATE TABLE queue_today PARTITION OF queue "
            "FOR VALUES FROM (CURRENT_DATE) TO (CURRENT_DATE + 1)"
        )
        if legacy:
            cur.execute(LEGACY_SCHEMA)
    c.autocommit = False
    return c


@pytest.fixture
def legacy_conn():
    c = _fresh(legacy=True)
    try:
        yield c
    finally:
        c.rollback()
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS t_dl CASCADE")
        c.close()


@pytest.fixture
def conn(legacy_conn):
    """A legacy database that has been through the migration."""
    DeadLetterManager(legacy_conn).ensure_table()
    return legacy_conn


def _archive(conn, n: int = 1, queue: str = "triage") -> list[int]:
    ids = []
    with conn.cursor() as cur:
        for i in range(n):
            cur.execute(
                "INSERT INTO dead_letter (original_id, queue_name, payload, attempts, "
                "last_error, created_at, failed_at, archived_from_partition) "
                "VALUES (%s, %s, %s::jsonb, 3, 'visibility timeout', NOW(), NOW(), 'p') "
                "RETURNING id",
                (i + 1, queue, f'{{"n": {i}}}'),
            )
            ids.append(cur.fetchone()[0])
    conn.commit()
    return ids


# --- the upgrade path --------------------------------------------------------


def test_ensure_table_upgrades_a_database_that_already_had_the_table(legacy_conn) -> None:
    """The path this whole feature exists for, and the one that was broken.

    CREATE TABLE IF NOT EXISTS no-ops on an existing table, and IF NOT EXISTS on
    an index guards the NAME, not the predicate -- so a partial index over
    dismissed_at in the create script raised UndefinedColumn against the old
    shape, before the migration that would have added the column ever ran.
    """
    DeadLetterManager(legacy_conn).ensure_table()

    with legacy_conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 't_dl' AND table_name = 'dead_letter'"
        )
        columns = {r[0] for r in cur.fetchall()}
    assert {"retried_at", "retried_as", "dismissed_at"} <= columns


def test_ensure_table_is_safe_to_run_on_every_boot(legacy_conn) -> None:
    mgr = DeadLetterManager(legacy_conn)
    mgr.ensure_table()
    mgr.ensure_table()
    mgr.ensure_table()
    assert mgr.ensure_lifecycle_columns() is False


def test_the_migration_runs_on_a_connection_the_caller_left_in_autocommit(
    legacy_conn,
) -> None:
    """SET LOCAL is a silent no-op with no transaction to be local to, and the
    ALTERs would then commit individually -- so both the lock bound and the
    all-or-nothing property are false exactly where this class does not own the
    connection, which is the cryo case."""
    legacy_conn.autocommit = True
    DeadLetterManager(legacy_conn).ensure_lifecycle_columns()
    assert legacy_conn.autocommit is True, "the caller's setting must be restored"
    legacy_conn.autocommit = False

    with legacy_conn.cursor() as cur:
        cur.execute("SELECT dismissed_at FROM dead_letter LIMIT 0")


def test_a_fresh_database_gets_the_same_shape(legacy_conn) -> None:
    """The create script and the migration must not drift: a new deployment and
    an upgraded one have to end up identical, or G1 is reopened."""
    DeadLetterManager(legacy_conn).ensure_table()
    with legacy_conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 't_dl' AND table_name = 'dead_letter' ORDER BY column_name"
        )
        upgraded = [r[0] for r in cur.fetchall()]
        cur.execute("DROP TABLE dead_letter")
    legacy_conn.commit()

    DeadLetterManager(legacy_conn).ensure_table()
    with legacy_conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 't_dl' AND table_name = 'dead_letter' ORDER BY column_name"
        )
        fresh = [r[0] for r in cur.fetchall()]

    assert upgraded == fresh


# --- retry keeps the record --------------------------------------------------


def test_retry_leaves_the_archive_row_and_stamps_it(conn) -> None:
    [dl_id] = _archive(conn)
    assert DeadLetterManager(conn).retry_job(dl_id) is True

    with conn.cursor() as cur:
        cur.execute("SELECT retried_at, retried_as FROM dead_letter WHERE id = %s", (dl_id,))
        retried_at, retried_as = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM queue")
        queued = cur.fetchone()[0]

    assert retried_at is not None, "the record that work was given up on must survive"
    assert retried_as is not None
    assert queued == 1


def test_a_second_sweep_does_not_re_enqueue_what_the_first_put_back(conn) -> None:
    """The DELETE this replaced was what made retry_jobs() self-limiting. Two
    identical sweeps used to give six queue rows from three dead letters, with
    retried_as overwritten -- erasing the evidence this change exists to keep."""
    _archive(conn, n=3)
    mgr = DeadLetterManager(conn)

    assert mgr.retry_jobs() == 3
    with conn.cursor() as cur:
        cur.execute("SELECT array_agg(retried_as ORDER BY id) FROM dead_letter")
        first = cur.fetchone()[0]

    assert mgr.retry_jobs() == 0, "nothing is left open"

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM queue")
        assert cur.fetchone()[0] == 3, "the second sweep must not double-enqueue"
        cur.execute("SELECT array_agg(retried_as ORDER BY id) FROM dead_letter")
        assert cur.fetchone()[0] == first, "retried_as must not be overwritten"


def test_retrying_a_dismissed_row_does_not_hide_live_work(conn) -> None:
    [dl_id] = _archive(conn)
    mgr = DeadLetterManager(conn)
    mgr.dismiss(dl_id)

    assert mgr.retry_job(dl_id) is False
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM queue")
        assert cur.fetchone()[0] == 0


# --- dismissal ---------------------------------------------------------------


def test_dismiss_is_idempotent_and_keeps_the_first_date(conn) -> None:
    [dl_id] = _archive(conn)
    mgr = DeadLetterManager(conn)

    mgr.dismiss(dl_id)
    with conn.cursor() as cur:
        cur.execute("SELECT dismissed_at FROM dead_letter WHERE id = %s", (dl_id,))
        first = cur.fetchone()[0]

    mgr.dismiss(dl_id)
    with conn.cursor() as cur:
        cur.execute("SELECT dismissed_at FROM dead_letter WHERE id = %s", (dl_id,))
        assert cur.fetchone()[0] == first, "the date answers when we stopped caring"


def test_dismiss_does_not_delete(conn) -> None:
    [dl_id] = _archive(conn)
    DeadLetterManager(conn).dismiss(dl_id)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dead_letter WHERE id = %s", (dl_id,))
        assert cur.fetchone()[0] == 1


# --- one number --------------------------------------------------------------


def test_the_list_and_the_count_agree_through_every_transition(conn) -> None:
    """A page saying 2 while the digest says 6 is worse than either being wrong
    alone: whichever you read last is the one you believe."""
    ids = _archive(conn, n=4)
    mgr = DeadLetterManager(conn)

    def agree() -> int:
        listed = len(mgr.list_jobs())
        counted = mgr.get_stats()["total"]
        assert listed == counted, f"list {listed} vs count {counted}"
        return listed

    assert agree() == 4
    mgr.dismiss(ids[0])
    assert agree() == 3
    mgr.retry_job(ids[1])
    assert agree() == 2, "a retried row is resolved, not still open"
    assert mgr.get_stats(include_resolved=True)["total"] == 4
    assert len(mgr.list_jobs(include_resolved=True)) == 4


def test_get_job_returns_what_the_list_returns(conn) -> None:
    [dl_id] = _archive(conn)
    mgr = DeadLetterManager(conn)
    mgr.dismiss(dl_id)
    job = mgr.get_job(dl_id)
    assert job is not None
    assert job["dismissed_at"] is not None
    assert set(job) >= {"retried_at", "retried_as", "dismissed_at"}


def test_purge_is_by_age_regardless_of_state(conn) -> None:
    ids = _archive(conn, n=2)
    mgr = DeadLetterManager(conn)
    mgr.dismiss(ids[0])
    with conn.cursor() as cur:
        cur.execute("UPDATE dead_letter SET failed_at = NOW() - INTERVAL '40 days'")
    conn.commit()

    assert mgr.purge(timedelta(days=30)) == 2


def test_show_and_retry_work_on_a_database_that_never_ran_the_migration(
    legacy_conn,
) -> None:
    """The round-1 bug through a different door. This PR widened get_job() and
    list_jobs() to select the lifecycle columns, but `dead-letter show` and
    `dead-letter retry` reached them without calling ensure_table() -- so on a
    legacy table they raised UndefinedColumn where the pre-PR versions worked.
    """
    with legacy_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dead_letter (original_id, queue_name, payload, attempts, "
            "last_error, created_at, failed_at, archived_from_partition) "
            "VALUES (1, 'triage', '{}'::jsonb, 3, 'boom', NOW(), NOW(), 'p') RETURNING id"
        )
        dl_id = cur.fetchone()[0]
    legacy_conn.commit()

    from click.testing import CliRunner

    from cortex_utils.cli import main

    dsn = os.environ["CORTEX_TEST_DSN"]
    parts = dict(kv.split("=", 1) for kv in dsn.split())
    env = {
        "POSTGRES_HOST": parts["host"],
        "POSTGRES_PORT": parts["port"],
        "POSTGRES_USER": parts["user"],
        "POSTGRES_PASSWORD": parts["password"],
        "POSTGRES_DB": parts["dbname"],
        "PGOPTIONS": "-c search_path=t_dl",
    }
    runner = CliRunner()
    for args in (["dead-letter", "show", str(dl_id)], ["dead-letter", "retry", "--id", str(dl_id)]):
        result = runner.invoke(main, args, env=env)
        assert result.exit_code == 0, f"{args} -> {result.output}\n{result.exception}"


def test_health_and_get_stats_never_report_different_numbers(conn) -> None:
    """A dashboard reading health() and a digest reading get_stats() must agree.
    Two human-facing views of one number that filter differently is worse than
    either being wrong alone: whichever you read last is the one you believe.
    """
    from cortex_utils.queue.inspect import health

    ids = _archive(conn, n=4)
    mgr = DeadLetterManager(conn)

    def agree() -> int:
        a = health(conn).dead_letter
        b = mgr.get_stats()["total"]
        c = len(mgr.list_jobs())
        assert a == b == c, f"health {a} / stats {b} / list {c}"
        return a

    assert agree() == 4
    mgr.dismiss(ids[0])
    assert agree() == 3
    mgr.retry_job(ids[1])
    assert agree() == 2


def test_health_works_on_a_dead_letter_table_that_predates_the_migration(
    legacy_conn,
) -> None:
    """health() is read-only and gets called during the upgrade window. Naming
    the lifecycle columns directly would make it raise UndefinedColumn there --
    the same way show/retry did before round 2. Every row counts as open, which
    is right: nothing can have been dismissed on a schema with nowhere to
    record it."""
    from cortex_utils.queue.inspect import health

    with legacy_conn.cursor() as cur:
        for i in range(3):
            cur.execute(
                "INSERT INTO dead_letter (original_id, queue_name, payload, attempts, "
                "last_error, created_at, failed_at, archived_from_partition) "
                "VALUES (%s, 'triage', '{}'::jsonb, 3, 'boom', NOW(), NOW(), 'p')",
                (i,),
            )
    legacy_conn.commit()

    assert health(legacy_conn).dead_letter == 3


def test_two_concurrent_retries_cannot_both_win(legacy_conn) -> None:
    """The guards read through get_job() and decide in Python. Without the same
    conditions on the UPDATE, both callers pass the read, both write, the job
    runs twice and retried_as is overwritten -- destroying the record this
    change exists to keep, while both are told it worked.
    """
    DeadLetterManager(legacy_conn).ensure_table()
    [dl_id] = _archive(legacy_conn)

    b = psycopg2.connect(DSN)
    try:
        with b.cursor() as cur:
            cur.execute("SET search_path = t_dl")
        b.commit()

        mgr_a = DeadLetterManager(legacy_conn)
        mgr_b = DeadLetterManager(b)

        # B's read happens before A commits, so B sees the row as open. Pinning
        # that stale answer is the point: leave it live and B's own re-read
        # catches the conflict, the UPDATE's WHERE is never exercised, and the
        # suite looks green while the database is the only thing that could
        # actually have arbitrated. Verified: without the pin, removing the
        # WHERE clause still passes.
        stale = mgr_b.get_job(dl_id)
        assert stale["retried_at"] is None
        mgr_b.get_job = lambda _id: dict(stale)  # type: ignore[method-assign]

        won = [mgr_a.retry_job(dl_id), mgr_b.retry_job(dl_id)]

        assert won.count(True) == 1, f"exactly one may win, got {won}"
        with legacy_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM queue")
            assert cur.fetchone()[0] == 1, "the work must not be queued twice"
            cur.execute("SELECT retried_as FROM dead_letter WHERE id = %s", (dl_id,))
            assert cur.fetchone()[0] is not None
    finally:
        b.rollback()
        b.close()


def test_the_cli_says_why_it_did_not_retry(conn) -> None:
    """retry_job returns False for three different reasons and the CLI used to
    report all of them as "not found", exit 0 -- telling an operator a row they
    can see in the list does not exist."""
    from click.testing import CliRunner

    from cortex_utils.cli import main

    [dl_id] = _archive(conn)
    mgr = DeadLetterManager(conn)
    mgr.dismiss(dl_id)

    parts = dict(kv.split("=", 1) for kv in os.environ["CORTEX_TEST_DSN"].split())
    env = {
        "POSTGRES_HOST": parts["host"],
        "POSTGRES_PORT": parts["port"],
        "POSTGRES_USER": parts["user"],
        "POSTGRES_PASSWORD": parts["password"],
        "POSTGRES_DB": parts["dbname"],
        "PGOPTIONS": "-c search_path=t_dl",
    }
    result = CliRunner().invoke(main, ["dead-letter", "retry", "--id", str(dl_id)], env=env)

    assert result.exit_code == 1, "a refusal must not exit 0"
    assert "dismissed" in result.output, result.output
    assert "not found" not in result.output


def test_the_cli_reports_a_genuinely_missing_row_as_missing(conn) -> None:
    from click.testing import CliRunner

    from cortex_utils.cli import main

    parts = dict(kv.split("=", 1) for kv in os.environ["CORTEX_TEST_DSN"].split())
    env = {
        "POSTGRES_HOST": parts["host"],
        "POSTGRES_PORT": parts["port"],
        "POSTGRES_USER": parts["user"],
        "POSTGRES_PASSWORD": parts["password"],
        "POSTGRES_DB": parts["dbname"],
        "PGOPTIONS": "-c search_path=t_dl",
    }
    result = CliRunner().invoke(main, ["dead-letter", "retry", "--id", "9999"], env=env)
    assert result.exit_code == 1
    assert "no dead letter job with id 9999" in result.output


def test_a_steady_state_boot_issues_no_dead_letter_ddl(conn) -> None:
    """The one step that did not pre-check itself.

    CREATE TABLE IF NOT EXISTS plus two CREATE INDEX IF NOT EXISTS ran
    unconditionally while the very next line checked pg_attribute first -- and
    by this module's own account CREATE INDEX IF NOT EXISTS still takes a lock
    and waits on an open writer even when the index is already there. On every
    boot of every consumer, forever, and outside any lock_timeout the caller
    set, because it opened its own cursor.
    """
    mgr = DeadLetterManager(conn)
    mgr.ensure_table()

    other = psycopg2.connect(DSN)
    try:
        other.autocommit = True
        with other.cursor() as cur:
            cur.execute("SET search_path = t_dl")
        other.autocommit = False
        # A live writer holding ROW EXCLUSIVE. DDL would queue behind it.
        with other.cursor() as cur:
            cur.execute(
                "INSERT INTO dead_letter (original_id, queue_name, payload, attempts, "
                "last_error, created_at, failed_at, archived_from_partition) "
                "VALUES (1, 't', '{}'::jsonb, 1, 'x', NOW(), NOW(), 'p')"
            )

        conn.rollback()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '2000ms'")
        conn.autocommit = False

        mgr.ensure_table()  # must not wait on the writer
    finally:
        other.rollback()
        other.close()
    conn.rollback()


def test_a_missing_dead_letter_table_is_still_created(conn) -> None:
    """The pre-check must not turn into a skip."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE dead_letter")
    conn.commit()

    DeadLetterManager(conn).ensure_table()

    # The thing, not the name. to_regclass resolving proves a name is taken,
    # which is the inference _index_present()'s own docstring refuses -- so
    # check the columns and the indexes actually arrived.
    assert missing_dead_letter_columns(conn) == []
    assert _indexes(conn) >= {
        "idx_dead_letter_queue",
        "idx_dead_letter_created",
        "idx_dead_letter_open",
    }


def _indexes(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE i.indrelid = to_regclass('dead_letter') AND i.indisvalid"
        )
        return {r[0] for r in cur.fetchall()}


def missing_dead_letter_columns(conn) -> list[str]:
    from cortex_utils.queue.dead_letter import LIFECYCLE_COLUMNS

    with conn.cursor() as cur:
        cur.execute(
            "SELECT attname FROM pg_attribute WHERE attrelid = to_regclass('dead_letter') "
            "AND attnum > 0 AND NOT attisdropped"
        )
        present = {r[0] for r in cur.fetchall()}
    return [c for c in LIFECYCLE_COLUMNS if c not in present]


@pytest.mark.parametrize(
    "index",
    ["idx_dead_letter_queue", "idx_dead_letter_created", "idx_dead_letter_open"],
)
def test_a_dropped_index_comes_back_on_the_next_boot(conn, index: str) -> None:
    """The table existing is not evidence that its indexes do.

    The base indexes used to ride along inside DEAD_LETTER_SCHEMA, so gating
    that whole script on "does the table exist" meant a dropped index never
    came back -- on any later boot, forever, because nothing else in the
    package creates them. ensure_table() returned normally and logged
    "Ensured dead_letter table exists" throughout.
    """
    mgr = DeadLetterManager(conn)
    mgr.ensure_table()
    assert index in _indexes(conn)

    with conn.cursor() as cur:
        cur.execute(f"DROP INDEX {index}")
    conn.commit()
    assert index not in _indexes(conn)

    mgr.ensure_table()
    assert index in _indexes(conn), f"{index} was never recreated"


def test_ensure_table_works_on_an_autocommit_connection(conn) -> None:
    """SET LOCAL is a silent no-op with no transaction to be local to, so the
    lock bound would not apply on exactly the connection shape a consumer is
    most likely to hand us."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE dead_letter")
    conn.commit()

    conn.autocommit = True
    try:
        mgr = DeadLetterManager(conn)
        mgr.ensure_table()
        # Boot twice. The first call does DDL, and its incidental commit closes
        # the transaction the catalogue probes opened -- so "repeated boot" and
        # "autocommit connection" were each covered while their intersection,
        # which is production, was not. On the steady-state path nothing
        # commits, and restoring autocommit inside a transaction raises.
        mgr.ensure_table()
        mgr.ensure_table()
        # Read it BEFORE the cleanup below writes it. Asserting after the
        # finally re-reads a value this test just set, so it cannot fail --
        # verified: adding a silent `self.conn.autocommit = False` at the end
        # of ensure_table left the whole suite green while it stole a
        # consumer's connection setting, which is the exact damage this is for.
        survived = conn.autocommit
    finally:
        conn.autocommit = False

    assert survived is True, "ensure_table kept the caller's connection non-autocommit"

    assert _indexes(conn) >= {"idx_dead_letter_queue", "idx_dead_letter_created"}


def test_the_lock_bound_applies_on_an_autocommit_connection(legacy_conn) -> None:
    """SET LOCAL is a silent no-op with no transaction to be local to. Asserting
    only that the table gets created misses that: the DDL still succeeds, it
    just succeeds *unbounded*, so a boot against a busy table hangs instead of
    failing. The guard's effect is the bound, so the bound is what to assert.
    """
    import threading

    with legacy_conn.cursor() as cur:
        cur.execute("DROP TABLE dead_letter")
    legacy_conn.commit()

    other = psycopg2.connect(DSN)
    try:
        other.autocommit = True
        with other.cursor() as cur:
            cur.execute("SET search_path = t_dl")
            # Hold the name so CREATE TABLE must wait.
            cur.execute("BEGIN; CREATE TABLE dead_letter (x int)")

        legacy_conn.autocommit = True
        outcome: dict[str, object] = {}

        def boot() -> None:
            try:
                DeadLetterManager(legacy_conn).ensure_table()
                outcome["r"] = "returned"
            except Exception as exc:  # noqa: BLE001 -- reported, not swallowed
                outcome["r"] = exc

        t = threading.Thread(target=boot, daemon=True)
        t.start()
        t.join(timeout=20)
        assert not t.is_alive(), (
            "still waiting after 20s -- SET LOCAL was a no-op under autocommit "
            "and the DDL ran unbounded"
        )
    finally:
        with contextlib.suppress(Exception):
            other.rollback()
        other.close()
        legacy_conn.autocommit = False
    with contextlib.suppress(Exception):
        legacy_conn.rollback()


def test_a_view_named_dead_letter_is_not_mistaken_for_the_table(legacy_conn) -> None:
    """to_regclass resolving proves a name is taken. Without the relkind check
    _has_table() says True for a view, ensure_table() skips creation entirely,
    and every later call reports success against something that is not the
    table -- the same inference this package refuses elsewhere."""
    with legacy_conn.cursor() as cur:
        cur.execute("DROP TABLE dead_letter")
        cur.execute("CREATE VIEW dead_letter AS SELECT 1 AS id")
    legacy_conn.commit()

    # A raw psycopg2 error would happen either way -- without the relkind check
    # we skip creation and fail several statements later on an index against a
    # view. What the check buys is a message that names what was found.
    with pytest.raises(QueueError, match="not a table"):
        DeadLetterManager(legacy_conn).ensure_table()
    legacy_conn.rollback()


def test_an_index_of_that_name_in_another_schema_is_not_ours(conn) -> None:
    """to_regclass(name) proves a name resolves somewhere on the search_path,
    not that the index is on THIS table.

    Under `search_path = app, shared`, an unrelated index of the same name in
    `shared` made the gate answer "present" and the index was never created --
    on any boot, forever, while ensure_table() logged success. Multi-schema is
    this package's normal deployment: every test in this file runs under a SET
    search_path.
    """
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS neighbour CASCADE")
        cur.execute("CREATE SCHEMA neighbour")
        cur.execute("CREATE TABLE neighbour.other (x int)")
        cur.execute("CREATE INDEX idx_dead_letter_queue ON neighbour.other (x)")
        cur.execute("DROP INDEX idx_dead_letter_queue")  # ours, so it is missing
        cur.execute("SET search_path = t_dl, neighbour")
    conn.commit()

    DeadLetterManager(conn).ensure_table()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relname = 'idx_dead_letter_queue' "
            "AND i.indrelid = to_regclass('t_dl.dead_letter')"
        )
        assert cur.fetchone()[0] == 1, "the neighbour's index was mistaken for ours"
        cur.execute("SET search_path = t_dl")
        cur.execute("DROP SCHEMA neighbour CASCADE")
    conn.commit()


def test_an_index_dropped_and_its_name_taken_is_caught_not_logged_as_created(conn) -> None:
    """The case three rewrites walked past.

    The suite tested "index present" and "index dropped, name free". Neither
    exercises what IF NOT EXISTS actually guards, which is the NAME: hand the
    name to an index on a DIFFERENT table and the CREATE returns normally with
    a NOTICE while idx_dead_letter_queue stays absent. Without the second probe
    that is a boot logging "Created dead_letter index" about nothing, taking
    the lock again every boot forever.

    Not covered by the other-schema test next door -- index names are unique
    per schema, so the CREATE succeeds there.
    """
    DeadLetterManager(conn).ensure_table()
    with conn.cursor() as cur:
        cur.execute("DROP INDEX idx_dead_letter_queue")
        cur.execute("CREATE TABLE decoy (queue_name TEXT)")
        cur.execute("CREATE INDEX idx_dead_letter_queue ON decoy (queue_name)")
    conn.commit()

    with pytest.raises(QueueError, match="did not create an index named"):
        DeadLetterManager(conn).ensure_table()


def test_the_index_lock_bound_applies_too(legacy_conn) -> None:
    """The bound on the CREATE TABLE path was asserted; the CREATE INDEX path
    was not, and it is the one every steady-state boot reaches. CREATE INDEX
    takes a ShareLock, so an open writer blocks it -- unbounded, that is a boot
    that hangs instead of failing, holding the schema advisory lock while it
    does.
    """
    import threading

    DeadLetterManager(legacy_conn).ensure_table()
    with legacy_conn.cursor() as cur:
        cur.execute("DROP INDEX idx_dead_letter_queue")
    legacy_conn.commit()

    other = psycopg2.connect(DSN)
    try:
        other.autocommit = True
        with other.cursor() as cur:
            cur.execute("SET search_path = t_dl")
            cur.execute("BEGIN; LOCK TABLE dead_letter IN ROW EXCLUSIVE MODE")

        legacy_conn.autocommit = True
        outcome: dict[str, object] = {}

        def boot() -> None:
            try:
                DeadLetterManager(legacy_conn).ensure_table()
                outcome["r"] = "returned"
            except Exception as exc:  # noqa: BLE001 -- reported, not swallowed
                outcome["r"] = exc

        t = threading.Thread(target=boot, daemon=True)
        t.start()
        t.join(timeout=20)
        assert not t.is_alive(), "still waiting after 20s -- the CREATE INDEX ran without the bound"
        assert isinstance(outcome["r"], psycopg2.errors.LockNotAvailable), outcome
    finally:
        with contextlib.suppress(Exception):
            other.rollback()
        other.close()
        legacy_conn.autocommit = False
    with contextlib.suppress(Exception):
        legacy_conn.rollback()


def test_the_lifecycle_index_bound_survives_ensure_table_restoring_autocommit(
    legacy_conn,
) -> None:
    """ensure_lifecycle_columns() is called AFTER ensure_table() has put
    autocommit back, so it is the one index site that actually reaches the
    autocommit branch -- the loop inside ensure_table never does, because the
    outer toggle has already fired. SET LOCAL there would be a silent no-op and
    this path would create its index unbounded.
    """
    import threading

    DeadLetterManager(legacy_conn).ensure_table()
    with legacy_conn.cursor() as cur:
        cur.execute("DROP INDEX idx_dead_letter_open")
    legacy_conn.commit()

    other = psycopg2.connect(DSN)
    try:
        other.autocommit = True
        with other.cursor() as cur:
            cur.execute("SET search_path = t_dl")
            cur.execute("BEGIN; LOCK TABLE dead_letter IN ROW EXCLUSIVE MODE")

        legacy_conn.autocommit = True
        outcome: dict[str, object] = {}

        def go() -> None:
            try:
                DeadLetterManager(legacy_conn).ensure_lifecycle_columns()
                outcome["r"] = "returned"
            except Exception as exc:  # noqa: BLE001 -- reported, not swallowed
                outcome["r"] = exc

        t = threading.Thread(target=go, daemon=True)
        t.start()
        t.join(timeout=20)
        assert not t.is_alive(), (
            "still waiting after 20s -- autocommit was left on and SET LOCAL "
            "had no transaction to be local to"
        )
        assert isinstance(outcome["r"], psycopg2.errors.LockNotAvailable), outcome
    finally:
        with contextlib.suppress(Exception):
            other.rollback()
        other.close()
        legacy_conn.autocommit = False
    with contextlib.suppress(Exception):
        legacy_conn.rollback()
