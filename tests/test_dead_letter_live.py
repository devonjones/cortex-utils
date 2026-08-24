"""Live-Postgres tests for the dead-letter lifecycle.

This module is mostly DDL and SQL, and the mock suite asserts statement text a
real server rejects: `'5000s'` passes the lock-bound test, and the first version
of the migration raised `UndefinedColumn` on every existing database while all
27 unit tests stayed green. The upgrade path is the one thing this feature is
for, so it gets tested against a real server.

Skipped unless CORTEX_TEST_DSN points at a throwaway Postgres; CI starts one.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from cortex_utils.queue.dead_letter import DeadLetterManager  # noqa: E402

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

QUEUE_SCHEMA = """
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
