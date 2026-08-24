"""Live-Postgres coverage for the claimed_by migration guard.

Every unit test of this guard runs against a fake exception whose `diag` is a
stub this repo defines. But the guard's correctness rests on a fact about
*Postgres* -- that SQLSTATE 42703 populates `diag.message_primary` with the
offending column name -- and a stub built to have that shape cannot testify to
it. If `message_primary` ever came back empty the `or ""` fall-through would
turn every primitive back into a raw `UndefinedColumn` and the whole suite would
stay green: the guard would silently become a no-op.

So this asks a real server, on a real pre-migration schema.
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
from cortex_utils.queue.schema import queue_ddl  # noqa: E402

DSN = os.environ.get("CORTEX_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not DSN, reason="set CORTEX_TEST_DSN to a throwaway Postgres to run these"
)

# The queue as it stands before ensure_claim_token_column() has ever run.
# Derived from the canonical DDL with exactly one column removed, rather than
# hand-written: the omission is the point of this file, and everything else
# about the shape should track the real one so a change there is not silently
# untested here.
PRE_MIGRATION_DDL = "\n".join(line for line in queue_ddl().splitlines() if "claimed_by" not in line)


@pytest.fixture
def unmigrated():
    c = psycopg2.connect(DSN)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS t_token CASCADE")
        cur.execute("CREATE SCHEMA t_token")
        cur.execute("SET search_path = t_token")
        cur.execute(PRE_MIGRATION_DDL)
        cur.execute(
            "CREATE TABLE queue_today PARTITION OF queue "
            "FOR VALUES FROM (CURRENT_DATE) TO (CURRENT_DATE + 1)"
        )
    c.autocommit = False
    try:
        yield c
    finally:
        c.rollback()
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS t_token CASCADE")
        c.close()


@pytest.mark.parametrize(
    ("name", "call"),
    [
        ("claim", lambda c: claim(c, "q", "worker-a")),
        ("complete", lambda c: complete(c, 1, "worker-a")),
        ("release", lambda c: release(c, 1, 60, "worker-a")),
        ("fail_or_retry", lambda c: fail_or_retry(c, 1, "boom", "worker-a")),
    ],
)
def test_every_primitive_names_the_remedy_on_a_real_unmigrated_schema(
    unmigrated, name, call
) -> None:
    """All four at once, because a half-guard reads as safe and is not."""
    with pytest.raises(QueueError, match="ensure_claim_token_column") as excinfo:
        call(unmigrated)
    assert "search_path" in str(excinfo.value), name
    unmigrated.rollback()


def test_the_connection_is_usable_afterwards(unmigrated) -> None:
    """Relabelling must not skip the rollback: a long-lived worker would then
    fail every later job for a reason unrelated to this one."""
    with pytest.raises(QueueError):
        complete(unmigrated, 1, "worker-a")
    with unmigrated.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1


def test_enqueue_still_works_without_the_claim_token_column(unmigrated) -> None:
    """The commit=False guard was deleted as dead code, on the strength of an
    invariant about callers: nothing reaching that branch names claimed_by.
    That is the kind of claim a later PR breaks silently, so it gets a test.
    """
    assert enqueue(unmigrated, "q", {"n": 1}) is not None
    assert enqueue(unmigrated, "q", {"n": 2}, dedup_key="n") is not None


def test_a_typo_in_another_column_keeps_its_own_error(unmigrated) -> None:
    """The guard must not claim every schema problem is this one. Postgres puts
    the offending column in message_primary and the LINE excerpt of our SQL in
    str(), which is why matching str() relabelled unrelated typos."""
    with pytest.raises(psycopg2.errors.UndefinedColumn) as excinfo:
        with unmigrated.cursor() as cur:
            cur.execute("SELECT id, claimed_at, attemptz FROM queue")
    exc = excinfo.value
    unmigrated.rollback()

    assert "attemptz" in (exc.diag.message_primary or "")
    assert "claimed_by" not in (exc.diag.message_primary or "")
    assert not isinstance(exc, QueueError)
