"""Queue primitives shared by every consumer of the cortex queue.

Both cortex and cryo grew their own copy of this pattern against the same table
shape in the same database, and the copies drifted in ways that cost real work:
cryo's stale-claim recovery consumed an attempt where cortex's did not, so an
expired OAuth token on 2026-08-18 burned four healthy videos to terminal. The
point of this module is that there is one copy.

Which queue is operated on is decided by search_path, exactly as in partitions.py
-- nothing here names a schema.

Three semantics are load-bearing and should not be "simplified" later:

Expiry never consumes an attempt. A worker that dies before reporting has not
proven anything about the work. Only an explicit fail_or_retry() call, made by a
worker that actually attempted the job, spends the budget. An outage must cost
latency, never work.

Every report is claim-token matched, which is why `worker` is required rather
than defaulted. A shared default would make every caller anonymous and every
anonymous claim indistinguishable, so the token would protect nothing.

Dedup returns success. "Already queued" and "emission failed" are different
answers, and a caller that must not proceed unless the work is covered needs to
tell them apart.

Transactions: each function is one unit of work and commits its own by default.
A failure rolls back before re-raising, so a caller never inherits an aborted
transaction and never sees its next, unrelated statement fail because of this
one.

The other operations take no such flag: a claim or a report is the caller's whole
unit of work, so there is nothing to compose it with. Only enqueue is ever one
half of something larger. Note also that a job id returned under commit=False is
not durable until the caller's own commit runs.

enqueue() takes commit=False for callers that must land the job atomically with
their own work -- approving a proposal and queueing its apply job, or moving a
row out of dead_letter and re-queueing it. Committing those separately would let
a crash between them leave the two halves disagreeing. In that mode the caller
owns the transaction entirely, which is the convention retry.fail_or_retry
already follows.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Any, Literal

import psycopg2
import structlog
from psycopg2.extras import Json

from cortex_utils.queue.retry import (
    DEFAULT_BASE_SECONDS,
    DEFAULT_CAP_SECONDS,
    DEFAULT_JITTER_RATIO,
    compute_backoff_delay,
)

log = structlog.get_logger()

DEFAULT_VISIBILITY_TIMEOUT_MIN = 30
ERROR_MAX_CHARS = 2000

# ALTER TABLE on a partitioned parent takes ACCESS EXCLUSIVE and cascades to
# every partition. Bounded so a boot cannot wedge the pipeline behind a claim.
MIGRATION_LOCK_TIMEOUT_MS = 5000

# CREATE TABLE ... PARTITION OF takes ACCESS EXCLUSIVE on the parent, which
# blocks every insert and every claim across all queue_names while held. On the
# deploy path a few seconds is fine; this one runs on the live producer path, so
# it gets a tighter bound. Losing the race is success -- whoever holds the lock
# is almost certainly creating this very partition.
PARTITION_LOCK_TIMEOUT_MS = 2000

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Values whose Python str() matches Postgres's jsonb ->> text output. bool gives
# "True" vs "true", float and container types differ too, so a mismatch would
# make dedup silently never match and double-queue instead of erroring.
_DEDUP_VALUE_TYPES = (str, int)

Report = Literal["pending", "failed", "stale"]


class QueueError(RuntimeError):
    """Base for queue operation failures."""


@contextmanager
def _tx(
    conn: psycopg2.extensions.connection, commit: bool = True
) -> Iterator[psycopg2.extensions.cursor]:
    """One unit of work: commit on success, roll back before re-raising.

    Without the rollback a failure leaves the connection in an aborted
    transaction, and the caller's next statement fails complaining about this
    one. For a long-lived worker that means every later job fails for reasons
    that look unrelated to the job that actually broke.

    commit=False hands both halves to the caller: no commit, and no rollback.

    Not because rollback would lose the caller's work -- Postgres has already
    aborted the whole transaction by the time we would call it, so that work is
    gone regardless. It is left alone because the caller may hold a SAVEPOINT and
    want to recover to it, and rolling back here would take that choice away.
    Either way the connection is aborted and unusable until the caller rolls
    back, which is the contract they accepted by owning the transaction.
    """
    if not commit:
        with conn.cursor() as cur:
            yield cur
        return
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def has_claim_token_column(conn: psycopg2.extensions.connection) -> bool:
    """True if this schema's queue table already has claimed_by.

    Goes through _tx like everything else: psycopg2 opens a transaction even for
    a read, and this is the fast path on every service boot, so a bare cursor
    would leave long-lived worker connections idle-in-transaction as the normal
    case rather than the exception.
    """
    with _tx(conn) as cur:
        cur.execute(
            """
            SELECT 1 FROM pg_attribute
            WHERE attrelid = to_regclass('queue')
              AND attname = 'claimed_by'
              AND NOT attisdropped
            LIMIT 1
            """
        )
        return cur.fetchone() is not None


def ensure_claim_token_column(conn: psycopg2.extensions.connection) -> bool:
    """Add claimed_by if this schema predates claim tokens. True if added.

    Services call this on every boot, so the common case must cost nothing: the
    ALTER takes ACCESS EXCLUSIVE on a partitioned parent under continuous claim
    traffic, and running it unconditionally would queue that lock behind live
    work on every redeploy. Same pre-check shape as add_retry_columns.py, and a
    lock_timeout so a boot fails fast rather than stalling the pipeline.
    """
    if has_claim_token_column(conn):
        return False

    with _tx(conn) as cur:
        cur.execute("SET LOCAL lock_timeout = %s", (f"{MIGRATION_LOCK_TIMEOUT_MS}ms",))
        cur.execute("ALTER TABLE queue ADD COLUMN IF NOT EXISTS claimed_by TEXT")
    log.info("Added queue.claimed_by")
    return True


def _partition_name(day: date) -> str:
    return f"queue_{day.strftime('%Y_%m_%d')}"


def server_today(conn: psycopg2.extensions.connection) -> date:
    """Today according to the server, not this process.

    created_at is TIMESTAMPTZ DEFAULT NOW(), a value the *server* produces, so
    any date used to route or create a partition for that row has to come from
    the same clock. A client-side date.today() is a second, unsynchronised clock
    in a possibly different timezone; it agrees only by coincidence, which means
    it tests green wherever client and server are both UTC and fails on the
    first deployment where they are not.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT_DATE")
        return cur.fetchone()[0]


def _partition_attached(conn: psycopg2.extensions.connection, name: str) -> bool:
    """True if `name` is a partition of this schema's queue.

    Bound via to_regclass so it answers about the queue this search_path
    resolves, not a same-named table in another schema.
    """
    with _tx(conn) as cur:
        cur.execute(
            """
            SELECT 1 FROM pg_class c
            JOIN pg_inherits i ON c.oid = i.inhrelid
            WHERE i.inhparent = to_regclass('queue') AND c.relname = %s
            """,
            (name,),
        )
        return cur.fetchone() is not None


def _ensure_partition(conn: psycopg2.extensions.connection, target: date, required: bool) -> str:
    """Make the partition for `target` exist. Returns created/present/absent.

    Neither failure mode proves anything on its own, so neither is interpreted:
    DuplicateTable can be a same-named relation that is not a partition of this
    queue, and LockNotAvailable can be ALTER TABLE or DROP TABLE holding the
    parent rather than another creator -- ensure_claim_token_column's lock
    timeout is longer than this one, so it can outlast us. Ask the catalogue
    instead of guessing.

    `required` marks the partition the caller actually needs. A speculative one
    must never fail the caller's write: its whole purpose is to save a later
    call, and failing now to save a hypothetical later failure is a bad trade.
    """
    name = _partition_name(target)
    try:
        with _tx(conn) as cur:
            cur.execute("SET LOCAL lock_timeout = %s", (f"{PARTITION_LOCK_TIMEOUT_MS}ms",))
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {name} PARTITION OF queue
                FOR VALUES FROM ('{target}') TO ('{target + timedelta(days=1)}')
                """
            )
        return "created"
    except (psycopg2.errors.DuplicateTable, psycopg2.errors.LockNotAvailable):
        if _partition_attached(conn, name):
            return "present"
        if required:
            raise
        return "absent"
    except psycopg2.Error:
        # Disk, permissions, a deadlock. The caller's own partition must surface
        # that; a speculative one must not take the write down with it.
        if required:
            raise
        log.warning("Could not pre-create tomorrow's partition", partition=name)
        return "absent"


def _create_partition_for(conn: psycopg2.extensions.connection, day: date) -> None:
    """Ensure the partitions covering `day` and the day after, in this schema.

    `day` must come from the server (see server_today), never from this process.

    Tomorrow is created in the same pass so a retry that crosses midnight does
    not find itself missing a partition again; that removes the race rather than
    narrowing it. Tomorrow is inside maintenance's normal +3 horizon, so this
    cannot push one schema's partitions past another's -- which the shared-image
    coupling forbids.

    Reaching here at all means scheduled maintenance is not keeping up, so the
    outcome is logged whatever happens: a silent self-heal would let that stay
    true for weeks, which is the failure this module exists to prevent.
    """
    outcomes = {}
    try:
        for offset in (0, 1):
            target = day + timedelta(days=offset)
            outcomes[_partition_name(target)] = _ensure_partition(
                conn, target, required=(offset == 0)
            )
    finally:
        log.warning(
            "Partitions ensured from the write path",
            outcomes=outcomes,
            hint="partition maintenance is not keeping up",
        )


def _is_missing_partition(exc: psycopg2.Error) -> bool:
    """Distinguish "no partition for this row" from a real CHECK violation.

    Both raise CheckViolation (SQLSTATE 23514), and queue carries
    queue_new_valid_status. A named constraint is a genuine violation; the
    routing failure names none. Keyed on that rather than the message text,
    which is locale-dependent and reworded between Postgres versions.
    """
    return not getattr(exc.diag, "constraint_name", None)


def _validate_dedup(dedup_key: str, payload: dict[str, Any]) -> str:
    """Check the dedup key is a usable identifier and its value is comparable."""
    if not _IDENTIFIER_RE.match(dedup_key):
        raise QueueError(f"dedup_key {dedup_key!r} is not a valid identifier")
    if dedup_key not in payload:
        raise QueueError(f"dedup_key {dedup_key!r} is absent from the payload")
    value = payload[dedup_key]
    if not isinstance(value, _DEDUP_VALUE_TYPES) or isinstance(value, bool):
        raise QueueError(
            f"dedup_key {dedup_key!r} holds {type(value).__name__}; "
            "only str and int dedup cleanly against jsonb text output"
        )
    return str(value)


def enqueue(
    conn: psycopg2.extensions.connection,
    queue_name: str,
    payload: dict[str, Any],
    priority: int = 0,
    dedup_key: str | None = None,
    commit: bool = True,
) -> int | None:
    """Insert a job, returning its id, or None if an identical job is queued.

    None means "the work is already covered", which is success. Failure raises.
    Callers that must not proceed unless the work is queued should treat only the
    exception as failure.

    `dedup_key` names a payload field holding a str or int. Producers of the same
    key are serialised on an advisory lock held to end of transaction: the
    partial unique indexes cannot backstop this, because partitioning forces
    created_at into every unique key and two concurrent producers get different
    timestamps.

    A missing partition for today is created and the insert retried once.

    commit=False leaves the transaction to the caller, for work that must land
    atomically with the job. It also gives up the partition self-heal: creating a
    partition requires committing, which would commit the caller's pending work
    behind their back. The CheckViolation propagates instead, the caller rolls
    back, and the next ordinary enqueue or maintenance run creates the partition.
    """
    dedup_value = _validate_dedup(dedup_key, payload) if dedup_key is not None else None

    try:
        return _insert(conn, queue_name, payload, priority, dedup_key, dedup_value, commit)
    except psycopg2.errors.CheckViolation as exc:
        if not commit or not _is_missing_partition(exc):
            raise

    _create_partition_for(conn, server_today(conn))
    # Exactly one retry. Anything still failing is not a partition problem.
    return _insert(conn, queue_name, payload, priority, dedup_key, dedup_value, commit)


def _insert(
    conn: psycopg2.extensions.connection,
    queue_name: str,
    payload: dict[str, Any],
    priority: int,
    dedup_key: str | None,
    dedup_value: str | None,
    commit: bool = True,
) -> int | None:
    """Do the insert itself, honouring dedup. Caller owns partition recovery."""
    with _tx(conn, commit=commit) as cur:
        if dedup_key is None:
            cur.execute(
                "INSERT INTO queue (queue_name, payload, priority) "
                "VALUES (%s, %s, %s) RETURNING id",
                (queue_name, Json(payload), priority),
            )
            row = cur.fetchone()
            return row[0] if row else None

        # Key the lock on queue AND field AND value: keying on the value alone
        # would serialise unrelated queues against each other.
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"{queue_name}:{dedup_key}:{dedup_value}",),
        )
        cur.execute(
            """
            INSERT INTO queue (queue_name, payload, priority)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM queue
                WHERE queue_name = %s
                  AND payload->>%s = %s
                  AND status IN ('pending', 'processing')
            )
            RETURNING id
            """,
            (queue_name, Json(payload), priority, queue_name, dedup_key, dedup_value),
        )
        row = cur.fetchone()
        return row[0] if row else None


def claim(
    conn: psycopg2.extensions.connection,
    queue_name: str,
    worker: str,
    limit: int = 1,
    visibility_timeout_min: int = DEFAULT_VISIBILITY_TIMEOUT_MIN,
) -> list[dict[str, Any]]:
    """Claim up to `limit` ready jobs, recovering abandoned ones first.

    `worker` identifies the claimant and is required: it is the token every
    later report is matched against, and a shared default would make all
    claimants indistinguishable.

    Stale recovery does NOT consume an attempt -- see the module docstring. A row
    whose claimant died returns to pending with its budget intact; only a row
    that has already spent its attempts through explicit failures is retired.

    Recovered rows become claimable on the NEXT call, not this one: a
    data-modifying CTE is invisible to its siblings.
    """
    if not worker:
        raise QueueError("worker is required; it is the claim token")

    with _tx(conn) as cur:
        cur.execute(
            """
            WITH reset_stale AS (
                UPDATE queue
                SET status = 'pending', claimed_at = NULL, claimed_by = NULL
                WHERE queue_name = %(q)s AND status = 'processing'
                  AND claimed_at < NOW() - (INTERVAL '1 minute' * %(vis)s)
                  AND attempts < max_attempts
            ),
            retire_exhausted AS (
                UPDATE queue
                SET status = 'failed', claimed_at = NULL, claimed_by = NULL,
                    last_error = COALESCE(last_error, 'attempts exhausted')
                WHERE queue_name = %(q)s AND status = 'processing'
                  AND claimed_at < NOW() - (INTERVAL '1 minute' * %(vis)s)
                  AND attempts >= max_attempts
            ),
            claimable AS (
                SELECT id, created_at FROM queue
                WHERE queue_name = %(q)s AND status = 'pending'
                  AND (next_attempt_at IS NULL
                       OR next_attempt_at <= statement_timestamp())
                  AND attempts < max_attempts
                ORDER BY priority DESC, created_at
                LIMIT %(lim)s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE queue q
            SET status = 'processing', claimed_at = NOW(), claimed_by = %(w)s
            FROM claimable c
            WHERE q.id = c.id AND q.created_at = c.created_at
            RETURNING q.id, q.queue_name, q.payload, q.attempts, q.priority
            """,
            {"q": queue_name, "vis": visibility_timeout_min, "lim": limit, "w": worker},
        )
        return [
            {
                "id": r[0],
                "queue_name": r[1],
                "payload": r[2],
                "attempts": r[3],
                "priority": r[4],
            }
            for r in cur.fetchall()
        ]


def complete(conn: psycopg2.extensions.connection, job_id: int, worker: str) -> bool:
    """Mark a claimed job done. False if the claim was no longer ours."""
    with _tx(conn) as cur:
        cur.execute(
            "UPDATE queue SET status = 'completed', completed_at = NOW() "
            "WHERE id = %s AND status = 'processing' AND claimed_by = %s",
            (job_id, worker),
        )
        held = cur.rowcount > 0
    if not held:
        log.warning("complete() bounced: claim no longer held", job_id=job_id, worker=worker)
    return held


def release(
    conn: psycopg2.extensions.connection,
    job_id: int,
    delay_s: int,
    worker: str,
) -> bool:
    """Hand a claimed job back unharmed, deferred by `delay_s`.

    Attempts are NOT consumed. This is the primitive for "the work never
    started" -- auth is dead, a dependency is unavailable, a precondition is not
    met yet. Using fail_or_retry() there charges the work for an outage, which is
    how four healthy videos reached terminal on 2026-08-18.

    False if the claim was no longer ours.
    """
    with _tx(conn) as cur:
        cur.execute(
            "UPDATE queue SET status = 'pending', claimed_at = NULL, claimed_by = NULL, "
            "next_attempt_at = clock_timestamp() + (INTERVAL '1 second' * %s) "
            "WHERE id = %s AND status = 'processing' AND claimed_by = %s",
            (delay_s, job_id, worker),
        )
        held = cur.rowcount > 0
    if not held:
        log.warning("release() bounced: claim no longer held", job_id=job_id, worker=worker)
    return held


def fail_or_retry(
    conn: psycopg2.extensions.connection,
    job_id: int,
    error: object,
    worker: str,
    base_seconds: int = DEFAULT_BASE_SECONDS,
    cap_seconds: int = DEFAULT_CAP_SECONDS,
    jitter_ratio: float = DEFAULT_JITTER_RATIO,
) -> Report:
    """Charge an attempt and either reschedule or retire the job.

    Returns "pending", "failed", or "stale" when the claim was no longer ours.
    Call this only when the work was attempted and failed; for "never started",
    use release().

    This reads attempts and then writes, rather than letting one UPDATE with a
    CASE decide -- which is the shape the database should normally arbitrate.
    The exception is deliberate: expressing the backoff in SQL would put a second
    copy of compute_backoff_delay in a second language, and two consumers already
    share the Python one. FOR UPDATE holds the row for the round trip, so the
    read-modify-write is atomic; the cost is one round trip to keep the backoff
    in a single place.
    """
    truncated = str(error)[:ERROR_MAX_CHARS]
    with _tx(conn) as cur:
        cur.execute(
            "SELECT attempts, max_attempts FROM queue "
            "WHERE id = %s AND status = 'processing' AND claimed_by = %s FOR UPDATE",
            (job_id, worker),
        )
        row = cur.fetchone()
        if row is None:
            log.warning(
                "fail_or_retry() bounced: claim no longer held",
                job_id=job_id,
                worker=worker,
            )
            return "stale"

        attempts = (row[0] or 0) + 1
        max_attempts = row[1]

        if attempts >= max_attempts:
            cur.execute(
                "UPDATE queue SET status = 'failed', attempts = %s, last_error = %s, "
                "claimed_at = NULL, claimed_by = NULL, next_attempt_at = NULL "
                "WHERE id = %s",
                (attempts, truncated, job_id),
            )
            log.error(
                "Job retired after exhausting attempts",
                job_id=job_id,
                attempts=attempts,
                worker=worker,
            )
            return "failed"

        delay = compute_backoff_delay(
            attempts,
            base_seconds=base_seconds,
            cap_seconds=cap_seconds,
            jitter_ratio=jitter_ratio,
        )
        cur.execute(
            "UPDATE queue SET status = 'pending', attempts = %s, last_error = %s, "
            "claimed_at = NULL, claimed_by = NULL, "
            "next_attempt_at = clock_timestamp() + (INTERVAL '1 second' * %s) "
            "WHERE id = %s",
            (attempts, truncated, delay, job_id),
        )
        log.warning(
            "Job failed, scheduled for retry",
            job_id=job_id,
            attempts=attempts,
            max_attempts=max_attempts,
            retry_in_s=delay,
            worker=worker,
        )
        return "pending"
