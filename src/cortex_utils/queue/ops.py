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

    commit=False hands both halves to the caller: no commit, and no rollback
    either, since rolling back here would discard the caller's own pending work
    on the same connection.
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


def _create_partition_for(conn: psycopg2.extensions.connection, day: date) -> None:
    """Create the daily partition covering `day` in the current schema.

    Only ever called for the current date: created_at defaults to NOW() and no
    caller supplies it, so nothing can steer creation into the past (resurrecting
    a partition retention just dropped) or the future (spraying junk partitions).
    """
    name = _partition_name(day)
    try:
        with _tx(conn) as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {name} PARTITION OF queue
                FOR VALUES FROM ('{day}') TO ('{day + timedelta(days=1)}')
                """
            )
    except psycopg2.errors.DuplicateTable:
        # IF NOT EXISTS checks the name before taking the lock that serialises
        # creation, so a concurrent creator can still win in between. The
        # partition exists either way, which is all the caller needs.
        log.info("Partition created concurrently", partition=name)
        return
    # Loud on purpose. Reaching here means scheduled maintenance is not running;
    # a silent self-heal would let that stay true for weeks.
    log.warning(
        "Created queue partition from the write path",
        partition=name,
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

    _create_partition_for(conn, date.today())
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
