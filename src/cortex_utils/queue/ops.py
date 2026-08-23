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

Every report is claim-token matched. A worker that stalls past its visibility
timeout and wakes up to report will otherwise complete or fail a row that another
worker has since re-claimed and may still be running.

Dedup returns success. "Already queued" and "emission failed" are different
answers, and a caller that must not proceed unless the work is covered needs to
tell them apart.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

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

# Postgres raises this for a missing partition *and* for a violated CHECK
# constraint -- queue has queue_new_valid_status. Matching the message keeps a
# bad status value from being misread as a partition problem and "fixed" by
# creating a partition nobody needed.
_MISSING_PARTITION = "no partition of relation"


class QueueError(RuntimeError):
    """Base for queue operation failures."""


def ensure_claim_token_column(conn: psycopg2.extensions.connection) -> None:
    """Add the claimed_by column if this schema predates claim tokens.

    Idempotent: every cortex service re-runs its schema setup on each boot, so
    this must be safe to execute repeatedly and against a table that already
    holds rows.
    """
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE queue ADD COLUMN IF NOT EXISTS claimed_by TEXT")
    conn.commit()


def _partition_name(day: date) -> str:
    return f"queue_{day.strftime('%Y_%m_%d')}"


def _create_partition_for(conn: psycopg2.extensions.connection, day: date) -> None:
    """Create the daily partition covering `day` in the current schema.

    Only ever called for the current date: created_at defaults to NOW() and no
    caller supplies it, so nothing can steer creation into the past (resurrecting
    a partition retention just dropped) or the future (spraying junk partitions).
    """
    name = _partition_name(day)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {name} PARTITION OF queue
            FOR VALUES FROM ('{day}') TO ('{day + timedelta(days=1)}')
            """
        )
    conn.commit()
    # Loud on purpose. Reaching here means scheduled maintenance is not running;
    # a silent self-heal would let that stay true for weeks.
    log.warning(
        "Created queue partition from the write path",
        partition=name,
        hint="partition maintenance is not keeping up",
    )


def _is_missing_partition(exc: psycopg2.Error) -> bool:
    return _MISSING_PARTITION in str(exc)


def enqueue(
    conn: psycopg2.extensions.connection,
    queue_name: str,
    payload: dict[str, Any],
    priority: int = 0,
    dedup_key: str | None = None,
) -> int | None:
    """Insert a job, returning its id, or None if an identical job is queued.

    None means "the work is already covered", which is success. Failure raises.
    Callers that must not proceed unless the work is queued should treat only the
    exception as failure.

    `dedup_key` names a payload field. Producers of the same key are serialised
    on an advisory lock held to end of transaction: the partial unique indexes
    cannot backstop this, because partitioning forces created_at into every
    unique key and two concurrent producers get different timestamps.

    A missing partition for today is created and the insert retried once.
    """
    if dedup_key is not None and dedup_key not in payload:
        raise QueueError(f"dedup_key {dedup_key!r} is absent from the payload")

    try:
        return _insert(conn, queue_name, payload, priority, dedup_key)
    except psycopg2.errors.CheckViolation as exc:
        if not _is_missing_partition(exc):
            raise
        conn.rollback()

    _create_partition_for(conn, date.today())
    # Exactly one retry. Anything still failing is not a partition problem.
    return _insert(conn, queue_name, payload, priority, dedup_key)


def _insert(
    conn: psycopg2.extensions.connection,
    queue_name: str,
    payload: dict[str, Any],
    priority: int,
    dedup_key: str | None,
) -> int | None:
    """Do the insert itself, honouring dedup. Caller owns partition recovery."""
    with conn.cursor() as cur:
        if dedup_key is None:
            cur.execute(
                "INSERT INTO queue (queue_name, payload, priority) "
                "VALUES (%s, %s, %s) RETURNING id",
                (queue_name, Json(payload), priority),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else None

        value = payload[dedup_key]
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"{queue_name}:{dedup_key}:{json.dumps(value, sort_keys=True)}",),
        )
        cur.execute(
            f"""
            INSERT INTO queue (queue_name, payload, priority)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM queue
                WHERE queue_name = %s
                  AND payload->>'{dedup_key}' = %s
                  AND status IN ('pending', 'processing')
            )
            RETURNING id
            """,
            (queue_name, Json(payload), priority, queue_name, str(value)),
        )
        row = cur.fetchone()
    conn.commit()
    return row[0] if row else None


def claim(
    conn: psycopg2.extensions.connection,
    queue_name: str,
    limit: int = 1,
    visibility_timeout_min: int = DEFAULT_VISIBILITY_TIMEOUT_MIN,
    worker: str = "",
) -> list[dict[str, Any]]:
    """Claim up to `limit` ready jobs, recovering abandoned ones first.

    Stale recovery does NOT consume an attempt -- see the module docstring. A row
    whose claimant died returns to pending with its budget intact; only a row
    that has already spent its attempts through explicit failures is retired.

    Recovered rows become claimable on the NEXT call, not this one: a
    data-modifying CTE is invisible to its siblings.
    """
    with conn.cursor() as cur:
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
            {"q": queue_name, "vis": visibility_timeout_min, "lim": limit, "w": worker or None},
        )
        jobs = [
            {
                "id": r[0],
                "queue_name": r[1],
                "payload": r[2],
                "attempts": r[3],
                "priority": r[4],
            }
            for r in cur.fetchall()
        ]
    conn.commit()
    return jobs


def complete(conn: psycopg2.extensions.connection, job_id: int, worker: str = "") -> bool:
    """Mark a claimed job done. False if the claim was no longer ours."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE queue SET status = 'completed', completed_at = NOW() "
            "WHERE id = %s AND status = 'processing' "
            "AND claimed_by IS NOT DISTINCT FROM %s",
            (job_id, worker or None),
        )
        held = cur.rowcount > 0
    conn.commit()
    if not held:
        log.warning("complete() bounced: claim no longer held", job_id=job_id, worker=worker)
    return held


def release(
    conn: psycopg2.extensions.connection,
    job_id: int,
    delay_s: int,
    worker: str = "",
) -> bool:
    """Hand a claimed job back unharmed, deferred by `delay_s`.

    Attempts are NOT consumed. This is the primitive for "the work never
    started" -- auth is dead, a dependency is unavailable, a precondition is not
    met yet. Using fail_or_retry() there charges the work for an outage, which is
    how four healthy videos reached terminal on 2026-08-18.

    False if the claim was no longer ours.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE queue SET status = 'pending', claimed_at = NULL, claimed_by = NULL, "
            "next_attempt_at = clock_timestamp() + (INTERVAL '1 second' * %s) "
            "WHERE id = %s AND status = 'processing' "
            "AND claimed_by IS NOT DISTINCT FROM %s",
            (delay_s, job_id, worker or None),
        )
        held = cur.rowcount > 0
    conn.commit()
    return held


def fail_or_retry(
    conn: psycopg2.extensions.connection,
    job_id: int,
    error: object,
    worker: str = "",
    base_seconds: int = DEFAULT_BASE_SECONDS,
    cap_seconds: int = DEFAULT_CAP_SECONDS,
    jitter_ratio: float = DEFAULT_JITTER_RATIO,
) -> str:
    """Charge an attempt and either reschedule or retire the job.

    Returns "pending", "failed", or "stale" when the claim was no longer ours.
    Call this only when the work was attempted and failed; for "never started",
    use release().
    """
    truncated = str(error)[:ERROR_MAX_CHARS]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT attempts, max_attempts FROM queue "
            "WHERE id = %s AND status = 'processing' "
            "AND claimed_by IS NOT DISTINCT FROM %s FOR UPDATE",
            (job_id, worker or None),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            log.warning("fail_or_retry() bounced: claim no longer held", job_id=job_id)
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
            status = "failed"
        else:
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
            status = "pending"
    conn.commit()
    log.info("Job reported failed", job_id=job_id, attempts=attempts, result=status)
    return status
