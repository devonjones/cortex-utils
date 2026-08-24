"""Applying approved rule proposals via a dedicated queue (cortex-uo9b.4).

An approved proposal is committed asynchronously: approving it enqueues a
``proposal_apply`` job, and a consumer (the teach bot's poll loop) claims the
job and turns the proposal into a triage email-mapping. Routing the apply
through the shared ``queue`` table makes it durable, retryable (via
``fail_or_retry``), and visible in the queue-depth view — the same machinery
every other Cortex worker uses.

This module owns only the DB-side primitives (enqueue, claim, complete, and the
"was the taught mail archived?" signal). The mapping HTTP call and the consumer
loop live in the service that runs the bot.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import psycopg2

from cortex_utils.queue.ops import enqueue
from cortex_utils.queue.retry import ready_predicate

APPLY_QUEUE = "proposal_apply"


@dataclass
class ApplyJob:
    """A claimed ``proposal_apply`` job."""

    job_id: int
    proposal_id: int
    attempts: int
    max_attempts: int


def enqueue_proposal_apply(
    conn: psycopg2.extensions.connection, proposal_id: int, *, priority: int = 0
) -> int:
    """Enqueue an apply job for an approved proposal. Returns the job id.

    Does not commit — the caller runs this in the same transaction that approves
    the proposal, so the job and the approval land atomically.
    """
    job_id = enqueue(
        conn,
        APPLY_QUEUE,
        {"proposal_id": proposal_id},
        priority=priority,
        commit=False,
    )
    # enqueue() returns None only for a deduplicated insert, and this call passes
    # no dedup_key, so None is unreachable here. Asserted rather than widening
    # the return type, so adding a dedup_key later fails loudly instead of
    # handing callers a None they do not expect.
    assert job_id is not None
    return job_id


def claim_proposal_apply_jobs(
    conn: psycopg2.extensions.connection, *, limit: int = 10
) -> list[ApplyJob]:
    """Claim up to ``limit`` ready apply jobs, marking them processing.

    Uses ``FOR UPDATE SKIP LOCKED`` so concurrent consumers don't collide.
    ``attempts`` is left untouched here — ``fail_or_retry`` owns the increment
    on failure — so a job's attempt count reflects failures, not claims. Does
    not commit.
    """
    # cortex_utils hand-writing its own claim is the same rule violation the
    # deprecation exists to flag, and it needs the same port (cortex-xluv) --
    # it also takes no worker identity, so it cannot set a claim token. Silenced
    # here only so the warning means "a consumer still needs porting" instead of
    # firing on ourselves every time this module is imported.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        predicate = ready_predicate()

    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH claimable AS (
                SELECT id FROM queue
                WHERE queue_name = %s AND status = 'pending'
                  AND {predicate}
                ORDER BY priority DESC, id
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE queue q
            SET status = 'processing', claimed_at = NOW()
            FROM claimable c
            WHERE q.id = c.id
            RETURNING q.id, q.payload, q.attempts, q.max_attempts
            """,
            (APPLY_QUEUE, limit),
        )
        rows = cur.fetchall()
    return [
        ApplyJob(
            job_id=row[0],
            proposal_id=int(row[1]["proposal_id"]),
            attempts=row[2] or 0,
            max_attempts=row[3],
        )
        for row in rows
    ]


def complete_apply_job(conn: psycopg2.extensions.connection, job_id: int) -> None:
    """Mark an apply job completed. Does not commit."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE queue SET status = 'completed', completed_at = NOW(), "
            "claimed_at = NULL WHERE id = %s",
            (job_id,),
        )


def taught_mail_archived(conn: psycopg2.extensions.connection, sender: str, label: str) -> bool:
    """Was the most recent email that taught this (sender, label) archived?

    Archived == the user removed it from the inbox (no ``INBOX`` label). That is
    the signal to set ``archive=true`` on the mapping so future mail from the
    sender is auto-archived too. Returns ``False`` when there's no example or the
    example is still in the inbox, i.e. leave the archive flag alone.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT er.label_ids
            FROM learning_opportunities lo
            JOIN emails_raw er ON er.gmail_id = lo.gmail_id
            WHERE lo.sender = %s AND lo.label = %s AND lo.direction = 'add'
            ORDER BY lo.id DESC
            LIMIT 1
            """,
            (sender, label),
        )
        row = cur.fetchone()
    if row is None:
        return False
    label_ids = row[0]
    if not label_ids:
        return False  # unknown/unparsed label state — don't flip archive on a guess
    return "INBOX" not in label_ids
