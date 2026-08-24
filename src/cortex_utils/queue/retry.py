"""Exponential-backoff retry helpers for the shared queue."""

from __future__ import annotations

import random
import re
from typing import Literal

import psycopg2

from cortex_utils.log import get_logger

log = get_logger()

DEFAULT_BASE_SECONDS = 30
DEFAULT_CAP_SECONDS = 900  # 15 minutes
DEFAULT_JITTER_RATIO = 0.2


def compute_backoff_delay(
    attempts: int,
    base_seconds: int = DEFAULT_BASE_SECONDS,
    cap_seconds: int = DEFAULT_CAP_SECONDS,
    jitter_ratio: float = DEFAULT_JITTER_RATIO,
) -> int:
    """Compute seconds to wait before the next retry.

    `attempts` is the number of attempts already made; the first retry
    is `attempts=1`. Jitter exists so simultaneous failures (e.g. an
    LLM outage stranding hundreds of jobs) don't all retry in lockstep.
    """
    if attempts < 1:
        attempts = 1

    exp_delay = base_seconds * (2 ** (attempts - 1))
    delay = min(exp_delay, cap_seconds)
    spread = delay * jitter_ratio
    jittered = delay + random.uniform(-spread, spread)
    return max(1, int(jittered))


_COLUMN_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def ready_predicate(column: str = "next_attempt_at") -> str:
    """SQL predicate that matches jobs ready to be claimed.

    Splice into the `claimable` CTE of a consumer's claim query.
    `column` must be a SQL identifier (alphanumeric/underscore only).
    """
    if not _COLUMN_NAME_RE.match(column):
        raise ValueError(f"Invalid column name: {column!r}")
    return f"({column} IS NULL OR {column} <= statement_timestamp())"


def fail_or_retry(
    conn: psycopg2.extensions.connection,
    job_id: int,
    error: object,
    max_attempts: int,
    base_seconds: int = DEFAULT_BASE_SECONDS,
    cap_seconds: int = DEFAULT_CAP_SECONDS,
    jitter_ratio: float = DEFAULT_JITTER_RATIO,
    error_max_chars: int = 1000,
) -> Literal["retrying", "failed"]:
    """Increment attempts and either schedule a retry or mark failed.

    Superseded by cortex_utils.queue.ops.fail_or_retry, which additionally
    matches the claim token so a worker that stalled past its visibility timeout
    cannot report on a row another worker has since re-claimed. This version is
    retained for callers not yet migrated; new code should use ops.

    Note the return values differ: this returns "retrying", ops returns
    "pending" for the same outcome. A migration that swaps the import without
    updating an `== "retrying"` check gets a comparison that is silently always
    false rather than an error.

    Returns "retrying" if the job was re-queued for a future attempt,
    or "failed" if it has now exhausted its retries (or the row was
    already in a terminal state, or the row was not found).

    The caller owns the transaction and is responsible for committing
    or rolling back.
    """
    truncated_error = str(error)[:error_max_chars]

    with conn.cursor() as cur:
        cur.execute(
            "SELECT attempts, status FROM queue WHERE id = %s FOR UPDATE",
            (job_id,),
        )
        row = cur.fetchone()
        if row is None:
            log.error("fail_or_retry: job not found", job_id=job_id)
            return "failed"
        current_attempts, status = row
        if status in ("completed", "failed"):
            log.warning(
                "fail_or_retry: job already in terminal state",
                job_id=job_id,
                status=status,
            )
            return "failed"
        current_attempts = current_attempts or 0

        next_attempts = current_attempts + 1
        if next_attempts >= max_attempts:
            cur.execute(
                """
                UPDATE queue
                SET status = 'failed',
                    attempts = %s,
                    last_error = %s,
                    next_attempt_at = NULL,
                    claimed_at = NULL
                WHERE id = %s
                """,
                (next_attempts, truncated_error, job_id),
            )
            log.info(
                "Job exhausted retries",
                job_id=job_id,
                attempts=next_attempts,
                max_attempts=max_attempts,
            )
            return "failed"

        delay_seconds = compute_backoff_delay(
            next_attempts,
            base_seconds=base_seconds,
            cap_seconds=cap_seconds,
            jitter_ratio=jitter_ratio,
        )
        # Release the claim so the row becomes eligible at next_attempt_at.
        cur.execute(
            """
            UPDATE queue
            SET status = 'pending',
                attempts = %s,
                last_error = %s,
                next_attempt_at = clock_timestamp() + (INTERVAL '1 second' * %s),
                claimed_at = NULL
            WHERE id = %s
            """,
            (next_attempts, truncated_error, delay_seconds, job_id),
        )
        log.info(
            "Job scheduled for retry",
            job_id=job_id,
            attempts=next_attempts,
            max_attempts=max_attempts,
            delay_seconds=delay_seconds,
        )
        return "retrying"
