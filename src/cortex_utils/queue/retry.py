"""Exponential-backoff retry support for the shared queue.

Failed jobs used to transition straight to status='failed' and stay there
until a human reset them. This module lets consumers re-queue a failure
with a delay (`next_attempt_at`) up to `max_attempts`, after which the
job moves to 'failed' for good.

Consumers wire this in two places:

1. `_fail_job(...)`: call `fail_or_retry(...)` instead of issuing the
   UPDATE that sets status='failed' directly.

2. The claim CTE: add `AND ready_predicate()` to the `claimable` SELECT
   so delayed jobs are not picked up until their backoff has elapsed.

The schema migration for the `next_attempt_at` column lives in
`add_retry_columns.py` and is exposed via the `cortex-utils migrate-retry`
CLI command.
"""

from __future__ import annotations

import random
from typing import Literal

import psycopg2
import structlog

log = structlog.get_logger()

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

    Doubles each attempt up to `cap_seconds`, then applies symmetric
    jitter of +/- `jitter_ratio` to avoid thundering herds when many
    jobs fail at once (e.g. an LLM outage).

    `attempts` is the number of attempts already made (so the first
    retry is `attempts=1`).
    """
    if attempts < 1:
        attempts = 1

    exp_delay = base_seconds * (2 ** (attempts - 1))
    delay = min(exp_delay, cap_seconds)
    spread = delay * jitter_ratio
    jittered = delay + random.uniform(-spread, spread)
    return max(1, int(jittered))


def ready_predicate(column: str = "next_attempt_at") -> str:
    """SQL predicate that matches jobs ready to be claimed.

    Splice into the `claimable` CTE of a consumer's claim query, e.g.:

        AND (next_attempt_at IS NULL OR next_attempt_at <= statement_timestamp())

    ``column`` must be a trusted SQL identifier (alphanumeric/underscore only).
    """
    import re

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column):
        raise ValueError(f"Invalid column name: {column!r}")
    return f"({column} IS NULL OR {column} <= statement_timestamp())"


def fail_or_retry(
    conn: psycopg2.extensions.connection,
    job_id: int,
    error: str,
    max_attempts: int,
    base_seconds: int = DEFAULT_BASE_SECONDS,
    cap_seconds: int = DEFAULT_CAP_SECONDS,
    jitter_ratio: float = DEFAULT_JITTER_RATIO,
    error_max_chars: int = 1000,
) -> Literal["retrying", "failed"]:
    """Increment attempts and either schedule a retry or mark failed.

    Returns "retrying" if the job was re-queued for a future attempt,
    or "failed" if it has now exhausted its retries.

    The caller owns the transaction and is responsible for committing
    or rolling back.
    """
    truncated_error = error[:error_max_chars]

    with conn.cursor() as cur:
        cur.execute(
            "SELECT attempts FROM queue WHERE id = %s FOR UPDATE",
            (job_id,),
        )
        row = cur.fetchone()
        if row is None:
            log.warning("fail_or_retry: job not found", job_id=job_id)
            return "failed"
        current_attempts = row[0] or 0

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
