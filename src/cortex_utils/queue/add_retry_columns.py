"""Schema migration: add `next_attempt_at` column to the queue table.

Idempotent: re-running is safe.

After running this, consumers should be upgraded to call
`cortex_utils.queue.retry.fail_or_retry` and to include
`ready_predicate()` in their claim CTE so delayed retries are
honored.
"""

from __future__ import annotations

from typing import Any

import psycopg2
import structlog

log = structlog.get_logger()


def has_next_attempt_at_column(conn: psycopg2.extensions.connection) -> bool:
    """Check whether the column already exists."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'queue' AND column_name = 'next_attempt_at'
            LIMIT 1
            """
        )
        return cur.fetchone() is not None


def add_retry_columns(
    conn: psycopg2.extensions.connection,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Add `next_attempt_at` column and supporting index.

    The new index, `idx_queue_ready`, covers claim queries that filter
    on `next_attempt_at` (the retry predicate).  The old `idx_queue_pending`
    stays in place for now; drop it in a follow-up after consumers cut over.
    """
    if dry_run and has_next_attempt_at_column(conn):
        log.info("queue.next_attempt_at already exists; nothing to do")
        return {"status": "already_applied"}

    if dry_run:
        log.info("Would add queue.next_attempt_at + supporting index")
        return {"status": "dry_run", "would_add_column": "next_attempt_at"}

    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE queue
            ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_queue_ready
            ON queue (queue_name, created_at, next_attempt_at)
            WHERE status = 'pending'
            """
        )
    conn.commit()
    log.info("Added queue.next_attempt_at column and idx_queue_ready")
    return {"status": "applied"}
