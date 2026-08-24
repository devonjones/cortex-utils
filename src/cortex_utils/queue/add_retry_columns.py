"""Schema migration: add `next_attempt_at` column to the queue table."""

from __future__ import annotations

from typing import Any

import psycopg2
import structlog

from cortex_utils.queue.ops import MIGRATION_LOCK_TIMEOUT_MS, _tx, require_queue_table

log = structlog.get_logger()


def has_next_attempt_at_column(conn: psycopg2.extensions.connection) -> bool:
    """True if this schema's queue has next_attempt_at.

    to_regclass('queue'), not 'public.queue'::regclass. Two schemas share this
    database and the rest of the package resolves the parent through
    search_path; hardcoding public meant a cryo connection was answered about
    cortex's table, so the migration would report itself already applied and
    silently skip. Same defect as the bare-relname lookups, one schema narrower.
    """
    require_queue_table(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM pg_attribute
            WHERE attrelid = to_regclass('queue')
              AND attname = 'next_attempt_at'
              AND NOT attisdropped
            LIMIT 1
            """
        )
        return cur.fetchone() is not None


def add_retry_columns(
    conn: psycopg2.extensions.connection,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Add `next_attempt_at` column and supporting index. Idempotent."""
    if has_next_attempt_at_column(conn):
        log.info("queue.next_attempt_at already exists; nothing to do")
        return {"status": "already_applied"}

    if dry_run:
        log.info("Would add queue.next_attempt_at + supporting index")
        return {"status": "dry_run", "would_add_column": "next_attempt_at"}

    # Bounded, and through _tx, for the same reasons ensure_claim_token_column
    # gives: this now runs on every service boot rather than as a manual CLI
    # step, and an unbounded ALTER queues ACCESS EXCLUSIVE on the parent -- which
    # blocks new readers while it waits, so a slow boot can wedge the live queue
    # instead of failing fast. Measured at 8s unbounded against a held lock,
    # versus 5s to fail. Without _tx a failure also left the caller's connection
    # aborted, which on a boot path is the next statement's problem.
    with _tx(conn) as cur:
        cur.execute("SET LOCAL lock_timeout = %s", (f"{MIGRATION_LOCK_TIMEOUT_MS}ms",))
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
    log.info("Added queue.next_attempt_at column and idx_queue_ready")
    return {"status": "applied"}
