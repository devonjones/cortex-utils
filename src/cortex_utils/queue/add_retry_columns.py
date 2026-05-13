"""Schema migration: add `next_attempt_at` column to the queue table."""

from __future__ import annotations

from typing import Any

import psycopg2
import structlog

log = structlog.get_logger()


def has_next_attempt_at_column(conn: psycopg2.extensions.connection) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM pg_attribute
            WHERE attrelid = 'public.queue'::regclass
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
