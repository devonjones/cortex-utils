"""Dead letter queue management.

Failed jobs are archived to the dead_letter table before partition drops.
This module provides tools to inspect, retry, and purge dead letter jobs.
"""

from datetime import datetime, timedelta
from typing import Any

import psycopg2
import structlog

log = structlog.get_logger()

# SQL to create dead_letter table
DEAD_LETTER_SCHEMA = """
CREATE TABLE IF NOT EXISTS dead_letter (
    id BIGSERIAL PRIMARY KEY,
    original_id BIGINT NOT NULL,
    queue_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INT NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    archived_from_partition TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_queue
    ON dead_letter(queue_name, failed_at DESC);
CREATE INDEX IF NOT EXISTS idx_dead_letter_created
    ON dead_letter(created_at);
"""


class DeadLetterManager:
    """Manages the dead letter queue."""

    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn

    def ensure_table(self) -> None:
        """Create the dead_letter table if it doesn't exist."""
        with self.conn.cursor() as cur:
            cur.execute(DEAD_LETTER_SCHEMA)
        self.conn.commit()
        log.debug("Ensured dead_letter table exists")

    def list_jobs(
        self,
        queue_name: str | None = None,
        since: timedelta | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List dead letter jobs.

        Args:
            queue_name: Filter by queue name
            since: Only jobs failed within this duration
            limit: Maximum jobs to return
        """
        conditions = []
        params: list[Any] = []

        if queue_name:
            conditions.append("queue_name = %s")
            params.append(queue_name)

        if since:
            conditions.append("failed_at > %s")
            params.append(datetime.now() - since)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id, original_id, queue_name, payload, attempts,
                    last_error, created_at, failed_at, archived_from_partition
                FROM dead_letter
                {where}
                ORDER BY failed_at DESC
                LIMIT %s;
            """,
                params,
            )
            rows = cur.fetchall()

        return [
            {
                "id": row[0],
                "original_id": row[1],
                "queue_name": row[2],
                "payload": row[3],
                "attempts": row[4],
                "last_error": row[5],
                "created_at": row[6],
                "failed_at": row[7],
                "archived_from_partition": row[8],
            }
            for row in rows
        ]

    def get_job(self, job_id: int) -> dict[str, Any] | None:
        """Get a specific dead letter job by ID."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id, original_id, queue_name, payload, attempts,
                    last_error, created_at, failed_at, archived_from_partition
                FROM dead_letter
                WHERE id = %s;
            """,
                (job_id,),
            )
            row = cur.fetchone()

        if not row:
            return None

        return {
            "id": row[0],
            "original_id": row[1],
            "queue_name": row[2],
            "payload": row[3],
            "attempts": row[4],
            "last_error": row[5],
            "created_at": row[6],
            "failed_at": row[7],
            "archived_from_partition": row[8],
        }

    def retry_job(self, job_id: int, dry_run: bool = False) -> bool:
        """Re-enqueue a dead letter job for processing.

        The job is moved back to the queue table with status='pending'.
        Returns True if job was retried.
        """
        job = self.get_job(job_id)
        if not job:
            log.warning("Dead letter job not found", job_id=job_id)
            return False

        if dry_run:
            log.info("Would retry job", job_id=job_id, queue=job["queue_name"])
            return True

        with self.conn.cursor() as cur:
            # Re-enqueue to main queue
            cur.execute(
                """
                INSERT INTO queue (queue_name, payload, status, attempts, created_at)
                VALUES (%s, %s, 'pending', 0, NOW())
                RETURNING id;
            """,
                (job["queue_name"], job["payload"]),
            )
            new_id = cur.fetchone()[0]

            # Remove from dead letter
            cur.execute("DELETE FROM dead_letter WHERE id = %s;", (job_id,))

        self.conn.commit()
        log.info(
            "Retried dead letter job",
            dead_letter_id=job_id,
            new_queue_id=new_id,
            queue=job["queue_name"],
        )
        return True

    def retry_jobs(
        self,
        queue_name: str | None = None,
        since: timedelta | None = None,
        dry_run: bool = False,
    ) -> int:
        """Retry multiple dead letter jobs matching criteria.

        Returns count of jobs retried.
        """
        jobs = self.list_jobs(queue_name=queue_name, since=since, limit=10000)
        retried = 0

        for job in jobs:
            if self.retry_job(job["id"], dry_run=dry_run):
                retried += 1

        log.info("Retried dead letter jobs", count=retried, dry_run=dry_run)
        return retried

    def purge(
        self,
        older_than: timedelta,
        queue_name: str | None = None,
        dry_run: bool = False,
    ) -> int:
        """Purge old dead letter jobs.

        Args:
            older_than: Delete jobs older than this duration
            queue_name: Only purge jobs from this queue
            dry_run: If True, only count what would be deleted

        Returns count of jobs purged.
        """
        cutoff = datetime.now() - older_than
        conditions = ["failed_at < %s"]
        params: list[Any] = [cutoff]

        if queue_name:
            conditions.append("queue_name = %s")
            params.append(queue_name)

        where = " AND ".join(conditions)

        with self.conn.cursor() as cur:
            if dry_run:
                cur.execute(f"SELECT COUNT(*) FROM dead_letter WHERE {where};", params)
                count = cur.fetchone()[0]
                log.info("Would purge dead letter jobs", count=count, cutoff=cutoff)
                return count

            cur.execute(f"DELETE FROM dead_letter WHERE {where};", params)
            count = cur.rowcount

        self.conn.commit()
        log.info("Purged dead letter jobs", count=count, cutoff=cutoff)
        return count

    def get_stats(self) -> dict[str, Any]:
        """Get dead letter queue statistics."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    queue_name,
                    COUNT(*) as count,
                    MIN(failed_at) as oldest,
                    MAX(failed_at) as newest
                FROM dead_letter
                GROUP BY queue_name
                ORDER BY count DESC;
            """
            )
            rows = cur.fetchall()

        return {
            "by_queue": [
                {
                    "queue_name": row[0],
                    "count": row[1],
                    "oldest": row[2],
                    "newest": row[3],
                }
                for row in rows
            ],
            "total": sum(row[1] for row in rows),
        }
