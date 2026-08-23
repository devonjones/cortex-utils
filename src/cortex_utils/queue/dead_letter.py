"""Dead letter queue management.

Failed jobs are archived to the dead_letter table before partition drops.
This module provides tools to inspect, retry, dismiss and purge them.

Three rules govern the lifecycle, each from an incident rather than a
preference:

**Retry leaves the archive row in place.** That row is the record that work was
given up on and when, and it is exactly the history you want when the same item
dies again. This module used to DELETE it, which erased the only evidence the
item had ever failed. Double-retry is handled by enqueue()'s dedup, not by
bookkeeping.

**A terminal state is mandatory.** Retry alone means every genuinely-undoable
item accumulates until the real failures are buried, and a triage list nobody
can clear stops being read. dismiss() is terminal but not destructive, and
idempotent, so re-dismissing keeps the date it was actually written off.

**Every count must count exactly what the list shows.** Once dismissal exists,
one view filtering and another not makes two human-facing versions of one
number; whichever you read last is the one you believe. list_jobs() and
get_stats() take the same flag and default the same way.

Ids here are a SEPARATE NAMESPACE from queue ids -- both tables number from 1 --
so every parameter says which it means.
"""

from datetime import timedelta
from typing import Any

import psycopg2
import structlog

from cortex_utils.queue.ops import enqueue

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
    archived_from_partition TEXT NOT NULL,
    retried_at TIMESTAMPTZ,
    retried_as BIGINT,
    dismissed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_queue
    ON dead_letter(queue_name, failed_at DESC);
CREATE INDEX IF NOT EXISTS idx_dead_letter_created
    ON dead_letter(created_at);
-- The default list and the default count are both "not written off yet".
CREATE INDEX IF NOT EXISTS idx_dead_letter_open
    ON dead_letter(queue_name, failed_at DESC) WHERE dismissed_at IS NULL;
"""

# Added after the table shipped, so an existing deployment needs the ALTER.
LIFECYCLE_COLUMNS = {
    "retried_at": "TIMESTAMPTZ",
    "retried_as": "BIGINT",
    "dismissed_at": "TIMESTAMPTZ",
}


class DeadLetterManager:
    """Manages the dead letter queue."""

    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn

    def ensure_table(self) -> None:
        """Create the dead_letter table and bring an existing one up to date."""
        with self.conn.cursor() as cur:
            cur.execute(DEAD_LETTER_SCHEMA)
        self.conn.commit()
        self.ensure_lifecycle_columns()
        log.debug("Ensured dead_letter table exists")

    def ensure_lifecycle_columns(self) -> bool:
        """Add retried_at/retried_as/dismissed_at if this schema predates them.

        Idempotent and safe on every boot. Returns True if anything was added.

        All three go on together or none do. A half-migrated table is worse than
        an unmigrated one: dismiss() would work while list_jobs() still could not
        filter, so an operator would clear a list that kept showing the rows
        they cleared.
        """
        missing = [c for c in LIFECYCLE_COLUMNS if not self._has_column(c)]
        if not missing:
            return False

        with self.conn.cursor() as cur:
            # Fail fast rather than queue ACCESS EXCLUSIVE behind a long read.
            cur.execute("SET LOCAL lock_timeout = '5000ms'")
            for name in missing:
                cur.execute(
                    f"ALTER TABLE dead_letter ADD COLUMN IF NOT EXISTS {name} "
                    f"{LIFECYCLE_COLUMNS[name]}"
                )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_dead_letter_open "
                "ON dead_letter(queue_name, failed_at DESC) WHERE dismissed_at IS NULL"
            )
        self.conn.commit()
        log.info("Added dead_letter lifecycle columns", columns=missing)
        return True

    def _has_column(self, name: str) -> bool:
        """Bound through to_regclass so it answers about THIS schema's table."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_attribute "
                "WHERE attrelid = to_regclass('dead_letter') "
                "AND attname = %s AND NOT attisdropped",
                (name,),
            )
            return cur.fetchone() is not None

    def list_jobs(
        self,
        queue_name: str | None = None,
        since: timedelta | None = None,
        limit: int = 100,
        include_dismissed: bool = False,
    ) -> list[dict[str, Any]]:
        """List dead letter jobs.

        Args:
            queue_name: Filter by queue name
            since: Only jobs failed within this duration
            limit: Maximum jobs to return
        """
        conditions = []
        params: list[Any] = []

        if not include_dismissed:
            # get_stats() applies the identical predicate from the identical
            # flag. Two human-facing views of one number that filter differently
            # is worse than either being wrong alone.
            conditions.append("dismissed_at IS NULL")

        if queue_name:
            conditions.append("queue_name = %s")
            params.append(queue_name)

        if since:
            # Server clock: failed_at is TIMESTAMPTZ set by the server, and a
            # naive local datetime.now() is a different clock in a possibly
            # different timezone.
            conditions.append("failed_at > NOW() - (INTERVAL '1 second' * %s)")
            params.append(since.total_seconds())

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id, original_id, queue_name, payload, attempts,
                    last_error, created_at, failed_at, archived_from_partition,
                    retried_at, retried_as, dismissed_at
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
                "retried_at": row[9],
                "retried_as": row[10],
                "dismissed_at": row[11],
            }
            for row in rows
        ]

    def get_job(self, dead_letter_id: int) -> dict[str, Any] | None:
        """Get a specific dead letter job.

        `dead_letter_id` is an id in THIS table, not a queue id -- both number
        from 1, so an API that just says "id" eventually fetches the wrong row.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id, original_id, queue_name, payload, attempts,
                    last_error, created_at, failed_at, archived_from_partition,
                    retried_at, retried_as, dismissed_at
                FROM dead_letter
                WHERE id = %s;
            """,
                (dead_letter_id,),
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

    def retry_job(self, dead_letter_id: int, dry_run: bool = False) -> bool:
        """Re-enqueue a dead letter job, keeping the archive row.

        `dead_letter_id` is an id in THIS table, not a queue id -- both number
        from 1, so passing the wrong one would resurrect an unrelated payload.

        The row is stamped with retried_at/retried_as rather than deleted. It
        used to be deleted, which erased the only evidence the item had ever
        failed -- precisely the history you want when it dies again. Retrying
        twice is handled by enqueue()'s dedup, not by removing the record.

        Returns True if the job was re-enqueued.
        """
        job = self.get_job(dead_letter_id)
        if not job:
            log.warning("Dead letter job not found", dead_letter_id=dead_letter_id)
            return False

        if dry_run:
            log.info("Would retry job", dead_letter_id=dead_letter_id, queue=job["queue_name"])
            return True

        # commit=False so the re-enqueue and the stamp land in one transaction:
        # committing separately would let a crash between them leave the job
        # queued with no record of where it came from.
        new_id = enqueue(self.conn, job["queue_name"], job["payload"], commit=False)

        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE dead_letter SET retried_at = NOW(), retried_as = %s WHERE id = %s",
                (new_id, dead_letter_id),
            )
            if cur.rowcount == 0:
                # The row went while we were working -- purge(). Reporting
                # success would claim we moved something nobody can see we
                # moved, and the re-enqueue above would be an orphan. Ask the
                # database rather than assume.
                self.conn.rollback()
                log.warning("Dead letter row vanished mid-retry", dead_letter_id=dead_letter_id)
                return False

        self.conn.commit()
        log.info(
            "Retried dead letter job",
            dead_letter_id=dead_letter_id,
            new_queue_id=new_id,
            queue=job["queue_name"],
        )
        return True

    def dismiss(self, dead_letter_id: int) -> bool:
        """Write a job off: terminal, but not destructive.

        `dead_letter_id` is an id in THIS table, not a queue id.

        Without a terminal state, retry alone means every genuinely-undoable item
        accumulates until the real failures are buried in noise -- and a triage
        list nobody can clear stops being read.

        Idempotent: re-dismissing keeps the date it was actually written off,
        because that date is the answer to "when did we stop caring about this",
        and moving it forward on every stray click destroys the only useful
        thing it records.

        Returns True if the row exists (whether or not this call was the one
        that dismissed it), False if there is no such row.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE dead_letter SET dismissed_at = COALESCE(dismissed_at, NOW()) "
                "WHERE id = %s RETURNING dismissed_at",
                (dead_letter_id,),
            )
            row = cur.fetchone()
            if row is None:
                self.conn.rollback()
                log.warning("Dead letter job not found", dead_letter_id=dead_letter_id)
                return False

        self.conn.commit()
        log.info("Dismissed dead letter job", dead_letter_id=dead_letter_id, at=row[0])
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
            # Per-job guard: without it one bad row aborts the shared connection
            # and every remaining job in the batch fails for a reason that has
            # nothing to do with it, losing the partial progress.
            try:
                if self.retry_job(job["id"], dry_run=dry_run):
                    retried += 1
            except psycopg2.Error as exc:
                self.conn.rollback()
                log.error(
                    "Dead letter retry failed",
                    dead_letter_id=job["id"],
                    queue=job.get("queue_name"),
                    error=str(exc),
                )

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

        Purges by age alone, dismissed or not. dismiss() is the triage verb and
        this is the housekeeping one: a row old enough to purge and still not
        dismissed was never triaged, and keeping it forever would not fix that.
        """
        # Server clock. This one drives a DELETE that cannot be undone: a local
        # clock running ahead of the server would purge rows that are younger
        # than the retention window the operator asked for.
        conditions = ["failed_at < NOW() - (INTERVAL '1 second' * %s)"]
        params: list[Any] = [older_than.total_seconds()]

        if queue_name:
            conditions.append("queue_name = %s")
            params.append(queue_name)

        where = " AND ".join(conditions)

        with self.conn.cursor() as cur:
            if dry_run:
                cur.execute(f"SELECT COUNT(*) FROM dead_letter WHERE {where};", params)
                count = cur.fetchone()[0]
                log.info("Would purge dead letter jobs", count=count, older_than=str(older_than))
                return count

            cur.execute(f"DELETE FROM dead_letter WHERE {where};", params)
            count = cur.rowcount

        self.conn.commit()
        log.info("Purged dead letter jobs", count=count, older_than=str(older_than))
        return count

    def get_stats(self, include_dismissed: bool = False) -> dict[str, Any]:
        """Dead letter counts, over exactly what list_jobs() shows.

        Same flag and same default as list_jobs() on purpose. A page reporting 2
        while a digest reports 6 is worse than either being wrong alone: whichever
        you read last is the one you believe.
        """
        where = "" if include_dismissed else "WHERE dismissed_at IS NULL"
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    queue_name,
                    COUNT(*) as count,
                    MIN(failed_at) as oldest,
                    MAX(failed_at) as newest
                FROM dead_letter
                {where}
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
