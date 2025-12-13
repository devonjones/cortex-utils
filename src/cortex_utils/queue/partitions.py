"""Queue table partition management.

Manages daily partitions for the queue table:
- Create future partitions
- Drop old partitions (after archiving failed jobs)
- Migrate from non-partitioned to partitioned table
"""

from datetime import date, datetime, timedelta
from typing import Any

import psycopg2
import structlog

log = structlog.get_logger()


class PartitionManager:
    """Manages queue table partitions."""

    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn

    def list_partitions(self) -> list[dict[str, Any]]:
        """List all queue partitions with their sizes."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.relname as partition_name,
                    pg_size_pretty(pg_relation_size(c.oid)) as size,
                    pg_relation_size(c.oid) as size_bytes
                FROM pg_class c
                JOIN pg_inherits i ON c.oid = i.inhrelid
                JOIN pg_class p ON i.inhparent = p.oid
                WHERE p.relname = 'queue'
                ORDER BY c.relname;
            """
            )
            rows = cur.fetchall()

        return [{"name": row[0], "size": row[1], "size_bytes": row[2]} for row in rows]

    def partition_exists(self, partition_date: date) -> bool:
        """Check if a partition exists for the given date."""
        partition_name = f"queue_{partition_date.strftime('%Y_%m_%d')}"
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM pg_class c
                JOIN pg_inherits i ON c.oid = i.inhrelid
                JOIN pg_class p ON i.inhparent = p.oid
                WHERE p.relname = 'queue' AND c.relname = %s;
            """,
                (partition_name,),
            )
            return cur.fetchone() is not None

    def create_partition(self, partition_date: date, dry_run: bool = False) -> bool:
        """Create a partition for the given date.

        Returns True if partition was created, False if it already exists.
        """
        partition_name = f"queue_{partition_date.strftime('%Y_%m_%d')}"
        next_date = partition_date + timedelta(days=1)

        if self.partition_exists(partition_date):
            log.debug("Partition already exists", partition=partition_name)
            return False

        sql = f"""
            CREATE TABLE {partition_name} PARTITION OF queue
            FOR VALUES FROM ('{partition_date}') TO ('{next_date}');
        """

        if dry_run:
            log.info("Would create partition", partition=partition_name, sql=sql)
            return True

        with self.conn.cursor() as cur:
            cur.execute(sql)
        self.conn.commit()

        log.info("Created partition", partition=partition_name)
        return True

    def drop_partition(
        self,
        partition_date: date,
        archive_failed: bool = True,
        force: bool = False,
        dry_run: bool = False,
    ) -> dict[str, int]:
        """Drop a partition safely.

        Before dropping:
        1. Re-enqueue any pending/processing jobs (they shouldn't be dropped)
        2. Archive failed jobs to dead_letter
        3. Only drop if partition contains only completed/cancelled jobs

        Args:
            partition_date: Date of partition to drop
            archive_failed: Archive failed jobs to dead_letter before dropping
            force: Force drop even if pending/processing jobs exist
            dry_run: Show what would be done without making changes

        Returns dict with counts: archived_failed, requeued, dropped_rows
        """
        partition_name = f"queue_{partition_date.strftime('%Y_%m_%d')}"

        if not self.partition_exists(partition_date):
            log.warning("Partition does not exist", partition=partition_name)
            return {"archived_failed": 0, "requeued": 0, "dropped_rows": 0}

        archived_count = 0
        requeued_count = 0
        row_count = 0

        with self.conn.cursor() as cur:
            # Lock the partition to prevent writes during this transaction
            # SHARE MODE allows reads but blocks INSERT/UPDATE/DELETE
            cur.execute(f"LOCK TABLE {partition_name} IN SHARE MODE;")

            # Count rows by status
            cur.execute(f"""
                SELECT status, COUNT(*) FROM {partition_name}
                GROUP BY status;
            """)
            status_counts = {row[0]: row[1] for row in cur.fetchall()}
            row_count = sum(status_counts.values())

            pending_count = status_counts.get("pending", 0)
            processing_count = status_counts.get("processing", 0)
            failed_count = status_counts.get("failed", 0)
            active_count = pending_count + processing_count

            # Handle pending/processing jobs - re-enqueue them to today's partition
            if active_count > 0:
                if not force:
                    log.warning(
                        "Partition has active jobs, skipping",
                        partition=partition_name,
                        pending=pending_count,
                        processing=processing_count,
                    )
                    return {
                        "archived_failed": 0,
                        "requeued": 0,
                        "dropped_rows": 0,
                        "skipped_active": active_count,
                    }

                # Re-enqueue active jobs with fresh created_at (goes to today's partition)
                if dry_run:
                    log.info(
                        "Would re-enqueue active jobs",
                        partition=partition_name,
                        pending=pending_count,
                        processing=processing_count,
                    )
                    requeued_count = active_count
                else:
                    cur.execute(f"""
                        INSERT INTO queue (
                            queue_name, payload, status, attempts, max_attempts,
                            last_error, created_at
                        )
                        SELECT
                            queue_name, payload, 'pending', 0, max_attempts,
                            last_error, NOW()
                        FROM {partition_name}
                        WHERE status IN ('pending', 'processing');
                    """)
                    requeued_count = cur.rowcount
                    log.info(
                        "Re-enqueued active jobs",
                        partition=partition_name,
                        count=requeued_count,
                    )

            # Archive failed jobs before drop
            if archive_failed and failed_count > 0:
                if dry_run:
                    log.info(
                        "Would archive failed jobs",
                        partition=partition_name,
                        count=failed_count,
                    )
                    archived_count = failed_count
                else:
                    cur.execute(
                        f"""
                        INSERT INTO dead_letter (
                            original_id, queue_name, payload, attempts,
                            last_error, created_at, failed_at, archived_from_partition
                        )
                        SELECT
                            id, queue_name, payload, attempts,
                            last_error, created_at, NOW(), %s
                        FROM {partition_name}
                        WHERE status = 'failed';
                    """,
                        (partition_name,),
                    )
                    archived_count = cur.rowcount
                    log.info(
                        "Archived failed jobs",
                        partition=partition_name,
                        count=archived_count,
                    )

            # Drop the partition
            if dry_run:
                log.info(
                    "Would drop partition",
                    partition=partition_name,
                    rows=row_count,
                )
            else:
                cur.execute(f"DROP TABLE {partition_name};")
                self.conn.commit()
                log.info(
                    "Dropped partition",
                    partition=partition_name,
                    rows=row_count,
                )

        return {
            "archived_failed": archived_count,
            "requeued": requeued_count,
            "dropped_rows": row_count,
        }

    def create_future_partitions(self, days_ahead: int = 3, dry_run: bool = False) -> int:
        """Create partitions for the next N days.

        Returns count of partitions created.
        """
        created = 0
        today = date.today()

        for i in range(days_ahead + 1):  # Include today
            partition_date = today + timedelta(days=i)
            if self.create_partition(partition_date, dry_run=dry_run):
                created += 1

        return created

    def drop_old_partitions(
        self,
        retention_days: int = 7,
        archive_failed: bool = True,
        force: bool = False,
        dry_run: bool = False,
    ) -> dict[str, int]:
        """Drop partitions older than retention period.

        By default, skips partitions that still have pending/processing jobs.
        Use force=True to re-enqueue active jobs and drop anyway.

        Returns totals: partitions_dropped, rows_dropped, failed_archived,
                       requeued, partitions_skipped
        """
        cutoff = date.today() - timedelta(days=retention_days)
        partitions = self.list_partitions()

        total_dropped = 0
        total_rows = 0
        total_archived = 0
        total_requeued = 0
        total_skipped = 0

        for p in partitions:
            # Parse date from partition name (queue_YYYY_MM_DD)
            try:
                name = p["name"]
                if not name.startswith("queue_"):
                    continue
                date_str = name.replace("queue_", "").replace("_", "-")
                partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                log.warning("Could not parse partition date", partition=p["name"])
                continue

            if partition_date < cutoff:
                result = self.drop_partition(
                    partition_date,
                    archive_failed=archive_failed,
                    force=force,
                    dry_run=dry_run,
                )
                if result.get("skipped_active"):
                    total_skipped += 1
                else:
                    total_dropped += 1
                    total_rows += result["dropped_rows"]
                    total_archived += result["archived_failed"]
                    total_requeued += result.get("requeued", 0)

        return {
            "partitions_dropped": total_dropped,
            "partitions_skipped": total_skipped,
            "rows_dropped": total_rows,
            "failed_archived": total_archived,
            "requeued": total_requeued,
        }

    def maintain(
        self,
        retention_days: int = 7,
        days_ahead: int = 3,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run full partition maintenance.

        1. Create future partitions
        2. Archive failed jobs from old partitions
        3. Drop old partitions

        Returns summary of actions taken.
        """
        log.info(
            "Starting partition maintenance",
            retention_days=retention_days,
            days_ahead=days_ahead,
            dry_run=dry_run,
        )

        created = self.create_future_partitions(days_ahead=days_ahead, dry_run=dry_run)
        drop_result = self.drop_old_partitions(
            retention_days=retention_days,
            archive_failed=True,
            dry_run=dry_run,
        )

        result = {
            "partitions_created": created,
            **drop_result,
            "dry_run": dry_run,
        }

        log.info("Partition maintenance complete", **result)
        return result

    def is_table_partitioned(self) -> bool:
        """Check if the queue table is partitioned."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT pt.partstrat
                FROM pg_class c
                JOIN pg_partitioned_table pt ON c.oid = pt.partrelid
                WHERE c.relname = 'queue';
            """
            )
            row = cur.fetchone()
            return row is not None
