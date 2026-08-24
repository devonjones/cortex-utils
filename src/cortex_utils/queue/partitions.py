"""Queue table partition management.

Manages daily partitions for the queue table:
- Create future partitions
- Drop old partitions (after archiving failed jobs)
- Migrate from non-partitioned to partitioned table

Which queue table is managed is decided by search_path (set PGOPTIONS to point a
job at a different schema). Catalog lookups must therefore resolve the parent via
to_regclass('queue') rather than matching pg_class.relname = 'queue', which would
match a same-named table in *any* schema. Getting this wrong once caused a 4.8-day
silent ingestion outage: a second queue table's partitions made partition_exists()
report True, so the real partitions were never created.
"""

from datetime import date, datetime, timedelta
from typing import Any

import psycopg2
import structlog

from cortex_utils.queue.ops import (
    PartitionError,
    PartitionNotAttachedError,
    QueueTableNotFoundError,
    server_today,
)

__all__ = [
    "PartitionError",
    "PartitionManager",
    "PartitionNotAttachedError",
    "QueueTableNotFoundError",
]

log = structlog.get_logger()


class PartitionManager:
    """Manages queue table partitions."""

    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn

    def _require_parent(self, cur: psycopg2.extensions.cursor) -> None:
        """Fail loudly when search_path resolves no queue table.

        to_regclass() yields NULL rather than raising, so every lookup here would
        report an ordinary empty result for a job pointed at the wrong schema --
        indistinguishable from a healthy "nothing to do". Every lookup shares this
        guard so the contract does not vary by method: `partitions drop` reaches
        partition_exists() without passing through is_table_partitioned().
        """
        cur.execute("SELECT to_regclass('queue');")
        if cur.fetchone()[0] is None:
            cur.execute("SHOW search_path;")
            log.error(
                "No queue table on search_path",
                search_path=cur.fetchone()[0],
                hint="check the job's PGOPTIONS",
            )
            raise QueueTableNotFoundError(
                "no 'queue' table on search_path; check the job's PGOPTIONS "
                "(each maintenance job manages the queue in its own schema)"
            )

    def list_partitions(self) -> list[dict[str, Any]]:
        """List all queue partitions with their sizes."""
        with self.conn.cursor() as cur:
            self._require_parent(cur)
            cur.execute(
                """
                SELECT
                    c.relname as partition_name,
                    pg_size_pretty(pg_relation_size(c.oid)) as size,
                    pg_relation_size(c.oid) as size_bytes
                FROM pg_class c
                JOIN pg_inherits i ON c.oid = i.inhrelid
                WHERE i.inhparent = to_regclass('queue')
                ORDER BY c.relname;
            """
            )
            rows = cur.fetchall()

        return [{"name": row[0], "size": row[1], "size_bytes": row[2]} for row in rows]

    def partition_exists(self, partition_date: date) -> bool:
        """Check if a partition exists for the given date."""
        partition_name = f"queue_{partition_date.strftime('%Y_%m_%d')}"
        with self.conn.cursor() as cur:
            self._require_parent(cur)
            cur.execute(
                """
                SELECT 1 FROM pg_class c
                JOIN pg_inherits i ON c.oid = i.inhrelid
                WHERE i.inhparent = to_regclass('queue') AND c.relname = %s;
            """,
                (partition_name,),
            )
            return cur.fetchone() is not None

    def create_partition(self, partition_date: date, dry_run: bool = False) -> bool:
        """Create a partition for the given date.

        Returns True if the partition was created or won by a concurrent
        creator, False if it already existed when we looked.

        Raises PartitionNotAttachedError if the name is taken by a relation that
        is not a partition of queue.
        """
        partition_name = f"queue_{partition_date.strftime('%Y_%m_%d')}"
        next_date = partition_date + timedelta(days=1)

        if self.partition_exists(partition_date):
            log.debug("Partition already exists", partition=partition_name)
            return False

        # IF NOT EXISTS narrows the window between the check above and this CREATE
        # but does not close it: the name check is not atomic with the creation, so
        # a concurrent creator can still land DuplicateTable here. Two maintenance
        # jobs share this database, and an unhandled duplicate would abort the
        # transaction and take the rest of maintain() down with it, so the caller
        # below catches it and re-asks the catalogue.
        sql = f"""
            CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF queue
            FOR VALUES FROM ('{partition_date}') TO ('{next_date}');
        """

        if dry_run:
            log.info("Would create partition", partition=partition_name, sql=sql)
            return True

        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
            self.conn.commit()
        except psycopg2.errors.DuplicateTable:
            # Someone created it between the check and the CREATE. That is a
            # success for our purposes, but only the catalogue can say so -- the
            # exception proves the name is taken, not that the partition exists.
            # The post-check below does the asking, so fall through to it.
            self.conn.rollback()

        # IF NOT EXISTS skips on ANY relation of that name, not just a partition of
        # queue -- migrate.py builds queue_YYYY_MM_DD tables under queue_new, so a
        # half-finished migration leaves exactly such a shadow. Without this check
        # the CREATE is silently absorbed and the day ends up with no partition,
        # which is the failure this module exists to prevent.
        if not self.partition_exists(partition_date):
            log.error("Partition name is shadowed", partition=partition_name)
            raise PartitionNotAttachedError(
                f"{partition_name} exists but is not a partition of queue; "
                "a same-named relation is shadowing it (check for leftovers from "
                "an interrupted migrate-queue)"
            )

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

        Default behaviour (force=False): a partition still holding pending or
        processing jobs is left alone and reported as skipped. Failed jobs are
        archived to dead_letter first, then the partition is dropped.

        With force=True the live jobs are re-enqueued with a fresh created_at --
        so they land in today's partition, not the one being dropped -- and the
        drop proceeds.

        With archive_failed=False the failed rows go with the partition. Nothing
        else preserves them; this is not a soft delete.

        Args:
            partition_date: Date of partition to drop
            archive_failed: Archive failed jobs to dead_letter before dropping
            force: Re-enqueue live jobs and drop anyway, instead of skipping
            dry_run: Show what would be done without making changes

        Returns dict with counts: archived_failed, requeued, dropped_rows -- plus
        skipped_active when the partition was kept back, which is the key
        drop_old_partitions branches on to tell a skip from a drop.
        """
        partition_name = f"queue_{partition_date.strftime('%Y_%m_%d')}"

        if not self.partition_exists(partition_date):
            log.warning("Partition does not exist", partition=partition_name)
            return {"archived_failed": 0, "requeued": 0, "dropped_rows": 0}

        try:
            return self._drop_locked(partition_name, archive_failed, force, dry_run)
        except Exception:
            # A PYTHON-side exception is what this catches, and it is worth
            # being exact about, because the obvious story is wrong: a database
            # error does NOT leak the lock. Postgres releases locks at
            # AbortTransaction -- when the statement errors, not when the client
            # gets around to sending ROLLBACK -- so a CheckViolation under the
            # LOCK leaves an aborted transaction holding nothing.
            #
            # An exception raised in our own code between the LOCK and the
            # commit never reaches the server, so nothing aborts: the
            # transaction stays open and SHARE ROW EXCLUSIVE stays held, on a
            # connection the caller is likely to keep. Measured -- without this,
            # the backend sits `idle in transaction` with the lock and the next
            # writer gets LockNotAvailable; with it, `idle` and no locks.
            self.conn.rollback()
            raise

    def _drop_locked(
        self,
        partition_name: str,
        archive_failed: bool,
        force: bool,
        dry_run: bool,
    ) -> dict[str, int]:
        """The locked section of drop_partition. See there for the contract."""
        archived_count = 0
        requeued_count = 0
        row_count = 0

        with self.conn.cursor() as cur:
            # Lock the partition to prevent all writes during this transaction
            # SHARE ROW EXCLUSIVE blocks INSERT/UPDATE/DELETE/SELECT FOR UPDATE
            cur.execute(f"LOCK TABLE {partition_name} IN SHARE ROW EXCLUSIVE MODE;")

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
                    self.conn.rollback()  # Release transaction lock (no changes made)
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
                # Release the SHARE ROW EXCLUSIVE taken above. Without this a
                # preview accumulates one lock per expired partition and holds
                # them for the rest of the connection -- and claim()'s stale
                # reset and retirement UPDATEs are not date-qualified, so a
                # dry run would block the claim path pipeline-wide. The
                # skipped-active branch above rolls back for the same reason.
                self.conn.rollback()
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

    def create_future_partitions(
        self, days_ahead: int = 3, dry_run: bool = False, days_back: int = 0
    ) -> int:
        """Create partitions for the next N days.

        Returns count of partitions created.

        One unusable date does not abandon the others. Raising straight out of the
        loop would let a shadowed relation on today suppress tomorrow's partition
        too, turning a guard against missing partitions into a cause of them --
        every remaining date is attempted, then the first failure is re-raised so
        the run still fails loudly.

        The caller's retention pass is skipped when this raises. Exposure is bounded
        by the days_ahead window rather than by run count: dates already created stay
        created, and a later run re-creates only what is still missing.

        `days_back` covers dates before today. Zero is right for steady-state
        maintenance -- retention is about to drop those anyway -- but a row can
        legitimately carry a created_at in the recent past: a producer whose
        clock is behind the server's, or a test that rewinds a visibility
        timestamp across local midnight. created_at is NOW() on the server, so
        which dates need covering is a property of that clock rather than of any
        one consumer, which is why this lives here.
        """
        created = 0
        # Server clock, not this process: created_at is NOW() on the server, so
        # a partition dated by the client only lines up by coincidence. Same
        # defect cryo found on the write path.
        today = server_today(self.conn)
        failures: list[PartitionNotAttachedError] = []

        for i in range(-days_back, days_ahead + 1):  # Include today
            partition_date = today + timedelta(days=i)
            try:
                if self.create_partition(partition_date, dry_run=dry_run):
                    created += 1
            except PartitionNotAttachedError as exc:
                # create_partition already logged this one by name; logging again
                # here would double-count the failure for anything watching error
                # rates, so only the run-level summary below is emitted.
                failures.append(exc)

        if failures:
            log.error(
                "Partition creation incomplete",
                created=created,
                failed=len(failures),
                days_ahead=days_ahead,
            )
            raise failures[0]

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
        cutoff = server_today(self.conn) - timedelta(days=retention_days)
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
        """Check if the queue table is partitioned.

        Raises QueueTableNotFoundError when search_path resolves no queue table at all.
        That is a misconfigured job, not an unpartitioned table, and the two must
        not share an answer: callers treat False as "run migrate-queue first" and
        return cleanly, which would turn a broken search_path into a green no-op --
        the same silent shape as the outage in the module docstring.
        """
        with self.conn.cursor() as cur:
            self._require_parent(cur)
            cur.execute(
                """
                SELECT pt.partstrat
                FROM pg_partitioned_table pt
                WHERE pt.partrelid = to_regclass('queue');
            """
            )
            row = cur.fetchone()
            return row is not None
