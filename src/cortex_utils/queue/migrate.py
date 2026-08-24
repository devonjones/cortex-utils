"""Migration script: non-partitioned queue to partitioned.

This is a one-time migration that:
1. Creates a new partitioned table (queue_new)
2. Creates partitions for existing data date range + future
3. Copies data from old table to new
4. Renames tables (queue → queue_old, queue_new → queue)
5. Recreates indexes
6. Verifies counts match

The old table is preserved as queue_old for safety.
"""

from datetime import timedelta
from typing import Any

import psycopg2

from cortex_utils.log import get_logger
from cortex_utils.queue.ops import server_today
from cortex_utils.queue.schema import queue_ddl

log = get_logger()

# Built from the canonical definition rather than kept as a second copy. This
# one had already drifted: it was missing priority, claimed_by and
# next_attempt_at -- three columns the primitives require -- so a queue built by
# this migration could not have been claimed from.
PARTITIONED_QUEUE_SCHEMA = queue_ddl("queue_new")


def analyze_existing_queue(conn: psycopg2.extensions.connection) -> dict[str, Any]:
    """Analyze the existing queue table.

    Returns:
        dict with min_date, max_date, total_rows, and status_counts
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                MIN(created_at)::date as min_date,
                MAX(created_at)::date as max_date,
                COUNT(*) as total_rows
            FROM queue;
        """
        )
        row = cur.fetchone()
        min_date, max_date, total_rows = row

        cur.execute(
            """
            SELECT status, COUNT(*)
            FROM queue
            GROUP BY status
            ORDER BY status;
        """
        )
        status_rows = cur.fetchall()

    return {
        "min_date": min_date,
        "max_date": max_date,
        "total_rows": total_rows,
        "status_counts": {row[0]: row[1] for row in status_rows},
    }


def is_queue_partitioned(conn: psycopg2.extensions.connection) -> bool:
    """Check if queue table is already partitioned."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pt.partstrat
            FROM pg_partitioned_table pt
            WHERE pt.partrelid = to_regclass('queue');
        """
        )
        return cur.fetchone() is not None


def migrate_to_partitioned(
    conn: psycopg2.extensions.connection,
    days_ahead: int = 7,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Migrate queue table from non-partitioned to partitioned.

    Args:
        conn: Database connection
        days_ahead: Number of future partitions to create
        dry_run: If True, only analyze and report what would happen

    Returns:
        Migration result summary
    """
    # Check if already partitioned
    if is_queue_partitioned(conn):
        log.info("Queue table is already partitioned")
        return {"status": "already_partitioned"}

    # Analyze existing data
    analysis = analyze_existing_queue(conn)
    log.info(
        "Analyzed existing queue",
        total_rows=analysis["total_rows"],
        min_date=analysis["min_date"],
        max_date=analysis["max_date"],
        status_counts=analysis["status_counts"],
    )

    today = server_today(conn)

    if analysis["total_rows"] == 0:
        log.warning("Queue table is empty")
        # Still proceed - create structure with future partitions
        analysis["min_date"] = today
        analysis["max_date"] = today

    # Anchor the window on today, not on the newest existing row. A quiet queue
    # whose last insert predates the migration would otherwise get a range that
    # ends before go-live: every partition created, none covering NOW(), and the
    # migration reporting success.
    end_date = max(analysis["max_date"], today) + timedelta(days=days_ahead)

    if dry_run:
        # Calculate partitions that would be created
        partitions = []
        current = analysis["min_date"]
        while current <= end_date:
            partitions.append(f"queue_{current.strftime('%Y_%m_%d')}")
            current += timedelta(days=1)

        return {
            "status": "dry_run",
            "would_create_partitions": len(partitions),
            "partition_range": f"{partitions[0]} to {partitions[-1]}",
            "rows_to_migrate": analysis["total_rows"],
            "status_counts": analysis["status_counts"],
        }

    # --- ACTUAL MIGRATION ---
    log.info("Starting migration to partitioned queue")

    with conn.cursor() as cur:
        # 0. Freeze the source. analyze_existing_queue's total_rows is read in an
        # earlier transaction, and nothing locks queue until the RENAME below, so
        # a row committed after the copy's snapshot is copied nowhere, counted in
        # neither side of the verification, and stranded in queue_old -- while the
        # migration returns success. The mirror ordering is caught, so the check
        # fires on the harmless case and stays silent on the lossy one.
        log.info("Locking queue for the duration of the migration")
        cur.execute("LOCK TABLE queue IN ACCESS EXCLUSIVE MODE;")

        # 1. Create partitioned table
        log.info("Creating partitioned table queue_new")
        cur.execute(PARTITIONED_QUEUE_SCHEMA)

        # 2. Create partitions for date range
        current = analysis["min_date"]
        partition_count = 0

        while current <= end_date:
            partition_name = f"queue_{current.strftime('%Y_%m_%d')}"
            next_date = current + timedelta(days=1)
            cur.execute(
                f"""
                CREATE TABLE {partition_name} PARTITION OF queue_new
                FOR VALUES FROM ('{current}') TO ('{next_date}');
            """
            )
            partition_count += 1
            current = next_date

        log.info("Created partitions", count=partition_count)

        # 3. Copy data
        log.info("Copying data to partitioned table")
        cur.execute(
            """
            INSERT INTO queue_new (
                id, queue_name, payload, status, attempts, max_attempts,
                last_error, created_at, claimed_at, completed_at
            )
            SELECT
                id, queue_name, payload, status, attempts, max_attempts,
                last_error, created_at, claimed_at, completed_at
            FROM queue;
        """
        )
        copied_rows = cur.rowcount
        log.info("Copied rows", count=copied_rows)

        # 4. Verify counts match
        cur.execute("SELECT COUNT(*) FROM queue_new;")
        new_count = cur.fetchone()[0]

        if new_count != analysis["total_rows"]:
            raise RuntimeError(
                f"Row count mismatch: original={analysis['total_rows']}, "
                f"new={new_count}. Rolling back."
            )

        # 5. Swap tables
        log.info("Swapping tables")
        cur.execute("ALTER TABLE queue RENAME TO queue_old;")
        cur.execute("ALTER TABLE queue_new RENAME TO queue;")

        # 6. Recreate indexes on new table
        # Note: The old indexes stay attached to queue_old, but we need unique names
        log.info("Recreating indexes")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_pending_new
            ON queue(queue_name, status, created_at)
            WHERE status = 'pending';
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_processing_new
            ON queue(queue_name, claimed_at)
            WHERE status = 'processing';
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_payload_gmail_id_new
            ON queue((payload->>'gmail_id'))
            WHERE status = 'pending';
        """)

        # 7. Drop old indexes (attached to queue_old) and rename new ones
        cur.execute("DROP INDEX IF EXISTS idx_queue_pending;")
        cur.execute("DROP INDEX IF EXISTS idx_queue_processing;")
        cur.execute("DROP INDEX IF EXISTS idx_queue_payload_gmail_id;")
        # Drop legacy index that existed in some deployments (not recreated)
        cur.execute("DROP INDEX IF EXISTS idx_queue_pending_gmail_id;")
        cur.execute("ALTER INDEX idx_queue_pending_new RENAME TO idx_queue_pending;")
        cur.execute("ALTER INDEX idx_queue_processing_new RENAME TO idx_queue_processing;")
        cur.execute(
            "ALTER INDEX idx_queue_payload_gmail_id_new RENAME TO idx_queue_payload_gmail_id;"
        )

        # 8. Reset sequence to continue from max id
        #
        # Ask the table which sequence it actually uses. queue_new was created
        # with BIGSERIAL, so it owns queue_new_id_seq -- and ALTER TABLE ...
        # RENAME TO does not rename an owned sequence. Naming queue_id_seq here
        # advanced the sequence now owned by queue_old while the live table kept
        # handing out ids from 1, colliding with every copied row. Legal, because
        # partitioning forces created_at into the primary key, so nothing raised:
        # the migration returned success and id stopped being unique. complete(),
        # release() and fail_or_retry() all address rows by id.
        cur.execute("SELECT pg_get_serial_sequence('queue', 'id');")
        sequence = cur.fetchone()[0]
        if sequence is None:
            raise RuntimeError("queue.id has no owned sequence; refusing to leave ids unreseeded")
        cur.execute("SELECT MAX(id) FROM queue;")
        max_id = cur.fetchone()[0] or 0
        cur.execute("SELECT setval(%s, %s, false);", (sequence, max_id + 1))

    conn.commit()

    result = {
        "status": "success",
        "partitions_created": partition_count,
        "rows_migrated": copied_rows,
        "old_table_preserved": "queue_old",
    }

    log.info("Migration complete", **result)
    return result


def drop_old_queue_table(
    conn: psycopg2.extensions.connection,
    dry_run: bool = True,
) -> bool:
    """Drop the queue_old table after migration verification.

    Only call this after confirming the migration was successful.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 WHERE to_regclass('queue_old') IS NOT NULL;
        """
        )
        if not cur.fetchone():
            log.info("queue_old table does not exist")
            return False

        if dry_run:
            cur.execute("SELECT COUNT(*) FROM queue_old;")
            count = cur.fetchone()[0]
            log.info("Would drop queue_old", rows=count)
            return True

        cur.execute("DROP TABLE queue_old;")

    conn.commit()
    log.info("Dropped queue_old table")
    return True
