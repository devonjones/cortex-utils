"""Queue statistics and monitoring."""

from datetime import datetime, timedelta
from typing import Any

import psycopg2
import structlog

log = structlog.get_logger()


def get_queue_stats(
    conn: psycopg2.extensions.connection,
    history_hours: int = 24,
) -> dict[str, Any]:
    """Get comprehensive queue statistics.

    Args:
        conn: Database connection
        history_hours: Hours of history to include for completed/failed counts

    Returns:
        Dictionary with queue stats by queue name
    """
    cutoff = datetime.now() - timedelta(hours=history_hours)

    with conn.cursor() as cur:
        # Current status counts
        cur.execute(
            """
            SELECT
                queue_name,
                status,
                COUNT(*) as count
            FROM queue
            GROUP BY queue_name, status
            ORDER BY queue_name, status;
        """
        )
        status_rows = cur.fetchall()

        # Historical completed/failed (within cutoff)
        cur.execute(
            """
            SELECT
                queue_name,
                status,
                COUNT(*) as count
            FROM queue
            WHERE status IN ('completed', 'failed')
              AND COALESCE(completed_at, created_at) > %s
            GROUP BY queue_name, status;
        """,
            (cutoff,),
        )
        history_rows = cur.fetchall()

    # Build stats by queue
    stats: dict[str, dict[str, int]] = {}

    for row in status_rows:
        queue_name, status, count = row
        if queue_name not in stats:
            stats[queue_name] = {
                "pending": 0,
                "processing": 0,
                "completed_total": 0,
                "failed_total": 0,
                "completed_recent": 0,
                "failed_recent": 0,
            }
        if status == "pending":
            stats[queue_name]["pending"] = count
        elif status == "processing":
            stats[queue_name]["processing"] = count
        elif status == "completed":
            stats[queue_name]["completed_total"] = count
        elif status == "failed":
            stats[queue_name]["failed_total"] = count

    for row in history_rows:
        queue_name, status, count = row
        if queue_name not in stats:
            continue
        if status == "completed":
            stats[queue_name]["completed_recent"] = count
        elif status == "failed":
            stats[queue_name]["failed_recent"] = count

    return {
        "queues": stats,
        "history_hours": history_hours,
        "timestamp": datetime.now().isoformat(),
    }


def get_queue_depth(conn: psycopg2.extensions.connection) -> dict[str, int]:
    """Get current pending job counts by queue.

    This is a lightweight query for monitoring.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT queue_name, COUNT(*)
            FROM queue
            WHERE status = 'pending'
            GROUP BY queue_name;
        """
        )
        rows = cur.fetchall()

    return {row[0]: row[1] for row in rows}


def get_stale_jobs(
    conn: psycopg2.extensions.connection,
    stale_minutes: int = 30,
) -> list[dict[str, Any]]:
    """Find jobs stuck in 'processing' state for too long.

    These may indicate crashed workers.
    """
    cutoff = datetime.now() - timedelta(minutes=stale_minutes)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id, queue_name, payload, claimed_at,
                EXTRACT(EPOCH FROM (NOW() - claimed_at)) / 60 as minutes_stuck
            FROM queue
            WHERE status = 'processing'
              AND claimed_at < %s
            ORDER BY claimed_at;
        """,
            (cutoff,),
        )
        rows = cur.fetchall()

    return [
        {
            "id": row[0],
            "queue_name": row[1],
            "payload": row[2],
            "claimed_at": row[3],
            "minutes_stuck": round(row[4], 1),
        }
        for row in rows
    ]


def format_stats_table(stats: dict[str, Any]) -> str:
    """Format queue stats as an ASCII table."""
    lines = []
    lines.append(f"Queue Statistics (last {stats['history_hours']}h) - {stats['timestamp'][:19]}")
    lines.append("")
    lines.append(
        f"{'Queue':<15} {'Pending':>8} {'Processing':>11} "
        f"{'Done (recent)':>14} {'Failed (recent)':>16}"
    )
    lines.append("-" * 70)

    for queue_name, s in sorted(stats["queues"].items()):
        lines.append(
            f"{queue_name:<15} {s['pending']:>8} {s['processing']:>11} "
            f"{s['completed_recent']:>14} {s['failed_recent']:>16}"
        )

    return "\n".join(lines)
