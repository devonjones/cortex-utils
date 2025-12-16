"""CLI for cortex-utils.

Usage:
    cortex-utils queue stats
    cortex-utils partitions maintain --retention-days 7
    cortex-utils dead-letter list
    cortex-utils migrate-queue --dry-run
"""

import os
from datetime import timedelta
from pathlib import Path

import click
import psycopg2
import structlog

from cortex_utils.alerter import AlerterDaemon, DiscordClient, run_alerter
from cortex_utils.config import Config
from cortex_utils.queue.dead_letter import DeadLetterManager
from cortex_utils.queue.migrate import drop_old_queue_table, migrate_to_partitioned
from cortex_utils.queue.partitions import PartitionManager
from cortex_utils.queue.stats import format_stats_table, get_queue_stats, get_stale_jobs

# Configure structlog for CLI output
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger()


def get_connection(config: Config) -> psycopg2.extensions.connection:
    """Get a database connection."""
    return psycopg2.connect(config.postgres.dsn)


@click.group()
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=False, path_type=Path),
    default=Path.home() / ".cortex" / "utils.yaml",
    help="Config file path",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.pass_context
def main(ctx: click.Context, config_path: Path, verbose: bool) -> None:
    """Cortex operational utilities."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = Config.from_file(config_path)
    ctx.obj["verbose"] = verbose


# --- Queue Commands ---


@main.group()
def queue() -> None:
    """Queue operations."""
    pass


@queue.command("stats")
@click.option("--history", "-h", default=24, help="Hours of history to include")
@click.pass_context
def queue_stats(ctx: click.Context, history: int) -> None:
    """Show queue statistics."""
    config = ctx.obj["config"]
    conn = get_connection(config)

    try:
        stats = get_queue_stats(conn, history_hours=history)
        click.echo(format_stats_table(stats))

        # Also show stale jobs
        stale = get_stale_jobs(conn, stale_minutes=30)
        if stale:
            click.echo("")
            click.echo(f"WARNING: {len(stale)} stale jobs (processing > 30 min):")
            for job in stale[:5]:
                click.echo(
                    f"  - [{job['queue_name']}] id={job['id']} stuck {job['minutes_stuck']} min"
                )
    finally:
        conn.close()


# --- Partition Commands ---


@main.group()
def partitions() -> None:
    """Manage queue table partitions."""
    pass


@partitions.command("list")
@click.pass_context
def partitions_list(ctx: click.Context) -> None:
    """List all queue partitions."""
    config = ctx.obj["config"]
    conn = get_connection(config)

    try:
        pm = PartitionManager(conn)

        if not pm.is_table_partitioned():
            click.echo("Queue table is NOT partitioned. Run 'migrate-queue' first.")
            return

        parts = pm.list_partitions()
        if not parts:
            click.echo("No partitions found.")
            return

        click.echo(f"{'Partition':<25} {'Size':>12}")
        click.echo("-" * 40)
        for p in parts:
            click.echo(f"{p['name']:<25} {p['size']:>12}")
    finally:
        conn.close()


@partitions.command("create")
@click.option("--date", "date_str", help="Date for partition (YYYY-MM-DD)")
@click.option("--days-ahead", default=3, help="Create partitions for N days ahead")
@click.option("--dry-run", is_flag=True, help="Show what would be done")
@click.pass_context
def partitions_create(
    ctx: click.Context, date_str: str | None, days_ahead: int, dry_run: bool
) -> None:
    """Create queue partitions."""
    from datetime import datetime

    config = ctx.obj["config"]
    conn = get_connection(config)

    try:
        pm = PartitionManager(conn)

        if not pm.is_table_partitioned():
            click.echo("Queue table is NOT partitioned. Run 'migrate-queue' first.")
            return

        if date_str:
            partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            if pm.create_partition(partition_date, dry_run=dry_run):
                click.echo(f"Created partition for {partition_date}")
            else:
                click.echo(f"Partition for {partition_date} already exists")
        else:
            created = pm.create_future_partitions(days_ahead=days_ahead, dry_run=dry_run)
            click.echo(f"Created {created} partitions")
    finally:
        conn.close()


@partitions.command("drop")
@click.option("--date", "date_str", required=True, help="Date of partition to drop (YYYY-MM-DD)")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
@click.option(
    "--force",
    is_flag=True,
    help="Force drop even if partition has pending/processing jobs (re-enqueues them)",
)
@click.option("--dry-run", is_flag=True, help="Show what would be done")
@click.pass_context
def partitions_drop(
    ctx: click.Context, date_str: str, yes: bool, force: bool, dry_run: bool
) -> None:
    """Drop a specific partition.

    By default, refuses to drop partitions with pending/processing jobs.
    Use --force to re-enqueue those jobs to today's partition and drop anyway.
    """
    from datetime import datetime

    config = ctx.obj["config"]
    conn = get_connection(config)

    try:
        pm = PartitionManager(conn)
        partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        if not yes and not dry_run:
            msg = f"Drop partition for {partition_date}?"
            if force:
                msg += " Active jobs will be re-enqueued."
            msg += " Failed jobs will be archived."
            click.confirm(msg, abort=True)

        result = pm.drop_partition(
            partition_date, archive_failed=True, force=force, dry_run=dry_run
        )

        if result.get("skipped_active"):
            click.echo(
                f"Skipped: partition has {result['skipped_active']} active jobs. "
                "Use --force to re-enqueue them."
            )
        else:
            parts = [f"{result['dropped_rows']} rows dropped"]
            if result.get("requeued", 0) > 0:
                parts.append(f"{result['requeued']} jobs re-enqueued")
            if result.get("archived_failed", 0) > 0:
                parts.append(f"{result['archived_failed']} failed jobs archived")
            click.echo("Dropped partition: " + ", ".join(parts))
    finally:
        conn.close()


@partitions.command("maintain")
@click.option("--retention-days", default=7, help="Keep partitions for N days")
@click.option("--days-ahead", default=3, help="Create partitions N days ahead")
@click.option("--dry-run", is_flag=True, help="Show what would be done")
@click.pass_context
def partitions_maintain(
    ctx: click.Context, retention_days: int, days_ahead: int, dry_run: bool
) -> None:
    """Run partition maintenance (create future, drop old).

    Skips partitions that still have pending/processing jobs.
    """
    config = ctx.obj["config"]
    conn = get_connection(config)

    try:
        pm = PartitionManager(conn)

        if not pm.is_table_partitioned():
            click.echo("Queue table is NOT partitioned. Run 'migrate-queue' first.")
            return

        result = pm.maintain(
            retention_days=retention_days,
            days_ahead=days_ahead,
            dry_run=dry_run,
        )

        click.echo("Maintenance complete:")
        click.echo(f"  Partitions created: {result['partitions_created']}")
        click.echo(f"  Partitions dropped: {result['partitions_dropped']}")
        if result.get("partitions_skipped", 0) > 0:
            click.echo(f"  Partitions skipped (have active jobs): {result['partitions_skipped']}")
        click.echo(f"  Rows dropped: {result['rows_dropped']}")
        click.echo(f"  Failed jobs archived: {result['failed_archived']}")
        if result.get("requeued", 0) > 0:
            click.echo(f"  Jobs re-enqueued: {result['requeued']}")
    finally:
        conn.close()


# --- Dead Letter Commands ---


@main.group("dead-letter")
def dead_letter() -> None:
    """Manage dead letter queue."""
    pass


@dead_letter.command("list")
@click.option("--queue", "queue_name", help="Filter by queue name")
@click.option("--since", help="Only jobs failed within duration (e.g., 24h, 7d)")
@click.option("--limit", default=20, help="Max jobs to show")
@click.pass_context
def dead_letter_list(
    ctx: click.Context, queue_name: str | None, since: str | None, limit: int
) -> None:
    """List dead letter jobs."""
    config = ctx.obj["config"]
    conn = get_connection(config)

    since_delta = None
    if since:
        since_delta = parse_duration(since)

    try:
        dlm = DeadLetterManager(conn)
        dlm.ensure_table()

        jobs = dlm.list_jobs(queue_name=queue_name, since=since_delta, limit=limit)

        if not jobs:
            click.echo("No dead letter jobs found.")
            return

        click.echo(f"{'ID':<8} {'Queue':<12} {'Failed At':<20} {'Error (truncated)':<40}")
        click.echo("-" * 80)
        for job in jobs:
            error = (job["last_error"] or "")[:37]
            if len(job["last_error"] or "") > 37:
                error += "..."
            click.echo(
                f"{job['id']:<8} {job['queue_name']:<12} "
                f"{str(job['failed_at'])[:19]:<20} {error:<40}"
            )
    finally:
        conn.close()


@dead_letter.command("show")
@click.argument("job_id", type=int)
@click.pass_context
def dead_letter_show(ctx: click.Context, job_id: int) -> None:
    """Show details of a dead letter job."""
    config = ctx.obj["config"]
    conn = get_connection(config)

    try:
        dlm = DeadLetterManager(conn)
        job = dlm.get_job(job_id)

        if not job:
            click.echo(f"Job {job_id} not found.")
            return

        click.echo(f"ID: {job['id']}")
        click.echo(f"Original ID: {job['original_id']}")
        click.echo(f"Queue: {job['queue_name']}")
        click.echo(f"Attempts: {job['attempts']}")
        click.echo(f"Created: {job['created_at']}")
        click.echo(f"Failed: {job['failed_at']}")
        click.echo(f"Archived From: {job['archived_from_partition']}")
        click.echo(f"Error: {job['last_error']}")
        click.echo(f"Payload: {job['payload']}")
    finally:
        conn.close()


@dead_letter.command("retry")
@click.option("--id", "job_id", type=int, help="Retry specific job ID")
@click.option("--queue", "queue_name", help="Retry all jobs from queue")
@click.option("--since", help="Only jobs failed within duration")
@click.option("--dry-run", is_flag=True, help="Show what would be done")
@click.pass_context
def dead_letter_retry(
    ctx: click.Context,
    job_id: int | None,
    queue_name: str | None,
    since: str | None,
    dry_run: bool,
) -> None:
    """Retry dead letter jobs."""
    config = ctx.obj["config"]
    conn = get_connection(config)

    since_delta = None
    if since:
        since_delta = parse_duration(since)

    try:
        dlm = DeadLetterManager(conn)

        if job_id:
            if dlm.retry_job(job_id, dry_run=dry_run):
                click.echo(f"Retried job {job_id}")
            else:
                click.echo(f"Job {job_id} not found")
        else:
            count = dlm.retry_jobs(queue_name=queue_name, since=since_delta, dry_run=dry_run)
            click.echo(f"Retried {count} jobs")
    finally:
        conn.close()


@dead_letter.command("purge")
@click.option("--older-than", required=True, help="Purge jobs older than duration (e.g., 30d)")
@click.option("--queue", "queue_name", help="Only purge from specific queue")
@click.option("--yes", is_flag=True, help="Skip confirmation")
@click.option("--dry-run", is_flag=True, help="Show what would be done")
@click.pass_context
def dead_letter_purge(
    ctx: click.Context,
    older_than: str,
    queue_name: str | None,
    yes: bool,
    dry_run: bool,
) -> None:
    """Purge old dead letter jobs."""
    config = ctx.obj["config"]
    conn = get_connection(config)

    older_delta = parse_duration(older_than)

    try:
        dlm = DeadLetterManager(conn)

        if not yes and not dry_run:
            click.confirm(f"Purge dead letter jobs older than {older_than}?", abort=True)

        count = dlm.purge(older_than=older_delta, queue_name=queue_name, dry_run=dry_run)
        click.echo(f"Purged {count} jobs")
    finally:
        conn.close()


@dead_letter.command("stats")
@click.pass_context
def dead_letter_stats(ctx: click.Context) -> None:
    """Show dead letter queue statistics."""
    config = ctx.obj["config"]
    conn = get_connection(config)

    try:
        dlm = DeadLetterManager(conn)
        dlm.ensure_table()
        stats = dlm.get_stats()

        click.echo(f"Dead Letter Queue: {stats['total']} total jobs")
        click.echo("")

        if stats["by_queue"]:
            click.echo(f"{'Queue':<15} {'Count':>8} {'Oldest':>20} {'Newest':>20}")
            click.echo("-" * 65)
            for q in stats["by_queue"]:
                oldest = str(q["oldest"])[:19] if q["oldest"] else "-"
                newest = str(q["newest"])[:19] if q["newest"] else "-"
                click.echo(f"{q['queue_name']:<15} {q['count']:>8} {oldest:>20} {newest:>20}")
    finally:
        conn.close()


# --- Migration Commands ---


@main.command("migrate-queue")
@click.option("--days-ahead", default=7, help="Create partitions N days ahead")
@click.option("--dry-run", is_flag=True, help="Analyze only, don't migrate")
@click.option("--execute", is_flag=True, help="Actually perform the migration")
@click.pass_context
def migrate_queue(ctx: click.Context, days_ahead: int, dry_run: bool, execute: bool) -> None:
    """Migrate queue table to partitioned design.

    This is a one-time migration. Run with --dry-run first to see what will happen.
    Then run with --execute to perform the actual migration.
    """
    if not dry_run and not execute:
        click.echo("Must specify either --dry-run or --execute")
        click.echo("Run with --dry-run first to see what will happen.")
        return

    config = ctx.obj["config"]
    conn = get_connection(config)

    try:
        result = migrate_to_partitioned(conn, days_ahead=days_ahead, dry_run=dry_run)

        if result["status"] == "already_partitioned":
            click.echo("Queue table is already partitioned. No migration needed.")
        elif result["status"] == "dry_run":
            click.echo("DRY RUN - No changes made")
            click.echo(f"  Would create {result['would_create_partitions']} partitions")
            click.echo(f"  Partition range: {result['partition_range']}")
            click.echo(f"  Rows to migrate: {result['rows_to_migrate']}")
            click.echo(f"  Status counts: {result['status_counts']}")
            click.echo("")
            click.echo("Run with --execute to perform the migration.")
        elif result["status"] == "success":
            click.echo("Migration successful!")
            click.echo(f"  Partitions created: {result['partitions_created']}")
            click.echo(f"  Rows migrated: {result['rows_migrated']}")
            click.echo(f"  Old table preserved as: {result['old_table_preserved']}")
            click.echo("")
            click.echo("Verify the migration, then drop the old table with:")
            click.echo("  cortex-utils drop-old-queue --execute")
    finally:
        conn.close()


@main.command("drop-old-queue")
@click.option("--dry-run", is_flag=True, help="Show what would be done")
@click.option("--execute", is_flag=True, help="Actually drop the table")
@click.pass_context
def drop_old_queue(ctx: click.Context, dry_run: bool, execute: bool) -> None:
    """Drop queue_old table after verifying migration.

    Only run this after confirming the migration was successful.
    """
    if not dry_run and not execute:
        click.echo("Must specify either --dry-run or --execute")
        return

    config = ctx.obj["config"]
    conn = get_connection(config)

    try:
        if drop_old_queue_table(conn, dry_run=dry_run):
            if not dry_run:
                click.echo("Dropped queue_old table.")
        else:
            click.echo("queue_old table does not exist.")
    finally:
        conn.close()


# --- Alerter Commands ---


@main.group()
def alerter() -> None:
    """Discord alerter for monitoring Cortex services."""
    pass


@alerter.command("run")
@click.option(
    "--containers",
    "-c",
    multiple=True,
    help="Containers to monitor (default: all cortex-* containers)",
)
@click.option("--no-ping", is_flag=True, help="Don't @here on critical alerts")
@click.option("--summary-hour", default=6, type=int, help="Hour (0-23) to send daily summary")
@click.pass_context
def alerter_run(
    ctx: click.Context,
    containers: tuple[str, ...],
    no_ping: bool,
    summary_hour: int,
) -> None:
    """Run the alerter daemon.

    Tails Docker logs from cortex containers and sends alerts to Discord.
    Requires DISCORD_WEBHOOK_URL environment variable.
    """
    webhook_url = get_webhook_url()
    container_list = list(containers) if containers else None

    click.echo("Starting alerter daemon...")
    click.echo(f"  Discord webhook: {webhook_url[:50]}...")
    click.echo(f"  Containers: {container_list or 'all cortex-* containers'}")
    click.echo(f"  Ping on critical: {not no_ping}")
    click.echo(f"  Daily summary at: {summary_hour:02d}:00")
    click.echo("")

    run_alerter(
        webhook_url=webhook_url,
        containers=container_list,
        ping_critical=not no_ping,
        summary_hour=summary_hour,
    )


@alerter.command("test")
@click.pass_context
def alerter_test(ctx: click.Context) -> None:
    """Send a test alert to verify Discord webhook.

    Requires DISCORD_WEBHOOK_URL environment variable.
    """
    webhook_url = get_webhook_url()
    daemon = AlerterDaemon(webhook_url=webhook_url)
    if daemon.send_test_alert():
        click.echo("Test alert sent successfully!")
    else:
        click.echo("Failed to send test alert")
        raise SystemExit(1)


@alerter.command("send")
@click.argument("message")
@click.option("--ping", is_flag=True, help="Include @here ping")
@click.pass_context
def alerter_send(ctx: click.Context, message: str, ping: bool) -> None:
    """Send a custom message to Discord.

    Requires DISCORD_WEBHOOK_URL environment variable.
    """
    webhook_url = get_webhook_url()
    client = DiscordClient(webhook_url)
    if client.send(message, ping=ping):
        click.echo("Message sent!")
    else:
        click.echo("Failed to send message")
        raise SystemExit(1)


# --- Utility Functions ---


def get_webhook_url() -> str:
    """Get Discord webhook URL from environment or exit with error."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        click.echo("Error: DISCORD_WEBHOOK_URL environment variable not set")
        raise SystemExit(1)
    return webhook_url


def parse_duration(s: str) -> timedelta:
    """Parse a duration string like '24h' or '7d' to timedelta."""
    s = s.strip().lower()
    if s.endswith("h"):
        return timedelta(hours=int(s[:-1]))
    elif s.endswith("d"):
        return timedelta(days=int(s[:-1]))
    elif s.endswith("m"):
        return timedelta(minutes=int(s[:-1]))
    else:
        raise ValueError(f"Invalid duration format: {s}. Use e.g., 24h, 7d, 30m")


if __name__ == "__main__":
    main()
