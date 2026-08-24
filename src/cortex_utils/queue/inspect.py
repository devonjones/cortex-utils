"""Reading the queue, and acting on what you find.

Every question here was asked during a real incident and answered with
hand-written SQL, which is the definition of a gap in this library.

Three properties hold throughout, and each exists because losing it cost
somebody an outage:

**Read-only, and independent of the workers.** These touch postgres and nothing
else. A queue dashboard that needs the pipeline alive dies alongside the thing
it is supposed to report -- cryo ran fourteen hours blind on 2026-08-18 for
exactly that reason, because its only failure channel was a digest the workers
produced. Nothing here may grow a dependency on a worker, a scheduler, or a
message bus.

**One round trip for the overview.** health() answers six of the eight questions
in a single statement. A card that needs five queries will either be slow or be
written wrong.

**Cheap enough to poll.** Counts over a partitioned table with a status index,
and catalogue reads. Nothing that scans history.

Everything binds the queue through to_regclass('queue') under the active
search_path. A bare relname lookup reports healthy off a *different* schema's
rows, which is the defect that cost cortex 4.8 days of email and cryo two days
of silently dropped enqueues.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg2
import structlog

from cortex_utils.queue.ops import (
    SELF_HEALED_MARKER,
    QueueError,
    _tx,
    enqueue,
    is_dedup_value,
)

log = structlog.get_logger()

DEFAULT_FAILURE_LIMIT = 50


@dataclass(frozen=True)
class QueueDepth:
    """One queue's shape right now.

    `ready` and `deferred` are reported apart on purpose. Collapsing them into
    "pending" is actively misleading: six rows all backing off for another hour
    read as a working queue with a backlog, when they are a queue in retry
    storm. `oldest_ready_age_s` separates "four things arrived this minute" from
    "four things have been stuck since Tuesday", which need different responses.
    """

    queue_name: str
    ready: int
    deferred: int
    processing: int
    failed: int
    oldest_ready_age_s: float | None


@dataclass(frozen=True)
class QueueHealth:
    """Everything a dashboard needs, from one statement."""

    depths: list[QueueDepth]
    dead_letter: int
    """Open dead letters: not dismissed, not already retried.

    The same set DeadLetterManager.list_jobs() and get_stats() report by
    default. Two human-facing views of one number that filter differently is
    worse than either being wrong alone -- whichever you read last is the one
    you believe.
    """

    partitioned: bool
    partition_headroom_days: int | None
    self_healed_partitions: int
    server_time: datetime

    @property
    def is_healthy(self) -> bool:
        """A cheap top-level assertion for a monitor to alert on.

        headroom_days counts days covered AFTER today, so 0 means tomorrow's
        writes already fail and 1 is the last day anything can be done about it.
        Alerting at 0 would first fire once the write path is broken, which is
        the wrong end of the problem.

        A queue that is not partitioned has no headroom to run out of, and
        inserts into it never fail for want of a partition. Reporting that as
        unhealthy forever would be a monitor crying about a supported state --
        this package ships migrate_to_partitioned(), so pre-migration is one.
        """
        if not self.partitioned:
            return self.self_healed_partitions == 0
        return (
            self.partition_headroom_days is not None
            and self.partition_headroom_days >= 1
            and self.self_healed_partitions == 0
        )


@dataclass(frozen=True)
class StuckJob:
    """Claimed, and older than the visibility window.

    `claimed_by` is the point: it separates "a worker is chewing on this" from
    "a worker died holding it and the row is waiting out its timeout".
    """

    id: int
    queue_name: str
    claimed_by: str | None
    claimed_at: datetime
    stuck_for_s: float
    attempts: int


@dataclass(frozen=True)
class Failure:
    """A failed row, with its error text intact.

    last_error is never summarised or truncated here. All fourteen rows cryo
    lost on 2026-08-18 read `visibility timeout, attempts exhausted`, and it was
    that uniformity -- visible only in the raw text -- that proved the cause was
    infrastructure rather than fourteen unrelated content failures.
    """

    id: int
    queue_name: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    last_error: str | None
    created_at: datetime

    def ref(self, dedup_key: str | None) -> str | None:
        """The value that IDENTIFIES this work, without the rest of the payload.

        A failure list needs to say WHICH item failed, and consumers reach for
        `payload` to get it — which hands the whole payload to whatever renders
        the list. That is often more than the surface should hold: cryo's drain
        payloads carry URLs captured from browser tabs, including from private
        sessions, and its failure list renders inside a privileged browser
        extension page.

        Offering the identifying value directly makes the minimal thing the
        easy thing. Consumers that genuinely need the payload still have it;
        those that only need a label no longer have to remember to strip it.

        Returns exactly the values the queue will let you dedup on, and None for
        anything else -- including a key that is absent, or one whose value is a
        container. str() on a container yields its Python repr, which would hand
        the renderer a stringified blob of the very thing this method exists to
        keep out of it: a caller who passes the wrong key would get more
        exposure, not less, and silently.

        Shares one predicate with enqueue()'s validation rather than restating
        the rule, so the promise above cannot drift from what enqueue accepts.
        """
        if not dedup_key:
            return None
        value = (self.payload or {}).get(dedup_key)
        return str(value) if is_dedup_value(value) else None


_HEALTH_SQL = """
WITH depth AS (
    SELECT
        queue_name,
        COUNT(*) FILTER (
            WHERE status = 'pending'
              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
        ) AS ready,
        COUNT(*) FILTER (
            WHERE status = 'pending' AND next_attempt_at > NOW()
        ) AS deferred,
        COUNT(*) FILTER (WHERE status = 'processing') AS processing,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed,
        EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (
            WHERE status = 'pending'
              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
        )))::float8 AS oldest_ready_age_s
    FROM queue
    GROUP BY queue_name
),
partition_days AS (
    SELECT
        (regexp_match(
            pg_get_expr(c.relpartbound, c.oid), $re$FROM \\('([^']+)'\\)$re$
        ))[1]::timestamptz::date AS day,
        obj_description(c.oid, 'pg_class') = %(marker)s AS self_healed
    FROM pg_class c
    JOIN pg_inherits i ON c.oid = i.inhrelid
    WHERE i.inhparent = to_regclass('queue')
),
-- Contiguous coverage from today, not MAX(bound). A partition for today and
-- one for +7 with a gap between is not seven days of headroom: the insert on
-- the first uncovered day raises CheckViolation, and reporting 7 there
-- overstates in the direction that lets the queue break unannounced.
islands AS (
    SELECT
        day,
        (day - DATE '2000-01-01')
            - (ROW_NUMBER() OVER (ORDER BY day))::int AS island
    FROM (SELECT DISTINCT day FROM partition_days WHERE day >= CURRENT_DATE) d
),
partitions AS (
    SELECT
        (
            SELECT MAX(day) - CURRENT_DATE FROM islands
            WHERE island = (SELECT island FROM islands WHERE day = CURRENT_DATE)
        ) AS headroom_days,
        (SELECT COUNT(*) FILTER (WHERE self_healed) FROM partition_days) AS self_healed
)
SELECT
    COALESCE(
        (SELECT json_agg(row_to_json(depth) ORDER BY queue_name) FROM depth),
        '[]'::json
    ),
    -- Open only, matching DeadLetterManager.list_jobs() and get_stats(). An
    -- unfiltered count here would put a dashboard and a digest on different
    -- numbers for the same thing, which is the failure the dead-letter
    -- lifecycle exists to prevent -- built into the library rather than merely
    -- tolerated.
    --
    -- The predicate below is chosen by the caller, not user input:
    -- either the lifecycle filter or TRUE, depending on whether this schema has
    -- been migrated. Doing it that way rather than through to_jsonb keeps the
    -- read sargable -- to_jsonb is not, and on a 50k-row archive it turned a
    -- 0.1ms index-only scan into a 118ms sequential one, on a path documented
    -- for polling and over a table this release stops deleting from.
    (SELECT COUNT(*) FROM dead_letter WHERE {dl_open}),
    -- Whether the table is partitioned at all. Without this, a plain queue --
    -- a supported pre-migration state, since this package ships
    -- migrate_to_partitioned() -- has no pg_inherits rows and so reports the
    -- same headroom as one whose partitions have run out, while its inserts
    -- work fine forever.
    (SELECT relkind = 'p' FROM pg_class WHERE oid = to_regclass('queue')),
    (SELECT headroom_days FROM partitions),
    (SELECT self_healed FROM partitions),
    NOW()
"""


def health(conn: psycopg2.extensions.connection) -> QueueHealth:
    """The whole overview in one round trip.

    Answers: what is in the queue, what is ready versus merely deferred, how far
    behind the oldest ready work is, how much has been given up on, how many
    days before enqueues start failing, and whether the write path has been
    self-healing partitions because maintenance stopped.
    """
    with _tx(conn) as cur:
        # One cheap catalogue read before the main statement. health() is
        # read-only and gets called during an upgrade window, so it must work on
        # a dead_letter table that predates the lifecycle columns -- naming them
        # unconditionally is how the CLI's show and retry broke. Two small round
        # trips beat one that scans the whole archive.
        cur.execute(
            "SELECT COUNT(*) = 2 FROM pg_attribute "
            "WHERE attrelid = to_regclass('dead_letter') "
            "AND attname IN ('dismissed_at', 'retried_at') AND NOT attisdropped"
        )
        has_lifecycle = bool(cur.fetchone()[0])
        # Nothing can have been dismissed on a schema with nowhere to record it,
        # so every row is open there.
        dl_open = "dismissed_at IS NULL AND retried_at IS NULL" if has_lifecycle else "TRUE"
        cur.execute(_HEALTH_SQL.format(dl_open=dl_open), {"marker": SELF_HEALED_MARKER})
        depths, dead_letter, partitioned, headroom, self_healed, now = cur.fetchone()

    return QueueHealth(
        depths=[
            QueueDepth(
                queue_name=d["queue_name"],
                ready=d["ready"],
                deferred=d["deferred"],
                processing=d["processing"],
                failed=d["failed"],
                oldest_ready_age_s=d["oldest_ready_age_s"],
            )
            for d in depths
        ],
        dead_letter=dead_letter,
        partitioned=bool(partitioned),
        partition_headroom_days=headroom,
        self_healed_partitions=self_healed,
        server_time=now,
    )


def stuck(
    conn: psycopg2.extensions.connection,
    visibility_timeout_min: int = 30,
    limit: int = DEFAULT_FAILURE_LIMIT,
) -> list[StuckJob]:
    """Rows claimed longer ago than the visibility window.

    The window is measured server-side: claimed_at is a server timestamp, so a
    locally-computed cutoff would be a second clock disagreeing with it.
    """
    with _tx(conn) as cur:
        cur.execute(
            """
            SELECT id, queue_name, claimed_by, claimed_at,
                   EXTRACT(EPOCH FROM (NOW() - claimed_at))::float8 AS stuck_for_s,
                   attempts
            FROM queue
            WHERE status = 'processing'
              AND claimed_at < NOW() - (INTERVAL '1 minute' * %s)
            ORDER BY claimed_at
            LIMIT %s
            """,
            (visibility_timeout_min, limit),
        )
        return [StuckJob(*row) for row in cur.fetchall()]


def failures(
    conn: psycopg2.extensions.connection,
    limit: int = DEFAULT_FAILURE_LIMIT,
    queue_name: str | None = None,
) -> list[Failure]:
    """Failed rows, newest first, with last_error intact."""
    with _tx(conn) as cur:
        cur.execute(
            """
            SELECT id, queue_name, payload, attempts, max_attempts,
                   last_error, created_at
            FROM queue
            WHERE status = 'failed'
              AND (%s::text IS NULL OR queue_name = %s)
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (queue_name, queue_name, limit),
        )
        return [Failure(*row) for row in cur.fetchall()]


def resubmit(
    conn: psycopg2.extensions.connection,
    job_id: int,
    dedup_key: str | None = None,
) -> int | None:
    """Re-queue a failed job as a new row. Returns the new id, or None if deduped.

    Deliberately not a status flip back to 'pending'. The obvious implementation
    is wrong: created_at is unchanged, so a revived row stays in an old
    partition, and retention drops partitions on age rather than status -- the
    row would be on a clock nobody intended, and could vanish mid-flight.

    Instead the payload is enqueued fresh, landing in today's partition, and the
    original is marked cancelled rather than deleted. A failure list whose
    entries disappear when someone retries them defeats the purpose; the record
    of what happened is the point.

    Both halves commit together, so a crash cannot leave the work re-queued and
    the original still showing failed.
    """
    with _tx(conn) as cur:
        cur.execute(
            "SELECT queue_name, payload, priority, created_at FROM queue "
            "WHERE id = %s AND status = 'failed' FOR UPDATE",
            (job_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise QueueError(f"job {job_id} is not a failed row")
        queue_name, payload, priority, created_at = row

        # commit=False: the cancel below must land with it or not at all.
        new_id = enqueue(
            conn, queue_name, payload, priority=priority, dedup_key=dedup_key, commit=False
        )

        # The whole key, not just id: (id, created_at) is what the table
        # declares, and this reads then writes. fail_or_retry addresses rows the
        # same way for the same reason.
        cur.execute(
            "UPDATE queue SET status = 'cancelled', "
            "last_error = COALESCE(last_error, '') || %s "
            "WHERE id = %s AND created_at = %s",
            (
                f" [resubmitted as {new_id}]" if new_id else " [resubmit deduped]",
                job_id,
                created_at,
            ),
        )
        if cur.rowcount != 1:
            # The row we locked is the row we just failed to update, so this is
            # not a race -- it is a bug. Raising rolls back the enqueue above
            # rather than leaving the work queued twice.
            raise QueueError(f"resubmit could not cancel job {job_id}; rolled back")

    # Say which of the two things happened. new_id is None when the enqueue
    # deduped against work that is already live again -- a correct and common
    # outcome, but "Resubmitted failed job ... new=None" reads as a resubmit
    # that produced nothing, which is the one line an operator would grep for
    # while working out why a requeue seemed to vanish.
    if new_id is None:
        log.info(
            "Resubmit deduped: work already queued",
            original=job_id,
            queue=queue_name,
        )
    else:
        log.info("Resubmitted failed job", original=job_id, new=new_id, queue=queue_name)
    return new_id
