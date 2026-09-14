#!/usr/bin/env python3
"""Walk the Gmail archive backwards one month per run.

Devon's mailbox goes back to 2002; Cortex currently holds 2025-01 onward.
Ingesting 23 years at once would hammer the Gmail API and bury the live queue,
so this queues exactly one month-long window per run, intended nightly.

WHY THE WATERMARK IS NOT `MIN(internal_date)`

Two traps, both real in this corpus as of 2026-09-14:

  * 82 SPAM messages carry `internal_date = 0`. Gmail strips the Date header on
    them and the fetch records epoch. A naive MIN returns 1970-01-01, and the
    walker would spend 55 years of runs on empty windows.
  * Excluding those, MIN is 2008-05-18 -- a single stray message. The bulk of
    the corpus starts 2025-01-01. Trusting MIN would declare 17 years already
    ingested and walk straight past them.

So the watermark is the walker's OWN history: the earliest `after_date` among
the windowed jobs it created. The job table is the state, which keeps it
inspectable (`GET /sync/backfill`) and means there is nothing extra to migrate
or keep in sync. With no prior windowed jobs, it seeds from --seed.

USAGE
    cortex-utils backfill walk [--dry-run] [--months 1]
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta
from typing import Any

# No default: the parent CLAUDE.md forbids homelab addresses as literals in
# these public repos, and a required service URL must not fall back to one.
GATEWAY_ENV = "CORTEX_GATEWAY_URL"
DEFAULT_SEED = date(2025, 1, 1)  # bulk corpus start; older mail predates Cortex
DEFAULT_FLOOR = date(2002, 1, 1)  # mailbox origin; nothing to find below this


def month_before(d: date, months: int = 1) -> date:
    """The same day-of-month, `months` earlier. Clamps to the 1st."""
    y, m = d.year, d.month - months
    while m < 1:
        m += 12
        y -= 1
    return date(y, m, 1)


def _get(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=30) as r:  # noqa: S310
        data: dict[str, Any] = json.loads(r.read())
        return data


def _post(url: str, payload: dict[str, str]) -> dict[str, Any]:
    req = urllib.request.Request(  # noqa: S310
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310
        data: dict[str, Any] = json.loads(r.read())
        return data


def current_watermark(jobs: list[dict[str, Any]], seed: date) -> date:
    """Earliest `after_date` among windowed jobs, else the seed.

    Two filters, both load-bearing:

    * `before_date` must be set. Those are this walker's windows; an
      open-ended `after:` job (someone catching up recent mail) says nothing
      about how far back history has been walked.
    * status must be `completed`. A failed or cancelled job recorded its
      window but ingested nothing, and counting it would advance the
      watermark past a month that was never fetched -- silently, and
      permanently, since nothing ever revisits it. Excluding them means the
      next run retries that window, which is the behaviour we want.
    """
    windowed = [
        date.fromisoformat(j["after_date"])
        for j in jobs
        if j.get("after_date") and j.get("before_date") and j.get("status") == "completed"
    ]
    return min(windowed) if windowed else seed


def _is_stale(created_at: str | None, stale_after_hours: int) -> bool:
    """Has an in-flight job been sitting longer than we tolerate?

    Unparseable or missing timestamps return False: refusing to queue is the
    safe side, and a bad timestamp is not evidence of a wedge.
    """
    if not created_at or stale_after_hours <= 0:
        return False
    try:
        started = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
    except ValueError:
        return False
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return (datetime.now(UTC) - started) > timedelta(hours=stale_after_hours)


def walk(
    gateway: str,
    months: int = 1,
    seed: date = DEFAULT_SEED,
    floor: date = DEFAULT_FLOOR,
    dry_run: bool = False,
    stale_after_hours: int = 24,
    overlap_days: int = 1,
) -> str:
    """Queue one window, or explain why it did not. Returns a status line.

    Raises RuntimeError on anything that should page a human: the gateway
    being unreachable, or the queue call failing.
    """
    try:
        body = _get(f"{gateway}/sync/backfill?limit=500")
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise RuntimeError(f"cannot reach gateway at {gateway}: {e}") from e

    # Do NOT fall back to [] here. An unexpected shape means we cannot know
    # the watermark, and treating that as "no jobs" would re-seed from the
    # start date and re-queue windows that are already ingested.
    jobs = body.get("jobs")
    if not isinstance(jobs, list):
        raise RuntimeError(
            f"unexpected response from {gateway}/sync/backfill: "
            f"expected a 'jobs' list, got {type(jobs).__name__}"
        )

    # One at a time. Gmail backfill is slow and shares workers with live mail;
    # a pile-up would starve incoming.
    busy = [j for j in jobs if j.get("status") in ("pending", "running")]
    if busy:
        oldest = min(busy, key=lambda j: str(j.get("created_at") or ""))
        stale = _is_stale(oldest.get("created_at"), stale_after_hours)
        # Still refuse to queue -- two concurrent Gmail backfills is worse than
        # a stalled walker -- but say loudly that this needs a human, instead
        # of exiting 0 with a routine-looking "skip" every night forever.
        if stale:
            raise RuntimeError(
                f"backfill job {oldest.get('id')} has been "
                f"{oldest.get('status')} since {oldest.get('created_at')} "
                f"(> {stale_after_hours}h): the walker is stalled until it is "
                f"cancelled or completed"
            )
        return f"skip: {len(busy)} job(s) still {busy[0]['status']} (id {busy[0]['id']})"

    upper = current_watermark(jobs, seed)
    if upper <= floor:
        return f"done: watermark {upper} has reached the floor {floor}"

    lower = max(month_before(upper, months), floor)

    # Extend the window FORWARD past the seam into already-ingested days.
    # Gmail's after:/before: are date-granular and timezone-sensitive, so a
    # message near midnight on the seam could otherwise land in neither
    # window. Re-covering the seam is cheap: verified in gmail_sync.py that a
    # message already in emails_raw skips the body store, the attachment
    # extraction, the emails_raw insert and every downstream enqueue -- it
    # costs one Gmail messages.get plus a label UPDATE, and that UPDATE is a
    # side benefit, refreshing label_ids for mail we already hold.
    #
    # The watermark still advances to `lower`, not to the overlapped bound,
    # so overlap never slows the walk.
    fetch_upper = upper + timedelta(days=max(overlap_days, 0))
    payload = {"after": lower.isoformat(), "before": fetch_upper.isoformat()}
    if dry_run:
        return f"dry-run: would queue {payload}"

    try:
        job = _post(f"{gateway}/sync/backfill", payload)
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise RuntimeError(f"queueing {payload} failed: {e}") from e

    return f"queued {job.get('id')}: {job.get('query')} (walked back to {lower})"
