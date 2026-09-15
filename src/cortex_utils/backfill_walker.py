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
    """How far back history has been walked CONTIGUOUSLY from the seed.

    Not `min(after_date)`. A global minimum trusts any completed window
    anywhere, and the table does not record who created a job, so an operator
    catching up an old month hijacks the frontier:

        walker at 2024-11-01, someone hand-queues 2010-03-01..2010-03-08
        min() -> 2010-03-01, next window 2010-02
        => 2010-04 .. 2024-10 are now unreachable, permanently and silently

    All nine jobs in the live table are hand-created and six are bounded, so
    this is real; it is harmless today only because their minimum happens to
    equal DEFAULT_SEED. A global minimum has the same blind spot for a GAP
    between the walker's own windows -- a month whose job failed, was
    cancelled, or never ran is stepped straight over.

    So the frontier is walked instead: start at the seed and follow completed
    windows that actually abut, stopping at the first month nothing covers.
    Anything disconnected below the chain is ignored, and a gap is re-walked
    rather than skipped. The chain terminates because every step strictly
    lowers the bound.

    `before_date >= upper` rather than `== upper`, because walk() deliberately
    extends each window forward past the seam by `overlap_days`.

    Two filters, both load-bearing:

    * `before_date` must be set. Those are this walker's windows; an
      open-ended `after:` job (someone catching up recent mail) says nothing
      about how far back history has been walked.
    * status must be `completed`. A failed or cancelled job recorded its
      window but ingested nothing, and counting it would advance the
      watermark past a month that was never fetched -- silently, and
      permanently, since nothing ever revisits it. Excluding them means the
      next run retries that window, which is the behaviour we want.

    Everything the gateway sends is treated as untrusted shape. This runs
    nightly under ofelia with nobody watching, and `walk()` documents
    string-or-RuntimeError, which is all `cli.py` catches: a job that is not a
    dict, or an `after_date` that is not an ISO date, would otherwise escape as
    AttributeError or ValueError -- an unhandled traceback out of a cron job,
    which is a worse failure than a loud RuntimeError saying what arrived.
    """

    def bounds(job: dict[str, Any]) -> tuple[date, date] | None:
        """(after, before) for a completed windowed job, else None."""
        if not (job.get("after_date") and job.get("before_date")):
            return None
        if job.get("status") != "completed":
            return None
        try:
            return (
                date.fromisoformat(str(job["after_date"])),
                date.fromisoformat(str(job["before_date"])),
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"backfill job {job.get('id', 'unknown')} has unreadable dates "
                f"(after={job.get('after_date')!r}, before={job.get('before_date')!r}): "
                f"refusing to guess the watermark, which decides which month is "
                f"ingested next"
            ) from exc

    windows = []
    for job in jobs:
        if not isinstance(job, dict):
            raise RuntimeError(
                f"unexpected entry in the gateway's job list: expected an "
                f"object, got {type(job).__name__} ({job!r:.60})"
            )
        window = bounds(job)
        if window is not None:
            windows.append(window)

    upper = seed
    while True:
        # Deepest window that reaches back from where we are. Deepest rather
        # than any, so a months>1 run is not undone by a smaller overlapping
        # window recorded alongside it.
        reaching = [after for after, before in windows if before >= upper and after < upper]
        if not reaching:
            return upper
        upper = min(reaching)


def _job_age_hours(created_at: str | None) -> float | None:
    """Hours since `created_at`, or None when that cannot be determined.

    None is deliberately NOT "fine" -- the caller raises on it. An earlier
    version returned False ("not stale") for a missing or unparseable
    timestamp, which quietly reintroduced the exact silent-stall this guard
    exists to close: a wedged job whose timestamp we cannot read would be
    skipped every night forever, with a routine-looking exit 0.

    CLOCK PROVENANCE: `created_at` is stamped by Postgres
    (backfill_jobs.created_at TIMESTAMPTZ DEFAULT NOW()) and compared against
    this process's clock, so the result is only as good as the skew between
    them. In this deployment both the walker and Postgres run on the same
    Docker host and share its clock, but that is a property of the topology,
    not a guarantee -- keep `stale_after_hours` comfortably larger than any
    skew you would tolerate, and see the negative-age branch below.

    The API serialises this column as tz-aware ISO ("...+00:00"), so the
    Z-suffix and naive-datetime branches are defensive rather than load
    bearing. They are tested anyway: an untested fallback is one that stops
    working silently the day it is first needed.
    """
    if not created_at:
        return None
    try:
        started = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return (datetime.now(UTC) - started).total_seconds() / 3600.0


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

    # Validate shape BEFORE touching any entry. The busy filter below is the
    # first thing that reads a job, so a non-dict entry escapes here as
    # AttributeError -- ahead of current_watermark's identical guard, and past
    # cli.py, which catches only RuntimeError.
    for entry in jobs:
        if not isinstance(entry, dict):
            raise RuntimeError(
                f"unexpected entry in the gateway's job list: expected an "
                f"object, got {type(entry).__name__} ({entry!r:.60})"
            )

    # One at a time. Gmail backfill is slow and shares workers with live mail;
    # a pile-up would starve incoming.
    busy = [j for j in jobs if j.get("status") in ("pending", "running")]
    if busy:
        oldest = min(busy, key=lambda j: str(j.get("created_at") or ""))
        age = _job_age_hours(oldest.get("created_at"))

        # Always refuse to queue while something is in flight -- two concurrent
        # Gmail backfills is worse than a stalled walker. The question is only
        # whether to exit 0 quietly or raise, and every branch below that
        # cannot prove the job is healthy raises, so a wedge can never hide
        # behind a routine-looking "skip".
        if age is None:
            raise RuntimeError(
                f"backfill job {oldest.get('id')} is {oldest.get('status')} but "
                f"its created_at ({oldest.get('created_at')!r}) cannot be read, "
                f"so its age is unknown: refusing to treat it as healthy"
            )
        if age < 0:
            raise RuntimeError(
                f"backfill job {oldest.get('id')} is stamped "
                f"{abs(age):.1f}h in the FUTURE ({oldest.get('created_at')}): "
                f"the walker's clock and the database's disagree, so staleness "
                f"cannot be judged"
            )
        if age > stale_after_hours:
            raise RuntimeError(
                f"backfill job {oldest.get('id')} has been "
                f"{oldest.get('status')} for {age:.1f}h since "
                f"{oldest.get('created_at')} (> {stale_after_hours}h): the "
                f"walker is stalled until it is cancelled or completed"
            )
        # .get, not []: this function's documented contract is that it returns
        # a string or raises RuntimeError, and cli.py catches only that. A job
        # dict missing a key would otherwise leak a KeyError straight past the
        # handler -- an unhandled traceback out of a nightly cron job, for a
        # field that is only being used to build a log line.
        return (
            f"skip: {len(busy)} job(s) still {busy[0].get('status', 'in flight')} "
            f"(id {busy[0].get('id', 'unknown')})"
        )

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
