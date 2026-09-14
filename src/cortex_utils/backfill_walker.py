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
from datetime import date
from typing import Any

DEFAULT_GATEWAY = "http://10.5.2.21:8097"
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

    Only jobs with a `before_date` count: those are this walker's. An
    open-ended `after:` job (someone catching up recent mail) says nothing
    about how far back history has been walked.
    """
    windowed = [
        date.fromisoformat(j["after_date"])
        for j in jobs
        if j.get("after_date") and j.get("before_date")
    ]
    return min(windowed) if windowed else seed



def walk(
    gateway: str = DEFAULT_GATEWAY,
    months: int = 1,
    seed: date = DEFAULT_SEED,
    floor: date = DEFAULT_FLOOR,
    dry_run: bool = False,
) -> str:
    """Queue one window, or explain why it did not. Returns a status line.

    Raises RuntimeError on anything that should page a human: the gateway
    being unreachable, or the queue call failing.
    """
    try:
        jobs = _get(f"{gateway}/sync/backfill?limit=500").get("jobs", [])
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise RuntimeError(f"cannot reach gateway at {gateway}: {e}") from e

    # One at a time. Gmail backfill is slow and shares workers with live mail;
    # a pile-up would starve incoming.
    busy = [j for j in jobs if j.get("status") in ("pending", "running")]
    if busy:
        return f"skip: {len(busy)} job(s) still {busy[0]['status']} (id {busy[0]['id']})"

    upper = current_watermark(jobs, seed)
    if upper <= floor:
        return f"done: watermark {upper} has reached the floor {floor}"

    lower = max(month_before(upper, months), floor)
    payload = {"after": lower.isoformat(), "before": upper.isoformat()}
    if dry_run:
        return f"dry-run: would queue {payload}"

    try:
        job = _post(f"{gateway}/sync/backfill", payload)
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise RuntimeError(f"queueing {payload} failed: {e}") from e

    return f"queued {job.get('id')}: {job.get('query')} (walked back to {lower})"
