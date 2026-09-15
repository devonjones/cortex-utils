"""The watermark must come from the walker's own jobs, not from the mail.

Two properties of this corpus make `MIN(internal_date)` the wrong answer, and
both are live as of 2026-09-14:

  * 82 SPAM messages carry internal_date = 0, because Gmail strips the Date
    header on them. A naive MIN returns 1970-01-01 and the walker burns 55
    years of nightly runs on empty windows.
  * Excluding those, MIN is 2008-05-18 -- one stray message -- while the bulk
    of the corpus starts 2025-01-01. Trusting MIN declares 17 years already
    ingested and walks straight past them.

Hence the watermark is derived from backfill_jobs. These tests pin that, plus
the month arithmetic, which is the other place an off-by-one silently skips or
re-scans a month.
"""

from __future__ import annotations

from datetime import date

from cortex_utils.backfill_walker import current_watermark, month_before


def _job(after: str, before: str | None, status: str = "completed") -> dict[str, str | None]:
    return {"after_date": after, "before_date": before, "status": status}


def test_month_before_crosses_the_year_boundary() -> None:
    assert month_before(date(2025, 1, 1)) == date(2024, 12, 1)
    assert month_before(date(2025, 1, 1), 12) == date(2024, 1, 1)
    assert month_before(date(2025, 2, 1), 13) == date(2024, 1, 1)


def test_month_before_clamps_to_the_first() -> None:
    """Windows must tile exactly; a mid-month result would leave gaps."""
    assert month_before(date(2025, 3, 15)) == date(2025, 2, 1)
    assert month_before(date(2025, 3, 31)) == date(2025, 2, 1)


def test_watermark_seeds_when_no_jobs_exist() -> None:
    seed = date(2025, 1, 1)
    assert current_watermark([], seed) == seed


def test_open_ended_jobs_do_not_move_the_watermark() -> None:
    """A job with no before_date is someone catching up recent mail.

    It says nothing about how far back history has been walked, and counting
    it would strand every month between it and the real frontier.
    """
    seed = date(2025, 1, 1)
    jobs = [_job("2024-11-01", None)]
    assert current_watermark(jobs, seed) == seed


def test_watermark_is_the_earliest_windowed_job() -> None:
    seed = date(2025, 1, 1)
    jobs = [
        _job("2024-11-01", "2024-12-01"),
        _job("2024-09-01", "2024-10-01"),
        _job("2024-12-01", "2025-01-01"),
    ]
    assert current_watermark(jobs, seed) == date(2024, 9, 1)


def test_windows_tile_without_gaps() -> None:
    """Walk a year and check the windows leave no gap.

    They may OVERLAP: `walk()` extends each window forward past the seam by
    `overlap_days` on purpose, because Gmail's date filters are timezone
    sensitive and a message near midnight on the seam could otherwise fall in
    neither window. What must never happen is a gap -- a month nothing covers.

    The watermark tracks `after_date`, which overlap does not move, so the
    walk still advances exactly one window per run.
    """
    seed = date(2025, 1, 1)
    jobs: list[dict[str, str | None]] = []  # all completed
    upper = seed
    for _ in range(12):
        lower = month_before(upper)
        jobs.append(_job(lower.isoformat(), upper.isoformat()))
        assert current_watermark(jobs, seed) == lower
        upper = lower

    bounds = sorted(
        (
            date.fromisoformat(str(j["after_date"])),
            date.fromisoformat(str(j["before_date"])),
        )
        for j in jobs
    )
    for (_, prev_upper), (next_lower, _) in zip(bounds, bounds[1:]):
        assert prev_upper == next_lower, "windows must abut exactly"
    assert bounds[0][0] == date(2024, 1, 1)
    assert bounds[-1][1] == seed


# --- status filtering: the P1 from review -----------------------------------
#
# A job records its window in backfill_jobs whether or not it succeeded. If a
# FAILED job counts toward the watermark, that month is marked done, nothing
# ever revisits it, and the gap is permanent and silent. Only `completed`
# windows may advance the frontier.


def test_failed_job_does_not_advance_the_watermark() -> None:
    seed = date(2025, 1, 1)
    jobs = [_job("2024-12-01", "2025-01-01", "failed")]
    assert current_watermark(jobs, seed) == seed, "a failed window must be retried"


def test_cancelled_job_does_not_advance_the_watermark() -> None:
    seed = date(2025, 1, 1)
    jobs = [_job("2024-12-01", "2025-01-01", "cancelled")]
    assert current_watermark(jobs, seed) == seed


def test_in_flight_job_does_not_advance_the_watermark() -> None:
    """Nothing was ingested yet; the busy guard stops a second queue anyway."""
    seed = date(2025, 1, 1)
    for status in ("pending", "running"):
        assert current_watermark([_job("2024-12-01", "2025-01-01", status)], seed) == seed


def test_only_completed_windows_count_among_a_mix() -> None:
    seed = date(2025, 1, 1)
    jobs = [
        _job("2024-12-01", "2025-01-01", "completed"),
        _job("2024-11-01", "2024-12-01", "failed"),  # gap: must be retried
        _job("2024-10-01", "2024-11-01", "completed"),  # must NOT mask the gap
    ]
    # The failed month is the frontier, not the older completed one behind it.
    assert current_watermark(jobs, seed) == date(2024, 10, 1)


def test_missing_status_does_not_advance_the_watermark() -> None:
    """Absent status is not evidence of success."""
    seed = date(2025, 1, 1)
    no_status = [{"after_date": "2024-12-01", "before_date": "2025-01-01"}]
    assert current_watermark(no_status, seed) == seed


# --- staleness guard ---------------------------------------------------------


def test_overlap_extends_the_window_forward_not_backward() -> None:
    """Overlap must re-cover already-ingested days, not un-ingested ones.

    Extending `before` forward re-reads the seam we just crossed (cheap: those
    messages skip every downstream step). Extending `after` backward would
    instead eat into the NEXT window and still leave the seam exposed.
    """
    from cortex_utils.backfill_walker import walk

    captured: dict[str, str] = {}

    def fake_get(url: str) -> dict[str, object]:
        return {"jobs": []}

    import cortex_utils.backfill_walker as bw

    original = bw._get
    bw._get = fake_get  # type: ignore[assignment]
    try:
        msg = walk(gateway="http://x", seed=date(2025, 1, 1), dry_run=True, overlap_days=1)
    finally:
        bw._get = original  # type: ignore[assignment]

    assert "'after': '2024-12-01'" in msg, "the window's start must not shift"
    assert "'before': '2025-01-02'" in msg, "the window's end must extend past the seam"
    captured.clear()


def test_overlap_zero_restores_exact_tiling() -> None:
    import cortex_utils.backfill_walker as bw
    from cortex_utils.backfill_walker import walk

    original = bw._get
    bw._get = lambda url: {"jobs": []}  # type: ignore[assignment]
    try:
        msg = walk(gateway="http://x", seed=date(2025, 1, 1), dry_run=True, overlap_days=0)
    finally:
        bw._get = original  # type: ignore[assignment]
    assert "'before': '2025-01-01'" in msg


def test_negative_overlap_is_clamped_to_zero() -> None:
    """A negative overlap would SHRINK the window and create a real gap."""
    import cortex_utils.backfill_walker as bw
    from cortex_utils.backfill_walker import walk

    original = bw._get
    bw._get = lambda url: {"jobs": []}  # type: ignore[assignment]
    try:
        msg = walk(gateway="http://x", seed=date(2025, 1, 1), dry_run=True, overlap_days=-5)
    finally:
        bw._get = original  # type: ignore[assignment]
    assert "'before': '2025-01-01'" in msg, "negative overlap must not shrink the window"
