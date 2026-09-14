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
    jobs = [{"after_date": "2024-11-01", "before_date": None}]
    assert current_watermark(jobs, seed) == seed


def test_watermark_is_the_earliest_windowed_job() -> None:
    seed = date(2025, 1, 1)
    jobs = [
        {"after_date": "2024-11-01", "before_date": "2024-12-01"},
        {"after_date": "2024-09-01", "before_date": "2024-10-01"},
        {"after_date": "2024-12-01", "before_date": "2025-01-01"},
    ]
    assert current_watermark(jobs, seed) == date(2024, 9, 1)


def test_windows_tile_without_gaps_or_overlap() -> None:
    """Walk a year and check the windows abut exactly.

    Gmail's after: is inclusive and before: exclusive, so each window's lower
    bound must equal the previous window's upper bound.
    """
    seed = date(2025, 1, 1)
    jobs: list[dict[str, str | None]] = []
    upper = seed
    for _ in range(12):
        lower = month_before(upper)
        jobs.append({"after_date": lower.isoformat(), "before_date": upper.isoformat()})
        assert current_watermark(jobs, seed) == lower
        upper = lower

    bounds = sorted((date.fromisoformat(str(j["after_date"])), date.fromisoformat(str(j["before_date"]))) for j in jobs)
    for (_, prev_upper), (next_lower, _) in zip(bounds, bounds[1:]):
        assert prev_upper == next_lower, "windows must abut exactly"
    assert bounds[0][0] == date(2024, 1, 1)
    assert bounds[-1][1] == seed
