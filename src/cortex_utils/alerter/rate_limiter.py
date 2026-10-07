"""Rate limiter for alert deduplication."""

from collections import defaultdict
from datetime import datetime, timedelta


class RateLimiter:
    """Rate limiter to prevent alert spam.

    Tracks when each error_key was last alerted and enforces
    a cooldown period before allowing another alert.
    """

    def __init__(self):
        self.last_alert: dict[str, datetime] = {}
        self.warning_counts: dict[str, int] = defaultdict(int)
        # One representative message per key. The counts alone rendered a
        # summary of opaque digests -- "Unclassified:8A416200 (cortex-gateway):
        # 2" -- which told a reader nothing about what had failed.
        self.warning_samples: dict[str, str] = {}
        self.last_reset: datetime = datetime.now()

    def should_alert(self, error_key: str, cooldown_minutes: int) -> bool:
        """Check if we should send an alert for this error.

        Args:
            error_key: Unique identifier for this error type
            cooldown_minutes: Minimum minutes between alerts (0 = always alert)

        Returns:
            True if we should alert, False if still in cooldown
        """
        if cooldown_minutes == 0:
            # No cooldown - always alert
            self.last_alert[error_key] = datetime.now()
            return True

        last = self.last_alert.get(error_key)
        now = datetime.now()

        if last is None or (now - last) >= timedelta(minutes=cooldown_minutes):
            self.last_alert[error_key] = now
            return True

        return False

    def increment_warning(self, error_key: str, sample: str = "") -> None:
        """Increment warning count for daily summary aggregation.

        `sample` is one representative message for this key, kept so the daily
        summary can say what failed rather than only how often. The FIRST is
        kept: every later occurrence shares the key, so they describe the same
        fault, and keeping the first avoids re-writing the dict on every line.
        """
        self.warning_counts[error_key] += 1
        if sample and error_key not in self.warning_samples:
            self.warning_samples[error_key] = sample

    def get_warning_counts(self) -> dict[str, int]:
        """Get accumulated warning counts since last reset."""
        return dict(self.warning_counts)

    def reset_warning_counts(self) -> dict[str, int]:
        """Reset warning counts and return the old values.

        Call this after sending daily summary.
        """
        counts = dict(self.warning_counts)
        self.warning_counts.clear()
        self.warning_samples.clear()
        self.last_reset = datetime.now()
        return counts

    def time_until_alert(self, error_key: str, cooldown_minutes: int) -> timedelta | None:
        """Get time remaining until this error can alert again.

        Returns:
            Time remaining, or None if can alert now
        """
        if cooldown_minutes == 0:
            return None

        last = self.last_alert.get(error_key)
        if last is None:
            return None

        elapsed = datetime.now() - last
        cooldown = timedelta(minutes=cooldown_minutes)

        if elapsed >= cooldown:
            return None

        return cooldown - elapsed
