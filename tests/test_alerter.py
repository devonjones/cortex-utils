"""Tests for the alerter module."""

import pytest

from cortex_utils.alerter import (
    Classification,
    RateLimiter,
    Severity,
    classify,
    is_error_line,
)


class TestClassifier:
    """Tests for the error classifier."""

    def test_critical_auth_failure(self):
        """401 errors should be classified as critical."""
        result = classify("cortex-gmail-sync", "ERROR: HttpError 401: Unauthorized")
        assert result is not None
        assert result.severity == Severity.CRITICAL
        assert result.title == "Authentication Failed"
        assert result.cooldown_minutes == 0  # Always alert

    def test_critical_history_expired(self):
        """History expired should be critical."""
        result = classify("cortex-gmail-sync", "History expired for historyId 12345")
        assert result is not None
        assert result.severity == Severity.CRITICAL
        assert result.title == "Gmail History Expired"

    def test_critical_oom(self):
        """Out of memory should be critical."""
        result = classify("cortex-triage-worker", "MemoryError: unable to allocate")
        assert result is not None
        assert result.severity == Severity.CRITICAL
        assert result.title == "Out of Memory"

    def test_high_rate_limit(self):
        """429 errors should be high priority."""
        result = classify("cortex-labeling-worker", "HttpError 429: Too Many Requests")
        assert result is not None
        assert result.severity == Severity.HIGH
        assert result.title == "API Rate Limited"
        assert result.cooldown_minutes == 10

    def test_high_server_error(self):
        """5xx errors should be high priority."""
        result = classify("cortex-gmail-sync", "HttpError 503 Service Unavailable")
        assert result is not None
        assert result.severity == Severity.HIGH
        assert result.title == "API Server Error"

    def test_high_timeout(self):
        """Timeouts should be high priority."""
        result = classify("cortex-triage-worker", "TimeoutError: request timed out after 30s")
        assert result is not None
        assert result.severity == Severity.HIGH
        assert result.title == "Request Timeout"

    def test_warning_parse_failed(self):
        """Parse failures should be warnings."""
        result = classify("cortex-parse-worker", "Failed to parse email: invalid format")
        assert result is not None
        assert result.severity == Severity.WARNING
        assert result.title == "Parse Failed"

    def test_no_match_normal_log(self):
        """Normal log lines should return None."""
        result = classify("cortex-gmail-sync", "INFO: Processed 10 emails successfully")
        assert result is None

    def test_error_key_format(self):
        """Error key should include container and normalized title."""
        result = classify("cortex-triage-worker", "HttpError 429: Rate limit")
        assert result is not None
        assert result.error_key == "cortex-triage-worker:api_rate_limited"


class TestIsErrorLine:
    """Tests for the error line pre-filter."""

    def test_error_line(self):
        assert is_error_line("ERROR: something failed") is True

    def test_critical_line(self):
        assert is_error_line("CRITICAL: database down") is True

    def test_exception_line(self):
        assert is_error_line("Exception in thread") is True

    def test_traceback_line(self):
        assert is_error_line("Traceback (most recent call last):") is True

    def test_info_line(self):
        assert is_error_line("INFO: all good") is False

    def test_debug_line(self):
        assert is_error_line("DEBUG: processing message") is False


class TestRateLimiter:
    """Tests for the rate limiter."""

    def test_first_alert_allowed(self):
        """First alert for a key should always be allowed."""
        rl = RateLimiter()
        assert rl.should_alert("test_key", 5) is True

    def test_second_alert_blocked(self):
        """Second alert within cooldown should be blocked."""
        rl = RateLimiter()
        assert rl.should_alert("test_key", 5) is True
        assert rl.should_alert("test_key", 5) is False

    def test_zero_cooldown_always_alerts(self):
        """Zero cooldown should always alert."""
        rl = RateLimiter()
        assert rl.should_alert("test_key", 0) is True
        assert rl.should_alert("test_key", 0) is True
        assert rl.should_alert("test_key", 0) is True

    def test_different_keys_independent(self):
        """Different keys should be independent."""
        rl = RateLimiter()
        assert rl.should_alert("key1", 5) is True
        assert rl.should_alert("key2", 5) is True
        assert rl.should_alert("key1", 5) is False
        assert rl.should_alert("key2", 5) is False

    def test_warning_counts(self):
        """Warning counts should accumulate."""
        rl = RateLimiter()
        rl.increment_warning("parse_failed")
        rl.increment_warning("parse_failed")
        rl.increment_warning("unknown_category")

        counts = rl.get_warning_counts()
        assert counts["parse_failed"] == 2
        assert counts["unknown_category"] == 1

    def test_reset_warning_counts(self):
        """Reset should return and clear counts."""
        rl = RateLimiter()
        rl.increment_warning("parse_failed")
        rl.increment_warning("parse_failed")

        counts = rl.reset_warning_counts()
        assert counts["parse_failed"] == 2

        # After reset, should be empty
        assert rl.get_warning_counts() == {}

    def test_time_until_alert_no_previous(self):
        """No previous alert should return None."""
        rl = RateLimiter()
        assert rl.time_until_alert("test_key", 5) is None

    def test_time_until_alert_can_alert(self):
        """Past cooldown should return None."""
        from datetime import datetime, timedelta

        rl = RateLimiter()
        # Manually set last alert to 10 minutes ago
        rl.last_alert["test_key"] = datetime.now() - timedelta(minutes=10)

        assert rl.time_until_alert("test_key", 5) is None

    def test_time_until_alert_in_cooldown(self):
        """Within cooldown should return remaining time."""
        rl = RateLimiter()
        rl.should_alert("test_key", 5)  # Sets last_alert to now

        remaining = rl.time_until_alert("test_key", 5)
        assert remaining is not None
        # Should be close to 5 minutes (with some tolerance)
        assert remaining.total_seconds() > 4.5 * 60
        assert remaining.total_seconds() <= 5 * 60
