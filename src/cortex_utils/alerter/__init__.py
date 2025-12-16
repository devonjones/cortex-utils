"""Discord alerter for monitoring Cortex services.

Provides log tailing, error classification, rate limiting, and Discord alerts.
"""

from .classifier import Classification, Severity, classify, is_error_line
from .daemon import AlerterDaemon, run_alerter
from .discord import (
    COLOR_CRITICAL,
    COLOR_HIGH,
    COLOR_INFO,
    COLOR_WARNING,
    DiscordClient,
)
from .rate_limiter import RateLimiter

__all__ = [
    # Daemon
    "AlerterDaemon",
    "run_alerter",
    # Discord client
    "DiscordClient",
    "COLOR_CRITICAL",
    "COLOR_HIGH",
    "COLOR_WARNING",
    "COLOR_INFO",
    # Classifier
    "classify",
    "is_error_line",
    "Classification",
    "Severity",
    # Rate limiter
    "RateLimiter",
]
