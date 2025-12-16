"""Error pattern classifier for log lines."""

import re
from dataclasses import dataclass
from enum import Enum


class Severity(Enum):
    """Alert severity levels."""

    CRITICAL = "critical"  # Immediate alert, ping channel
    HIGH = "high"  # Alert with cooldown
    WARNING = "warning"  # Aggregate for daily summary
    IGNORE = "ignore"  # Not an error


@dataclass
class Classification:
    """Result of classifying a log line."""

    severity: Severity
    error_key: str  # Unique key for deduplication
    cooldown_minutes: int  # Minimum time between alerts for this key
    title: str  # Short title for alert
    description: str  # Longer description


# Pattern definitions: (regex, severity, cooldown_minutes, title, description)
PATTERNS: list[tuple[re.Pattern, Severity, int, str, str]] = [
    # === CRITICAL (data loss risk, auth failures) ===
    (
        re.compile(r"History expired|historyId.*404|history.*too old", re.IGNORECASE),
        Severity.CRITICAL,
        0,  # No cooldown - always alert
        "Gmail History Expired",
        "History ID is too old. Emails may be lost. Run manual backfill.",
    ),
    (
        re.compile(r"MemoryError|exit code 137|OOM|Out of memory", re.IGNORECASE),
        Severity.CRITICAL,
        0,
        "Out of Memory",
        "Container ran out of memory and may have crashed.",
    ),
    (
        re.compile(r"HttpError 401|Unauthorized|401 Unauthorized"),
        Severity.CRITICAL,
        0,
        "Authentication Failed",
        "API authentication failed. Token may need refresh.",
    ),
    (
        re.compile(r"HttpError 403|Forbidden|403 Forbidden|permission denied", re.IGNORECASE),
        Severity.CRITICAL,
        5,
        "Permission Denied",
        "API permission denied. Check OAuth scopes or token.",
    ),
    (
        re.compile(r"SIGKILL|killed|Killed"),
        Severity.CRITICAL,
        0,
        "Container Killed",
        "Container was killed (likely OOM or manual stop).",
    ),
    (
        re.compile(
            r"psycopg2\.OperationalError|database.*connection|connection.*database",
            re.IGNORECASE,
        ),
        Severity.CRITICAL,
        5,
        "Database Connection Failed",
        "Cannot connect to PostgreSQL. Service is degraded.",
    ),
    # === HIGH (degraded service, recoverable) ===
    (
        re.compile(r"HttpError 429|429 Too Many Requests|rate.?limit", re.IGNORECASE),
        Severity.HIGH,
        10,
        "API Rate Limited",
        "Gmail API rate limit hit. Service is backing off.",
    ),
    (
        re.compile(r"HttpError 5\d{2}|5\d{2} "),
        Severity.HIGH,
        5,
        "API Server Error",
        "Gmail API returned server error. Will retry.",
    ),
    (
        re.compile(r"Connection refused|ECONNREFUSED|ConnectionRefusedError"),
        Severity.HIGH,
        5,
        "Connection Refused",
        "Cannot connect to service. It may be down.",
    ),
    (
        re.compile(r"timeout|timed out|TimeoutError", re.IGNORECASE),
        Severity.HIGH,
        10,
        "Request Timeout",
        "Request timed out. Service may be slow or overloaded.",
    ),
    (
        re.compile(r"Ollama.*error|ollama.*fail|LLM.*error", re.IGNORECASE),
        Severity.HIGH,
        10,
        "LLM Error",
        "Ollama/LLM request failed. Classification may fall back to rules.",
    ),
    # === WARNING (aggregate for daily summary) ===
    (
        re.compile(r"Failed to parse|parse.*failed|parsing.*error", re.IGNORECASE),
        Severity.WARNING,
        0,
        "Parse Failed",
        "Email parsing failed.",
    ),
    (
        re.compile(r"unknown.*category|category.*unknown|unclassified", re.IGNORECASE),
        Severity.WARNING,
        0,
        "Unknown Category",
        "Email could not be classified.",
    ),
    (
        re.compile(r"attachment.*too large|file.*too large|skip.*large", re.IGNORECASE),
        Severity.WARNING,
        0,
        "Attachment Skipped",
        "Attachment too large, skipped.",
    ),
    (
        re.compile(r"retry.*failed|max.*attempts|exceeded.*retries", re.IGNORECASE),
        Severity.WARNING,
        0,
        "Retry Exhausted",
        "Job failed after max retries.",
    ),
]


def classify(container: str, log_line: str) -> Classification | None:
    """Classify a log line and return alert info if it's an error.

    Args:
        container: Container name (e.g., "cortex-gmail-sync")
        log_line: The log line to classify

    Returns:
        Classification if this is an error worth tracking, None otherwise
    """
    for pattern, severity, cooldown, title, description in PATTERNS:
        if pattern.search(log_line):
            # Create unique key for deduplication
            error_key = f"{container}:{title.lower().replace(' ', '_')}"

            return Classification(
                severity=severity,
                error_key=error_key,
                cooldown_minutes=cooldown,
                title=title,
                description=description,
            )

    return None


# Module-level constant for performance (avoid recreating on each call)
_ERROR_INDICATORS = [
    "ERROR",
    "CRITICAL",
    "FATAL",
    "Exception",
    "Traceback",
    "Error:",
    "Failed",
    "error:",
    "failed:",
]


def is_error_line(log_line: str) -> bool:
    """Quick check if a log line looks like an error.

    Use this to pre-filter before full classification.
    """
    return any(indicator in log_line for indicator in _ERROR_INDICATORS)
