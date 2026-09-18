"""Error pattern classifier for log lines."""

import hashlib
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
        # NOT historyId.*404 -- a Gmail history ID is a plain integer and
        # routinely CONTAINS "404" as a substring. Measured: 9 routine INFO
        # Pub/Sub notifications in 24h matched, e.g. historyId=79564045, each
        # of which would have fired this CRITICAL zero-cooldown alert saying
        # "Emails may be lost. Run manual backfill." They stayed quiet only
        # because the daemon's is_error_line() gate drops INFO lines first --
        # an accidental protection, not a designed one.
        re.compile(
            r"History expired"
            r"|history.*too old"
            r"|\bHttpError 404\b.*histor"
            r"|histor\w*\b.*\bHttpError 404\b"
            r"|startHistoryId.*\b404\b",
            re.IGNORECASE,
        ),
        Severity.CRITICAL,
        0,  # No cooldown - always alert
        "Gmail History Expired",
        "History ID is too old. Emails may be lost. Run manual backfill.",
    ),
    (
        # \bOOM\b, not OOM -- unanchored it matches inside ordinary words.
        # Measured: it fired on a Gmail label "Cortex/Automated/Zoom", which
        # would have been a CRITICAL "Container ran out of memory and may have
        # crashed". Same defect class as the 5xx pattern below: a substring
        # match where a token match was meant.
        re.compile(
            r"\bMemoryError\b|\bexit code 137\b|\bOOM\b|\bOut of memory\b",
            re.IGNORECASE,
        ),
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
        # NOT a bare \b5\d{2}\b -- that matches ANY three-digit number from
        # 500-599 anywhere in a line. Measured: it fired 122 times in 24h on
        # Postgres checkpoint logs ("wrote 571 buffers"), and it is why a
        # traceback frame at capture_worker.py:517 alerted as an API error in a
        # sibling project. Require HTTP context.
        re.compile(
            r"HttpError 5\d{2}"
            r"|\bHTTP[/ ]?\d?\.?\d?\s*5\d{2}\b"
            r"|\bstatus(?:[ _]code)?[=: ]\s*5\d{2}\b"
            r"|\b5\d{2}\s+(?:Internal Server Error|Bad Gateway|"
            r"Service Unavailable|Gateway Time-?out)\b",
            re.IGNORECASE,
        ),
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
    # GATE FIRST. This used to sit below the pattern loop, so a tuned pattern
    # could classify a line that is not an error at all -- and one did, 122
    # times in 24h: "checkpoint complete: wrote 571 buffers" matched the 5xx
    # pattern below and alerted as API Server Error. Nothing in that line is an
    # error; it is routine Postgres bookkeeping.
    # A client's bad SQL is not our error, whatever it matches below.
    if is_client_sql_fault(container, log_line):
        return None

    # The tuned patterns run FIRST and are deliberately NOT gated on
    # is_error_line(). They encode domain knowledge the generic indicator list
    # lacks: "History expired for historyId 12345" and "HttpError 503 Service
    # Unavailable" are both real failures, and neither contains ERROR, Failed
    # or Traceback. Gating them broke four existing tests, correctly.
    #
    # The price is that a BAD pattern can classify a line that is not an error
    # at all, so the patterns carry the burden of precision. One did not: a
    # bare \b5\d{2}\b matched "checkpoint complete: wrote 571 buffers" and
    # alerted as API Server Error 122 times in 24h. Fixed at the pattern, not
    # by gating the patterns.
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

    # The catch-all IS gated, because it is generic where the patterns are
    # specific. Without it, "INFO: Processed 10 emails successfully" would
    # alert for any caller that skipped the daemon's own gate -- a pre-existing
    # test caught exactly that.
    if not is_error_line(log_line):
        return None

    # NO SPECIFIC PATTERN, BUT is_error_line() ALREADY SAID THIS IS AN ERROR.
    # Falling through to None here is what made this component decorative:
    # measured 2026-09-18 against 40 distinct log.error/exception/critical
    # message strings harvested from cortex's own services, the tuned patterns
    # matched 2. The other 38 -- "Failed to create required label",
    # "Gmail batch modify failed", "Missing required environment variables" --
    # were correctly identified as errors by is_error_line() and then dropped
    # here because nobody had written a pattern for that phrasing.
    #
    # An alerter whose default is silence reports only the failures someone
    # already thought of, which are the ones least likely to surprise anyone.
    # Alert on the rest and let the specific patterns be ENRICHMENT -- they
    # keep their tuned severities and cooldowns; they no longer act as a gate.
    #
    # The key is hashed off the message so a service failing in a loop is one
    # alert per cooldown rather than thousands. That dedup is what makes a
    # noisy default survivable.
    # WARNING, deliberately: this severity aggregates into the daily summary
    # rather than pinging the channel, which is exactly the right landing place
    # for "an error nobody has triaged yet". A tuned pattern can still raise
    # something to HIGH or CRITICAL once someone decides it deserves that.
    digest = hashlib.sha1(log_line[:200].encode("utf-8", "replace")).hexdigest()[:8]
    return Classification(
        severity=Severity.WARNING,
        error_key=f"{container}:unclassified:{digest}",
        cooldown_minutes=60,
        title="Unclassified Error",
        description=f"Unrecognised error in {container}. No tuned pattern matched.",
    )


# Postgres logs CLIENT SQL faults at ERROR level. A database rejecting bad SQL
# is working correctly -- the bug is in whoever sent the query, and in this
# estate that is usually an agent at a psql prompt. Measured over 24h of real
# logs: 21 error lines, ALL from cortex-postgres, and 17 of them were failed
# ad-hoc queries typed by agents that evening (including several of mine:
# "aggregate functions are not allowed in GROUP BY", "unterminated quoted
# identifier", "ORDER BY position 2 is not in select list").
#
# Without this filter the catch-all above turns the notifications channel into
# a feed of our own typos, which is the fastest way to teach someone to ignore
# it. Server-side trouble -- FATAL, PANIC, shared memory, disk, corruption --
# is not matched here and still alerts.
_CLIENT_SQL_FAULT = re.compile(
    r"ERROR:\s+(?:"
    r"column .* does not exist"
    r"|relation .* does not exist"
    r"|function .* does not exist"
    r"|operator does not exist"
    r"|syntax error"
    r"|unterminated quoted"
    r"|aggregate functions are not allowed"
    r"|cannot call \w+ on"
    r"|ORDER BY position .* is not in select list"
    r"|invalid input syntax"
    r")",
    re.IGNORECASE,
)


def is_client_sql_fault(container: str, log_line: str) -> bool:
    """True for a database rejecting malformed SQL -- the caller's bug, not ours."""
    return "postgres" in container and bool(_CLIENT_SQL_FAULT.search(log_line))


# Module-level constant for performance (avoid recreating on each call)
_ERROR_INDICATORS = [
    "ERROR",
    "CRITICAL",
    "FATAL",
    # Postgres's HIGHEST severity -- the server is aborting, usually disk or
    # corruption. It was missing, so a PANIC line was not even recognised as an
    # error, let alone classified. Found by a test written for the catch-all
    # change rather than by looking for it.
    "PANIC",
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
