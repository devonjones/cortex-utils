"""Error pattern classifier for log lines."""

import hashlib
import json
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
        # Not historyId.*404: history ids are integers that routinely
        # contain "404" (historyId=79564045). Require 404 to be an HTTP
        # status.
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
        # Word-bounded: unanchored, OOM matches inside ordinary words --
        # it fired on the Gmail label "Cortex/Automated/Zoom".
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
        # Require HTTP context. A bare \b5\d{2}\b matches any number in
        # 500-599 anywhere in a line -- Postgres's "wrote 571 buffers", a
        # traceback's line 517.
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


# Everything that differs between two occurrences of the SAME fault. The key
# must identify the fault, not the line: structlog stamps every emit with a
# timestamp that lands inside the 200-char window, and messages embed per-item
# ids, so hashing the raw line gave one key per line. Measured over 7 days of
# all cortex containers: 42 unclassified errors, 42 distinct keys without
# normalisation, 25 with.
#
# This is load-bearing. The daily summary truncates at 20 entries, so without
# it one looping service fills the summary with near-identical lines exactly
# when something is wrong.
_VOLATILE = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ][\d:.]+Z?"  # ISO timestamps
    r"|\b[0-9a-f]{6,}\b"  # hex ids -- gmail ids, sha digests, uuids
    r"|\[\d+\]",  # postgres's [pid] -- bracketed, so any width
    # No bare \b\d{3,}\b: on real traffic it collapsed nothing the rules
    # above did not (25 keys either way) while merging distinct faults --
    # exit code 137 with 139, HttpError 404 with 410.
    re.IGNORECASE,
)


def _dedup_source(log_line: str) -> str:
    """The part of a line that is the same on every occurrence of one fault.

    structlog lines are JSON, and only "event" carries the fault; every other
    field is either constant (service, logger, level) or volatile (timestamp).
    Falls back to the raw line for anything that is not structlog JSON --
    Postgres and Traefik log plain text.
    """

    def message(line: str) -> str:
        try:
            parsed = json.loads(line)
        except (ValueError, TypeError):
            return line
        event = parsed.get("event") if isinstance(parsed, dict) else None
        return event if isinstance(event, str) else line

    return _VOLATILE.sub("<>", message(log_line))[:200]


def classify(container: str, log_line: str) -> Classification | None:
    """Classify a log line and return alert info if it's an error.

    Args:
        container: Container name (e.g., "cortex-gmail-sync")
        log_line: The log line to classify

    Returns:
        Classification if this is an error worth tracking, None otherwise
    """
    # A client's bad SQL is not our error, whatever it matches below.
    if is_client_sql_fault(container, log_line):
        return None

    # Tuned patterns are deliberately NOT gated on is_error_line(): "History
    # expired for historyId 12345" and "HttpError 503" are real failures
    # containing no error word. The price is that an over-broad pattern
    # classifies a non-error, so the patterns carry the burden of precision.
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

    # The catch-all IS gated: it is generic where the patterns are specific,
    # so ungated it would classify "INFO: Processed 10 emails successfully".
    if not is_error_line(log_line):
        return None

    # No tuned pattern, but the line is an error: alert on it anyway. Before
    # this, an unrecognised error was dropped here, and of 67 error messages
    # cortex can emit, 3 matched a pattern and 0 could reach the channel.
    # The patterns are ENRICHMENT -- tuned severity and cooldown -- not a gate.
    #
    # WARNING is deliberate: it aggregates into the daily summary rather than
    # pinging the channel, which is where an untriaged error belongs.
    # cooldown_minutes is unused on this path (the WARNING branch only counts,
    # and a tuned pattern returns before reaching here); the dataclass
    # requires a number.
    digest = hashlib.sha1(_dedup_source(log_line).encode("utf-8", "replace")).hexdigest()[:8]
    return Classification(
        severity=Severity.WARNING,
        error_key=f"{container}:unclassified:{digest}",
        cooldown_minutes=60,
        title="Unclassified Error",
        description=f"Unrecognised error in {container}. No tuned pattern matched.",
    )


# Postgres logs CLIENT SQL faults at ERROR level. A database rejecting bad SQL
# is working correctly -- the bug is in whoever sent the query, and here that
# is usually an agent at a psql prompt. Without this filter the catch-all
# turns the channel into a feed of our own typos.
#
# Deliberately narrow. Server-side distress -- FATAL, PANIC, shared memory,
# disk, and btree corruption (see cortex-rj2b) -- must NOT be matched here.
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
    # Postgres's highest severity: the server is aborting.
    "PANIC",
    "Exception",
    "Traceback",
    "Error:",
    "Failed",
    "error:",
    "failed:",
]


# Cortex services log structlog JSON with the severity in a "level" field,
# not in the message text: {"event": "Missing required environment variables",
# "level": "error"}. The indicator list above reads text only, so a service
# reporting it cannot start was not recognised as an error at all.
_LEVEL_FIELD = re.compile(r'"level"\s*:\s*"(error|critical)"')


def is_error_line(log_line: str) -> bool:
    """Quick check if a log line looks like an error.

    Use this to pre-filter before full classification. Matches either an error
    word in the message text or a structlog ``"level": "error"`` field, because
    a service can report a fatal condition without using any of those words.
    """
    if any(indicator in log_line for indicator in _ERROR_INDICATORS):
        return True
    return _LEVEL_FIELD.search(log_line) is not None
