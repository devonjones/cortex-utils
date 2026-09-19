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
        # 500-599 anywhere in a line. Measured: it MATCHED 122 times in 24h on
        # Postgres checkpoint logs ("wrote 571 buffers"). It reads a traceback
        # frame at capture_worker.py:517 as an HTTP status the same way -- a
        # source line number. Require HTTP context.
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


# Everything in a log line that changes between two occurrences of the SAME
# fault. Hashing the raw line made the digest unique per LINE rather than per
# ERROR, which is the opposite of dedup:
#
#   * structlog renders a "timestamp" field, and for any event under ~137
#     characters it lands INSIDE the 200-char window -- so two identical
#     messages one second apart got two different keys. Measured 2026-09-19:
#     "Config reload failed" at .111111Z and at .222222Z hashed to 7949f4e4
#     and 31df69bf.
#   * messages embed per-item ids. The six "Pattern detection failed for
#     <gmail-id>" lines Hades emitted in 14 days are ONE recurring fault and
#     produced SIX distinct keys.
#
# Both matter because the catch-all's survivability rests entirely on this
# dedup: a service failing in a loop must collapse to one summary line, and
# the daily summary truncates at 20 entries. Without normalisation the first
# real incident renders 20 near-identical lines and "... and N more", which is
# useless exactly when it is needed.
_VOLATILE = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ][\d:.]+Z?"  # ISO timestamps
    r"|\b[0-9a-f]{6,}\b"  # hex ids -- gmail ids, sha digests, uuids
    r"|\[\d+\]"  # postgres's [pid] -- bracketed, so any width
    r"|\b\d{3,}\b",  # counts, durations, ports
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
    # FIRST, unconditionally: a client's bad SQL is not our error, whatever it
    # matches below. This is a source filter, not a severity gate -- Postgres
    # logs a rejected query at ERROR level and it is still Postgres working
    # correctly. See the note on is_client_sql_fault for the measurement.
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
    # bare \b5\d{2}\b MATCHED "checkpoint complete: wrote 571 buffers" 122
    # times in 24h. It never alerted -- cortex-postgres was not in the watched
    # set, and the daemon gates on is_error_line() before classifying -- so
    # this was a live landmine rather than a live incident. Fixed at the
    # pattern, not by gating the patterns.
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
    # measured 2026-09-19 by AST walk over every string literal logged at
    # error/critical/exception across six cortex services -- 67 messages -- the
    # tuned patterns matched 3. "Failed to create required label" and "Gmail
    # batch modify failed" reached this point and were dropped because nobody
    # had written a pattern for that phrasing.
    #
    # The other failure was upstream and larger: only 15 of the 67 got past the
    # gate at all, because it read the message text and a cortex service puts
    # its severity in a structlog "level" field. None of the 3 pattern-matchers
    # was among those 15, and daemon.py gates before it classifies -- so the
    # number of these 67 that could actually reach the channel was ZERO. Fixed
    # in is_error_line; the two together take the 67 from 0 reachable to 67.
    #
    # An alerter whose default is silence reports only the failures someone
    # already thought of, which are the ones least likely to surprise anyone.
    # Alert on the rest and let the specific patterns be ENRICHMENT -- they
    # keep their tuned severities and cooldowns; they no longer act as a gate.
    #
    # The key is hashed off the NORMALISED message (see _dedup_source) so a
    # service failing in a loop collapses to one summary line rather than
    # hundreds. That dedup is what makes a noisy default survivable.
    #
    # It did not work when first written, and the failure was total rather than
    # partial: hashing the raw log_line[:200] hashed a per-emit timestamp, so
    # the key was unique per LINE. Measured over 7 days of all 11 cortex
    # containers, 42 unclassified errors produced 42 distinct keys -- a
    # collapse rate of ZERO. With normalisation, 25. The comment claiming the
    # property shipped before the property did; that is what the tests in
    # TestTheDedupKeyCollapsesRealTraffic now hold in place.
    #
    # The digest is what does the work here: the
    # daemon's WARNING branch calls increment_warning() only, never
    # should_alert(), so cooldown_minutes is inert on the path that actually
    # consumes this classification. It is set for the case where a tuned
    # pattern later raises the severity to HIGH or CRITICAL, which are the
    # branches that do consult it.
    # WARNING, deliberately: this severity aggregates into the daily summary
    # rather than pinging the channel, which is exactly the right landing place
    # for "an error nobody has triaged yet". A tuned pattern can still raise
    # something to HIGH or CRITICAL once someone decides it deserves that.
    digest = hashlib.sha1(_dedup_source(log_line).encode("utf-8", "replace")).hexdigest()[:8]
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


# Cortex services log structlog JSON, where the severity lives in a "level"
# field and NOT in the message text: {"event": "Missing required environment
# variables: ...", "level": "error", ...}. The indicator list above reads the
# text only, so such a line was not recognised as an error at all -- the
# service announcing it cannot start was invisible to the alerter.
#
# Measured 2026-09-19 against every error-level line Hades emitted in 14 days
# -- 8 lines, cortex-triage-worker and cortex-labeling-worker. The text-only
# list caught 8 of 8, but NOT for the reason it looks like: six of them read
# "Pattern detection failed for <id>", and bare lowercase "failed" is not an
# indicator ("Failed" and "failed:" are). Those six matched on "Traceback",
# which structlog had serialised into the exception payload. On the event text
# alone only the 2 "Gmail batch modify failed:" lines would have passed.
#
# So the coverage rested on those six carrying exc_info. A line reporting a
# fatal condition without a traceback and without an error word -- a startup
# abort is exactly that shape -- was invisible.
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
