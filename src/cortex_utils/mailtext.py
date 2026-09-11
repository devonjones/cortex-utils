"""Turn an email body into readable plain text.

Lifted out of postmark's duckdb_api so a second consumer does not become a third
implementation. postmark had two already -- this one, and a BeautifulSoup
`get_text()` in `postmark/email/parser.py` -- and cortex-school needs the same
thing because 71% of the mail it reads (646 of 910 school emails) has an empty
`body_text` and only `body_html`.

`body_text` being empty is not a bug to repair upstream: those messages
genuinely have no `text/plain` part. Derived plaintext is a different value from
the stored one, so it is computed at read time rather than backfilled into the
column.
"""

from __future__ import annotations

import re

import html2text

# Patterns that suggest code or configuration rather than an HTML email. Checked
# first, because a JSON body full of angle brackets should not be run through a
# markdown converter.
_CODE_PATTERNS = (
    "<?xml",  # XML declaration
    "<configuration>",  # Config files
    "<project>",  # Maven/build files
    '{"',  # JSON (not HTML)
    "{{",  # Template variables
    "<template>",  # Template files
)

# Email-specific HTML patterns. General HTML/XML markup is deliberately not
# enough on its own -- see looks_like_html().
_HTML_INDICATORS = (
    "<!doctype html",
    "<html",
    "<head>",
    "<body",
    "<div",
    "<table",
    "<tr>",
    "<td",
    "cellpadding=",  # Table attributes common in email HTML
    "cellspacing=",
    "bgcolor=",
    'align="center"',  # Email layout patterns
    'style="',  # Inline styles (common in emails)
    'class="',
)

_TAG_RE = re.compile(r"<[a-zA-Z][a-zA-Z0-9]*[^>]*>")


def _converter() -> html2text.HTML2Text:
    """A fresh converter per call.

    HTML2Text accumulates output on the instance, so a module-level singleton
    interleaves two documents when called concurrently -- which the original
    home for this code (a Flask app that may be served by a threaded worker)
    could do. Construction only sets attributes, so per-call costs nothing worth
    measuring and removes the shared mutable state entirely.
    """
    h = html2text.HTML2Text()
    h.ignore_links = False  # Keep link URLs as markdown [text](url)
    h.ignore_images = True  # Skip image references
    h.ignore_emphasis = False  # Keep **bold** and *italic* markdown
    h.body_width = 0  # Don't wrap lines
    h.single_line_break = True  # More compact output
    return h


def html_to_markdown(html_content: str) -> str:
    """Convert HTML to markdown. Links kept, images dropped, no line wrapping."""
    if not html_content:
        return ""
    return _converter().handle(html_content).strip()


def looks_like_html(text: str) -> bool:
    """Whether text labelled `text/plain` is actually HTML.

    Some senders mislabel HTML as text/plain, so plain-text parts cannot be
    trusted to be plain. Thresholds are conservative on purpose -- a false
    positive mangles genuinely plain text, which is worse than leaving some
    mislabelled HTML unconverted:

    - at least 50 chars (shorter cannot be meaningful HTML)
    - only the first 8KB is sampled (some mail carries long tracking URLs first)
    - 5+ tags, to avoid tripping on a small XML or code snippet
    - 2+ email-specific indicators on top of that
    - excluded outright if it looks like XML, JSON, or a template
    """
    if not text or len(text) < 50:
        return False

    sample = text[:8000]
    if "<" not in sample:
        return False

    sample_lower = sample.lower()
    if any(pattern in sample_lower for pattern in _CODE_PATTERNS):
        return False

    if len(_TAG_RE.findall(sample)) < 5:
        return False

    indicators = sum(1 for ind in _HTML_INDICATORS if ind in sample_lower)
    return indicators >= 2


def to_text(body_text: str | None, body_html: str | None) -> str:
    """Best readable text for a message, from whichever parts exist.

    The order matters. A real `text/plain` part is the sender's own rendering and
    beats anything derived, but only once it has been checked for being HTML in
    disguise -- so a mislabelled part falls through to conversion rather than
    reaching a reader as raw markup.

    Returns "" when there is nothing usable, so callers can treat "no body" as
    one case instead of juggling None against empty string.
    """
    if body_text and body_text.strip():
        if looks_like_html(body_text):
            converted = html_to_markdown(body_text)
            if converted:
                return converted
        else:
            return body_text.strip()

    if body_html and body_html.strip():
        return html_to_markdown(body_html)

    return ""
