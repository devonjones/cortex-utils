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
from html import unescape

import html2text

from cortex_utils.log import get_logger

# Library-side logger: writes to stderr and never calls structlog.configure(),
# so importing this module cannot hijack a consumer's logging setup.
log = get_logger()

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

# Counts OPENING tags, for detection only. Deliberately does not match "</p>":
# a closing tag is not evidence of anything the tag count is trying to measure.
_TAG_RE = re.compile(r"<[a-zA-Z][a-zA-Z0-9]*[^>]*>")

# Strips ANY tag, for the salvage path. Separate from _TAG_RE because stripping
# and counting want opposite things -- reusing the counting pattern here left
# every closing tag in the output.
_ANY_TAG_RE = re.compile(r"<[^>]*>")
# Script and style carry code, not prose; their bodies must go with the tags.
_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1\s*>", re.IGNORECASE | re.DOTALL)


def _converter() -> html2text.HTML2Text:
    """A fresh converter per call.

    HTML2Text accumulates output on the instance, so a module-level singleton
    corrupts concurrent callers -- which the original home for this code
    (postmark's duckdb_api, a FastAPI app whose sync `def` routes run in
    uvicorn's thread pool) could do. Construction only sets attributes, so
    per-call costs nothing worth measuring and removes the shared mutable state
    entirely.
    """
    h = html2text.HTML2Text()
    h.ignore_links = False  # Keep link URLs as markdown [text](url)
    h.ignore_images = True  # Skip image references
    h.ignore_emphasis = False  # Keep **bold** and *italic* markdown
    h.body_width = 0  # Don't wrap lines
    h.single_line_break = True  # More compact output
    # Keep real characters instead of ASCII approximations. Off by default,
    # html2text renders entities down to ASCII: "Reuni&oacute;n" arrives as
    # "Reunion" and "Jos&#233;" as "Jose", while the same characters sent as
    # literal UTF-8 survive -- so a name is mangled or not depending only on
    # how the sender encoded it. Districts mail in Spanish and people's names
    # carry accents; silently rewriting them is not an acceptable default.
    h.unicode_snob = True
    return h


def html_to_markdown(html_content: str) -> str:
    """Convert HTML to markdown. Links kept, images dropped, no line wrapping.

    Raises whatever html2text raises. `to_text()` is the forgiving entry point;
    this one stays honest so a caller that wants to know can.
    """
    if not html_content:
        return ""
    # unicode_snob keeps &nbsp; as U+00A0, which reads as a space but breaks
    # naive equality and looks like a stray character in a digest. Normalise it
    # here rather than giving up the rest of what unicode_snob preserves.
    return _converter().handle(html_content).replace("\xa0", " ").strip()


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


def _has_content(text: str) -> bool:
    """Whether converted output is worth preferring over another MIME part.

    An image-only or table-skeleton email converts to markdown punctuation --
    "---" for a bare table, say -- which is non-empty but says nothing, and
    would hide the other part that may carry the actual message.

    This decides FALLTHROUGH ONLY, never whether output is returnable. A body
    that is legitimately all symbols (a checkmark attendance grid, an ASCII
    diagram) is still that message's content when no other part does better.
    """
    return any(ch.isalnum() for ch in text)


def _tags_stripped(html_content: str) -> str:
    """Last-resort plain text when the markdown converter itself failed.

    Crude by construction -- if html2text could not parse it, the markup is
    malformed and there is nothing better to do than drop tags, decode
    entities and collapse whitespace. Gives the reader the words.
    """
    text = unescape(_ANY_TAG_RE.sub(" ", _SCRIPT_STYLE_RE.sub(" ", html_content)))
    return " ".join(text.split())


def _try_markdown(content: str, what: str) -> str | None:
    """Convert, or log and report failure. Never raises.

    Returns None for "conversion failed" and "" for "converted to nothing".
    They are different: a failure means fall back to the raw text, while an
    empty result means this part genuinely carried no readable content and the
    caller should try the next one.

    Broad catch on purpose, but not because malformed HTML is known to raise:
    a fuzz over ~3000 adversarial inputs found no single-threaded failure, and
    the AssertionError this module's tests provoke from html.parser needs a
    shared converter, which `_converter()` no longer permits. The reason is
    weaker and sufficient -- a third-party parser over input from arbitrary
    senders, where one bad message must not take down the worker draining a
    mailbox.
    """
    try:
        return html_to_markdown(content)
    except Exception as e:  # noqa: BLE001 - see docstring
        log.warning("html2text failed, falling back to raw text", part=what, error=str(e))
        return None


def to_text(*, body_text: str | None, body_html: str | None) -> str:
    """Best readable text for a message, from whichever parts exist.

    The order matters. A real `text/plain` part is the sender's own rendering and
    beats anything derived, but only once it has been checked for being HTML in
    disguise -- so a mislabelled part falls through to conversion rather than
    reaching a reader as raw markup.

    Keyword-only. Both arguments are nullable strings read from adjacent columns,
    so a positional call site that swapped them would type-check cleanly and
    return confident nonsense; naming them makes the swap visible.

    Never raises: a conversion failure degrades to the raw text rather than
    propagating. Returns "" when there is nothing usable, so callers can treat
    "no body" as one case instead of juggling None against empty string.
    """
    # Best thing seen that no reader would call content -- a bare table rule,
    # say. Returned only if no part does better, because "---" still beats
    # dropping a body that genuinely is all symbols.
    fallback = ""

    if body_text and body_text.strip():
        if not looks_like_html(body_text):
            return body_text.strip()
        converted = _try_markdown(body_text, "mislabelled text/plain")
        if converted is None:
            # Conversion failed. Raw markup reads badly but beats nothing,
            # and it is what postmark's call sites did.
            return body_text.strip()
        if _has_content(converted):
            return converted
        fallback = fallback or converted

    if body_html and body_html.strip():
        converted = _try_markdown(body_html, "text/html")
        if converted is None:
            # Symmetric with the branch above: salvage the words rather than
            # collapsing a failure into the same "" a genuinely empty body
            # returns. There is no text/plain to fall back to here, and
            # html-only is the majority of real mail, so this is the path that
            # matters most.
            return _tags_stripped(body_html)
        if _has_content(converted):
            return converted
        fallback = fallback or converted

    return fallback
