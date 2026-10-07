"""LLM client for OpenAI-compatible endpoints.

Supports LiteLLM proxy, Ollama with /v1 endpoints, and any OpenAI-compatible API.
"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

from cortex_utils.logging import get_logger

logger = get_logger(__name__)


class LLMError(Exception):
    """Raised when LLM call fails (network, HTTP, or invalid response)."""

    # Whether repeating the identical call could plausibly succeed.
    retryable = True


class LLMTruncatedError(LLMError):
    """The model stopped because it ran out of budget, not because it finished.

    A truncated response is indistinguishable from a complete one by shape --
    valid JSON, plausible fields -- which is why this is raised rather than
    returned.

    Observed on this estate's ollama with gemma4:26b (measured in a sibling
    project, NOT re-measured here, and no cortex service is configured to use
    that model): a reasoning model emits a `thinking` field before `content`,
    so under a tight max_tokens the whole budget goes on thinking and `content`
    comes back an EMPTY STRING with HTTP 200 and done_reason "length".

    NOT RETRYABLE, and that is the point. The stop is budget exhaustion, not a
    sampling accident, so repeating the identical call mostly reproduces it
    while only the attempt counter moves -- a sibling project measured a day
    failing 23 times on the same unparseable bytes and staying blank for a week.
    The escape is to change an input -- a larger budget, a different model,
    think=False -- not to repeat the call.

    An earlier version argued this from "at temperature 0 a retry is
    deterministic". That premise does not hold here: only check_intent passes
    temperature=0, and check_email_intent carries a comment explicitly refusing
    it because some models return empty content at 0.
    """

    retryable = False


# Max characters of email body to include in LLM prompts
LLM_BODY_PREVIEW_LENGTH = 1000

# Patterns for extracting reply chain from email bodies
_REPLY_PATTERNS = [
    # "On Mon, Jan 15, 2025 at 6:28 PM, Person Name <email@example.com> wrote:"
    re.compile(
        r"On\s+.{10,60}\s+(.+?)\s*<([^>]+)>\s*wrote:",
        re.IGNORECASE,
    ),
    # Same pattern but name on next line (long titles/orgs)
    re.compile(
        r"<([^>]+@[^>]+)>\s*wrote:",
        re.IGNORECASE,
    ),
    # "From: Person Name <email@example.com>"
    re.compile(
        r"^From:\s*(.+?)\s*<([^>]+)>",
        re.MULTILINE | re.IGNORECASE,
    ),
    # "[person] <email> schrieb am" (German Outlook)
    re.compile(
        r"(.+?)\s*<([^>]+)>\s*schrieb\s+am",
        re.IGNORECASE,
    ),
]


def extract_reply_hierarchy(body: str | None, from_addr: str) -> str:
    """Extract reply chain participants from email body.

    Parses "On [date], Person <email> wrote:" and "From: Person <email>"
    patterns to build a structured view of who participated in the thread.

    Returns a formatted string like:
        Reply chain:
        1. sender@example.com (this email)
        2. bob@company.com (quoted)
        3. alice@company.com (quoted)

    Or "No reply chain detected" if no patterns found.
    """
    if not body:
        return "No reply chain detected"

    # Collect unique email addresses in order of appearance
    seen_emails: set[str] = set()
    participants: list[str] = []  # email addresses

    all_matches = []
    for pattern in _REPLY_PATTERNS:
        all_matches.extend(pattern.finditer(body))

    for match in sorted(all_matches, key=lambda m: m.start()):
        groups = match.groups()
        # Patterns with 2 groups: (name, email). With 1 group: (email,)
        email = groups[-1].strip()
        # Clean up mailto: artifacts
        if "<" in email:
            email = email.split("<")[0].strip()
        email_lower = email.lower()
        if email_lower not in seen_emails:
            seen_emails.add(email_lower)
            participants.append(email)

    if not participants:
        return "No reply chain detected"

    lines = [f"1. {from_addr} (this email)"]
    for i, email in enumerate(participants, 2):
        lines.append(f"{i}. {email} (quoted)")

    return "Reply chain:\n" + "\n".join(lines)


# Invalid LLM extraction responses to reject
INVALID_LLM_EXTRACTION_VALUES = {
    "none",
    "n/a",
    "unknown",
    "null",
    "undefined",
}


class LLMClient:
    """Client for LLM calls (supports OpenAI-compatible endpoints)."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=120.0)

    def _post_completion(
        self,
        model: str,
        prompt: str,
        max_tokens: int,
        response_format: dict[str, str] | None = None,
        temperature: float | None = None,
        reject_truncated: bool = True,
    ) -> dict[str, Any]:
        """Post to the completions endpoint and return the result.

        Args:
            model: Model name to use.
            prompt: The prompt content to send.
            max_tokens: Maximum tokens for the response.
            response_format: Optional response format (e.g., {"type": "json_object"}).
            temperature: Sampling temperature (0 = deterministic). None = model default.
            reject_truncated: Raise if the model stopped at the token budget.
                On by default. Callers turn it OFF only where a truncated body
                cannot produce a WRONG answer, just a negative one -- see
                check_intent.

        Raises:
            httpx.RequestError: Network errors.
            httpx.HTTPStatusError: HTTP errors (non-2xx status).
            LLMTruncatedError: The model stopped at the token budget.
            LLMError: The response shape is unreadable.
        """
        payload: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
        }
        if temperature is not None:
            payload["temperature"] = temperature
        if response_format:
            payload["response_format"] = response_format

        response = self.client.post(
            f"{self.base_url}/v1/chat/completions",
            json=payload,
        )
        response.raise_for_status()
        result: dict[str, Any] = response.json()

        # Some models (qwen3.5, qwen3) return empty content on the chat
        # completions API. Fall back to native Ollama /api/generate endpoint.
        try:
            content = self._get_content_from_response(result)
        except LLMError:
            # A body can be both truncated and unreadable -- content null, or
            # choices cut off entirely. Truncation is the more specific
            # diagnosis and the non-retryable one, so let it win.
            if reject_truncated:
                self._reject_if_truncated(result)
            raise
        if not content.strip():
            logger.debug("Empty response from chat completions, falling back to native API")
            native_payload: dict[str, Any] = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                # Reasoning models spend the budget on a `thinking` field and
                # return empty `content`. Say so explicitly, so swapping in a
                # reasoning model cannot silently empty every response.
                "think": False,
            }
            # Don't pass num_predict — thinking models (qwen3.5) consume
            # the token budget on thinking tokens, leaving nothing for
            # the visible answer. Let the model decide when to stop.
            native_response = self.client.post(
                f"{self.base_url}/api/generate",
                json=native_payload,
            )
            native_response.raise_for_status()
            native_result = native_response.json()
            # Wrap in chat completions format for consistent handling
            result = {
                "choices": [
                    {
                        "message": {
                            "content": native_result.get("response", ""),
                        },
                        # Carried through, not dropped: the wrap used to lose it,
                        # so a truncated native response arrived looking complete.
                        "finish_reason": native_result.get("done_reason"),
                    }
                ]
            }
        # After the recovery, never before it: an empty length-stop has to
        # reach /api/generate first, or the think=False retry is unreachable
        # for the exact case it exists for. Pinned by
        # test_an_empty_truncated_response_still_reaches_the_recovery.
        if reject_truncated:
            self._reject_if_truncated(result)

        return result

    @staticmethod
    def _required(data: dict[str, Any], field: str) -> Any:
        """The value of `field`, or raise. Never a default.

        A default turns "the model said nothing" into "the model said this",
        which is indistinguishable downstream.

        An explicit value is honoured, including an explicit "unknown": the
        distinction drawn here is present-vs-absent, NOT a value or type
        check. For a field a caller will use as a string, that is not enough
        -- see _required_str.
        """
        if field not in data:
            raise LLMError(
                f"LLM response has no {field!r}; the prompt asks for it, so the "
                "response is incomplete rather than a judgement of absence"
            )
        return data[field]

    @classmethod
    def _required_str(cls, data: dict[str, Any], field: str) -> str:
        """Present AND a string. A type annotation is not a guard.

        `classify` is annotated `-> tuple[str, float, str]` and three services
        read `category` out of it, but nothing enforced the type, so a model
        answering `{"category": ["Newsletter"]}` handed a LIST to callers that
        had been told it was a str.

        Where that lands, traced 2026-10-07 rather than assumed:

            triage/engine/matcher.py:1301  unpacks the tuple
            matcher.py:1307                `if category in rule.routes`
            rule.routes                    dict[str, Action]
            -> TypeError: unhashable type: 'list'

        The nearest handler is `except VariableError` at matcher.py:1227,
        which does not catch it, so it reaches the worker's broad
        `except Exception` -> _fail_job -> three attempts -> dead letter. The
        intended refusal (category not in routes, fall through to the next
        rule) never happens, and the email is never classified and therefore
        never labelled. "Labels are the API", so that is a workflow that never
        dispatches.

        Raising LLMError instead puts it where the callers already look:
        school and triage both have `except LLMError` handlers, and in triage
        a bad model response becomes a retried job rather than a crash.

        Found by cortex-to6h, applying an assertion from
        docs/agent-isolation.md item 3 -- a type check has to come BEFORE a
        membership test, because the membership test is what raises.
        """
        value = cls._required(data, field)
        if not isinstance(value, str):
            raise LLMError(
                f"LLM returned {field!r} as {type(value).__name__}, not a string: "
                f"{value!r}. Callers are annotated for str and use this in a "
                "membership test, which raises on an unhashable value."
            )
        return value

    @staticmethod
    def _reject_if_truncated(result: dict[str, Any]) -> None:
        """Raise if the model stopped on a budget limit rather than finishing.

        Checks both shapes: OpenAI's `choices[].finish_reason` and ollama's
        native `done_reason`. A truncated answer is a failed call, not a short
        one -- nothing downstream can tell the difference, and the fields this
        client reads all default rather than fail.
        """
        choices = result.get("choices") or []
        reasons = [c.get("finish_reason") for c in choices if isinstance(c, dict)]
        reasons.append(result.get("done_reason"))
        if "length" in reasons:
            raise LLMTruncatedError(
                "model stopped at the token budget (finish/done_reason='length'); "
                "the response is incomplete. Raise max_tokens, set think=False, "
                "or use a different model -- do NOT retry unchanged."
            )

    def _get_content_from_response(self, result: dict[str, Any]) -> str:
        """Extracts message content from a completion response.

        Args:
            result: The JSON response from the API.

        Returns:
            The content string from the first choice.

        Raises:
            LLMError: If the response format is unexpected or malformed.
        """
        try:
            # Safely extract content from the first choice.
            # An empty choices list will raise an IndexError, which is caught.
            content = result["choices"][0]["message"]["content"]
            if not isinstance(content, str):
                raise TypeError(f"LLM content is not a string, but {type(content).__name__}")
            return content
        except (KeyError, IndexError, TypeError) as e:
            raise LLMError(f"LLM returned unexpected response format: {e}") from e

    def check_intent(self, subject: str, prompt: str, model: str) -> bool:
        """Check if subject matches an intent using LLM.

        Args:
            subject: Email subject (for logging/context).
            prompt: The fully formatted prompt string to send.
            model: Model name to use.

        Returns:
            True if the intent matches, False otherwise.

        Raises:
            LLMError: If the LLM call fails (network, HTTP, or other error).
        """
        try:
            # reject_truncated=False: this returns `answer == "yes"`, so a
            # truncated body can only ever make that False -- the same
            # negative this method already returns when the model declines.
            # 3 live rules reach this at a 10-token cap (measured on the
            # active config 2026-10-07), where a cut-off answer is routine
            # rather than a fault.
            result = self._post_completion(
                model=model,
                prompt=prompt,
                max_tokens=10,
                temperature=0,
                reject_truncated=False,
            )
            content = self._get_content_from_response(result)
            answer: str = content.strip().lower()
            return answer == "yes"
        except httpx.RequestError as e:
            logger.error(f"LLM intent check network error: {e}")
            raise LLMError(f"LLM network error: {e}") from e
        except httpx.HTTPStatusError as e:
            logger.error(f"LLM intent check HTTP error {e.response.status_code}: {e}")
            raise LLMError(f"LLM HTTP {e.response.status_code}") from e
        except LLMError:
            raise  # Re-raise LLMError as-is
        except Exception as e:
            logger.error(f"LLM intent check failed unexpectedly: {e}")
            raise LLMError(f"LLM error: {e}") from e

    def classify(self, prompt: str, model: str) -> tuple[str, float, str]:
        """Classify an email using LLM.

        Args:
            prompt: Fully formatted classification prompt.
            model: Model name to use.

        Returns:
            Tuple of (category, confidence, reasoning).

        Raises:
            LLMError: If the LLM call fails (network, HTTP, or invalid response).
        """
        try:
            result = self._post_completion(
                model=model,
                prompt=prompt,
                max_tokens=200,
                response_format={"type": "json_object"},
            )
            text = self._get_content_from_response(result)
            data = json.loads(text)

            # Validate response is a dict
            if not isinstance(data, dict):
                logger.error(f"LLM returned non-dict JSON: {text}")
                raise LLMError(f"LLM returned non-dict JSON: {text}")

            # Safe float conversion for confidence
            raw_confidence = self._required(data, "confidence")
            try:
                confidence = float(raw_confidence)
            except (ValueError, TypeError) as e:
                raise LLMError(f"LLM returned a non-numeric confidence {raw_confidence!r}") from e

            return (
                self._required_str(data, "category"),
                confidence,
                str(data.get("reasoning") or ""),
            )
        except httpx.RequestError as e:
            logger.error(f"LLM network error: {e}")
            raise LLMError(f"LLM network error: {e}") from e
        except httpx.HTTPStatusError as e:
            logger.error(f"LLM HTTP error {e.response.status_code}: {e}")
            raise LLMError(f"LLM HTTP {e.response.status_code}") from e
        except json.JSONDecodeError as e:
            logger.error(f"LLM returned invalid JSON: {e}")
            raise LLMError(f"LLM invalid JSON: {e}") from e
        except LLMError:
            raise  # Re-raise LLMError as-is
        except Exception as e:
            logger.error(f"LLM classification failed: {e}")
            raise LLMError(f"LLM error: {e}") from e

    def check_email_intent(
        self,
        from_addr: str,
        subject: str,
        body: str | None,
        prompt: str,
        model: str,
    ) -> bool:
        """Check if full email matches an intent using LLM.

        Args:
            from_addr: Email sender address.
            subject: Email subject.
            body: Email body (may be None).
            prompt: Prompt template with {from_addr}, {subject}, {body_preview}.
            model: Model name to use.

        Returns:
            True if the intent matches, False otherwise.

        Raises:
            LLMError: If the LLM call fails (network, HTTP, or other error).
        """
        # Format the prompt with email content
        body_preview = (body or "")[:LLM_BODY_PREVIEW_LENGTH]
        format_args: dict[str, str] = {
            "from_addr": from_addr,
            "subject": subject,
            "body_preview": body_preview,
        }
        # Only compute reply_hierarchy if the prompt uses it
        if "{reply_hierarchy}" in prompt:
            format_args["reply_hierarchy"] = extract_reply_hierarchy(body, from_addr)
        formatted_prompt = prompt.format(**format_args)

        try:
            # Don't use temperature=0 here — some models (qwen3.5) return
            # empty responses with temperature=0 on the chat completions API.
            # check_email_intent is used for Stage 2 verification which may
            # use larger models that have this issue.
            # reject_truncated=False for the same reason as check_intent: the
            # return is `answer == "yes"`, so truncation can only produce the
            # negative this method already produces. 5 live rules.
            result = self._post_completion(
                model=model,
                prompt=formatted_prompt,
                max_tokens=10,
                reject_truncated=False,
            )
            content = self._get_content_from_response(result)
            answer: str = content.strip().lower()
            return answer == "yes"
        except httpx.RequestError as e:
            logger.error(f"LLM email intent check network error: {e}")
            raise LLMError(f"LLM network error: {e}") from e
        except httpx.HTTPStatusError as e:
            logger.error(f"LLM email intent check HTTP error {e.response.status_code}: {e}")
            raise LLMError(f"LLM HTTP {e.response.status_code}") from e
        except LLMError:
            raise  # Re-raise LLMError as-is
        except Exception as e:
            logger.error(f"LLM email intent check failed unexpectedly: {e}")
            raise LLMError(f"LLM error: {e}") from e

    def categorize_email(
        self,
        from_addr: str,
        subject: str,
        body: str | None,
        prompt: str,
        model: str,
        categories: list[str],
    ) -> str | None:
        """Categorize an email into one of the predefined categories.

        Args:
            from_addr: Email sender address.
            subject: Email subject.
            body: Email body (may be None).
            prompt: Prompt template with {from_addr}, {subject},
                {body_preview}, {categories}.
            model: Model name to use.
            categories: List of valid category names.

        Returns:
            The matched category name, or None if no valid category matched.

        Raises:
            LLMError: If the LLM call fails (network, HTTP, or other error).
        """
        # Format the prompt with email content and categories
        body_preview = (body or "")[:LLM_BODY_PREVIEW_LENGTH]
        categories_str = ", ".join(categories)
        formatted_prompt = prompt.format(
            from_addr=from_addr,
            subject=subject,
            body_preview=body_preview,
            categories=categories_str,
        )

        try:
            result = self._post_completion(model=model, prompt=formatted_prompt, max_tokens=50)
            content = self._get_content_from_response(result)
            answer: str = content.strip().lower()

            # Validate the response is one of the allowed categories
            for cat in categories:
                if cat.lower() == answer:
                    return cat  # Return original case
            # Check if answer contains a category (in case LLM adds extra text)
            # Sort by length descending to match longer categories first
            # (e.g., 'presales' before 'sales')
            for cat in sorted(categories, key=len, reverse=True):
                if cat.lower() in answer:
                    return cat
            logger.warning(
                f"LLM returned unknown category '{answer}', expected one of {categories}"
            )
            return None  # Not an error - LLM responded but no category matched
        except httpx.RequestError as e:
            logger.error(f"LLM categorize network error: {e}")
            raise LLMError(f"LLM network error: {e}") from e
        except httpx.HTTPStatusError as e:
            logger.error(f"LLM categorize HTTP error {e.response.status_code}: {e}")
            raise LLMError(f"LLM HTTP {e.response.status_code}") from e
        except LLMError:
            raise  # Re-raise LLMError as-is
        except Exception as e:
            logger.error(f"LLM categorize failed unexpectedly: {e}")
            raise LLMError(f"LLM error: {e}") from e

    def extract_value(
        self,
        from_addr: str,
        subject: str,
        body: str | None,
        prompt: str,
        model: str,
    ) -> str | None:
        """Extract a single value from email using LLM.

        Used for standalone variable extraction (not classification).
        The prompt template can use {from_addr}, {subject}, {body_preview}.

        Args:
            from_addr: Email sender address.
            subject: Email subject.
            body: Email body (may be None).
            prompt: Prompt template with {from_addr}, {subject}, {body_preview}.
            model: Model name to use.

        Returns:
            Extracted value as string, or None if extraction failed or
            returned an obviously invalid response.

        Note:
            This method returns None on failure rather than raising LLMError,
            because extraction failure should cause the rule to fall through
            to the next rule (not fail the entire job).
        """
        body_preview = (body or "")[:LLM_BODY_PREVIEW_LENGTH]
        try:
            formatted = prompt.format(
                from_addr=from_addr,
                subject=subject,
                body_preview=body_preview,
            )
        except KeyError as e:
            logger.warning(f"Invalid prompt template for extraction: missing {e}")
            return None

        try:
            result = self._post_completion(model=model, prompt=formatted, max_tokens=100)
            value: str = self._get_content_from_response(result).strip()

            # Reject obviously invalid responses
            if not value or value.lower() in INVALID_LLM_EXTRACTION_VALUES:
                logger.debug(f"LLM extraction returned invalid or empty value: {value}")
                return None

            return value
        except httpx.RequestError as e:
            logger.warning(f"LLM extraction network error: {e}")
            return None
        except httpx.HTTPStatusError as e:
            logger.warning(f"LLM extraction HTTP error {e.response.status_code}: {e}")
            return None
        except LLMError as e:
            # Helper raised LLMError - log as warning since extraction is optional
            logger.warning(f"LLM extraction failed: {e}")
            return None
        except Exception as e:
            logger.warning(f"LLM extraction failed: {e}")
            return None

    def classify_with_extraction(
        self,
        prompt: str,
        model: str,
        extract_fields: list[str] | None = None,
    ) -> tuple[str, float, str, dict[str, str]]:
        """Classify email and optionally extract fields in one LLM call.

        Used for LLM classification rules that also need to populate variables.
        The prompt should already include extraction instructions if extract_fields
        is provided.

        Args:
            prompt: Fully formatted classification prompt (including extraction
                instructions if needed).
            model: Model name to use.
            extract_fields: List of field names to extract (for validation).
                If None, no extraction is performed.

        Returns:
            Tuple of (category, confidence, reasoning, extracted_dict).
            On failure, returns ("unknown", 0.0, error_message, {}).

        Raises:
            LLMError: If the LLM call fails (network, HTTP, or invalid response).
        """
        try:
            result = self._post_completion(
                model=model,
                prompt=prompt,
                max_tokens=300,
                response_format={"type": "json_object"},
            )
            text = self._get_content_from_response(result)
            data = json.loads(text)

            # Validate response is a dict
            if not isinstance(data, dict):
                logger.error(f"LLM returned non-dict JSON: {text}")
                raise LLMError(f"LLM returned non-dict JSON: {text}")

            # Safe float conversion for confidence
            raw_confidence = self._required(data, "confidence")
            try:
                confidence = float(raw_confidence)
            except (ValueError, TypeError) as e:
                raise LLMError(f"LLM returned a non-numeric confidence {raw_confidence!r}") from e

            # Extract extracted fields if present
            extracted: dict[str, str] = {}
            if extract_fields:
                raw_extracted = data.get("extracted", {})
                if isinstance(raw_extracted, dict):
                    # Only include requested fields with string values
                    for field in extract_fields:
                        if field in raw_extracted:
                            val = raw_extracted[field]
                            if isinstance(val, str) and val.strip():
                                extracted[field] = val.strip()

            return (
                self._required_str(data, "category"),
                confidence,
                str(data.get("reasoning") or ""),
                extracted,
            )
        except httpx.RequestError as e:
            logger.error(f"LLM network error: {e}")
            raise LLMError(f"LLM network error: {e}") from e
        except httpx.HTTPStatusError as e:
            logger.error(f"LLM HTTP error {e.response.status_code}: {e}")
            raise LLMError(f"LLM HTTP {e.response.status_code}") from e
        except json.JSONDecodeError as e:
            logger.error(f"LLM returned invalid JSON: {e}")
            raise LLMError(f"LLM invalid JSON: {e}") from e
        except LLMError:
            raise  # Re-raise LLMError as-is
        except Exception as e:
            logger.error(f"LLM classification with extraction failed: {e}")
            raise LLMError(f"LLM error: {e}") from e
