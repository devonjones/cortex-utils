"""LLM client for OpenAI-compatible endpoints.

Supports LiteLLM proxy, Ollama with /v1 endpoints, and any OpenAI-compatible API.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from cortex_utils.logging import get_logger

logger = get_logger(__name__)


class LLMError(Exception):
    """Raised when LLM call fails (network, HTTP, or invalid response)."""

    pass


# Max characters of email body to include in LLM prompts
LLM_BODY_PREVIEW_LENGTH = 1000

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
        self.client = httpx.Client(timeout=60.0)

    def _post_completion(
        self,
        model: str,
        prompt: str,
        max_tokens: int,
        response_format: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Helper to post to completions endpoint and return the result.

        Args:
            model: Model name to use.
            prompt: The prompt content to send.
            max_tokens: Maximum tokens for the response.
            response_format: Optional response format (e.g., {"type": "json_object"}).

        Returns:
            The JSON response from the API.

        Raises:
            httpx.RequestError: Network errors.
            httpx.HTTPStatusError: HTTP errors (non-2xx status).
        """
        payload: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
        }
        if response_format:
            payload["response_format"] = response_format

        response = self.client.post(
            f"{self.base_url}/v1/chat/completions",
            json=payload,
        )
        response.raise_for_status()
        result: dict[str, Any] = response.json()
        return result

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
                raise TypeError(
                    f"LLM content is not a string, but {type(content).__name__}"
                )
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
            result = self._post_completion(model=model, prompt=prompt, max_tokens=10)
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
            raw_confidence = data.get("confidence")
            try:
                confidence = (
                    float(raw_confidence) if raw_confidence is not None else 0.5
                )
            except (ValueError, TypeError):
                logger.warning(f"Invalid confidence value from LLM: {raw_confidence}")
                confidence = 0.5

            return (
                data.get("category", "unknown"),
                confidence,
                data.get("reasoning", ""),
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
        formatted_prompt = prompt.format(
            from_addr=from_addr,
            subject=subject,
            body_preview=body_preview,
        )

        try:
            result = self._post_completion(
                model=model, prompt=formatted_prompt, max_tokens=10
            )
            content = self._get_content_from_response(result)
            answer: str = content.strip().lower()
            return answer == "yes"
        except httpx.RequestError as e:
            logger.error(f"LLM email intent check network error: {e}")
            raise LLMError(f"LLM network error: {e}") from e
        except httpx.HTTPStatusError as e:
            logger.error(
                f"LLM email intent check HTTP error {e.response.status_code}: {e}"
            )
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
            result = self._post_completion(
                model=model, prompt=formatted_prompt, max_tokens=50
            )
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
                f"LLM returned unknown category '{answer}', "
                f"expected one of {categories}"
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
            result = self._post_completion(
                model=model, prompt=formatted, max_tokens=100
            )
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
            raw_confidence = data.get("confidence")
            try:
                confidence = (
                    float(raw_confidence) if raw_confidence is not None else 0.5
                )
            except (ValueError, TypeError):
                logger.warning(f"Invalid confidence value from LLM: {raw_confidence}")
                confidence = 0.5

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
                data.get("category", "unknown"),
                confidence,
                data.get("reasoning", ""),
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
