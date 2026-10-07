"""A truncated model response must not look like a complete one.

The client read every field with a default, so a truncated or failed response
produced a well-formed, plausible result: ("unknown", 0.5, ""). Nothing
downstream could tell it from a real answer, and reflex compares that 0.5
against a threshold to decide whether to escalate to a larger model.

Measured on the box this estate runs: gemma4:26b is a reasoning model. It emits
a `thinking` field before `content`, so under a tight token budget the whole
budget goes on thinking and `content` comes back as an EMPTY STRING with
HTTP 200 and done_reason "length".
"""

from __future__ import annotations

import json

import pytest

from cortex_utils.llm.client import LLMClient, LLMError, LLMTruncatedError


class TestTruncationIsNotCompletion:
    """Both API shapes report it, and neither used to be read."""

    @pytest.mark.parametrize(
        "payload,shape",
        [
            ({"choices": [{"message": {"content": ""}, "finish_reason": "length"}]}, "openai"),
            ({"done_reason": "length", "response": ""}, "ollama native"),
            (
                {"choices": [{"message": {"content": "half an ans"}, "finish_reason": "length"}]},
                "openai, partial content",
            ),
        ],
    )
    def test_a_budget_stop_raises(self, payload, shape):
        with pytest.raises(LLMTruncatedError):
            LLMClient._reject_if_truncated(payload)

    @pytest.mark.parametrize(
        "payload",
        [
            {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]},
            {"done_reason": "stop", "response": "ok"},
            {"choices": [{"message": {"content": "ok"}}]},
            {},
        ],
    )
    def test_a_normal_stop_passes_through(self, payload):
        LLMClient._reject_if_truncated(payload)

    def test_it_is_not_retryable_and_says_so(self):
        """At temperature 0 a retry reproduces the identical truncation.

        A guard that refuses correctly will refuse for ever, so the caller must
        change an input rather than repeat the call. The flag is how a caller
        can tell without parsing the message.
        """
        assert LLMTruncatedError.retryable is False
        assert LLMError.retryable is True, "the base class stays retryable"
        assert issubclass(LLMTruncatedError, LLMError), "existing handlers still catch it"


class TestAMissingFieldIsNotAnAnswer:
    """Presence, not value. An explicit 'unknown' is a judgement; absence is not."""

    def test_a_missing_confidence_raises_rather_than_becoming_point_five(self):
        with pytest.raises(LLMError, match="confidence"):
            LLMClient._required({"category": "admin"}, "confidence")

    def test_a_missing_category_raises_rather_than_becoming_unknown(self):
        with pytest.raises(LLMError, match="category"):
            LLMClient._required({"confidence": 0.9}, "category")

    @pytest.mark.parametrize("value", ["unknown", "", 0, 0.0, False, None])
    def test_an_explicit_value_is_honoured_including_falsey_ones(self, value):
        """The check is present-vs-absent. A model may legitimately say 'unknown'."""
        assert LLMClient._required({"category": value}, "category") == value


def _client_returning(body, native_body=None):
    """A real LLMClient whose HTTP layer returns `body`.

    Drives the actual public method rather than the helper, because a correct
    helper is worthless if the call site stops calling it -- three mutants that
    reverted the call sites survived a suite that tested only the helpers.
    """
    from unittest import mock

    import httpx

    from cortex_utils.llm.client import LLMClient

    c = LLMClient("http://llm.test")
    calls: list[dict] = []

    def post(url, json=None, **kw):
        calls.append({"url": url, "json": json})
        payload = native_body if url.endswith("/api/generate") and native_body else body
        return httpx.Response(200, json=payload, request=httpx.Request("POST", url))

    c.client = mock.Mock()
    c.client.post.side_effect = post
    c.posted = calls  # type: ignore[attr-defined]
    return c


def _wrap(content):
    return {"choices": [{"message": {"content": content}, "finish_reason": "stop"}]}


class TestTheCallSitesActuallyRefuse:
    """classify() must not invent a field. Its sibling is covered below."""

    @pytest.mark.parametrize("missing", ["confidence", "category"])
    def test_classify_refuses_a_response_missing_a_required_field(self, missing):
        full = {"category": "admin", "confidence": 0.9, "reasoning": "because"}
        del full[missing]
        c = _client_returning(_wrap(json.dumps(full)))
        # match on _required's OWN message. `match=missing` was satisfied by a
        # bare KeyError re-raised as LLMError("LLM error: 'confidence'"), so it
        # could not tell the guard from no guard at all.
        with pytest.raises(LLMError, match=f"has no {missing!r}"):
            c.classify("prompt", "some-model")

    def test_classify_accepts_a_complete_response(self):
        """The control: refusing everything would also pass the tests above."""
        c = _client_returning(
            _wrap(json.dumps({"category": "admin", "confidence": 0.9, "reasoning": "r"}))
        )
        assert c.classify("prompt", "some-model") == ("admin", 0.9, "r")

    def test_classify_refuses_a_non_numeric_confidence(self):
        c = _client_returning(
            _wrap(json.dumps({"category": "admin", "confidence": "high", "reasoning": "r"}))
        )
        with pytest.raises(LLMError, match="confidence"):
            c.classify("prompt", "some-model")

    def test_a_truncated_response_raises_from_the_public_method(self):
        """Partial content, not empty.

        Empty content now triggers the native think=False recovery FIRST -- see
        the ordering test below. An earlier version used empty content and
        passed only because the guard ran before the recovery, which made the
        recovery unreachable for the one case it was written for.
        """
        c = _client_returning(
            {"choices": [{"message": {"content": '{"category": "adm'}, "finish_reason": "length"}]}
        )
        with pytest.raises(LLMTruncatedError):
            c.classify("prompt", "some-model")

    def test_an_empty_truncated_response_still_reaches_the_recovery(self):
        """The ordering itself, pinned.

        A reasoning model spends its budget on `thinking` and returns EMPTY
        content with done_reason "length" -- both the fallback's trigger and the
        guard's. Judging before recovering cancelled the fix.
        """
        c = _client_returning(
            {"choices": [{"message": {"content": ""}, "finish_reason": "length"}]},
            native_body={
                "response": json.dumps({"category": "admin", "confidence": 0.9, "reasoning": "r"}),
                "done_reason": "stop",
            },
        )
        assert c.classify("prompt", "some-model") == ("admin", 0.9, "r")
        assert any(p["url"].endswith("/api/generate") for p in c.posted), (
            "the think=False recovery must run for an empty truncated response"
        )


class TestTheNativeFallbackDisablesThinking:
    """A reasoning model must not silently spend the budget on thinking."""

    def test_the_fallback_sends_think_false(self):
        # empty content on the chat API triggers the native fallback
        c = _client_returning(
            _wrap(""),
            native_body={
                "response": json.dumps({"category": "admin", "confidence": 0.5, "reasoning": "r"}),
                "done_reason": "stop",
            },
        )
        c.classify("prompt", "some-model")
        native = [p for p in c.posted if p["url"].endswith("/api/generate")]
        assert native, "the native fallback was never reached"
        assert native[0]["json"].get("think") is False, (
            "think must be explicitly disabled, or a reasoning model returns empty content"
        )


def test_the_truncation_error_is_importable_by_name():
    """A caller cannot catch what it cannot import.

    LLMTruncatedError existed in client.py but `__init__.py` never re-exported
    it, so `from cortex_utils.llm import LLMTruncatedError` raised ImportError.
    The guard existed and could not be reached.

    `__all__` governs `import *` only and does NOT affect an explicit-name
    import -- an earlier version of this docstring said it did, which would
    teach the next reader a false rule about Python. The re-export is what
    binds the name; this asserts both.
    """
    import cortex_utils.llm as pkg
    from cortex_utils.llm import LLMTruncatedError as Imported

    assert "LLMTruncatedError" in pkg.__all__
    assert Imported is LLMTruncatedError


class TestTheOtherCallSiteRefusesToo:
    """classify_with_extraction() is the LIVE path, not the fallback.

    triage's matcher calls it whenever a rule sets `llm.extract` and only falls
    back to classify() otherwise -- so the method the first version of this file
    tested was the fallback, and the one it left uncovered was the feature.
    Reverting its two _required calls shipped ('unknown', 0.5, 'r', {}) green:
    the exact tuple this ticket exists to eliminate.
    """

    @pytest.mark.parametrize("missing", ["confidence", "category"])
    def test_it_refuses_a_response_missing_a_required_field(self, missing):
        full = {"category": "admin", "confidence": 0.9, "reasoning": "r"}
        del full[missing]
        c = _client_returning(_wrap(json.dumps(full)))
        with pytest.raises(LLMError, match=f"has no {missing!r}"):
            c.classify_with_extraction("prompt", "some-model", ["amount"])

    def test_it_accepts_a_complete_response_and_strips_extracted(self):
        """The control, which also covers the extracted filter loop."""
        c = _client_returning(
            _wrap(
                json.dumps(
                    {
                        "category": "admin",
                        "confidence": 0.9,
                        "reasoning": "r",
                        "extracted": {"amount": " 12 ", "unwanted": "x"},
                    }
                )
            )
        )
        assert c.classify_with_extraction("prompt", "some-model", ["amount"]) == (
            "admin",
            0.9,
            "r",
            {"amount": "12"},
        )


def test_a_truncated_native_fallback_raises():
    """The PR's headline fix, which had no test.

    The native wrap used to drop `done_reason`, so a truncated native response
    arrived looking complete. Deleting the carry-through, or the reject that
    follows it, was green: classify() returned ('admin', 0.9, '') instead of
    raising. The only other fallback test sets done_reason "stop", so it never
    touches this.
    """
    c = _client_returning(
        _wrap(""),  # empty chat content triggers the native retry
        native_body={
            "response": json.dumps({"category": "admin", "confidence": 0.9, "reasoning": ""}),
            "done_reason": "length",
        },
    )
    with pytest.raises(LLMTruncatedError):
        c.classify("prompt", "some-model")
