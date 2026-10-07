"""A truncated model response must not look like a complete one.

The client read every field of the parsed JSON with a default, so a truncated
response produced a well-formed, plausible result: ("unknown", 0.5, ""). The
shape errors were already loud; these three fields were not.

The consumer is triage's matcher (matcher.py:1292, :1301) -- the only caller of
classify/classify_with_extraction in the estate. It does not threshold the
confidence, it records it, so the damage is a fabricated row rather than a
wrong branch. reflex parses its own JSON and has its own guards.

gemma4:26b is a reasoning model: it emits a `thinking` field before `content`,
so under a tight token budget the whole budget goes on thinking and `content`
comes back as an EMPTY STRING with HTTP 200 and done_reason "length". Measured
in the foodcam project, not re-measured here.
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
        """A retry re-spends the same budget on the same prompt.

        The generator is not the variable -- the budget is -- so the attempt
        counter is the only thing a retry moves. The caller has to change an
        input (raise max_tokens, think=False, another model) instead.

        NOTHING BRANCHES ON THIS FLAG YET. triage's worker catches the base
        LLMError and retries three times regardless (worker.py:1372 ->
        fail_or_retry). Wiring it up is cortex-hox9; until then the flag is
        machine-readable documentation, and the message carries the advice.
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


class TestEveryPublicMethodAgreesAboutTruncation:
    """The guard lives in _post_completion, so it governs six public methods.

    A mutant that moved it out of the shared helper and into classify() alone
    survived the whole suite -- only classify was asserted, so the shared
    POSITION was unpinned even though the behaviour was right.

    Two methods opt OUT, and the rule is which direction a truncated body can
    push the answer. check_intent and check_email_intent return
    `answer == "yes"`, so truncation can only ever yield False -- the same
    negative they already return when the model declines. Everything else
    either parses structure or hands back a value that gets used, where a
    partial body is a WRONG answer rather than a negative one.

    Measured on the active config 2026-10-07: 3 live rules reach check_intent,
    5 check_email_intent, 13 categorize_email, and 0 reach classify or
    classify_with_extraction -- so the two methods this ticket was written
    about have no live caller, and the guard's whole live effect is on the
    other three. That is why the opt-out is not a detail.
    """

    TRUNCATED = {
        "choices": [{"message": {"content": "yes, because the sender"}, "finish_reason": "length"}]
    }

    def _call(self, name):
        c = _client_returning(self.TRUNCATED)
        return {
            "check_intent": lambda: c.check_intent("s", "p", "m"),
            "check_email_intent": lambda: c.check_email_intent("f", "s", "b", "p", "m"),
            "categorize_email": lambda: c.categorize_email(
                "f", "s", "b", "p", "m", ["yes", "sales"]
            ),
            "extract_value": lambda: c.extract_value("f", "s", "b", "p", "m"),
            "classify": lambda: c.classify("p", "m"),
            "classify_with_extraction": lambda: c.classify_with_extraction("p", "m", ["f"]),
        }[name]()

    @pytest.mark.parametrize("method", ["categorize_email", "classify", "classify_with_extraction"])
    def test_a_structured_reader_refuses_a_truncated_body(self, method):
        """categorize_email is the non-obvious one, and it belongs here.

        It falls back to a substring match over the category list, sorted
        longest-first. A body cut from "sales_followup" to "sales" matches the
        WRONG category and returns it as a confident answer -- which is this
        ticket's defect with a different payload, so it keeps the guard.
        """
        with pytest.raises(LLMTruncatedError):
            self._call(method)

    def test_extraction_degrades_to_no_value_rather_than_a_partial_one(self):
        """extract_value converts LLMError to None by documented design.

        It must not return the partial string: the value becomes a rule
        variable, so half an answer is substituted into a live rule.
        """
        assert self._call("extract_value") is None

    @pytest.mark.parametrize("method", ["check_intent", "check_email_intent"])
    def test_a_yes_no_check_tolerates_truncation(self, method):
        """Deliberately NOT raising. Reverting this breaks 8 live rules.

        A 10-token budget on a yes/no question truncates as a matter of
        course; raising would dead-letter the job and leave the mail
        unlabelled, which is worse than the False these already return.
        """
        assert self._call(method) is False

    def test_the_opt_out_is_narrow(self):
        """The yes/no pair tolerates truncation; it does not ignore it.

        A truncated body whose answer IS "yes" still reads as yes -- the
        method is tolerant because its match is robust, not because it stopped
        looking.
        """
        c = _client_returning(
            {"choices": [{"message": {"content": "yes"}, "finish_reason": "length"}]}
        )
        assert c.check_intent("s", "p", "m") is True


class TestTruncationBeatsAnUnreadableShape:
    """A body can be both truncated and unparseable; truncation is the cause.

    The content read used to run first, so a response whose budget went on
    `thinking` and came back with content null -- the exact case in the module
    docstring -- was reported as a retryable shape error.
    """

    @pytest.mark.parametrize(
        "payload,what",
        [
            (
                {"choices": [{"message": {"content": None}, "finish_reason": "length"}]},
                "content null",
            ),
            ({"choices": [], "done_reason": "length"}, "choices cut off entirely"),
        ],
    )
    def test_it_is_reported_as_truncation_not_as_a_bad_shape(self, payload, what):
        c = _client_returning(payload)
        with pytest.raises(LLMTruncatedError):
            c.classify("p", "m")

    def test_an_unreadable_shape_that_is_not_truncated_still_raises_plainly(self):
        """The control: truncation must not become the explanation for everything."""
        c = _client_returning({"choices": [{"message": {"content": None}}]})
        with pytest.raises(LLMError) as e:
            c.classify("p", "m")
        assert not isinstance(e.value, LLMTruncatedError)
