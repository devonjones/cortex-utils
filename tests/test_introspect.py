"""The model must not be able to choose a target.

docs/agent-isolation.md: "A model may not choose the target of a capability."
These pin the specific ways that could quietly stop being true, because the
consequence is not a crash -- it is one household's mail answered out of
another deployment, or a free-form read over a corpus that includes the kids'
school mail.
"""

from __future__ import annotations

import pytest

from cortex_utils.introspect.client import CortexClient, CortexReadError
from cortex_utils.introspect.instances import Instance, resolve
from cortex_utils.introspect.tools import CortexTools, ToolRefusalError, tool_specs

MESSAGE = {
    "gmail_id": "abc123",
    "from_addr": "sender@example.com",
    "from_name": "A Sender",
    "subject": "hello",
    "date_header": "Mon, 1 Sep 2026 00:00:00 +0000",
    "label_ids": ["INBOX"],
}
DISTRIBUTION = {
    "labels": [{"label": "Cortex/Receipts", "count": 5}, {"label": "Cortex/Github", "count": 9}]
}


class FakeClient:
    """Records every path requested, so tests assert on the TARGET."""

    def __init__(self, instance_name: str = "personal") -> None:
        self.instance = Instance(instance_name, "http://gw", "tok")
        self.paths: list[str] = []

    def get(self, path, params=None):
        self.paths.append(path)
        if path.startswith("/emails/sender/"):
            return {"total": 3, "classifications": [{"label": "Cortex/Receipts", "count": 3}]}
        if path == "/emails/classifications/distribution":
            return DISTRIBUTION
        if path == "/mappings":
            return {"mappings": []}
        if path == "/emails/":
            return {"emails": []}
        return MESSAGE


# --- the model supplies no targets ------------------------------------------


def test_only_one_tool_takes_an_argument_at_all() -> None:
    """Every other tool's target is derived by Python, so it has no parameters."""
    taking_args = {
        s["function"]["name"] for s in tool_specs() if s["function"]["parameters"]["properties"]
    }
    assert taking_args == {"label_sample"}, (
        "a tool grew a parameter: check it cannot be used to point the tool "
        "somewhere the model chose"
    )


def test_sender_is_read_off_the_bound_message_not_supplied() -> None:
    """The model cannot ask about a different sender than the one in scope."""
    c = FakeClient()
    tools = CortexTools(c, "abc123")
    tools.dispatch("sender_history", {"sender": "victim@elsewhere.com", "from_addr": "x@y.z"})
    lookups = [p for p in c.paths if p.startswith("/emails/sender/")]
    assert len(lookups) == 1, lookups
    # Assert the DECODED identity, not the literal path: the address is
    # percent-encoded before it goes on the wire, and pinning the raw form
    # made this pass identically with and without that encoding.
    from urllib.parse import unquote

    looked_up = unquote(lookups[0].split("/")[3])
    assert looked_up == "sender@example.com", (
        f"sender lookup must come from the bound message, not the model; got {looked_up!r}"
    )


def test_arguments_to_no_argument_tools_are_dropped() -> None:
    """Extra keys are a model steering a target it does not own. Ignore them."""
    c = FakeClient()
    tools = CortexTools(c, "abc123")
    tools.dispatch("message_details", {"gmail_id": "someone-elses-id"})
    assert all("someone-elses-id" not in p for p in c.paths), (
        f"a model-supplied gmail_id reached the client: {c.paths}"
    )
    assert "/emails/abc123" in c.paths


def test_label_sample_refuses_a_label_this_instance_does_not_have() -> None:
    c = FakeClient()
    tools = CortexTools(c, "abc123")
    with pytest.raises(ToolRefusalError, match="unknown label"):
        tools.dispatch("label_sample", {"label": "../../etc/passwd"})
    with pytest.raises(ToolRefusalError, match="unknown label"):
        tools.dispatch("label_sample", {"label": "Cortex/Invented"})
    # ...and accepts one it does have
    tools.dispatch("label_sample", {"label": "Cortex/Receipts"})


def test_unknown_tool_is_refused_not_executed() -> None:
    tools = CortexTools(FakeClient(), "abc123")
    with pytest.raises(ToolRefusalError, match="no such tool"):
        tools.dispatch("search_mail", {"query": "password"})


def test_there_is_no_free_form_search_tool() -> None:
    """cortex-ewf2 names search_mail(query) as the sniped shape. Keep it absent."""
    names = {s["function"]["name"] for s in tool_specs()}
    for banned in ("search_mail", "search", "query", "sql", "fetch_url", "read_file"):
        assert banned not in names, f"{banned} is a free-form target; see agent-isolation.md"


def test_tools_require_a_subject_message() -> None:
    """Unscoped tools would be instance-wide reads chosen by the model."""
    for bad in ("", "   "):
        with pytest.raises(ValueError, match="subject gmail_id"):
            CortexTools(FakeClient(), bad)


# --- instance binding --------------------------------------------------------


def test_instance_must_be_named_and_is_never_defaulted(monkeypatch) -> None:
    monkeypatch.delenv("CORTEX_INSTANCE_PERSONAL_URL", raising=False)
    with pytest.raises(ValueError, match="required"):
        resolve("")
    with pytest.raises(ValueError, match="unknown cortex instance"):
        resolve("personal")


def test_resolve_reads_the_token_of_that_instance_only(monkeypatch) -> None:
    monkeypatch.setenv("CORTEX_INSTANCE_PERSONAL_URL", "http://personal:8097")
    monkeypatch.setenv("CORTEX_INSTANCE_PERSONAL_TOKEN", "personal-token")
    monkeypatch.setenv("CORTEX_INSTANCE_WORK_URL", "http://work:8080")
    assert resolve("personal").token == "personal-token"
    # work has no token yet: absent, not inherited from the other deployment
    assert resolve("work").token is None
    assert resolve("work").has_token is False


def test_the_token_is_never_in_the_repr() -> None:
    """Reprs land in logs and tracebacks; a credential must not ride along."""
    inst = Instance("personal", "http://gw", "super-secret-value")
    assert "super-secret" not in repr(inst)
    assert "has_token=True" in repr(inst)


def test_a_401_names_the_instance_and_suggests_the_right_cause() -> None:
    """Using one deployment's token against the other must fail loudly."""
    import urllib.error

    inst = Instance("personal", "http://gw", "wrong-token")
    client = CortexClient(inst)

    def boom(*a, **k):
        raise urllib.error.HTTPError("http://gw/x", 401, "Unauthorized", {}, None)

    client._opener = type("O", (), {"open": staticmethod(boom)})()
    with pytest.raises(CortexReadError, match="other deployment"):
        client.get("/emails/")


def test_the_client_cannot_be_pointed_off_its_instance() -> None:
    client = CortexClient(Instance("personal", "http://gw", None))
    for bad in ("http://evil.example/x", "//evil.example/x", "emails"):
        with pytest.raises(ValueError):
            client.get(bad)


def test_sender_mapping_returns_only_the_bound_senders_rows() -> None:
    """The endpoint has no email filter, so filtering is ours to do.

    The first version passed an `email` param that /mappings silently ignores
    and returned the whole table -- answering "does THIS sender have a
    mapping?" with five unrelated senders. False context, stated confidently,
    is worse for a model than no context.
    """

    class MappingClient(FakeClient):
        def get(self, path, params=None):
            if path == "/mappings":
                return {
                    "mappings": [
                        {"email_address": "someone@else.com", "label": "Other/Thing"},
                        {"email_address": "SENDER@example.com", "label": "Mine/Label"},
                        {"email_address": "third@party.com", "label": "Third/Thing"},
                    ]
                }
            return super().get(path, params)

    tools = CortexTools(MappingClient(), "abc123")
    out = tools.dispatch("sender_mapping", {})
    assert out["has_mapping"] is True
    assert [m["label"] for m in out["mappings"]] == ["Mine/Label"], (
        f"only the bound sender's mapping may be returned, got {out['mappings']}"
    )


def test_sender_mapping_reports_absence_rather_than_someone_elses_row() -> None:
    class NoMappingClient(FakeClient):
        def get(self, path, params=None):
            if path == "/mappings":
                return {"mappings": [{"email_address": "someone@else.com", "label": "Other"}]}
            return super().get(path, params)

    out = CortexTools(NoMappingClient(), "abc123").dispatch("sender_mapping", {})
    assert out["has_mapping"] is False
    assert out["mappings"] == []


# --- the From: header is attacker input, not a name ---------------------------
#
# parseaddr() is a PARSER, not a validator. Unquoted, a hostile From: chooses
# the ENDPOINT, not just a path segment. Verified on the wire before the fix:
#   From: <a@b.com#>             -> the fragment stripped "/classifications";
#                                   the GET became /emails/sender/a@b.com
#   From: <a@b.com?limit=9>      -> remainder became a query string
#   From: <x/../../config?@evil> -> dot segments reached the wire
# Only a route converter in the gateway repo stopped the last resolving to
# GET /config -- the entire triage ruleset, returned to the model.


HOSTILE_SENDERS = [
    "a@b.com#",
    "a@b.com?limit=9",
    "x/../../config?@evil.com",
    "../../../config",
    "a@b.com/../../mappings",
    "a@b.com%2f..%2f..%2fconfig",
    "a b@c.com",
    "a@b.com\n",
]


@pytest.mark.parametrize("hostile", HOSTILE_SENDERS)
def test_a_hostile_from_header_cannot_leave_its_path_segment(hostile: str) -> None:
    """The sender must stay ONE segment: no new path, query or fragment."""

    class HostileSender(FakeClient):
        def get(self, path, params=None):
            if path.startswith("/emails/") and path.count("/") == 2:
                return dict(MESSAGE, from_addr=hostile)
            return super().get(path, params)

    c = HostileSender()
    CortexTools(c, "abc123").dispatch("sender_history", {})

    looked_up = [p for p in c.paths if p.startswith("/emails/sender/")]
    assert len(looked_up) == 1, looked_up
    path = looked_up[0]

    assert path.endswith("/classifications"), (
        f"the endpoint itself was changed by the From: header: {path!r}"
    )

    # The structural property, not a character blocklist: the sender must
    # occupy exactly ONE path segment. ".." inside a segment is inert --
    # traversal needs separators, and quote(safe="") escapes them -- so
    # asserting on ".." would fail the correct encoding. Assert the shape.
    assert path.split("/") == ["", "emails", "sender", path.split("/")[3], "classifications"], (
        f"the sender escaped its path segment: {path!r}"
    )
    middle = path.split("/")[3]
    for ch in ("?", "#", "\n", "\r", " "):
        assert ch not in middle, f"{ch!r} survived unencoded into the path: {path!r}"


def test_the_quoting_is_reversible_so_real_addresses_still_resolve() -> None:
    """Escaping must not break the ordinary case it protects."""
    from urllib.parse import unquote

    c = FakeClient()
    CortexTools(c, "abc123").dispatch("sender_history", {})
    path = next(p for p in c.paths if p.startswith("/emails/sender/"))
    middle = path[len("/emails/sender/") : -len("/classifications")]
    assert unquote(middle) == "sender@example.com"


# --- the Ollama loop: untested until review pointed out every crasher lived here


def _stub_chat(monkeypatch, replies, final="forced answer"):
    """Drive ask() with canned assistant messages."""
    import cortex_utils.introspect.session as sess

    seq = list(replies)
    final_answer = [final]
    seen: list[dict] = []

    def fake_chat(url, model, messages, tools, timeout):
        seen.append({"tools": tools, "messages": list(messages)})
        if not tools:
            # Offered no tools, a real model answers. Modelling that matters:
            # the budget-exhausted turn deliberately passes tools=[].
            return {"message": {"content": final_answer[0]}}
        return {"message": seq.pop(0) if seq else {"content": "done"}}

    monkeypatch.setattr(sess, "_chat", fake_chat)
    return seen


def _tools() -> CortexTools:
    return CortexTools(FakeClient(), "abc123")


def test_a_single_reply_with_many_tool_calls_cannot_exceed_the_budget(monkeypatch) -> None:
    """The budget is CALLS, not turns.

    One assistant message carrying 100 tool_calls previously executed all 100
    authenticated GETs under a limit of 3, while the flag is --max-tool-calls.
    """
    from cortex_utils.introspect.session import ask

    flood = {"tool_calls": [{"function": {"name": "sender_history", "arguments": {}}}] * 100}
    _stub_chat(monkeypatch, [flood, {"content": "answer"}])

    tools = _tools()
    result = ask(tools, "q", ollama_url="http://o", model="m", max_tool_calls=3)
    assert len(tools.calls) <= 3, f"executed {len(tools.calls)} tool calls under a budget of 3"
    assert len(result["tool_calls"]) <= 3


def test_arguments_arriving_as_a_json_string_that_is_not_an_object(monkeypatch) -> None:
    """A JSON string can decode to a list, a number or a bare string."""
    from cortex_utils.introspect.session import ask

    for junk in ("[1,2]", '"x"', "7", "null"):
        _stub_chat(
            monkeypatch,
            [
                {"tool_calls": [{"function": {"name": "message_details", "arguments": junk}}]},
                {"content": "ok"},
            ],
        )
        result = ask(_tools(), "q", ollama_url="http://o", model="m")
        assert result["answer"] == "ok", f"crashed on arguments={junk!r}"


def test_tool_calls_entries_that_are_not_dicts_are_ignored(monkeypatch) -> None:
    from cortex_utils.introspect.session import ask

    _stub_chat(
        monkeypatch,
        [{"tool_calls": ["not-a-dict", None, 7]}, {"content": "ok"}],
    )
    assert ask(_tools(), "q", ollama_url="http://o", model="m")["answer"] == "ok"


def test_an_unknown_tool_is_reported_back_and_the_conversation_continues(monkeypatch) -> None:
    """A refusal is conversational: the model gets told, and may choose again."""
    from cortex_utils.introspect.session import ask

    _stub_chat(
        monkeypatch,
        [
            {"tool_calls": [{"function": {"name": "search_mail", "arguments": {"q": "x"}}}]},
            {"content": "understood"},
        ],
    )
    result = ask(_tools(), "q", ollama_url="http://o", model="m")
    assert result["answer"] == "understood"
    assert result["tool_calls"][0]["ok"] is False


def test_a_model_that_never_stops_calling_tools_still_returns_an_answer(monkeypatch) -> None:
    """Budget exhaustion must produce a reply, not an empty result."""
    from cortex_utils.introspect.session import ask

    forever = [{"tool_calls": [{"function": {"name": "sender_history", "arguments": {}}}]}] * 30
    _stub_chat(monkeypatch, forever + [{"content": "forced answer"}])

    result = ask(_tools(), "q", ollama_url="http://o", model="m", max_tool_calls=2)
    assert result.get("budget_exhausted") is True
    assert result["answer"] == "forced answer"
    assert len(result["tool_calls"]) == 2


def test_the_final_turn_offers_no_tools(monkeypatch) -> None:
    """With the budget spent, re-offering tools invites another call."""
    from cortex_utils.introspect.session import ask

    seen = _stub_chat(
        monkeypatch,
        [
            {"tool_calls": [{"function": {"name": "sender_history", "arguments": {}}}]},
            {"content": "a"},
        ],
    )
    ask(_tools(), "q", ollama_url="http://o", model="m", max_tool_calls=1)
    assert seen[-1]["tools"] == [], "the budget-exhausted turn must not offer tools"


def test_a_gated_instance_pointed_at_an_ungated_gateway_is_refused() -> None:
    """The guard the module docstring promises, now real.

    Personal is gated and work is not. Aiming CORTEX_INSTANCE_PERSONAL_URL at
    the work gateway would read work mail while every log line said
    "personal" -- the exact cross-contamination this module is shaped around.
    """

    client = CortexClient(Instance("personal", "http://actually-work", "a-token"))

    class Resp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    client._opener = type("O", (), {"open": staticmethod(lambda *a, **k: Resp())})()
    with pytest.raises(CortexReadError, match="answers WITHOUT one"):
        client.verify_instance()


def test_an_ungated_instance_is_not_probed() -> None:
    """Work has no token today; that is configuration, not an error."""

    called = []
    c = CortexClient(Instance("work", "http://work", None))
    c._opener = type("O", (), {"open": staticmethod(lambda *a, **k: called.append(1))})()
    c.verify_instance()
    assert called == [], "an instance with no token must not be probed"


def test_a_properly_gated_instance_passes_the_probe() -> None:
    """A 401 to the anonymous probe is the expected, correct answer."""
    import urllib.error
    import urllib.request

    def challenge(*a, **k):
        raise urllib.error.HTTPError("http://gw/config", 401, "Unauthorized", {}, None)

    c = CortexClient(Instance("personal", "http://gw", "tok"))
    c._opener = type("O", (), {"open": staticmethod(challenge)})()
    c.verify_instance()


def test_model_output_is_flattened_before_it_reaches_a_terminal() -> None:
    """A subject carrying ANSI or \\r can overwrite the line just printed."""
    from cortex_utils.introspect.session import flatten_for_terminal

    hostile = "Totally safe\r\x1b[2KAPPROVED: transfer authorised\x00\n\nsecond line"
    out = flatten_for_terminal(hostile)
    assert "\r" not in out and "\n" not in out and "\x00" not in out
    assert "\x1b" not in out, "an ANSI escape reached the terminal"
    assert "Totally safe" in out and "second line" in out
    assert flatten_for_terminal("") == ""
    assert flatten_for_terminal("a" * 9000, 100) == "a" * 100


def test_an_unusable_url_is_a_cortex_read_error_not_a_traceback() -> None:
    """http.client.InvalidURL is neither ValueError nor OSError.

    It is raised inside putrequest -- after the try: is entered, but matched by
    none of its excepts -- so before this it walked out of get(), out of ask()
    (which catches only ToolRefusalError and CortexReadError) and out of the
    CLI handler, printing a raw traceback. Reachable with no effort:
    `From: <a b@c.com>` survives parseaddr intact.
    """
    import http.client

    client = CortexClient(Instance("personal", "http://gw", "tok"))

    def boom(*a, **k):
        raise http.client.InvalidURL("URL can't contain control characters.")

    client._opener = type("O", (), {"open": staticmethod(boom)})()
    with pytest.raises(CortexReadError, match="not a usable URL"):
        client.get("/emails/sender/a%20b@c.com/classifications")


def test_a_model_emitting_only_malformed_tool_calls_terminates(monkeypatch) -> None:
    """The branch that tells the model 'those were malformed' must spend budget.

    It did not, so it was a free turn: a model that keeps emitting malformed
    calls looped forever -- 501 requests under a budget of 2, measured. The
    commit that made the budget count CALLS added this path directly above it
    and left it outside the accounting.
    """
    from cortex_utils.introspect.session import ask

    junk = {"tool_calls": ["not-a-dict", None, 7]}
    calls = {"n": 0}

    import cortex_utils.introspect.session as sess

    def counting_chat(url, model, messages, tools, timeout):
        calls["n"] += 1
        assert calls["n"] < 50, f"loop did not terminate: {calls['n']} ollama calls"
        if not tools:
            return {"message": {"content": "forced"}}
        return {"message": dict(junk)}

    monkeypatch.setattr(sess, "_chat", counting_chat)
    result = ask(_tools(), "q", ollama_url="http://o", model="m", max_tool_calls=2)
    assert result["answer"] == "forced"
    assert result.get("budget_exhausted") is True
    assert calls["n"] <= 4, f"took {calls['n']} ollama calls for a budget of 2"


def test_a_malformed_ollama_reply_does_not_crash(monkeypatch) -> None:
    """`message` missing or not an object, and tool_calls not a list."""
    import cortex_utils.introspect.session as sess
    from cortex_utils.introspect.session import ask

    for bad_reply in (
        {},  # no message at all
        {"message": "a string"},  # message not an object
        {"message": 7},
        {"message": {"tool_calls": 7}},  # tool_calls not a list
        {"message": {"tool_calls": "abc"}},
    ):
        seq = [bad_reply, {"message": {"content": "recovered"}}]

        def chat(url, model, messages, tools, timeout, _s=seq):
            return _s.pop(0) if _s else {"message": {"content": "recovered"}}

        monkeypatch.setattr(sess, "_chat", chat)
        # must not raise
        ask(_tools(), "q", ollama_url="http://o", model="m", max_tool_calls=2)


# --- the redirect handler: round 2 found it had zero tests -------------------


def _redirect_handler():
    from cortex_utils.introspect.client import _NoCrossHostRedirect

    return _NoCrossHostRedirect()


@pytest.mark.parametrize(
    "start,target,allowed",
    [
        ("https://gw:8097/config", "https://gw:8097/config/", True),
        ("https://gw:8097/config", "/config/", True),
        ("https://gw:8097/config", "https://evil.example/x", False),
        ("https://gw:8097/config", "http://gw:8097/config", False),  # TLS downgrade
        ("https://gw:8097/config", "https://gw:9999/config", False),  # other port
        ("http://gw:8097/config", "http://other:8097/config", False),
    ],
)
def test_redirects_leaving_the_origin_are_refused(start, target, allowed) -> None:
    """Scheme AND host: comparing netloc alone let https->http walk the bearer
    token out of TLS on the same host."""
    import urllib.error
    import urllib.request

    h = _redirect_handler()
    req = urllib.request.Request(start)
    req.add_header("Authorization", "Bearer secret")

    class Hdrs(dict):
        def get_all(self, *a, **k):
            return []

    if allowed:
        out = h.redirect_request(req, None, 302, "Found", Hdrs(), target)
        assert out is None or out.full_url
    else:
        with pytest.raises(urllib.error.HTTPError) as ei:
            h.redirect_request(req, None, 302, "Found", Hdrs(), target)
        assert "Authorization header would go with it" in str(ei.value.reason)


def test_the_probe_does_not_fail_open_when_a_redirect_is_refused() -> None:
    """A probe that cannot complete has verified nothing."""
    import urllib.error

    client = CortexClient(Instance("personal", "https://gw", "tok"))

    def refused(*a, **k):
        raise urllib.error.HTTPError(
            "https://evil/x", 302, "refusing to follow a redirect ...", {}, None
        )

    client._opener = type("O", (), {"open": staticmethod(refused)})()
    with pytest.raises(CortexReadError, match="cannot verify gating"):
        client.verify_instance()


def test_flatten_strips_bidi_and_c1_not_just_ansi() -> None:
    """U+202E reverses displayed text; the C1 block is control too.

    The tab exemption this replaces was dead code: str.split() eats tabs
    before translate() sees them, so the docstring's "tabs survive" was false.
    """
    from cortex_utils.introspect.session import flatten_for_terminal

    assert "\u202e" not in flatten_for_terminal("safe\u202ederevnu")
    assert "\u0085" not in flatten_for_terminal("a\u0085b")
    assert "\u2066" not in flatten_for_terminal("a\u2066b")
    assert "\u009b" not in flatten_for_terminal("a\u009bb")
    # non-str input must not raise
    assert flatten_for_terminal(None) == ""
    assert flatten_for_terminal(12345) == "12345"
    assert flatten_for_terminal(["a", "b"])


def test_the_gmail_id_site_is_quoted_too() -> None:
    """The sibling interpolation the first fix missed."""
    c = FakeClient()
    CortexTools(c, "../../config").dispatch("message_details", {})
    first = c.paths[0]
    assert first.split("/") == ["", "emails", first.split("/")[2]], (
        f"gmail_id escaped its segment: {first!r}"
    )


def test_an_unencodable_sender_is_a_tool_error_not_a_crash() -> None:
    """A lone surrogate would raise UnicodeEncodeError out of ask()."""

    class SurrogateClient(FakeClient):
        def get(self, path, params=None):
            if path.startswith("/emails/") and path.count("/") == 2:
                return dict(MESSAGE, from_addr="bad\udcffsender@example.com")
            return super().get(path, params)

    tools = CortexTools(SurrogateClient(), "abc123")
    with pytest.raises(CortexReadError, match="unencodable from_addr"):
        tools.dispatch("sender_history", {})
