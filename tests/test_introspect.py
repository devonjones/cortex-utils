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
    assert lookups == ["/emails/sender/sender@example.com/classifications"], (
        f"sender lookup must come from the bound message; got {lookups}"
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

    import urllib.request

    orig = urllib.request.urlopen
    urllib.request.urlopen = boom  # type: ignore[assignment]
    try:
        with pytest.raises(CortexReadError, match="other deployment"):
            client.get("/emails/")
    finally:
        urllib.request.urlopen = orig  # type: ignore[assignment]


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
