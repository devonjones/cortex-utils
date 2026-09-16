"""The tools a model may call, and the reason each one is safe to offer.

docs/agent-isolation.md states the rule this file exists to satisfy:

    A model may not choose the target of a capability.

So the toolset is bound at construction to ONE instance and ONE subject
message, and the model supplies no targets:

  * message_details()      -- the bound message. No arguments.
  * sender_history()       -- Python reads the sender OFF the bound message
                              and asks about that sender. No arguments.
  * sender_mapping()       -- likewise, the mapping for that same sender.
  * label_distribution()   -- instance-wide totals. No arguments.
  * label_sample(label)    -- the one tool taking an argument, validated
                              against the closed set of labels this instance
                              actually has. The model chooses FROM OUR SET and
                              cannot widen it.

What is deliberately absent is `search_mail(query)`. cortex-ewf2 names it as
the sniped shape: a free-form query over a corpus that includes the household's
mail, chosen by a model whose input is attacker-controlled text. Introspection
here means "tell me more about the message already in front of you", never
"fetch whatever you like".
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import quote

from cortex_utils.introspect.client import CortexClient, CortexReadError

MAX_ROWS = 25


class ToolRefusalError(RuntimeError):
    """The model asked for something outside the allowed set. Not an error."""


class CortexTools:
    """Tool implementations bound to one instance and one message."""

    def __init__(self, client: CortexClient, gmail_id: str) -> None:
        if not gmail_id or not gmail_id.strip():
            raise ValueError("a subject gmail_id is required: tools are scoped to one message")
        self._client = client
        self._gmail_id = gmail_id.strip()
        self._message: dict[str, Any] | None = None
        self._labels: list[str] | None = None
        self.calls: list[dict[str, Any]] = []

    # --- helpers Python owns -------------------------------------------------

    def _msg(self) -> dict[str, Any]:
        if self._message is None:
            self._message = self._client.get(f"/emails/{self._gmail_id}")
        return self._message

    def _sender(self) -> str:
        sender = (self._msg() or {}).get("from_addr") or ""
        if not sender:
            raise CortexReadError(f"message {self._gmail_id} has no from_addr to look up")
        return sender

    def _known_labels(self) -> list[str]:
        if self._labels is None:
            dist = self._client.get("/emails/classifications/distribution") or {}
            self._labels = [row.get("label") for row in dist.get("labels", []) if row.get("label")]
        return self._labels

    # --- tools ---------------------------------------------------------------

    def message_details(self) -> dict[str, Any]:
        m = self._msg() or {}
        return {
            "gmail_id": m.get("gmail_id"),
            "from": m.get("from_addr"),
            "from_name": m.get("from_name"),
            "subject": m.get("subject"),
            "date": m.get("date_header"),
            "labels": m.get("label_ids"),
            "classification": m.get("classification"),
        }

    def sender_history(self) -> dict[str, Any]:
        sender = self._sender()
        # quote(safe="") -- the sender comes from the inbound From: header,
        # which is attacker-controlled text, and parseaddr() is a PARSER, not a
        # validator. Unquoted it chooses the endpoint, not just the path
        # segment. Verified on the wire before this fix:
        #   From: <a@b.com#>              -> fragment stripped "/classifications"
        #                                    off the selector; the GET became
        #                                    /emails/sender/a@b.com
        #   From: <a@b.com?limit=9>       -> remainder became a query string
        #   From: <x/../../config?@evil>  -> dot segments reached the wire
        # Only a route converter in ANOTHER repo stopped the last one resolving
        # to GET /config -- the whole triage ruleset, handed back to the model
        # as a tool result. agent-isolation.md calls escaping weak and
        # inexpressibility structural; quoting makes traversal inexpressible
        # here rather than merely unlikely downstream.
        data = self._client.get(f"/emails/sender/{quote(sender, safe='')}/classifications") or {}
        return {
            "sender": sender,
            "total_messages": data.get("total"),
            "labels": data.get("classifications"),
        }

    def sender_mapping(self) -> dict[str, Any]:
        """The mapping for THIS sender, or none.

        /mappings has no email filter -- only type/limit/offset -- so an
        `email` parameter is silently ignored and the endpoint returns the
        whole table. Filter here, exactly and case-insensitively.

        The first version passed `email` and returned whatever came back,
        which handed the model five unrelated senders' mappings while
        answering a question about one. Not a privilege leak (same instance,
        same operator) but false context, which for a model is worse than no
        context: it answered confidently about addresses that had nothing to
        do with the message.
        """
        sender = self._sender().strip().lower()
        data = self._client.get("/mappings", {"limit": 1000}) or {}
        rows = data.get("mappings") if isinstance(data, dict) else data
        mine = [
            r for r in (rows or []) if str(r.get("email_address", "")).strip().lower() == sender
        ]
        return {"sender": sender, "mappings": mine, "has_mapping": bool(mine)}

    def label_distribution(self) -> dict[str, Any]:
        data = self._client.get("/emails/classifications/distribution") or {}
        return {"labels": (data.get("labels") or [])[:MAX_ROWS]}

    def label_sample(self, label: str) -> dict[str, Any]:
        """Recent mail under ONE label, validated against this instance's set."""
        known = self._known_labels()
        if label not in known:
            raise ToolRefusalError(
                f"unknown label {label!r}. Choose one of the labels this cortex "
                f"actually has, e.g. {', '.join(known[:5])}"
            )
        data = self._client.get("/emails/", {"label": label, "limit": 10}) or {}
        return {
            "label": label,
            "emails": [
                {
                    "subject": e.get("subject"),
                    "from": e.get("from_addr"),
                    "date": e.get("date_header"),
                }
                for e in (data.get("emails") or [])[:10]
            ],
        }

    # --- dispatch ------------------------------------------------------------

    def dispatch(self, name: str, arguments: dict[str, Any] | None) -> Any:
        impl: dict[str, Callable[..., Any]] = {
            "message_details": self.message_details,
            "sender_history": self.sender_history,
            "sender_mapping": self.sender_mapping,
            "label_distribution": self.label_distribution,
            "label_sample": self.label_sample,
        }
        if name not in impl:
            raise ToolRefusalError(f"no such tool {name!r}; available: {', '.join(sorted(impl))}")

        args = dict(arguments or {})
        # Record BEFORE executing. Appending after the call recorded only
        # successes, and the budget test in session reads this as a count of
        # attempts -- a refusal or an error still spent a request.
        self.calls.append({"tool": name, "arguments": args})
        if name == "label_sample":
            label = args.get("label")
            if not isinstance(label, str):
                raise ToolRefusalError("label_sample requires a 'label' string")
            result = impl[name](label)
        else:
            # Every other tool takes NO arguments. Ignore anything the model
            # attached rather than passing it on: extra keys are the model
            # trying to steer a target it does not own.
            result = impl[name]()

        return result


def tool_specs() -> list[dict[str, Any]]:
    """Ollama/OpenAI-style function specs for the tools above."""

    def fn(name: str, desc: str, props: dict | None = None, required: list | None = None):
        return {
            "type": "function",
            "function": {
                "name": name,
                "description": desc,
                "parameters": {
                    "type": "object",
                    "properties": props or {},
                    "required": required or [],
                },
            },
        }

    return [
        fn(
            "message_details",
            "Details of the message under review: sender, subject, date, "
            "current labels. Takes no arguments.",
        ),
        fn(
            "sender_history",
            "How much mail this message's sender has sent before, and how it "
            "was labelled. Takes no arguments.",
        ),
        fn(
            "sender_mapping",
            "Whether this message's sender has a priority or fallback mapping "
            "configured. Takes no arguments.",
        ),
        fn(
            "label_distribution",
            "Every label in this cortex and how many messages carry it. Takes "
            "no arguments. Use it to discover valid labels.",
        ),
        fn(
            "label_sample",
            "Recent messages under one existing label. Use label_distribution "
            "first to see valid labels.",
            {
                "label": {
                    "type": "string",
                    "description": "An existing label, exactly as label_distribution reports it.",
                }
            },
            ["label"],
        ),
    ]
