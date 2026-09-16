"""Run one Ollama chat that may introspect cortex to answer.

The loop is ordinary software: Python sends the prompt with the tool specs,
executes any tool the model asks for against the bound instance, feeds the
result back, and repeats until the model answers or the call budget runs out.
The model never sees a token, never names an instance, and never supplies a
target -- see tools.py.

Ollama runs natively on the GPU host and is reached over the network; this
harness ships in the cortex-utils image like every other cortex process.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from cortex_utils.introspect.client import CortexReadError
from cortex_utils.introspect.tools import CortexTools, ToolRefusalError, tool_specs

OLLAMA_ENV = "OLLAMA_URL"
DEFAULT_MAX_TOOL_CALLS = 8
DEFAULT_TIMEOUT = 180

SYSTEM = (
    "You answer questions about a single email held in a system called cortex. "
    "You may call tools to look up more about THAT message, its sender, and the "
    "labels this cortex uses. The tools are scoped to that one message; you "
    "cannot search the mailbox freely, and you cannot choose which cortex you "
    "are talking to. Call a tool when you need a fact rather than guessing, and "
    "when you have enough, answer plainly and say which facts you used."
)


def _chat(
    ollama_url: str, model: str, messages: list[dict], tools: list[dict], timeout: int
) -> dict:
    payload = {"model": model, "messages": messages, "tools": tools, "stream": False}
    req = urllib.request.Request(
        ollama_url.rstrip("/") + "/api/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"ollama HTTP {e.code}: {e.read()[:200].decode('utf-8', 'replace')}"
        ) from e
    except (urllib.error.URLError, OSError) as e:
        raise RuntimeError(f"cannot reach ollama at {ollama_url}: {e}") from e


def ask(
    tools_impl: CortexTools,
    question: str,
    *,
    ollama_url: str,
    model: str,
    max_tool_calls: int = DEFAULT_MAX_TOOL_CALLS,
    timeout: int = DEFAULT_TIMEOUT,
    verbose: bool = False,
) -> dict[str, Any]:
    """Ask one question, letting the model introspect. Returns the answer and a trace."""
    specs = tool_specs()
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": question},
    ]
    trace: list[dict[str, Any]] = []

    # Budget CALLS, not turns. The previous loop counted assistant messages,
    # so one reply carrying 100 tool_calls executed all 100 authenticated GETs
    # under a limit of 3 -- while the flag is named --max-tool-calls.
    spent = 0

    while True:
        reply = _chat(ollama_url, model, messages, specs, timeout)
        msg = reply.get("message") or {}
        raw_calls = msg.get("tool_calls") or []
        calls = [c for c in raw_calls if isinstance(c, dict)]

        if raw_calls and not calls:
            # It meant to call something; every entry was malformed. Returning
            # here would hand back that message's content, which is empty --
            # the caller gets nothing and no reason. Say so and let it retry.
            messages.append(msg)
            messages.append(
                {
                    "role": "tool",
                    "name": "error",
                    "content": json.dumps(
                        {"refused": "tool_calls were malformed; call a tool or answer plainly"}
                    ),
                }
            )
            continue

        if not calls:
            return {
                "answer": (msg.get("content") or "").strip(),
                "tool_calls": trace,
                "instance": tools_impl._client.instance.name,
                "gmail_id": tools_impl._gmail_id,
            }

        messages.append(msg)
        for call in calls:
            if spent >= max_tool_calls:
                messages.append(
                    {
                        "role": "tool",
                        "name": "budget",
                        "content": json.dumps(
                            {
                                "refused": f"tool-call budget of {max_tool_calls} is "
                                "spent; answer from what you have"
                            }
                        ),
                    }
                )
                break
            spent += 1
            fn = call.get("function") or {}
            name = fn.get("name", "")
            args = fn.get("arguments") or {}
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except ValueError:
                    args = {}
            if not isinstance(args, dict):
                # A JSON string can decode to a list, a number or a bare
                # string. dispatch() expects a mapping, and without this the
                # TypeError escapes ask() entirely.
                args = {}
            try:
                result = tools_impl.dispatch(name, args)
                ok = True
            except ToolRefusalError as e:
                # A refusal is conversational, not fatal: tell the model why and
                # let it choose again from the set it is actually allowed.
                result, ok = {"refused": str(e)}, False
            except CortexReadError as e:
                result, ok = {"error": str(e)}, False

            trace.append({"tool": name, "arguments": args, "ok": ok})
            if verbose:
                print(f"  -> {name}({json.dumps(args)}) {'ok' if ok else 'refused/error'}")
            messages.append(
                {"role": "tool", "name": name, "content": json.dumps(result, default=str)[:4000]}
            )

        if spent >= max_tool_calls:
            # One final turn with the budget spent, so the model answers from
            # what it already has rather than the caller getting nothing.
            final = _chat(ollama_url, model, messages, [], timeout)
            return {
                "answer": ((final.get("message") or {}).get("content") or "").strip(),
                "tool_calls": trace,
                "instance": tools_impl._client.instance.name,
                "gmail_id": tools_impl._gmail_id,
                "budget_exhausted": True,
            }
