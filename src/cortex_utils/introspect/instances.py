"""Which cortex am I talking to, and with which credential.

One Ollama box serves two cortex deployments -- personal and work -- and they
hold different people's mail. Instance is therefore part of the tool's
identity, resolved once by Python from the invoking context, and never
something a model can name or vary. See docs/agent-isolation.md: a model may
not choose the target of a capability.

Configured entirely by environment, because this repo is public and must not
carry deployment addresses:

    CORTEX_INSTANCE_<NAME>_URL     required -- base URL of that gateway
    CORTEX_INSTANCE_<NAME>_TOKEN   optional -- bearer token, if it needs one

`<NAME>` is upper-cased; the instance is addressed in lower case. The token is
optional on purpose: the work deployment is still on a pre-auth build and
answers without a credential. That is a fact about today, not a permission --
`requires_token` records which instances are expected to carry one so the
mistake of pointing a token-bearing name at an open gateway is visible.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

_URL_TMPL = "CORTEX_INSTANCE_{}_URL"
_TOKEN_TMPL = "CORTEX_INSTANCE_{}_TOKEN"


@dataclass(frozen=True)
class Instance:
    """A single cortex deployment. Frozen: nothing may retarget it in flight."""

    name: str
    base_url: str
    token: str | None

    @property
    def has_token(self) -> bool:
        return bool(self.token)

    def __repr__(self) -> str:  # never print the token
        return (
            f"Instance(name={self.name!r}, base_url={self.base_url!r}, has_token={self.has_token})"
        )


def known_instance_names() -> list[str]:
    """Instance names configured in this environment, from the URL vars."""
    names = []
    for key in os.environ:
        if key.startswith("CORTEX_INSTANCE_") and key.endswith("_URL"):
            names.append(key[len("CORTEX_INSTANCE_") : -len("_URL")].lower())
    return sorted(names)


def resolve(name: str) -> Instance:
    """Resolve one instance by name, or raise with what IS configured.

    Raises rather than falling back to a default. Guessing which mailbox to
    read is the one mistake this module exists to make impossible.
    """
    if not name:
        raise ValueError("instance name is required; nothing is addressed by default")

    key = name.strip().lower()
    url = os.environ.get(_URL_TMPL.format(key.upper()), "").strip()
    if not url:
        known = known_instance_names()
        raise ValueError(
            f"unknown cortex instance {name!r}: {_URL_TMPL.format(key.upper())} is not set. "
            + (f"Configured: {', '.join(known)}." if known else "No instances are configured.")
        )

    token = os.environ.get(_TOKEN_TMPL.format(key.upper()), "").strip() or None
    return Instance(name=key, base_url=url.rstrip("/"), token=token)
