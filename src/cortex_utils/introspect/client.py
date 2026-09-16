"""Read-only HTTP access to one cortex instance.

GET only, by construction: there is no method here that can write. The agent
tool in cortex-ewf2 is scoped to reads deliberately -- the one write that
motivated the ticket (PUT /config, which rewrote production triage rules from
an unauthenticated curl) is exactly the operation that should stay out of
reach of anything a model can influence.

The token lives here and is never returned, logged, or placed anywhere a model
can see it. A credential cannot be target-constrained: it is a key that works
everywhere the service accepts it, so the only safe place for it is the side
of the boundary the model does not reach.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from cortex_utils.introspect.instances import Instance

DEFAULT_TIMEOUT = 20


class CortexReadError(RuntimeError):
    """A read failed. Carries no credential material."""


class CortexClient:
    """Bound to exactly one instance for its whole life."""

    def __init__(self, instance: Instance, timeout: int = DEFAULT_TIMEOUT) -> None:
        self._instance = instance
        self._timeout = timeout

    @property
    def instance(self) -> Instance:
        return self._instance

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET one path on the bound instance.

        `path` is supplied by this package's own tool implementations, never by
        a model. It is still constrained to an absolute path on the bound host
        so that a future bug cannot turn it into a full URL pointing somewhere
        else.
        """
        if not path.startswith("/"):
            raise ValueError(f"path must be absolute, got {path!r}")
        if path.startswith("//"):
            # Protocol-relative: "//host/x" appended to a base is a URL whose
            # HOST is attacker-chosen under any client that resolves it that
            # way, or under a redirect. urllib happens to keep our host; do not
            # rely on that.
            raise ValueError(f"path must not be protocol-relative, got {path!r}")
        if "://" in path or "\\" in path:
            raise ValueError(f"path must not be a URL, got {path!r}")

        url = self._instance.base_url + path
        if params:
            clean = {k: v for k, v in params.items() if v is not None}
            if clean:
                url += "?" + urllib.parse.urlencode(clean)

        req = urllib.request.Request(url, method="GET")
        if self._instance.token:
            req.add_header("Authorization", f"Bearer {self._instance.token}")

        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise CortexReadError(
                    f"{self._instance.name}: 401 unauthorised. "
                    + (
                        "A token is configured, so it is wrong for this instance -- "
                        "check it is not the other deployment's token."
                        if self._instance.has_token
                        else f"No token is configured; set CORTEX_INSTANCE_"
                        f"{self._instance.name.upper()}_TOKEN."
                    )
                ) from e
            raise CortexReadError(f"{self._instance.name}: GET {path} -> HTTP {e.code}") from e
        except (urllib.error.URLError, OSError, ValueError) as e:
            raise CortexReadError(f"{self._instance.name}: GET {path} failed: {e}") from e
