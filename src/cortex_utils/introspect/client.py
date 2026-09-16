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

import http.client
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from cortex_utils.introspect.instances import Instance

DEFAULT_TIMEOUT = 20


class _NoCrossHostRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse a redirect that would carry the bearer token to another host.

    urlopen follows 3xx by default, and CPython's redirect_request strips only
    content-length and content-type -- Authorization rides along. A gateway
    that can be made to 302 (a proxy misconfiguration, a compromised route)
    would hand this instance's credential to wherever it points. A credential
    cannot be target-constrained, so the target is constrained instead.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Resolve first. CPython's http_error_302 urljoins before calling us,
        # so in practice newurl is absolute -- but a handler that only works
        # when its caller pre-normalises is one refactor from silently
        # comparing "" against a real host and allowing everything.
        newurl = urllib.parse.urljoin(req.full_url, newurl)
        old_parts = urllib.parse.urlsplit(req.full_url)
        new_parts = urllib.parse.urlsplit(newurl)

        # Compare SCHEME as well as host. Comparing netloc alone allowed
        # https -> http on the same host, which walks the bearer token out of
        # TLS onto the wire -- a downgrade is a credential disclosure even
        # though the host never changed.
        old_origin = (old_parts.scheme, old_parts.netloc)
        new_origin = (new_parts.scheme or old_parts.scheme, new_parts.netloc or old_parts.netloc)

        if new_origin != old_origin:
            raise urllib.error.HTTPError(
                newurl,
                code,
                f"refusing to follow a redirect off "
                f"{old_origin[0]}://{old_origin[1]} to "
                f"{new_origin[0]}://{new_origin[1]}: the Authorization header "
                "would go with it",
                headers,
                fp,
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class CortexReadError(RuntimeError):
    """A read failed. Carries no credential material."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never follow a redirect. Used only by the gating probe.

    A redirect is not an answer to "are you gated". A gated gateway that
    answers /config with a same-origin 302 to a /login that returns 200 would
    otherwise look ungated and abort a correct configuration.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class CortexClient:
    """Bound to exactly one instance for its whole life."""

    def __init__(self, instance: Instance, timeout: int = DEFAULT_TIMEOUT) -> None:
        self._instance = instance
        self._timeout = timeout
        self._opener = urllib.request.build_opener(_NoCrossHostRedirect)
        self._probe_opener = urllib.request.build_opener(_NoRedirect)

    @property
    def instance(self) -> Instance:
        return self._instance

    def verify_instance(self, probe_path: str = "/config") -> None:
        """Check the gateway agrees about whether it is gated.

        A token says "this deployment is gated". If an anonymous request to a
        gated path succeeds anyway, this is not the gateway we think it is --
        the name has been pointed at another deployment. Reading one person's
        mail while reporting the other's name is the failure this whole module
        is shaped around, so it is worth one request to rule out.

        Cheap and one-shot. Silent when it cannot tell (network trouble is the
        caller's problem to surface, not this check's to guess at).
        """
        if not self._instance.requires_token:
            return
        # The probe does NOT follow redirects. A gated gateway that answers
        # /config with a same-origin 302 to /login, where /login returns 200,
        # would otherwise look UNGATED and abort a correct configuration. A
        # redirect is not an answer to "are you gated", so treat it as one of
        # the inconclusive cases rather than guessing from where it leads.
        req = urllib.request.Request(self._instance.base_url + probe_path, method="GET")
        try:
            with self._probe_opener.open(req, timeout=self._timeout) as resp:
                code = resp.status
        except urllib.error.HTTPError:
            # 401/403 is the expected, correct answer: the gateway IS gated.
            # Everything else -- 3xx, 404, 5xx -- says nothing either way, so
            # it is not this check's business to judge. One return, because
            # two arms doing the same thing is a trap for whoever later makes
            # one of them log or count and does not notice the other diverge.
            #
            # ponytail: a same-origin 3xx is treated as inconclusive, so an
            # UNGATED gateway that redirects /config to a 200 (trailing-slash
            # normalisation, the common shape) escapes detection. Accepted
            # knowingly: following the redirect instead made a GATED gateway
            # that redirects to a login page look ungated, which aborts a
            # correct configuration -- a false positive an operator cannot
            # work around. Upgrade path if it ever bites: re-probe the
            # same-origin Location once and raise only if it answers 200 with
            # JSON.
            return
        except (urllib.error.URLError, OSError, http.client.InvalidURL, ValueError):
            # ValueError belongs here for the same reason it is in get(): our
            # OWN urlsplit(newurl) raises it on a hostile Location header, and
            # without it that escapes past the CLI as a traceback. A probe is
            # the one call that must never be louder than the read it guards.
            return
        if 200 <= code < 300:
            raise CortexReadError(
                f"{self._instance.name}: a token is configured, but "
                f"{self._instance.base_url}{probe_path} answers WITHOUT one. "
                "That gateway is not gated, so this name is probably pointed at "
                "the wrong deployment -- check CORTEX_INSTANCE_"
                f"{self._instance.name.upper()}_URL."
            )

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

        try:
            # Construction inside the try. urlencode and Request both raise on
            # input this module does not fully control (a lone surrogate in a
            # label or an address), and building outside meant each new site
            # needed its own guard -- which is why there is already a surrogate
            # check in _sender(). One boundary is better than N patches.
            url = self._instance.base_url + path
            if params:
                clean = {k: v for k, v in params.items() if v is not None}
                if clean:
                    url += "?" + urllib.parse.urlencode(clean)

            req = urllib.request.Request(url, method="GET")
            if self._instance.token:
                req.add_header("Authorization", f"Bearer {self._instance.token}")
            with self._opener.open(req, timeout=self._timeout) as resp:
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
        except http.client.InvalidURL as e:
            # NOT a ValueError or OSError, so without this it escapes the
            # client, escapes ask(), escapes the CLI handler and prints a
            # traceback. Any From: header with a space in it triggers it --
            # which is to say, any spammer.
            raise CortexReadError(
                f"{self._instance.name}: GET {path} is not a usable URL: {e}"
            ) from e
        except (urllib.error.URLError, OSError, ValueError) as e:
            raise CortexReadError(f"{self._instance.name}: GET {path} failed: {e}") from e
