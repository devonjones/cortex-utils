"""Loggers for this package, defaulting to stderr.

structlog's own default writes to stdout. That is fine for an application and
wrong for a library: a consumer whose stdout is a protocol gets our log lines
counted as data, and the corruption reads as data rather than as an error.

Reported by a downstream consumer who hit it exactly: their `claim-drain`
stdout is TSV, one claimed row per line, and `Ensured dead_letter table exists`
was parsed as a claimed row. Reproduced here before fixing -- a consumer that
imports cortex_utils and never configures structlog still gets our logs on
stdout.

We do NOT call structlog.configure(). That is global state, and a library
deciding how its consumer logs is the same class of mistake one layer up. So:

  - consumer has configured structlog -> use their configuration, unchanged
  - consumer has not -> our own loggers go to stderr

The choice is made at log time rather than import time, because every module
here binds its logger at import and the consumer configures later.
"""

from __future__ import annotations

import sys
from typing import Any

import structlog


class _LazyStderr:
    """sys.stderr resolved per write, not captured when the logger is built.

    A harness that replaces sys.stderr -- pytest, a CLI runner, anything
    capturing output -- would otherwise leave this holding a closed file, and
    every later log raises ValueError. That is not hypothetical: binding the
    stream directly broke 121 tests the first time.
    """

    def write(self, message: str) -> int:
        return sys.stderr.write(message)

    def flush(self) -> None:
        sys.stderr.flush()

    def isatty(self) -> bool:
        # ConsoleRenderer asks, to decide on colour.
        return sys.stderr.isatty()


_stderr_factory = structlog.PrintLoggerFactory(file=_LazyStderr())


class _StderrDefaultLogger:
    """Defers the choice of logger to the moment something is logged.

    Modules bind `log = get_logger()` at import, which is before a consumer has
    had the chance to configure structlog. Deciding then would lock in the
    wrong answer for every consumer that configures after importing us -- which
    is all of them.
    """

    def __init__(self, name: str | None = None) -> None:
        self._name = name

    def _resolve(self) -> Any:
        if structlog.is_configured():
            # Their configuration wins. We are a guest here.
            return structlog.get_logger(self._name)
        return structlog.wrap_logger(
            _stderr_factory(),
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.dev.ConsoleRenderer(colors=False),
            ],
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._resolve(), name)


def get_logger(name: str | None = None) -> Any:
    """A logger for this package. Falls back to stderr, never to stdout."""
    return _StderrDefaultLogger(name)
