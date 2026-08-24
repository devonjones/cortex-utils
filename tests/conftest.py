"""Keep one test's structlog configuration out of the next one's.

structlog.configure() is global process state, and `structlog.testing
.capture_logs()` -- which several tests here use to assert on a log line --
leaves it CONFIGURED on exit even when it was not configured on entry:

    before capture_logs, is_configured: False
    after  capture_logs, is_configured: True
    resulting logger factory: PrintLoggerFactory

That factory writes to **stdout**. cortex_utils.log defers to a configured
structlog on purpose -- the consumer's choice wins, we are a guest -- so after
any capture_logs() this package's own log output follows it onto stdout.

Which is the bug this package exists to have fixed. A downstream consumer lost a
day to log lines interleaved with protocol output on stdout, and
test_a_per_connection_timezone_override_is_reported asserts the warning lands on
stderr. That test passed alone and failed after test_schema_live.py ran -- so the
suite's verdict depended on file ordering, and the ordering that hid it is the
one CI happened to use.

Reset per test rather than fixed at the one call site: the leak is a property of
capture_logs, so anything that reaches for it inherits the same trap.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
import structlog


@pytest.fixture(autouse=True)
def _isolate_structlog_configuration() -> Iterator[None]:
    was_configured = structlog.is_configured()
    saved: dict[str, Any] | None = dict(structlog.get_config()) if was_configured else None
    try:
        yield
    finally:
        if saved is not None:
            structlog.configure(**saved)
        else:
            structlog.reset_defaults()
