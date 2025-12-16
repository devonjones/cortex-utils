"""Metrics server for exposing Prometheus endpoints."""

import logging
import threading
from collections.abc import Callable
from typing import Any
from wsgiref.simple_server import WSGIRequestHandler, make_server

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

logger = logging.getLogger(__name__)

# Module-level state for idempotent server startup
_server_lock = threading.Lock()
_server_thread: threading.Thread | None = None


class _QuietHandler(WSGIRequestHandler):
    """WSGI handler that doesn't log every request."""

    def log_message(self, format: str, *args: object) -> None:
        pass  # Suppress access logs


StartResponse = Callable[[str, list[tuple[str, str]]], Any]


def _metrics_app(environ: dict[str, Any], start_response: StartResponse) -> list[bytes]:
    """WSGI app that serves Prometheus metrics."""
    path = environ.get("PATH_INFO", "/")

    if path == "/metrics":
        output = generate_latest(REGISTRY)
        status = "200 OK"
        headers = [("Content-Type", CONTENT_TYPE_LATEST)]
    elif path == "/health":
        output = b"ok"
        status = "200 OK"
        headers = [("Content-Type", "text/plain")]
    else:
        output = b"Not Found"
        status = "404 Not Found"
        headers = [("Content-Type", "text/plain")]

    start_response(status, headers)
    return [output]


def start_metrics_server(port: int = 8000, host: str = "0.0.0.0") -> threading.Thread:
    """Start a background thread serving Prometheus metrics.

    This function is idempotent. If called multiple times, it returns
    the existing running thread.

    Args:
        port: Port to listen on (default 8000)
        host: Host to bind to (default 0.0.0.0)

    Returns:
        The daemon thread running the server
    """
    global _server_thread
    with _server_lock:
        if _server_thread is not None and _server_thread.is_alive():
            logger.debug("Metrics server already running")
            return _server_thread

        server = make_server(host, port, _metrics_app, handler_class=_QuietHandler)

        def serve_forever() -> None:
            try:
                logger.info(f"Metrics server listening on {host}:{port}")
                server.serve_forever()
            except Exception:
                logger.exception("Metrics server failed unexpectedly")

        thread = threading.Thread(target=serve_forever, daemon=True)
        thread.start()
        _server_thread = thread
        return thread
