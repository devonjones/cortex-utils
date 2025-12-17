"""WSGI middleware for Prometheus HTTP metrics."""

import time
from collections.abc import Callable, Iterable
from typing import Any

from prometheus_client import REGISTRY, Counter, Histogram


def _get_or_create_metric(
    metric_class: type, name: str, description: str, labelnames: list[str]
) -> Any:
    """Get existing metric or create new one (handles uvicorn reload)."""
    try:
        return metric_class(name, description, labelnames)
    except ValueError:
        # Already registered - fetch from registry
        # NOTE: _names_to_collectors is private API, but prometheus_client doesn't provide
        # a public way to retrieve registered metrics by name. This is safe because it's
        # wrapped in error handling and only used to handle development server reloads.
        for collector in REGISTRY._names_to_collectors.values():
            if hasattr(collector, "_name") and collector._name == name:
                return collector
        raise


# HTTP metrics
HTTP_REQUESTS = _get_or_create_metric(
    Counter,
    "http_requests_total",
    "Total HTTP requests",
    ["service", "method", "endpoint", "status"],
)

HTTP_REQUEST_DURATION = _get_or_create_metric(
    Histogram,
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["service", "method", "endpoint"],
)


class MetricsMiddleware:
    """WSGI middleware for recording HTTP request metrics.

    Records Prometheus metrics for HTTP requests:
    - http_requests_total: Counter with method, endpoint, status labels
    - http_request_duration_seconds: Histogram with method, endpoint labels

    Path normalization is applied to prevent metric cardinality explosion:
    - Numeric IDs are replaced with {id}
    - UUIDs are replaced with {id}
    - Gmail message IDs (16-20 char hex) are replaced with {id}

    Usage:
        from flask import Flask
        from cortex_utils.api.middleware import MetricsMiddleware

        app = Flask(__name__)
        app.wsgi_app = MetricsMiddleware(app.wsgi_app, "my-service")

    Note: The /metrics endpoint is skipped to avoid recursion.
    """

    def __init__(self, app: Callable[..., Iterable[bytes]], service_name: str):
        self.app = app
        self.service_name = service_name

    def __call__(
        self,
        environ: dict[str, Any],
        start_response: Callable[..., Any],
    ) -> Iterable[bytes]:
        start_time = time.time()
        method = environ.get("REQUEST_METHOD", "UNKNOWN")
        path = environ.get("PATH_INFO", "/")

        # Skip metrics endpoint to avoid recursion
        if path == "/metrics":
            return self.app(environ, start_response)

        status_code = 200

        def custom_start_response(
            status: str, headers: list[tuple[str, str]], exc_info: Any = None
        ) -> Any:
            nonlocal status_code
            status_code = int(status.split()[0])
            return start_response(status, headers, exc_info)

        try:
            result = self.app(environ, custom_start_response)
            return result
        finally:
            duration = time.time() - start_time

            # Normalize path to avoid cardinality explosion
            # Replace numeric segments with {id} placeholder
            normalized_path = self._normalize_path(path)

            HTTP_REQUESTS.labels(
                service=self.service_name,
                method=method,
                endpoint=normalized_path,
                status=status_code,
            ).inc()

            HTTP_REQUEST_DURATION.labels(
                service=self.service_name,
                method=method,
                endpoint=normalized_path,
            ).observe(duration)

    def _normalize_path(self, path: str) -> str:
        """Normalize path to prevent metric cardinality explosion."""
        parts = path.split("/")
        normalized = []
        for part in parts:
            # Replace numeric IDs and UUIDs with placeholder
            if part.isdigit() or self._is_uuid(part) or self._is_gmail_id(part):
                normalized.append("{id}")
            else:
                normalized.append(part)
        return "/".join(normalized)

    def _is_uuid(self, s: str) -> bool:
        """Check if string looks like a UUID."""
        if len(s) == 36 and s.count("-") == 4:
            return all(c in "0123456789abcdef-" for c in s.lower())
        return False

    def _is_gmail_id(self, s: str) -> bool:
        """Check if string looks like a Gmail message ID (hex string)."""
        if len(s) >= 16 and len(s) <= 20:
            return all(c in "0123456789abcdef" for c in s.lower())
        return False
