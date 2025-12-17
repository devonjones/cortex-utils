"""Prometheus metrics for Cortex services.

This module provides:
- Standard Cortex metrics (QUEUE_PENDING, QUEUE_PROCESSED, etc.)
- A metrics server that serves all registered metrics

Services can also define their own custom metrics using prometheus_client directly.
All metrics (standard and custom) are automatically served by start_metrics_server().

Usage:
    from cortex_utils.metrics import start_metrics_server, QUEUE_PENDING, QUEUE_PROCESSED

    # Start metrics server (call once at startup)
    start_metrics_server(port=8000)

    # Use standard metrics
    QUEUE_PENDING.labels(queue="triage").set(100)
    QUEUE_PROCESSED.labels(queue="triage", status="success").inc()

    # Define custom service-specific metrics (optional)
    from prometheus_client import Counter
    MY_CUSTOM_METRIC = Counter("myservice_custom_total", "My custom counter", ["label"])
    MY_CUSTOM_METRIC.labels(label="value").inc()  # Automatically included in /metrics
"""

from cortex_utils.metrics.cortex import (
    DEAD_LETTERS,
    EMAILS_CLASSIFIED,
    EMAILS_LABELED,
    EMAILS_PARSED,
    EMAILS_SYNCED,
    ERRORS,
    LLM_REQUESTS,
    PROCESSING_DURATION,
    QUEUE_PENDING,
    QUEUE_PROCESSED,
    SERVICE_INFO,
)
from cortex_utils.metrics.server import start_metrics_server

__all__ = [
    "start_metrics_server",
    "QUEUE_PENDING",
    "QUEUE_PROCESSED",
    "DEAD_LETTERS",
    "PROCESSING_DURATION",
    "ERRORS",
    "LLM_REQUESTS",
    "SERVICE_INFO",
    "EMAILS_SYNCED",
    "EMAILS_PARSED",
    "EMAILS_CLASSIFIED",
    "EMAILS_LABELED",
]
