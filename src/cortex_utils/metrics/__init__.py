"""Prometheus metrics for Cortex services.

Usage:
    from cortex_utils.metrics import start_metrics_server, QUEUE_PENDING, QUEUE_PROCESSED

    # Start metrics server (call once at startup)
    start_metrics_server(port=8000)

    # Update metrics
    QUEUE_PENDING.labels(queue="triage").set(100)
    QUEUE_PROCESSED.labels(queue="triage", status="success").inc()
"""

from cortex_utils.metrics.cortex import (
    DEAD_LETTERS,
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
]
