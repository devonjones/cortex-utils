"""Standard Prometheus metrics for Cortex services.

All metrics use the 'cortex_' prefix for consistency.
"""

from prometheus_client import Counter, Gauge, Histogram, Info

# Service info - set once at startup
SERVICE_INFO = Info(
    "cortex_service",
    "Service metadata",
)

# Queue metrics
QUEUE_PENDING = Gauge(
    "cortex_queue_pending",
    "Number of pending jobs in queue",
    ["queue"],
)

QUEUE_PROCESSED = Counter(
    "cortex_queue_processed_total",
    "Total jobs processed",
    ["queue", "status"],  # status: success, error, skipped
)

DEAD_LETTERS = Counter(
    "cortex_dead_letters_total",
    "Total jobs moved to dead letter queue",
    ["queue", "reason"],
)

# Processing metrics
PROCESSING_DURATION = Histogram(
    "cortex_processing_duration_seconds",
    "Time spent processing jobs",
    ["queue", "operation"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
)

# Error metrics
ERRORS = Counter(
    "cortex_errors_total",
    "Total errors encountered",
    ["service", "error_type"],
)

# LLM metrics (for triage)
LLM_REQUESTS = Counter(
    "cortex_llm_requests_total",
    "Total LLM API requests",
    ["model", "status"],  # status: success, error, timeout
)
