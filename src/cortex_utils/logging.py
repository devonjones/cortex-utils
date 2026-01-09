"""Structured logging configuration for Cortex services.

Sets up structlog with JSON output for production, pretty console output for dev.
Integrates with stdlib logging to capture third-party library logs.
"""

import logging
import sys
from typing import cast

import structlog


def configure_logging(service_name: str, level: str = "INFO") -> None:
    """Configure structured logging for a service.

    Integrates structlog with Python's standard logging to ensure all logs
    (including from third-party libraries like boto3, googleapiclient) are
    formatted as structured JSON in production.

    Args:
        service_name: Name of the service (e.g., 'gmail-sync', 'attachment-worker')
        level: Log level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    """
    log_level = getattr(logging, level.upper())
    is_tty = sys.stdout.isatty()

    # Shared processors for all log entries
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    # Configure structlog to integrate with stdlib logging
    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Choose renderer based on environment
    renderer: structlog.types.Processor = (
        structlog.dev.ConsoleRenderer()
        if is_tty
        else structlog.processors.JSONRenderer()
    )

    # Create formatter that processes logs from stdlib logging
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processor=renderer,
    )

    # Configure stdlib logging to use structlog formatter
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    # Add service name to all log entries
    structlog.contextvars.bind_contextvars(service=service_name)


def get_logger(name: str | None = None) -> structlog.BoundLogger:
    """Get a logger instance.

    Args:
        name: Optional logger name (usually __name__ from calling module)

    Returns:
        Configured structlog logger
    """
    return cast(structlog.BoundLogger, structlog.get_logger(name))
