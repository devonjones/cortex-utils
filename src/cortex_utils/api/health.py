"""Health check blueprint for Flask applications."""

from collections.abc import Callable
from typing import Any

from flask import Blueprint, current_app, jsonify

health_bp = Blueprint("health", __name__)


@health_bp.route("/health")
def health_check() -> tuple[Any, int]:
    """Basic health check endpoint."""
    checks = {}
    all_healthy = True

    # Run registered health checks
    health_checks = getattr(current_app, "health_checks", [])
    for check in health_checks:
        try:
            name, healthy = check()
            checks[name] = healthy
            if not healthy:
                all_healthy = False
        except Exception:
            checks[check.__name__] = False
            all_healthy = False

    status = "ok" if all_healthy else "degraded"
    service_name = current_app.name

    return jsonify({"status": status, "service": service_name, "checks": checks}), (
        200 if all_healthy else 503
    )


def register_health_check(app: Any, check: Callable[[], tuple[str, bool]]) -> None:
    """Register a health check function.

    Args:
        app: Flask application
        check: Function returning (name, is_healthy) tuple
    """
    if not hasattr(app, "health_checks"):
        app.health_checks = []
    app.health_checks.append(check)
