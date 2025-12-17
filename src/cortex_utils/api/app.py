"""Flask application factory for Cortex services."""

import os

from flask import Flask

from cortex_utils.api.health import health_bp
from cortex_utils.api.middleware import MetricsMiddleware
from cortex_utils.metrics import start_metrics_server


def create_app(
    service_name: str,
    *,
    enable_metrics_middleware: bool = True,
    enable_health: bool = True,
    start_metrics: bool = True,
    metrics_port: int | None = None,
) -> Flask:
    """Create a pre-configured Flask application.

    Args:
        service_name: Name of the service (used in metrics labels)
        enable_metrics_middleware: Whether to add HTTP metrics middleware
        enable_health: Whether to register /health endpoint
        start_metrics: Whether to start the Prometheus metrics server
        metrics_port: Port for metrics server (default: METRICS_PORT env or 8001)

    Returns:
        Configured Flask application
    """
    app = Flask(service_name)

    # Apply metrics middleware
    if enable_metrics_middleware:
        app.wsgi_app = MetricsMiddleware(app.wsgi_app, service_name)  # type: ignore[method-assign]

    # Register health blueprint
    if enable_health:
        app.register_blueprint(health_bp)

    # Start metrics server on separate port
    if start_metrics:
        port = metrics_port or int(os.environ.get("METRICS_PORT", "8001"))
        start_metrics_server(port=port)

    return app


def run_app(app: Flask, host: str = "0.0.0.0", port: int = 8080, debug: bool = False) -> None:
    """Run Flask application with development server.

    For production, use gunicorn instead:
        gunicorn -w 4 -b 0.0.0.0:8080 'myservice:app'
        # Where app is created with: app = create_app("myservice")

    Args:
        app: Flask application
        host: Host to bind to
        port: Port to listen on
        debug: Enable debug mode
    """
    app.run(host=host, port=port, debug=debug)
