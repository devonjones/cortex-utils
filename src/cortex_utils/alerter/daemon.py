"""Alerter daemon that tails Docker logs and sends alerts to Discord."""

import os
import threading
import time
from datetime import datetime

import docker
import schedule
import structlog

from .classifier import Severity, classify, is_error_line
from .discord import (
    COLOR_CRITICAL,
    COLOR_HIGH,
    COLOR_INFO,
    COLOR_WARNING,
    DiscordClient,
)
from .rate_limiter import RateLimiter

log = structlog.get_logger()

# Default containers to monitor
DEFAULT_CONTAINERS = [
    "cortex-gmail-sync",
    "cortex-duckdb-api",
    "cortex-parse-worker",
    "cortex-attachment-worker",
    "cortex-triage-worker",
    "cortex-labeling-worker",
]


class AlerterDaemon:
    """Main alerter daemon that monitors Docker containers."""

    def __init__(
        self,
        webhook_url: str,
        containers: list[str] | None = None,
        ping_critical: bool = True,
        summary_hour: int = 0,
    ):
        """Initialize the alerter daemon.

        Args:
            webhook_url: Discord webhook URL
            containers: List of container names to monitor (default: cortex-* containers)
            ping_critical: Whether to @here on critical alerts
            summary_hour: Hour (0-23) to send daily summary
        """
        self.discord = DiscordClient(webhook_url)
        self.rate_limiter = RateLimiter()
        self.containers = containers or DEFAULT_CONTAINERS
        self.ping_critical = ping_critical
        self.summary_hour = summary_hour

        # Thread-safe lock for rate limiter access
        self._lock = threading.Lock()

        # Docker client - connect via socket
        self.docker_client: docker.DockerClient | None = None

        # Track running threads
        self._threads: list[threading.Thread] = []
        self._stop_event = threading.Event()

    def _connect_docker(self) -> bool:
        """Connect to Docker daemon."""
        try:
            self.docker_client = docker.from_env()
            self.docker_client.ping()
            log.info("Connected to Docker daemon")
            return True
        except docker.errors.DockerException as e:
            log.error("Failed to connect to Docker", error=str(e))
            return False

    def _process_log_line(self, container: str, log_line: str) -> None:
        """Process a single log line and send alerts if needed."""
        # Quick filter - only classify lines that look like errors
        if not is_error_line(log_line):
            return

        # Classify the error
        classification = classify(container, log_line)
        if classification is None:
            return

        with self._lock:
            if classification.severity == Severity.CRITICAL:
                # Critical: always check rate limiter (some have 0 cooldown = always alert)
                if self.rate_limiter.should_alert(
                    classification.error_key, classification.cooldown_minutes
                ):
                    log.warning(
                        "Critical error detected",
                        container=container,
                        title=classification.title,
                    )
                    self.discord.send_embed(
                        title=f"CRITICAL: {classification.title}",
                        description=classification.description,
                        color=COLOR_CRITICAL,
                        fields=[
                            {"name": "Container", "value": container, "inline": True},
                            {
                                "name": "Time",
                                "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "inline": True,
                            },
                            {"name": "Log", "value": f"```{log_line[:500]}```", "inline": False},
                        ],
                        ping=self.ping_critical,
                    )

            elif classification.severity == Severity.HIGH:
                if self.rate_limiter.should_alert(
                    classification.error_key, classification.cooldown_minutes
                ):
                    log.info(
                        "High priority error detected",
                        container=container,
                        title=classification.title,
                    )
                    self.discord.send_embed(
                        title=f"Warning: {classification.title}",
                        description=classification.description,
                        color=COLOR_HIGH,
                        fields=[
                            {"name": "Container", "value": container, "inline": True},
                            {
                                "name": "Time",
                                "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "inline": True,
                            },
                            {"name": "Log", "value": f"```{log_line[:300]}```", "inline": False},
                        ],
                        ping=False,
                    )

            elif classification.severity == Severity.WARNING:
                # Warnings are just counted for daily summary
                self.rate_limiter.increment_warning(classification.error_key)
                log.debug(
                    "Warning counted",
                    container=container,
                    title=classification.title,
                )

    def _tail_container(self, container_name: str) -> None:
        """Tail logs from a specific container."""
        while not self._stop_event.is_set():
            try:
                if self.docker_client is None:
                    log.error("Docker client not connected")
                    time.sleep(30)
                    continue

                container = self.docker_client.containers.get(container_name)
                log.info("Starting log tail", container=container_name)

                # Tail logs from now onwards
                for log_entry in container.logs(
                    stream=True, follow=True, since=datetime.now(), timestamps=False
                ):
                    if self._stop_event.is_set():
                        break

                    line = log_entry.decode("utf-8", errors="replace").strip()
                    if line:
                        self._process_log_line(container_name, line)

            except docker.errors.NotFound:
                log.warning("Container not found, will retry", container=container_name)
                time.sleep(60)  # Wait before retrying

            except docker.errors.APIError as e:
                log.error("Docker API error", container=container_name, error=str(e))
                time.sleep(30)

            except Exception:
                log.exception("Unexpected error tailing container", container=container_name)
                time.sleep(30)

    def _send_daily_summary(self) -> None:
        """Send daily summary of warnings."""
        with self._lock:
            counts = self.rate_limiter.reset_warning_counts()

        if not counts:
            # No warnings to report
            log.info("Daily summary: no warnings")
            date_str = datetime.now().strftime("%Y-%m-%d")
            self.discord.send_embed(
                title="Cortex Daily Summary",
                description=f"**{date_str}**\n\nNo warnings or errors to report.",
                color=COLOR_INFO,
                ping=False,
            )
            return

        # Format warning counts
        warning_lines = []
        for error_key, count in sorted(counts.items(), key=lambda x: -x[1]):
            # error_key format: "container:error_type"
            parts = error_key.split(":", 1)
            container = parts[0] if len(parts) > 1 else "unknown"
            error_type = parts[1].replace("_", " ").title() if len(parts) > 1 else error_key
            warning_lines.append(f"- **{error_type}** ({container}): {count}")

        description = f"**{datetime.now().strftime('%Y-%m-%d')}**\n\n"
        description += "**Warnings:**\n" + "\n".join(warning_lines[:20])  # Limit to 20 items

        if len(counts) > 20:
            description += f"\n... and {len(counts) - 20} more"

        log.info("Sending daily summary", warning_count=len(counts))
        self.discord.send_embed(
            title="Cortex Daily Summary",
            description=description,
            color=COLOR_WARNING if counts else COLOR_INFO,
            ping=False,
        )

    def _schedule_loop(self) -> None:
        """Run the scheduler loop."""
        while not self._stop_event.is_set():
            schedule.run_pending()
            time.sleep(60)

    def send_test_alert(self) -> bool:
        """Send a test alert to verify webhook is working."""
        time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return self.discord.send_embed(
            title="Test Alert",
            description="Alerter is configured correctly.",
            color=COLOR_INFO,
            fields=[
                {"name": "Time", "value": time_str, "inline": True},
                {"name": "Containers", "value": ", ".join(self.containers), "inline": False},
            ],
            ping=False,
        )

    def run(self) -> None:
        """Start the alerter daemon."""
        log.info("Starting alerter daemon", containers=self.containers)

        # Connect to Docker
        if not self._connect_docker():
            log.error("Cannot start without Docker connection")
            return

        # Schedule daily summary
        schedule.every().day.at(f"{self.summary_hour:02d}:00").do(self._send_daily_summary)
        log.info("Daily summary scheduled", hour=self.summary_hour)

        # Start scheduler thread
        scheduler_thread = threading.Thread(target=self._schedule_loop, daemon=True)
        scheduler_thread.start()
        self._threads.append(scheduler_thread)

        # Start log tailer threads for each container
        for container_name in self.containers:
            thread = threading.Thread(
                target=self._tail_container,
                args=(container_name,),
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)
            log.info("Started log tailer", container=container_name)

        # Send startup notification
        self.discord.send(
            f"Alerter started. Monitoring {len(self.containers)} containers.",
        )

        # Keep main thread alive
        try:
            while not self._stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Received shutdown signal")
            self.stop()

    def stop(self) -> None:
        """Stop the alerter daemon."""
        log.info("Stopping alerter daemon")
        self._stop_event.set()

        # Send shutdown notification
        self.discord.send("Alerter shutting down.")


def run_alerter(
    webhook_url: str | None = None,
    containers: list[str] | None = None,
    ping_critical: bool = True,
    summary_hour: int = 0,
) -> None:
    """Run the alerter daemon.

    Args:
        webhook_url: Discord webhook URL (or from DISCORD_WEBHOOK_URL env)
        containers: Containers to monitor (default: cortex-* containers)
        ping_critical: Whether to @here on critical alerts
        summary_hour: Hour to send daily summary
    """
    url = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        raise ValueError(
            "Discord webhook URL required (set DISCORD_WEBHOOK_URL or pass webhook_url)"
        )

    daemon = AlerterDaemon(
        webhook_url=url,
        containers=containers,
        ping_critical=ping_critical,
        summary_hour=summary_hour,
    )
    daemon.run()
