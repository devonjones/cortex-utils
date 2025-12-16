"""Discord webhook client for sending alerts."""

import httpx
import structlog

log = structlog.get_logger()


class DiscordClient:
    """Simple Discord webhook client."""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.username = "Cortex Alerter"

    def send(self, message: str, ping: bool = False) -> bool:
        """Send a message to Discord.

        Args:
            message: The message content (supports Discord markdown)
            ping: If True, prepend @here to alert channel members

        Returns:
            True if successful, False otherwise
        """
        content = f"@here\n{message}" if ping else message

        try:
            response = httpx.post(
                self.webhook_url,
                json={
                    "content": content,
                    "username": self.username,
                },
                timeout=10.0,
            )
            response.raise_for_status()
            log.debug("Discord message sent", ping=ping)
            return True
        except httpx.HTTPStatusError as e:
            log.error("Discord API error", status=e.response.status_code)
            return False
        except httpx.RequestError as e:
            log.error("Discord request failed", error=str(e))
            return False

    def send_embed(
        self,
        title: str,
        description: str,
        color: int,
        fields: list[dict] | None = None,
        ping: bool = False,
    ) -> bool:
        """Send a rich embed message to Discord.

        Args:
            title: Embed title
            description: Embed description
            color: Embed color (decimal, e.g., 0xFF0000 for red)
            fields: Optional list of {"name": "...", "value": "...", "inline": bool}
            ping: If True, prepend @here

        Returns:
            True if successful, False otherwise
        """
        embed = {
            "title": title,
            "description": description,
            "color": color,
        }
        if fields:
            embed["fields"] = fields

        payload = {
            "username": self.username,
            "embeds": [embed],
        }
        if ping:
            payload["content"] = "@here"

        try:
            response = httpx.post(
                self.webhook_url,
                json=payload,
                timeout=10.0,
            )
            response.raise_for_status()
            log.debug("Discord embed sent", title=title, ping=ping)
            return True
        except httpx.HTTPStatusError as e:
            log.error("Discord API error", status=e.response.status_code)
            return False
        except httpx.RequestError as e:
            log.error("Discord request failed", error=str(e))
            return False


# Discord embed colors
COLOR_CRITICAL = 0xFF0000  # Red
COLOR_HIGH = 0xFFA500  # Orange
COLOR_WARNING = 0xFFFF00  # Yellow
COLOR_INFO = 0x00FF00  # Green
