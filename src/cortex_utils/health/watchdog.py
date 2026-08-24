"""Mutual liveness watchdog.

Every host runs this and checks every *other* host, so no single machine has to
be the reliable one. The monitoring stack lives on one box today, which means an
outage of that box is silent -- the failure reports nothing because the reporter
is what failed.

Duplicate alerts are deliberate. With N hosts a real outage produces N-1 reports
from independent observers, and that is the signal worth having: two observers
agreeing means the host is down, one observer alone means a network partition
between those two. Deduplicating would need a coordinator, which is the single
point of failure this design exists to remove.

Checks are TCP connects, not pings. A host that drops ICMP while serving traffic
happily reads as dead to a ping check -- macOS does exactly that.
"""

from __future__ import annotations

import socket
import time
from dataclasses import dataclass, field

from cortex_utils.alerter.discord import DiscordClient
from cortex_utils.log import get_logger

log = get_logger()

RED = 0xE74C3C
GREEN = 0x2ECC71


@dataclass(frozen=True)
class Peer:
    """A host to watch, and the port used to decide it is alive.

    `failures_before_alert` overrides the watchdog default for this host alone.
    Hosts are not equally reliable: a Raspberry Pi that drops off the network for
    a couple of minutes and returns is behaving normally for a Raspberry Pi, and
    paging on it trains you to ignore the channel. Raising the global threshold
    instead would blind every solid host to a real outage.
    """

    name: str
    host: str
    port: int
    failures_before_alert: int | None = None


@dataclass
class PeerState:
    """Consecutive failures and last reported state for one peer."""

    failures: int = 0
    reported_down: bool = False


def parse_peers(spec: str) -> list[Peer]:
    """Parse "name=host:port[:failures],..." into peers.

    The optional third field overrides the alert threshold for that host only.

    Raises ValueError on a malformed entry rather than skipping it: a typo that
    silently drops a host would leave that host unwatched, which is the failure
    this whole module exists to prevent.
    """
    peers = []
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            name, target = entry.split("=", 1)
            host, port, *rest = target.split(":")
            if len(rest) > 1:
                raise ValueError("too many fields")
            peers.append(
                Peer(
                    name=name.strip(),
                    host=host.strip(),
                    port=int(port),
                    failures_before_alert=int(rest[0]) if rest else None,
                )
            )
        except ValueError as exc:
            raise ValueError(
                f"malformed peer entry {entry!r}; expected name=host:port[:failures]"
            ) from exc
    return peers


def is_up(peer: Peer, timeout: float) -> bool:
    """True if a TCP connection to the peer succeeds."""
    try:
        with socket.create_connection((peer.host, peer.port), timeout=timeout):
            return True
    except OSError:
        return False


@dataclass
class Watchdog:
    """Watches peers and reports transitions to Discord.

    `observer` names the host doing the watching so a reader can tell N reports
    of one outage from N separate outages.
    """

    peers: list[Peer]
    discord: DiscordClient
    observer: str
    failures_before_alert: int = 3
    timeout: float = 5.0
    state: dict[str, PeerState] = field(default_factory=dict)

    def poll_once(self) -> dict[str, bool]:
        """Check every peer once, alerting on any state transition."""
        results = {}
        for peer in self.peers:
            up = is_up(peer, self.timeout)
            results[peer.name] = up
            self._record(peer, up)
        return results

    def _record(self, peer: Peer, up: bool) -> None:
        """Update a peer's state and alert if it crossed a threshold."""
        state = self.state.setdefault(peer.name, PeerState())

        if up:
            if state.reported_down:
                self._announce_up(peer)
            state.failures = 0
            state.reported_down = False
            return

        threshold = peer.failures_before_alert or self.failures_before_alert
        state.failures += 1
        log.warning(
            "Peer check failed",
            peer=peer.name,
            failures=state.failures,
            threshold=threshold,
        )
        if state.failures >= threshold and not state.reported_down:
            state.reported_down = True
            self._announce_down(peer, state.failures)

    def _announce_down(self, peer: Peer, failures: int) -> None:
        self.discord.send_embed(
            title=f"🔴 {peer.name} is unreachable",
            description=(
                f"`{peer.host}:{peer.port}` refused {failures} consecutive "
                f"connections, seen from **{self.observer}**."
            ),
            color=RED,
            fields=[{"name": "Observer", "value": self.observer, "inline": True}],
            ping=True,
        )
        log.error("Peer down", peer=peer.name, observer=self.observer)

    def _announce_up(self, peer: Peer) -> None:
        self.discord.send_embed(
            title=f"🟢 {peer.name} is back",
            description=f"`{peer.host}:{peer.port}` is accepting connections again.",
            color=GREEN,
            fields=[{"name": "Observer", "value": self.observer, "inline": True}],
        )
        log.info("Peer recovered", peer=peer.name, observer=self.observer)

    def run(self, interval: float) -> None:
        """Poll forever. Only returns if interrupted."""
        log.info(
            "Watchdog starting",
            observer=self.observer,
            peers=[p.name for p in self.peers],
            interval=interval,
        )
        while True:
            self.poll_once()
            time.sleep(interval)
