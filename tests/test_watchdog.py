"""Tests for the mutual liveness watchdog.

The state machine is the part worth pinning: alerting on every failed poll would
make an outage unreadable, and never re-arming after recovery would make the
second outage silent.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cortex_utils.health.watchdog import Peer, Watchdog, parse_peers


def _watchdog(failures_before_alert: int = 3) -> tuple[Watchdog, MagicMock]:
    discord = MagicMock()
    dog = Watchdog(
        peers=[Peer(name="hades", host="10.0.0.1", port=22)],
        discord=discord,
        observer="ares",
        failures_before_alert=failures_before_alert,
    )
    return dog, discord


def _feed(dog: Watchdog, *ups: bool) -> None:
    """Drive the state machine directly, without touching a socket."""
    for up in ups:
        dog._record(dog.peers[0], up)


def test_parse_peers_reads_name_host_port() -> None:
    peers = parse_peers("hades=10.0.0.1:22, gaia=10.0.0.2:9000")
    assert peers == [
        Peer(name="hades", host="10.0.0.1", port=22),
        Peer(name="gaia", host="10.0.0.2", port=9000),
    ]


def test_parse_peers_rejects_malformed_entry() -> None:
    """A silently dropped host would be an unwatched host."""
    with pytest.raises(ValueError):
        parse_peers("hades=10.0.0.1")


def test_no_alert_before_threshold() -> None:
    """A single dropped poll is noise, not an outage."""
    dog, discord = _watchdog(failures_before_alert=3)
    _feed(dog, False, False)
    discord.send_embed.assert_not_called()


def test_alerts_once_at_threshold_and_not_again() -> None:
    """A host that stays down must not alert on every poll."""
    dog, discord = _watchdog(failures_before_alert=3)
    _feed(dog, False, False, False, False, False)
    assert discord.send_embed.call_count == 1
    assert "unreachable" in discord.send_embed.call_args.kwargs["title"]


def test_recovery_alerts_and_rearms() -> None:
    """After recovery a second outage must alert again, or it goes unnoticed."""
    dog, discord = _watchdog(failures_before_alert=2)
    _feed(dog, False, False)  # down -> 1 alert
    _feed(dog, True)  # up   -> recovery alert
    _feed(dog, False, False)  # down -> must alert again
    titles = [c.kwargs["title"] for c in discord.send_embed.call_args_list]
    assert sum("unreachable" in t for t in titles) == 2
    assert sum("is back" in t for t in titles) == 1


def test_recovery_before_threshold_sends_nothing() -> None:
    """A blip that recovers before alerting should stay silent, both ways."""
    dog, discord = _watchdog(failures_before_alert=3)
    _feed(dog, False, False, True)
    discord.send_embed.assert_not_called()


def test_alert_names_the_observer() -> None:
    """N reports of one outage must be distinguishable from N outages."""
    dog, discord = _watchdog(failures_before_alert=1)
    _feed(dog, False)
    assert "ares" in discord.send_embed.call_args.kwargs["description"]


def test_parse_peers_reads_optional_threshold() -> None:
    peers = parse_peers("hades=10.0.0.1:22,trident=10.0.0.9:22:20")
    assert peers[0].failures_before_alert is None
    assert peers[1].failures_before_alert == 20


def test_parse_peers_rejects_extra_fields() -> None:
    with pytest.raises(ValueError):
        parse_peers("hades=10.0.0.1:22:3:9")


def test_per_peer_threshold_overrides_the_default() -> None:
    """A flaky host must not page on a blip that a solid host should page on."""
    discord = MagicMock()
    flaky = Peer(name="trident", host="10.0.0.9", port=22, failures_before_alert=5)
    dog = Watchdog(peers=[flaky], discord=discord, observer="ares", failures_before_alert=2)
    for _ in range(4):
        dog._record(flaky, False)
    discord.send_embed.assert_not_called()  # past the global 2, under its own 5
    dog._record(flaky, False)
    assert discord.send_embed.call_count == 1


def test_default_still_applies_to_peers_without_an_override() -> None:
    """Raising one host's tolerance must not blind the others."""
    discord = MagicMock()
    solid = Peer(name="hades", host="10.0.0.1", port=22)
    dog = Watchdog(peers=[solid], discord=discord, observer="ares", failures_before_alert=2)
    dog._record(solid, False)
    dog._record(solid, False)
    assert discord.send_embed.call_count == 1
