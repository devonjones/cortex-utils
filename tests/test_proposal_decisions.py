"""Tests for reading and deciding rule proposals (cortex-uo9b.3)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cortex_utils.learning import (
    APPROVED,
    REJECTED,
    RuleProposal,
    approve_proposal,
    get_proposal_by_message_id,
    list_pending_proposals,
    mark_proposal_posted,
    reject_proposal,
    set_proposal_status,
    unposted_pending_proposals,
)


def make_conn(
    fetchall: list | None = None, fetchone: object = "unset"
) -> tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    cur = MagicMock()
    if fetchall is not None:
        cur.fetchall.return_value = fetchall
    if fetchone != "unset":
        cur.fetchone.return_value = fetchone
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    return conn, cur


def test_list_pending_proposals_maps_rows() -> None:
    conn, _ = make_conn(
        fetchall=[
            (1, "bob@x.com", "Cortex/Foo", "add", "pending", "teach", 3, None, False),
            (2, "amy@x.com", "Cortex/Bar", "remove", "pending", "teach", 1, "msg-9", True),
        ]
    )
    proposals = list_pending_proposals(conn)
    assert proposals == [
        RuleProposal(1, "bob@x.com", "Cortex/Foo", "add", "pending", "teach", 3, None),
        RuleProposal(2, "amy@x.com", "Cortex/Bar", "remove", "pending", "teach", 1, "msg-9", True),
    ]


def test_list_pending_proposals_empty() -> None:
    conn, _ = make_conn(fetchall=[])
    assert list_pending_proposals(conn) == []


def test_unposted_pending_proposals_maps_rows() -> None:
    conn, cur = make_conn(
        fetchall=[(3, "cy@x.com", "Cortex/Baz", "add", "pending", "teach", 1, None, False)]
    )
    proposals = unposted_pending_proposals(conn)
    assert proposals == [
        RuleProposal(3, "cy@x.com", "Cortex/Baz", "add", "pending", "teach", 1, None)
    ]
    assert "discord_message_id IS NULL" in cur.execute.call_args_list[0].args[0]


def test_get_proposal_by_message_id_found() -> None:
    conn, _ = make_conn(
        fetchone=(5, "bob@x.com", "Cortex/Foo", "add", "pending", "teach", 2, "msg-1", False)
    )
    proposal = get_proposal_by_message_id(conn, "msg-1")
    assert proposal is not None
    assert proposal.id == 5 and proposal.discord_message_id == "msg-1"


def test_get_proposal_by_message_id_missing() -> None:
    conn, _ = make_conn(fetchone=None)
    assert get_proposal_by_message_id(conn, "nope") is None


def test_mark_proposal_posted_updates() -> None:
    conn, cur = make_conn()
    mark_proposal_posted(conn, 7, "msg-42")
    sql, params = cur.execute.call_args_list[0].args
    assert "SET discord_message_id" in sql
    assert params == ("msg-42", 7)


def test_set_status_transitions_pending() -> None:
    conn, _ = make_conn(fetchone=(7,))  # RETURNING id -> transitioned
    assert set_proposal_status(conn, 7, APPROVED) is True


def test_set_status_no_op_when_already_decided() -> None:
    conn, _ = make_conn(fetchone=None)  # WHERE status='pending' matched nothing
    assert set_proposal_status(conn, 7, REJECTED) is False


def test_set_status_rejects_invalid_status() -> None:
    conn, _ = make_conn()
    with pytest.raises(ValueError):
        set_proposal_status(conn, 7, "pending")  # not a decision status
    with pytest.raises(ValueError):
        set_proposal_status(conn, 7, "bogus")


def test_approve_and_reject_wrappers() -> None:
    conn, cur = make_conn(fetchone=(7,))
    assert approve_proposal(conn, 7) is True
    assert reject_proposal(conn, 7) is True
    statuses = [c.args[1][0] for c in cur.execute.call_args_list]
    assert APPROVED in statuses and REJECTED in statuses


def test_approve_with_archive_sets_flag() -> None:
    conn, cur = make_conn(fetchone=(7,))
    assert approve_proposal(conn, 7, archive=True) is True
    sql, params = cur.execute.call_args_list[0].args
    assert "archive = %s" in sql
    assert params == (APPROVED, True, 7)


def test_approve_defaults_archive_false() -> None:
    conn, cur = make_conn(fetchone=(7,))
    approve_proposal(conn, 7)
    assert cur.execute.call_args_list[0].args[1] == (APPROVED, False, 7)
