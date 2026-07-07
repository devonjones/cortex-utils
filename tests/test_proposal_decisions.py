"""Tests for reading and deciding rule proposals (cortex-uo9b.3)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cortex_utils.learning import (
    APPROVED,
    REJECTED,
    RuleProposal,
    approve_proposal,
    list_pending_proposals,
    reject_proposal,
    set_proposal_status,
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
            (1, "bob@x.com", "Cortex/Foo", "add", "pending", "teach", 3),
            (2, "amy@x.com", "Cortex/Bar", "remove", "pending", "teach", 1),
        ]
    )
    proposals = list_pending_proposals(conn)
    assert proposals == [
        RuleProposal(1, "bob@x.com", "Cortex/Foo", "add", "pending", "teach", 3),
        RuleProposal(2, "amy@x.com", "Cortex/Bar", "remove", "pending", "teach", 1),
    ]


def test_list_pending_proposals_empty() -> None:
    conn, _ = make_conn(fetchall=[])
    assert list_pending_proposals(conn) == []


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
