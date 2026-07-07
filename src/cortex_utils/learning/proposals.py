"""Rule proposals derived from learning opportunities (cortex-uo9b.2).

A rule proposal is a candidate triage rule — map ``sender -> label``, or stop
mapping ``sender -> label`` — rolled up from one or more learning opportunities.
Proposals are presented for confirmation (uo9b.3) and, once approved, committed
as triage config (uo9b.4). Shared in cortex-utils so the teach flow and Inbox
Taming (cortex-w5u) can produce and consume the same model.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg2

SOURCE_TEACH = "teach"

# Proposal statuses.
PENDING = "pending"
APPROVED = "approved"
REJECTED = "rejected"
SUPERSEDED = "superseded"

# Statuses that mean "already decided or awaiting a decision" — a proposal in
# any of these blocks a fresh one for the same (sender, label, direction).
_ACTIVE_STATUSES = (PENDING, APPROVED, REJECTED)


@dataclass
class ProposalRun:
    """Summary of one ``propose_from_opportunities`` pass."""

    created: int = 0
    updated: int = 0
    skipped: int = 0


def ensure_proposals_schema(conn: psycopg2.extensions.connection) -> None:
    """Create the ``rule_proposals`` table and indexes. Idempotent.

    Does not commit — leaves transaction management to the caller.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rule_proposals (
                id BIGSERIAL PRIMARY KEY,
                sender TEXT NOT NULL,
                label TEXT NOT NULL,
                direction TEXT NOT NULL CHECK (direction IN ('add', 'remove')),
                status TEXT NOT NULL DEFAULT 'pending',
                source TEXT NOT NULL DEFAULT 'teach',
                opportunity_count INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        # At most one open (pending) proposal per (sender, label, direction).
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_rule_proposal_pending "
            "ON rule_proposals (sender, label, direction) WHERE status = 'pending'"
        )


def propose_from_opportunities(
    conn: psycopg2.extensions.connection, *, source: str = SOURCE_TEACH
) -> ProposalRun:
    """Roll pending ``learning_opportunities`` into deduped rule proposals.

    Many opportunities for the same ``(sender, label, direction)`` collapse into
    one proposal (``opportunity_count`` bumped). A sender with an already-decided
    proposal (approved or rejected) is skipped so we don't re-nag. Opportunities
    with no sender can't be mapped and are skipped. Every processed opportunity
    is marked so it isn't reconsidered. Does not commit — the caller owns the
    transaction.
    """
    run = ProposalRun()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, sender, label, direction "
            "FROM learning_opportunities WHERE status = 'pending' ORDER BY id"
        )
        opportunities = cur.fetchall()

    for opp_id, sender, label, direction in opportunities:
        outcome = _upsert_proposal(conn, sender, label, direction, source)
        _mark_processed(conn, opp_id)
        setattr(run, outcome, getattr(run, outcome) + 1)
    return run


def _upsert_proposal(
    conn: psycopg2.extensions.connection,
    sender: str | None,
    label: str,
    direction: str,
    source: str,
) -> str:
    """Create or bump a proposal for a triple; return created|updated|skipped."""
    if not sender:
        return "skipped"  # cannot map a null sender

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, status FROM rule_proposals "
            "WHERE sender = %s AND label = %s AND direction = %s "
            "  AND status IN %s "
            "ORDER BY id DESC LIMIT 1",
            (sender, label, direction, _ACTIVE_STATUSES),
        )
        existing = cur.fetchone()

        if existing is not None:
            proposal_id, status = existing
            if status != PENDING:
                return "skipped"  # already approved or rejected -> don't re-propose
            cur.execute(
                "UPDATE rule_proposals "
                "SET opportunity_count = opportunity_count + 1, updated_at = NOW() "
                "WHERE id = %s",
                (proposal_id,),
            )
            return "updated"

        cur.execute(
            "INSERT INTO rule_proposals (sender, label, direction, source) VALUES (%s, %s, %s, %s)",
            (sender, label, direction, source),
        )
        return "created"


def _mark_processed(conn: psycopg2.extensions.connection, opportunity_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE learning_opportunities SET status = 'processed' WHERE id = %s",
            (opportunity_id,),
        )
