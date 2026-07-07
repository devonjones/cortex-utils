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
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'approved', 'rejected', 'superseded')),
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
        # Non-partial index so lookups of decided (approved/rejected) proposals
        # for a triple don't fall back to a sequential scan.
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_rule_proposals_lookup "
            "ON rule_proposals (sender, label, direction)"
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
        # FOR UPDATE SKIP LOCKED lets a concurrent proposer skip rows this run
        # already holds rather than block; ORDER BY id keeps a consistent lock
        # order. (Same claim idiom the triage worker uses on the queue table.)
        cur.execute(
            "SELECT id, sender, label, direction "
            "FROM learning_opportunities WHERE status = 'pending' "
            "ORDER BY id FOR UPDATE SKIP LOCKED"
        )
        opportunities = cur.fetchall()

    if not opportunities:
        return run

    # Group by (sender, label, direction) so each triple is upserted once with
    # its rolled-up count, rather than one round-trip per opportunity.
    grouped: dict[tuple[str, str, str], int] = {}
    opportunity_ids: list[int] = []
    for opp_id, sender, label, direction in opportunities:
        opportunity_ids.append(opp_id)
        if not sender:
            run.skipped += 1  # cannot map a null sender
            continue
        key = (sender, label, direction)
        grouped[key] = grouped.get(key, 0) + 1

    # Sorted for a deterministic rule_proposals lock order across concurrent runs.
    for (sender, label, direction), count in sorted(grouped.items()):
        outcome = _upsert_proposal(conn, sender, label, direction, source, count)
        setattr(run, outcome, getattr(run, outcome) + 1)

    _mark_processed(conn, opportunity_ids)
    return run


def _upsert_proposal(
    conn: psycopg2.extensions.connection,
    sender: str,
    label: str,
    direction: str,
    source: str,
    count: int,
) -> str:
    """Create or bump a proposal for a triple; return created|updated|skipped."""
    with conn.cursor() as cur:
        # FOR UPDATE guards the read-then-write against a concurrent
        # approve/reject racing between the SELECT and the UPDATE.
        cur.execute(
            "SELECT id, status FROM rule_proposals "
            "WHERE sender = %s AND label = %s AND direction = %s "
            "  AND status IN %s "
            "ORDER BY id DESC LIMIT 1 FOR UPDATE",
            (sender, label, direction, _ACTIVE_STATUSES),
        )
        existing = cur.fetchone()

        if existing is not None:
            proposal_id, status = existing
            if status != PENDING:
                return "skipped"  # already approved or rejected -> don't re-propose
            cur.execute(
                "UPDATE rule_proposals "
                "SET opportunity_count = opportunity_count + %s, updated_at = NOW() "
                "WHERE id = %s",
                (count, proposal_id),
            )
            return "updated"

        # ON CONFLICT handles a concurrent run inserting the same pending triple
        # between our SELECT (which found nothing to lock) and this INSERT.
        cur.execute(
            "INSERT INTO rule_proposals "
            "(sender, label, direction, source, opportunity_count) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (sender, label, direction) WHERE status = 'pending' "
            "DO UPDATE SET "
            "opportunity_count = rule_proposals.opportunity_count "
            "+ EXCLUDED.opportunity_count, updated_at = NOW()",
            (sender, label, direction, source, count),
        )
        return "created"


def _mark_processed(conn: psycopg2.extensions.connection, opportunity_ids: list[int]) -> None:
    if not opportunity_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE learning_opportunities SET status = 'processed' WHERE id = ANY(%s)",
            (opportunity_ids,),
        )
