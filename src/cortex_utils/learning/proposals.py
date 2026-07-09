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
# Terminal apply outcomes (uo9b.4): an approved proposal is either committed as
# a mapping (applied), recorded for manual handling (deferred), or gave up after
# exhausting apply retries (failed).
APPLIED = "applied"
DEFERRED = "deferred"
FAILED = "failed"

# Statuses that mean "already handled or awaiting a decision" — a proposal in
# any of these blocks a fresh one for the same (sender, label, direction) so we
# don't re-nag. FAILED (apply gave up) and SUPERSEDED are deliberately excluded
# so a later opportunity can re-propose and try again.
_ACTIVE_STATUSES = (PENDING, APPROVED, REJECTED, APPLIED, DEFERRED)


@dataclass
class ProposalRun:
    """Summary of one ``propose_from_opportunities`` pass."""

    created: int = 0
    updated: int = 0
    skipped: int = 0


@dataclass
class RuleProposal:
    """A pending candidate rule, as read for presentation/confirmation."""

    id: int
    sender: str
    label: str
    direction: str
    status: str
    source: str
    opportunity_count: int
    discord_message_id: str | None = None
    # Set when the proposal is approved with archive (the 📦 reaction): the
    # committed mapping gets archive=true so the sender's mail is auto-archived.
    archive: bool = False


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
                    CHECK (status IN (
                        'pending', 'approved', 'rejected', 'superseded',
                        'applied', 'deferred', 'failed'
                    )),
                source TEXT NOT NULL DEFAULT 'teach',
                opportunity_count INTEGER NOT NULL DEFAULT 1,
                discord_message_id TEXT,
                archive BOOLEAN NOT NULL DEFAULT FALSE,
                applied_at TIMESTAMPTZ,
                apply_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        # Migration for tables created before discord_message_id existed.
        cur.execute("ALTER TABLE rule_proposals ADD COLUMN IF NOT EXISTS discord_message_id TEXT")
        # Migration for the uo9b.4 apply columns and the widened status set.
        cur.execute("ALTER TABLE rule_proposals ADD COLUMN IF NOT EXISTS applied_at TIMESTAMPTZ")
        cur.execute("ALTER TABLE rule_proposals ADD COLUMN IF NOT EXISTS apply_error TEXT")
        cur.execute(
            "ALTER TABLE rule_proposals ADD COLUMN IF NOT EXISTS "
            "archive BOOLEAN NOT NULL DEFAULT FALSE"
        )
        cur.execute(
            "ALTER TABLE rule_proposals DROP CONSTRAINT IF EXISTS rule_proposals_status_check"
        )
        cur.execute(
            "ALTER TABLE rule_proposals ADD CONSTRAINT rule_proposals_status_check "
            "CHECK (status IN ('pending', 'approved', 'rejected', 'superseded', "
            "'applied', 'deferred', 'failed'))"
        )
        # Index the message lookup, and enforce one proposal per Discord message.
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_rule_proposals_discord_message "
            "ON rule_proposals (discord_message_id) WHERE discord_message_id IS NOT NULL"
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
        # Partial index for listing pending proposals in order as the table
        # accumulates decided rows.
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_rule_proposals_pending_order "
            "ON rule_proposals (created_at, id) WHERE status = 'pending'"
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
        # RETURNING (xmax = 0) is true for a genuine insert, false when the
        # conflict path merged into a row inserted concurrently.
        cur.execute(
            "INSERT INTO rule_proposals "
            "(sender, label, direction, source, opportunity_count) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (sender, label, direction) WHERE status = 'pending' "
            "DO UPDATE SET "
            "opportunity_count = rule_proposals.opportunity_count "
            "+ EXCLUDED.opportunity_count, updated_at = NOW() "
            "RETURNING (xmax = 0)",
            (sender, label, direction, source, count),
        )
        inserted = cur.fetchone()[0]
        return "created" if inserted else "updated"


def _mark_processed(conn: psycopg2.extensions.connection, opportunity_ids: list[int]) -> None:
    if not opportunity_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE learning_opportunities SET status = 'processed' WHERE id = ANY(%s)",
            (opportunity_ids,),
        )


# Statuses a decision can transition a pending proposal into.
_DECISION_STATUSES = (APPROVED, REJECTED, SUPERSEDED)


_PROPOSAL_COLUMNS = (
    "id, sender, label, direction, status, source, opportunity_count, discord_message_id, archive"
)


def _row_to_proposal(row: tuple) -> RuleProposal:
    return RuleProposal(
        id=row[0],
        sender=row[1],
        label=row[2],
        direction=row[3],
        status=row[4],
        source=row[5],
        opportunity_count=row[6],
        discord_message_id=row[7],
        archive=row[8],
    )


def list_pending_proposals(
    conn: psycopg2.extensions.connection,
) -> list[RuleProposal]:
    """Return pending proposals, oldest first, for presentation/confirmation."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_PROPOSAL_COLUMNS} FROM rule_proposals "
            "WHERE status = 'pending' ORDER BY created_at, id"
        )
        rows = cur.fetchall()
    return [_row_to_proposal(row) for row in rows]


def unposted_pending_proposals(
    conn: psycopg2.extensions.connection,
) -> list[RuleProposal]:
    """Pending proposals not yet presented in Discord (no message linked)."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_PROPOSAL_COLUMNS} FROM rule_proposals "
            "WHERE status = 'pending' AND discord_message_id IS NULL "
            "ORDER BY created_at, id"
        )
        rows = cur.fetchall()
    return [_row_to_proposal(row) for row in rows]


def get_proposal_by_message_id(
    conn: psycopg2.extensions.connection, discord_message_id: str
) -> RuleProposal | None:
    """Look up the proposal a Discord message presents, for reaction handling."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_PROPOSAL_COLUMNS} FROM rule_proposals WHERE discord_message_id = %s",
            (discord_message_id,),
        )
        row = cur.fetchone()
    return _row_to_proposal(row) if row is not None else None


def mark_proposal_posted(
    conn: psycopg2.extensions.connection, proposal_id: int, discord_message_id: str
) -> None:
    """Link a proposal to the Discord message presenting it. No commit."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE rule_proposals SET discord_message_id = %s, updated_at = NOW() WHERE id = %s",
            (discord_message_id, proposal_id),
        )


def set_proposal_status(
    conn: psycopg2.extensions.connection, proposal_id: int, status: str
) -> bool:
    """Transition a *pending* proposal to a decision status.

    Only pending proposals move (guards against double-deciding a proposal when
    two reactions race). Returns ``True`` if the row transitioned, ``False`` if
    it was already decided or missing. Does not commit — caller owns the
    transaction.
    """
    if status not in _DECISION_STATUSES:
        raise ValueError(f"invalid decision status: {status!r}")
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE rule_proposals SET status = %s, updated_at = NOW() "
            "WHERE id = %s AND status = 'pending' RETURNING id",
            (status, proposal_id),
        )
        return cur.fetchone() is not None


def approve_proposal(
    conn: psycopg2.extensions.connection, proposal_id: int, *, archive: bool = False
) -> bool:
    """Mark a pending proposal approved (its apply job runs it in uo9b.4).

    ``archive=True`` (the 📦 reaction) also stamps the proposal so the committed
    mapping archives the sender's mail. Archive is only meaningful for an
    add-direction proposal (there's no mapping to archive on a "stop labeling"
    remove), so it's coerced to false for non-add proposals — the invariant
    holds even if a caller passes ``archive=True`` on a remove. Only a pending
    proposal transitions, so a duplicate reaction can't re-decide it. Does not
    commit.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE rule_proposals "
            "SET status = %s, archive = (%s AND direction = 'add'), "
            "    updated_at = NOW() "
            "WHERE id = %s AND status = 'pending' RETURNING id",
            (APPROVED, archive, proposal_id),
        )
        return cur.fetchone() is not None


def reject_proposal(conn: psycopg2.extensions.connection, proposal_id: int) -> bool:
    """Mark a pending proposal rejected so it isn't re-surfaced."""
    return set_proposal_status(conn, proposal_id, REJECTED)


def get_proposal_by_id(
    conn: psycopg2.extensions.connection, proposal_id: int
) -> RuleProposal | None:
    """Load a proposal by id, for an apply job resolving its queue payload."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_PROPOSAL_COLUMNS} FROM rule_proposals WHERE id = %s",
            (proposal_id,),
        )
        row = cur.fetchone()
    return _row_to_proposal(row) if row is not None else None


def _finish_apply(
    conn: psycopg2.extensions.connection,
    proposal_id: int,
    status: str,
    apply_error: str | None,
) -> bool:
    """Transition an *approved* proposal to a terminal apply outcome.

    Only approved proposals move, so a duplicate apply job (e.g. a retry after a
    committed mapping but a lost job-completion) can't re-run the outcome.
    Returns ``True`` if the row transitioned. Does not commit.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE rule_proposals "
            "SET status = %s, apply_error = %s, "
            "    applied_at = CASE WHEN %s = 'applied' THEN NOW() ELSE applied_at END, "
            "    updated_at = NOW() "
            "WHERE id = %s AND status = 'approved' RETURNING id",
            (status, apply_error, status, proposal_id),
        )
        return cur.fetchone() is not None


def mark_proposal_applied(conn: psycopg2.extensions.connection, proposal_id: int) -> bool:
    """Mark an approved proposal committed as a mapping."""
    return _finish_apply(conn, proposal_id, APPLIED, None)


def mark_proposal_deferred(
    conn: psycopg2.extensions.connection, proposal_id: int, reason: str
) -> bool:
    """Record an approved proposal for manual handling (no auto-apply)."""
    return _finish_apply(conn, proposal_id, DEFERRED, reason)


def mark_proposal_failed(
    conn: psycopg2.extensions.connection, proposal_id: int, error: str
) -> bool:
    """Mark an approved proposal as having exhausted its apply retries."""
    return _finish_apply(conn, proposal_id, FAILED, error)
