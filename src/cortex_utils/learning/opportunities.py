"""Storage for learning opportunities (cortex-uo9b).

A learning opportunity is a divergence between a manual Gmail label change and
what triage classified: a human added a managed label triage did not assign, or
removed one triage had assigned. Written inline by postmark's gmail-sync when it
detects such an event; read by the rule-proposal step. Kept in cortex-utils so
both sides share one schema without cross-service imports.
"""

from __future__ import annotations

import psycopg2
from psycopg2.extras import Json

# Divergence directions.
ADD = "add"  # human added a managed label triage did not assign
REMOVE = "remove"  # human removed a managed label triage had assigned
DIRECTIONS = (ADD, REMOVE)


def ensure_learning_schema(conn: psycopg2.extensions.connection) -> None:
    """Create the ``learning_opportunities`` table and index. Idempotent.

    Does not commit — leaves transaction management to the caller so schema
    setup composes with the caller's other DDL (e.g. gmail-sync's startup
    schema block).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS learning_opportunities (
                id BIGSERIAL PRIMARY KEY,
                gmail_id TEXT NOT NULL,
                sender TEXT,
                label TEXT NOT NULL,
                direction TEXT NOT NULL CHECK (direction IN ('add', 'remove')),
                expected_labels JSONB,
                status TEXT NOT NULL DEFAULT 'pending',
                detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_learning_opportunity
                    UNIQUE (gmail_id, label, direction)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_learning_opportunities_pending "
            "ON learning_opportunities (status, detected_at)"
        )


def record_learning_opportunity(
    conn: psycopg2.extensions.connection,
    *,
    gmail_id: str,
    label: str,
    direction: str,
    sender: str | None = None,
    expected_labels: set[str] | None = None,
) -> bool:
    """Record a divergence, deduped by ``(gmail_id, label, direction)``.

    Returns ``True`` if a new row was inserted, ``False`` if an identical
    divergence was already recorded. Does not commit — participates in the
    caller's transaction so the label-event update and this insert commit
    together.
    """
    if direction not in DIRECTIONS:
        raise ValueError(f"invalid direction: {direction!r}")

    expected = Json(sorted(expected_labels)) if expected_labels is not None else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO learning_opportunities
                (gmail_id, sender, label, direction, expected_labels)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (gmail_id, label, direction) DO NOTHING
            RETURNING id
            """,
            (gmail_id, sender, label, direction, expected),
        )
        return cur.fetchone() is not None
