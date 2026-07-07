"""Tests for rule-proposal rollup (cortex-uo9b.2)."""

from __future__ import annotations

from unittest.mock import MagicMock

from cortex_utils.learning import (
    ensure_proposals_schema,
    propose_from_opportunities,
)


def make_conn(
    fetchall: list | None = None, fetchone_seq: list | None = None
) -> tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    cur = MagicMock()
    if fetchall is not None:
        cur.fetchall.return_value = fetchall
    if fetchone_seq is not None:
        cur.fetchone.side_effect = list(fetchone_seq)
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    return conn, cur


def executed(cur: MagicMock, needle: str) -> int:
    return sum(1 for c in cur.execute.call_args_list if needle in c.args[0])


def test_ensure_schema_creates_table_and_index_no_commit() -> None:
    conn, cur = make_conn()
    ensure_proposals_schema(conn)
    assert executed(cur, "CREATE TABLE IF NOT EXISTS rule_proposals") == 1
    assert executed(cur, "CREATE UNIQUE INDEX") == 1
    assert executed(cur, "CREATE INDEX IF NOT EXISTS idx_rule_proposals_lookup") == 1
    conn.commit.assert_not_called()


def test_creates_new_proposal() -> None:
    conn, cur = make_conn(
        fetchall=[(1, "bob@example.com", "Cortex/Foo", "add")],
        fetchone_seq=[None],  # no existing proposal
    )
    run = propose_from_opportunities(conn)
    assert (run.created, run.updated, run.skipped) == (1, 0, 0)
    assert executed(cur, "INSERT INTO rule_proposals") == 1
    assert executed(cur, "UPDATE learning_opportunities SET status = 'processed'") == 1


def test_dedups_into_existing_pending() -> None:
    conn, cur = make_conn(
        fetchall=[(2, "bob@example.com", "Cortex/Foo", "add")],
        fetchone_seq=[(99, "pending")],
    )
    run = propose_from_opportunities(conn)
    assert (run.created, run.updated, run.skipped) == (0, 1, 0)
    assert executed(cur, "opportunity_count = opportunity_count + %s") == 1
    assert executed(cur, "INSERT INTO rule_proposals") == 0


def test_groups_same_triple_into_one_proposal() -> None:
    # Three opportunities for the same triple -> one proposal, count 3.
    conn, cur = make_conn(
        fetchall=[
            (1, "bob@example.com", "Cortex/Foo", "add"),
            (2, "bob@example.com", "Cortex/Foo", "add"),
            (3, "bob@example.com", "Cortex/Foo", "add"),
        ],
        fetchone_seq=[None],  # a single upsert for the group
    )
    run = propose_from_opportunities(conn)
    assert (run.created, run.updated, run.skipped) == (1, 0, 0)
    inserts = [c for c in cur.execute.call_args_list if "INSERT INTO rule_proposals" in c.args[0]]
    assert len(inserts) == 1
    assert inserts[0].args[1][-1] == 3  # opportunity_count reflects the group size


def test_skips_when_already_rejected() -> None:
    conn, cur = make_conn(
        fetchall=[(3, "bob@example.com", "Cortex/Foo", "add")],
        fetchone_seq=[(50, "rejected")],
    )
    run = propose_from_opportunities(conn)
    assert (run.created, run.updated, run.skipped) == (0, 0, 1)
    assert executed(cur, "INSERT INTO rule_proposals") == 0
    # still marks the opportunity processed
    assert executed(cur, "UPDATE learning_opportunities SET status = 'processed'") == 1


def test_skips_when_already_approved() -> None:
    conn, _ = make_conn(
        fetchall=[(4, "bob@example.com", "Cortex/Foo", "add")],
        fetchone_seq=[(51, "approved")],
    )
    run = propose_from_opportunities(conn)
    assert (run.created, run.updated, run.skipped) == (0, 0, 1)


def test_skips_null_sender() -> None:
    conn, cur = make_conn(
        fetchall=[(5, None, "Cortex/Foo", "add")],
        fetchone_seq=[],  # _upsert returns before any fetchone
    )
    run = propose_from_opportunities(conn)
    assert (run.created, run.updated, run.skipped) == (0, 0, 1)
    assert executed(cur, "INSERT INTO rule_proposals") == 0
    # null-sender opportunity is still marked processed (not left dangling)
    assert executed(cur, "UPDATE learning_opportunities SET status = 'processed'") == 1


def test_no_opportunities_is_empty_run() -> None:
    conn, _ = make_conn(fetchall=[], fetchone_seq=[])
    run = propose_from_opportunities(conn)
    assert (run.created, run.updated, run.skipped) == (0, 0, 0)


def test_multiple_opportunities_mixed_outcomes() -> None:
    conn, _ = make_conn(
        fetchall=[
            (10, "a@x.com", "Cortex/A", "add"),  # new -> created
            (11, "b@x.com", "Cortex/B", "add"),  # existing pending -> updated
            (12, None, "Cortex/C", "add"),  # null sender -> skipped
        ],
        fetchone_seq=[None, (77, "pending")],  # a: none, b: pending (c: no fetchone)
    )
    run = propose_from_opportunities(conn)
    assert (run.created, run.updated, run.skipped) == (1, 1, 1)
