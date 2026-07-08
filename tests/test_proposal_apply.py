"""Tests for applying approved proposals via the apply queue (cortex-uo9b.4)."""

from __future__ import annotations

from unittest.mock import MagicMock

from cortex_utils.learning import (
    APPLIED,
    APPLY_QUEUE,
    DEFERRED,
    FAILED,
    ApplyJob,
    claim_proposal_apply_jobs,
    complete_apply_job,
    enqueue_proposal_apply,
    get_proposal_by_id,
    mark_proposal_applied,
    mark_proposal_deferred,
    mark_proposal_failed,
    taught_mail_archived,
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


# --- enqueue ---


def test_enqueue_returns_job_id_and_targets_apply_queue() -> None:
    conn, cur = make_conn(fetchone=(101,))
    assert enqueue_proposal_apply(conn, 7) == 101
    sql, params = cur.execute.call_args_list[0].args
    assert "INSERT INTO queue" in sql
    assert params[0] == APPLY_QUEUE
    assert params[1].adapted == {"proposal_id": 7}  # psycopg2 Json wrapper
    assert params[2] == 0  # default priority


def test_enqueue_honours_priority() -> None:
    conn, cur = make_conn(fetchone=(1,))
    enqueue_proposal_apply(conn, 3, priority=-100)
    assert cur.execute.call_args_list[0].args[1][2] == -100


# --- claim ---


def test_claim_maps_rows_to_apply_jobs() -> None:
    conn, cur = make_conn(
        fetchall=[
            (11, {"proposal_id": 7}, 0, 3),
            (12, {"proposal_id": 9}, 2, 3),
        ]
    )
    jobs = claim_proposal_apply_jobs(conn, limit=5)
    assert jobs == [
        ApplyJob(job_id=11, proposal_id=7, attempts=0, max_attempts=3),
        ApplyJob(job_id=12, proposal_id=9, attempts=2, max_attempts=3),
    ]
    sql, params = cur.execute.call_args_list[0].args
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert params == (APPLY_QUEUE, 5)


def test_claim_empty() -> None:
    conn, _ = make_conn(fetchall=[])
    assert claim_proposal_apply_jobs(conn) == []


def test_complete_marks_job_completed() -> None:
    conn, cur = make_conn()
    complete_apply_job(conn, 11)
    sql, params = cur.execute.call_args_list[0].args
    assert "status = 'completed'" in sql
    assert params == (11,)


# --- terminal transitions (guarded on status='approved') ---


def test_mark_applied_transitions_from_approved() -> None:
    conn, cur = make_conn(fetchone=(7,))
    assert mark_proposal_applied(conn, 7) is True
    sql, params = cur.execute.call_args_list[0].args
    assert "status = 'approved'" in sql  # only approved rows move
    assert params[0] == APPLIED and params[1] is None and params[3] == 7


def test_mark_applied_no_op_when_not_approved() -> None:
    conn, _ = make_conn(fetchone=None)
    assert mark_proposal_applied(conn, 7) is False


def test_mark_deferred_records_reason() -> None:
    conn, cur = make_conn(fetchone=(7,))
    assert mark_proposal_deferred(conn, 7, "remove — recorded") is True
    params = cur.execute.call_args_list[0].args[1]
    assert params[0] == DEFERRED and params[1] == "remove — recorded"


def test_mark_failed_records_error() -> None:
    conn, cur = make_conn(fetchone=(7,))
    assert mark_proposal_failed(conn, 7, "gateway 500") is True
    params = cur.execute.call_args_list[0].args[1]
    assert params[0] == FAILED and params[1] == "gateway 500"


def test_get_proposal_by_id_missing() -> None:
    conn, _ = make_conn(fetchone=None)
    assert get_proposal_by_id(conn, 999) is None


# --- archive signal ---


def test_taught_mail_archived_true_when_no_inbox() -> None:
    conn, _ = make_conn(fetchone=(["STARRED", "Label_98"],))
    assert taught_mail_archived(conn, "bob@x.com", "Cortex/Foo") is True


def test_taught_mail_archived_false_when_in_inbox() -> None:
    conn, _ = make_conn(fetchone=(["INBOX", "Label_98"],))
    assert taught_mail_archived(conn, "bob@x.com", "Cortex/Foo") is False


def test_taught_mail_archived_false_when_no_example() -> None:
    conn, _ = make_conn(fetchone=None)
    assert taught_mail_archived(conn, "bob@x.com", "Cortex/Foo") is False


def test_taught_mail_archived_handles_null_label_ids() -> None:
    conn, _ = make_conn(fetchone=(None,))
    assert taught_mail_archived(conn, "bob@x.com", "Cortex/Foo") is False
