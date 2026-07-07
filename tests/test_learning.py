"""Tests for the rule-learning primitives (cortex-uo9b)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cortex_utils.learning import (
    ADD,
    REMOVE,
    active_label_prefix,
    ensure_learning_schema,
    expected_cortex_labels,
    managed_labels,
    record_learning_opportunity,
)


def make_conn(fetchone_returns: list) -> tuple[MagicMock, MagicMock]:
    """Build a mock connection whose cursor yields the given fetchone values."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.side_effect = list(fetchone_returns)
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    return conn, cur


# --- managed_labels (pure) -------------------------------------------------


def test_managed_labels_add_label_list() -> None:
    action = {"add_label": ["Cortex/Work", "Cortex/Family"]}
    assert managed_labels(action, "Cortex") == {"Cortex/Work", "Cortex/Family"}


def test_managed_labels_add_label_str_and_label_field() -> None:
    action = {"label": "Cortex/A", "add_label": "Cortex/B"}
    assert managed_labels(action, "Cortex") == {"Cortex/A", "Cortex/B"}


def test_managed_labels_filters_unmanaged_prefix() -> None:
    action = {"add_label": ["Cortex/Work", "Personal/Notes", "INBOX"]}
    assert managed_labels(action, "Cortex") == {"Cortex/Work"}


def test_managed_labels_empty_prefix_manages_all() -> None:
    action = {"add_label": ["Cortex/Work", "Personal/Notes"]}
    assert managed_labels(action, "") == {"Cortex/Work", "Personal/Notes"}


def test_managed_labels_ignores_non_string_entries() -> None:
    action = {"add_label": ["Cortex/Work", None, 5]}
    assert managed_labels(action, "Cortex") == {"Cortex/Work"}


def test_managed_labels_empty_action() -> None:
    assert managed_labels({}, "Cortex") == set()


# --- active_label_prefix ---------------------------------------------------


def test_active_label_prefix_from_config() -> None:
    conn, _ = make_conn([("Cortex",)])
    assert active_label_prefix(conn) == "Cortex"


def test_active_label_prefix_default_when_no_active_config() -> None:
    conn, _ = make_conn([None])
    assert active_label_prefix(conn) == "Cortex"


def test_active_label_prefix_default_when_prefix_is_null() -> None:
    # Active config row exists but label_prefix is NULL -> fall back.
    conn, _ = make_conn([(None,)])
    assert active_label_prefix(conn) == "Cortex"


def test_active_label_prefix_preserves_empty_manage_all() -> None:
    conn, _ = make_conn([("",)])
    assert active_label_prefix(conn) == ""


# --- expected_cortex_labels ------------------------------------------------


def test_expected_labels_none_when_not_triaged() -> None:
    # Single fetchone -> no classification row -> None (noise).
    conn, cur = make_conn([None])
    assert expected_cortex_labels(conn, "gmail-1") is None
    assert cur.fetchone.call_count == 1  # prefix lookup skipped


def test_expected_labels_returns_managed_set() -> None:
    conn, _ = make_conn(
        [
            ({"add_label": ["Cortex/Work", "Personal/Notes"]},),  # classification
            ("Cortex",),  # active prefix
        ]
    )
    assert expected_cortex_labels(conn, "gmail-1") == {"Cortex/Work"}


def test_expected_labels_empty_set_when_action_null() -> None:
    conn, _ = make_conn([(None,), ("Cortex",)])
    assert expected_cortex_labels(conn, "gmail-1") == set()


def test_expected_labels_explicit_prefix_skips_config_query() -> None:
    # Only the classification is fetched; no active-config lookup.
    conn, cur = make_conn([({"add_label": ["Cortex/Work", "Personal/N"]},)])
    assert expected_cortex_labels(conn, "gmail-1", prefix="Cortex") == {"Cortex/Work"}
    assert cur.fetchone.call_count == 1


def test_expected_labels_parses_json_string_action() -> None:
    # action_taken arrives as a raw JSON string (adapter not registered).
    conn, _ = make_conn([('{"add_label": ["Cortex/Work"]}',)])
    assert expected_cortex_labels(conn, "gmail-1", prefix="Cortex") == {"Cortex/Work"}


def test_expected_labels_unparseable_action_is_empty() -> None:
    conn, _ = make_conn([("not json",)])
    assert expected_cortex_labels(conn, "gmail-1", prefix="Cortex") == set()


# --- record_learning_opportunity -------------------------------------------


def test_record_rejects_bad_direction() -> None:
    conn, _ = make_conn([])
    with pytest.raises(ValueError):
        record_learning_opportunity(conn, gmail_id="g", label="Cortex/X", direction="sideways")


def test_record_returns_true_on_insert() -> None:
    conn, _ = make_conn([(42,)])
    inserted = record_learning_opportunity(
        conn,
        gmail_id="g",
        label="Cortex/X",
        direction=ADD,
        sender="bob@example.com",
        expected_labels={"Cortex/Y"},
    )
    assert inserted is True


def test_record_returns_false_on_conflict() -> None:
    # ON CONFLICT DO NOTHING -> no RETURNING row -> False (deduped).
    conn, _ = make_conn([None])
    inserted = record_learning_opportunity(conn, gmail_id="g", label="Cortex/X", direction=REMOVE)
    assert inserted is False


def test_record_does_not_commit() -> None:
    # Participates in caller's transaction.
    conn, _ = make_conn([(1,)])
    record_learning_opportunity(conn, gmail_id="g", label="Cortex/X", direction=ADD)
    conn.commit.assert_not_called()


def test_ensure_schema_does_not_commit() -> None:
    # Leaves transaction management to the caller.
    conn, cur = make_conn([])
    ensure_learning_schema(conn)
    assert cur.execute.call_count == 2  # CREATE TABLE + CREATE INDEX
    conn.commit.assert_not_called()
