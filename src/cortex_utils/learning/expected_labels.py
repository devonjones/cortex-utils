"""Expected Cortex labels for an email, derived from its triage classification.

Shared logic for the teach-by-labeling flow (cortex-uo9b): the authoritative
set of managed-prefix labels triage decided an email should have, so a manual
Gmail label change can be compared against it. Lives in cortex-utils because
both postmark (inline divergence detection) and the rule-proposal step need it,
and neither service may import the other.
"""

from __future__ import annotations

from typing import Any

import psycopg2

DEFAULT_LABEL_PREFIX = "Cortex"


def expected_cortex_labels(conn: psycopg2.extensions.connection, gmail_id: str) -> set[str] | None:
    """Return the managed-prefix labels triage assigned to ``gmail_id``.

    Returns ``None`` when the email has no classification yet (not triaged) —
    callers treat that as noise, not a teaching signal. Returns a set of full
    label names (e.g. ``{"Cortex/Work"}``, possibly empty) when a
    classification exists.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT action_taken FROM classifications WHERE gmail_id = %s",
            (gmail_id,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    action = row[0] or {}
    return managed_labels(action, active_label_prefix(conn))


def active_label_prefix(conn: psycopg2.extensions.connection) -> str:
    """Managed label prefix from the active triage config.

    Empty string means manage all user labels; a non-empty value (e.g.
    ``"Cortex"``) means only labels under ``{prefix}/``. Defaults to
    ``"Cortex"`` when no active config exists. Mirrors the labeling worker.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT label_prefix FROM triage_config_versions WHERE is_active = TRUE LIMIT 1"
        )
        row = cur.fetchone()
    # Preserve an empty prefix ("" = manage all); fall back only when the
    # row is absent or the column is NULL.
    if row and row[0] is not None:
        return row[0]
    return DEFAULT_LABEL_PREFIX


def managed_labels(action: dict[str, Any], prefix: str) -> set[str]:
    """Extract labels under ``prefix`` from a classification ``action_taken``.

    Mirrors the labeling worker's target-label logic: an empty prefix manages
    all labels; otherwise only names under ``{prefix}/`` count.
    """
    names: list[str] = []

    single = action.get("label")
    if isinstance(single, str):
        names.append(single)

    add = action.get("add_label")
    if isinstance(add, str):
        names.append(add)
    elif isinstance(add, list):
        names.extend(name for name in add if isinstance(name, str))

    if not prefix:
        return set(names)
    marker = f"{prefix}/"
    return {name for name in names if name.startswith(marker)}
