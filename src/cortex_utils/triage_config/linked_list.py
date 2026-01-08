"""Linked list operations for triage rules.

Rules are stored as doubly-linked lists (prev_rule_id/next_rule_id pointers)
for O(1) insertion and deletion at any position.
"""

import logging
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)


class LinkedListError(Exception):
    """Raised when linked list operation fails."""

    pass


def traverse_chain(cursor: psycopg2.extensions.cursor, chain_id: int) -> list[dict[str, Any]]:
    """Traverse linked list to get rules in order.

    Args:
        cursor: Database cursor
        chain_id: Chain ID to traverse

    Returns:
        List of rule dicts in order (position 0 = head)

    Uses recursive CTE to follow next_rule_id pointers from head to tail.
    """
    cursor.execute(
        """
        WITH RECURSIVE chain_walk AS (
            -- Base: find head (prev_rule_id IS NULL)
            SELECT
                r.*,
                0 AS position
            FROM triage_rules r
            WHERE r.chain_id = %s AND r.prev_rule_id IS NULL

            UNION ALL

            -- Recursive: follow next_rule_id
            SELECT
                r.*,
                cw.position + 1
            FROM triage_rules r
            JOIN chain_walk cw ON r.id = cw.next_rule_id
        )
        SELECT
            id,
            chain_id,
            config_version,
            prev_rule_id,
            next_rule_id,
            match_condition,
            variables,
            action,
            jump_to_chain,
            return_to_parent,
            llm_config,
            routes,
            rule_name,
            description,
            row_version,
            position
        FROM chain_walk
        ORDER BY position
        """,
        (chain_id,),
    )

    rules = []
    for row in cursor.fetchall():
        (
            rule_id,
            chain_id,
            config_version,
            prev_rule_id,
            next_rule_id,
            match_condition,
            variables,
            action,
            jump_to_chain,
            return_to_parent,
            llm_config,
            routes,
            rule_name,
            description,
            row_version,
            position,
        ) = row

        rules.append(
            {
                "id": rule_id,
                "chain_id": chain_id,
                "config_version": config_version,
                "prev_rule_id": prev_rule_id,
                "next_rule_id": next_rule_id,
                "match_condition": match_condition,
                "variables": variables,
                "action": action,
                "jump_to_chain": jump_to_chain,
                "return_to_parent": return_to_parent,
                "llm_config": llm_config,
                "routes": routes,
                "rule_name": rule_name,
                "description": description,
                "row_version": row_version,
                "position": position,
            }
        )

    return rules


def insert_rule_after(
    conn: psycopg2.extensions.connection,
    chain_id: int,
    after_rule_id: int | None,
    rule_data: dict[str, Any],
) -> int:
    """Insert a new rule after the specified position.

    Args:
        conn: Database connection
        chain_id: Chain ID to insert into
        after_rule_id: Rule ID to insert after (None = insert at head)
        rule_data: Rule content (match_condition, action, etc.)

    Returns:
        New rule ID

    Uses pessimistic locking (SELECT FOR UPDATE) to prevent concurrent modifications.
    Only locks the specific rules being modified (prev/next), not the entire chain,
    allowing concurrent edits to different parts of the same chain.
    """
    cursor = conn.cursor()

    try:
        # Get config_version from chain
        cursor.execute(
            "SELECT config_version FROM triage_chains WHERE id = %s",
            (chain_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise LinkedListError(f"Chain {chain_id} not found")
        config_version = row[0]

        if after_rule_id is None:
            # Insert at head: lock current head only
            cursor.execute(
                """SELECT id FROM triage_rules
                   WHERE chain_id = %s AND prev_rule_id IS NULL
                   FOR UPDATE""",
                (chain_id,),
            )
            current_head = cursor.fetchone()
            next_rule_id = current_head[0] if current_head else None
            prev_rule_id = None
        else:
            # Insert after specified rule: get next_rule_id first
            cursor.execute(
                "SELECT next_rule_id FROM triage_rules WHERE id = %s",
                (after_rule_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise LinkedListError(f"Rule {after_rule_id} not found")
            next_rule_id = row[0]
            prev_rule_id = after_rule_id

            # Lock both prev and next rules in single query to prevent deadlocks
            ids_to_lock = [
                rule_id for rule_id in (prev_rule_id, next_rule_id) if rule_id is not None
            ]
            if ids_to_lock:
                cursor.execute(
                    "SELECT id FROM triage_rules WHERE id = ANY(%s) FOR UPDATE",
                    (ids_to_lock,),
                )

        # Insert new rule
        cursor.execute(
            """
            INSERT INTO triage_rules (
                chain_id,
                config_version,
                prev_rule_id,
                next_rule_id,
                match_condition,
                variables,
                action,
                jump_to_chain,
                return_to_parent,
                llm_config,
                routes,
                rule_name,
                description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                chain_id,
                config_version,
                prev_rule_id,
                next_rule_id,
                rule_data.get("match_condition"),
                rule_data.get("variables"),
                rule_data.get("action"),
                rule_data.get("jump_to_chain"),
                rule_data.get("return_to_parent", False),
                rule_data.get("llm_config"),
                rule_data.get("routes"),
                rule_data.get("rule_name"),
                rule_data.get("description"),
            ),
        )
        result = cursor.fetchone()
        assert result is not None
        new_rule_id: int = result[0]

        # Update pointers
        if prev_rule_id:
            cursor.execute(
                "UPDATE triage_rules SET next_rule_id = %s WHERE id = %s",
                (new_rule_id, prev_rule_id),
            )

        if next_rule_id:
            cursor.execute(
                "UPDATE triage_rules SET prev_rule_id = %s WHERE id = %s",
                (new_rule_id, next_rule_id),
            )

        conn.commit()
        logger.info(f"Inserted rule {new_rule_id} into chain {chain_id} after {after_rule_id}")
        return new_rule_id

    except Exception as e:
        conn.rollback()
        raise LinkedListError(f"Failed to insert rule: {e}") from e
    finally:
        cursor.close()


def delete_rule(conn: psycopg2.extensions.connection, rule_id: int) -> None:
    """Delete a rule from the linked list, reconnecting prev/next pointers.

    Args:
        conn: Database connection
        rule_id: Rule ID to delete

    Deletes the rule and reconnects the linked list by updating:
    - prev_rule.next_rule_id = this_rule.next_rule_id
    - next_rule.prev_rule_id = this_rule.prev_rule_id

    Only locks the specific rule being deleted and its adjacent rules,
    not the entire chain, allowing concurrent edits to other parts of the chain.
    """
    cursor = conn.cursor()

    try:
        # Get rule details and lock the rule being deleted
        cursor.execute(
            """SELECT chain_id, prev_rule_id, next_rule_id
               FROM triage_rules WHERE id = %s FOR UPDATE""",
            (rule_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise LinkedListError(f"Rule {rule_id} not found")

        chain_id, prev_rule_id, next_rule_id = row

        # Lock adjacent rules in single query to prevent deadlocks
        ids_to_lock = [rule_id for rule_id in (prev_rule_id, next_rule_id) if rule_id is not None]
        if ids_to_lock:
            cursor.execute(
                "SELECT id FROM triage_rules WHERE id = ANY(%s) FOR UPDATE",
                (ids_to_lock,),
            )

        # Update prev → next pointer
        if prev_rule_id:
            cursor.execute(
                "UPDATE triage_rules SET next_rule_id = %s WHERE id = %s",
                (next_rule_id, prev_rule_id),
            )

        # Update next → prev pointer
        if next_rule_id:
            cursor.execute(
                "UPDATE triage_rules SET prev_rule_id = %s WHERE id = %s",
                (prev_rule_id, next_rule_id),
            )

        # Delete rule
        cursor.execute("DELETE FROM triage_rules WHERE id = %s", (rule_id,))

        conn.commit()
        logger.info(f"Deleted rule {rule_id} from chain {chain_id}")

    except Exception as e:
        conn.rollback()
        raise LinkedListError(f"Failed to delete rule: {e}") from e
    finally:
        cursor.close()


def move_rule(
    conn: psycopg2.extensions.connection, rule_id: int, after_rule_id: int | None
) -> None:
    """Move a rule to a new position in the chain.

    Args:
        conn: Database connection
        rule_id: Rule ID to move
        after_rule_id: Rule ID to move after (None = move to head)

    Implemented as: delete from current position, insert at new position.
    """
    cursor = conn.cursor()

    try:
        # Get rule data
        cursor.execute(
            """
            SELECT
                chain_id,
                match_condition,
                variables,
                action,
                jump_to_chain,
                return_to_parent,
                llm_config,
                routes,
                rule_name,
                description
            FROM triage_rules
            WHERE id = %s
            """,
            (rule_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise LinkedListError(f"Rule {rule_id} not found")

        (
            chain_id,
            match_condition,
            variables,
            action,
            jump_to_chain,
            return_to_parent,
            llm_config,
            routes,
            rule_name,
            description,
        ) = row

        rule_data = {
            "match_condition": match_condition,
            "variables": variables,
            "action": action,
            "jump_to_chain": jump_to_chain,
            "return_to_parent": return_to_parent,
            "llm_config": llm_config,
            "routes": routes,
            "rule_name": rule_name,
            "description": description,
        }

        # Delete from current position
        delete_rule(conn, rule_id)

        # Insert at new position
        insert_rule_after(conn, chain_id, after_rule_id, rule_data)

        logger.info(f"Moved rule {rule_id} after {after_rule_id}")

    except Exception as e:
        raise LinkedListError(f"Failed to move rule: {e}") from e
    finally:
        cursor.close()


def update_rule_content(
    conn: psycopg2.extensions.connection,
    rule_id: int,
    rule_data: dict[str, Any],
    expected_version: int | None = None,
) -> None:
    """Update rule content with optimistic locking.

    Args:
        conn: Database connection
        rule_id: Rule ID to update
        rule_data: New rule content
        expected_version: Expected row_version for optimistic locking (optional)

    Raises:
        LinkedListError: If optimistic lock fails (version mismatch)
    """
    cursor = conn.cursor()

    try:
        # Build UPDATE statement
        update_fields = []
        values = []

        for field in [
            "match_condition",
            "variables",
            "action",
            "jump_to_chain",
            "return_to_parent",
            "llm_config",
            "routes",
            "rule_name",
            "description",
        ]:
            if field in rule_data:
                update_fields.append(f"{field} = %s")
                values.append(rule_data[field])

        if not update_fields:
            return  # Nothing to update

        # Add WHERE clause
        where_clause = "id = %s"
        values.append(rule_id)

        if expected_version is not None:
            where_clause += " AND row_version = %s"
            values.append(expected_version)

        # Execute update
        cursor.execute(
            f"UPDATE triage_rules SET {', '.join(update_fields)} WHERE {where_clause}",
            values,
        )

        if cursor.rowcount == 0:
            if expected_version is not None:
                raise LinkedListError(
                    f"Optimistic lock failed: rule {rule_id} version mismatch "
                    f"(expected {expected_version})"
                )
            else:
                raise LinkedListError(f"Rule {rule_id} not found")

        conn.commit()
        logger.info(f"Updated rule {rule_id}")

    except Exception as e:
        conn.rollback()
        raise LinkedListError(f"Failed to update rule: {e}") from e
    finally:
        cursor.close()
