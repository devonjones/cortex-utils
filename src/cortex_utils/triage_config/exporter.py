"""Config exporter - YAML export for triage config database.

Exports triage configuration from Postgres to YAML format.
Used by both triage workers (for loading config) and gateway (for API export).
"""

import logging
from typing import Any

import psycopg2
import yaml

from cortex_utils.triage_config.importer import ConfigLoadError, load_rules_from_string
from cortex_utils.triage_config.linked_list import traverse_chain
from cortex_utils.triage_config.models import RulesConfig

logger = logging.getLogger(__name__)


def export_config_to_yaml(conn: psycopg2.extensions.connection, version: int | None = None) -> str:
    """Export triage config from database to YAML string.

    Args:
        conn: Database connection
        version: Config version to export (default: active version)

    Returns:
        YAML string representation of config

    Raises:
        ValueError: If config version not found
    """
    cursor = conn.cursor()

    try:
        # Get config version
        if version is None:
            cursor.execute("SELECT version FROM triage_config_versions WHERE is_active = TRUE")
        else:
            cursor.execute(
                "SELECT version FROM triage_config_versions WHERE version = %s",
                (version,),
            )

        row = cursor.fetchone()
        if not row:
            if version is None:
                raise ValueError("No active config version found")
            else:
                raise ValueError(f"Config version {version} not found")

        version_num: int = row[0]

        # Get version metadata and JSONB fields
        cursor.execute(
            """
            SELECT
                label_prefix,
                intents,
                email_categories,
                prompts,
                body_extraction_prompts
            FROM triage_config_versions
            WHERE version = %s
            """,
            (version_num,),
        )
        result = cursor.fetchone()
        assert result is not None

        (
            label_prefix,
            intents_json,
            email_categories_json,
            prompts_json,
            body_extraction_prompts_json,
        ) = result

        # Build chains dict by traversing linked lists
        chains: dict[str, list[dict[str, Any]]] = {}

        # Get all chains for this version
        cursor.execute(
            """
            SELECT id, chain_name
            FROM triage_chains
            WHERE config_version = %s
            ORDER BY display_order, chain_name
            """,
            (version_num,),
        )

        for chain_id, chain_name in cursor.fetchall():
            # Traverse rules in order
            rules_data = traverse_chain(cursor, chain_id)

            # Convert to dict format for YAML
            chain_rules = []
            for rule in rules_data:
                rule_dict: dict[str, Any] = {"match": rule["match_condition"]}

                # Add optional fields if present
                if rule["variables"]:
                    rule_dict["variables"] = rule["variables"]
                if rule["action"]:
                    rule_dict["action"] = rule["action"]
                if rule["jump_to_chain"]:
                    rule_dict["jump"] = rule["jump_to_chain"]
                if rule["return_to_parent"]:
                    rule_dict["return_to_parent"] = True
                if rule["llm_config"]:
                    rule_dict["llm"] = rule["llm_config"]
                if rule["routes"]:
                    rule_dict["routes"] = rule["routes"]

                chain_rules.append(rule_dict)

            chains[chain_name] = chain_rules

        # Fetch email mappings from global table (not versioned)
        cursor.execute(
            """
            SELECT email_address, label, archive, mark_read
            FROM triage_email_mappings
            WHERE mapping_type = 'priority'
            AND deleted_at IS NULL
            ORDER BY email_address
            """
        )
        priority_mappings = {
            email: {
                "label": label,
                "archive": archive if archive is not None else False,
                "mark_read": mark_read if mark_read is not None else False,
            }
            for email, label, archive, mark_read in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT email_address, label, archive, mark_read
            FROM triage_email_mappings
            WHERE mapping_type = 'fallback'
            AND deleted_at IS NULL
            ORDER BY email_address
            """
        )
        fallback_mappings = {
            email: {
                "label": label,
                "archive": archive if archive is not None else False,
                "mark_read": mark_read if mark_read is not None else False,
            }
            for email, label, archive, mark_read in cursor.fetchall()
        }

        # Construct config dict
        config_dict = {
            "version": version_num,
            "label_prefix": label_prefix,
            "intents": intents_json,
            "email_categories": email_categories_json,
            "prompts": prompts_json,
            "body_extraction_prompts": body_extraction_prompts_json,
            "chains": chains,
            "priority_email_mappings": priority_mappings,
            "fallback_email_mappings": fallback_mappings,
        }

        # Convert to YAML
        return yaml.dump(config_dict, default_flow_style=False, sort_keys=False)

    finally:
        cursor.close()


def load_config_from_db(
    conn: psycopg2.extensions.connection, version: int | None = None
) -> RulesConfig:
    """Load triage config from database and convert to RulesConfig.

    Args:
        conn: Database connection
        version: Config version to load (default: active version)

    Returns:
        RulesConfig object

    Raises:
        ConfigLoadError: If config cannot be loaded
    """
    try:
        # Export DB to YAML, then parse with existing loader
        yaml_content = export_config_to_yaml(conn, version)
        return load_rules_from_string(yaml_content)
    except Exception as e:
        raise ConfigLoadError(f"Failed to load config from database: {e}") from e
