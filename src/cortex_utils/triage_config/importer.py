"""Config importer - YAML import and validation for triage config database."""

import hashlib
import json
import logging
import re

import psycopg2
import yaml

from cortex_utils.triage_config.models import EmailMappingAction, RulesConfig

logger = logging.getLogger(__name__)

# Template pattern for variable extraction
TEMPLATE_PATTERN = re.compile(r"\{(\w+)\}")

# Category descriptions for prompt generation
CATEGORY_DESCRIPTIONS = {
    "automated_noise": "GitHub notifications, service alerts, social media, marketing",
    "human_request": "Personal emails requiring response (friends, family, colleagues)",
    "action_item": "Has deadline (invoices, permission slips, renewals, bills)",
    "wrong_email": "Misdirected mail",
    "subscription": "Newsletters, marketing, recurring service emails",
    "school": "Kids' school communications",
}

# Built-in intents with default prompts
# These can be overridden or extended by defining intents in rules.yaml
BUILTIN_INTENTS = {
    "archive_request": {
        "prompt": """Does this email subject indicate the sender wants to save,
archive, or bookmark something for later?
Subject: "{subject}"
Answer only: yes or no""",
        "model": "qwen2.5:0.5b",
    },
    "todo_request": {
        "prompt": """Does this email subject indicate a task, reminder, or todo item?
Subject: "{subject}"
Answer only: yes or no""",
        "model": "qwen2.5:0.5b",
    },
    "question": {
        "prompt": """Does this email subject contain a question requiring a response?
Subject: "{subject}"
Answer only: yes or no""",
        "model": "qwen2.5:0.5b",
    },
}


class ConfigLoadError(Exception):
    """Raised when config cannot be loaded from database."""

    pass


class ConfigImportError(Exception):
    """Raised when YAML import fails."""

    pass


def _load_email_mappings(
    mappings: dict[str, dict], section_name: str
) -> dict[str, EmailMappingAction]:
    """Load and normalize email mappings from config.

    Args:
        mappings: Raw mappings dict from YAML (email -> action config)
        section_name: Name of the section (for error messages)

    Returns:
        Dict with lowercase email keys and validated EmailMappingAction values

    Raises:
        ValueError: If email address is empty or action config is invalid
    """
    normalized = {}
    for email, action_config in mappings.items():
        if not email or not email.strip():
            raise ValueError(f"{section_name}: Empty email address not allowed")

        normalized_email = email.lower().strip()
        try:
            action = EmailMappingAction.model_validate(action_config)
            normalized[normalized_email] = action
        except Exception as e:
            raise ValueError(f"{section_name}: Invalid action config for '{email}': {e}") from e

    if normalized:
        logger.info(f"Loaded {len(normalized)} mappings from {section_name}")

    return normalized


def _validate_no_duplicate_mappings(
    priority_mappings: dict[str, EmailMappingAction],
    fallback_mappings: dict[str, EmailMappingAction],
) -> None:
    """Validate no duplicate email addresses across priority and fallback sections.

    Args:
        priority_mappings: Priority email mappings
        fallback_mappings: Fallback email mappings

    Raises:
        ValueError: If duplicate email addresses found
    """
    duplicates = set(priority_mappings.keys()) & set(fallback_mappings.keys())
    if duplicates:
        raise ValueError(
            f"Duplicate email addresses found in both priority_email_mappings "
            f"and fallback_email_mappings: {duplicates}"
        )


def load_rules_from_string(yaml_content: str) -> RulesConfig:
    """Load rules from a YAML string.

    Args:
        yaml_content: YAML content as string.

    Returns:
        Parsed RulesConfig object.

    Raises:
        ValueError: If the YAML is invalid.
    """
    # Import here to avoid circular dependency with models
    from cortex_utils.triage_config.models import BUILTIN_PROMPTS_DATA

    data = yaml.safe_load(yaml_content)

    if data is None:
        raise ValueError("Empty YAML content")

    # Merge built-in intents with user-defined ones
    # User intents override built-ins
    intents = dict(BUILTIN_INTENTS)
    user_intents = data.get("intents") or {}
    for name, config in user_intents.items():
        if name in intents:
            # Merge: user config overrides built-in fields
            intents[name] = {**intents[name], **config}
        else:
            intents[name] = config
    data["intents"] = intents

    # Merge built-in prompts with user-defined ones
    # User prompts override built-ins
    prompts = {k: dict(v) for k, v in BUILTIN_PROMPTS_DATA.items()}
    user_prompts = data.get("prompts") or {}
    for version, config in user_prompts.items():
        if version in prompts:
            prompts[version] = {**prompts[version], **config}
        else:
            prompts[version] = config
    data["prompts"] = prompts

    # Load email mappings with normalized lowercase keys
    priority_mappings = _load_email_mappings(
        data.get("priority_email_mappings") or {}, "priority_email_mappings"
    )
    fallback_mappings = _load_email_mappings(
        data.get("fallback_email_mappings") or {}, "fallback_email_mappings"
    )

    # Validate no duplicate keys across both sections
    _validate_no_duplicate_mappings(priority_mappings, fallback_mappings)

    data["priority_email_mappings"] = priority_mappings
    data["fallback_email_mappings"] = fallback_mappings

    return RulesConfig.model_validate(data)


def validate_rules(config: RulesConfig) -> list[str]:
    """Validate rules configuration.

    Returns a list of validation errors (empty if valid).
    """
    errors: list[str] = []

    # Must have a 'main' chain
    if "main" not in config.chains:
        errors.append("Rules must have a 'main' chain")

    # Validate jump targets and intent references
    # Note: Rule model_validator handles action/jump/llm exclusivity and llm+routes
    for chain_name, rules in config.chains.items():
        for i, rule in enumerate(rules):
            if rule.jump and rule.jump not in config.chains:
                errors.append(
                    f"Chain '{chain_name}' rule {i}: jump target '{rule.jump}' does not exist"
                )

            # Validate intent references
            intent = rule.match.subject_intent
            if isinstance(intent, str) and intent not in config.intents:
                errors.append(f"Chain '{chain_name}' rule {i}: unknown intent '{intent}'")

            # Validate regex patterns in match conditions
            if rule.match.subject_regex:
                patterns = (
                    [rule.match.subject_regex]
                    if isinstance(rule.match.subject_regex, str)
                    else rule.match.subject_regex
                )
                for pattern in patterns:
                    try:
                        re.compile(pattern)
                    except re.error as e:
                        errors.append(
                            f"Chain '{chain_name}' rule {i}: invalid subject_regex '{pattern}': {e}"
                        )

            # Validate variables
            defined_vars: set[str] = set()
            if rule.variables:
                for var_name, var in rule.variables.items():
                    # Variable name must be a valid identifier
                    if not var_name.isidentifier():
                        errors.append(
                            f"Chain '{chain_name}' rule {i}: "
                            f"invalid variable name '{var_name}' "
                            "(must be valid identifier)"
                        )

                    defined_vars.add(var_name)

                    # Validate regex patterns in variable extractors
                    if var.header_regex:
                        try:
                            re.compile(var.header_regex.pattern)
                        except re.error as e:
                            errors.append(
                                f"Chain '{chain_name}' rule {i}: "
                                f"invalid header_regex pattern for '{var_name}': {e}"
                            )
                    if var.subject_regex:
                        try:
                            re.compile(var.subject_regex.pattern)
                        except re.error as e:
                            errors.append(
                                f"Chain '{chain_name}' rule {i}: "
                                f"invalid subject_regex pattern for '{var_name}': {e}"
                            )

            # Add LLM extract fields to defined vars
            if rule.llm and rule.llm.extract:
                for field in rule.llm.extract:
                    defined_vars.add(field)

            # Helper to find undefined variables in a label template
            def get_undefined_vars(label: str, defined: set[str] = defined_vars) -> set[str]:
                return set(TEMPLATE_PATTERN.findall(label)) - defined

            # Validate label templates reference only defined variables
            if (
                rule.action
                and rule.action.label
                and (undefined := get_undefined_vars(rule.action.label))
            ):
                errors.append(
                    f"Chain '{chain_name}' rule {i}: "
                    f"label template references undefined variables: {undefined}"
                )

            # Validate route action label templates
            if rule.routes:
                for route_key, route_action in rule.routes.items():
                    if route_action.label and (undefined := get_undefined_vars(route_action.label)):
                        errors.append(
                            f"Chain '{chain_name}' rule {i}: "
                            f"route '{route_key}' label template references "
                            f"undefined variables: {undefined}"
                        )

    return errors


def import_yaml_to_db(
    conn: psycopg2.extensions.connection,
    yaml_content: str,
    created_by: str,
    notes: str | None = None,
) -> int:
    """Import YAML config to database, creating new version.

    Args:
        conn: Database connection
        yaml_content: YAML config content
        created_by: User/system creating this version
        notes: Optional notes about this version

    Returns:
        New version number

    Raises:
        ConfigImportError: If import fails
    """
    cursor = conn.cursor()

    try:
        # 1. Parse and validate YAML
        config = load_rules_from_string(yaml_content)
        errors = validate_rules(config)
        if errors:
            raise ConfigImportError(f"Invalid config: {errors}")

        # 2. Calculate hash for dedup
        config_hash = hashlib.sha256(yaml_content.encode()).hexdigest()

        # 3. Check for duplicate
        cursor.execute(
            "SELECT version FROM triage_config_versions WHERE config_hash = %s",
            (config_hash,),
        )
        existing = cursor.fetchone()
        if existing:
            version_num: int = existing[0]
            logger.info(
                f"Config already exists as version {version_num} (hash: {config_hash[:8]}...)"
            )
            return version_num

        # 4. Create new version (transaction)
        # Serialize Pydantic models to dicts
        intents_dict = {
            k: v.model_dump() if hasattr(v, "model_dump") else v for k, v in config.intents.items()
        }
        email_categories_dict = {
            k: v.model_dump() if hasattr(v, "model_dump") else v
            for k, v in config.email_categories.items()
        }
        prompts_dict = {
            k: v.model_dump() if hasattr(v, "model_dump") else v for k, v in config.prompts.items()
        }
        body_extraction_prompts_dict = {
            k: v.model_dump() if hasattr(v, "model_dump") else v
            for k, v in config.body_extraction_prompts.items()
        }

        cursor.execute(
            """
            INSERT INTO triage_config_versions (
                created_by,
                notes,
                is_active,
                config_hash,
                label_prefix,
                intents,
                email_categories,
                prompts,
                body_extraction_prompts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING version
            """,
            (
                created_by,
                notes,
                True,  # Auto-deactivates old version via trigger
                config_hash,
                config.label_prefix,
                json.dumps(intents_dict),
                json.dumps(email_categories_dict),
                json.dumps(prompts_dict),
                json.dumps(body_extraction_prompts_dict),
            ),
        )
        result = cursor.fetchone()
        assert result is not None
        new_version: int = result[0]

        # 5. Insert chains and rules with linked list
        for chain_name, rules in config.chains.items():
            cursor.execute(
                """
                INSERT INTO triage_chains (config_version, chain_name)
                VALUES (%s, %s)
                RETURNING id
                """,
                (new_version, chain_name),
            )
            chain_result = cursor.fetchone()
            assert chain_result is not None
            chain_id: int = chain_result[0]

            prev_rule_id = None
            for rule in rules:
                # Extract rule fields
                match_condition = rule.match.model_dump() if rule.match else {}
                action = rule.action.model_dump() if rule.action else None
                jump_to_chain = rule.jump
                return_to_parent = rule.return_to_parent
                # Serialize variables dict (each value is a Variable Pydantic model)
                variables = (
                    {k: v.model_dump() for k, v in rule.variables.items()}
                    if rule.variables
                    else None
                )
                llm_config = rule.llm.model_dump() if rule.llm else None
                routes = rule.routes if rule.routes else None

                cursor.execute(
                    """
                    INSERT INTO triage_rules (
                        chain_id,
                        config_version,
                        prev_rule_id,
                        match_condition,
                        action,
                        jump_to_chain,
                        return_to_parent,
                        variables,
                        llm_config,
                        routes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        chain_id,
                        new_version,
                        prev_rule_id,
                        json.dumps(match_condition),
                        json.dumps(action) if action else None,
                        jump_to_chain,
                        return_to_parent,
                        json.dumps(variables) if variables else None,
                        json.dumps(llm_config) if llm_config else None,
                        json.dumps(routes) if routes else None,
                    ),
                )
                rule_result = cursor.fetchone()
                assert rule_result is not None
                rule_id: int = rule_result[0]

                # Update previous rule's next pointer
                if prev_rule_id:
                    cursor.execute(
                        "UPDATE triage_rules SET next_rule_id = %s WHERE id = %s",
                        (rule_id, prev_rule_id),
                    )

                prev_rule_id = rule_id

        # 6. Insert email mappings
        for email, action_config in config.priority_email_mappings.items():
            cursor.execute(
                """
                INSERT INTO triage_email_mappings (
                    config_version,
                    mapping_type,
                    email_address,
                    label,
                    archive,
                    mark_read
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    new_version,
                    "priority",
                    email.lower(),
                    action_config.label,
                    action_config.archive,
                    action_config.mark_read,
                ),
            )

        for email, action_config in config.fallback_email_mappings.items():
            cursor.execute(
                """
                INSERT INTO triage_email_mappings (
                    config_version,
                    mapping_type,
                    email_address,
                    label,
                    archive,
                    mark_read
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    new_version,
                    "fallback",
                    email.lower(),
                    action_config.label,
                    action_config.archive,
                    action_config.mark_read,
                ),
            )

        conn.commit()
        logger.info(f"Imported config as version {new_version}")
        return new_version

    except Exception as e:
        conn.rollback()
        raise ConfigImportError(f"Failed to import config: {e}") from e
    finally:
        cursor.close()
