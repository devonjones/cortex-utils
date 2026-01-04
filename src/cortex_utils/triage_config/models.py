"""Data models for triage rules."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class IntentConfig(BaseModel):
    """Configuration for a named intent.

    Intents are boolean classifiers that answer yes/no questions.
    Used with subject_intent (subject only) or email_intent (full email).
    """

    prompt: str
    model: str = "qwen2.5:0.5b"


class EmailCategoryConfig(BaseModel):
    """Configuration for email category classification.

    Categories are multi-class classifiers that return one of several
    predefined categories. The prompt should instruct the LLM to return
    exactly one of the category names.
    """

    prompt: str
    model: str = "qwen2.5:0.5b"
    categories: list[str]

    @field_validator("categories")
    @classmethod
    def categories_not_empty(cls, v: list[str]) -> list[str]:
        """Ensure categories list is not empty."""
        if not v:
            raise ValueError("categories list must not be empty")
        return v


# Variable extraction models for dynamic labels


class HeaderRegexVariable(BaseModel):
    """Extract variable from header using regex capture group.

    Example:
        header_regex:
          header: "list-id"
          pattern: "^([^<\\s]+)"  # Captures "owner/repo" from GitHub List-ID
    """

    header: str  # Header name (case-insensitive)
    pattern: str  # Regex with capture group
    group: int = Field(1, gt=0)  # Which capture group to use (default: first)


class SubjectRegexVariable(BaseModel):
    """Extract variable from subject using regex capture group.

    Example:
        subject_regex:
          pattern: "#(\\d{3}-\\d{7}-\\d{7})"  # Captures Amazon order ID
    """

    pattern: str  # Regex with capture group
    group: int = Field(1, gt=0)  # Which capture group to use (default: first)


class BodyRegexVariable(BaseModel):
    """Extract variable from email body using regex capture group.

    Example:
        body_regex:
          pattern: "Order #(\\d+)"  # Captures order number from body
    """

    pattern: str  # Regex with capture group
    group: int = Field(1, gt=0)  # Which capture group to use (default: first)


class FromRegexVariable(BaseModel):
    """Extract variable from sender address using regex capture group.

    Example:
        from_regex:
          pattern: "([^@]+)@"  # Captures username from email address
    """

    pattern: str  # Regex with capture group
    group: int = Field(1, gt=0)  # Which capture group to use (default: first)


class ToRegexVariable(BaseModel):
    """Extract variable from first matching to_addr using regex capture group.

    Example:
        to_regex:
          pattern: "([^+]+)\\+"  # Captures the local part before + extension
    """

    pattern: str  # Regex with capture group
    group: int = Field(1, gt=0)  # Which capture group to use (default: first)


class CcRegexVariable(BaseModel):
    """Extract variable from first matching cc_addr using regex capture group.

    Example:
        cc_regex:
          pattern: "([^@]+)@"  # Captures username from CC address
    """

    pattern: str  # Regex with capture group
    group: int = Field(1, gt=0)  # Which capture group to use (default: first)


class AttachmentFilenameRegexVariable(BaseModel):
    """Extract variable from first matching attachment filename using regex.

    Example:
        attachment_filename_regex:
          pattern: "invoice_(\\d+)\\.pdf"  # Captures invoice number
    """

    pattern: str  # Regex with capture group
    group: int = Field(1, gt=0)  # Which capture group to use (default: first)


class LLMVariable(BaseModel):
    """Extract variable using LLM.

    The prompt template can use {from_addr}, {subject}, {body_preview}.

    Example:
        llm:
          prompt: "Extract the project name. Return ONLY the name."
          model: "qwen2.5:0.5b"
    """

    prompt: str  # Prompt template
    model: str = "qwen2.5:0.5b"


class PatternFieldVariable(BaseModel):
    """Extract variable from pattern info.

    Requires email.pattern_info to be populated.

    Example:
        pattern_field:
          field: "merchant"  # Extract merchant name from pattern
    """

    field: Literal[
        "merchant", "sender", "interval_type", "status", "confidence"
    ]  # Field name from PatternInfo


class Variable(BaseModel):
    """A variable definition - exactly one source type.

    Variables are resolved after a rule matches and before the action is applied.
    If resolution fails, the rule is considered not matched and evaluation
    continues to the next rule.

    Example:
        variables:
          repo:
            header_regex:
              header: "list-id"
              pattern: "^([^<\\s]+)"
    """

    header_regex: HeaderRegexVariable | None = None
    subject_regex: SubjectRegexVariable | None = None
    body_regex: BodyRegexVariable | None = None
    from_regex: FromRegexVariable | None = None
    to_regex: ToRegexVariable | None = None
    cc_regex: CcRegexVariable | None = None
    attachment_filename_regex: AttachmentFilenameRegexVariable | None = None
    llm: LLMVariable | None = None
    pattern_field: PatternFieldVariable | None = None

    @model_validator(mode="after")
    def exactly_one_source(self) -> Variable:
        """Ensure exactly one extraction method is specified."""
        sources = sum(
            [
                self.header_regex is not None,
                self.subject_regex is not None,
                self.body_regex is not None,
                self.from_regex is not None,
                self.to_regex is not None,
                self.cc_regex is not None,
                self.attachment_filename_regex is not None,
                self.llm is not None,
                self.pattern_field is not None,
            ]
        )
        if sources != 1:
            raise ValueError(
                "Variable must have exactly one of: header_regex, subject_regex, "
                "body_regex, from_regex, to_regex, cc_regex, "
                "attachment_filename_regex, llm, pattern_field"
            )
        return self


class MatchCondition(BaseModel):
    """Conditions for matching an email.

    All specified fields must match (implicit AND). Within a field, lists are
    OR (any value matches). Use any_of/all_of for compound conditions across
    different field types.

    Field lists (OR within field):
        # Any of these exact addresses
        from: ["newsletter@foo.com", "deals@bar.com"]

        # Any of these glob patterns
        from_glob: ["*@mailchimp.com", "*@sendgrid.net"]

    Compound conditions (OR/AND across fields):
        - any_of: At least one sub-condition must match (OR)
        - all_of: All sub-conditions must match (AND) - useful for grouping

    Examples:
        # Match emails from any newsletter domain (using list)
        match:
          from_glob: ["*@mailchimp.com", "*@sendgrid.net", "*@newsletter.com"]

        # Same thing using any_of (more verbose but allows mixing field types)
        match:
          any_of:
            - from_glob: "*@mailchimp.com"
            - from_glob: "*@sendgrid.net"
            - from_addr: "newsletter@example.com"

        # Match GitHub bugs (AND with OR)
        match:
          from_glob: "*@github.com"
          subject_contains: ["bug", "issue", "error"]
    """

    model_config = ConfigDict(populate_by_name=True)

    # From/To - Exact matches (single or list - any match)
    from_addr: list[str] | str | None = Field(None, alias="from")
    to_addr: list[str] | str | None = Field(None, alias="to")

    # From/To - Glob patterns (single or list - any match)
    from_glob: list[str] | str | None = None
    to_glob: list[str] | str | None = None

    # From/To - Substring matches (single or list - any match)
    from_contains: list[str] | str | None = None
    to_contains: list[str] | str | None = None

    # From/To - Regex (single or list - any match)
    from_regex: list[str] | str | None = None
    to_regex: list[str] | str | None = None

    # Subject - all match types
    subject: list[str] | str | None = None  # Exact match
    subject_glob: list[str] | str | None = None  # Glob pattern
    subject_contains: list[str] | str | None = None  # Substring
    subject_regex: list[str] | str | None = None  # Regex

    # Body - all match types
    body_glob: list[str] | str | None = None  # Glob pattern
    body_contains: list[str] | str | None = None  # Substring
    body_regex: list[str] | str | None = None  # Regex

    # Intent (LLM-based boolean classifiers)
    subject_intent: str | IntentConfig | None = None  # Subject only
    email_intent: str | IntentConfig | None = None  # Full email context

    # Category (LLM-based multi-class classifier)
    # Format: "category_name/expected_value" e.g., "transactional/invoice"
    email_category: str | None = None

    # Header matching (from emails_raw.headers)
    # Format: {"Header-Name": "pattern"} - all specified headers must match
    # Header names are case-insensitive. Values support glob patterns.
    # Example: header: {"List-ID": "*googlegroups*", "Reply-To": "*@noreply*"}
    header: dict[str, str] | None = None  # Glob pattern match
    header_contains: dict[str, str] | None = None  # Substring match
    header_regex: dict[str, str] | None = None  # Regex match
    header_exists: list[str] | str | None = None  # Check if header is present

    # Header name pattern matching (match header name pattern + value pattern)
    # Example: header_name_glob: {"X-*": "*spam*"} - any X-* header containing "spam"
    header_name_glob: dict[str, str] | None = None  # Glob both name and value
    header_name_regex: dict[str, str] | None = None  # Regex both name and value

    # Common header aliases (shortcuts for header/header_contains)
    list_id: list[str] | str | None = None  # Exact List-ID match
    list_id_glob: list[str] | str | None = None  # Glob pattern on List-ID
    list_id_contains: list[str] | str | None = None  # Substring in List-ID
    list_id_regex: list[str] | str | None = None  # Regex on List-ID
    reply_to: list[str] | str | None = None  # Exact Reply-To match
    reply_to_glob: list[str] | str | None = None  # Glob pattern on Reply-To
    reply_to_contains: list[str] | str | None = None  # Substring in Reply-To
    reply_to_regex: list[str] | str | None = None  # Regex on Reply-To

    # CC recipients (from emails_parsed.cc_addrs)
    cc: list[str] | str | None = None  # Exact CC match
    cc_glob: list[str] | str | None = None  # Glob pattern on CC
    cc_contains: list[str] | str | None = None  # Substring in CC
    cc_regex: list[str] | str | None = None  # Regex on CC

    # BCC recipients (from emails_parsed.bcc_addrs)
    bcc: list[str] | str | None = None  # Exact BCC match
    bcc_glob: list[str] | str | None = None  # Glob pattern on BCC
    bcc_contains: list[str] | str | None = None  # Substring in BCC
    bcc_regex: list[str] | str | None = None  # Regex on BCC

    # Delivered-To header (from headers)
    deliveredto: list[str] | str | None = None  # Exact Delivered-To match
    deliveredto_glob: list[str] | str | None = None  # Glob pattern
    deliveredto_contains: list[str] | str | None = None  # Substring
    deliveredto_regex: list[str] | str | None = None  # Regex

    # Status conditions (based on Gmail label_ids)
    is_read: bool | None = None  # True if UNREAD not in label_ids
    is_starred: bool | None = None  # True if STARRED in label_ids
    is_important: bool | None = None  # True if IMPORTANT in label_ids
    in_inbox: bool | None = None  # True if INBOX in label_ids
    has_label: list[str] | str | None = None  # Check for specific label(s)

    # Size conditions (from emails_raw.size_estimate)
    size_larger: int | None = None  # size_estimate > N bytes
    size_smaller: int | None = None  # size_estimate < N bytes

    # Date conditions (from emails_raw.internal_date, ms since epoch)
    # Absolute: ISO format string "2024-01-15" or timestamp in ms
    date_before: str | int | None = None  # internal_date < date
    date_after: str | int | None = None  # internal_date > date
    # Relative: duration strings like "7d", "2w", "1m", "1y"
    older_than: str | None = None  # internal_date < (now - duration)
    newer_than: str | None = None  # internal_date > (now - duration)

    # Attachment conditions
    has_attachment: bool | None = None
    # Filename matching (from attachments table)
    attachment_filename: list[str] | str | None = None  # Exact filename match
    attachment_filename_glob: list[str] | str | None = None  # Glob pattern
    attachment_filename_contains: list[str] | str | None = None  # Substring
    attachment_filename_regex: list[str] | str | None = None  # Regex
    # MIME type matching
    attachment_mime: list[str] | str | None = None  # Exact MIME type
    attachment_mime_glob: list[str] | str | None = None  # Glob pattern (e.g., "image/*")
    attachment_mime_contains: list[str] | str | None = None  # Substring
    # Convenience shortcuts
    has_calendar_invite: bool | None = None  # .ics or text/calendar
    has_pdf: bool | None = None  # application/pdf
    has_image: bool | None = None  # image/*
    has_spreadsheet: bool | None = None  # Excel/Sheets MIME types

    # Other
    body_is_mostly_links: bool | None = None  # True if body is mostly URLs

    # Pattern matching (subscription detection)
    # Requires email.pattern_info to be populated before matching
    matches_pattern: bool | None = None  # True if email matches any known pattern
    pattern_confidence_min: float | None = None  # Minimum pattern confidence (0.0-1.0)
    pattern_interval: list[str] | str | None = None  # monthly, weekly, yearly, etc.
    pattern_status: list[str] | str | None = None  # emerging, active, dormant, ended

    # Negation - inverts the entire condition result
    negate: bool = False  # If True, condition matches when it would normally NOT match

    # Compound conditions
    any_of: list[MatchCondition] | None = None
    all_of: list[MatchCondition] | None = None

    @field_validator("subject_intent", "email_intent", mode="before")
    @classmethod
    def validate_intent(cls, v: Any) -> Any:
        """Parse inline intent dict into an IntentConfig model."""
        if isinstance(v, dict):
            return IntentConfig.model_validate(v)
        return v

    @model_validator(mode="after")
    def validate_compound_not_empty(self) -> MatchCondition:
        """Ensure any_of and all_of have at least one condition if specified."""
        if self.any_of is not None and len(self.any_of) == 0:
            raise ValueError("any_of must contain at least one condition")
        if self.all_of is not None and len(self.all_of) == 0:
            raise ValueError("all_of must contain at least one condition")
        return self


class Action(BaseModel):
    """Action to take when a rule matches.

    Triage handles inbox management only. Workflow dispatch happens via the
    actions service which watches for Cortex/* label events.

    Labels:
    - label: Gets prefixed with label_prefix (e.g., "Todo" -> "Cortex/Todo")
    - add_label: Applied as-is, no prefix (e.g., "Work/Projects")
    - remove_label: Removed as-is, no prefix
    """

    label: str | None = None  # Auto-prefixed with label_prefix
    add_label: str | list[str] | None = None  # Raw label(s), no prefix
    remove_label: str | list[str] | None = None  # Raw label(s) to remove
    archive: bool | None = None
    mark_read: bool | None = None
    star: bool | None = None


class EmailMappingAction(BaseModel):
    """Action for email address to label mapping.

    Used for O(1) hash-based lookups (priority_email_mappings, fallback_email_mappings).

    Three-state semantics for optional fields:
    - archive: true = archive if not already archived
    - archive: false = unarchive if archived (rarely needed)
    - archive: omitted = leave archive state alone (default)
    - Same logic applies to mark_read
    """

    label: str  # Required - the Cortex/* label to apply
    archive: bool | None = None  # Optional - three-state (true/false/omit)
    mark_read: bool | None = None  # Optional - three-state (true/false/omit)


class ClassificationPrompt(BaseModel):
    """A versioned classification prompt template."""

    template: str
    categories: list[str]
    model: str = "qwen2.5:7b"


class BodyExtractionPrompt(BaseModel):
    """A prompt template for extracting structured data from email bodies."""

    template: str
    model: str = "qwen2.5:3b"


# Built-in classification prompts as raw dicts (single source of truth)
# Used by both _default_prompts() and load_rules() for merging
BUILTIN_PROMPTS_DATA: dict[str, dict[str, Any]] = {
    "v1": {
        "template": """You are categorizing emails for a personal Gmail inbox.

Categories:
{categories}

Email:
From: {from_addr}
Subject: {subject}
Body preview: {body_preview}

Respond with JSON only, for example:
{{"category": "human_request", "confidence": 0.85, "reasoning": "..."}}""",
        "categories": [
            "automated_noise",
            "human_request",
            "action_item",
            "wrong_email",
            "subscription",
            "school",
        ],
        "model": "qwen2.5:7b",
    },
}


def _default_prompts() -> dict[str, ClassificationPrompt]:
    """Return default prompts dict."""
    return {
        version: ClassificationPrompt.model_validate(data)
        for version, data in BUILTIN_PROMPTS_DATA.items()
    }


# Built-in body extraction prompts
BUILTIN_BODY_EXTRACTION_PROMPTS_DATA: dict[str, dict[str, Any]] = {
    "apple_merchant_v1": {
        "template": """Extract the product/service name from this Apple receipt line.
Return ONLY the product name, nothing else. Be concise.

Examples:
"TIDAL Music: HiFi SoundTIDAL Family (Monthly)" -> "TIDAL Music"
"iCloud+ with 200 GB (Monthly)" -> "iCloud+ 200GB"
"Plex: Stream Live TV ChannelsMonthly Plex Pass" -> "Plex Pass"
"AppleCare+IPAD MINI 5,WIFI,64GB" -> "AppleCare+ iPad Mini"

Receipt line:
{product_line}

Product name:""",
        "model": "qwen2.5:3b",
    },
}


def _default_body_extraction_prompts() -> dict[str, BodyExtractionPrompt]:
    """Return default body extraction prompts dict."""
    return {
        version: BodyExtractionPrompt.model_validate(data)
        for version, data in BUILTIN_BODY_EXTRACTION_PROMPTS_DATA.items()
    }


class LLMConfig(BaseModel):
    """Configuration for LLM classification chain.

    When `extract` is specified, the LLM will also extract variable values
    alongside classification. These variables can be used in route action labels
    with {{variable_name}} syntax.

    Example:
        llm:
          prompt_version: "v1"
          extract:
            - company_name
        routes:
          subscription:
            label: "Commercial/{{company_name}}/Subscription"
    """

    model: str = "qwen2.5:7b"
    prompt_version: str = "v1"
    extract: list[str] | None = None  # Variable names to extract alongside classification


class Rule(BaseModel):
    """A single rule in a chain.

    Outcomes (mutually exclusive):
    - action: Apply labels/archive/etc
    - jump: Enter a sub-chain for further evaluation
    - return_to_parent: Exit this chain and continue matching in parent chain
    - llm: Use LLM classification with routes

    Variables:
    - variables: Extract values from email to use in label templates
    - Variables are resolved after match, before action
    - If variable resolution fails, rule is skipped (falls through to next)

    Example with variables:
        - match:
            from_glob: "*@github.com"
          variables:
            repo:
              header_regex:
                header: "list-id"
                pattern: "^([^<\\s]+)"
          action:
            label: "GitHub/{{repo}}"
    """

    match: MatchCondition = Field(
        default_factory=lambda: MatchCondition()  # type: ignore[call-arg]
    )
    variables: dict[str, Variable] | None = None  # Variable definitions
    action: Action | None = None
    jump: str | None = None
    return_to_parent: bool | None = None  # Exit chain, continue in parent
    llm: LLMConfig | None = None
    routes: dict[str, Action] | None = None

    @model_validator(mode="after")
    def check_exclusive_outcomes(self) -> Rule:
        """Ensure a rule has exactly one outcome defined."""
        # Note: return_to_parent only counts as an outcome when True
        # (False or None = no outcome, True = outcome)
        outcomes_defined = sum(
            [
                self.action is not None,
                self.jump is not None,
                self.return_to_parent is True,
                self.llm is not None,
            ]
        )

        if outcomes_defined == 0:
            raise ValueError(
                "Rule must have one of 'action', 'jump', 'return_to_parent', or 'llm'."
            )

        if outcomes_defined > 1:
            raise ValueError(
                "Rule can only have one of 'action', 'jump', 'return_to_parent', or 'llm' defined."
            )

        if self.llm and not self.routes:
            raise ValueError("LLM rule must have 'routes'.")

        return self


class RulesConfig(BaseModel):
    """Top-level rules configuration."""

    version: int = 1
    label_prefix: str = "Cortex"
    intents: dict[str, IntentConfig] = Field(default_factory=dict)
    email_categories: dict[str, EmailCategoryConfig] = Field(default_factory=dict)
    prompts: dict[str, ClassificationPrompt] = Field(default_factory=_default_prompts)
    body_extraction_prompts: dict[str, BodyExtractionPrompt] = Field(
        default_factory=_default_body_extraction_prompts
    )
    chains: dict[str, list[Rule]] = Field(default_factory=dict)
    # Email mappings for O(1) hash-based lookup by sender address (normalized lowercase)
    priority_email_mappings: dict[str, EmailMappingAction] = Field(default_factory=dict)
    fallback_email_mappings: dict[str, EmailMappingAction] = Field(default_factory=dict)


class PatternInfo(BaseModel):
    """Pattern information for an email (if it matches a known pattern)."""

    pattern_id: int
    sender: str
    merchant: str
    interval_type: str  # IntervalType: monthly, weekly, yearly, irregular
    confidence: float
    occurrence_count: int
    status: str  # PatternStatus: emerging, active, dormant, ended


class Email(BaseModel):
    """Email data for rule matching."""

    id: int
    gmail_id: str
    from_addr: str
    to_addrs: list[str]  # All recipient addresses
    cc_addrs: list[str] = Field(default_factory=list)  # CC recipients
    bcc_addrs: list[str] = Field(default_factory=list)  # BCC recipients
    subject: str
    body: str | None = None
    has_attachment: bool = False
    headers: dict[str, str] = Field(default_factory=dict)  # Raw headers from emails_raw
    label_ids: list[str] = Field(default_factory=list)  # Gmail label IDs (STARRED, INBOX, etc.)
    size_estimate: int | None = None  # Approximate message size in bytes
    internal_date: int | None = None  # Email date in milliseconds since epoch
    attachment_filenames: list[str] = Field(default_factory=list)  # Attachment filenames
    attachment_mime_types: list[str] = Field(default_factory=list)  # Attachment MIME types

    # Pattern detection info (populated before matching if available)
    pattern_info: PatternInfo | None = None


class EvaluationResult(BaseModel):
    """Result of evaluating rules against an email."""

    matched: bool = False
    returned: bool = False  # True if chain returned to parent via return_to_parent
    chain: str | None = None
    rule_index: int | None = None
    action: Action | None = None
    llm_category: str | None = None
    llm_confidence: float | None = None
    llm_reasoning: str | None = None
    trace: list[str] = Field(default_factory=list)  # Decision trace for debugging
    variables: dict[str, str] = Field(default_factory=dict)  # Resolved variable values
