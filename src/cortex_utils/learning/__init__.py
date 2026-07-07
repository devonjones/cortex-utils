"""Rule-learning primitives shared across Cortex services (cortex-uo9b).

Teach-by-labeling: detect when a manual Gmail label change diverges from what
triage classified, and record it as a learning opportunity to propose a rule.
"""

from cortex_utils.learning.expected_labels import (
    DEFAULT_LABEL_PREFIX,
    active_label_prefix,
    expected_cortex_labels,
    managed_labels,
)
from cortex_utils.learning.opportunities import (
    ADD,
    DIRECTIONS,
    REMOVE,
    ensure_learning_schema,
    record_learning_opportunity,
)
from cortex_utils.learning.proposals import (
    APPROVED,
    PENDING,
    REJECTED,
    SOURCE_TEACH,
    SUPERSEDED,
    ProposalRun,
    RuleProposal,
    approve_proposal,
    ensure_proposals_schema,
    get_proposal_by_message_id,
    list_pending_proposals,
    mark_proposal_posted,
    propose_from_opportunities,
    reject_proposal,
    set_proposal_status,
    unposted_pending_proposals,
)

__all__ = [
    "DEFAULT_LABEL_PREFIX",
    "active_label_prefix",
    "expected_cortex_labels",
    "managed_labels",
    "ADD",
    "REMOVE",
    "DIRECTIONS",
    "ensure_learning_schema",
    "record_learning_opportunity",
    "PENDING",
    "APPROVED",
    "REJECTED",
    "SUPERSEDED",
    "SOURCE_TEACH",
    "ProposalRun",
    "RuleProposal",
    "ensure_proposals_schema",
    "propose_from_opportunities",
    "list_pending_proposals",
    "unposted_pending_proposals",
    "get_proposal_by_message_id",
    "mark_proposal_posted",
    "set_proposal_status",
    "approve_proposal",
    "reject_proposal",
]
