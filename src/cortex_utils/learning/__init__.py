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
]
