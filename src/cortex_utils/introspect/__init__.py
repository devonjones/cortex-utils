"""Let a local model introspect one cortex instance, safely.

Bound at construction to one deployment and one message; the model supplies
questions, never targets. See tools.py and docs/agent-isolation.md.
"""

from cortex_utils.introspect.client import CortexClient, CortexReadError
from cortex_utils.introspect.instances import Instance, known_instance_names, resolve
from cortex_utils.introspect.session import ask
from cortex_utils.introspect.tools import CortexTools, ToolRefusalError, tool_specs

__all__ = [
    "CortexClient",
    "CortexReadError",
    "CortexTools",
    "Instance",
    "ToolRefusalError",
    "ask",
    "known_instance_names",
    "resolve",
    "tool_specs",
]
