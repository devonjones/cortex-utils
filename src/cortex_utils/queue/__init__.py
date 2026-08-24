"""Queue management utilities.

`X as X` on an import marks a deliberate but unadvertised re-export: the name
stays importable from this package for callers who already use it, and stays out
of __all__ because it is not what a new consumer should reach for. See the
__all__ comment below.
"""

from cortex_utils.queue.add_retry_columns import (
    add_retry_columns as add_retry_columns,
)
from cortex_utils.queue.add_retry_columns import (
    has_next_attempt_at_column as has_next_attempt_at_column,
)
from cortex_utils.queue.dead_letter import DeadLetterManager
from cortex_utils.queue.inspect import (
    Failure,
    QueueDepth,
    QueueHealth,
    StuckJob,
    failures,
    health,
    resubmit,
    stuck,
)
from cortex_utils.queue.migrate import (
    is_queue_partitioned as is_queue_partitioned,
)
from cortex_utils.queue.migrate import (
    migrate_to_partitioned as migrate_to_partitioned,
)
from cortex_utils.queue.ops import (
    JobNotFailedError,
    QueueError,
    claim,
    complete,
    enqueue,
    release,
)
from cortex_utils.queue.ops import (
    ensure_claim_token_column as ensure_claim_token_column,
)
from cortex_utils.queue.ops import fail_or_retry as fail_or_retry
from cortex_utils.queue.ops import (
    has_claim_token_column as has_claim_token_column,
)
from cortex_utils.queue.partitions import (
    PartitionError,
    PartitionManager,
    PartitionNotAttachedError,
    QueueTableNotFoundError,
)
from cortex_utils.queue.retry import (
    DEFAULT_BASE_SECONDS as DEFAULT_BASE_SECONDS,
)
from cortex_utils.queue.retry import (
    DEFAULT_CAP_SECONDS as DEFAULT_CAP_SECONDS,
)
from cortex_utils.queue.retry import (
    DEFAULT_JITTER_RATIO as DEFAULT_JITTER_RATIO,
)
from cortex_utils.queue.retry import (
    compute_backoff_delay as compute_backoff_delay,
)
from cortex_utils.queue.retry import (
    ready_predicate as ready_predicate,
)
from cortex_utils.queue.schema import (
    REQUIRED_COLUMNS as REQUIRED_COLUMNS,
)
from cortex_utils.queue.schema import (
    ensure_queue_schema,
)
from cortex_utils.queue.schema import (
    ensure_queue_table as ensure_queue_table,
)
from cortex_utils.queue.schema import (
    missing_columns as missing_columns,
)
from cortex_utils.queue.schema import (
    queue_ddl as queue_ddl,
)
from cortex_utils.queue.stats import (
    get_queue_depth as get_queue_depth,
)
from cortex_utils.queue.stats import (
    get_queue_stats as get_queue_stats,
)
from cortex_utils.queue.stats import (
    get_stale_jobs as get_stale_jobs,
)

# The surface a consumer should reach for. Everything else stays importable by
# its full path -- nothing is removed -- but is no longer advertised here.
#
# It had grown three generations deep: two inspection APIs, two ways to create
# the schema, and every migration internal exported at top level, which invited
# consumers to call the individual pieces that ensure_queue_schema() exists to
# sequence. A new consumer's plausible-looking first day was ensure_queue_table()
# plus retry.fail_or_retry -- both wrong, neither warning.
__all__ = [
    # Boot. One entry point: it sequences the DDL, the migrations, the
    # dead-letter table and the first partitions, in that order.
    "ensure_queue_schema",
    # Work.
    "enqueue",
    "claim",
    "complete",
    "release",
    "fail_or_retry",
    # Watch. Read-only, and independent of the workers by design.
    "health",
    "failures",
    "stuck",
    "resubmit",
    "QueueHealth",
    "QueueDepth",
    "StuckJob",
    "Failure",
    # Operate.
    "PartitionManager",
    "DeadLetterManager",
    # Errors.
    "QueueError",
    "JobNotFailedError",
    "PartitionError",
    "PartitionNotAttachedError",
    "QueueTableNotFoundError",
]
