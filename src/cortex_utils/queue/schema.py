"""The canonical queue table shape.

The table shape *is* the shared contract. Every primitive in this package
compiles assumptions about it -- the status values, `attempts`/`max_attempts`,
`next_attempt_at`, `claimed_by`, the partition key -- so a consumer that
maintains its own copy is maintaining half of an interface whose other half
lives here. Two copies drift, and they drift silently: a column added on one
side is invisible to the other until a query returns the wrong thing.
`ensure_claim_token_column()` exists only because exactly that happened.

The evidence was not hypothetical when this module was written. Six copies of
this DDL existed -- one in `migrate.py` under the name `queue_new`, four
hand-written across the live test suites, and one in a downstream consumer --
and they already disagreed. `migrate.py`'s copy, the one closest to
authoritative, was missing `priority`, `claimed_by` and `next_attempt_at`: three
columns the primitives require. It had drifted from the code it exists to
support, in the same repository, unnoticed.

So: one definition, and a verification that reports drift instead of assuming
its absence.
"""

from __future__ import annotations

import psycopg2
import structlog

from cortex_utils.queue.ops import QueueError, _tx

log = structlog.get_logger()

# What the primitives read and write. The value is the type a fresh table gets;
# verify_queue_table() checks presence rather than type, because an existing
# deployment may legitimately have a wider one (INT vs BIGINT) and narrowing it
# is not this function's business.
REQUIRED_COLUMNS: dict[str, str] = {
    "id": "BIGSERIAL",
    "queue_name": "TEXT NOT NULL",
    "payload": "JSONB NOT NULL",
    "status": "TEXT NOT NULL DEFAULT 'pending'",
    "priority": "INT NOT NULL DEFAULT 0",
    "attempts": "INT NOT NULL DEFAULT 0",
    "max_attempts": "INT NOT NULL DEFAULT 3",
    "last_error": "TEXT",
    "claimed_at": "TIMESTAMPTZ",
    # The claim token. Without it every caller is anonymous and a worker that
    # stalled past its visibility timeout can report on a row somebody else has
    # since claimed -- see ensure_claim_token_column().
    "claimed_by": "TEXT",
    "next_attempt_at": "TIMESTAMPTZ",
    "completed_at": "TIMESTAMPTZ",
    "created_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
}

VALID_STATUSES = ("pending", "processing", "completed", "failed", "cancelled")


def queue_ddl(table: str = "queue") -> str:
    """The canonical CREATE TABLE, parameterised only by name.

    `table` is a name this package chooses -- 'queue', or 'queue_new' during a
    migration. It is never user input, and interpolating it is deliberate:
    Postgres has no parameter binding for identifiers.
    """
    if not table.replace("_", "").isalnum():
        raise QueueError(f"not a plausible table name: {table!r}")
    columns = ",\n    ".join(f"{name} {spec}" for name, spec in REQUIRED_COLUMNS.items())
    statuses = ", ".join(f"'{s}'" for s in VALID_STATUSES)
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    {columns},
    CONSTRAINT {table}_valid_status CHECK (status IN ({statuses})),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
"""


def queue_indexes(table: str = "queue") -> list[str]:
    """Indexes the primitives' own queries depend on.

    On the parent, so every partition inherits them. claim() filters on
    (queue_name, status, next_attempt_at) and orders by (priority DESC,
    created_at); the stale-recovery pass filters on (status, claimed_at).
    """
    return [
        f"CREATE INDEX IF NOT EXISTS idx_{table}_claimable ON {table} "
        f"(queue_name, status, next_attempt_at, priority DESC, created_at)",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_processing ON {table} (status, claimed_at)",
    ]


def missing_columns(conn: psycopg2.extensions.connection, table: str = "queue") -> list[str]:
    """Which REQUIRED_COLUMNS this schema's `table` does not have.

    Bound through to_regclass, so it answers about the table the connection's
    search_path resolves rather than a same-named one in another schema -- the
    distinction that cost cortex 4.8 days of email.

    Empty list means the shape is compatible. It does NOT mean identical: a
    consumer may add columns of its own, and that is the composition this module
    is meant to allow.
    """
    with _tx(conn) as cur:
        cur.execute(
            "SELECT attname FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped",
            (table,),
        )
        present = {r[0] for r in cur.fetchall()}
    return [name for name in REQUIRED_COLUMNS if name not in present]


def ensure_queue_table(conn: psycopg2.extensions.connection, table: str = "queue") -> str:
    """Make `table` exist with the canonical shape. Safe on every boot.

    Returns "created" if this call made it, "present" if it was already there
    and compatible.

    Raises QueueError if it exists but is missing columns the primitives need.
    Adding them here is not safe to do silently: this package cannot know
    whether a column is absent because the table predates it or because a
    consumer deliberately maintains a different shape, and an ALTER on a live
    queue takes ACCESS EXCLUSIVE. Reporting exactly which columns are missing
    lets the operator decide, which is the whole point of having one definition
    to compare against.
    """
    missing = missing_columns(conn, table)
    if not missing:
        with _tx(conn) as cur:
            cur.execute("SELECT to_regclass(%s)", (table,))
            if cur.fetchone()[0] is not None:
                _ensure_indexes(conn, table)
                return "present"

    with _tx(conn) as cur:
        cur.execute("SELECT to_regclass(%s)", (table,))
        exists = cur.fetchone()[0] is not None

    if exists:
        raise QueueError(
            f"{table} exists but is missing {', '.join(missing)}. "
            "The queue primitives read those columns; add them deliberately "
            "(ALTER TABLE takes ACCESS EXCLUSIVE) rather than having a boot "
            "path do it under you."
        )

    with _tx(conn) as cur:
        cur.execute(queue_ddl(table))
    _ensure_indexes(conn, table)
    log.info("Created queue table", table=table)
    return "created"


def _ensure_indexes(conn: psycopg2.extensions.connection, table: str) -> None:
    """Create any missing index, asking first.

    CREATE INDEX IF NOT EXISTS still takes a lock and waits on an open writer
    even when the index is already there, and its queued ShareLock times out
    inserts behind it. This runs on every boot, so it asks the catalogue.
    """
    for statement in queue_indexes(table):
        name = statement.split("IF NOT EXISTS ")[1].split()[0]
        with _tx(conn) as cur:
            cur.execute("SELECT to_regclass(%s)", (name,))
            if cur.fetchone()[0] is not None:
                continue
            cur.execute(statement)
        log.info("Created queue index", index=name)
