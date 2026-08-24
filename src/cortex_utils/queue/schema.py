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

from collections.abc import Sequence

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

# pg_class.relkind, for saying what was found instead of a table.
_RELKINDS = {
    "v": "view",
    "m": "materialized view",
    "i": "index",
    "I": "partitioned index",
    "S": "sequence",
    "f": "foreign table",
    "c": "composite type",
    "t": "TOAST table",
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


# Names, deliberately not idx_queue_pending / idx_queue_processing. migrate.py
# already creates indexes under both of those on any migrated deployment, with
# different column lists -- and _ensure_indexes treats a name that resolves as
# "the index is there". Reusing either name would mean the canonical index is
# silently never created on exactly the deployments that have been around
# longest. That is this module's own subject matter: two definitions sharing an
# identifier, with the difference invisible until a query is slow.
CLAIM_INDEX = "idx_{table}_claim"
STALE_INDEX = "idx_{table}_stale"


def queue_indexes(table: str = "queue") -> list[tuple[str, str]]:
    """(name, statement) for the indexes the primitives' own queries need.

    On the parent, so every partition inherits them.

    Column order is measured, not guessed. claim() filters queue_name = ? AND
    status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= NOW()),
    then orders by priority DESC, created_at. Putting next_attempt_at -- a range
    predicate -- between the equality prefix and the sort keys stops the index
    serving the ORDER BY: on 200k rows that was a 13k-row Sort, 2272 buffers,
    4.3ms. With the range column out of the way it is no Sort, 15 buffers,
    0.05ms.

    The stale-recovery pass filters on queue_name too, so leading with status
    alone leaves it as a Filter: 86ms against 1.2ms.
    """
    return [
        (
            CLAIM_INDEX.format(table=table),
            f"CREATE INDEX IF NOT EXISTS {CLAIM_INDEX.format(table=table)} ON {table} "
            f"(queue_name, status, priority DESC, created_at)",
        ),
        (
            STALE_INDEX.format(table=table),
            f"CREATE INDEX IF NOT EXISTS {STALE_INDEX.format(table=table)} ON {table} "
            f"(queue_name, status, claimed_at)",
        ),
    ]


def _inspect(conn: psycopg2.extensions.connection, table: str) -> tuple[bool, bool, list[str]]:
    """(exists, partitioned, missing_columns) in one round trip.

    relkind comes free on the join the column lookup already needs, and asking
    the same catalogue three times about one table is three chances for the
    answers to describe three different moments.
    """
    with _tx(conn) as cur:
        cur.execute(
            "SELECT c.relkind, "
            "  array_remove(array_agg(a.attname) FILTER (WHERE NOT a.attisdropped), NULL) "
            "FROM pg_class c "
            "LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 "
            "WHERE c.oid = to_regclass(%s) "
            "GROUP BY c.relkind",
            (table,),
        )
        row = cur.fetchone()

    if row is None:
        return False, False, list(REQUIRED_COLUMNS)
    relkind, present = row

    # to_regclass resolving is not proof that `table` is a table -- the same
    # argument this module makes about index names, one relation kind over. A
    # matview with the right column names would otherwise read as "present", a
    # view would raise WrongObjectType from inside _ensure_indexes, and a
    # sequence would be reported as a table missing every column it never had.
    if relkind not in ("r", "p"):
        raise QueueError(
            f"{table} exists but is a {_RELKINDS.get(relkind, relkind)}, not a table. "
            "Check the connection's search_path -- the queue primitives need a "
            "table (partitioned, ideally) of that name."
        )

    present = set(present or ())
    return True, relkind == "p", [n for n in REQUIRED_COLUMNS if n not in present]


def missing_columns(conn: psycopg2.extensions.connection, table: str = "queue") -> list[str]:
    """Which REQUIRED_COLUMNS this schema's `table` does not have.

    Bound through to_regclass, so it answers about the table the connection's
    search_path resolves rather than a same-named one in another schema -- the
    distinction that cost cortex 4.8 days of email.

    Empty list means the shape is compatible. It does NOT mean identical: a
    consumer may add columns of its own, and that is the composition this module
    is meant to allow.
    """
    return _inspect(conn, table)[2]


def ensure_queue_table(
    conn: psycopg2.extensions.connection,
    table: str = "queue",
    extra_indexes: Sequence[tuple[str, str]] = (),
) -> str:
    """Make `table` exist with the canonical shape. Safe on every boot.

    Returns "created" if this call made it, "present" if it was already there
    and compatible.

    `extra_indexes` is (name, CREATE INDEX statement) pairs a consumer needs on
    top of the canonical ones -- per-queue partial unique indexes for dedup, a
    payload expression index. They are applied with the same discipline as ours:
    the catalogue is asked first, because CREATE INDEX IF NOT EXISTS still takes
    a lock and waits on an open writer even when the index is already there, and
    this runs on every boot.

    Passing them here rather than keeping a private migration is the point. A
    consumer that adopts this function and deletes its own DDL otherwise loses
    those indexes silently -- nothing here would put them back, and the absence
    shows up as a slow query or a constraint that has quietly stopped
    constraining. Hand them over instead.

    Raises QueueError if it exists but is missing columns the primitives need.
    Adding them here is not safe to do silently: this package cannot know
    whether a column is absent because the table predates it or because a
    consumer deliberately maintains a different shape, and an ALTER on a live
    queue takes ACCESS EXCLUSIVE. Reporting exactly which columns are missing
    lets the operator decide, which is the whole point of having one definition
    to compare against.
    """
    exists, partitioned, missing = _inspect(conn, table)

    if exists and not missing:
        if not partitioned:
            # Legal and supported -- migrate_to_partitioned() exists for exactly
            # this -- but say so here rather than letting partitions.py fail
            # later with an error about a relation that "is not partitioned",
            # several frames from the thing that could have mentioned it.
            log.warning(
                "queue table is not partitioned",
                table=table,
                hint="run migrate-queue to partition it; retention needs partitions",
            )
        _ensure_indexes(conn, table, extra_indexes)
        return "present"

    if exists:
        raise QueueError(
            f"{table} exists but is missing {', '.join(missing)}. "
            "The queue primitives read those columns; add them deliberately "
            "(ALTER TABLE takes ACCESS EXCLUSIVE) rather than having a boot "
            "path do it under you. For the two that have their own migrations, "
            "that is ensure_claim_token_column(conn) for claimed_by and "
            "add_retry_columns(conn) for next_attempt_at."
        )

    with _tx(conn) as cur:
        cur.execute(queue_ddl(table))
    _ensure_indexes(conn, table, extra_indexes)
    log.info("Created queue table", table=table)
    return "created"


def _ensure_indexes(
    conn: psycopg2.extensions.connection,
    table: str,
    extra: Sequence[tuple[str, str]] = (),
) -> None:
    """Create any missing index, asking first.

    CREATE INDEX IF NOT EXISTS still takes a lock and waits on an open writer
    even when the index is already there, and its queued ShareLock times out
    inserts behind it. This runs on every boot, so it asks the catalogue.
    """
    for name, statement in [*queue_indexes(table), *extra]:
        with _tx(conn) as cur:
            cur.execute("SELECT to_regclass(%s)", (name,))
            if cur.fetchone()[0] is not None:
                continue
            cur.execute(statement)
        log.info("Created queue index", index=name)
