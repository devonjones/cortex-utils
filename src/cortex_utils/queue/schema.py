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

from cortex_utils.log import get_logger
from cortex_utils.queue.add_retry_columns import add_retry_columns
from cortex_utils.queue.dead_letter import DeadLetterManager
from cortex_utils.queue.ops import (
    QueueError,
    _tx,
    ensure_claim_token_column,
    index_present,
)
from cortex_utils.queue.partitions import PartitionManager

log = get_logger()

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

# The columns ensure_queue_schema() can add to an existing table. Anything else
# missing is refused rather than half-migrated.
_MIGRATED_COLUMNS = frozenset({"next_attempt_at", "claimed_by"})

# How long a boot waits for the schema lock. Generous, because legitimate
# contention here is a fleet redeploy -- but finite, because an indefinite fleet
# hang is worse than a loud boot failure, and the key is per DATABASE, so one
# tenant's wedged boot would otherwise hang the other's.
SCHEMA_LOCK_TIMEOUT_MS = 60_000

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
# different column lists, and renames them onto queue -- so they are indexes on
# this table and _index_present finds them. Reusing either name would mean the
# canonical index is silently never created on exactly the deployments that have
# been around longest, and a claim() that quietly stopped having an index. That
# is this module's own subject matter: two definitions sharing an identifier,
# with the difference invisible until a query is slow.
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
    top of the canonical ones -- a payload expression index, a per-queue partial
    index to keep a hot lookup cheap. Not a unique index for dedup: on a
    partitioned table the key must include created_at, so two producers in
    separate transactions both satisfy it. Use enqueue()'s dedup_key, which
    takes an advisory lock. They are applied with the same discipline as ours:
    the catalogue is asked first, because CREATE INDEX IF NOT EXISTS still takes
    a lock and waits on an open writer even when the index is already there, and
    this runs on every boot.

    Your statement is unqualified and resolves through the connection's
    search_path, same as everything else here. That is what makes composition
    work -- your index lands in your schema without either side naming it -- but
    it is worth saying out loud, because when the search_path is wrong the index
    is built somewhere else and nothing complains.

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
    # Canonical first, deliberately. An extra whose name collides with
    # idx_queue_claim is then discarded rather than replacing it -- reversed,
    # a consumer would silently take over the index claim() depends on.
    for name, statement in [*queue_indexes(table), *extra]:
        with _tx(conn) as cur:
            if _index_present(cur, table, name):
                continue
            cur.execute(statement)
            # Ask again. The statement is the consumer's, so nothing until this
            # moment guarantees it creates the index the name promised -- and a
            # mismatch is not a one-off: with IF NOT EXISTS the statement runs
            # on every boot, taking exactly the lock the probe exists to avoid;
            # without it, the second boot raises DuplicateTable and the service
            # gets no queue at all. This is the one point where it is provable.
            if not _index_present(cur, table, name):
                raise QueueError(
                    f"{statement.strip()[:60]}... did not create an index named "
                    f"{name!r} on {table}. The name and the statement have to "
                    "agree; the name is what every later boot checks."
                )
        log.info("Created queue index", index=name, table=table)


def _index_present(cur: psycopg2.extensions.cursor, table: str, name: str) -> bool:
    """Delegates to ops.index_present -- one implementation, because two drifted."""
    return index_present(cur, table, name)


def ensure_queue_schema(
    conn: psycopg2.extensions.connection,
    extra_indexes: Sequence[tuple[str, str]] = (),
) -> str:
    """Everything a service needs before it touches the queue. Call on boot.

    One call rather than four, because the order matters and getting it wrong is
    an incident that has already happened: the exponential-backoff feature was
    merged months before it first RAN in production, its migration existed as a
    manual CLI step, the deploy flow never ran it, and two workers crash-looped
    on `column "next_attempt_at" does not exist`. Nothing was wrong with the
    migration. Nothing called it.

    Additive migrations first, then the shape check -- that order is the whole
    point. ensure_queue_table() refuses to ALTER a live table, so on a
    deployment that predates a column it would raise where the column's own
    migration would simply have added it. Running them first means an old
    database is brought forward and a new one is created complete.

    Serialised across callers by an advisory lock, because CREATE TABLE / CREATE
    INDEX IF NOT EXISTS are not atomic against concurrent DDL: with eight
    services booting together, one succeeded and seven died on DuplicateTable or
    a UniqueViolation against pg_class. That only happens on a deploy that
    changes the schema -- which is precisely the deploy this function exists to
    perform, and a loud crash-loop there is the shape of the incident it is
    meant to prevent.

    Idempotent, and cheap on the steady-state path: every step pre-checks the
    catalogue before touching anything, so the common case is the advisory lock,
    a handful of reads, and no table locks.
    """
    with _tx(conn) as cur:
        # Bounded. Every DDL statement below runs under a 5s lock_timeout, but
        # this front door had none -- so a holder that wedges BETWEEN its
        # bounded statements blocked every later boot indefinitely. And the key
        # is per DATABASE, not per schema, so a wedged cryo boot would hang
        # cortex boots: cross-tenant coupling at the one place this package
        # otherwise keeps tenants apart. Generous, because legitimate contention
        # here is a fleet redeploy, but finite, because an indefinite fleet hang
        # is worse than a loud boot failure.
        cur.execute("SET LOCAL lock_timeout = %s", (f"{SCHEMA_LOCK_TIMEOUT_MS}ms",))
        # Session-scoped, not xact-scoped: the steps below commit individually,
        # and an xact lock would be released by the first one.
        cur.execute("SELECT pg_advisory_lock(hashtext('cortex_queue_schema'))")
    try:
        return _ensure_queue_schema_locked(conn, extra_indexes)
    finally:
        # The transaction may be aborted -- a step above may have failed
        # mid-DDL -- and the unlock would then raise InFailedSqlTransaction,
        # replacing the real error with a confusing one AND leaving a
        # SESSION-scoped lock held for the life of the connection, which
        # serialises every later boot on it behind a lock nobody holds
        # deliberately. Clear the transaction first, and never let this
        # displace the exception that sent us here.
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 -- a dead connection releases it anyway
            log.warning("Could not clear the transaction before unlocking")
        with _tx(conn) as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext('cortex_queue_schema'))")
            if not cur.fetchone()[0]:
                # False means this session did not hold it, so the lock/unlock
                # pairing broke -- someone unlocked underneath us, or the lock
                # was never taken. Logged rather than raised: this runs in a
                # finally, and raising here would replace whatever real error
                # sent us into it.
                log.warning(
                    "Released a schema lock this session did not hold",
                    hint="lock/unlock pairing in ensure_queue_schema",
                )


def _ensure_queue_schema_locked(
    conn: psycopg2.extensions.connection,
    extra_indexes: Sequence[tuple[str, str]],
) -> str:
    exists, _partitioned, missing = _inspect(conn, "queue")

    if exists:
        # Refuse before altering anything if the shape is wrong in a way the two
        # additive migrations cannot fix. _inspect already told us, in the same
        # round trip, and doing the ALTERs first would take ACCESS EXCLUSIVE
        # twice and build an index before raising about a column neither step
        # was ever going to add.
        unfixable = [c for c in missing if c not in _MIGRATED_COLUMNS]
        if unfixable:
            raise QueueError(
                f"queue exists but is missing {', '.join(unfixable)}, which no "
                "migration here adds. Add them deliberately (ALTER TABLE takes "
                "ACCESS EXCLUSIVE) before booting a service against this table."
            )
        # These ALTER an existing table, so they only make sense once there is
        # one. A fresh database gets every column from queue_ddl() instead.
        add_retry_columns(conn, dry_run=False)
        ensure_claim_token_column(conn)

    result = ensure_queue_table(conn, extra_indexes=extra_indexes)

    # health() reads dead_letter, and drop_partition() archives into it, so a
    # service that never dead-letters anything itself still needs it to exist.
    DeadLetterManager(conn).ensure_table()

    # Today and tomorrow, so a fresh schema does not report itself as a dead
    # maintenance incident. Without this the table is created with zero
    # partitions, the first enqueue self-heals, stamps SELF_HEALED_MARKER, and
    # health().is_healthy reads False -- "maintenance is not keeping up" -- for
    # up to retention_days on every new install, while maintenance is fine and
    # simply has not had its 2AM turn yet. The marker exists so the count means
    # what it says; on the bootstrap path it was saying something false.
    #
    # Boot is also the cheapest moment to create them: uncontended, and it
    # removes the only steady-state path where a correctly configured system
    # routes writes through the self-heal.
    PartitionManager(conn).create_future_partitions(days_ahead=1)
    return result
