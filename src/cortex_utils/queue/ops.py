"""Queue primitives shared by every consumer of the cortex queue.

Both cortex and cryo grew their own copy of this pattern against the same table
shape in the same database, and the copies drifted in ways that cost real work:
cryo's stale-claim recovery consumed an attempt where cortex's did not, so an
expired OAuth token on 2026-08-18 burned four healthy videos to terminal. The
point of this module is that there is one copy.

Which queue is operated on is decided by search_path, exactly as in partitions.py
-- nothing here names a schema.

Three semantics are load-bearing and should not be "simplified" later:

Expiry never consumes an attempt. A worker that dies before reporting has not
proven anything about the work. Only an explicit fail_or_retry() call, made by a
worker that actually attempted the job, spends the budget. An outage must cost
latency, never work.

Every report is claim-token matched, which is why `worker` is required rather
than defaulted. A shared default would make every caller anonymous and every
anonymous claim indistinguishable, so the token would protect nothing.

Dedup returns success. "Already queued" and "emission failed" are different
answers, and a caller that must not proceed unless the work is covered needs to
tell them apart.

Transactions: each function is one unit of work and commits its own by default.
A failure rolls back before re-raising, so a caller never inherits an aborted
transaction and never sees its next, unrelated statement fail because of this
one.

The other operations take no such flag: a claim or a report is the caller's whole
unit of work, so there is nothing to compose it with. Only enqueue is ever one
half of something larger. Note also that a job id returned under commit=False is
not durable until the caller's own commit runs.

enqueue() takes commit=False for callers that must land the job atomically with
their own work -- approving a proposal and queueing its apply job, or moving a
row out of dead_letter and re-queueing it. Committing those separately would let
a crash between them leave the two halves disagreeing. In that mode the caller
owns the transaction entirely, which is the convention retry.fail_or_retry
already follows.
"""

from __future__ import annotations

import os
import re
import socket
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Any, Literal

import psycopg2
from psycopg2.extras import Json

from cortex_utils.log import get_logger
from cortex_utils.queue.retry import (
    DEFAULT_BASE_SECONDS,
    DEFAULT_CAP_SECONDS,
    DEFAULT_JITTER_RATIO,
    compute_backoff_delay,
)

log = get_logger()

DEFAULT_VISIBILITY_TIMEOUT_MIN = 30
ERROR_MAX_CHARS = 2000

# ALTER TABLE on a partitioned parent takes ACCESS EXCLUSIVE and cascades to
# every partition. Bounded so a boot cannot wedge the pipeline behind a claim.
MIGRATION_LOCK_TIMEOUT_MS = 5000

# CREATE TABLE ... PARTITION OF takes ACCESS EXCLUSIVE on the parent, which
# blocks every insert and every claim across all queue_names while held. On the
# deploy path a few seconds is fine; this one runs on the live producer path, so
# it gets a tighter bound. Losing the race is success -- whoever holds the lock
# is almost certainly creating this very partition.
PARTITION_LOCK_TIMEOUT_MS = 2000

# Stamped on partitions the write path had to create. A non-zero count means
# scheduled maintenance is not running -- a countable signal rather than a log
# line, because a log line is the channel that already failed to surface a
# two-day outage. Recorded on the object itself so Postgres holds the fact.
SELF_HEALED_MARKER = "created by enqueue self-heal"

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Values whose Python str() matches Postgres's jsonb ->> text output. bool gives
# "True" vs "true", float and container types differ too, so a mismatch would
# make dedup silently never match and double-queue instead of erroring.
_DEDUP_VALUE_TYPES = (str, int)

Report = Literal["pending", "failed", "stale"]
_CLAIM_TOKEN_RE = re.compile(r"\bclaimed_by\b")


class QueueError(RuntimeError):
    """Base for queue operation failures."""


class JobNotFailedError(QueueError):
    """resubmit() was given a job id that is not a failed row.

    Its own condition, because resubmit() raises for three different things and
    a batch caller has to tell them apart: a caller error (both dedup arguments),
    THIS -- the ordinary "someone already handled it, or the id was stale" -- and
    the cancel-rollback path, which resubmit's own comment calls "not a race, it
    is a bug".

    Collapsing the last two loses the distinction that matters: one is a row to
    skip and carry on, the other is an internal invariant broken, and reporting
    that as a stale click is how it stays unnoticed. Without a subclass the only
    way to separate them is to re-read failures() after the raise and infer --
    which cryo had to write, and got wrong first.
    """


class PartitionError(QueueError):
    """Base for partition-management failures, so callers can catch the category."""


class QueueTableNotFoundError(PartitionError):
    """No queue table is visible on the connection's search_path."""


class PartitionNotAttachedError(PartitionError):
    """A relation of the partition's name exists but is not a partition of queue."""


@contextmanager
def _tx(
    conn: psycopg2.extensions.connection,
    commit: bool = True,
    lock_timeout_ms: int | None = None,
) -> Iterator[psycopg2.extensions.cursor]:
    """One unit of work: commit on success, roll back before re-raising.

    Without the rollback a failure leaves the connection in an aborted
    transaction, and the caller's next statement fails complaining about this
    one. For a long-lived worker that means every later job fails for reasons
    that look unrelated to the job that actually broke.

    commit=False hands both halves to the caller: no commit, and no rollback.

    Not because rollback would lose the caller's work -- Postgres has already
    aborted the whole transaction by the time we would call it, so that work is
    gone regardless. It is left alone because the caller may hold a SAVEPOINT and
    want to recover to it, and rolling back here would take that choice away.
    Either way the connection is aborted and unusable until the caller rolls
    back, which is the contract they accepted by owning the transaction.

    lock_timeout_ms bounds how long the statements inside will wait for a lock.
    It lives here, and not at the call sites, because SET LOCAL is a silent
    no-op under autocommit -- there is no transaction for it to be local to --
    so every hand-rolled bound was conditional on a connection shape the library
    does not control. Four sites issued one without the guard and were therefore
    unbounded on exactly the shape dead_letter's own docstrings call "the
    connection shape a consumer is most likely to hand us"; dead_letter learned
    it three separate times and wrote the dance out three times.

    Measured, with the schema advisory lock held elsewhere and the bound at
    1500ms: a transaction-mode connection raised LockNotAvailable at 1.50s, an
    autocommit one was still waiting at 12s and died only to a statement_timeout
    backstop.
    """
    # psycopg2 refuses to change the session mode inside a transaction, but
    # under autocommit there is none open to refuse for -- that is the whole
    # point of the branch. Restored in the finally, including on the error path,
    # so the caller's connection goes back the way it arrived.
    if lock_timeout_ms is not None and not commit:
        # These two cannot both hold. The bound is issued as SET LOCAL in the
        # committing branch only, so combining them silently produced NO bound
        # -- while the autocommit flip and its finally-rollback still fired off
        # lock_timeout_ms alone, destroying the caller's uncommitted work with
        # no error. Nothing combines them today; this refuses rather than
        # leaving the trap set, because a bound that is conditional on the
        # caller's connection shape is the exact defect this parameter exists
        # to remove. A caller who owns the transaction owns its bound too.
        raise ValueError(
            "lock_timeout_ms needs a transaction of its own; with commit=False "
            "the caller owns the transaction and sets its own bound"
        )
    # `is not None`, not truthiness: lock_timeout_ms=0 means "no wait at all" in
    # Postgres, and reading it as "unbounded" would invert the caller's intent.
    was_autocommit = lock_timeout_ms is not None and conn.autocommit
    if was_autocommit:
        conn.autocommit = False
    try:
        yield from _tx_body(conn, commit, lock_timeout_ms)
    finally:
        if was_autocommit:
            conn.rollback()
            conn.autocommit = True


def _tx_body(
    conn: psycopg2.extensions.connection, commit: bool, lock_timeout_ms: int | None
) -> Iterator[psycopg2.extensions.cursor]:
    if not commit:
        # No claimed_by guard here deliberately: the only caller that reaches
        # this branch is _insert(), whose statement names no such column. A
        # guard that cannot fire reads as protection and is not, and it would
        # have to re-raise anyway since the caller owns the transaction.
        with conn.cursor() as cur:
            yield cur
        return
    try:
        with conn.cursor() as cur:
            if lock_timeout_ms is not None:
                cur.execute("SET LOCAL lock_timeout = %s", (f"{lock_timeout_ms}ms",))
            yield cur
        conn.commit()
    except psycopg2.errors.UndefinedColumn as exc:
        conn.rollback()
        _reraise_missing_migration(exc)
        raise
    except Exception:
        conn.rollback()
        raise


def _reraise_missing_migration(exc: psycopg2.errors.UndefinedColumn) -> None:
    """Turn a raw UndefinedColumn on claimed_by into the remedy.

    claim(), complete(), release() and fail_or_retry() all reference claimed_by,
    and the only thing protecting them is the convention that a service calls
    ensure_claim_token_column() on boot. A consumer who forgets gets
    UndefinedColumn out of claim() -- from the core of the queue, not a side
    feature -- naming a column they never wrote.

    Guarded here rather than at the four call sites because a half-guard reads
    as safe and is not: guard three and the fourth still raises the raw error on
    the same schema, which is worse than guarding none. Every primitive routes
    through this function, so one place covers all of them, and it costs one
    regex match only on a path that was already failing.

    Not a degraded answer, because there is no useful one: without the claim
    token, complete() and release() cannot tell your row from one another worker
    re-claimed, so proceeding would silently reintroduce the bug the column
    exists to prevent.
    """
    # diag.message_primary, NOT str(exc). str() appends the LINE excerpt of our
    # own SQL and the HINT, so any statement that merely *mentions* claimed_by
    # matches -- including `SELECT id, claimed_by, attemptz FROM queue`, whose
    # real error is the typo, and `SELECT claimed_bx`, whose HINT helpfully
    # suggests claimed_by. Relabelling those would destroy the column name the
    # caller needs and point them at an idempotent migration they already ran.
    #
    # Word boundaries rather than the quoted form: the quotes are not
    # dependable. A qualified reference drops them entirely
    # ("column q.claimed_by does not exist"), and a translated server uses its
    # own -- de emits >>claimed_by<<. The boundaries still exclude a
    # claimed_by_anything column, since _ is a word character.
    #
    # diag.column_name would be better but Postgres does not populate it for
    # SQLSTATE 42703, so the message is the only thing that identifies it.
    if not _CLAIM_TOKEN_RE.search(exc.diag.message_primary or ""):
        # Not ours. Return so the caller re-raises the original plainly --
        # `raise exc from exc` would set __cause__ to itself and suppress the
        # context a reader needs.
        return
    raise QueueError(
        "queue.claimed_by is missing on this search_path. Run "
        "cortex_utils.queue.ensure_claim_token_column(conn) once against this "
        "schema before using the queue -- it is idempotent and safe on every boot."
    ) from exc


def has_claim_token_column(conn: psycopg2.extensions.connection) -> bool:
    """True if this schema's queue table already has claimed_by.

    Guarded the same way as the other column probe: to_regclass returns NULL for
    a missing table and `attrelid = NULL` matches nothing, so an unguarded answer
    for a misconfigured search_path is False -- "the column is missing" rather
    than "there is no table", which sends the caller down a migration path.

    Goes through _tx like everything else: psycopg2 opens a transaction even for
    a read, and this is the fast path on every service boot, so a bare cursor
    would leave long-lived worker connections idle-in-transaction as the normal
    case rather than the exception.
    """
    require_queue_table(conn)
    with _tx(conn) as cur:
        cur.execute(
            """
            SELECT 1 FROM pg_attribute
            WHERE attrelid = to_regclass('queue')
              AND attname = 'claimed_by'
              AND NOT attisdropped
            LIMIT 1
            """
        )
        return cur.fetchone() is not None


def ensure_claim_token_column(conn: psycopg2.extensions.connection) -> bool:
    """Add claimed_by if this schema predates claim tokens. True if added.

    Services call this on every boot, so the common case must cost nothing: the
    ALTER takes ACCESS EXCLUSIVE on a partitioned parent under continuous claim
    traffic, and running it unconditionally would queue that lock behind live
    work on every redeploy. Same pre-check shape as add_retry_columns.py, and a
    lock_timeout so a boot fails fast rather than stalling the pipeline.
    """
    if has_claim_token_column(conn):
        return False

    with _tx(conn, lock_timeout_ms=MIGRATION_LOCK_TIMEOUT_MS) as cur:
        cur.execute("ALTER TABLE queue ADD COLUMN IF NOT EXISTS claimed_by TEXT")
    log.info("Added queue.claimed_by")
    return True


def _partition_name(day: date) -> str:
    return f"queue_{day.strftime('%Y_%m_%d')}"


# GUC sources that mean "the server handed this out", so every connection to it
# agrees. Everything else -- 'client' (PGOPTIONS), 'session' (SET TIME ZONE),
# 'user' (ALTER ROLE ... SET) and 'database' (ALTER DATABASE ... SET) -- is a
# per-connection or per-role override.
#
# Written as an allow-list, not a deny-list. The first version named only
# 'client' and 'session', which missed 'user' and 'database' -- and ALTER ROLE
# is the IDIOMATIC way to give one application its own timezone in a shared
# database, so it missed the likeliest cause of the disagreement it looks for.
# An unknown future source now reads as suspicious rather than as fine.
_INHERITED_TIMEZONE_SOURCES = frozenset(
    {"default", "configuration file", "environment variable", "command line", "override"}
)


def server_today(conn: psycopg2.extensions.connection) -> date:
    """Today according to the server, not this process.

    created_at is TIMESTAMPTZ DEFAULT NOW(), a value the *server* produces, so
    any date used to route or create a partition for that row has to come from
    the same clock. A client-side date.today() is a second, unsynchronised clock
    in a possibly different timezone; it agrees only by coincidence, which means
    it tests green wherever client and server are both UTC and fails on the
    first deployment where they are not.

    Goes through _tx like every other read here: psycopg2 opens a transaction
    even for a SELECT, and callers such as create_future_partitions can finish
    without ever reaching a write, which would leave the connection
    idle-in-transaction on the steady-state path.

    Invariant this rests on: every connection operating a given queue uses the
    same session TimeZone. CURRENT_DATE and the FROM/TO bounds on a TIMESTAMPTZ
    column are both TimeZone-dependent, so two connections disagreeing puts the
    boundary rows of each day in the wrong partition. PGOPTIONS is already the
    schema-selection knob here (`-c search_path=cryo`) and carries `-c timezone=`
    too, so this is one env var away from being violated. The create path fails
    loudly if it happens; drop_old_partitions compares a name-derived date
    against a differently-framed cutoff and would drop silently.
    """
    with _tx(conn) as cur:
        # Folded into the same round trip the date already costs. pg_settings
        # reports where the value came from; anything the server did not hand
        # out means this connection was given its own TimeZone, which is how two
        # connections on one queue end up framing day boundaries differently.
        cur.execute(
            "SELECT CURRENT_DATE, current_setting('TimeZone'), "
            "(SELECT source FROM pg_settings WHERE name = 'TimeZone')"
        )
        today, zone, source = cur.fetchone()
    if source not in _INHERITED_TIMEZONE_SOURCES:
        # A warning, not an error: a deployment where every connection sets the
        # same zone this way is consistent and fine. What cannot be checked from
        # one connection is whether the OTHER ones agree -- so this reports the
        # thing that makes disagreement possible, and leaves the judgement to
        # whoever set it.
        log.warning(
            "Session TimeZone is overridden per connection, not inherited from "
            "the server. Every connection operating this queue must agree: "
            "CURRENT_DATE and the partition bounds are both TimeZone-dependent, "
            "and drop_old_partitions compares a name-derived date against a "
            "differently-framed cutoff -- it would drop silently, not loudly.",
            timezone=zone,
            source=source,
        )
    return today


_ATTACHED_SQL = """
    SELECT 1 FROM pg_class c
    JOIN pg_inherits i ON c.oid = i.inhrelid
    WHERE i.inhparent = to_regclass('queue') AND c.relname = %s
"""


def index_present(cur: psycopg2.extensions.cursor, table: str, name: str) -> bool:
    """True if `name` is a VALID index ON `table`.

    One implementation, because two drifted. schema.py had this shape and
    dead_letter.py had `to_regclass(name) IS NOT NULL` -- which proves a name
    resolves somewhere on the search_path, not that the index is on this table.
    Under `search_path = app, shared`, an unrelated index of that name in
    `shared` made the gate answer "present" and the index was never created, on
    any boot, forever, while the caller logged success. Multi-schema is this
    package's normal deployment; every live test runs under a SET search_path.

    indisvalid too: an interrupted CREATE INDEX CONCURRENTLY leaves an invalid
    index that resolves and joins and indexes nothing.

    Takes a cursor rather than a connection so it shares the caller's
    transaction -- a post-CREATE probe has to see the uncommitted index.
    """
    cur.execute(
        "SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE c.relname = %s AND i.indrelid = to_regclass(%s) AND i.indisvalid",
        (name, table),
    )
    return cur.fetchone() is not None


def require_queue_table(conn: psycopg2.extensions.connection) -> None:
    """Fail loudly when search_path resolves no queue table.

    to_regclass() yields NULL rather than raising, so a lookup bound through it
    reports an ordinary empty result for a connection pointed at the wrong
    schema -- indistinguishable from a healthy "nothing to do". That is how a
    migration probe comes back "column missing" for a table that does not exist,
    and an operator is handed a migration to run that then blows up.

    Every catalogue read that can be reached without one should call this, so
    the contract does not vary by entry point. That promise is why the relkind
    is checked here too: a matview or a view named queue resolves through
    to_regclass and carries pg_attribute rows, so without it
    has_claim_token_column() would answer True about a matview -- a wrong branch
    reported as success, and the same entry point disagreeing with
    schema.missing_columns() about the same connection.
    """
    with _tx(conn) as cur:
        cur.execute(
            "SELECT 1 FROM pg_class WHERE oid = to_regclass('queue') AND relkind IN ('r', 'p')"
        )
        if cur.fetchone() is not None:
            return
        cur.execute("SHOW search_path")
        log.error(
            "No queue table on search_path",
            search_path=cur.fetchone()[0],
            hint="check the connection's PGOPTIONS",
        )
    raise QueueTableNotFoundError(
        "no 'queue' table on search_path; check the connection's PGOPTIONS "
        "(a view or matview of that name does not count)"
    )


def _partition_attached(conn: psycopg2.extensions.connection, name: str) -> bool:
    """True if `name` is a partition of this schema's queue.

    Bound via to_regclass so it answers about the queue this search_path
    resolves, not a same-named table in another schema.
    """
    with _tx(conn) as cur:
        cur.execute(_ATTACHED_SQL, (name,))
        return cur.fetchone() is not None


def _ensure_partition(conn: psycopg2.extensions.connection, target: date, required: bool) -> str:
    """Make the partition for `target` exist. Returns created/present/absent.

    The distinction is real here, not a guess: the catalogue is consulted before
    the CREATE, so "created" means this call made it. That costs one round trip
    on a path that only runs when maintenance has already fallen behind, and it
    buys a self-heal count that means what it says.

    Neither failure mode proves anything on its own, so neither is interpreted:
    DuplicateTable can be a same-named relation that is not a partition of this
    queue, and LockNotAvailable can be ALTER TABLE or DROP TABLE holding the
    parent rather than another creator -- ensure_claim_token_column's lock
    timeout is longer than this one, so it can outlast us. Ask the catalogue
    instead of guessing.

    Success is not interpreted either. CREATE TABLE IF NOT EXISTS raises nothing
    when the name is already taken by a relation that is not a partition of this
    queue -- it emits a NOTICE and skips -- so the statement returning cleanly is
    not evidence the partition exists. migrate.py creates exactly those shadow
    names (queue_YYYY_MM_DD as partitions of queue_new), and partitions.py guards
    the identical statement the same way.

    `required` marks the partition the caller actually needs. A speculative one
    must never fail the caller's write: its whole purpose is to save a later
    call, and failing now to save a hypothetical later failure is a bad trade.
    """
    name = _partition_name(target)
    try:
        with _tx(conn, lock_timeout_ms=PARTITION_LOCK_TIMEOUT_MS) as cur:
            # Ask before creating, not only after. Two things depend on knowing
            # whether the partition was already there, and CREATE TABLE IF NOT
            # EXISTS cannot tell us afterwards -- it succeeds either way:
            #
            #  - the self-heal marker. _create_partition_for also ensures
            #    tomorrow, which usually exists already, so stamping
            #    unconditionally labels partitions maintenance created as
            #    self-heals. health() reads that count to decide whether
            #    maintenance is dead, so a false stamp is a false alarm that
            #    never clears -- COMMENT is permanent.
            #  - COMMENT ON TABLE requires ownership. On a healthy partition
            #    owned by another role, stamping unconditionally raises
            #    InsufficientPrivilege and fails the caller's write, where
            #    doing nothing would have succeeded.
            cur.execute(_ATTACHED_SQL, (name,))
            if cur.fetchone() is not None:
                return "present"

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {name} PARTITION OF queue
                FOR VALUES FROM ('{target}') TO ('{target + timedelta(days=1)}')
                """
            )
            cur.execute(_ATTACHED_SQL, (name,))
            if cur.fetchone() is None:
                raise PartitionNotAttachedError(
                    f"{name} exists but is not a partition of this queue"
                )
            # Only now: this partition is ours and we are the one who made it.
            cur.execute(f"COMMENT ON TABLE {name} IS %s", (SELF_HEALED_MARKER,))
        return "created"
    except PartitionNotAttachedError:
        if required:
            raise
        log.warning("Partition name is taken by something else", partition=name)
        return "absent"
    except (psycopg2.errors.DuplicateTable, psycopg2.errors.LockNotAvailable):
        if _partition_attached(conn, name):
            return "present"
        if required:
            raise
        return "absent"
    except psycopg2.Error:
        # Disk, permissions, a deadlock. The caller's own partition must surface
        # that; a speculative one must not take the write down with it.
        if required:
            raise
        log.warning("Could not pre-create tomorrow's partition", partition=name)
        return "absent"


def _create_partition_for(conn: psycopg2.extensions.connection, day: date) -> None:
    """Ensure the partitions covering `day` and the day after, in this schema.

    `day` must come from the server (see server_today), never from this process.

    Tomorrow is created in the same pass so a retry that crosses midnight does
    not find itself missing a partition again; that removes the race rather than
    narrowing it. Tomorrow is inside maintenance's normal +3 horizon, so this
    cannot push one schema's partitions past another's -- which the shared-image
    coupling forbids.

    Reaching here at all means scheduled maintenance is not keeping up, so the
    outcome is logged whatever happens: a silent self-heal would let that stay
    true for weeks, which is the failure this module exists to prevent.
    """
    outcomes = {}
    try:
        for offset in (0, 1):
            target = day + timedelta(days=offset)
            outcomes[_partition_name(target)] = _ensure_partition(
                conn, target, required=(offset == 0)
            )
    finally:
        log.warning(
            "Partition self-heal ran from the write path",
            outcomes=outcomes,
            hint="partition maintenance is not keeping up",
        )


def _is_missing_partition(exc: psycopg2.Error) -> bool:
    """Distinguish "no partition for this row" from a real CHECK violation.

    Both raise CheckViolation (SQLSTATE 23514), and queue carries
    queue_new_valid_status. A named constraint is a genuine violation; the
    routing failure names none. Keyed on that rather than the message text,
    which is locale-dependent and reworded between Postgres versions.
    """
    return not getattr(exc.diag, "constraint_name", None)


def is_dedup_value(value: Any) -> bool:
    """True if the queue can deduplicate on this value.

    One predicate rather than the rule written twice: inspect.Failure.ref()
    promises to return "exactly the values the queue will let you dedup on", and
    a promise like that decays the moment the two copies can drift.

    bool is excluded even though isinstance(True, int) is True, because
    str(True) is "True" while Postgres jsonb ->> yields "true" -- a mismatch
    that makes dedup silently never fire. Floats and containers diverge the same
    way; rejecting them loudly beats a comparison that quietly never matches.
    """
    return isinstance(value, _DEDUP_VALUE_TYPES) and not isinstance(value, bool)


def _validate_dedup(dedup_key: str, payload: dict[str, Any]) -> str:
    """Check the dedup key is a usable identifier and its value is comparable."""
    if not _IDENTIFIER_RE.match(dedup_key):
        raise QueueError(f"dedup_key {dedup_key!r} is not a valid identifier")
    if dedup_key not in payload:
        raise QueueError(f"dedup_key {dedup_key!r} is absent from the payload")
    value = payload[dedup_key]
    if not is_dedup_value(value):
        raise QueueError(
            f"dedup_key {dedup_key!r} holds {type(value).__name__}; "
            "only str and int dedup cleanly against jsonb text output"
        )
    return str(value)


def enqueue(
    conn: psycopg2.extensions.connection,
    queue_name: str,
    payload: dict[str, Any],
    priority: int = 0,
    dedup_key: str | None = None,
    commit: bool = True,
) -> int | None:
    """Insert a job, returning its id, or None if an identical job is queued.

    None means "the work is already covered", which is success. Failure raises.
    Callers that must not proceed unless the work is queued should treat only the
    exception as failure.

    `dedup_key` names a payload field holding a str or int. Producers of the same
    key are serialised on an advisory lock held to end of transaction: the
    partial unique indexes cannot backstop this, because partitioning forces
    created_at into every unique key and two concurrent producers get different
    timestamps.

    A missing partition for today is created and the insert retried once.

    commit=False leaves the transaction to the caller, for work that must land
    atomically with the job. It also gives up the partition self-heal: creating a
    partition requires committing, which would commit the caller's pending work
    behind their back. The CheckViolation propagates instead, the caller rolls
    back, and the next ordinary enqueue or maintenance run creates the partition.
    """
    dedup_value = _validate_dedup(dedup_key, payload) if dedup_key is not None else None

    try:
        return _insert(conn, queue_name, payload, priority, dedup_key, dedup_value, commit)
    except psycopg2.errors.CheckViolation as exc:
        if not commit or not _is_missing_partition(exc):
            raise

    _create_partition_for(conn, server_today(conn))
    # Exactly one retry. Anything still failing is not a partition problem.
    return _insert(conn, queue_name, payload, priority, dedup_key, dedup_value, commit)


def _insert(
    conn: psycopg2.extensions.connection,
    queue_name: str,
    payload: dict[str, Any],
    priority: int,
    dedup_key: str | None,
    dedup_value: str | None,
    commit: bool = True,
) -> int | None:
    """Do the insert itself, honouring dedup. Caller owns partition recovery."""
    with _tx(conn, commit=commit) as cur:
        if dedup_key is None:
            cur.execute(
                "INSERT INTO queue (queue_name, payload, priority) "
                "VALUES (%s, %s, %s) RETURNING id",
                (queue_name, Json(payload), priority),
            )
            row = cur.fetchone()
            return row[0] if row else None

        # Key the lock on queue AND field AND value: keying on the value alone
        # would serialise unrelated queues against each other.
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"{queue_name}:{dedup_key}:{dedup_value}",),
        )
        cur.execute(
            """
            INSERT INTO queue (queue_name, payload, priority)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM queue
                WHERE queue_name = %s
                  AND payload->>%s = %s
                  AND status IN ('pending', 'processing')
            )
            RETURNING id
            """,
            (queue_name, Json(payload), priority, queue_name, dedup_key, dedup_value),
        )
        row = cur.fetchone()
        return row[0] if row else None


def worker_identity(service: str) -> str:
    """A claim token for one worker process: service, host and pid.

    claim() requires a `worker` and deliberately has no default, because a
    shared one would make every claimant indistinguishable and the token would
    stop discriminating. That leaves each consumer to invent a format, and four
    services inventing four formats is the drift this package exists to stop --
    so the format lives here, once.

    What it has to be: unique per process (host plus pid is), stable for that
    process's life (both are), and legible in a log line when someone is trying
    to work out which container is sitting on a job. In a container the hostname
    is the container id, which is exactly what an operator needs to `docker logs`.
    """
    if not service:
        raise QueueError("service is required; it is what makes the token legible")
    return f"{service}@{socket.gethostname()}:{os.getpid()}"


def claim(
    conn: psycopg2.extensions.connection,
    queue_name: str,
    worker: str,
    limit: int = 1,
    visibility_timeout_min: int = DEFAULT_VISIBILITY_TIMEOUT_MIN,
) -> list[dict[str, Any]]:
    """Claim up to `limit` ready jobs, recovering abandoned ones first.

    `worker` identifies the claimant and is required: it is the token every
    later report is matched against, and a shared default would make all
    claimants indistinguishable.

    Stale recovery does NOT consume an attempt -- see the module docstring. A row
    whose claimant died returns to pending with its budget intact; only a row
    that has already spent its attempts through explicit failures is retired.

    Recovered rows become claimable on the NEXT call, not this one: a
    data-modifying CTE is invisible to its siblings.
    """
    if not worker:
        raise QueueError("worker is required; it is the claim token")

    with _tx(conn) as cur:
        cur.execute(
            """
            WITH reset_stale AS (
                UPDATE queue
                SET status = 'pending', claimed_at = NULL, claimed_by = NULL
                WHERE queue_name = %(q)s AND status = 'processing'
                  AND claimed_at < NOW() - (INTERVAL '1 minute' * %(vis)s)
                  AND attempts < max_attempts
            ),
            retire_exhausted AS (
                UPDATE queue
                SET status = 'failed', claimed_at = NULL, claimed_by = NULL,
                    last_error = COALESCE(last_error, 'attempts exhausted')
                WHERE queue_name = %(q)s AND status = 'processing'
                  AND claimed_at < NOW() - (INTERVAL '1 minute' * %(vis)s)
                  AND attempts >= max_attempts
            ),
            claimable AS (
                SELECT id, created_at FROM queue
                WHERE queue_name = %(q)s AND status = 'pending'
                  AND (next_attempt_at IS NULL
                       OR next_attempt_at <= statement_timestamp())
                  AND attempts < max_attempts
                ORDER BY priority DESC, created_at
                LIMIT %(lim)s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE queue q
            SET status = 'processing', claimed_at = NOW(), claimed_by = %(w)s
            FROM claimable c
            WHERE q.id = c.id AND q.created_at = c.created_at
            RETURNING q.id, q.queue_name, q.payload, q.attempts, q.priority,
                      q.created_at
            """,
            {"q": queue_name, "vis": visibility_timeout_min, "lim": limit, "w": worker},
        )
        return [
            {
                "id": r[0],
                "queue_name": r[1],
                "payload": r[2],
                "attempts": r[3],
                "priority": r[4],
                # Free: partitioning forces created_at into the primary key, so
                # the CTE already joins on it and the row is already in hand.
                # Without it a consumer that needs the age of the work -- to
                # route it, to report it, to decide it is too old to bother
                # with -- must issue a second query per claimed row. Verified
                # against PG16: EXPLAIN is byte-identical with and without it.
                #
                # It is for READING. Together with `id` it is the whole primary
                # key, which makes a partition-pruned UPDATE easy to write and
                # tempting -- and any such UPDATE bypasses the claim token,
                # which is the one thing standing between a stalled worker and
                # reporting on a row somebody else has since claimed. Report
                # through complete()/release()/fail_or_retry(); they match on
                # claimed_by and tell you when you have lost the row.
                #
                # It is also a SERVER timestamp. Comparing it to a local
                # datetime.now() is two clocks; ask the server for the
                # comparison, as everything else in this package does.
                "created_at": r[5],
            }
            for r in cur.fetchall()
        ]


def complete(conn: psycopg2.extensions.connection, job_id: int, worker: str) -> bool:
    """Mark a claimed job done. False if the claim was no longer ours."""
    with _tx(conn) as cur:
        cur.execute(
            "UPDATE queue SET status = 'completed', completed_at = NOW() "
            "WHERE id = %s AND status = 'processing' AND claimed_by = %s",
            (job_id, worker),
        )
        # id alone, not the full (id, created_at) key: this is one statement, so
        # there is no window to lose the claim in, and the shared sequence keeps
        # id unique across partitions. fail_or_retry needs created_at because it
        # reads first and writes second.
        held = cur.rowcount > 0
    if not held:
        log.warning("complete() bounced: claim no longer held", job_id=job_id, worker=worker)
    return held


def release(
    conn: psycopg2.extensions.connection,
    job_id: int,
    delay_s: int,
    worker: str,
) -> bool:
    """Hand a claimed job back unharmed, deferred by `delay_s`.

    Attempts are NOT consumed. This is the primitive for "the work never
    started" -- auth is dead, a dependency is unavailable, a precondition is not
    met yet. Using fail_or_retry() there charges the work for an outage, which is
    how four healthy videos reached terminal on 2026-08-18.

    False if the claim was no longer ours.
    """
    with _tx(conn) as cur:
        cur.execute(
            "UPDATE queue SET status = 'pending', claimed_at = NULL, claimed_by = NULL, "
            "next_attempt_at = clock_timestamp() + (INTERVAL '1 second' * %s) "
            "WHERE id = %s AND status = 'processing' AND claimed_by = %s",
            (delay_s, job_id, worker),
        )
        # id alone, not the full (id, created_at) key: this is one statement, so
        # there is no window to lose the claim in, and the shared sequence keeps
        # id unique across partitions. fail_or_retry needs created_at because it
        # reads first and writes second.
        held = cur.rowcount > 0
    if not held:
        log.warning("release() bounced: claim no longer held", job_id=job_id, worker=worker)
    return held


def fail_or_retry(
    conn: psycopg2.extensions.connection,
    job_id: int,
    error: object,
    worker: str,
    base_seconds: int = DEFAULT_BASE_SECONDS,
    cap_seconds: int = DEFAULT_CAP_SECONDS,
    jitter_ratio: float = DEFAULT_JITTER_RATIO,
) -> Report:
    """Charge an attempt and either reschedule or retire the job.

    Returns "pending", "failed", or "stale" when the claim was no longer ours.
    Call this only when the work was attempted and failed; for "never started",
    use release().

    This reads attempts and then writes, rather than letting one UPDATE with a
    CASE decide -- which is the shape the database should normally arbitrate.
    The exception is deliberate: expressing the backoff in SQL would put a second
    copy of compute_backoff_delay in a second language, and two consumers already
    share the Python one. FOR UPDATE holds the row for the round trip, so the
    read-modify-write is atomic; the cost is one round trip to keep the backoff
    in a single place.
    """
    truncated = str(error)[:ERROR_MAX_CHARS]
    with _tx(conn) as cur:
        cur.execute(
            "SELECT attempts, max_attempts, created_at FROM queue "
            "WHERE id = %s AND status = 'processing' AND claimed_by = %s FOR UPDATE",
            (job_id, worker),
        )
        row = cur.fetchone()
        if row is None:
            log.warning(
                "fail_or_retry() bounced: claim no longer held",
                job_id=job_id,
                worker=worker,
            )
            return "stale"

        attempts = (row[0] or 0) + 1
        max_attempts = row[1]
        created_at = row[2]

        if attempts >= max_attempts:
            cur.execute(
                "UPDATE queue SET status = 'failed', attempts = %s, last_error = %s, "
                "claimed_at = NULL, claimed_by = NULL, next_attempt_at = NULL "
                "WHERE id = %s AND created_at = %s",
                (attempts, truncated, job_id, created_at),
            )
            log.error(
                "Job retired after exhausting attempts",
                job_id=job_id,
                attempts=attempts,
                worker=worker,
            )
            return "failed"

        delay = compute_backoff_delay(
            attempts,
            base_seconds=base_seconds,
            cap_seconds=cap_seconds,
            jitter_ratio=jitter_ratio,
        )
        cur.execute(
            "UPDATE queue SET status = 'pending', attempts = %s, last_error = %s, "
            "claimed_at = NULL, claimed_by = NULL, "
            "next_attempt_at = clock_timestamp() + (INTERVAL '1 second' * %s) "
            "WHERE id = %s AND created_at = %s",
            (attempts, truncated, delay, job_id, created_at),
        )
        log.warning(
            "Job failed, scheduled for retry",
            job_id=job_id,
            attempts=attempts,
            max_attempts=max_attempts,
            retry_in_s=delay,
            worker=worker,
        )
        return "pending"
