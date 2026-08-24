# `cortex_utils.queue` — API reference

The shared queueing layer. Every service that touches a queue goes through this
module; nothing writes its own SQL against the `queue` tables.

Two schemas in one Postgres database currently use it — `public` (cortex) and
`cryo` — with a partitioned `queue` in each. Nothing in this API names a schema.
Which queue you operate on is decided entirely by the connection's
`search_path`, so a consumer sets `PGOPTIONS=-c search_path=<schema>` (or
`SET search_path`) and everything below follows it.

---

## Install

```
cortex-utils @ git+https://github.com/devonjones/cortex-utils.git@<sha>
```

Not on PyPI and not in Artifact Registry — a public GitHub repo installed by
URL, so `pip install -r requirements.txt` needs no registry auth. Your image
needs `git`.

**Dependency weight, from cryo's integration.** Installing this for the queue
alone pulls the whole package's base dependencies — `docker`, `schedule`,
`prometheus-client`, `pydantic`, `httpx`, `pyyaml`, `click` — into the image.
`src/cortex_utils/queue/` itself imports only `psycopg2` and `structlog`, plus
stdlib. cryo's daemon went from four dependencies to roughly thirty by adding
this line.

That is not a blocker and cryo shipped it, but a `queue` extra with a lighter
base would make the package cheaper for a consumer that wants one subsystem.
Splitting it needs an audit of what the *other* modules genuinely require,
which is yours to do — recording the observation and the evidence rather than
guessing at the split from outside.

**Pin a SHA, not a branch.** Existing consumers pin different SHAs on purpose,
so each upgrades on its own cadence. `version` in `pyproject.toml` has said
`0.1.0` since the repo began and is vestigial; the SHA is the only honest
version until we tag releases.

---

## The rule

**Unless there is an app-level reason to join queue data against another table,
`cortex_utils` owns all SQL that talks to the queue database.**

Not a style preference. Every incident this package has been shaped by came
from a second copy of some query or shape existing somewhere else and drifting:
partition lookups that matched a same-named table in another schema and cost
4.8 days of email; a `queue_new` DDL missing three columns the primitives
require; a dead-letter retry that deleted the record it existed to keep. One
owner is the only arrangement where a fix reaches everyone.

So the practical consequence, for a consumer and for cortex's own services
alike:

- **If you are writing SQL against the queue, that is a gap here.** Report it
  rather than writing it. Every gap reported so far turned out to be a real
  missing primitive, and several turned out to be live bugs on this side.
- **A join against your own tables is the exception**, because this package
  cannot know your schema. Read what you need through the API, join in your own
  query, and keep the queue side of it here.
- **Schema included.** `ensure_queue_table()` owns the shape; hand your extra
  indexes to it rather than keeping a private migration alongside it.

## The five primitives

```python
from cortex_utils.queue import (
    enqueue, claim, complete, release, fail_or_retry, QueueError,
)
```

### `enqueue(conn, queue_name, payload, priority=0, dedup_key=None, commit=True) -> int | None`

Returns the new job id, or `None` if a duplicate was suppressed. **Failures
raise.** That split matters: an emit-before-pop producer can treat only the
exception as failure and a `None` as "already queued", without a second query.

`payload` is a `dict` and is wrapped for you. Passing a raw dict to psycopg2
yourself raises `can't adapt type 'dict'` — a bug that made `dead-letter retry`
fail on every non-dry-run call for as long as it existed.

`priority` is higher-first. Use `0` for live traffic and `-100` for backfill, so
a historical replay never blocks real-time work.

`dedup_key` names a field *inside* the payload; the value at that key is what
gets deduplicated. It is validated as an identifier and bound as a parameter.
Values must be `str` or `int` — other types are rejected loudly rather than
silently failing to match. (`str(True)` is `"True"`, Postgres jsonb `->>` gives
`"true"`; the mismatch makes dedup never fire while reporting success.)

Deduplication uses a transaction-scoped advisory lock keyed
`queue_name:dedup_key:value`, so unrelated queues do not serialise against each
other. A unique index cannot do this job: partitioning forces `created_at` into
every unique key, so two concurrent producers get different timestamps and both
inserts succeed.

`commit=False` runs inside your transaction. You then own the commit — and you
forfeit the partition self-heal below, because it needs to commit DDL.

### `claim(conn, queue_name, worker, limit=1, visibility_timeout_min=30) -> list[dict]`

`FOR UPDATE SKIP LOCKED`, so N workers polling the same queue never collide.

Each dict has `id`, `queue_name`, `payload`, `attempts`, `priority`,
`created_at`.

`created_at` is there so you do not have to ask a second time — it is half the
primary key, so the claim already had it. **It is for reading.** Together with
`id` it is the whole key, which makes a partition-pruned `UPDATE` easy to write
and tempting; any such write bypasses the claim token, which is the one thing
standing between a stalled worker and reporting on a row somebody else has since
claimed. Report through `complete()` / `release()` / `fail_or_retry()`. It is
also a *server* timestamp, so compare it server-side rather than against a local
`datetime.now()`.

**`worker` is required and must be non-empty** — `QueueError` otherwise. It is
the claim token, and it is the whole point: `complete()` and `release()` match
on it, so a worker that stalled past its visibility timeout cannot report on a
row another worker has since re-claimed. Pass a stable per-process identity;
hostname + pid is fine. An empty or `NULL` worker makes every caller anonymous
and defeats this entirely.

Rows past `visibility_timeout_min` are reset to `pending` as a side effect, so a
*later* claim can pick them up. They are not returned by the call that recovers
them.
**Recovery does not consume an attempt.** Only `fail_or_retry()` spends the
budget. This is deliberate: an expired OAuth token kills a worker, the row comes
back with its budget intact, and the work runs when auth returns — rather than
burning three attempts on an outage that was never about the job.

### `complete(conn, job_id, worker) -> bool`

`False` means the claim was lost — the row was re-claimed while you worked, so
your result is stale and must not be published.

### `release(conn, job_id, delay_s, worker) -> bool`

Put it back without spending an attempt. Use this when the work **never
started**: auth dead, dependency unavailable, precondition unmet. `False` means
the claim was lost.

### `fail_or_retry(conn, job_id, error, worker, base_seconds=30, cap_seconds=900, jitter_ratio=0.2) -> Report`

Use this when the work **was attempted and failed**. Spends an attempt, then:

| Return | Meaning |
|---|---|
| `"pending"` | Retrying, with exponential backoff plus jitter |
| `"failed"` | Attempts exhausted; the row is retired |
| `"stale"` | The claim was lost — same condition as `False` above |

The backoff constants are the library's, overridable per call. Do not
hand-copy them.

> **Migration trap.** `cortex_utils.queue.retry.fail_or_retry` is a different,
> older function that returns `"retrying"` where this one returns `"pending"`.
> Swapping the import without updating an `== "retrying"` check yields a
> comparison that is **silently always false**.
>
> It has **five production callers** — `triage`'s `worker.py`, `labeling_worker.py`
> and `teach_bot.py`, and `postmark`'s `parse_worker.py` and
> `attachment_worker.py` — all on its older signature, where `max_attempts` is
> the 4th positional argument rather than `error`. `teach_bot.py` branches on
> the return value, so it is the one that breaks silently rather than loudly.
>
> Do not import it in new code. Migrating those five is tracked as cortex-i5jc.

---

## Reading the queue

```python
from cortex_utils.queue import health, stuck, failures, resubmit
```

These touch Postgres and nothing else — no worker, no scheduler, no bus. A
dashboard that depends on the pipeline dies alongside the thing it reports on.

### `health(conn) -> QueueHealth`

The whole overview in **one round trip**, cheap enough to poll.

```python
QueueHealth(
    depths: list[QueueDepth],
    dead_letter: int,
    partition_headroom_days: int | None,
    self_healed_partitions: int,
    server_time: datetime,
)
```

`.is_healthy` is a single boolean for a monitor to alert on. It covers
**partition health only** — headroom and self-heals. It deliberately says
nothing about `depths` or `dead_letter`, because there is no queue depth that is
universally wrong; alert on those with thresholds that suit your workload.

`partition_headroom_days` counts days of **contiguous** coverage after today —
not the furthest partition bound. A partition for today and one for +7 with a
gap between is `0`, not `7`, because the insert on the first uncovered day
fails. So `0` means tomorrow's writes already fail, and `None` means today is
uncovered and writes are failing now. A monitor should be able to tell those
apart, which is why the second is not reported as `-1` or `0`.

`self_healed_partitions` counts partitions the *write path* had to create
because scheduled maintenance had stopped. Non-zero means maintenance is dead;
the enqueue succeeded anyway, which is exactly why it needs surfacing rather
than swallowing.

`QueueDepth` reports `ready` and `deferred` **separately** on purpose. Collapsed
into one "pending" number, six rows all backing off for another hour read as a
working queue with a backlog when they are a queue in retry storm.
`oldest_ready_age_s` separates "four things arrived this minute" from "four
things have been stuck since Tuesday".

### `stuck(conn, visibility_timeout_min=30, limit=50) -> list[StuckJob]`

Rows claimed longer ago than the visibility window. `claimed_by` is the useful
field: it separates "a worker is chewing on this" from "a worker died holding it
and the row is waiting out its timeout".

### `failures(conn, limit=50, queue_name=None) -> list[Failure]`

Failed rows, newest first. **`last_error` is returned whole** — never truncated
or summarised on the way out. Fourteen rows all reading `visibility timeout,
attempts exhausted` is the signal that the cause was infrastructure and not
fourteen unrelated content failures, and that uniformity is visible only in the
raw text.

(`fail_or_retry()` does cap what it *writes* at `ERROR_MAX_CHARS` = 2000, so a
stack trace longer than that was already cut before it reached the row. This
call adds no further loss.)

### `resubmit(conn, job_id, dedup_key=None) -> int | None`

Re-queue a failed job. Enqueues the payload fresh (landing in today's partition)
and marks the original **cancelled**, both in one transaction.

Deliberately not a status flip back to `pending`: that leaves `created_at`
unchanged, so the revived row stays in an old partition — and retention drops
partitions on age, not status. The job would be on a clock nobody intended and
could vanish mid-flight. The original is cancelled rather than deleted because a
failure list whose entries disappear when someone retries them defeats its own
purpose.

---

## Partitions

```python
from cortex_utils.queue import PartitionManager
PartitionManager(conn).maintain(retention_days=7, days_ahead=3)
```

Daily `RANGE` partitions on `created_at`, named `queue_YYYY_MM_DD`. Retention is
`DROP PARTITION` — O(1), no vacuum. `maintain()` creates ahead, archives failed
rows to `dead_letter`, then drops.

**There is no `DEFAULT` partition, and there must not be.** Rows in a default
partition are never in a dropped one, so it converts a loud failure into a
silent leak.

**`enqueue()` self-heals.** A missing-partition `CheckViolation` is caught,
today's partition is created, and the insert is retried once. It logs a
**warning** when this fires — reaching it means maintenance is dead, and a
silent self-heal hides that for weeks.

If you catch the missing-partition case yourself, key on
`exc.diag.constraint_name`, not on the message. `queue_new_valid_status` raises
the same SQLSTATE 23514, so a blanket catch creates a partition nobody needed
and misreports a bad `status` as a missing partition. A genuine violation names
its constraint; a routing failure does not. That survives locale changes and
Postgres rewording; a message match does not.

---

## Rules this library holds itself to

You get these for free; they are here so you can tell when something is wrong.

**The database is the arbiter.** Nothing infers state from an exception. Losing
a lock race or hitting `DuplicateTable` proves a name is taken, not that the
partition exists — so the code asks `pg_inherits` rather than concluding. The
same applies to success: `CREATE TABLE IF NOT EXISTS ... PARTITION OF` raises
nothing when the name belongs to a non-partition relation, so a clean return is
not evidence either.

**Every catalogue lookup binds `to_regclass('queue')`.** A bare
`pg_class.relname = 'queue'` matches a same-named table in *any* schema. That
one is not hypothetical: it reported cortex's partitions as present when they
were cryo's and cost 4.8 days of silently dropped email.

**Every date and window comes from the server clock.** `created_at` is
`NOW()`, produced by the server, so any date that routes or creates a partition
must come from the same clock. A client `date.today()` agrees only by
coincidence — it tests green wherever both are UTC and fails on the first
deployment where they are not. Windows are `NOW() - (INTERVAL '1 second' * %s)`,
never a Python cutoff.

**Nothing reports an outcome it did not confirm.** A `DELETE` whose `rowcount`
is never read is the same defect as a success-reporting `except`, with no
exception to grep for.

---

## The table

```python
from cortex_utils.queue import ensure_queue_table, missing_columns, queue_ddl

ensure_queue_table(conn)     # on boot; idempotent. "created" or "present"
missing_columns(conn)        # [] means the shape is compatible
```

The table shape **is** the contract — every primitive compiles assumptions
about the status values, `attempts`/`max_attempts`, `next_attempt_at`,
`claimed_by` and the partition key. Maintaining your own copy means maintaining
half of an interface whose other half lives here, and the two drift silently.

`ensure_queue_table()` **only ever creates.** None of the SQL this package
emits for it is a `DROP` or an `ALTER`. (Statements you pass in `extra_indexes`
run verbatim — it does not inspect them, so that guarantee covers our half, not
yours.) If the table exists and is missing columns the primitives need, it
raises and names them, plus the migration for the two that have one — adding them silently
would take `ACCESS EXCLUSIVE` on a live queue, and this package cannot tell a
table that predates a column from one you shape deliberately.

### What it owns, and what stays yours

It owns the columns in `REQUIRED_COLUMNS`, the status `CHECK`, the partition
key, and exactly two indexes: `idx_queue_claim` and `idx_queue_stale`, which
`claim()`'s own queries need.

**Hand your own indexes over rather than keeping a private migration.** If you
need more — a per-queue partial index, a payload expression index — pass them:

```python
ensure_queue_table(conn, extra_indexes=[
    ("idx_queue_dedup_video",
     "CREATE INDEX IF NOT EXISTS idx_queue_dedup_video ON queue "
     "((payload->>'video_id')) WHERE queue_name = 'drain'"),
])
```

They get the same discipline as the canonical ones: the catalogue is asked
before each `CREATE INDEX`, because `IF NOT EXISTS` still takes a lock and waits
on an open writer even when the index is already there, and this runs on every
boot.

This matters at the moment of adoption. `ensure_queue_table()` only ever
creates — it will not remove an index you made — but a consumer that adopts it
and *deletes its own DDL* loses those indexes on any fresh deployment, and the
absence is silent until the query they served turns slow.

`missing_columns()` treats extra columns as fine. Composing on top is the point;
only absence is a problem.

> **A partial unique index is not a dedup backstop.** Worth stating because it
> reads like one. A unique index on a partitioned table must include the
> partition key, so the key becomes `(your_field, created_at)`. `NOW()` is
> `transaction_timestamp()`, so two inserts in *one* transaction share a
> timestamp and the second is rejected — but two producers in *separate*
> transactions get different ones and both rows satisfy the index. Concurrent
> producers are the case dedup exists for. Use `dedup_key`, which takes a
> transaction-scoped advisory lock; keep the index for lookups if it earns its
> place.

### Naming

Names are checked against `pg_index` bound to this table, not against
`to_regclass`. A name that resolves to a table, a sequence, or an index on some
*other* relation does not count as your index being present — otherwise the
`CREATE` is skipped forever and the index silently never exists. The statement
is also re-probed immediately after it runs, so a name that disagrees with what
the statement actually creates is refused at the one moment that is provable
rather than becoming a lock on every boot.

Canonical indexes are applied first, so an extra that collides with
`idx_queue_claim` is discarded rather than taking it over.

The canonical indexes are deliberately **not** `idx_queue_pending` or
`idx_queue_processing`. `migrate.py` already creates indexes under both of those
names with different column lists, and renames them onto `queue` — so they are
indexes on this table and the probe finds them. Reusing either name would mean
the canonical index is silently never created on exactly the deployments that
have been around longest, and a `claim()` that has quietly stopped having an
index to use.
Pick names of your own that collide with neither set.

---

## Setup

```python
from cortex_utils.queue import ensure_queue_schema

ensure_queue_schema(conn)     # on boot, before anything touches the queue
```

One call, idempotent, cheap on the steady-state path — every step pre-checks the
catalogue before touching anything, so a normal boot is a handful of reads and
no locks.

It runs the additive migrations first (`next_attempt_at`, `claimed_by`), then
the shape check, then ensures `dead_letter`. **That order is the point.**
`ensure_queue_table()` refuses to `ALTER` a live table, so reversed it would
raise on exactly the deployments the additive migrations exist to bring
forward. An old database is migrated; a new one is created complete.

Pass `extra_indexes=[...]` here the same way as to `ensure_queue_table()`.

> **Why one call rather than four.** A backoff feature was merged months before
> it first *ran* in production. Its migration existed and was correct — as a
> manual CLI step the deploy flow never invoked. Two workers crash-looped on
> `column "next_attempt_at" does not exist`. Nothing was wrong with the
> migration; nothing called it. Six services each remembering four calls in the
> right order is that incident waiting to recur.



Run once per schema, before first use:

```python
from cortex_utils.queue import DeadLetterManager, ensure_claim_token_column

ensure_claim_token_column(conn)
DeadLetterManager(conn).ensure_table()
```

(These are what `ensure_queue_schema()` calls for you. Reach for them
individually only if you have a reason to run one without the others.)

`ensure_claim_token_column` adds `claimed_by` if the table predates it. Pre-checks `pg_attribute` before the
`ALTER` and runs under a 5s `lock_timeout`, so a boot fails fast rather than
queueing `ACCESS EXCLUSIVE` behind live claim traffic. Idempotent — safe on
every boot.

`DeadLetterManager.ensure_table()` creates `dead_letter` and brings an existing
one up to date. `health()` reads that table, so without it the call fails.

---

## Dead letters

```python
from cortex_utils.queue import DeadLetterManager

dlm = DeadLetterManager(conn)
dlm.ensure_table()                       # on boot; idempotent
dlm.list_jobs(queue_name="triage")       # open items only
dlm.retry_job(dead_letter_id)            # re-enqueue, keep the record
dlm.dismiss(dead_letter_id)              # write off, keep the record
dlm.purge(older_than=timedelta(days=30)) # retention, by age
```

Failed rows are archived here before their partition is dropped. Ids in this
table are a **separate namespace** from queue ids — both number from 1 — so
every parameter says `dead_letter_id`.

**Retry keeps the archive row.** It stamps `retried_at` / `retried_as` rather
than deleting. That row is the record that work was given up on, when, after how
many attempts and with what error, and it is exactly the history you want when
the same item dies again. Retry refuses a row that was already retried: if the
retry itself failed, that failure archived a *new* row, and that is the one to
retry.

**`dismiss()` is the terminal state** — not destructive, and idempotent, because
the date answers *when did we stop caring about this* and moving it forward on a
stray re-dismissal destroys the only thing it records. Without a terminal state
the list only grows until real failures are buried in noise and nobody reads it.

**One number.** `list_jobs()`, `get_stats()` and `health().dead_letter` all
report the same set — open, meaning neither dismissed nor retried. `list_jobs`
and `get_stats` share an `include_resolved` flag. A page reporting 2 while a
digest reports 6 is worse than either being wrong alone.

**`purge()` is by age alone**, dismissed or not. `dismiss()` is the triage verb
and this is the housekeeping one; a row old enough to purge and still not
dismissed was never triaged, and keeping it forever would not fix that.

*This section answers cryo's G7, which is why it uses cryo's `dismissed_at`
name: an existing cryo schema satisfies the shared one without a rename.*

---

## Known limitations

**One concurrency branch is still unreached.** `SKIP LOCKED` not
double-claiming and advisory-lock serialisation now have live-Postgres
coverage — `tests/test_ops_live.py`, contributed by cryo, two real connections
genuinely raced, and CI fails if that layer silently skips.

What remains uncovered is `_ensure_partition`'s `DuplicateTable` handler.
Nothing reaches it: the mock tests synthesise the exception, and it could not be
provoked through `enqueue()` at all — an uncommitted `CREATE` on one connection
blocks the other on the parent's `ACCESS EXCLUSIVE` before it can reach
`CheckViolation`, and four producers behind a barrier came back clean 12 times
out of 12, one reporting `created` and the rest `present` because the pre-check
had already seen the winner's partition. Two sessions issuing the raw
`CREATE TABLE IF NOT EXISTS ... PARTITION OF` simultaneously *does* raise it
(40/40 `42P07`), so the branch is right — it is just not reachable from here.

**Claim-loss is encoded two ways** — `False` from `complete()`/`release()`,
`"stale"` from `fail_or_retry()` — so a worker loop cannot share a branch.
Unification is tracked as cortex-i5jc.

**`claim()` returns `list[dict[str, Any]]`,** not a `TypedDict`. The fields are
`id`, `queue_name`, `payload`, `attempts`, `priority`, `created_at` — that last
one is free, because partitioning forces it into the primary key so the CTE has
the row in hand anyway, and without it a consumer needing the age of the work
runs a second query per claimed row. Worth a `TypedDict` now the set has
settled.

**No LISTEN/NOTIFY.** Cortex polls. Keep any notify trigger consumer-side for
now; when it moves here it moves with a validated `channel` parameter.
