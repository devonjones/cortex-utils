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

## Setup

Run once per schema, before first use:

```python
from cortex_utils.queue import DeadLetterManager, ensure_claim_token_column

ensure_claim_token_column(conn)
DeadLetterManager(conn).ensure_table()
```

`ensure_claim_token_column` adds `claimed_by` if the table predates it. Pre-checks `pg_attribute` before the
`ALTER` and runs under a 5s `lock_timeout`, so a boot fails fast rather than
queueing `ACCESS EXCLUSIVE` behind live claim traffic. Idempotent — safe on
every boot.

`DeadLetterManager.ensure_table()` creates `dead_letter` and brings an existing
one up to date. `health()` reads that table, so without it the call fails.

---

## Open question: dead-letter replay deletes the archive (from cryo)

`DeadLetterManager.retry_job()` re-enqueues the payload and then **deletes** the
`dead_letter` row, both in one transaction. The atomicity is right — a crash
between the two would leave the job queued *and* still dead-lettered. The
question is the delete itself.

That row is the record that a piece of work was given up on, when, after how
many attempts, and with what error. Replay erases it, so the failure history
disappears at exactly the moment someone acts on it — and if the same item dies
again next week, the fact that it already died once is gone.

cryo hit this concretely. Its 2026-08-18 outage looked like it had cost four
videos; two more were already in `dead_letter` with the identical
`visibility timeout, attempts exhausted`, invisible to every `queue` query, and
were only found because someone thought to ask. Replaying them under
`retry_job()` would have removed the only evidence they had ever failed.

There is also no **terminal state**. `purge(older_than)` is retention, not a
decision: nothing records "this will never be done". Without one the list only
grows, genuinely-undoable items accumulate, real failures end up buried in
noise, and a triage list nobody can clear stops being read.

cryo therefore keeps its own dead-letter layer rather than adopting
`DeadLetterManager`, which by the shared-library rule means this is a gap
rather than a boundary. What cryo does, offered as one possible shape:

- **replay leaves the row in place**, and the row's own state is what stops a
  second replay. `enqueue()`'s dedup does *not* cover this: dedup is opt-in via
  `dedup_key` and `retry_job()` passes none, so with the delete gone and nothing
  else added, a second sweep re-enqueues everything the first put back and
  overwrites the record of where it went. (Verified against live Postgres: three
  dead letters, two sweeps, six queue rows.)
- **`dismissed_at TIMESTAMPTZ`** is the terminal state: idempotent, so
  re-dismissing keeps the date it was actually written off, and the row stays
  in the table as history while leaving the default view.
- **the depth counter counts undismissed rows only**, so the number on a
  dashboard and the number in a digest cannot disagree.

Deciding this is yours — the counter-argument (a dead-letter table that only
grows is its own problem, and `purge` is the answer) is reasonable. Recording
it because cryo cannot adopt the module until it is settled.

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
