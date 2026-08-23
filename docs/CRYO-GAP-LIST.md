# Gaps cryo still fills with its own SQL

**From:** cryo's Claude · **Date:** 2026-08-23 · **Re:** `CRYO-QUEUE-ADOPTION.md`, cryo-64

Devon's rule, and it is the right one:

> we just implemented the entire queue lifecycle in cortex_utils. we should have
> basically no SQL for the queue part, as that points to gaps in the
> cortex_utils implementation.

So this is not a wish list. Every item below is SQL cryo is **currently
writing** against the shared queue, which means the shared module does not
cover the lifecycle yet. Read `## 8. What is still yours` in the adoption doc as
a gap list rather than a boundary.

Ordered by what blocks cryo's adoption.

---

## G1 — The DDL is not shared (P1)

**cryo has its own `migrate()` and its own `CREATE TABLE queue ... PARTITION BY
RANGE (created_at)`.** The adoption doc assigns that to cryo, but the table
shape *is* the shared contract: every one of the five primitives compiles
assumptions about it — `status` values, `attempts`/`max_attempts`,
`next_attempt_at`, `claimed_by`, the partition key, the partial indexes.

Two independently-maintained copies of a contract drift, and they drift
silently: a column added on one side is invisible to the other until a query
returns the wrong thing. `ensure_claim_token_column()` already concedes the
point — it exists precisely because the two schemas had drifted over
`claimed_by`.

**Ask:** the module owns the canonical DDL and a migration entry point. If a
consumer needs extras (cryo's per-queue partial unique dedup indexes), they
should compose on top of a shared base, not fork it.

---

## G2 — No observability primitive (P1)

cryo wrote `stats(conn)` because nothing shared answers "what is in the queue".
It returns counts by `queue_name` × `status`, `dead_letter` depth, and partition
headroom in days.

This is not a nicety. cryo lost 14 hours to an outage nobody saw because the
only failure channel was a digest that itself needed the workers to be alive.
A read path that touches **only postgres** keeps working precisely when the
pipeline does not.

Partition headroom deserves to be shared specifically: `days_ahead=3` is the
entire safety margin, and a 3-day margin is what let cryo's partitions lapse
unnoticed for two days. The query **must** bind `current_schema()` — a bare
`relname` lookup reports healthy from the other schema's partitions, which is
`cortex-jst7`, the defect that cost cortex 4.8 days of email.

---

## G3 — No failure-triage read (P1)

cryo wrote `failures(conn, limit)` — `status='failed'` rows with `last_error`,
newest first. There is no shared way to ask "what failed and why" without
hand-writing SQL, so every consumer that wants a dashboard writes it again.

---

## G4 — No requeue primitive (P1)

cryo wrote `resubmit(conn, job_id)`. The interesting part is that the obvious
implementation is wrong, which is exactly why it belongs in the library rather
than in each consumer:

- **Do not flip the failed row back to `pending`.** Its `created_at` is
  unchanged, so it stays in an old partition — which retention drops on age,
  not status. The revived row is on a clock nobody intended.
- **Do enqueue a new row from the old payload**, so it lands in today's
  partition, and let `enqueue()`'s advisory-lock dedup stop it double-queueing
  work that is already live again.
- **Mark the old row `cancelled`, never delete it.** The point of a failure
  list is that somebody can see what happened; a requeue that erases its own
  history defeats it.

---

## G5 — No readiness query (P2)

cryo wrote `ready_queues(conn)`: which `queue_name`s have at least one row
claimable *right now* (`status='pending'` AND `next_attempt_at` null or due).

Any event-driven consumer needs this. Polling `claim()` per queue to find out
whether there is work is the polling the queue was supposed to remove, and it
consumes visibility windows as a side effect of asking a question.

---

## G6 — LISTEN/NOTIFY, recorded so it moves intact (P3)

Deliberately declined for now (adoption doc §6.3) and cryo is fine owning it.
Logging it so the four properties travel with it when it moves — each was found
by review of cryo's implementation and each has a regression assertion:

1. **Refuse a connection with an open transaction.** `LISTEN` only takes effect
   at commit, so `wait()` must commit — and committing the *caller's* in-flight
   work as a side effect of going to sleep is silent data movement.
2. **Loop to a deadline, do not `select()` once.** A readable socket yielding no
   notification returns empty early, which a caller reads as "the timeout won"
   and answers by re-claiming immediately: tight-loop polling, the thing NOTIFY
   removes. Also drain buffered notifications *before* sleeping, or a notify
   that landed between the caller's empty claim and the wait is slept through.
3. **Refuse a schema with no trigger**, naming the migrate command. Without it
   `wait()` is correct but useless and every worker silently full-timeout polls.
4. **Refuse a trigger bound to a different channel.** The channel is baked into
   the stored function at `CREATE OR REPLACE` time, not resolved per NOTIFY, so
   a trigger that merely *exists* proves nothing. Check `pg_proc.prosrc`.

When it moves it should take a `channel` parameter **with validation**: the
value reaches both an f-string `LISTEN` (identifiers cannot be parameterised)
and a single-quoted literal inside the plpgsql body, where one quote escapes and
injects the trigger function.

---

# Defects found while reading `ops.py`

## D1 — `_create_partition_for(conn, date.today())` uses a client-side date (P2)

`ops.py:247`. `date.today()` is the **client's** local date; `created_at` is
`NOW()` on the **server**. When the two disagree, the self-heal creates the
wrong partition and the retry — exactly-once by design — raises for a partition
that was never missing.

Verified on ares today: both are UTC, `date.today() == CURRENT_DATE`, so it is
**not currently firing**. It is latent for any consumer whose container TZ is not
UTC, and independent of TZ it is reachable whenever a retry crosses local
midnight under lock contention.

**Suggested fix:** compute the date server-side, and create **today and
tomorrow** in one pass. That removes the race rather than narrowing it, and
tomorrow is inside maintenance's normal `+3` horizon so it cannot push one
schema's partitions past another's — which the shared-image coupling forbids.

cryo's `_ensure_partition_today()` on `cryo-63-partition-selfheal` does both, if
useful as a reference.

## D2 — No `lock_timeout` on the write-path partition DDL (P2)

`CREATE TABLE ... PARTITION OF` takes **ACCESS EXCLUSIVE on the parent**, which
blocks every insert and every `claim()` across all `queue_name`s while held.
`MIGRATION_LOCK_TIMEOUT_MS` bounds this on `ensure_claim_token_column`
(`ops.py:144`) but I could not find an equivalent on `_create_partition_for`,
which now runs on the **live producer path** rather than at deploy time.

It matters more than the raw wait: cryo's daemon calls this inside a process
mutex *and* a repo flock, so a long lock wait pins its whole mutation path.
`statement_timeout` does bound lock acquisition, but the retry path chains three
separately-timed statements, so the budget is ~3× what a reader expects.

A distinct `LockNotAvailable` is also better operator signal than a generic
timeout: if someone else holds the parent they are almost certainly creating
this very partition, so losing the race is success and the retry finds it there.

## D3 — Question, not a defect: rollback before the retry (P3)

`enqueue()` catches `CheckViolation` and calls `_create_partition_for` without an
explicit `conn.rollback()`. I assume `_tx(commit=True)` rolls back on exception —
I did not read far enough to confirm, and the aborted-transaction case is worth a
regression test either way, since the retry's `_insert` would fail with
`InFailedSqlTransaction` rather than anything describing the real cause.

---

# What cryo would keep after all of this

`connect()` (DSN and timeout policy), the `DEDUP_KEYS` map (per-queue config,
not mechanism), and `jobs/selfcheck_queue.sh`.

On that last one — adoption doc §7 says `ops.py` has 61 mock-based tests and
that `SKIP LOCKED` not double-claiming, advisory-lock serialisation, and the
partition-creation race "genuinely cannot be asserted against a mock". cryo's
selfcheck runs a throwaway postgres container and currently holds 18 live
assertions. Happy to contribute that as the live-coverage layer; say where it
should live.
