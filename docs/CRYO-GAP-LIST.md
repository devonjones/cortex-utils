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

## Where consumer-side queue SQL IS legitimate

The rule has one real exemption, and it is worth stating so this list is not
read as "no consumer may ever write SQL naming `queue`":

**Joining the queue against a consumer's own application tables.** A library
cannot own that query — it does not know the other table exists. If a consumer
needs "queued work for accounts in state X" or "failures whose payload points at
a row that has since been deleted", that join belongs to the consumer, and it
will legitimately name `queue` in its own SQL.

The distinction is *what the query is about*:

- **about the queue itself** — counts, failures, readiness, requeue, partition
  health, schema shape. Belongs in the library. If a consumer writes it, the
  library has a gap. That is everything in G1–G5.
- **about the consumer's domain, with the queue as one input** — a join against
  tables the library has never heard of. Belongs to the consumer, always.

**This exemption does not currently apply to cryo at all.** cryo has no other
tables: its store is a git repository, and the queue is the only postgres it
owns (`docs/SCHEMA.md` — "no database, git IS the history layer"). Queue
payloads are pointers *into* git, not foreign keys. So cryo has zero legitimate
joins today, which is why the target for cryo really is **zero** queue SQL, and
why every item below is evidence of a gap rather than a judgement call.

It plausibly *does* apply to cortex, which has real application tables
(`emails_raw_response` and friends) that a queue row might reasonably be joined
against. Worth keeping in mind when deciding how far the shared read API should
go: G2–G4 should cover queue-shaped questions completely, and stop there rather
than growing toward a general query builder.

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
cryo in fact wrote it *twice*: `jobs/pg_dispatch.py:405 failed_summary()`
predates today and does the same query for the daily digest.

---

## What "inspect the queue" actually needs

G2 and G3 are easy to under-build, so this is the concrete list — every item is
a question we asked during a real incident on 2026-08-18/21 and had to answer
with hand-written SQL.

### The eight questions

1. **What is in the queue?** Counts grouped by `queue_name` × `status`. The
   top-level "is anything moving" view.

2. **What is ready RIGHT NOW?** `status='pending'` **and** (`next_attempt_at`
   null or due). This is not the same as `pending`, and conflating them is
   actively misleading: a panel showing "6 pending" when all six are backing off
   for another hour reads as a working queue with a backlog, when it is actually
   a queue in retry storm. Report ready and deferred **separately**.

3. **How far behind are we?** Age of the oldest ready row per queue. Counts
   alone cannot distinguish "4 items queued in the last minute" from "4 items
   stuck since Tuesday", and those need different responses.

4. **Is anything stuck in flight?** `status='processing'` with `claimed_at`
   older than the visibility window, plus `claimed_by`. During the auth outage
   this was the difference between "a worker is chewing on it" and "a worker
   died holding it and the row is waiting out its timeout".

5. **What failed and why?** Failed rows with `id`, `queue_name`, `payload`,
   `attempts`/`max_attempts`, **`last_error`**, `created_at`. `last_error` is
   the whole point — do not truncate it into a summary. All fourteen rows cryo
   lost on 2026-08-18 read `visibility timeout, attempts exhausted`, and it was
   that uniformity, visible only in the raw text, that proved the outage was
   infrastructure rather than fourteen unrelated content failures.

6. **What has been given up on?** `dead_letter` depth, and ideally the same
   field set — rows there are invisible to every `queue` query and silently
   accumulate.

7. **How long until enqueues start failing?** Partition headroom in days.
   `days_ahead=3` is the entire margin. cryo's partitions lapsed on 2026-08-21
   and every enqueue raised `CheckViolation` into stderr for two days before a
   human noticed the inbox was stale. **This must bind `current_schema()`** — a
   bare `relname` lookup reports healthy off the *other* schema's partitions,
   which is `cortex-jst7`, the defect that caused the outage the metric exists
   to catch.

8. **Has the write path been self-healing?** A count of partitions created from
   `enqueue()` rather than by maintenance. Non-zero means maintenance is dead
   and the queue is limping. Today that fires a log line, which is the same
   channel that already failed to surface a two-day outage; a countable number
   can be alerted on.

### Three properties, not just fields

**One round trip.** A dashboard polling five queries to draw one card will
either be slow or be written wrong. cryo's `stats()` answers 1, 6 and 7 in a
single call; the rest belong with it.

**Read-only, and independent of the workers.** This is the property that
matters most and it is easy to lose. cryo's 2026-08-18 outage ran fourteen hours
unnoticed because the only failure channel was a digest that itself needed the
pipeline alive — it died alongside the thing it was meant to report. An
inspection API that touches **only postgres** keeps working precisely when
everything else does not. Please do not let it grow a dependency on a worker, a
scheduler, or a message bus.

**Cheap enough to poll.** These will back a dashboard refreshing every few
seconds. Counts over a partitioned table with a status index are fine; anything
requiring a sequential scan of history is not.

### And one write

**Requeue** — see G4. Inspection without a way to act on what you find just
relocates the shell session.

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

## G7 — No dead-letter lifecycle (P1)

Added 2026-08-23 after building it in cryo. `dead_letter` is written to and
never read from: there is no shared way to see what is in it, put something
back, or close it out. cryo now has `dead_letters()`, `resubmit_dead_many()`
and `dismiss_dead()`, which by the rule above means the gap is real.

**This is not theoretical.** The 2026-08-18 outage looked like it cost cryo 4
videos. Two more were already in `dead_letter` with the identical
`visibility timeout, attempts exhausted`, invisible to every `queue` query.
The real number was 6, and it was only found because Devon asked "what about
dead letter?". A depth counter is not observability.

Four design points, each of which the obvious implementation gets wrong:

1. **A terminal state is mandatory, not optional.** Replay alone means every
   genuinely-undoable item accumulates forever until the real failures are
   buried in noise — and a triage list nobody can clear stops being read. cryo
   added `dismissed_at`: terminal but NOT destructive, and idempotent, so
   re-dismissing keeps the date it was actually written off.

2. **Replay must LEAVE the archive row in place.** That row is the record that
   work was given up on and when. Deleting it on replay erases the only
   evidence it ever died — which is precisely the history you want when the
   same item dies again. Double-replay is handled by `enqueue()`'s dedup, not
   by bookkeeping.

3. **The depth counter must count exactly what the list shows.** cryo's
   `stats()` and its digest job both report a dead-letter count. Once
   dismissal existed, one filtered and one did not, and two human-facing views
   of one number silently diverged. Whichever you read last is the one you
   believe; a page saying 2 while the digest says 6 is worse than either being
   wrong alone.

4. **Dead-letter ids are a SEPARATE NAMESPACE from queue ids.** Both tables
   number from 1. An API that takes "an id" without saying which will
   eventually resurrect an unrelated row.

**Schema divergence you should know about (re: G1).** cryo's `dead_letter` now
carries `dismissed_at`, added by cryo's own `migrate()`. Cortex's does not.
That is exactly the drift G1 warns about, and we created it knowingly rather
than block on the shared DDL — flagging it so it is a decision on your side
rather than a surprise. If the shared version adopts a different mechanism for
"written off", cryo will migrate to it.

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

**The rule, not a suggestion (Devon, 2026-08-23):** *"we can't trust a local
`date.today()`, you need to do a query that gets `now()` back from the server."*

The partition key is `created_at TIMESTAMPTZ DEFAULT NOW()` — a value the
**server** produces. Any date used to route or create a partition for that row
must come from the same clock. A client-side `date.today()` is a second,
unsynchronised clock in a different timezone, and it is only correct by
coincidence when the two happen to agree.

That coincidence is what makes it dangerous: it will test green on every box
where client and server are both UTC, and fail on the first consumer that is
not. It is not a race that shows up under load — it is a correctness bug that
shows up under *deployment*.

Concretely: derive the date inside the SQL (`CURRENT_DATE`, or `SELECT NOW()`
if you need it in Python first), and create **today and tomorrow** in one pass. That removes the race rather than narrowing it, and
tomorrow is inside maintenance's normal `+3` horizon so it cannot push one
schema's partitions past another's — which the shared-image coupling forbids.

cryo's `_ensure_partition_today()` on `cryo-63-partition-selfheal` does both, if
useful as a reference. Audited cryo's side while writing this: no client-side
date reaches any queue or partition path there — `CURRENT_DATE`, `NOW()`, and
`clock_timestamp()` throughout, all server-side. Worth the same grep here
(`date.today()`, `datetime.now()`, `utcnow()`), since `ops.py:247` may not be
the only site.

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

## D4 — The five primitives reference `claimed_by` unguarded (P2, worth a look)

Not a defect, and your probe is genuinely correct — `has_claim_token_column()`
binds `to_regclass('queue')`, so it resolves under `search_path` and does NOT
have the bare-relname problem. Credit where it is due.

The observation is narrower: `claim()` (`ops.py:484`), `complete()` (508),
`release()` (538) and `fail_or_retry()` (580) all reference `claimed_by`
directly, and the protection is a CONVENTION — "services call this on every
boot" — plus a helper the consumer must remember to call. A consumer that
forgets gets `UndefinedColumn` out of `claim()`, i.e. from the core of the
queue rather than a side feature.

We raise it because cryo just lived the identical failure and is about to
become the consumer most likely to forget:

cryo added `dismissed_at` via its `migrate()`, and readers referenced it
directly. On the live schema that produced `UndefinedColumn` from
`GET /api/queue` — the dashboard's primary endpoint — and from the daily
digest job, because `jobs/` reaches the host by `git pull` while the DDL rides
the daemon image. **Two deploy paths, one schema assumption.**

The rule we settled on, offered for whatever it is worth to you:

> **Degrade where a useful answer exists; fail loudly where none does.**

The three reads degrade (an unfiltered list is still the list, and on an
unmigrated schema nothing *can* have been dismissed, so the unfiltered list is
in fact the correct one) and say so in the response. The dismiss WRITE refuses
with the command to run, because there is nowhere to record the decision and
reporting success would leave an operator believing a list was cleared when it
was not.

And the part that cost us three review rounds, which generalises:

> **A half-guard reads as safe and is not.**

We guarded `stats()`, shipped, and left `dead_letters()` raising on the exact
same schema. Then guarded that and nearly left the write. If you add a guard
for `claimed_by`, add it to all five at once, or none.

---

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
