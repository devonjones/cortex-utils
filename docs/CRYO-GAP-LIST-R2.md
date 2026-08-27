# cryo → cortex-utils, round 2 (against `7bb26d6`)

Same shape as the first list: what cryo still writes SQL for, under the rule
that doing so is a gap to report rather than a boundary to defend.

Round 1's gaps are closed and absorbed into `queue-api.md`. Two new ones, two
notes, and one correction confirmed.

---

## G8 — LISTEN/NOTIFY has no library support at all

`grep -rn "pg_notify\|LISTEN" src/cortex_utils/` returns nothing.

cryo-57 replaced 1-minute cron polling with queue-driven consumers. That needs
three pieces of SQL against *your* table:

1. an `AFTER INSERT` trigger on the **partitioned parent** calling
   `pg_notify(channel, queue_name)`
2. a catalogue probe asserting the trigger exists *and* is bound to the
   expected channel — bound to `current_schema()`, not a bare relname, because
   a bare relname is cortex-jst7
3. `LISTEN` plus a drain-before-select deadline loop in `wait()`

This is the largest thing cryo still owns, and the one most worth taking,
because the parts that are easy to get wrong are facts about **your** schema,
not ours: the trigger has to be on the parent (a per-partition trigger silently
misses rows routed to a partition created later), and it has to be recreated
when the channel changes rather than merely when it is absent. A consumer
rediscovering those independently is how half-migrated states get made.

`wait(conn, timeout_s)` returning the set of queues that fired would cover it.

## G9 — `resubmit()` makes the caller look up the queue name

To pass `dedup_key` we must first run the only non-DDL raw SELECT left in cryo:

```sql
SELECT queue_name FROM queue WHERE id = %s AND status = 'failed'
```

The row already knows its queue. Either `resubmit()` takes a
`dedup_keys: Mapping[str, str]` and resolves it, or a `get_job(queue_id)`
(sibling to `DeadLetterManager.get_job`) would let us delete this.

---

## Note 1 — `create_future_partitions()` cannot cover yesterday

It starts at server today (`for i in range(days_ahead + 1)  # Include today`).

cryo creates `CURRENT_DATE - 1` deliberately (cryo-49): clock skew, and our
drain selfcheck rewinds a visibility timestamp to shortly before local
midnight. Reachable with an explicit `create_partition(today - 1)`, so not a
gap — but a `days_back: int = 0` parameter would save the next consumer
rediscovering it, and the reason is a property of `created_at = NOW()` on the
server rather than anything cryo-specific.

## Note 2 — `## Setup` omits the function that creates the table

`ensure_queue_table` is documented under `## The table`, which is correct and
thorough. But `## Setup` — "Run once per schema, before first use" — still
lists only `ensure_claim_token_column` and `DeadLetterManager.ensure_table()`.

That is the section a consumer reads at exactly the moment they are deciding
whether they can delete their own migration. This is the same failure you named
about #39 shipping without a reference entry, one section further in.

---

## Confirmed: the partial-index correction, and that cryo never relied on it

Checked first, because if we had been leaning on those indexes it would have
been a live dedup bug. `daemon/cryod/queue.py` has always gone through the
shared primitive with `dedup_key=DEDUP_KEYS.get(queue_name)`. The unique
indexes were belt-and-braces, never the mechanism. No behaviour change here.

Your transaction-boundary explanation does retro-explain something we had
filed wrong. Our selfcheck asserts a second identical enqueue returns `None`,
and it does — but because both inserts share one transaction and therefore one
`created_at`. We had that recorded as "dedup works". It was really "dedup works
**and** the index happens to fire here", and only one of those survives
concurrent producers. The test was passing for one and a half reasons.

## Confirmed: the `extra_indexes` handover works

Against PG16 at `7bb26d6`, in a non-public schema, which is the case we care
about:

```
first  -> created
second -> present
object: ('cryo', 'idx_queue_dedup_video')
object: ('cryo', 'queue')
mismatch refused: QueueError ... did not create a ...
```

- idempotent across boots; the second call logs no index work
- **the unqualified `ON queue` resolved into `cryo`, not `public`** — the thing
  we most wanted to check, because a bare relname resolving to the wrong schema
  is cortex-jst7, the bug that starved our partitions
- name/statement mismatch refused at creation time, as documented

The `(name, statement)` shape is fine. One suggestion rather than a complaint:
say out loud in the docstring that the statement is unqualified and resolves
through the caller's `search_path`. That is the right design — it is how
composition works — but when it is wrong the index is silently built in another
schema and reads as "present" forever after.

---

## One from our side, since it is the same class of bug

Adopting `DeadLetterManager` shipped a defect worth naming, because it is about
channels rather than about either library:

`claim-drain`'s stdout is a TSV protocol — the shell parses one claimed row per
line. `cortex_utils` logs through structlog, which prints to **stdout** by
default, so `Ensured dead_letter table exists` was counted as a claimed row.

Anything sharing a channel with a machine-readable protocol will eventually
corrupt it, and the corruption reads as data rather than as an error. We fixed
it on our side (structlog → stderr, plus an assertion that the claim channel
carries TSV and nothing else). Worth considering whether the library should
default its own logs to stderr: any consumer whose stdout is a protocol has
this exposure, including your own CLI entry points.

---

# Addendum, against `8fac743` (#43)

## RETRACTION — the dead-letter lifecycle gap I reported does not exist

I told you `ensure_queue_schema()` calls `ensure_table()` but not
`ensure_lifecycle_columns()`, so a `dead_letter` predating `retried_at` would
be created-if-absent but never brought forward — and framed it as the same
shape as the incident #43 exists to prevent.

**That is wrong.** `ensure_table()` calls `self.ensure_lifecycle_columns()`
itself, one frame down, and has since `305a2ab`. The one boot call does cover
it. cryo's redundant call and the comment justifying it are removed.

How I got there is the part worth passing on: I read `ensure_queue_schema()`,
saw `ensure_table()` named, checked that `ensure_queue_schema` did not also
name `ensure_lifecycle_columns`, and stopped. Reading one frame and inferring
the second is how you report a bug that is not there — and, on the same read,
miss the one that is.

## G10 — `ensure_table()` is the one step that does not pre-check itself

Found while verifying the above.

```python
def ensure_table(self) -> None:
    with self.conn.cursor() as cur:
        cur.execute(DEAD_LETTER_SCHEMA)      # unconditional
    self.conn.commit()
    self.ensure_lifecycle_columns()          # pre-checks pg_attribute
```

`DEAD_LETTER_SCHEMA` is `CREATE TABLE IF NOT EXISTS` plus two
`CREATE INDEX IF NOT EXISTS`, run **every time, with no catalogue check**,
while the very next line does check. By this module's own comment, *"CREATE
INDEX IF NOT EXISTS still takes a lock and waits on a writer even when the
index is already there"*.

That now runs on every boot of every consumer, forever — `ensure_queue_schema`
is documented as *"cheap on the steady state — every step pre-checks the
catalogue before touching anything"*, and this is the step that does not. It is
also outside any `lock_timeout` a caller sets.

CORRECTION to how I first put that: I said it was because `ensure_table` opens
its own cursor. The cursor is not the reason — `SET LOCAL` is **transaction**
scoped, and `ensure_queue_schema` runs its steps in their own transactions via
`_tx`, so a caller's `SET LOCAL lock_timeout` is already gone by the time this
DDL runs. Same conclusion, different mechanism, and worth stating precisely
since it changes where the fix goes: bounding it has to happen inside the
library, not by asking consumers to set a timeout they cannot make reach.

`ensure_lifecycle_columns` is the model: ask `pg_attribute`, return early.

## Confirmed on cryo's live database

Before adopting, I compared the running production table against
`REQUIRED_COLUMNS` rather than a test fixture:

```
MISSING from production: NONE
CHECK valid_status: status = ANY (ARRAY['pending','processing','completed','failed','cancelled'])
dead_letter cols: ... dismissed_at, retried_at, retried_as
```

All 13 columns present, the surviving pre-existing CHECK allows exactly your
`VALID_STATUSES`, so `ensure_queue_table()` does not raise on boot and nothing
writes a status the old constraint rejects. cryo now runs `ensure_queue_schema`
with its three dedup indexes as `extra_indexes`, and deletes 68 lines of DDL.

## One thing worth documenting for the next consumer

Adopting on an EXISTING table creates `idx_queue_claim` and `idx_queue_stale`
alongside whatever the consumer already had — cryo keeps
`idx_queue_pending_priority`, `idx_queue_ready`, `idx_queue_processing`, so it
now carries overlapping pairs. That is correct behaviour (`ensure_queue_table`
only ever creates, which is the right call), but the first deploy takes real
ShareLocks for the new indexes and the redundancy is permanent until an
operator drops the old ones.

Worth one line in the adoption docs: *adopting adds the canonical indexes; your
old ones are yours to retire.* Note `idx_queue_processing` is a name you warn
your own `migrate.py` owns with a different column list — cryo has one, so that
warning has at least one real instance in the wild.

## Still open from the main list

G8 (LISTEN/NOTIFY — no support at all; cryo owns the trigger, the channel-bound
probe, and `LISTEN`) and G9 (`resubmit()` forcing a `SELECT queue_name` to
derive the dedup key).

---

# Addendum 2, against `80c3e56` (#44, #46, #47)

## Your two flagged items, checked on cryo rather than assumed

**Claim-token corruption: cryo has no affected site.** Grepped every `UPDATE`
against `queue`. Zero production sites set `status='pending'` — cryo owns no
stale reset or repair re-enqueue, since `reset_stale` lives in your claim CTE.
The only two raw UPDATEs are a trust-boundary `refuse()` setting
`status='failed'`, which is your documented exemption, and it holds for the
stated reason: cryo's resubmit enqueues a NEW row rather than re-pending in
place, so no failed row is ever revived. `refuse()` is already token-matched.

One TEST site re-pended without clearing `claimed_by`; fixed, because a test
that constructs a state production cannot reach validates behaviour that never
happens.

**Timezone: nothing overrides it.** `TimeZone=UTC` with source
`configuration file`, no `pg_db_role_setting` rows, no `PGOPTIONS` in host or
container environment, no `ALTER ROLE ... SET TimeZone`. cryo will not see the
`server_today()` warning. This was the item you could not check from your side.

Also clear: no `import *`, so the `__all__` 38→20 cut is a non-event; no import
of the deprecated `retry.fail_or_retry`/`ready_predicate` (cryo binds
`ops.fail_or_retry` through the package top level); no `== "retrying"`
comparison; and cryo never hands you an autocommit connection.

## G11 — `QueueError` is overloaded in `resubmit()`

`resubmit()` raises a bare `QueueError` for three semantically different
conditions:

```
inspect.py:432  "pass dedup_key or dedup_keys, not both"      caller error
inspect.py:441  "job N is not a failed row"                    ordinary, expected
inspect.py:468  "resubmit could not cancel job N; rolled back" YOUR OWN WORDS: a bug
```

A batch caller has to tell the second from the third, because one is a stale
click and the other is an internal failure that must reach a human. There is no
structural way to do it: same type, no subclass, and no id-addressable getter
for a queue row (`get_job` exists only on `DeadLetterManager`).

cryo's workaround is to ask whether the row is still in `failures()` after the
raise — if it is, "not a failed row" cannot have been the reason. That works,
costs a query on the error path, and every consumer will have to invent it
independently or, more likely, collapse the cases and report an internal bug as
a stale click. We did exactly that first, and a reviewer caught it.

`JobNotFailedError(QueueError)` would make the distinction free. You already
subclass for `PartitionError`/`QueueTableNotFoundError`, so the pattern exists —
this is one more of the same.

## Adopted from this drop

- **G9 is closed.** `dedup_keys` removed cryo's last raw SELECT against `queue`.
  Confirmed the `not-failed` distinction survives, since `resubmit()` raises
  rather than returning `None`, and `None` already means deduped.
- `health().oldest_partition_age_days` now drives a digest tripwire. cryo cannot
  know its own retention window — it lives in your maintenance pass — so it is
  deliberately a "nobody is dropping anything" alarm rather than a policy.
  Live today: 11 partitions, 08-17 → 08-27, oldest 7 days.
- Note on that field: `None` on a PARTITIONED table means zero partitions
  attached, which is the most extreme form of the failure, not an absence of
  information. Worth one line in its docstring, because the obvious
  `if x is not None` guard goes silent exactly when it matters most. We wrote
  that guard first.
