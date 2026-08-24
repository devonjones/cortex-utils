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
