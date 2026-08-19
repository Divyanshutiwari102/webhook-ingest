# SOLUTION.md

## What was broken, and why

**1. Duplicate events and drifting call counts.** `Ingest` checked whether an
event existed (`EventExists`) and then inserted it as two separate,
non-atomic steps. Two near-simultaneous redeliveries of the same `event_id`
could both pass the check before either had inserted, so both proceeded to
increment `account_stats` and the in-memory cache. This wasn't just an
application bug — the `events` table only had a plain index on `event_id`,
not a `UNIQUE` constraint, so the database itself had no way to reject the
second write.

**2. Recordings never marked processed, with nothing logged.** The recording
goroutine ran on the *request's* context (`r.Context()`), which `net/http`
cancels the instant the handler returns. Since `Ingest` returns almost
immediately after spawning the goroutine, `processRecording` was usually
running on an already-cancelled context. The resulting error was then
discarded by an empty `// TODO: handle` block, so the failure was invisible.

**3. In-flight work lost on deploy.** `http.Server.Shutdown` only waits for
active HTTP connections — it has no visibility into detached goroutines. The
recording goroutine wasn't tracked anywhere, so `main()` could exit and kill
the process mid-write while a recording was still being marked processed.

**4. Unsynchronized in-memory cache (fixed).** An earlier version of `stats.Cache.Record` mutated the map
and counters without holding `mu`, while `Get` took a read lock. Under
concurrent requests this was a data race — lost increments at best, a panic
on concurrent map writes at worst. The fix added `c.mu.Lock()` (and defer `Unlock()`) at the start of `Record`, making the operation thread‑safe. Existing tests never triggered it because
they call `Record` sequentially from a single goroutine.

None of these were caught by the original test suite because it only ever
exercises one request at a time, sequentially. All four bugs are concurrency
bugs; they require overlapping requests to manifest.

## Deduplication strategy

I used a `UNIQUE` index on `events.event_id` with
`INSERT ... ON CONFLICT (event_id) DO NOTHING`, wrapped in the same Postgres
transaction as the `calls` upsert and the `account_stats` increment, and
removed the separate `EventExists` pre-check from the ingest path entirely
(it's now redundant and would just add a round trip).

I considered a Redis-based dedup (`SETNX event:<id>`), which is faster and
keeps write load off Postgres. I rejected it because it introduces a second
system making the same "have I seen this event" decision as Postgres, with
no transaction spanning both. Any partial failure — Redis SET succeeds but
the Postgres write later fails or the process crashes in between — leaves
the two stores disagreeing about whether an event was processed. Since
Postgres is already the source of truth for events, calls, and account
stats, and those three writes already need to be atomic with each other,
folding the dedup check into that same transaction via a unique constraint
costs nothing extra and has no second store to fall out of sync with.

## What I'd change at 10,000 webhooks/second

The first thing to break is the `account_stats` row-level lock. Every
webhook for a given account runs an `UPDATE ... WHERE account_id = $1`
inside the same transaction as the event insert, holding a pooled
connection across five sequential round trips. Any account with concurrent
traffic serializes its transactions on that one row, and with
`DBMaxConns` at 20 the connection pool itself becomes a second ceiling —
requests queue behind both the lock and the pool well before 10k/sec.

I'd take the counter increment out of the per-request transaction:
accumulate call counts and durations per account in memory (or Redis), and
flush aggregated deltas to `account_stats` on a short interval instead of
one `UPDATE` per webhook. This removes the row lock as the serialization
point rather than just moving the same contention to a different service —
the trade-off is that `/accounts/{id}/stats` (and the durable table) can lag
the true count by up to one flush interval.

Separately, the recording pipeline (50ms of synthetic work per event) would
also need to move off ad-hoc goroutines and onto a bounded worker pool or
external queue at that volume, to avoid unbounded goroutine growth — but
since the question asked what breaks *first*, the database hotspot is the
earlier failure.