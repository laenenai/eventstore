# ADR 0009: Postgres Global Position via Advisory Lock + Sequence

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

The `global_position` envelope field (ADR 0005) must be store-wide
monotonic so projectors can poll or consume in order without missing
events. SQLite is single-writer by construction; an
`INTEGER PRIMARY KEY AUTOINCREMENT` column gives gap-free monotonic
ordering for free. Postgres is harder.

The problem on Postgres: `bigserial` is **not gap-free under concurrent
commits**. Two transactions T1 and T2 can:

1. T1 fetches `nextval()` → 100. T2 fetches `nextval()` → 101.
2. T2 commits at time *t*.
3. T1 commits at time *t + 50ms*.

A projector polling `WHERE global_position > last_seen ORDER BY
global_position` reads 101 at time *t*, advances its cursor, and **never
sees 100** when T1 finally commits. Lost event.

This is the canonical failure mode every Postgres-based ES system must
solve. ULIDs and UUIDv7 do not fix it — they are unique-ID strategies, not
coordination mechanisms; the same out-of-order-commit problem applies with
their generation timestamps.

Three real solutions:

- **(A)** Single-writer with `pg_advisory_xact_lock`. Serializes appends
  store-wide. Sequence consumed in commit order. Gap-free.
- **(B)** Logical replication slot / WAL LSN as the position source.
  Postgres orders by commit at the WAL layer; consumers read from the
  slot. No append serialization. Operationally heavy.
- **(C)** `bigserial` + projector-side xmin watermark. Parallel appends.
  Projectors defer reading past position N until no in-flight transaction
  could still commit a row with `xmin <=` the xmin of N.

## Decision

Adopt option (A) for v1.

### Append flow on Postgres

```sql
BEGIN;
SELECT pg_advisory_xact_lock(<store-wide constant>);
-- Insert constraint claims (UNIQUE will fail-fast on conflict).
INSERT INTO unique_claims (...) VALUES (...);
-- Insert events with caller-supplied versions and sequence-allocated positions.
INSERT INTO events (..., version, global_position)
  VALUES (..., :expected_version + 1, nextval('events_global_position_seq')),
         (..., :expected_version + 2, nextval('events_global_position_seq')),
         ...;
COMMIT;
```

The advisory lock auto-releases on commit or rollback.

### Optimistic concurrency

Falls out of the schema for free. The events PRIMARY KEY is
`(tenant_id, stream_id, version)`. Callers supply versions
`expected_version + 1, +2, …`. A stale read produces a unique-violation
on the PK → typed `ErrConflict` to the caller. No read-before-write.

### SQLite

Single-writer by construction. `INTEGER PRIMARY KEY AUTOINCREMENT` for
`global_position`. No lock needed. Same projector cursor logic applies.

## Consequences

### Positive

- **Conceptually trivial.** Same mental model on both adapters.
- **Gap-free monotonic.** Projectors are dumb:
  `WHERE global_position > :cursor ORDER BY global_position LIMIT N`.
- **Crash-safe.** A crash during append rolls back; the next attempt
  re-uses the next sequence value.
- **Optimistic concurrency is free** — comes from the PK, not from
  read-before-write.
- **Migration path preserved.** Switching to option (C) later does not
  change the events schema; only the projector runtime gains a watermark
  abstraction. Roll out per-projection.

### Negative

- **Append throughput is single-thread bound** by Postgres commit speed.
  Empirically 5k-20k appends/sec store-wide on modern Postgres with NVMe
  and reasonable `fsync` policy. Considered acceptable for the target
  use cases.
- **Multi-tenant write fairness.** A noisy tenant cannot starve others
  more than serially. Per-tenant locking would parallelize tenants but
  would also break store-wide global position; we deliberately keep
  global position store-wide (see ADR 0007).

## Alternatives Considered

### Option (B) — Logical replication slot

Rejected for v1. Operationally heavy: slots can become bloated if a
consumer dies; slot retention must be managed; a slot is a single point
of failure unless replicated. Diverges substantially from the SQLite
adapter, doubling implementation surface. LSNs are opaque and
non-human-friendly as cursors. Reserved for a future scale ceiling that
v1 is not expected to hit.

### Option (C) — bigserial + xmin watermark

Deferred. Migration path is documented: the events table does not
change; the projector runtime gains a "stable cursor" abstraction that
defers reading past position N until `pg_snapshot_xmin(pg_current_snapshot())`
is greater than the xmin of N. Trade-off: parallel appends, projectors
lag by the duration of the longest in-flight write transaction. To be
revisited if v1's throughput ceiling becomes a real constraint.

### UUIDv7 / ULID as cursor

Rejected. Time-prefixed IDs are not a coordination mechanism. The same
concurrent-commit ordering problem applies — generation time is captured
before commit. Equivalent to option (C) with a larger cursor (16 bytes vs
8) and no implementation benefit.

### Hash-of-tenant advisory lock (per-tenant serialization)

Rejected. Parallelizes appends across tenants but breaks the store-wide
`global_position` invariant. To support per-tenant parallelism, position
would have to become per-tenant, which loses cross-tenant subscription
semantics relied on by admin and billing flows.

## Reference — measured ceiling

The "Negative" section above states that append throughput is
single-thread bound by Postgres commit speed at "empirically 5k-20k
appends/sec store-wide on modern Postgres with NVMe." That figure
remains the **production ceiling** on dedicated hardware. Spike 0001
(`docs/spikes/0001-laenen-tenancy.md`) added a second data point at
the other end of the hardware spectrum: the **development-environment
floor**.

| Environment | Sustained advisory-lock throughput | Notes |
| --- | --- | --- |
| Production-class Postgres on NVMe | 5,000–20,000 appends/sec | Original ADR estimate; unchanged. |
| testcontainers Postgres on Docker, M1 Max laptop | ~167–180 appends/sec | Spike 0001 §11.2.2 — observed during the 100K saturation incident; confirmed by the 10K seed phase rate. |

The two-orders-of-magnitude gap matters for adopters running the
test suite or local dev: integration tests that simulate "busy
production" (hundreds of writes/sec) will saturate the advisory
lock on a developer laptop and produce queueing artefacts that do
**not** reproduce in production. Bench harnesses (`estest/bench/`)
default to write rates well below this floor — see
`bench.DefaultConfig`'s godoc.

This is documentation, not a decision change: the ceiling was
always going to be hardware-dependent, and the original "5k-20k"
range remains the operationally meaningful number. The reference
exists so future readers comparing local-dev numbers against the
ADR's headline figure understand why they look so different.
