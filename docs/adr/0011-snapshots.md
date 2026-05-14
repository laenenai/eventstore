# ADR 0011: Snapshot Strategy

- **Status:** Superseded by ADR 0023 (state_cache absorbs the snapshot role)
- **Date:** 2026-05-13

## Context

Replaying an aggregate's full event history on every command becomes too
expensive past a certain stream length. Snapshots cache the folded state
at a given version so that the runtime can load `(snapshot at version N)
+ (events from N+1 to current)` rather than replaying from version 1.

Three coupled decisions:

1. **When** to take snapshots — every N events (eager), on read after N
   events (lazy), or app-driven (explicit).
2. **Where** to store them — in the eventstore DB, in a separate table,
   or in an external store (S3, Redis).
3. **How** to invalidate them when the state struct shape changes —
   "snapshot poisoning" is one of the top recurring ES bugs.

## Decision

### Cadence: lazy

Snapshots are taken on **read** when N events have accumulated since the
last snapshot for that stream. Default `N = 100`. Per-aggregate
configurable.

- Cold streams that are never read pay zero snapshot cost.
- Hot read streams get snapshotted just-in-time.
- The read path occasionally writes (the snapshot itself). Callers must
  accept this side effect, or opt out per-aggregate.

### Storage: in the eventstore database

A dedicated `snapshots` table partitioned by `HASH(tenant_id)`:

```sql
CREATE TABLE snapshots (
  tenant_id            TEXT         NOT NULL,
  stream_id            TEXT         NOT NULL,
  version              BIGINT       NOT NULL,
  state_schema_version INT          NOT NULL,
  state                BYTEA        NOT NULL,
  created_at           TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, stream_id)
) PARTITION BY HASH (tenant_id);
```

One row per stream, latest wins. State stored as proto bytes (same codec
discipline as event payloads, ADR 0006).

### Invalidation: strict `state_schema_version`

Snapshots carry the `state_schema_version` of the state shape they
represent. When the state struct changes (any field added, removed, or
renamed), the version bumps. Snapshots with a mismatched version are
silently discarded with a metric, and the runtime falls back to full
replay until a new snapshot is written.

Corruption (decode error, integrity check failure) is treated identically
to schema mismatch: discard and full-replay.

## Consequences

### Positive

- **Replay cost stays bounded.** No stream grows unboundedly slow.
- **Cold streams have zero overhead.** No snapshot writes for streams
  that are never read.
- **Snapshot is a cache, not data.** Losing all snapshots loses zero
  information — the event log is the source of truth. Backups,
  partitions, and partition drops are all forgiving.
- **Schema invalidation is automatic.** Snapshot poisoning is
  structurally prevented; no operator intervention needed when state
  shape changes.
- **Same operational model as events.** Same DB, same backup, same
  partition strategy, same tenant isolation.

### Negative

- **Read path occasionally writes.** Most aggregates this is fine; for
  read-only flows that must not write, the framework provides a
  `Read(WithoutSnapshotWrite)` option that skips the snapshot side
  effect.
- **One additional table per tenant** to back up and partition.
  Negligible in practice.

## Alternatives Considered

### Eager snapshots (every N events, in the writer transaction)

Rejected as the default. Wastes writes on streams that are never read.
For the rare case of "this specific stream is so hot that lazy
snapshotting introduces a noticeable read pause", an explicit eager
mode can be opted into per-aggregate without changing the framework
contract.

### External snapshot store (S3, Redis, etc.)

Rejected. Breaks atomic load semantics — loading a snapshot becomes a
network call to a different system with different failure semantics from
the event read. Adds operational infrastructure. The win (cheaper storage
at scale) is not load-bearing at the volumes the framework targets.

### Keep all historical snapshots

Rejected for v1. Latest-only is sufficient for replay. If a forensic use
case ("show me what the aggregate looked like at version 173") becomes
real, a `snapshots_history` sidecar table can be added without disturbing
the primary path.

### App-driven explicit snapshotting

Rejected as the default. Maximum flexibility but maximum rope. Few
domains can reliably guess when a snapshot is worth taking. Lazy by
default with explicit overrides is the better trade.
