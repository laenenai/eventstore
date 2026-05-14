# 09: Snapshots — Fast Load via `state_cache`

Streams grow. Past a few thousand events, replaying the full history
on every `Load` becomes the dominant cost of every command. The
framework's answer is **the Tier 1 `state_cache`** (ADR 0020): a
synchronous, in-transaction state write that doubles as the snapshot
for `Load`. ADR 0023 documents the consolidation.

This recipe explains:

- How `state_cache` makes `Load` constant-time in the steady state.
- How `StateSchemaVersion` invalidates stale rows when the state
  proto shape changes.
- The operator runbook around schema bumps and rebuilds.

> Earlier versions of the framework had a separate `snapshots` table
> with a lazy "write on read after N events" cadence. ADR 0011
> defined that strategy; ADR 0023 supersedes it. There is no longer
> a `SnapshotEvery` field, no separate snapshot table, and no
> opt-in beyond enabling the state cache.

## Enable: set `StateCodec` and `StateSchemaVersion`

```go
rt := &aggregate.Runtime[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
    Store:              store,
    Decider:            invoice.Decider,
    Codec:              invoicev1.EventCodec{},
    StateCodec:         aggregate.ProtoStateCodec[*invoicev1.Invoice]{},
    StateSchemaVersion: 1, // bump on shape changes; default 1 when unset
}
```

Both fields together unlock fast Load:

- **`StateCodec`** enables the state_cache write on every Append.
- **`StateSchemaVersion`** tags each row with the current state
  shape so a future Load can decide whether the bytes still parse.

When the same Runtime later calls `Handle`:

1. `Handle` calls `Load` internally.
2. `Load` reads the state_cache row for the stream.
3. If `row.StateSchemaVersion == 1`: decode → use as state, replay
   only events with `version > row.Version` (in the steady state,
   zero events).
4. Decide → events → Append (which writes a fresh state_cache row).

In steady state, `Load` reads exactly one row and zero events. The
worst case — first-ever Handle on a stream, or schema mismatch — is
the same as before (full replay).

## Bumping `StateSchemaVersion`

`StateSchemaVersion` tracks the *shape* of `S`, not its contents.
Bump whenever the state struct changes in a way that would
mis-deserialize old bytes:

| Change | Bump? |
| ------ | ----- |
| Add a new optional proto field | No — proto handles forward compat |
| Remove a field that held data | Yes — old rows' bytes contain it |
| Rename / renumber a field | Yes |
| Change a field type | Yes |
| Change semantics (same shape, new meaning) | Yes — Evolve logic differs |

When you bump:

- Existing state_cache rows still have the old `state_schema_version`.
- The next Load for each affected stream sees the mismatch, **silently
  falls back to full replay**, and writes a fresh row at the new
  version on the next Append.
- No operator action is *required*. The transition is
  self-healing — over time, hot streams get re-cached; cold streams
  stay un-cached until they're next written.

To force the rebuild proactively (e.g., before a deploy where slow
first-loads would matter):

```go
rb := store.(aggregate.StateCacheRebuilder)
n, err := aggregate.RebuildStateCache(ctx, rb, rt, tenantID)
log.Info("state_cache rebuilt", "rows", n)
```

This replays events for the tenant, applies `Evolve`, and writes
fresh `state_cache` rows under the current `StateSchemaVersion`.

## Storage and observability

One row per stream that has been state-cached. Columns:

- `tenant_id`, `stream_id` — PK
- `type_url` — proto full name of the state
- `state` — JSONB (Postgres) / JSONB-on-BLOB (SQLite, ADR 0021)
- `version` — last applied event version
- `terminal` — whether `IsTerminal(state)` is true
- `state_schema_version` — runtime invariant for invalidation
- `updated_at`

Storage cost: ~1 row per active stream, ~few KB each.

To see whether state_cache is keeping up with events (useful for
sanity-checking when troubleshooting):

```sql
SELECT
    sc.tenant_id,
    sc.stream_id,
    sc.version          AS cached_version,
    MAX(e.version)      AS event_version,
    sc.state_schema_version,
    sc.updated_at
FROM state_cache sc
JOIN events e
  ON e.tenant_id = sc.tenant_id
 AND e.stream_id = sc.stream_id
GROUP BY sc.tenant_id, sc.stream_id, sc.version, sc.state_schema_version, sc.updated_at
HAVING sc.version <> MAX(e.version);
```

In a healthy deployment this query returns zero rows: state_cache is
written in the same transaction as the events.

## What about deeply cold streams?

If a stream hasn't been read since the framework was deployed (or
since you started enabling state_cache for this aggregate), there's
no row yet. The first Load full-replays. Subsequent Loads use the
row written by that first Handle.

To pre-warm: `aggregate.RebuildStateCache` walks events and writes
rows. Run as a one-off after enabling `StateCodec` for an
already-deployed aggregate.

## Disable / read-only deployments

Two situations to leave `StateCodec` unset:

1. **Pure event-sourcing.** Some teams want every read to be a
   replay, full stop. Don't enable state_cache, don't pay the
   per-Append write.
2. **Strict read-only replicas.** A Runtime that must never write
   (cross-region read replica, audit-mode load) should leave
   `StateCodec` nil. Without it, Load still works via full replay;
   the read-only replica never tries to write.

## See also

- ADR 0023 — state_cache subsumes snapshots
- ADR 0011 — Snapshot Strategy (superseded; kept for historical context)
- ADR 0020 — Projections and Read Models (Tier 1 `state_cache`)
- Cookbook recipe 07 — Read models via materialized views (also reads from `state_cache`)
- [`aggregate/runtime.go`](../../aggregate/runtime.go) — `Load` reads from `state_cache`
- [`es/state_cache.go`](../../es/state_cache.go) — `StateCacheReader` API
