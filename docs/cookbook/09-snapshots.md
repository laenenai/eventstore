# 09: Snapshots

Streams grow. Past a few thousand events, replaying the full history on
every `Load` becomes the dominant cost of every command. Snapshots
cache the folded state at a given version so `Load` only has to fold
`(snapshot) + (events from snapshot.Version + 1 to latest)`.

The framework's snapshot strategy is defined in **ADR 0011**:

- **Lazy cadence** — written on read after enough events accumulate.
- **In-DB storage** — same database as events, same backup, same
  tenant isolation.
- **Strict schema invalidation** — stale snapshots are silently
  discarded, replay reconstructs.

This recipe shows how to turn them on and how to operate them.

## Enable for one aggregate

Two fields on `aggregate.Runtime`:

```go
rt := &aggregate.Runtime[*invoicev1.State, invoicev1.Command, invoicev1.Event]{
    Store:      store,
    Decider:    invoice.Decider,
    Codec:      invoicev1.EventCodec{},
    StateCodec: aggregate.ProtoStateCodec[*invoicev1.State]{},

    StateSchemaVersion: 1,    // <-- enables schema-driven invalidation
    SnapshotEvery:      100,  // <-- enables lazy snapshots; 0 = off
}
```

Both adapters implement `es.SnapshotStore` automatically — no extra
wiring. The same `StateCodec` that powers the Tier 1 state cache
(ADR 0020) is reused for snapshot serialization.

Behavior when both fields are set:

- **First Load.** No snapshot exists; full replay from version 1.
- **Periodic Loads while events accumulate.** When `Load` computes a
  state at version V and `V - lastSnapshotVersion ≥ SnapshotEvery`, a
  fresh snapshot row is written before `Load` returns.
- **Later Loads with a snapshot present.** `LoadSnapshot` returns a
  matching-schema row; `ReadStream` only fetches events with
  `version > snapshot.Version`. Cold-cache cost drops from O(events)
  to O(events since last snapshot).

The snapshot write is **best-effort** — if it fails (network blip,
disk full), `Load` still returns the correct state. The next cycle
will retry.

## Tuning `SnapshotEvery`

| Value | When to use |
| ----- | ----------- |
| `0` (default) | Aggregates with bounded stream lengths (< 100 events lifetime). No reason to spend storage. |
| `25–50` | Hot, fast-growing streams where `Load` latency matters more than storage. |
| `100` (recommended) | Most production aggregates. Snapshots cost ~1 row per 100 events; replay stays bounded. |
| `500+` | Cold streams, archival-style aggregates where reads are rare. |

A useful rule: set `SnapshotEvery` so the worst-case replay length
(`SnapshotEvery + slack`) reads in well under your Handle latency
budget. If reading 100 events takes 5 ms and your budget is 50 ms,
`SnapshotEvery = 500` is fine.

## Schema version: when (and how) to bump

`StateSchemaVersion` identifies the **shape of S**, not the contents.
Bump it **whenever the state struct changes** in a way that could
mis-deserialize the old bytes:

| Change | Bump? |
| ------ | ----- |
| Add a new optional proto field | No — proto handles forward compat |
| Remove a field that contained data | Yes — old snapshots' bytes contain extra fields |
| Rename a field (proto field number changes) | Yes |
| Change a field type | Yes |
| Change a field's *semantics* (same shape, new meaning) | Yes — Evolve logic differs |

When you bump, the framework silently invalidates all snapshots whose
`state_schema_version` differs from the new value. The next `Load`
for each stream runs full replay once, then writes a fresh snapshot
at the new version. **No operator action required.**

What if old snapshots survive on disk after a bump? They are
*dead rows*. Two ways to reclaim:

```sql
-- Either: targeted cleanup for one aggregate's stale snapshots.
DELETE FROM snapshots
WHERE state_schema_version < $current_version;

-- Or: leave them. The next Load on each stream overwrites; cold
-- streams' stale rows linger but cost ~hundreds of bytes each.
```

## Operator actions

`es.SnapshotStore.DeleteSnapshot(ctx, tenant, stream)` forces a
full-replay on the next read. Useful for:

- **Debugging.** "Snapshot is corrupt? Drop it and reload." Cheap.
- **Forensics.** Want to step through Evolve from the start? Drop the
  snapshot for that one stream.
- **Crypto-shredding.** When ADR 0010 ships, shredded streams may
  have snapshots that hold derived plaintext. Drop the snapshot when
  you shred. (Until then: deferred work.)

There's no "drop all snapshots" admin call — `DELETE FROM snapshots`
is one SQL line and the right primitive for the rare bulk case.

## Interaction with state_cache

`state_cache` (Tier 1, ADR 0020) and `snapshots` (this recipe) serve
different purposes:

| | `state_cache` | `snapshots` |
| --- | ------------- | ----------- |
| When written | On every Append (in same tx) | Lazy, on read after N events |
| Storage format | JSONB (Postgres) / JSON TEXT (SQLite) | Same protojson bytes |
| Purpose | Query-able read model + read-your-writes | Speed up Load |
| Reads | `GetState` / `ListStates` API | Transparent inside `Load` |
| Cost | One JSONB write per Append | One BYTEA write per N reads |

You can enable either, both, or neither. The most common production
setup: **both on**, since they answer different questions and the
combined write cost is still tiny.

## Gotchas

**Snapshot version captures pre-Append state.** `Load` runs *inside*
`Handle` before the new event is appended. So if `Handle` increments
state to version 6, the snapshot it might write captures version 5
(the loaded state). This is correct — snapshots are a cache of "what
Load saw," not "what Handle produced." Don't expect
`snapshot.Version == latest version` after a Handle call.

**Reads can write.** If your read path must be strictly read-only
(replica that mustn't write back), set `SnapshotEvery = 0` on that
read-only `Runtime` and let the writer-side runtime maintain
snapshots. Same DB, different Runtime configurations.

**Best-effort on failure.** Snapshot write errors are swallowed. If
you want to know about them, wrap `Store.SaveSnapshot` in your own
monitoring shim — or inspect `snapshot.created_at` over time as a
liveness signal.

## Reference

- ADR 0011 — Snapshot Strategy (the decisions this recipe implements)
- ADR 0020 — Projections and Read Models (Tier 1 `state_cache`)
- [`aggregate/runtime.go`](../../aggregate/runtime.go) — `Load` integration
- [`es/snapshot.go`](../../es/snapshot.go) — `SnapshotStore` interface
- [`adapters/storage/postgres/snapshot.go`](../../adapters/storage/postgres/snapshot.go) / [`adapters/storage/sqlite/snapshot.go`](../../adapters/storage/sqlite/snapshot.go) — adapter implementations
