# ADR 0023: state_cache Subsumes Snapshots

- **Status:** Accepted
- **Date:** 2026-05-14
- **Supersedes:** ADR 0011 (Snapshot Strategy)
- **Amends:** ADR 0020 (Projections and Read Models)

## Context

ADR 0011 introduced **lazy snapshots** for replay speed-up. ADR 0020
introduced **`state_cache`** — a synchronous, in-transaction state
write for read-your-writes queries. The two ended up storing the same
information (the marshalled current state of a stream) under different
access patterns:

| | `state_cache` (ADR 0020) | `snapshots` (ADR 0011) |
| --- | --- | --- |
| What it stores | Marshalled state | Marshalled state |
| Cadence | Every Append (synchronous, in tx) | Lazy, on read after N events |
| Has version | ✅ | ✅ |
| Has type_url | ✅ | ✅ |
| Has schema_version | ❌ | ✅ |
| Queryable | ✅ (JSONB, MV pattern) | ❌ |
| Used by aggregate.Runtime.Load | ❌ | ✅ |

The inefficiency this produces, observed when both are enabled:

- Each Append writes a state_cache row (one JSONB upsert).
- Each Load reads from `snapshots`, not from `state_cache` — even
  though `state_cache` is always at the *latest* version while
  snapshots are by definition older.
- Storage holds the same bytes twice.
- The runtime carries two opt-in mechanisms (`StateCodec` + `SnapshotEvery`)
  that solve overlapping problems.

A user who only enables `StateCodec` (the Tier 1 state cache) but
leaves `SnapshotEvery = 0` still pays for full event replay on every
`Handle`, even though the framework wrote the answer to disk on the
previous Append.

The fix is to make `state_cache` carry the only feature snapshots
had that it didn't — `state_schema_version` — and use it as the
canonical "current state per stream" everywhere.

## Decision

### `state_cache` carries `state_schema_version`

A new column added by migration (`00009_state_cache_schema_version`
on Postgres, `00008_state_cache_schema_version` on SQLite). Existing
rows default to `state_schema_version = 1`, matching the runtime's
default `Runtime.StateSchemaVersion` when unset.

`aggregate.Runtime` writes `state_schema_version` through
`AppendParams` on every state-cached append. The adapter passes it
through to the row.

### `aggregate.Runtime.Load` reads from `state_cache`

When `StateCodec` is set and the Store implements
`es.StateCacheReader`:

1. Read the `state_cache` row for `(tenant, stream_id)`.
2. If `row.StateSchemaVersion == r.StateSchemaVersion`: decode the
   row, use it as the replay base, only replay events with
   `version > row.Version`.
3. Otherwise (schema mismatch, decode failure, missing row): full
   replay from version 1.

In the steady state — `StateCodec` enabled, schema unchanged — Load
reads zero tail events: state_cache is always at the latest version
because it commits in the same transaction as the events.

### Snapshot infrastructure is removed

- `es.SnapshotStore` interface deleted.
- `es.Snapshot` struct deleted.
- `es.ErrSnapshotNotFound` deleted.
- Adapter `LoadSnapshot` / `SaveSnapshot` / `DeleteSnapshot`
  implementations deleted.
- `aggregate.Runtime.SnapshotEvery` field removed.
- `snapshots` table dropped via migration
  (`00010_drop_snapshots` on Postgres, `00009_drop_snapshots` on
  SQLite).

The framework is pre-v1; there are no callers to grace, no
backwards-compatibility shim required. The deprecated names are gone.

## Consequences

**Positive:**

- **One opt-in** (`StateCodec`) instead of two. Enabling state-cache
  is enabling fast-Load.
- **state_cache is always fresher than any snapshot was.** Load in
  the steady state reads zero tail events.
- **One table, one storage shape, one mental model.** "Where does
  the current state live?" → `state_cache`.
- **Schema invalidation contract retained.** `state_schema_version`
  on `state_cache` does exactly what it did on snapshots — stale
  rows are silently discarded with full-replay fallback.
- **Aggregates that don't opt into `StateCodec` are unchanged.**
  Pure event-sourcing path still does full replay; no implicit
  cache writes.

**Given up:**

- **Snapshots-without-state_cache is no longer a configuration.**
  Previously a user could opt into lazy snapshots for Load speedup
  without paying the per-Append state_cache write cost. That's gone;
  to get fast Load you must enable state_cache (one JSONB write per
  Append). The Append-time cost is measurably small for typical
  state shapes and almost no real aggregate would prefer the old
  asymmetry.
- **Lazy cadence is gone.** state_cache writes happen on *every*
  Append, not after-N-events. Same Append-write cost as before; no
  separate lazy mechanism to tune.

**Migration story:**

- New code: declare `StateCodec` to get fast Load. `SnapshotEvery`
  no longer exists.
- Existing data: any state_cache rows that pre-date the schema
  version column got `state_schema_version = 1` via the migration's
  `DEFAULT 1`. Aggregates with `Runtime.StateSchemaVersion = 1`
  (the default) decode them. Aggregates that bumped past 1 need to
  run `aggregate.RebuildStateCache` to repopulate. The old
  `snapshots` table is dropped.
- Cookbook recipe 09 (Snapshots) is retargeted at the
  state_cache-as-snapshot semantics; recipe 11 (state cache) is
  unchanged in spirit.

## Alternatives Considered

### Keep both, document state_cache as preferred

The framework would document "use state_cache for read-your-writes,
use snapshots for Load speedup," and Load would still consult
snapshots. Users who wanted both would write the same bytes twice and
keep a strictly older copy in `snapshots`. Rejected — there is no
deployment shape where this is the right answer; the duplication
serves no use case.

### Make Load fall back: state_cache → snapshots → full replay

Three-tier fallback. Considered briefly to support an in-between
state during migration, but the framework hasn't shipped and we
have no callers to support. The simpler "state_cache or full replay"
fallback is the right destination.

### Use the snapshots table; deprecate state_cache

Symmetric alternative: keep `snapshots`, drop `state_cache`. Snapshots
would have to be written synchronously and gain JSONB-style queryable
storage. The end result is the same table doing both jobs, but
renaming "state_cache" to "snapshots" everywhere would break the
operator mental model: state_cache's name reflects its primary use
(query API). Snapshots' name reflected an implementation detail
(periodic state writes for Load speedup). The user-facing name
should describe the use; we kept `state_cache`.

### Single name `stream_state` for both

Considered. Rejected — `state_cache` is already in cookbook
recipes 07, 08, 09, 11 and ADR 0020; the name has weight. Renaming
the table would touch more code than it's worth.

## Reference

- ADR 0011 — Snapshot Strategy (this ADR supersedes it)
- ADR 0020 — Projections and Read Models (Tier 1 `state_cache`)
- [`aggregate/runtime.go`](../../aggregate/runtime.go) — Load + Handle changes
- [`es/state_cache.go`](../../es/state_cache.go) — `StateCacheReader` / `StateCacheRow.StateSchemaVersion`
- Cookbook recipe 09 — Snapshots (retargeted)
