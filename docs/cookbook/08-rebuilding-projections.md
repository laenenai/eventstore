# 08: Rebuilding Projections

Every Tier 3 projection needs an answer to "what do I do when this
projection is wrong?" — schema change, handler bug, corruption,
field added to the read model. The framework provides the runtime
surface; the rebuild *plan* depends on whether the projection can
tolerate downtime.

This recipe covers four patterns:

| Pattern | Downtime | Reads | When to use |
| ------- | -------- | ----- | ----------- |
| 1. Truncate-and-replay | Yes (read model empty during catch-up) | Events | Internal dashboards, ops tools, anywhere a brief stale window is acceptable |
| 2. Versioned parallel rebuild | None | Events | User-facing read paths where a hole would hurt |
| 3. State-cache rebuild | None visible | Events | Tier 1 only — repopulate `state_cache` after a state proto change |
| 4. State-cache projection rebuild | Yes (read model empty during scan) | `state_cache` | Read models that are a pure function of current state. **O(streams)** instead of O(events). |

## Pattern 1 — Truncate-and-replay (simple)

Use `ProjectionAdmin` to reset the cursor and let the runner re-emit
every event:

```go
admin := store.(es.ProjectionAdmin)

// 1. (Optional) stop the runner if it's a long-running goroutine.
//    Skip if you're running RunOnce on a scheduled trigger and can
//    accept the next tick re-processing.

// 2. App-specific: clear the read model.
if _, err := pool.Exec(ctx, "TRUNCATE user_view"); err != nil {
    return err
}

// 3. Reset the projection's cursor.
if err := admin.Reset(ctx, "user-view", tenantID); err != nil {
    return err
}

// 4. Next RunOnce sees cursor=0 and re-reads from the start.
//    A long-running Run() picks up automatically; for scheduled
//    triggers the next tick rebuilds.
```

During rebuild, queries against `user_view` see partial or empty
results until the projection catches up. For non-critical reads
(internal dashboards) this is fine. For anything user-facing, use
Pattern 2.

### Partial replay

When you know events 1..N are still good and only N+1..now were
mishandled, use `ResetTo`:

```go
admin.ResetTo(ctx, "user-view", tenantID, lastGoodGlobalPosition)
```

Common case: a handler bug shipped at deploy time T, then was fixed.
Pick the global_position closest to T from the events table; reset to
that. Saves replaying old, already-correct rows.

## Pattern 2 — Versioned parallel rebuild (zero-downtime)

For user-facing read paths where empty/wrong data is unacceptable,
run the new projection version in parallel until it catches up, then
swap reads atomically.

The framework supports this without any special features — projection
*name* is the unit of independence. Two projectors with different
names have independent cursors and never collide.

### Step-by-step

1. **Add a new read-model table** (`user_view_v2`) alongside the old
   one (`user_view`) in a migration.
2. **Deploy a new projection** with name `user-view-v2` writing to
   `user_view_v2`. Same code as before, just a different name and a
   different write target.
3. **Both projections run.** v1 stays current on `user_view`; v2
   catches up from gp=0 against `user_view_v2`.
4. **Wait for v2 to catch up.** Monitor via `admin.Status`:

   ```go
   v1, _ := admin.Status(ctx, "user-view",    tenantID)
   v2, _ := admin.Status(ctx, "user-view-v2", tenantID)
   delta := int64(v1.Cursor) - int64(v2.Cursor)
   // Wait until delta is small enough that the swap window won't
   // visibly lag.
   ```

5. **Atomic swap.** Postgres:

   ```sql
   BEGIN;
   ALTER TABLE user_view        RENAME TO user_view_v1_retired;
   ALTER TABLE user_view_v2     RENAME TO user_view;
   COMMIT;
   ```

   …or, if your read code targets a view:

   ```sql
   CREATE OR REPLACE VIEW user_view_current AS SELECT * FROM user_view_v2;
   ```

6. **Decommission v1.** After enough time that no in-flight reads
   point at v1:

   ```go
   admin.Reset(ctx, "user-view", tenantID)  // stop advancing
   ```

   ```sql
   DROP TABLE user_view_v1_retired;
   DELETE FROM projection_checkpoint
     WHERE name = 'user-view' AND tenant_id = $1;
   ```

### When parallel rebuild is hard

This pattern works best when the read model is one or two tables in
the same DB. Complications:

- **External read stores** (Elasticsearch, Redis). The "atomic swap"
  becomes a coordinated rename or alias swap on the external store.
  Doable but more moving parts.
- **Cross-system reads** (the projection writes to *N* destinations).
  Each destination has its own swap story; coordination is harder.
- **Aggregation accuracy.** If the new projection counts events
  differently (different bucketing, different filter), the old and
  new MVs disagree during the catch-up window. Plan UX accordingly
  ("data is rebuilding — refresh in 5 min").

For these cases either accept Pattern 1's downtime or stage the
rollout: run v2 read-only ("shadow projection"), compare against v1,
swap only after dry-run validation.

## Pattern 3 — State-cache rebuild

When you change a *state* proto in a way that requires re-folding
events (added a derived field, fixed an Evolve bug), use the
framework's dedicated helper rather than going through the projection
runtime:

```go
rt := &aggregate.Runtime[*invoicev1.State, invoicev1.Command, invoicev1.Event]{
    Store:      store,
    Decider:    invoice.Decider,
    Codec:      invoicev1.EventCodec{},
    StateCodec: aggregate.ProtoStateCodec[*invoicev1.State]{},
}

rb := store.(aggregate.StateCacheRebuilder)
n, err := aggregate.RebuildStateCache(ctx, rb, rt, tenantID)
log.Info("state_cache rebuilt", "rows", n)
```

What it does:

1. Wipes `state_cache` rows matching this aggregate's `type_url`.
2. Streams events for the tenant in `global_position` order.
3. Folds per-stream via `Decider.Evolve`.
4. Writes final state per stream via the adapter's direct upsert
   (bypassing the events table — no new events are produced).

Pass `tenantID = ""` to rebuild across all tenants. Tier 2 MVs over
`state_cache` will be stale until their next `REFRESH MATERIALIZED
VIEW CONCURRENTLY` — schedule the refresh after the rebuild
completes.

This is the right path for state proto changes specifically — Patterns
1 and 2 above are for arbitrary Tier 3 projections.

## Pattern 4 — State-cache projection rebuild

When the projection is a pure function of **current state** (no
event-history dependence), skip event replay and iterate
`state_cache` directly. This is **O(streams)** instead of
**O(events)** — for an aggregate with 10k streams averaging 100
events each, that's 100× faster.

**Works for:**
- "All active customers" — current state.
- "Per-region invoice totals" — current state.
- "Search index of current product catalogue" — current state.

**Does NOT work for:**
- "Login history feed" — needs per-event timestamps.
- "Audit ledger" — needs per-event actor / occurred_at.
- "Daily revenue chart" — needs per-event amounts and dates.
- Anything that reads `OccurredAt`, `RecordedAt`, or any field that
  isn't in the marshaled state.

If your projection mixes both styles, fall back to Pattern 1 or 2.

### The pattern

`es.ScanAllStates` is an iterator that pages through `state_cache`
for one `(tenant_id, type_url)` pair via the underlying
`ListStates` cursor:

```go
import "github.com/laenenai/eventstore/es"

// 1. Truncate the read model.
if _, err := pool.Exec(ctx, "TRUNCATE customer_view"); err != nil {
    return err
}

// 2. Iterate every current customer state.
for row, err := range es.ScanAllStates(ctx, store,
    tenantID, "myapp.customer.v1.Customer", 1000) {
    if err != nil {
        return err
    }
    var s customerv1.Customer
    if err := proto.Unmarshal(row.State, &s); err != nil {
        return err
    }
    // 3. Project current state into the read model.
    if _, err := pool.Exec(ctx,
        `INSERT INTO customer_view (id, name, email, region)
         VALUES ($1, $2, $3, $4)`,
        s.Id, s.Name, s.Email, s.Region); err != nil {
        return err
    }
}

// 4. Reset the projection's cursor to current head so the live runner
//    picks up from there instead of replaying events 1..N (which would
//    double-write everything we just scanned).
admin.ResetTo(ctx, "customer-view", tenantID, currentGlobalPosition)
```

### Cross-aggregate projections

When the projection consumes multiple aggregate types (linked
projections — recipe 12), call `ScanAllStates` once per type:

```go
for row, err := range es.ScanAllStates(ctx, store, tenantID,
    "myapp.invoice.v1.Invoice", 1000) {
    // project Invoice state
}
for row, err := range es.ScanAllStates(ctx, store, tenantID,
    "myapp.order.v1.Order", 1000) {
    // project Order state
}
```

Cursor pagination is per-call; pages from different types don't
interleave. Order doesn't matter unless your projection has
cross-aggregate referential constraints — in which case Pattern 1 /
2 (event-order replay) is probably the right shape anyway.

### When stepping back to event replay is the right move

- The state proto **doesn't carry the field you're projecting**.
  Add the field to state and bump `state_schema_version` (Pattern 3
  repopulates `state_cache` from events), then run Pattern 4. Two
  passes total, both still cheaper than full event replay for hot
  aggregates.
- **state_cache hasn't been backfilled** for old streams (their rows
  are absent). Pattern 4 silently skips those streams. Either
  rebuild `state_cache` first (Pattern 3) or fall back to Pattern 1.

### Failure modes

| Failure                                            | What happens / what to do                                                                                              |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Scan starts mid-write; some rows pre-update, some post-update | For projection rebuild this is harmless — the live runner advances the cursor past every event written during the scan; nothing is lost. Pattern 4's premise is idempotent over current state. |
| Mixed `state_schema_version` rows                  | Caller decides — either filter inside the loop (`if row.StateSchemaVersion != currentVersion { continue }`) or upcast in user code. Framework v1 has no state upcaster registry; rebuild state_cache first if you need a uniform version. |
| Stream terminal between scan and projection apply  | `row.Terminal` indicates the aggregate is closed. Caller decides whether terminal streams belong in the read model.    |
| `ListStates` returns 0 rows                        | `state_cache` is opt-in per aggregate (set `StateCodec` on the Runtime). If you haven't enabled it, Pattern 4 sees nothing. Pattern 1 is the only option. |

## Operational notes

**During rebuild, hold the LockKey.** If you're running with
`Runtime.LockKey` set (recipe 06's pattern 3), the rebuild itself
should hold the lock — otherwise a sibling replica could start a
parallel rebuild and you'd double-process. `admin.Reset` doesn't
acquire the lock; combine with a coordinated rollout (stop the
runners, reset, start fresh) or use a single-runner deployment for
the rebuild window.

**Avoid rebuilding under load.** Tier 1 rebuild reads every event for
the tenant. For large tenants, run during a quiet window or rebuild
per-aggregate-type at a time.

**Re-run is idempotent.** Handler idempotency (ADR 0020 decision 3d)
means a partial rebuild followed by a retry doesn't double-apply
anything. Safe to abort and retry.

**Watch for projection_dlq when it lands** (deferred per ADR 0020).
A future opt-in DLQ mode will skip handler-failing events and record
them for operator inspection. Rebuild semantics will need to consider
whether to replay DLQ'd rows automatically or leave them skipped —
this recipe will be updated when the feature ships.

## Reference

- ADR 0020 — Projections and Read Models (decision 3g — rebuild)
- Cookbook recipe 06 — Running the Outbox Drain (the LockKey pattern
  applies identically to projections)
- Cookbook recipe 07 — Read Models via Materialized Views (Tier 2
  freshness depends on REFRESH; rebuild after Tier 1 rebuild)
- [`es/projection_admin.go`](../../es/projection_admin.go) — admin interface
- [`aggregate/rebuild.go`](../../aggregate/rebuild.go) — Tier 1 rebuild helper
