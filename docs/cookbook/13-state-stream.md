# 13: state_stream — Coalesced State-Mirror Delivery

When you want **the current state** of every aggregate replicated
to an external system — full-text search, ad-hoc analytics, CDC into
a separate read-store — `state_stream` is the pattern. It rides
`state_cache` (cookbook 07) and mirrors `outbox.Drain` (cookbook 06)
operationally, with one crucial difference: **delivery is coalesced**.

ADR 0024 fixes the design. This recipe is the operator-and-developer
view: what semantics to rely on, how to wire it up, what gotchas to
watch for.

## What "coalesced" actually means

If a stream gets 50 Appends between two drain ticks, the subscriber
receives **one** delivery with the current state at tick time — not 50.
This is by design: subscribers that mirror state (search indexes,
caches, denormalized read-stores) don't care about intermediate
versions. They care about *current truth*.

Concretely:

| Scenario | Outbox behavior | state_stream behavior |
| -------- | --------------- | --------------------- |
| 50 appends, then drain runs | 50 row publishes | 1 state publish (latest) |
| Drain runs, all idle | 0 publishes | 0 publishes |
| Delivery fails, stream advances, drain re-runs | Failed event re-published (in order) | **Latest state** published, failed version skipped |

That last row is **coalescing-on-retry**, and it's the property most
worth internalizing. If subscriber X temporarily can't reach the
search index, and the stream had 10 more Appends in the meantime,
the next drain delivers the version-after-the-10. Earlier intermediate
states never travel — and your subscriber must be okay with that.

## Receiver contract (the non-negotiables)

```go
type StatePublisher interface {
    PublishState(ctx context.Context, env StateEnvelope) error
}

type StateEnvelope struct {
    TenantID           string
    StreamID           string
    TypeURL            string
    Version            uint64
    StateSchemaVersion uint32
    State              []byte    // marshalled state (proto-json by default)
    UpdatedAt          time.Time
}
```

Your `PublishState` MUST:

1. **Be idempotent on Version.** Coalesced delivery still re-sends the
   same version after a transient failure (the position advances only
   on success). If you've already applied version N, ignore version N
   on re-delivery.

   ```go
   if existing.Version >= env.Version { return nil }
   ```

2. **Be okay with skipped versions.** You will not see every version —
   you'll see *some* versions, ending at the current one. If your
   downstream needs every event, use outbox (cookbook 06), not
   state_stream.

3. **Handle `TypeURL` routing.** One subscriber can mirror many
   aggregate types or just one; the envelope tells you which.

4. **Return errors fast.** Errors keep the position in place;
   the next drain cycle delivers the *latest* state at that point.
   Long retries inside `PublishState` defeat the coalescing benefit —
   let the drain do retry-on-next-tick instead.

## Wiring it up

Same shape as outbox.Drain:

```go
import "github.com/laenenai/eventstore/state_stream"

drain := &state_stream.Drain{
    SubscriberName: "invoice-search-index", // stable; identifies position rows
    Tenant:         "acme",                  // per-tenant subscriber
    Store:          store,                   // your adapter (Postgres or SQLite)
    Publisher:      myIndex,                 // implements es.StatePublisher
    BatchSize:      200,                     // default 100
    LockKey:        "invoice-search-index",  // optional single-runner cross-replica
    Shard:          0,                       // optional sharding
    TotalShards:    0,                       // 0 = no sharding
    OnDeliveryError: func(env es.StateEnvelope, err error) {
        log.Printf("state_stream: stream=%s version=%d err=%v",
            env.StreamID, env.Version, err)
    },
}

for {
    delivered, err := drain.Run(ctx)
    if err != nil { /* log */ }
    time.Sleep(2 * time.Second)
}
```

See `examples/statestream/` for a complete end-to-end demo.

## Subtleties to internalize

### Cold-start backfill is automatic — and free

A new subscriber added against a database with N existing streams
will receive the **current state of every stream** on its first
`Run`. This is the same query path as steady state — a `LEFT JOIN` on
the position table treats `NULL` as "behind from version 0", so any
state_cache row with `version > 0` qualifies for delivery.

There is no separate "backfill" mode. No code path to write. This is
deliberate: it's what makes the design composable. Add a subscriber
on day 800 and it just works.

**Cost:** one drain cycle processes up to `BatchSize` streams. With
the default 100 and a million streams, expect ~10000 cycles to clear
the backlog. Bump `BatchSize` if you need faster cold-start, but
remember each batch is one transaction worth of position updates.

### No-history streams are not a thing

state_stream reads from `state_cache`, which is populated synchronously
in the writer's Append transaction (ADR 0020 § Tier 1). The current
state is always there for every appended stream. There is no edge case
of "this stream has no cache row yet" — unless an operator manually
deleted the cache row, in which case `RebuildStateCache` is the fix
(see crypto-shred section below).

### Per-stream isolation

A delivery failure for stream A does not block streams B and C in the
same batch. The drain counts the failure, calls `OnDeliveryError`,
and moves on. The next cycle retries A *with its current state*
(which may have advanced).

Implication: a permanently-broken subscriber for one stream (e.g., a
schema mismatch only on one record) does not stop the world. It just
wastes one delivery's worth of work per cycle. Catch this with metrics
on per-stream retry count, not by stopping the drain.

### No DLQ in v1 — design call

Outbox has a DLQ for poisoned events. state_stream doesn't. Reasoning:

- Coalesced delivery means there's no "poison message" to quarantine —
  whatever caused the failure is in the *current state*, not a stuck
  historical record.
- If the current state is structurally bad (schema mismatch, decode
  error), bumping `state_schema_version` causes a rebuild + redelivery
  — the *content* updates, not just the framing.
- Operator-driven shred (next section) covers the rare case where one
  stream needs to be excluded.

If you find yourself wishing for a DLQ here, the fix is almost always
"reset just this one stream" via `ResetStateStreamSubscriberForStream`
(see Replay below) — or fix the subscriber.

### Crypto-shred propagation: the 3-step runbook

GDPR / RTBF deletes need to flow into state_stream subscribers too,
not just the eventstore. The runbook:

1. **`ForgetSubject`** on the eventstore — encrypted PII becomes
   un-decryptable. (Cookbook 11.)
2. **`RebuildStateCache`** for the affected streams — rerun the
   decider against the *now-shredded* events, write the new
   PII-free state to `state_cache`. The state_schema_version
   does not need to change — the bytes do.
3. **`ResetStateStreamSubscriberForStream`** for each affected stream
   on each downstream subscriber — forces re-delivery of the
   now-shredded state.

The reason `ResetForStream` exists (vs. just letting the version
increment) is that step 2 doesn't bump the stream's version — it
rewrites the cache. Without an explicit Reset, the subscriber
wouldn't re-pull because `last_delivered_version` is still
current. Reset clears the position row for that stream; the next
drain cycle sees `NULL` and re-delivers.

Schedule step 3 as part of your shred pipeline. If you forget, your
search index still has the PII.

### Schema-version bumps cascade

When you change the state struct shape (add a field, rename, restructure),
bump the codegen `state_schema_version`. The framework writes the new
version to `state_cache` on the next Append for each stream. Subscribers
receive the new schema version in `StateEnvelope.StateSchemaVersion` and
can choose to:

- Quietly accept the new shape (most common — proto schemas are
  forward/backward compatible by default).
- Refuse and bubble an error (will block delivery until subscriber is
  updated).
- Run a one-time mass-`Reset` after deploying the subscriber update.

This is the same conversation as projection schema versioning
(cookbook 08), and the answers are the same.

### Position-row storage math

One row per `(subscriber_name, tenant_id, stream_id)`. For a system
with 5 subscribers and 1M streams per tenant, that's 5M rows in
`state_stream_subscribers`. Each row is ~80 bytes serialized (UUIDs,
timestamps, version int). Budget ~400 MB for 5M positions — call
it a rounding error on top of the events table.

If you have 1000+ subscribers, this gets non-trivial. Consider
hashing the subscriber set into fewer rows, or moving long-tail
subscribers off state_stream onto event-bus consumers.

### Replay = Reset, scoped two ways

The framework gives two reset surfaces via `es.StateStreamAdmin`:

```go
// Reset everything for a subscriber — re-delivers all streams.
admin.ResetStateStreamSubscriber(ctx, "invoice-search-index", tenant)

// Reset just one stream — re-delivers that stream only.
admin.ResetStateStreamSubscriberForStream(ctx,
    "invoice-search-index", tenant, streamID)
```

Use full-reset for: subscriber schema upgrade, downstream system
rebuild, on-call recovery from corrupted index.

Use per-stream reset for: crypto-shred follow-up, fixing one bad
row, debugging.

Neither involves replaying events. `state_cache` is the source of
truth for the *state*; reset just clears the *position* the
subscriber has reached.

## See also

- ADR 0024 — state_stream design (8 decisions, full Q&A)
- ADR 0020 — Projections and Read Models (state_cache is Tier 1)
- ADR 0023 — state_cache supersedes snapshots
- Cookbook 06 — outbox.Drain (sibling pattern, event-by-event)
- Cookbook 07 — read models via materialized views (state_cache use)
- Cookbook 11 — crypto-shredding (step 1 of the 3-step runbook)
- `examples/statestream/` — runnable demo with `Index` subscriber
