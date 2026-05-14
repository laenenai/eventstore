# Example: state_stream

A worked example demonstrating **coalesced state-mirror delivery**
(ADR 0024 / cookbook recipe 13): a `state_stream.Drain` reading from
`state_cache` and feeding an `es.StatePublisher` subscriber that maintains
a search-index-like view of every Invoice aggregate.

In production the subscriber would be Elasticsearch / Algolia / a separate
Postgres read model / an event-bus relay. Here it's an in-memory map so
the example fits in one file.

## What state_stream is for

When you want **the current state** of every aggregate mirrored elsewhere
— for full-text search, ad-hoc analytics, CDC to a separate system —
not the full event history. The framework already gives you `outbox` for
event-by-event delivery; state_stream is the dual for *state-by-state*
delivery, with coalescing as the central design property.

| Property | What it means |
| -------- | ------------- |
| **Coalesced** | One delivery per stream per drain cycle. If a stream gets 10 events between two drain ticks, the subscriber sees one row with the final state — not 10. |
| **Coalesced-on-retry** | If a delivery fails and the stream advances before the next cycle, the *latest* state is delivered next time — not the failed version. |
| **Receiver idempotent** | The subscriber's `PublishState` must accept duplicate versions and no-op. See `index.go`'s `existing.Version >= env.Version` guard. |
| **Cold-start backfill** | A subscriber added after streams already exist will be delivered the current state of every existing stream on its first `Run`. No-history backfill is automatic. |
| **Replay = Reset** | `ResetStateStreamSubscriber` deletes the subscriber's position rows; the next drain redelivers from scratch. |

## Run the tests

```bash
cd examples/statestream
go test ./...
```

Three tests cover:

- **`ColdStartBackfill`** — three invoices created before the subscriber
  exists; the first drain run delivers all three.
- **`DeltaDelivery`** — incremental state changes produce single
  deliveries; idle runs are zero-delivery.
- **`IdempotentReceiver`** — after `Reset` the drain re-runs and the
  subscriber's index is unchanged (version-based dedup).

## Wiring in your app

```go
import (
    "github.com/laenenai/eventstore/state_stream"
    "github.com/laenenai/eventstore/adapters/storage/postgres"
)

func main() {
    store := postgres.New(pool) // implements es.StateStreamStore + StateStreamAdmin
    index := NewIndex()

    drain := &state_stream.Drain{
        SubscriberName: "invoice-search-index",  // stable name → position rows
        Tenant:         "acme",                  // per-tenant subscriber
        Store:          store,
        Publisher:      index,
        BatchSize:      200,
        LockKey:        "invoice-search-index",  // optional cross-replica single-runner
    }

    // Periodic: run from cron, or in a goroutine with a ticker.
    for {
        if _, err := drain.Run(ctx); err != nil { /* log */ }
        time.Sleep(2 * time.Second)
    }
}
```

## Subtleties covered in cookbook recipe 13

- Coalescing-on-retry semantics — what the subscriber sees after failures.
- Cold-start vs. steady-state — backfill costs and how to bound them.
- Per-stream isolation — failures on one stream don't block others.
- No DLQ in v1 — design tradeoff and what to do instead.
- Crypto-shred propagation — the 3-step operator runbook.
- Position-row storage math.
- Schema-version bumps cascade into redelivery.

## Files

- `index.go` — the `Index` subscriber: `es.StatePublisher` implementing
  idempotent-by-version upsert + secondary index by customer.
- `index_test.go` — three end-to-end tests against in-memory SQLite +
  in-process invoice runtime.

## See also

- ADR 0024 — state_stream design (8 decisions)
- ADR 0020 — Projections and Read Models (Tier 1 state_cache foundation)
- ADR 0023 — state_cache supersedes snapshots
- Cookbook recipe 13 — state_stream (full subtleties)
- Cookbook recipe 06 — outbox.Drain (sibling pattern; same operational model)
