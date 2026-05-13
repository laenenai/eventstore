# 12: Linked Projections — Derived Event Streams

A normal Tier-3 projection (recipe 08) writes to external storage:
read-model tables, search indexes, queues. Sometimes you want the
output to be **another event stream** — queryable like a primary
stream, with `global_position`, replay, subscribers — but produced
automatically from upstream events.

That's what **linked projections** (Tier 3.5, ADR 0022) ship. They're
the framework's answer to EventStoreDB's `linkTo` / `emit`.

This recipe walks through two realistic patterns:

1. **Routing** — `OrderShipped` becomes a `FulfillmentTask` in the
   warehouse aggregate's stream.
2. **Fan-in audit ledger** — every authz-relevant event from across
   the system collected into one tenant-wide audit stream.

## Pattern 1 — Routing: order shipping → fulfillment tasks

**Scenario.** The order-management context produces `OrderShipped`
events. The warehouse service runs in a different bounded context but
inside the same eventstore. We want each `OrderShipped` to create
exactly one `FulfillmentTask` event in a dedicated fulfillment stream
that the warehouse pickers subscribe to.

### Proto definitions

```proto
// myapp/order/v1/order.proto
package myapp.order.v1;

message OrderShipped {
  string order_id    = 1;
  string customer_id = 2;
  string warehouse   = 3;
  int64  ship_by_ms  = 4;
}
```

```proto
// myapp/fulfillment/v1/fulfillment.proto
package myapp.fulfillment.v1;

import "es/v1/options.proto";

message FulfillmentTask {
  option (es.v1.aggregate) = "fulfillment_task";

  string task_id    = 1 [(es.v1.subject_field) = true];
  string order_id   = 2;
  string warehouse  = 3;
  string status     = 4;  // "pending" / "picked" / "shipped"
  int64  due_by_ms  = 5;
}

message Created {
  string task_id   = 1 [(es.v1.subject_field) = true];
  string order_id  = 2;
  string warehouse = 3;
  int64  due_by_ms = 4;
}

message Events {
  option (es.v1.sum_type) = "Event";
  oneof variant {
    Created created = 1;
  }
}
```

### Wire the linked projection

```go
import (
    "github.com/google/uuid"

    "github.com/laenenai/eventstore/aggregate"
    "github.com/laenenai/eventstore/es"
    "github.com/laenenai/eventstore/linked"
    "github.com/laenenai/eventstore/projection"

    orderv1       "myapp/gen/order/v1"
    fulfillmentv1 "myapp/gen/fulfillment/v1"
)

func startOrderToFulfillment(ctx context.Context, store es.Store, tenant string) error {
    lp, err := linked.New(linked.Config{
        Name:           "order-to-fulfillment",
        Destination:    store,
        SourceTypeURLs: []string{"myapp.order.v1.OrderShipped"},

        Route: func(_ context.Context, env es.Envelope) (linked.Route, error) {
            // Decode the source event.
            shipped := &orderv1.OrderShipped{}
            if err := proto.Unmarshal(env.Payload, shipped); err != nil {
                return linked.Route{}, err
            }

            // Derive the fulfillment task's stream id. One task per
            // order is the natural cardinality; using order_id keeps
            // the destination stream deterministic across replays.
            destStream := es.StreamID{
                Tenant:    env.TenantID,
                Aggregate: "fulfillment_task",
                ID:        "task-" + shipped.OrderId,
            }

            return linked.Route{
                DestinationStream: destStream,
                DerivedEvent: &fulfillmentv1.Created{
                    TaskId:    "task-" + shipped.OrderId,
                    OrderId:   shipped.OrderId,
                    Warehouse: shipped.Warehouse,
                    DueByMs:   shipped.ShipByMs,
                },
                DerivedTypeURL:  "myapp.fulfillment.v1.Created",
                ExpectedVersion: 0, // stream is new — one Created per order
            }, nil
        },
    })
    if err != nil {
        return err
    }

    rt := &projection.Runtime{
        Name:       "linked-order-to-fulfillment",
        Tenant:     tenant,
        Store:      store,
        Checkpoint: store.(projection.Checkpoint),
        Handler:    lp.Handler(),

        // Mirror-the-drain pattern from recipe 06.
        LockKey: "linked-order-to-fulfillment:" + tenant,
    }
    return rt.Run(ctx)
}
```

What the framework does for you:

- **Idempotent emit** (default on). The linked projection claims a
  uniqueness constraint on `(tenant, "linked:order-to-fulfillment",
  source.event_id)` every time it appends. If a replay (cursor reset,
  rebuild) re-issues the same `OrderShipped`, the constraint conflict
  is silently swallowed — no duplicate `Created` event.
- **Lineage.** The `CausationID` on the derived event equals the
  source event's `EventID`. Any audit query can trace
  `Created` back to the originating `OrderShipped`.
- **Synthetic actor.** Derived events are attributed to
  `service:linked-projection:order-to-fulfillment` — easy to
  filter out from human-authored events in audit views.

### Querying the derived stream

Downstream consumers don't know (or care) that the stream is
synthetic. The fulfillment subscriber subscribes to the
`fulfillment_task` aggregate exactly like any other:

```go
disp := fulfillmentv1.NewProjectionDispatcher(&FulfillmentView{db: db})
rt := &projection.Runtime{
    Name:   "fulfillment-view",
    Tenant: tenant,
    Handler: disp,
    ...
}
```

The full chain:

```
OrderShipped (primary, written by command)
    │
    ▼ (linked projection)
fulfillment_task/task-ord-001  v1: Created
    │
    ▼ (warehouse aggregate accepts further commands)
fulfillment_task/task-ord-001  v2: Picked
fulfillment_task/task-ord-001  v3: Shipped
```

The framework treats `Created` as a normal event from version 1
onward. Subsequent commands (`Pick`, `Ship`) come from human / API
input on the warehouse side and append to the same stream.

## Pattern 2 — Fan-in audit ledger

**Scenario.** Compliance needs one unified ledger across every authz-
relevant event from every aggregate. Per ADR 0010 § events, those
events are the source of truth; we want a *projection* of them into
a single stream so subscribers can consume "everything" without
joining multiple aggregate type streams.

### Wire

```go
auditLedger, _ := linked.New(linked.Config{
    Name:        "audit-ledger",
    Destination: store,

    // No SourceTypeURLs filter — we mirror events from every aggregate.
    // The Route function decides which events warrant an audit row.

    Route: func(_ context.Context, env es.Envelope) (linked.Route, error) {
        // Cheap filter: only events with an explicit principal.
        if env.Actor.Principal == "" {
            return linked.Route{Skip: true}, nil
        }

        // One destination stream per tenant. ExpectedVersion grows
        // unboundedly — we look it up from the store. For very high
        // throughput, shard by year or week.
        destStream := es.StreamID{
            Tenant:    env.TenantID,
            Aggregate: "audit_ledger",
            ID:        env.TenantID,
        }
        version, err := currentLedgerVersion(ctx, store, destStream)
        if err != nil {
            return linked.Route{}, err
        }

        return linked.Route{
            DestinationStream: destStream,
            DerivedEvent: &auditv1.Recorded{
                SourceTypeUrl:   env.TypeURL,
                SourceEventId:   env.EventID.String(),
                SourceStreamId:  env.StreamID.Canonical(),
                SourceVersion:   env.Version,
                SourceActor:     env.Actor.Principal,
                SourceOccurred:  env.OccurredAt.UnixMilli(),
            },
            DerivedTypeURL:  "myapp.audit.v1.Recorded",
            ExpectedVersion: version,
        }, nil
    },
})

rt := &projection.Runtime{
    Name:    "linked-audit-ledger",
    Store:   store,         // no Tenant: cross-tenant projector
    Handler: auditLedger.Handler(),
    LockKey: "linked-audit-ledger",
}
```

The `currentLedgerVersion` helper is a small ReadStream lookup. For
high-throughput ledgers, prefer **per-period sharding** (one stream
per tenant per day or week) so `ExpectedVersion` stays bounded and
backfills parallelize naturally:

```go
destStream := es.StreamID{
    Tenant:    env.TenantID,
    Aggregate: "audit_ledger",
    ID:        env.TenantID + ":" + env.OccurredAt.Format("2006-01-02"),
}
```

Consumers query "everything that happened on 2026-03-14" by reading
that single stream.

## Configuration: idempotency, filtering, multi-destination

### Opting out of idempotent emit

```go
yes := false
cfg := linked.Config{
    ...
    IdempotentEmit: &yes,
}
```

When false, the constraint claim is skipped and every replay produces
a duplicate derived event. Only useful when the destination
aggregate dedups by `CausationID` (which is always set to the source
event id by the framework).

### Multi-destination routing

The `Route` callback returns one destination per source event. To
fan out to multiple destinations from one source, compose two
LinkedProjections — each with its own `Name` and constraint scope:

```go
toWarehouse, _  := linked.New(linked.Config{Name: "order-warehouse",  Route: routeWarehouse,  ...})
toAccounting, _ := linked.New(linked.Config{Name: "order-accounting", Route: routeAccounting, ...})

handler := projection.Chain(
    toWarehouse.Handler(),
    toAccounting.Handler(),
)
```

Each linked projection's constraint scope is distinct
(`linked:order-warehouse` vs `linked:order-accounting`), so each can
independently idempotent-emit without interfering with the other.

### Filtering

`SourceTypeURLs` is a TypeURL allowlist. Anything outside the list
short-circuits before `Route` is called — cheap. For richer filters
(e.g., "only events older than 24h"), evaluate inside `Route` and
return `Skip: true`.

## Operational concerns

**Cycle prevention.** The framework doesn't enforce "this derived
stream's aggregate can't feed back into the source set" — that's a
human responsibility. Document the dependency direction in your
architecture diagram. A cycle ([A] → [B] → [A]) produces an event
storm bounded only by the idempotent-emit constraint, but the symptom
is "events keep appearing forever in both streams." Easy to spot in
monitoring.

**Replay cost.** A `Reset` on a linked projection re-issues every
source event. Idempotent emit makes this safe but not free — each
re-issued source goes through `Route` and incurs one constraint
lookup. For high-volume linked projections, prefer
`admin.ResetTo(known-good-position)` instead of full reset.

**Auditability.** Combine with recipe 11's `pii_manifest.json`
review. A linked projection that produces an audit ledger must
itself declare which fields in the derived event are PII (typically
none in an audit stream, but the manifest forces the conversation).

**Latency.** Linked projections run as Tier-3 polling projectors —
delivery latency = projector poll interval + drain interval (recipe
06). For sub-second latency between primary and derived events,
deploy the projector in Profile A (always-on, recipe 06 pattern 2 or
3). Profile B (scale-to-zero) delivers within one scheduled tick.

## When NOT to use linked projections

| If you need... | Reach for... |
| -------------- | ------------ |
| Real-time aggregation (counts, sums) | Tier 2 materialized view over `state_cache` (recipe 07) |
| Synchronous "produce derived event in same tx" | Not possible — linked projections are async by design. Promote the logic into the source aggregate's Decider. |
| External destination (Kafka topic, Elasticsearch) | Plain Tier-3 projection (recipe 08) — don't round-trip through events table just to fan out. |
| Cross-aggregate workflow with compensation | Saga (recipe 02 or 03) — sagas handle multi-step coordination and compensating writes; linked projections produce one derived event per source. |

The right mental model: linked projections are **causal echoes** —
faithful, lineage-tracked, replayable. Anything more (filters,
joins, aggregations) you want elsewhere.

## Reference

- ADR 0022 — Linked Projections (Tier 3.5)
- [`linked/projection.go`](../../linked/projection.go) — the runtime
- Cookbook recipe 06 — Running the Outbox Drain (deployment patterns
  also apply to projection runners)
- Cookbook recipe 08 — Rebuilding Projections (cursor reset replays
  linked projections via the same `admin.Reset` API)
- EventStoreDB [Projections documentation](https://developers.eventstore.com/server/v22.10/projections.html) — prior art for `linkTo`/`emit`
