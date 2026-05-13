# 01: Stateless Process Manager

The simplest cross-aggregate pattern. Subscribe to an event, dispatch one
or more commands, no saga-internal state.

## When to use this

- You react to a single event with no need to wait for other events.
- The decision of *what to dispatch* depends only on the incoming event,
  not on history.
- Examples: "when an order is placed, reserve inventory"; "when a user
  registers, enqueue a welcome email"; "when a payment is captured, mark
  the invoice paid".

If you need state across multiple events ("wait for both A and B"),
reach for recipe 02 (stateful saga) instead.

## The pattern

A subscriber registered with the `EventPublisher` calls the target
aggregate's `Handle` method directly. Idempotency under at-least-once
bus delivery is provided by `command_id` dedup at the target.

## Example: reserve inventory when an order is placed

```go
package inventoryreserver

import (
    "context"

    "github.com/<org>/eventstore/es"
    "github.com/<org>/myapp/inventory"
    "github.com/<org>/myapp/orderpb"
)

type Handler struct {
    inv *inventory.Runtime
}

func (h *Handler) Handle(ctx context.Context, env es.Envelope) error {
    placed, ok := env.Payload.(*orderpb.OrderPlaced)
    if !ok {
        return nil // event we don't care about — let the framework move on
    }

    sid, err := inventory.NewStreamID(ctx, placed.ItemId)
    if err != nil {
        return err
    }

    // Deterministic command_id: same source event + same handler + same
    // output position always produce the same UUID. The inventory
    // aggregate dedups by command_id, so bus retries are safe.
    cmdID := es.DeriveCommandID("inventory-reserver", env.EventID, 0)

    return h.inv.Handle(ctx, sid,
        &inventory.Reserve{
            OrderID: placed.OrderId,
            ItemID:  placed.ItemId,
            Qty:     placed.Qty,
        },
        es.WithCommandID(cmdID),
    )
}
```

Wiring once in `main.go`:

```go
pub := restatepublisher.New(rt, ...)
reserver := &inventoryreserver.Handler{inv: inventory.NewRuntime(...)}

pub.Subscribe(
    []es.EventTypeURL{"myapp.order.v1.OrderPlaced"},
    reserver.Handle,
)
```

That's the entire pattern. The framework does not need to know that
`inventoryreserver` exists in any special way — it's a subscriber like
any other.

## Why this is safe under retries

1. The bus may deliver `OrderPlaced` more than once (at-least-once
   semantics from ADR 0012).
2. Each delivery calls `Handle` with the same `env.EventID`.
3. `DeriveCommandID` is pure: same inputs → same `command_id`.
4. The inventory aggregate has already deduplicated this `command_id`
   from a previous successful retry; the second call returns success
   without re-applying the command.

If the bus is Restate, the handler invocation itself is exactly-once at
the Restate runtime level, and the `command_id` dedup is a belt-and-
suspenders safety net.

## Multiple commands per event

If one event must trigger multiple commands, give each a distinct
`outputIndex`:

```go
cmdID1 := es.DeriveCommandID("multi-dispatcher", env.EventID, 0)
cmdID2 := es.DeriveCommandID("multi-dispatcher", env.EventID, 1)

if err := target.Handle(ctx, sid1, cmd1, es.WithCommandID(cmdID1)); err != nil {
    return err
}
return otherTarget.Handle(ctx, sid2, cmd2, es.WithCommandID(cmdID2))
```

If the first call succeeds and the handler crashes before the second,
the next delivery re-runs both. The first is a no-op (already
dedup'd); the second proceeds. No saga state needed.

## What this does not handle

- **State across events.** "Grant access only when registered AND
  verified AND paid" needs recipe 02.
- **Rollback if a later step fails.** "If inventory reservation fails,
  cancel the order" needs recipe 03 (compensation).
- **Time-based decisions.** "Cancel the order if no payment in 24h"
  needs recipe 04.

For everything else, this five-line subscriber is enough.
