# Example: Order → Fulfillment (Linked Projections)

Demonstrates **Tier 3.5 linked projections** (ADR 0022, cookbook 12).
Two aggregates wired together by a framework-managed derived stream:

```
Order aggregate (commands: Place, Ship, Complete)
        │
        ▼ OrderShipped event
        │
   linked projection
        │
        ▼ Created event
Fulfillment aggregate (commands: Pick, MarkShipped)
```

The linked projection:

- Watches for `myapp.order.v1.OrderShipped` events.
- For each, derives a `Created` event in a dedicated `fulfillment_task`
  stream (one stream per order).
- Idempotent emit: a uniqueness constraint on the source event_id
  ensures replays don't produce duplicate `Created` events.
- Lineage: each derived event's `CausationID` points at the source's
  `EventID`.

## Run the tests

```bash
cd examples/orderfulfillment
go test ./...
```

The single test exercises the full chain end-to-end:

1. PlaceOrder + Ship on the source aggregate.
2. Linked projection processes OrderShipped → produces Created.
3. Warehouse service: Pick + MarkShipped on the destination
   aggregate.
4. Verify final state.
5. Replay the projection (cursor reset) — no duplicate Created.

## Files

- `proto/myapp/order/v1/order.proto`            — source aggregate
- `proto/myapp/fulfillment/v1/fulfillment.proto` — destination aggregate
- `decider.go`        — both deciders, separately typed and
  independent
- `orderfulfillment_test.go` — full end-to-end

## Key takeaways

**The derived stream is queryable like a primary stream.** Consumers
subscribing to `fulfillment_task` events don't know (or care) that
the first event was produced by a linked projection — it's just a
normal stream of events with `global_position`, version, the works.

**Two aggregates, one event-table.** Both aggregates live in the
same eventstore. The linked projection appends across them; cycle
prevention is the operator's responsibility (don't make the
destination aggregate's events feed back into the source).

**Idempotent replay is free.** The framework's uniqueness primitive
(used here as a constraint claim on `linked:<name>` + source event
id) makes "what if the projection runs the same source event
twice?" a non-question.

## See also

- ADR 0022 — Linked Projections (Tier 3.5)
- Cookbook recipe 12 — Linked projections (routing + fan-in patterns)
- [`linked/`](../../linked/) — the runtime package
