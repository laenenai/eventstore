# Example: Customer View — v2 Spec-Driven Projection

A worked example of the **v2 proto-driven projection codegen** from
ADR 0020. One `.proto` annotation declares a projection's name and
the events it consumes; `protoc-gen-es-go` emits the typed handler
interface, dispatcher, and a stable name constant.

This is a cross-aggregate projection — it consumes events from both
the **Invoice** and **Order** aggregates and maintains a single
denormalized "customer summary" read model.

## The proto annotation

```proto
message CustomerView {
  option (es.v1.projection) = {
    name: "customer-view"
    events: [
      "myapp.invoice.v1.Created",
      "myapp.invoice.v1.Paid",
      "myapp.invoice.v1.Voided",
      "myapp.order.v1.OrderPlaced",
      "myapp.order.v1.OrderShipped",
      "myapp.order.v1.OrderCompleted"
    ]
  };
}
```

That's it. The codegen plugin reads the option, resolves the event
type URLs across the input file set, and emits:

```go
// gen/myapp/customerview/v1/customerview_es.pb.go

const CustomerViewName = "customer-view"

type CustomerViewHandler interface {
    OnCreated(ctx, env, e *invoicev1.Created)        error
    OnPaid(ctx, env, e *invoicev1.Paid)              error
    OnVoided(ctx, env, e *invoicev1.Voided)          error
    OnOrderPlaced(ctx, env, e *orderv1.OrderPlaced)  error
    OnOrderShipped(ctx, env, e *orderv1.OrderShipped) error
    OnOrderCompleted(ctx, env, e *orderv1.OrderCompleted) error
}

func NewCustomerViewDispatcher(
    p CustomerViewHandler,
    opts ...projection.DispatcherOption,
) projection.Handler { ... }
```

## What the application writes

Just the handler implementation:

```go
type View struct { /* read model */ }

func (v *View) OnCreated(_ context.Context, env es.Envelope, e *invoicev1.Created) error {
    // increment open-invoice count, total cents, etc.
    return nil
}

// ... one method per listed event.
```

And the runtime wiring:

```go
view := customerview.NewView()
handler := customerviewv1.NewCustomerViewDispatcher(view, projection.IgnoreUnknown())

rt := &projection.Runtime{
    Name:       "customer-view",
    Tenant:     tenant,
    Store:      store,
    Checkpoint: store.(projection.Checkpoint),
    Handler:    handler,
}
rt.RunOnce(ctx)
```

## Run the tests

```bash
cd examples/customerview
go test ./...
```

Two tests:

1. **AggregatesEventsAcrossAggregates** — produces Invoice + Order
   events for multiple customers, runs the projection, checks the
   per-customer summary rows reflect everything correctly.
2. **ReplayProducesSameState** — running the projection twice against
   the same event stream produces identical state. Handlers must be
   idempotent; this verifies the discipline.

## Key takeaways

**Type safety across aggregates.** Adding an event to the
`(es.v1.projection)` annotation produces a new method on
`CustomerViewHandler` — and a compile error in every implementation
until you decide how to handle it.

**IgnoreUnknown() for cross-aggregate composition.** The dispatcher
errors by default on unknown TypeURLs. When consuming events from
multiple aggregates whose union of types changes over time,
`IgnoreUnknown()` says "I only know about the events I listed; skip
anything else."

**No projection codegen sprawl.** One annotation, one generated
file, one application-side handler. The application owns the read
model shape; the framework owns dispatch.

**Replay safety via Checkpoint.** The projection.Runtime tracks
progress through a `projection_checkpoint` row. Reset → full replay;
ResetTo → partial replay; the codegen'd dispatcher handles each
event identically whether it's live or replayed.

## See also

- ADR 0020 — Projections and Read Models (§ v2 future direction)
- Cookbook recipe 08 — Rebuilding projections (covers the Reset workflow)
- Cookbook recipe 12 — Linked projections (the other Tier 3.5 pattern)
- [`projection/`](../../projection/) — Runtime + Dispatcher infrastructure
