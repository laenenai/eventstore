# 10: Restate as the Event Publisher

ADR 0012 designates **Restate** as the recommended publisher for the
serverless / scale-to-zero profile (Neon, Turso, Cloudflare D1).
Restate provides durable invocations with exactly-once dispatch on the
receiver side, which lifts a large burden off the framework: the
publisher just hands an envelope to Restate; Restate guarantees the
handler runs.

This recipe wires up the framework's `publisher/restate` adapter and
the receiver-side handler.

## The setup at a glance

```
Aggregate.Handle ──→ outbox row ──→ Drain.Run ──→ restate.Publisher.Publish
                                                          │
                                                          ▼
                                                    Restate ingress
                                                          │
                                                          ▼
                                              Your Restate service handler
                                                          │
                                                          ▼
                                                Projection / Saga logic
```

Two pieces to wire:

1. **Publisher side** — the outbox drain calls `restate.Publisher.Publish`
   per pending row.
2. **Receiver side** — a Restate service (your code) accepts the POST
   and dispatches to the right handler.

## Publisher side

```go
import "github.com/laenenai/eventstore/publisher/restate"

pub, err := restate.New(restate.Config{
    IngressURL: os.Getenv("RESTATE_INGRESS_URL"),  // e.g. http://restate:8080
    Service:    "event-dispatcher",                // your Restate service name
    Handler:    "OnEvent",                         // handler within the service
    AuthToken:  os.Getenv("RESTATE_TOKEN"),        // optional bearer
})
if err != nil { return err }

drain := &outbox.Drain{
    Store:       store.(es.OutboxStore),
    Publisher:   pub,
    Tenant:      tenantID,         // or "" for cross-tenant
    MaxAttempts: 5,
    BackoffBase: 30 * time.Second,
    BackoffMax:  10 * time.Minute,
}

// Then either Run from a scheduler (Profile B) or in a goroutine
// (Profile A). See cookbook recipe 06.
```

That's it on the framework side. The drain POSTs one HTTP request per
pending outbox row; Restate's ingress accepts and persists the
invocation.

### What goes on the wire

Each POST carries:

| Header / field | Value |
| -------------- | ----- |
| `POST /{Service}/{Handler}` | Path is built from Config |
| `Content-Type: application/x-protobuf` | Body is the raw event payload |
| `Idempotency-Key: <event_id>` | Restate dedupes retries from the drain |
| `X-EventStore-Tenant` | `env.TenantID` |
| `X-EventStore-Stream` | `env.StreamID.Canonical()` |
| `X-EventStore-Version` | `env.Version` |
| `X-EventStore-Global-Position` | `env.GlobalPosition` |
| `X-EventStore-Type-URL` | `env.TypeURL` |
| `X-EventStore-Schema-Version` | `env.SchemaVersion` |
| Body | `env.Payload` (proto bytes) |

The receiver gets everything it needs to decode and dispatch from the
headers + body. No additional envelope serialization.

## Receiver side: the Restate service

The receiver is your code, deployed as a Restate service. The
framework doesn't ship a Restate service runtime — but here's the
canonical handler shape (using the official `github.com/restatedev/sdk-go`).

```go
package main

import (
    "context"

    "github.com/restatedev/sdk-go"

    invoicev1 "myapp/gen/invoice/v1"
)

// EventDispatcher is the Restate service. Single handler that
// dispatches by TypeURL.
type EventDispatcher struct{}

func (EventDispatcher) OnEvent(ctx restate.Context, raw []byte) error {
    typeURL := restate.Request(ctx).Headers["X-EventStore-Type-URL"]
    switch typeURL {
    case "myapp.invoice.v1.Created":
        var e invoicev1.Created
        if err := proto.Unmarshal(raw, &e); err != nil { return err }
        return handleInvoiceCreated(ctx, &e)
    case "myapp.invoice.v1.Paid":
        var e invoicev1.Paid
        if err := proto.Unmarshal(raw, &e); err != nil { return err }
        return handleInvoicePaid(ctx, &e)
    // ...
    }
    // Unknown type: log and return 200 — don't bounce events the
    // publisher will keep re-sending.
    restate.Log(ctx, "ignoring unknown TypeURL", "type", typeURL)
    return nil
}

func handleInvoiceCreated(ctx restate.Context, e *invoicev1.Created) error {
    // Your projection / saga logic. Restate gives you exactly-once
    // semantics here — even if the publisher retries, this runs once.
    return nil
}

func main() {
    if err := restate.Start(context.Background(),
        restate.NewServer().Bind(restate.Service(&EventDispatcher{}))); err != nil {
        panic(err)
    }
}
```

Two important properties:

- **Exactly-once at the handler.** Restate's durable invocation model
  guarantees one execution per `Idempotency-Key`. The drain can retry
  arbitrarily without producing duplicate side effects.
- **Failure surface is non-2xx.** If your handler returns an error,
  Restate retries internally up to its configured retry policy; if
  retries are exhausted, Restate marks the invocation as failed.
  Eventually the drain will time out and bring the row back to
  `pending` for the next cycle.

### Idiomatic dispatch on the receiver side

The switch on TypeURL gets repetitive. Two cleaner patterns:

**Pattern A — codegen-aware generic dispatcher.** Reuse the
framework's per-aggregate `Projection` interface + dispatcher
(ADR 0020 decision 3a). On the receiver:

```go
func (EventDispatcher) OnEvent(ctx restate.Context, raw []byte) error {
    env := es.Envelope{
        TypeURL: restate.Request(ctx).Headers["X-EventStore-Type-URL"],
        Payload: raw,
    }
    // Reuse the generated dispatcher in your projection.
    return projectionHandler(restate.WrapContext(ctx), env)
}
```

**Pattern B — Restate handler per event type.** Register a separate
Restate handler per event variant. The publisher would need to know
the per-event handler name to call, which complicates the URL
construction. Stick with pattern A.

## Deployment notes

- **Restate Ingress URL.** Restate exposes HTTP on a configurable port
  (default 8080 for the ingress). The framework's `restate.Publisher`
  POSTs there; outbound HTTP is the only requirement from the
  database/drain side.
- **Auth.** Restate supports bearer tokens via a fronting proxy.
  `Config.AuthToken` is added as `Authorization: Bearer <token>`.
- **TLS.** Standard HTTPS works; just pass an `https://` IngressURL
  and a properly-configured `http.Client` via `Config.HTTPClient`.
- **Retries.** The framework's outbox drain handles publisher-side
  retries (see recipe 06's backoff section). Restate handles
  receiver-side retries (its own config). The two are independent;
  the `Idempotency-Key` keeps them from colliding.
- **Concurrency.** `restate.Publisher` is safe for concurrent use
  (it holds only a configured `*http.Client`, which is itself
  concurrent-safe).

## When NOT to use Restate

- **Profile A pure-Postgres with `LISTEN/NOTIFY`.** When deferred
  ADR 0001 ships, the in-DB delivery path will be the simpler choice
  for always-on Postgres deployments.
- **Heavy fan-out with cheap subscribers.** Restate's per-invocation
  cost matters when one event drives thousands of subscriber
  invocations. NATS JetStream or a managed bus is cheaper for that
  shape.
- **Existing infrastructure.** If you already operate Kafka, SQS,
  Pub/Sub, or NATS, prefer that — the framework's `EventPublisher`
  abstraction means switching costs little.

The framework is intentionally pluggable here. Restate is the
recommended *default* for serverless deployments, not the only choice.

## Reference

- ADR 0012 — Event Delivery and EventPublisher
- Cookbook recipe 06 — Running the Outbox Drain (deployment patterns
  and retry/DLQ semantics)
- [`publisher/restate/publisher.go`](../../publisher/restate/publisher.go) — adapter source
- [`publisher/publisher.go`](../../publisher/publisher.go) — generic Publisher contract
- Restate docs: <https://docs.restate.dev>
