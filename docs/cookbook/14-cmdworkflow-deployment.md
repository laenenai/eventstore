# 14: Workflow-Orchestrated Command Bus — Deployment

How to deploy `cmdworkflow.Workflow` in production. The framework
ships three runtime adapters; pick by deployment shape, not by
preference:

| Adapter | When | Notes |
| ------- | ---- | ----- |
| `inproc` | Tests, local dev, single-process apps that don't need durability | No journal; crash = lose in-flight Async subscribers |
| `restate` | Profile B (scale-to-zero DBs) **OR** Profile A with a separate workflow runtime | Restate is a separate process; HTTP/2 cleartext between your app and Restate |
| `dbos` | Profile A on Postgres; the workflow journal lives in the same PG as the eventstore | One DB, one backup story. SQLite eventstore + DBOS = unsupported. |

This recipe covers the production wiring for **Restate** (the v1
target). DBOS arrives in Phase 2b; the deployment story is mostly
the same minus the separate-runtime piece — workflows live in a
`dbos` schema in your existing Postgres.

## The three-step start (production app)

Every production app using the Restate adapter does the same three
things at startup. The `cmdworkflow/restate/testsupport` package
does these in test code; copy the pattern into your app's `main`.

### 1. Build the Workflow

Application code, runtime-agnostic. Same shape regardless of adapter:

```go
// main.go
db := openPostgres()                                // your event store
store := pgadapter.New(db)                          // eventstore adapter

// One Workflow per aggregate.
runtime := cwrestate.New()                          // Restate WorkflowRuntime
invoiceWf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command](
    aggregate.NewProto(store, invoice.Decider, invoicev1.EventCodec{}),
    store,
    runtime,
).
    WithDLQ(store).
    With(
        invoiceReadModel.Subscriber(),     // Sync — local read-model
        invoiceSearchIndex.Subscriber(),   // Async — Typesense mirror
        invoiceCreditCheck.Subscriber(),   // Sync+Compensate — saga step
    )

customerWf := cmdworkflow.New[*customerv1.Customer, customerv1.Command](...)...
```

### 2. Bind generated services to Restate

The codegen plugin (`runtime=restate`) emits a `RestateService`
struct per annotated aggregate. Bind each to a Restate server:

```go
import (
    "github.com/restatedev/sdk-go/server"
    invoicev1restate "github.com/laenenai/eventstore/adapters/cmdworkflow/restate/gen/myapp/invoice/v1"
    customerv1restate "github.com/laenenai/eventstore/adapters/cmdworkflow/restate/gen/myapp/customer/v1"
)

srv := server.NewRestate().
    Bind(restate.Reflect(invoicev1restate.NewRestateService(invoiceWf))).
    Bind(restate.Reflect(customerv1restate.NewRestateService(customerWf)))
```

### 3. Listen for HTTP/2 (cleartext) + self-register

Restate calls into your service via HTTP/2 cleartext. Your service
listens on a port; you tell Restate where to find it.

```go
// HTTP/2 cleartext listener — Restate's protocol requirement.
go func() {
    if err := srv.Start(context.Background(), ":9080"); err != nil {
        log.Fatalf("restate server: %v", err)
    }
}()

// Self-register with Restate's admin API. In production this is
// usually done once at deploy time via `restate-cli` rather than
// from the app. For test environments and quick prototypes, the
// app can register itself at startup.
registerURL := fmt.Sprintf("%s/deployments", os.Getenv("RESTATE_ADMIN_URL"))
body := fmt.Sprintf(`{"uri":"%s"}`, os.Getenv("SELF_PUBLIC_URL"))
resp, err := http.Post(registerURL, "application/json", strings.NewReader(body))
// ... check StatusCreated
```

For production: use `restate-cli register http://your-app.example.com:9080`
at deploy time, then drop the self-registration code.

## Deployment topologies

### Topology A — co-located Restate + app (simplest)

```
┌──────────────────────────────────────────┐
│  Pod / VM                                │
│                                          │
│  ┌─────────────┐    ┌──────────────────┐│
│  │ Restate     │◀──▶│  Your Go service ││
│  │ (sidecar)   │    │  (SDK + cwf)     ││
│  │ port 8080,  │    │  port 9080       ││
│  │  9070       │    └────────┬─────────┘│
│  └─────────────┘             │          │
│        ▲                     ▼          │
└────────│─────────────────────│──────────┘
         │ ingress             │ JDBC
         │                     ▼
   ┌─────┴─────┐         ┌──────────┐
   │ HTTP API  │         │ Postgres │
   │ caller    │         └──────────┘
   └───────────┘
```

Restate and your service share a network namespace; communication
is localhost. Restate persists its journal to a local volume.

**When**: small deployments, single-tenant, single-region. Simple to
operate; Restate goes down with the pod.

### Topology B — Restate cluster, app pool

```
   ┌────────────┐    ┌────────────┐    ┌────────────┐
   │ Restate-1  │◀──▶│ Restate-2  │◀──▶│ Restate-3  │  cluster
   └─────┬──────┘    └─────┬──────┘    └─────┬──────┘
         │                  │                  │
         │ HTTP/2           │                  │
         ▼                  ▼                  ▼
   ┌────────────┐    ┌────────────┐    ┌────────────┐
   │ app pod    │    │ app pod    │    │ app pod    │  N replicas
   │ (SDK+cwf)  │    │ (SDK+cwf)  │    │ (SDK+cwf)  │
   └─────┬──────┘    └─────┬──────┘    └─────┬──────┘
         │                  │                  │
         └──────────────────┼──────────────────┘
                            ▼
                      ┌──────────┐
                      │ Postgres │
                      └──────────┘
```

Restate's cluster handles invocation routing + journal replication.
Your app pods are stateless from Restate's perspective — any pod can
handle any invocation; Restate picks one. Postgres is the only
stateful coupling.

**When**: production, multi-tenant, multi-region. Restate operates
as managed infrastructure.

### Topology C — Restate Cloud + serverless app

Restate offers managed cloud; you write only the app side. The
Restate URL points at their endpoint; the rest is the same as
Topology B from the app's perspective.

## Observability

Three places to look when a workflow misbehaves:

| Where | What it tells you |
| ----- | ----------------- |
| Restate's UI / admin API | Per-invocation journal; which step is stuck, retry count, failure messages |
| Your eventstore | Whether `Append` actually committed — the source of truth |
| `subscriber_dlq` table | Per-subscriber permanent failures with `last_error` + `attempts` |

The framework emits no metrics directly — Restate's own metrics
(invocation rate, failure rate, journal storage) cover the workflow
layer; your application metrics cover the read-model + business
side. Adding framework-side Prometheus hooks is a future enhancement
(not on the v1 roadmap).

## Idempotency at the edge

Production apps put their **Connect-go / gRPC / HTTP** layer in front
of the Restate ingress. That layer is where idempotency keys come
from. Three steps:

1. Extract a request id from the inbound HTTP header (`X-Request-Id`,
   or a JWT claim, or a query param).
2. Set it as the Restate idempotency key when invoking:
   ```go
   client := ingress.Service[*invoicev1.Create, *invoicev1.Invoice](
       restateClient, "Invoice", "Create")
   state, err := client.Request(ctx, cmd,
       restatesdk.WithIdempotencyKey(req.Header.Get("X-Request-Id")))
   ```
3. Restate dedupes natively — two calls with the same key get the
   same result, only one invocation actually runs.

Optionally also set the framework's deterministic command_id:
```go
state, err := wf.HandleCmd(ctx, sid, cmd,
    cmdworkflow.WithIdempotencyKey(req.Header.Get("X-Request-Id")))
```

This makes `command_id` deterministic for downstream subscribers
doing ADR 0015-style dedup, even when Restate's own dedup is
bypassed (rare, e.g., during disaster recovery replay from raw
events).

## Common pitfalls

### Sync subscriber slowness blocks the caller

Sync = `HandleCmd` waits for the subscriber. If your read-model
UPSERT takes 5 seconds, every command takes 5 seconds. Match Mode
to what actually requires consistency at command return:

- **Sync** only when read-your-writes matters for THIS subscriber.
- **Async** for everything else (search indexes, audit, webhooks).

Multiple Sync subscribers run in parallel (one `RunAsync` each), so
3 × 100ms subscribers complete in ~100ms, not 300ms. But ONE slow
subscriber still bottlenecks.

### Forgetting to set tenant on commands

Every command must have a `(es.v1.tenant_id) = true` field per ADR
0026 § 3. The codegen-emitted handler builds the StreamID from
that field. An empty `tenant_id` means `StreamID.Tenant = ""`,
which the adapter rejects with `ErrUnknownTenant`. Set it in your
Connect-go layer before invoking:

```go
cmd.TenantId = tenantFromAuthHeader(req)
```

### Restate retries an entire invocation on fn-error

The framework's `runSyncSubscriber` always returns `(bytes, nil)`
from the RunAsync fn — even when the subscriber exhausts. The
"exhausted, here's the lastErr" signal is carried in the bytes,
not the error.

Returning a non-nil error from `RunAsync`'s fn would make Restate
treat the step as failed and retry the WHOLE invocation, which
defeats our retry budget. ADR 0026 § 5c documents this.

If you write custom WorkflowRuntime adapters, observe the same
convention.

### Sub-second deployments

Restate's container takes ~1s to boot. The framework's testcontainer
helper handles cold-start automatically. For production Cloud Run /
Lambda style deploys, prefer **Topology C** (managed Restate Cloud)
or pin Restate as a separate always-on service in **Topology B**.

## See also

- ADR 0025 — workflow-orchestrated command bus
- ADR 0026 — workflow adapters (Restate + DBOS)
- `cmdworkflow/README.md` — framework overview
- `examples/cmdworkflow-restate/` — runnable end-to-end demo
- `adapters/cmdworkflow/restate/testsupport/restate.go` — the
  three-step start in ~120 lines, production-mirror code
- Recipe 06 — running the outbox drain (sibling pattern)
- Recipe 13 — state_stream coalesced delivery (recovery path for
  Async-DLQ subscribers)
