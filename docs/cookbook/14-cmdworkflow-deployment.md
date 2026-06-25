# 14: Workflow-Orchestrated Command Bus — Deployment

How to deploy `cmdworkflow.Workflow` in production.

**The recommended production wiring is DBOS** (ADR 0033). DBOS is a
library that embeds in your Go process and lays its workflow journal
tables in your existing eventstore database — one DB, one backup,
one transaction story. It supports Postgres for production and
SQLite for local dev. Reach for the Restate adapter only when you
have a specific reason (polyglot fleet, managed-runtime preference,
scale-to-zero pairing) — see *Alternative deployments* below.

| Adapter | Status | Best for | Operational footprint |
| ------- | ------ | -------- | --------------------- |
| **`dbos`** | **Default — actively maintained** | Postgres-first or SQLite-for-dev apps. One DB, one backup, one transaction story. | Library — no extra process. App embeds it; `dctx.Launch(ctx)` starts pollers in goroutines. |
| `restate` | Community-maintained (ADR 0033 § 3) | Polyglot deployments (Go + TS + Java in one fleet). Managed-runtime preference (Restate Cloud). Scale-to-zero DBs where the workflow runtime must stay alive while the DB sleeps. | Separate cluster — HTTP/2 cleartext between your app and Restate, own journal storage. |
| `inproc` | Tests / local prototypes only | Single-process prototypes that don't need durability. | No journal; crash = lose in-flight Async subscribers. |

This recipe walks the DBOS path twice (Postgres for production, then
SQLite for local dev), then covers cross-adapter concerns
(observability, idempotency, pitfalls). The Restate path lives in
*Alternative deployments* at the end of the recipe.

## DBOS topology — library embedded in your app

DBOS is a library, not a separate runtime. Your Go service:

1. Builds the `cmdworkflow.Workflow` as usual.
2. Constructs a `dbos.DBOSContext` against the eventstore's Postgres.
3. Registers each codegen-emitted `DBOSService.<Command>` workflow.
4. Calls `dctx.Launch()` — DBOS starts its pollers + recovery
   goroutines inside your process.
5. Your Connect-go / gRPC / HTTP handler invokes commands via
   `dbos.RunWorkflow(dctx, svc.Create, cmd, dbos.WithWorkflowID(reqID))`.

```go
// main.go — production app, DBOS adapter
pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
store := pgadapter.New(pool)
store.Migrate(ctx) // eventstore tables

// Workflow: same shape as inproc + restate examples. Per-batch
// subscriber delivery (ADR 0029) means the constructor takes the
// event Codec[E] alongside the aggregate runtime + store + runtime
// adapter.
wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
    aggregate.NewProto(store, invoice.Decider, invoicev1.EventCodec{}),
    store,
    cwdbos.New(),
    invoicev1.EventCodec{},
).
    WithDLQ(store).
    With(readModel.Subscriber(), searchIndex.Subscriber(), creditCheck.Subscriber())

// DBOS context sharing the pgxpool — one PG, two schemas.
dctx, _ := dbos.NewDBOSContext(ctx, dbos.Config{
    DatabaseURL:  os.Getenv("DATABASE_URL"),
    AppName:      "myapp",
    SystemDBPool: pool,
})

// Register the codegen-emitted DBOSService methods as DBOS workflows.
svc := invoicev1dbos.NewDBOSService(wf)
dbos.RegisterWorkflow(dctx, svc.Create,   dbos.WithWorkflowName("Invoice.Create"))
dbos.RegisterWorkflow(dctx, svc.MarkPaid, dbos.WithWorkflowName("Invoice.MarkPaid"))
dbos.RegisterWorkflow(dctx, svc.Void,     dbos.WithWorkflowName("Invoice.Void"))

if err := dctx.Launch(); err != nil { log.Fatal(err) }

// Now your Connect-go handlers invoke commands directly.
func (h *InvoiceHandler) Create(ctx context.Context, req *connect.Request[CreateReq]) (...) {
    cmd := buildCmd(req, h.tenantFromAuth(req))
    handle, err := dbos.RunWorkflow(dctx, svc.Create, cmd,
        dbos.WithWorkflowID(req.Header().Get("X-Request-Id")))
    if err != nil { return nil, err }
    return handle.GetResult()
}
```

That's the whole production deployment. No sidecar, no admin port,
no HTTP/2 protocol bridge. Backups: one `pg_dump`. Migrations: your
eventstore migrations + DBOS's auto-migrations on first `Launch`,
both in the same PG.

### DBOS deployment topology

```
┌────────────────────────────────────────────────┐
│  Pod / VM                                      │
│                                                │
│  ┌──────────────────────────────────────────┐  │
│  │  Your Go service                         │  │
│  │   ┌────────────────┐  ┌────────────────┐ │  │
│  │   │ Connect/HTTP   │  │ DBOS workers   │ │  │
│  │   │ handlers       │  │ (in-proc       │ │  │
│  │   │  ↓             │  │  goroutines)   │ │  │
│  │   │ dbos.RunWorkflow│ │                │ │  │
│  │   └────────────────┘  └────────────────┘ │  │
│  └─────────────────┬────────────────────────┘  │
└────────────────────│───────────────────────────┘
                     │ PGX
                     ▼
            ┌──────────────────┐
            │ Postgres         │
            │  ├─ eventstore   │  (events, state_cache,
            │  │  tables       │   outbox, subscriber_dlq…)
            │  └─ dbos schema  │  (workflow_status,
            │                  │   operation_outputs…)
            └──────────────────┘
```

**When**: most production apps. Single binary, one stateful
dependency, scale by adding app pods.

### DBOS scaling

DBOS coordinates work across N app replicas via the workflow_status
table. Adding a pod adds another worker. Idempotency keys
(`WithWorkflowID`) ensure a request-id duplicated across pods runs
exactly once.

## DBOS workflows on SQLite for local dev

ADR 0033 § 2 retracted the previous "DBOS requires Postgres"
limitation. The DBOS Go SDK (v0.16.0+) accepts a `*sql.DB` handle
pointing at SQLite via `Config.SqliteSystemDB`. The framework's
SQLite eventstore adapter accepts the same handle. That means a
local dev demo can run the **full production architecture** — same
codegen, same `cmdworkflow.Workflow`, same `DBOSService` registration
— against one SQLite file with no Docker and no Postgres.

The shape:

- One `*sql.DB` handle pointing at a SQLite file.
- The eventstore SQLite adapter (`adapters/storage/sqlite`) uses that
  handle.
- The DBOS context uses the same handle via
  `dbossdk.Config.SqliteSystemDB`.
- DBOS lays its workflow journal tables alongside the framework's
  event log in the same SQLite file.
- One file, one transaction story, one backup.

```go
// local-dev / single-binary demo — DBOS on SQLite
path := filepath.Join(workDir, "myapp.db")
db, err := sql.Open("sqlite", path)
if err != nil { log.Fatal(err) }
// Single-connection pool sidesteps SQLite's "database is locked"
// surprise under concurrent writers — fine for local dev.
db.SetMaxOpenConns(1)

store := sqliteadapter.New(db)
if err := store.Migrate(ctx); err != nil { log.Fatal(err) }

dctx, err := dbossdk.NewDBOSContext(ctx, dbossdk.Config{
    AppName:        "myapp-local",
    SqliteSystemDB: db, // <-- the v0.16.0 hook
})
if err != nil { log.Fatal(err) }

wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
    aggregate.NewProto(store, invoice.Decider, invoicev1.EventCodec{}),
    store,
    cwdbos.New(),
    invoicev1.EventCodec{},
).WithDLQ(store)

svc := invoicev1dbos.NewDBOSService(wf)
dbossdk.RegisterWorkflow(dctx, svc.Create,        dbossdk.WithWorkflowName("Invoice.Create"))
dbossdk.RegisterWorkflow(dctx, svc.AsyncDispatch, dbossdk.WithWorkflowName("Invoice.AsyncDispatch"))

if err := dctx.Launch(); err != nil { log.Fatal(err) }

// Handlers invoke commands via dbos.RunWorkflow exactly as in
// production. The only thing that changes between local-dev and
// production is the *sql.DB handle and the SqliteSystemDB vs
// DatabaseURL/SystemDBPool config knob.
```

This is the same architecture as production. Same codegen, same
`Workflow`, same `DBOSService` registration, same `RunWorkflow` call
site. There is no "but in production…" caveat — the only thing that
swaps is the storage handle and the DBOS config knob.

### Test fixture and worked example

The framework ships a test fixture for this configuration:

- **`adapters/cmdworkflow/dbos/testsupport/sqlite.go`** —
  `StartSQLite(t)` returns a ready `*SqliteEnv` (`*sql.DB`,
  `DBOSContext`, eventstore adapter) wired against a fresh SQLite
  file under `t.TempDir`. Caller registers workflows and calls
  `Launch`; cleanup runs in LIFO via `t.Cleanup`.
- **`adapters/cmdworkflow/dbos/sqlite_spike_test.go`** — two passing
  tests that demonstrate the end-to-end shape. `TestDBOS_SQLiteSystemDB_BasicCreate`
  covers the Sync command flow (RunWorkflow → service handler →
  framework HandleCmd → SQLite append). `TestDBOS_SQLiteSystemDB_AsyncSubscriber`
  covers the harder case: an Async subscriber delivered through
  DBOS's queue runner against a SQLite handle. Both files are the
  canonical reference for adopters who want to mirror the pattern.

### When to use SQLite + DBOS

- **Local dev demos** — `go run ./cmd/myapp-cli` works without
  Docker, without testcontainers, without a Postgres install. The
  command bus is real (durable journal, idempotency, retries,
  DLQ), the storage is real (events, state_cache, outbox), just
  inside one file.
- **Integration tests** — `go test` against `StartSQLite(t)` is
  ~100 ms cold-start. Production-mirror architecture without a
  testcontainer dependency.
- **Single-tenant single-binary deployments** — small apps that
  ship as one binary with a SQLite file alongside (CLIs, on-prem
  appliances). Production-grade durability and dedup, no
  infrastructure to operate.

### When NOT to use SQLite + DBOS

- **Multi-pod production deployments.** SQLite is single-writer;
  DBOS's worker coordination assumes the system DB is reachable
  from every pod. Use Postgres.
- **Anything beyond ~hundreds of writes/sec.** SQLite's writer
  serialization caps throughput; the framework hits the limit
  before DBOS does.
- **Multi-region or HA setups.** Same as above — SQLite doesn't
  give you replicated storage.

## Observability

Three places to look when a workflow misbehaves:

| Where | What it tells you |
| ----- | ----------------- |
| The workflow runtime's UI / admin API (DBOS admin, Restate UI) | Per-invocation journal; which step is stuck, retry count, failure messages |
| Your eventstore | Whether `Append` actually committed — the source of truth |
| `subscriber_dlq` table | Per-subscriber permanent failures with `last_error` + `attempts` |

The framework emits no metrics directly — the workflow runtime's own
metrics (invocation rate, failure rate, journal storage) cover the
workflow layer; your application metrics cover the read-model +
business side. Adding framework-side Prometheus hooks is a future
enhancement (not on the v1 roadmap).

## Idempotency at the edge

Production apps put their **Connect-go / gRPC / HTTP** layer in
front of the workflow runtime. That layer is where idempotency keys
come from.

For DBOS, the idempotency key is the workflow id:

```go
handle, err := dbos.RunWorkflow(dctx, svc.Create, cmd,
    dbos.WithWorkflowID(req.Header.Get("X-Request-Id")))
```

DBOS dedupes natively — two `RunWorkflow` calls with the same
workflow id return the same result, only one invocation actually
runs. Pair with the framework's deterministic command_id if
downstream subscribers do ADR 0015-style dedup:

```go
state, err := wf.HandleCmd(ctx, sid, cmd,
    cmdworkflow.WithIdempotencyKey(req.Header.Get("X-Request-Id")))
```

This makes `command_id` deterministic for downstream subscribers
even when the runtime's own dedup is bypassed (rare, e.g., during
disaster recovery replay from raw events).

(The Restate equivalent is `restatesdk.WithIdempotencyKey`; see
*Alternative deployments* below.)

## Common pitfalls

### Sync subscriber slowness blocks the caller

Sync = `HandleCmd` waits for the subscriber. If your read-model
UPSERT takes 5 seconds, every command takes 5 seconds. Match Mode
to what actually requires consistency at command return:

- **Sync** only when read-your-writes matters for THIS subscriber.
- **Async** for everything else (search indexes, audit, webhooks).

Multiple Sync subscribers run in parallel (one `RunAsync` each — on
DBOS, one `StartChildStep` each), so 3 × 100ms subscribers complete
in ~100ms, not 300ms. But ONE slow subscriber still bottlenecks.

### Forgetting to set tenant on commands

Every command must have a `(es.v1.tenant_id) = true` field per ADR
0026 § 3. The codegen-emitted handler builds the StreamID from that
field. An empty `tenant_id` means `StreamID.Tenant = ""`, which the
adapter rejects with `ErrUnknownTenant`. Set it in your Connect-go
layer before invoking:

```go
cmd.TenantId = tenantFromAuthHeader(req)
```

### Runtime retries an entire invocation on fn-error

The framework's `runSyncSubscriber` always returns `(bytes, nil)`
from the runtime step fn — even when the subscriber exhausts. The
"exhausted, here's the lastErr" signal is carried in the bytes, not
the error.

Returning a non-nil error would make the runtime treat the step as
failed and retry the WHOLE invocation, which defeats our retry
budget. ADR 0026 § 5c documents this for Restate; the DBOS adapter
observes the same convention against `StartChildStep`. If you write
custom `WorkflowRuntime` adapters, observe the same convention.

## Alternative deployments

The Restate adapter remains in the tree as community-maintained
(ADR 0033 § 3). It works against the current `cmdworkflow` API and
existing deployments need not change. Reach for it only when you
have a concrete reason; otherwise prefer the DBOS topology above.

> **Heads-up.** The DBOS topology in the first half of this recipe
> is the recommended default. The Restate sections below stay
> useful for adopters with an existing Restate deployment, polyglot
> fleets, or a managed-runtime preference. New code that doesn't
> have one of those constraints should start with DBOS.
>
> See `adapters/cmdworkflow/restate/STATUS.md` for the maintenance
> posture (community-maintained, nightly integration cadence,
> deletion gate).

### Restate topology — separate runtime

Reach for Restate when one of these is true:

- **Polyglot service fleet.** Restate orchestrates Go + TS + Java +
  Python from one cluster.
- **Scale-to-zero.** Your serverless DB (Neon, Turso, D1) goes
  cold; Restate's separate runtime keeps invocation journals alive.
- **Managed runtime preference.** Restate Cloud is a paid managed
  service; DBOS is library-only.

The Restate setup is more involved: HTTP/2 server, self-register
with admin API, run a Restate cluster (or use Cloud).

### Restate: the three-step start (production app)

Every production app using the Restate adapter does the same three
things at startup. The `cmdworkflow/restate/testsupport` package
does these in test code; copy the pattern into your app's `main`.

#### 1. Build the Workflow

Application code, runtime-agnostic. Same shape regardless of adapter:

```go
// main.go
db := openPostgres()                                // your event store
store := pgadapter.New(db)                          // eventstore adapter

// One Workflow per aggregate. The Workflow is generic over
// [S, C, E] (ADR 0029) so subscribers receive typed events
// alongside the post-Decide state in one Handle call.
runtime := cwrestate.New()                          // Restate WorkflowRuntime
invoiceWf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
    aggregate.NewProto(store, invoice.Decider, invoicev1.EventCodec{}),
    store,
    runtime,
    invoicev1.EventCodec{},
).
    WithDLQ(store).
    With(
        invoiceReadModel.Subscriber(),     // Sync — local read-model
        invoiceSearchIndex.Subscriber(),   // Async — Typesense mirror
        invoiceCreditCheck.Subscriber(),   // Sync+Compensate — saga step
    )

customerWf := cmdworkflow.New[*customerv1.Customer, customerv1.Command, customerv1.Event](...)...
```

#### 2. Bind generated services to Restate

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

#### 3. Listen for HTTP/2 (cleartext) + self-register

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

### Restate deployment topologies

#### Topology A — co-located Restate + app (simplest)

```
┌──────────────────────────────────────────┐
│  Pod / VM                                │
│                                          │
│  ┌─────────────┐    ┌──────────────────┐ │
│  │ Restate     │◀──▶│  Your Go service │ │
│  │ (sidecar)   │    │  (SDK + cwf)     │ │
│  │ port 8080,  │    │  port 9080       │ │
│  │  9070       │    └────────┬─────────┘ │
│  └─────────────┘             │           │
│        ▲                     ▼           │
└────────│─────────────────────│───────────┘
         │ ingress             │ PGX
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

#### Topology B — Restate cluster, app pool

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

#### Topology C — Restate Cloud + serverless app

Restate offers managed cloud; you write only the app side. The
Restate URL points at their endpoint; the rest is the same as
Topology B from the app's perspective.

### Restate idempotency at the edge

Same shape as the DBOS path, different SDK call:

```go
client := ingress.Service[*invoicev1.Create, *invoicev1.Invoice](
    restateClient, "Invoice", "Create")
state, err := client.Request(ctx, cmd,
    restatesdk.WithIdempotencyKey(req.Header.Get("X-Request-Id")))
```

Restate dedupes natively — two calls with the same key get the same
result, only one invocation actually runs.

### Restate-specific pitfalls

#### Restate retries an entire invocation on fn-error

ADR 0026 § 5c documents the Restate-specific shape: the framework's
`runSyncSubscriber` always returns `(bytes, nil)` from the
`RunAsync` fn — even when the subscriber exhausts. The "exhausted,
here's the lastErr" signal is carried in the bytes, not the error.
A non-nil error from `RunAsync`'s fn would make Restate treat the
step as failed and retry the WHOLE invocation.

#### Sub-second deployments

Restate's container takes ~1s to boot. The framework's testcontainer
helper handles cold-start automatically. For production Cloud Run /
Lambda style deploys, prefer **Topology C** (managed Restate Cloud)
or pin Restate as a separate always-on service in **Topology B**.

## See also

- ADR 0025 — workflow-orchestrated command bus
- ADR 0026 — workflow adapters (Restate + DBOS), amended by ADR 0033
- ADR 0033 — DBOS as the default command-bus adapter
- `adapters/cmdworkflow/restate/STATUS.md` — Restate maintenance
  posture and deletion gate
- `adapters/cmdworkflow/dbos/testsupport/sqlite.go` — SQLite + DBOS
  test fixture
- `adapters/cmdworkflow/dbos/sqlite_spike_test.go` — worked example
  of SQLite + DBOS Sync + Async
- `cmdworkflow/README.md` — framework overview
- `examples/cmdworkflow-restate/` — runnable Restate end-to-end demo
- `adapters/cmdworkflow/restate/testsupport/restate.go` — the
  three-step Restate start in ~120 lines, production-mirror code
- Recipe 06 — running the outbox drain (sibling pattern)
- Recipe 13 — state_stream coalesced delivery (recovery path for
  Async-DLQ subscribers)
