# Example: cmdworkflow-dbos

A worked example of the cmdworkflow framework running against DBOS.
Mirror of `examples/cmdworkflow-restate`, with the **only difference**
being the workflow runtime:

| Concern | Restate example | This example |
| ------- | --------------- | ------------ |
| Workflow runtime | `cwrestate.New()` | `cwdbos.New()` |
| Generated service | `invoicev1restate.RestateService` | `invoicev1dbos.DBOSService` |
| Transport | HTTP/2 + Restate ingress | Direct `dbos.RunWorkflow` call (library) |
| Journal storage | Restate's own | Same Postgres as the eventstore (`dbos` schema) |
| Idempotency key | `restate.WithIdempotencyKey` | `dbos.WithWorkflowID` |
| Backups | Eventstore PG + Restate journal | One PG, one backup |

The Decider, Subscribers, and Workflow assembly are **identical** —
this is the load-bearing claim of ADR 0026: applications swap
runtime adapters without changing subscriber code.

## When DBOS is the natural fit

- **Postgres-first apps** — your eventstore is already on Postgres,
  and DBOS adds a `dbos` schema to the same database. One backup
  story, one transaction boundary, one consistency model.
- **No appetite for separate runtime infrastructure** — DBOS is a
  library you embed; no Restate cluster to operate, no HTTP/2 bridge
  to maintain.
- **Always-on app pods (Profile A)** — DBOS workers run in your Go
  service process. Scale by adding pods; the journal coordinates.

## When Restate is the better choice

- **Scale-to-zero / serverless DBs** — DBOS keeps connections open
  to recover workflows; Restate's separate runtime can sit idle
  while your serverless app sleeps.
- **Polyglot deployments** — Restate has SDKs in Go, TS, Java,
  Kotlin, Python; one Restate cluster orchestrates them all.
- **You want a managed runtime** — Restate Cloud is a paid managed
  service; DBOS is library-only (no managed runtime).

## Run

```bash
cd examples/cmdworkflow-dbos
go test ./...
```

The test spawns a `postgres:17-alpine` container via testcontainers,
runs the eventstore migrations on the shared pgxpool, and creates a
DBOSContext bound to the same pool. DBOS auto-migrates the `dbos.*`
schema on `Launch`. ~3 seconds end-to-end after first image pull.

No HTTP server. No external runtime. The Connect-go / gRPC / HTTP
edge layer (not shown in the test) sets the tenant on the command
and invokes `dbos.RunWorkflow` directly — no ingress hop, no
serialization roundtrip.

## What the tests exercise

- **`TestDBOSExample_FullLifecycle`** — Create → MarkPaid through the
  generated `DBOSService` workflows. Sync `ReadModel` UPSERT settles
  inline (read-your-writes); Async `AuditLog` fires via Spawn and
  catches up in the background.
- **`TestDBOSExample_SagaCompensation`** — Sync+Compensate saga step.
  The `CreditReservation` subscriber declines deterministically; the
  framework emits the compensating `Void` command under the same
  `DBOSContext` (step names prefixed `compensate:credit-reservation:…`,
  journaled distinctly). The stream ends with `Created + Voided`; the
  active-invoices view drops the voided row. Mirror of the inproc
  `TestExample_SagaCompensation` — same subscriber code, different
  runtime. See cookbook recipe 16 § "Inline compensation under DBOS".

## See also

- ADR 0025 — workflow-orchestrated command bus
- ADR 0026 — workflow adapters (Restate + DBOS)
- `cmdworkflow/README.md` — framework overview
- `examples/cmdworkflow` — same example using inproc (tests / dev)
- `examples/cmdworkflow-restate` — same example using Restate
- Cookbook recipe 14 — deployment patterns + adapter selection
