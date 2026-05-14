# cmdworkflow

Workflow-orchestrated command handler + subscriber registry. One
generic entry point per aggregate — `Workflow.HandleCmd` — appends
events through `aggregate.Runtime`, then fans out to registered
subscribers governed by per-subscriber `Mode`, `MaxRetries`, and
`OnExhausted` policy.

See **ADR 0025** for the full design and **architecture overview
§ 6** for the topology diagram.

## What it replaces

Without this package you'd write one Restate / DBOS handler per
aggregate, each doing:

1. `ctx.Run("append", ...)` to call `aggregate.Runtime.Handle`
2. `ctx.Run("subscriber-1", ...)` for each subscriber, with its own
   retry policy, DLQ logic, and compensation flow

Per aggregate × per subscriber × per command-type = a lot of
hand-written orchestration. The package collapses it into one
generic shape and a declarative subscriber registry.

## Quick start

```go
import (
    "github.com/laenenai/eventstore/aggregate"
    "github.com/laenenai/eventstore/cmdworkflow"
    "github.com/laenenai/eventstore/cmdworkflow/inproc"
)

// One aggregate.Runtime per aggregate.
rt := aggregate.NewProto(store, invoiceDecider, invoicev1.EventCodec{})

// One Workflow per aggregate. The workflow runtime (inproc here;
// Restate or DBOS in production) is the pluggable durability layer.
wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command](rt, store, inproc.New()).
    WithDLQ(store).
    With(
        readModel.Subscriber(),     // Sync, DLQ — local read model
        searchIndex.Subscriber(),   // Async, DLQ — external mirror
        creditCheck.Subscriber(),   // Sync, Compensate — saga step
    )

// Single entry point for every command of this aggregate.
state, err := wf.HandleCmd(ctx, streamID, &invoicev1.Create{...})
```

## The three knobs

Each `Subscriber` declares three orthogonal properties at
registration. The combinations cover every common subscriber kind
without proliferating per-flavor code paths.

| Knob | Values | Effect |
| ---- | ------ | ------ |
| `Mode` | `Sync` / `Async` | Whether `HandleCmd` blocks on the subscriber's completion. |
| `MaxRetries` | `0..N` / `-1` | Failure budget. `-1` = retry on every workflow replay forever. |
| `OnExhausted` | `DLQ` / `Compensate` / `Drop` | Behavior after retries exhausted. |

Plus optional `AttemptTimeout` for per-call deadlines, `Filter` for
declarative event narrowing (TypeURL / StreamGlob / Tenants / Custom
predicate), and `Compensate` (required for `OnExhausted = Compensate`)
that returns a compensating command to be appended through the bus.

| Use case | Mode | MaxRetries | OnExhausted | Compensate |
| -------- | ---- | ---------- | ----------- | ---------- |
| Local read-model UPSERT | `Sync` | 3 | `DLQ` | — |
| Saga step (reserve, charge, etc.) | `Sync` | 5 | `Compensate` | reverses the step |
| State mirror to search index | `Async` | `-1` | `DLQ` | — |
| Audit / analytics fan-out | `Async` | 10 | `DLQ` | — |
| Best-effort webhook | `Async` | 3 | `Drop` | — |

## What's in the package

- **`Workflow[S, C]`** — the generic command handler. One instance
  per aggregate (the `E` parameter on the underlying runtime is
  hidden behind the `AggregateRunner[S, C]` interface).
- **`Subscriber[C]`** — declarative subscription with the three
  knobs above.
- **`EventFilter`** — declarative event narrowing: `TypeURLs`,
  `StreamGlob` (shell-style), `Tenants`, `Custom func(env) bool`.
  Evaluated *before* any journal entry, so unmatched subscribers
  cost zero.
- **`WorkflowRuntime`** — two-method interface (`Run` + `Spawn`)
  the durability layer implements. Adapters: `cmdworkflow/inproc`
  (this package, for tests), Restate / DBOS (Phase 2).
- **`SubscriberDLQRow` / `SubscriberDLQWriter` / `SubscriberDLQAdmin`** —
  the operator surface for `OnExhausted = DLQ` subscribers. Mirrors
  `projection_dlq`. Both shipped storage adapters implement it.
- **`HandleCmdOption`** — `WithIdempotencyKey(string)` to make
  command IDs deterministic across retries.

## What it deliberately doesn't do

- **No cross-aggregate routing.** One `Workflow[S, C]` per
  aggregate. Apps that want one HTTP endpoint to handle every
  command of every aggregate write a thin dispatch layer
  themselves (~30 lines of `reflect.TypeOf` → workflow map). The
  framework type-erasure cost outweighs the convenience.
- **No saga primitives in v1.** `Wait`, `Sleep`, `Awakeable`,
  `Cancel`, `Terminate` aren't part of `WorkflowRuntime`. They land
  with the saga API in a future ADR. Today, long-running waits go
  through the workflow runtime's native primitives outside this
  package.
- **No cross-call Append dedup in the inproc adapter.** Real
  cross-call idempotency requires a durable runtime (Restate's
  invocation key, DBOS's workflow id). The `command_id` is made
  deterministic for ADR 0015-style subscriber-side dedup, but two
  Append calls with the same key would succeed twice on inproc.

## Adapters

| Adapter | Path | When |
| ------- | ---- | ---- |
| `inproc` | `cmdworkflow/inproc` | Unit tests, examples, local dev before wiring durability. Synchronous, no journal. |
| `restate` | Phase 2 | Production. Durable steps, native invocation-key dedup, language SDKs in Go / TS / Java. |
| `dbos` | Phase 2+ | When DBOS Go SDK matures. Postgres-native workflow journal. |

## See also

- ADR 0025 — full design (11 decisions, including idempotency
  layering, fresh-context compensation, framework-owned retry)
- ADR 0024 — `state_stream`: the recovery path for `Async + DLQ`
  state-mirror subscribers (refresh from current state instead of
  replaying DLQ rows)
- `examples/cmdworkflow/` — runnable example: Invoice aggregate
  with three subscribers covering the matrix
- `aggregate.NewProto` — proto-state aggregate constructor;
  eliminates `StateCodec` boilerplate
