# cmdworkflow

Workflow-orchestrated command handler + subscriber registry. One
generic entry point per aggregate — `Workflow.HandleCmd` — appends
events through `aggregate.Runtime`, then fans out to registered
subscribers governed by per-subscriber `Mode`, `MaxRetries`, and
`OnExhausted` policy. Each subscriber receives the post-Decide state
and the typed event batch in **one Handle call per command** (ADR
0029).

See **ADR 0025** for the full bus design, **ADR 0029** for the
per-batch delivery model, and **architecture overview § 6** for the
topology diagram.

## What it replaces

Without this package you'd write one Restate / DBOS handler per
aggregate, each doing:

1. `ctx.Run("append", ...)` to call `aggregate.Runtime.Handle`
2. `ctx.Run("subscriber-1", ...)` for each subscriber, with its own
   retry policy, DLQ logic, and compensation flow
3. State re-derivation in every projection that needs current state

Per aggregate × per subscriber × per command-type = a lot of
hand-written orchestration. The package collapses it into one
generic shape and a declarative subscriber registry that hands
projections the state they want directly.

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
// The Codec[E] decodes envelopes into typed events for subscribers.
wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
    rt, store, inproc.New(), invoicev1.EventCodec{},
).
    WithDLQ(store).
    With(
        readModel.Subscriber(),     // Sync, DLQ — local read model
        searchIndex.Subscriber(),   // Async, DLQ — external mirror
        creditCheck.Subscriber(),   // Sync, Compensate — saga step
    )

// Single entry point for every command of this aggregate.
state, err := wf.HandleCmd(ctx, streamID, &invoicev1.Create{...})
```

## Subscriber shape

Each subscriber receives a whole command-batch in one Handle call:

```go
type Subscriber[S, C, E any] struct {
    Name        string
    Filter      EventFilter
    Mode        DeliveryMode       // Sync / Async
    MaxRetries  int
    OnExhausted ExhaustedPolicy    // DLQ / Compensate / Drop

    // Handle is called once per command, with:
    //   - envs:   envelopes from this command that matched Filter
    //   - state:  the post-Decide state (same value state_cache holds)
    //   - events: typed events index-aligned with envs
    Handle func(ctx context.Context, envs []es.Envelope, state S, events []E) error

    Compensate func(ctx context.Context, envs []es.Envelope, state S, events []E) (C, error)
}
```

A subscriber whose Filter rejects every envelope in a batch is **not
called** — no journal entry, no DLQ row, no spawned workflow.

The post-Decide state and the typed events come for free; the
projection picks whichever it needs. A "mirror current state into a
read-model" subscriber rarely needs `events` at all — it stores
`state` keyed by stream id and is done.

## The three knobs

Each `Subscriber` declares three orthogonal properties at
registration. The combinations cover every common subscriber kind
without proliferating per-flavor code paths.

| Knob | Values | Effect |
| ---- | ------ | ------ |
| `Mode` | `Sync` / `Async` | Whether `HandleCmd` blocks on the subscriber's completion. |
| `MaxRetries` | `0..N` / `-1` | Failure budget. Per-BATCH — one Handle call = one attempt. `-1` = retry on every workflow replay forever. |
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

- **`Workflow[S, C, E]`** — the generic command handler. One instance
  per aggregate. The `E` parameter is the event sum type — the bus
  decodes envelopes through the supplied `Codec[E]` once per dispatch
  so subscribers receive `[]E` rather than raw bytes.
- **`Subscriber[S, C, E]`** — declarative subscription with the
  three knobs above and a `Handle(ctx, envs, state, events)`
  signature.
- **`EventFilter`** — declarative event narrowing: `TypeURLs`,
  `StreamGlob` (shell-style), `Tenants`, `Custom func(env) bool`.
  Evaluated *before* any journal entry, so unmatched subscribers
  cost zero.
- **`WorkflowRuntime`** — three-method interface (`Run`, `RunAsync`,
  `Spawn`) the durability layer implements. Adapters:
  `cmdworkflow/inproc` (this repo, for tests), Restate / DBOS
  (Phase 2).
- **`SubscriberDLQRow` / `SubscriberDLQWriter` / `SubscriberDLQAdmin`** —
  the operator surface for `OnExhausted = DLQ` subscribers. One row
  per (subscriber, failed command-batch); `EventIDs` and `TypeURLs`
  carry the whole batch. Both shipped storage adapters implement
  the interface.
- **`HandleCmdOption`** — `WithIdempotencyKey(string)` to make
  command IDs deterministic across retries.

## What it deliberately doesn't do

- **No cross-aggregate routing.** One `Workflow[S, C, E]` per
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
| `restate` | `adapters/cmdworkflow/restate` | Production. Durable steps, native invocation-key dedup, language SDKs in Go / TS / Java. |
| `dbos` | `adapters/cmdworkflow/dbos` | Postgres-native workflow journal. Co-locates with the eventstore tables. |

## See also

- ADR 0025 — full design (11 decisions, including idempotency
  layering, fresh-context compensation, framework-owned retry)
- ADR 0029 — per-command subscriber batch delivery; the current
  Subscriber contract
- ADR 0024 — `state_stream`: the recovery path for `Async + DLQ`
  state-mirror subscribers (refresh from current state instead of
  replaying DLQ rows)
- `examples/cmdworkflow/` — runnable example: Invoice aggregate
  with three subscribers covering the matrix
- `aggregate.NewProto` — proto-state aggregate constructor;
  eliminates `StateCodec` boilerplate
