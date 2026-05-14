# ADR 0025: Workflow-Orchestrated Command Bus

- **Status:** Accepted (Phase 1 — framework primitives + inproc adapter)
- **Date:** 2026-05-14
- **Pairs with:** ADR 0001 (Profile A — deferred), ADR 0012 (event
  delivery), ADR 0020 (projections), ADR 0024 (state_stream).

## Context

For Profile B (scale-to-zero DBs), polling-based projections defeat the
deployment model — the DB cannot be kept awake by projection runners.
ADR 0012 named the architectural shift: projection and saga delivery
moves out of the DB into an external durable runtime.

The natural shape that emerges:

- A command becomes a **durable workflow** (Restate / DBOS / equivalent).
- The workflow does **one synchronous step** to Append events + state to
  the eventstore.
- The workflow then **fans out to registered subscribers**, each step
  independently journaled and retried.
- The eventstore stays the source of truth; subscribers derive from it.

Subscribers want richer semantics than a binary sync/async. They span:

- Strict-consistency local read-models (own DB, Sync, bounded retry,
  DLQ on exhaustion).
- Saga steps where command success depends on external completion
  (Sync, retry, **compensation** on exhaustion).
- State mirrors to external systems (search indexes, caches — Async,
  forever-retry, DLQ-on-permanent-fail with recovery via state_stream).
- Audit / analytics fan-out (Async, bounded retry, DLQ).
- Best-effort webhooks (Async, small retry budget, Drop on exhaustion).

The framework must offer one mechanism that encodes all five without
proliferating per-flavor code paths.

## Decision

### 1. Workflow is framework-provided; workflow runtime is pluggable

`cmdworkflow.Workflow[S, C, E]` is a framework-shipped type wrapping
`aggregate.Runtime` and a subscriber registry. The workflow runtime
(Restate, DBOS, inproc, future others) is behind a narrow interface.
This mirrors the `EventPublisher` pluggability already established
in ADR 0012.

### 2. Three knobs per subscriber

Subscribers declare three orthogonal properties at registration:

| Knob | Values | Meaning |
| ---- | ------ | ------- |
| `Mode` | `Sync` / `Async` | Whether `HandleCmd` blocks on this subscriber. |
| `MaxRetries` | `0..N` / `-1` | Failure budget. `-1` = forever. |
| `OnExhausted` | `DLQ` / `Compensate` / `Drop` | What happens at exhaustion. |

The combinations form a small matrix that covers the five common
subscriber kinds (read-model UPSERT, saga step, state mirror, audit,
nice-to-have). See architecture overview section 6 for the matrix.

### 3. Narrow `WorkflowRuntime` interface

```go
type WorkflowRuntime interface {
    Run(ctx context.Context, name string,
        fn func(context.Context) ([]byte, error)) ([]byte, error)

    RunAsync(ctx context.Context, name string,
        fn func(context.Context) ([]byte, error)) Future

    Spawn(ctx context.Context, name string,
        fn func(context.Context) error) error
}

type Future interface {
    Wait() ([]byte, error)
}
```

Three methods for v1. Run is durable journaled step; RunAsync is the
same with a Future the caller awaits (enables parallel Sync fan-out);
Spawn is fire-and-forget child workflow. No Cancel, no Terminate, no
Sleep / Wait / Awakeable. Saga primitives come in a future ADR.

### 4. Sync subscribers run in parallel; retries hidden inside one step

`Workflow.HandleCmd` fans out matched Sync subscribers concurrently
via `RunAsync` and awaits all futures before proceeding to the next
event. Within one subscriber, the retry loop runs **inside** that
subscriber's RunAsync fn — one journal entry per (subscriber, event)
regardless of how many attempts the retry budget consumed.

Trade-off vs. per-attempt journal entries (the rejected alternative):

| Aspect | Per-attempt Run | RunAsync per (sub, event) |
| ------ | --------------- | ------------------------- |
| Parallelism | Sync subs serial | Sync subs parallel |
| Journal entries | 1..MaxRetries+1 per sub | 1 per sub |
| Per-attempt observability | yes | no (final outcome only) |
| Replay restart granularity | last journaled attempt | from attempt 1 |
| Journal storage cost | high | low |

Replay safety: subscribers are required to be idempotent on
`env.EventID` (at-least-once contract). Restarting from attempt 1
on replay is safe by construction — the dedup mechanism in the
subscriber absorbs duplicate work.

Events from one command still process serially — ordering may
matter for downstream (search index sees `Created` before `Paid`).
Only subscribers within one event run concurrently.

Async subscribers continue to use `Spawn` — fire-and-forget durable
child workflows. They are not waited on by HandleCmd.

Rationale: portability across runtimes (Restate has RunAsync, DBOS
has StartChildStep, inproc has goroutines). Slightly more journal entries
per failed step, in exchange for "Mode + MaxRetries + OnExhausted
behaves identically everywhere". The cost is bounded by `MaxRetries`,
which is small for most subscribers.

### 5. Per-subscriber Compensate, not decider-emitted plans

When `OnExhausted = Compensate` and the subscriber is exhausted, the
workflow invokes `Subscriber.Compensate(env, state) C`, which returns
a compensating command. The command is appended to the same stream
through `HandleCmd` (a nested invocation), producing a compensating
event the aggregate's Decide acknowledges.

Rationale:

- **Local reasoning.** Each subscriber declares its own rollback. No
  central plan to maintain.
- **The eventstore never undoes itself.** Compensation is a *new*
  event, not an erasure. The audit trail shows action and reaction
  as separate steps — what actually happened.
- **Saga semantics drop out for free.** A subscriber whose Handle
  reserves inventory and whose Compensate releases it is a complete
  saga step in one type.

Decider-emitted compensation plans are deferred. They can be added
later as `Decider.Compensate(...)` returning a Plan; the Workflow
would consult that first and fall back to per-subscriber.

### 6. DLQ is per-subscriber, mirrors `projection_dlq`

A `subscriber_dlq` table on both adapters with the same shape as
`projection_dlq`:

```sql
CREATE TABLE subscriber_dlq (
    subscriber_name TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    event_id        TEXT        NOT NULL,
    stream_id       TEXT        NOT NULL,
    type_url        TEXT        NOT NULL,
    last_error      TEXT        NOT NULL,
    attempts        INT         NOT NULL,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (subscriber_name, tenant_id, event_id)
);
```

Operations: `ListSubscriberDLQ`, `ReplaySubscriberDLQ`,
`ClearSubscriberDLQ`. Same admin shape as the existing
`ProjectionDLQAdmin`.

### 7. AttemptTimeout: framework field, optional

`Subscriber.AttemptTimeout time.Duration` caps a single Handle
invocation. The Workflow wraps each `Run` call in a child context
with that deadline. Zero = no cap (inherits caller ctx).

Caller deadline (level 1) and async-child-workflow lifetime (level 3)
are not framework primitives — caller deadline is stdlib `ctx`, and
async child lifetime is a saga-primitive concern.

### 8. Compensation runs on a fresh context, not the caller's

If a `Sync + Compensate` step times out under the caller's deadline,
compensation MUST run to completion — otherwise the aggregate is left
in a half-rolled-back state. The Workflow uses
`context.WithoutCancel(parentCtx)` (Go 1.21+) to detach the
compensation invocation from the caller's deadline.

This is a load-bearing invariant. Tests verify it.

### 9. Filter evaluation: before `Run`, not inside

`Subscriber.Filter.Matches(env)` is pure Go evaluated by the framework
before any `WorkflowRuntime.Run` call. Unmatched subscribers cost zero
journal entries. Filter struct: `TypeURLs []string`, `StreamGlob
string`, `Tenants []string`, `Custom func(env) bool`.

### 10. Idempotency is layered, not framework-monolithic

Three independent layers of idempotency, each with its own enforcement
point:

| Layer | Duplicates what | Key | Enforced by |
| ----- | --------------- | --- | ----------- |
| Workflow invocation | Caller retries (HTTP, gRPC) | `WithIdempotencyKey(string)` | **Workflow runtime** (Restate invocation key, DBOS workflow id). Inproc has no native dedup — Phase 1 limitation. |
| `command_id` determinism | Subscriber observes the same command twice | Derived from `WithIdempotencyKey` via UUIDv5 in `idempKeyNamespace` | Framework — `aggregate.WithCommandID` propagates the derived id to every event. |
| Subscriber delivery | At-least-once retry, workflow replay | `env.EventID` | Subscriber code (cookbook recipe pattern: dedup table keyed by EventID). |

The framework does NOT implement cross-call Append-layer dedup. The
`command_id` column is indexed but not UNIQUE; two Append calls with
the same `command_id` succeed twice. Real workflow-invocation dedup
is the runtime adapter's responsibility (Restate / DBOS handle this
natively). Future framework-level dedup (Append-time lookup +
short-circuit) is a separate ADR if and when it's warranted —
typically when teams need it for the inproc adapter, which today is
test-only.

### 11. `HandleCmd` returns aggregate state, not a view model

`HandleCmd[S, C, E]` returns the aggregate's own `S` — the
deterministic post-Decide state that `aggregate.Runtime` already
computes and the `state_cache` row already stores. It MUST NOT return
view-shaped data: no joins across aggregates, no denormalized
projections, no fields derived from other read models.

Returning `S` is not a CQRS violation — it's the read-your-writes
property the framework was built around (ADR 0020). Returning a view
model from a command IS the anti-pattern, because it couples the
command shape to read-model evolution and breaks every time a
projection changes. View-shaped reads go through projections,
queried separately after the command returns.

Concretely: an `InvoicePay` command returns `*invoicev1.Invoice`
(`status: PAID, paidAt: …`). It does NOT return a joined "Invoice +
Customer + RecentPayments" shape. The UI calls a separate query for
that view, served by a read-model populated by a Sync subscriber.

## Consequences

### Positive

- **One generic command bus** for all aggregates. Adding an aggregate
  registers it with the bus; no new workflow code per aggregate.
- **Declarative subscriber registration.** Adding a Typesense mirror,
  an audit sink, a webhook = one struct literal. No workflow rewrite.
- **Portable across runtimes.** Restate / DBOS / inproc all implement
  the same two-method interface.
- **Saga semantics drop out.** Sync + Compensate is the saga step
  pattern with no special API.
- **state_stream stays load-bearing.** Async + DLQ subscribers
  recover via state_stream (current-state catch-up), not by replaying
  every DLQ row. The matrix encodes this naturally.
- **Profile A still works.** Teams that don't want the workflow layer
  use the existing polled `projection.Runtime`. Workflow is opt-in.

### Negative

- **The matrix has a learning surface.** Teams will mis-classify
  subscribers (treating an audit pipeline as `Drop`, a webhook as
  `Sync+Compensate`) before they internalize the axes. Cookbook
  recipe 14 will lead with the matrix.
- **Journal storage costs scale with subscriber count.** N matched
  subscribers × M retries per command. Bounded by `MaxRetries`;
  unbounded only for `MaxRetries = -1` subscribers, where retries
  happen on workflow replay rather than in foreground.
- **Two recovery paths** for exhausted subscribers (event-shaped DLQ
  replay vs. state-shaped `state_stream.Drain`). Cookbook documents
  the choice.
- **No cross-subscriber atomicity.** Subscribers complete
  independently. A read model and a search index may be momentarily
  inconsistent. By design — the eventstore is the consistency anchor.

### Neutral

- **No Cancel / Terminate in v1.** Domain cancellation is a command;
  hard kill is admin-side. Saga-style cancellation lands with the
  saga primitive ADR.

## Alternatives Considered

### One workflow per aggregate (no generic bus)

Rejected. Linear code growth with aggregate count. Loses declarative
subscriber registration.

### Runtime-owned retry policies

Rejected. Restate, DBOS, and any future adapter have different native
retry policies. Pushing retry into the runtime makes
`MaxRetries`/`OnExhausted` behave subtly differently across adapters.
Framework-owned retry costs slightly more journal entries; pays for
itself in portability and predictability.

### Decider-emitted compensation plans

Deferred. Per-subscriber compensation is sufficient for the common
saga pattern. Decider-emitted plans can be added later without
breaking existing subscribers — the Workflow consults the plan
first, falls back to per-subscriber.

### One shared DLQ table across all subscribers

Rejected. Operationally muddier ("which DLQ row belongs to which
subscriber?"). Per-subscriber matches the existing `projection_dlq`
pattern and lets operators clear/replay one subscriber's queue
without affecting others.

### Connect-go as a mandatory transport layer

Deferred. Connect-go fits naturally as the codegen target for the
HTTP entrypoint and inter-domain RPC, but baking it into the
framework now adds dependencies. The bus is transport-agnostic in
v1; Connect-go codegen lands as a separate `protoc-gen-es-go`
extension.

## Implementation Plan

**Phase 1 (this ADR):**

1. `cmdworkflow.` package — types + `Workflow[S, C, E]` + retry loop.
2. `cmdworkflow/inproc` — non-durable WorkflowRuntime for tests.
3. `subscriber_dlq` migrations + sqlc queries on both adapters.
4. `examples/cmdworkflow/` — Invoice with three subscribers.
5. Tests covering filter, retry, compensation, DLQ, context detach.

**Phase 2 (separate ADR):**

6. `cmdworkflow.restate` — Restate Go SDK adapter.
7. Cookbook recipe 14 — workflow-orchestrated command bus.
8. `examples/cmdworkflow-restate/` — same example, deployed against
   a Restate testcontainer.

**Phase 3 (separate ADR if needed):**

9. Codegen extension — typed `RegisterFor[*EventType](handler)`.
10. Connect-go service stubs from `protoc-gen-es-go`.

**Phase 4 (separate ADR if needed):**

11. Saga primitives (`Sleep`, `Wait`, `Awakeable`, `Cancel`,
    `Terminate`) — extends `WorkflowRuntime` interface.
12. Decider-emitted compensation plans — optional override of
    per-subscriber Compensate.
