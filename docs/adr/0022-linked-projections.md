# ADR 0022: Linked Projections (Tier 3.5)

- **Status:** Proposed
- **Date:** 2026-05-14
- **Pairs with:** ADR 0012 (Event Delivery), ADR 0020 (Projections and
  Read Models)

## Context

ADR 0020 established a three-tier projection model:

| Tier | Output | Mechanism |
| ---- | ------ | --------- |
| 1 | Current state per stream | `state_cache` written in-tx with events |
| 2 | Read-shape transformations (joins, filters, aggregations) | Postgres materialized views over `state_cache` |
| 3 | Anything event-driven — search indexes, audit ledgers, time series | `projection.Runtime` with codegen'd dispatcher |

That model covers most read-side needs. One pattern it doesn't address
directly: **derived event streams.** EventStoreDB's "Advanced Projections
Engine" supports this natively via `linkTo` and `emit`:

> A projection observes events; for each one matching a filter or
> condition, it produces a new event into a *derived stream* that
> downstream consumers subscribe to as if it were a primary stream.

Use cases:

- **Routing.** "Every `OrderShipped` event becomes a `FulfillmentTask`
  in a `tenant:fulfillment` stream that the picking-warehouse service
  subscribes to."
- **Categorization.** "Tag events by type into per-category streams"
  — `$by_type` style. EventStoreDB ships this as a system projection.
- **Re-aggregation.** "Fan-in events from N source streams into one
  unified `audit_log` stream for a compliance subscriber."

In all three cases the output is an **event stream**, not a state table.
That's what makes them different from Tier 3 (which writes to
external storage) and Tier 1 (which writes one row per stream).

This ADR proposes a Tier 3.5 — **linked projections** — to address
this gap.

## Decision

### Add a "linked projection" type to the framework

A `LinkedProjection` is a Tier 3 projection whose handler is
constrained: it consumes events and **emits events into derived
streams** rather than writing to arbitrary external storage.

```go
type LinkedProjection[E any] struct {
    Name      string
    Tenant    string
    Store     es.Store           // events come from here
    Producer  AggregateRuntime   // emit calls into Producer.Handle
    Match     func(env es.Envelope) bool
    Route     func(env es.Envelope) (es.StreamID, E, error)
}
```

Internally it's a `projection.Runtime` with a generated handler that:

1. Decodes the source event.
2. Calls `Match(env)`; skips when false.
3. Calls `Route(env)` to compute the destination stream + the derived
   event.
4. Calls `Producer.Handle(destStreamID, derivedCommand)` to append.

The destination stream's aggregate is a thin pass-through: its
`Decide` produces the supplied event verbatim, its `Evolve` is a
no-op or maintains a count, its `Initial` is empty. The result is a
stream of events derivable from upstream events, queryable through
the normal events API.

### Why a separate tier vs. just a Tier 3 projection

Tier 3 is "you can do anything." Linked projections are constrained
to a specific shape with a specific consequence: **the output is
itself a queryable event stream**, with `global_position`, replay
guarantees, the works. That uniformity is the point — a downstream
consumer should not need to know whether a stream was written by a
human-issued command or by a linked projection.

A separate type clarifies the constraint, makes codegen possible, and
allows operational tooling (lineage, drift detection) to recognise
the pattern.

### Derived stream identity

Linked projection output streams carry a `derived_from` envelope
metadata field pointing at the source event's `event_id`. Useful for:

- **Lineage queries.** "Which upstream events produced this derived
  one?"
- **Idempotent re-emit.** If a linked projection re-runs (cursor
  reset, schema rebuild), it deterministically produces the same
  events — same `derived_from`, same payload, same logical
  identity.
- **Drift detection.** Compare derived stream content against the
  source — divergence indicates a bug.

### Cycle prevention

A linked projection that emits into a stream **its own source set
includes** would loop. The framework rejects this at registration:
the destination stream's `aggregate` must not match any source the
projection consumes. Static check at startup; logged + refused.

### Replay behavior

When a linked projection's cursor is reset, it re-reads source events
and re-emits derived events. The events table accumulates duplicates
unless the destination aggregate dedups by `derived_from` — which
the framework provides as a `LinkedProjection.IdempotentEmit` flag
(default true): the aggregate's Decide checks for an existing event
with the same `derived_from` before producing a new one.

### Configuration via proto annotation

Extending the v2 proto-driven projection annotation (ADR 0020):

```proto
message OrderFulfillmentRouter {
  option (es.v1.linked_projection) = {
    name: "order-fulfillment-router"
    source_events: ["myapp.order.v1.Shipped"]
    destination_aggregate: "myapp.fulfillment.v1.Task"
  };
}
```

Codegen emits the `LinkedProjection` runtime plus the destination
aggregate's pass-through Decider. User implements only:

- The `Route` function (typed: takes the source event, returns a
  destination stream id + derived event).
- Optional `Match` function (defaults to always-true).

## Consequences

**Positive:**

- **Closes the EventStoreDB parity gap.** "I want to fan-out events
  into named streams" becomes a first-class feature.
- **Codegen-friendly.** Same proto-annotation pattern as v2
  projection codegen; users write minimal code.
- **Lineage and replay safety.** `derived_from` + idempotent emit
  give clean operator semantics.
- **Tier 3 is now genuinely "free-form".** Anything that writes to
  external storage is Tier 3; anything that produces derived events
  is Tier 3.5. The mental model is cleaner.

**Negative:**

- **More events in the events table.** Derived streams add volume.
  Acceptable for routing/categorization; not for high-fan-out
  scenarios (millions of derived events per source). For those,
  prefer Tier 3 (write directly to a search index / queue).
- **Operational complexity.** A new aggregate type that's mostly a
  pass-through can confuse readers. Mitigated by codegen — derived
  aggregates are auto-generated, not hand-written.
- **Cycle detection requires startup wiring.** The framework needs
  to know the full set of linked projections at startup to validate
  no cycles exist. New constraint on initialization order.

**Codegen sunset criterion.**

The framework already has the runtime building blocks
(`projection.Runtime`, `aggregate.Runtime`, `Decider`); cookbook
recipe 12 documents the pattern in user code. What's deferred is
the **codegen** — a proto annotation that generates the Route /
Match scaffolding so users don't write `func Route(env es.Envelope)
(event, dest)` by hand.

Codegen ships when one of:

- **≥2 in-house linked projections** (any pattern — routing,
  fan-in audit, etc.) exist in the same shop, with the hand-rolled
  Route / Match boilerplate visibly repetitive across them. The
  repetition pain is per-projection, not per-aggregate.
- **EventStoreDB migration** path requires `linkTo` / `emit`
  wire-protocol compatibility for the bridge.
- **External adopter request** with a concrete use case.

Estimated effort when triggered: ~4 engineer-days. Proto extension,
codegen emitter, recipe 12 update.

## Alternatives considered

### Allow Tier 3 projections to call Append directly

Possible today — a Tier 3 handler can call `store.Append` on a
derived stream. Why introduce a new abstraction?

Because the bare Tier 3 path has no:
- Idempotent re-emit
- Cycle detection
- Lineage tracking via `derived_from`
- Codegen for the destination aggregate's pass-through Decider

Users hand-rolling this pattern repeatedly will reinvent (some of)
these. Better to capture the pattern once and codegen it.

### EventStoreDB-style in-DB JavaScript

Rejected. JavaScript-in-database is a debugging and observability
nightmare. Go code, codegen, and the same projection runtime keep
the operational model uniform.

### Triggers on the events table

Postgres triggers on `INSERT INTO events ...` could produce derived
events inside the same transaction. Performance is appealing — no
async lag. Rejected for:

- **Trigger code becomes load-bearing.** Hard to test, hard to
  evolve, debugger-unfriendly.
- **PL/pgSQL is the language.** Cross-aggregate logic in PL/pgSQL is
  a step backwards from typed Go.
- **Failure semantics complicate Append.** A trigger failure aborts
  the source append, coupling unrelated streams.

The linked projection's async-but-replayable model is the right
trade.

### Emit destination events into a dedicated "events_derived" table

Rejected. Separates "events written by humans" from "events derived
by the system." Downstream consumers would need to read both. The
uniformity of "one events table" is more valuable than the
classification.

## Status

**Proposed.** Not implemented in v1. ADR exists so the design
intention is preserved.

When implementation begins:

1. Define `LinkedProjection[E]` in a new `linked/` package.
2. Add the `(es.v1.linked_projection)` option to `proto/es/v1/options.proto`.
3. Extend protoc-gen-es-go to emit the runtime + the pass-through
   destination aggregate.
4. Cycle-detection helper, startup validation hook.
5. Cookbook recipe with concrete fan-out / routing example.

## Reference

- ADR 0012 — Event Delivery and EventPublisher (publishers fan
  primary events out; linked projections produce derived primary
  events that publishers also see)
- ADR 0020 — Projections and Read Models (tiers 1–3 — this ADR adds
  3.5)
- EventStoreDB's [Projections documentation](https://developers.eventstore.com/server/v22.10/projections.html) — prior art for `linkTo` / `emit`
