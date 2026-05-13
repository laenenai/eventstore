# ADR 0015: Decider Output Scope and Saga Boundary

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

A design proposal considered extending the `Decider` signature to return
outbound commands alongside events and constraints, and introducing a
"saga" as a first-class framework primitive with its own subscription
type, runtime, and codegen helpers. This was an overshoot.

A saga is a domain pattern — long-running, stateful coordination across
aggregates — not a framework primitive. The framework's existing
primitives (aggregates, projectors, publisher, outbox) are sufficient to
express any coordination pattern an application needs.

Including outbound commands in `Decide` would mix two concepts in the
decider signature: events that become the aggregate's own state, and
commands that are side effects to other aggregates. The minimal,
honest split is to keep `Decide` focused on its own state evolution.

## Decision

### Decider signature stays minimal

```go
Decide func(state S, cmd C) (
    events      []E,
    constraints []ConstraintOp,
    err         error,
)
```

No outbound commands. Aggregates emit events for their own stream and
declare uniqueness constraints. That is the entire output domain.

### Saga is not a framework word

The framework does not ship a `saga` type, a `saga.Subscription`, a
saga-specific runtime, or saga-flavored codegen. Coordination patterns
that the industry calls "saga" or "process manager" are written by the
application using existing primitives:

- An **aggregate** with its own stream and state, command type
  envelope-driven (e.g., `OnEvent{env}`).
- A **subscriber** registered with the `EventPublisher` that translates
  bus events into commands to the saga aggregate or other aggregates.

This is the same primitive composition that builds projectors, with
different semantics on the output side.

### Cross-aggregate command dispatch is application code

When subscriber code needs to dispatch a command to another aggregate,
it calls that aggregate's `Handle(ctx, streamID, cmd)` directly.
At-least-once delivery from the bus is dedup'd at the target aggregate
via `command_id` (already required by ADR 0005).

For deterministic `command_id` derivation, the framework provides a
helper:

```go
es.DeriveCommandID(handlerName string, sourceEventID uuid.UUID, outputIndex int) uuid.UUID
```

Use is recommended, not enforced. Same `(handlerName, sourceEventID,
outputIndex)` triple produces the same `command_id`, so retries at the
bus boundary are safely dedup'd at the target.

### A cookbook documents the patterns

Recipes for common coordination patterns (stateless process manager,
stateful saga, cross-aggregate workflow with compensation, time-based
triggers) live in `docs/cookbook/`. They use only the framework's
public API. None of them are framework features.

## Consequences

### Positive

- **Framework API stays small.** No saga vocabulary, no saga runtime,
  no saga codegen. The aggregate and projector primitives carry their
  weight.
- **Coordination patterns live in domain code,** where they belong.
  Domain teams own and evolve their own workflows without negotiating
  with the framework.
- **The cookbook is updateable independently** of the framework
  release cycle. New patterns emerge, recipes get added; framework
  surface doesn't change.
- **Decider signature is purely about own-state evolution.** Easier to
  reason about, easier to test, easier to compose.

### Negative

- **Application teams write more code** for cross-aggregate workflows
  than they would with a framework-provided saga runtime. The cookbook
  mitigates this by providing well-tested patterns to copy.
- **Durability of cross-aggregate command dispatch is application
  concern.** The framework provides primitives (subscriber, outbox,
  publisher, command_id dedup); the application composes them. In
  practice, when the publisher is Restate, this composition is trivial
  (Restate handles durability of the handler invocation chain).
- **The `command_id` discipline is now a documented convention,** not
  a framework enforcement. Teams that ignore it accept duplicate
  command processing on retries.

## Alternatives Considered

### Include outbound commands in `Decide`

Rejected. Mixes two concepts in the decider signature. Adds a
`command_outbox` (or kind discriminator on the existing outbox). Adds
framework surface for a pattern the application can express cleanly
with existing primitives. Can be added later as an additive extension
if a deployment proves we are forcing every app to reinvent the same
pattern.

### Ship a `saga` framework primitive

Rejected. Saga is a domain pattern, not infrastructure. Framework
primitives for it would either duplicate aggregate/projector machinery
or constrain the patterns that domain teams can express.

### Provide only the `DeriveCommandID` helper and skip the cookbook

Rejected. The helper is useful but the patterns need to be documented
somewhere accessible. The cookbook is the right home — out of the
framework code, in the docs tree, freely revisable.
