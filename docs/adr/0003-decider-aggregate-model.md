# ADR 0003: Decider Aggregate Model

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

Two real options exist for how aggregate behavior is modeled in an
event-sourced system:

1. **Classic DDD aggregate** — a struct (or class) holds state and exposes
   methods. Commands invoke methods; `apply(event)` mutates `this` in place.
   The aggregate instance carries identity and state together.
2. **Decider** (Chassaing, 2021) — three pure functions: `Initial()`,
   `Decide(state, command) → ([]event, error)`, and
   `Evolve(state, event) → state`. State is a value threaded through the
   functions; identity is external (the runtime owns the stream key).
   Optionally `IsTerminal(state) → bool` signals a closed stream.

The decider model has become the dominant serious-ES pattern across the F#,
Scala, and Haskell communities, with growing adoption in mainstream
languages.

The framework treats uniqueness as a first-class store capability, which
means the aggregate's decision function must be able to declare constraint
operations alongside the events it emits. This signature integrates more
cleanly with the decider's pure-function shape than with classic DDD method
calls.

## Decision

Adopt the decider model. The framework's `Decider` type is:

```go
type Decider[S, C, E any] struct {
    Initial    func() S
    Decide     func(state S, cmd C) (events []E, constraints []ConstraintOp, err error)
    Evolve     func(state S, event E) S
    IsTerminal func(state S) bool // optional
}
```

`Decide` returns events and constraint operations together so the runtime
can commit them in a single transaction. `Evolve` is the only function that
applies events to state; it is pure, with no I/O, time, or randomness.

## Consequences

### Positive

- **Determinism enforced by shape.** There is no `this` to mutate, no
  closure over wall-clock time. Replay safety is structural, not a code-review
  convention.
- **Tests collapse to a single expression.**
  `Decide(fold(Evolve, Initial(), history), cmd) == expected_events`.
  No DB, no mocks, no setup.
- **Algebraic composition.** Two deciders compose into one over the
  product or sum of their states/commands/events. Multi-aggregate
  transactions and uniqueness integration both fall out of this property.
- **Schema evolution is cleaner.** Adding a new event only extends
  `Evolve`. `Decide` is untouched. The read side and write side of the
  aggregate are cleanly separated.
- **Aligns with code generation.** Deciders are flat function signatures
  the codegen plugin can scaffold cleanly per `.proto`.

### Negative

- **Less familiar to Go developers expecting OO aggregates.** The first
  week of onboarding requires examples and pair programming.
- **Go lacks ADTs.** Command and event dispatch is a type switch. Codegen
  mitigates by generating the dispatch skeleton; an exhaustiveness analyzer
  ships alongside it (see ADR 0004).
- **State is a public type.** Its shape becomes a contract that affects
  snapshots and requires discipline around versioning (see ADR 0011).

## Alternatives Considered

### Classic DDD aggregate (struct + methods + in-place mutation)

Familiar to Go developers, but:
- Mutation hides inside `apply()`, which is exactly where replay
  determinism gets quietly broken.
- Couples state to identity, which is awkward when identity is really
  the stream key owned by the runtime.
- Harder to unit-test in isolation — instantiate, prime with events,
  invoke command, inspect side effects.
- Doesn't compose. Aggregate methods are not algebraic.

### Hybrid (decider under the hood, method-receiver facade for ergonomics)

Tempting, but rejected. The facade doubles the API surface, doubles the
documentation, and the determinism property leaks back through the facade
the moment someone uses `time.Now()` "just this once" in a method.
