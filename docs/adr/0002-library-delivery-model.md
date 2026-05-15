# ADR 0002: Library Delivery Model

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

The framework can be shipped two ways:

1. **As a Go library** that consumer services embed and wire into their own
   `main.go`.
2. **As a platform/runtime binary** that hosts aggregates and projectors,
   with consumers writing only the domain code that the runtime executes.

Each pushes operational concerns in a different direction. Option (1) keeps
the framework team out of the deployment business but means every consumer
re-solves wiring (subscriptions, outbox drain, key custody at the host
level). Option (2) centralizes operational concerns (rebuild orchestration,
backpressure, multi-tenant isolation, key management) but the framework
team is now operating a runtime, with upgrade-compatibility burden across
all consumers.

Successful in-house ES frameworks observed in practice tend to start as
libraries and grow a thin runtime host once the primitives stabilize.

## Decision

Ship as a Go library. Consumers embed the framework in their own services
and own deployment, transport, observability, and `main.go` wiring.

If a platform/runtime emerges later, it will be a thin host layered over
the same library primitives — not a separate codebase.

## Consequences

### Positive

- Idiomatic Go shape. Consumers retain control over their own deployment,
  transport (HTTP / gRPC / queue handlers), logging library, metrics
  library, and observability stack.
- No deployment or upgrade burden on the framework team.
- Public API stability becomes the single contract — easy to version with
  strict semver from v0.1.
- Smaller initial surface area; can grow a thin host later when the
  primitives are proven.

### Negative

- Every consumer re-solves wiring: subscription setup, outbox drain,
  rebuild orchestration, key custody at the host level.
- Operational concerns are not centralized. Best-practice runbooks must be
  documented rather than enforced.

### Neutral

- Library hygiene becomes non-negotiable: no global state, no `init()`
  side effects, no hidden goroutines, typed errors, zero opinion on
  logging/metrics/tracing libraries, context propagation everywhere.

## Alternatives Considered

### Platform / runtime binary

Rejected for v1. Reasoning: the framework primitives need to be proven
before they can be wrapped in a runtime; building both at once doubles the
surface area and risks designing the library's API around the runtime's
needs rather than around library users.

### Hybrid (library plus official runtime)

Rejected. A "thin host" wrapping the library was considered as a future
add-on, but absent concrete demand it is not on the roadmap. If a
platform-shaped product opportunity appears with a real customer and
real shape, that's a new ADR — not a continuation of this one. The
library API is the deliberate commitment.
