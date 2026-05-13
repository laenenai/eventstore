# Cookbook

Patterns for composing the framework's primitives into common application
shapes. These are application-level recipes — **none of them are baked
into the framework**. Each recipe uses only the framework's public API.

## When to reach for the cookbook

The framework provides primitives: aggregates, projectors, publishing,
outbox, snapshots, crypto-shredding, codegen. The cookbook covers
*everything you build with them* — sagas, process managers, workflows,
compensation, timeouts, retries, integrations.

If you find yourself thinking "the framework should provide X", check the
cookbook first. Often X is a five-line subscriber plus an aggregate.

## Recipes

| #  | Title                              | What it solves                                                                  |
| -- | ---------------------------------- | ------------------------------------------------------------------------------- |
| 01 | [Stateless process manager](./01-stateless-process-manager.md) | "When event X happens, dispatch command Y." No saga-internal state.             |
| 02 | [Stateful saga](./02-stateful-saga.md)                          | Multi-step coordination across events with internal state.                      |
| 03 | [Cross-aggregate workflow with compensation](./03-cross-aggregate-workflow.md) | Money transfer and similar all-or-nothing flows across aggregates.              |
| 04 | [Time-based triggers](./04-time-based-triggers.md)              | "Cancel order after 24h with no payment" — in a scale-to-zero deployment.        |
| 05 | [Layered authorization](./05-layered-authz.md)                  | Wrap `aggregate.Runtime` with a Policy check (Cedar / OPA / RBAC); deliberately not a framework feature. |
| 06 | [Running the outbox drain](./06-running-the-drain.md)           | Five deployment patterns, plus failure handling: backoff, retries, DLQ semantics (quarantine vs auto-resume), `OutboxAdmin` for dashboards and operator replay/abandon. |

## Conventions used in these recipes

- All examples assume the framework's public API as established in
  ADRs 0001–0015.
- Restate is shown as the publisher (ADR 0012); the patterns work
  identically with any `EventPublisher` adapter — the calling code is
  the same.
- Proto types are illustrative. Substitute your own domain shapes.
- Error handling is shown for clarity. In production, wrap with your
  observability layer (logging, metrics, tracing).
- The deterministic-command-id helper
  `es.DeriveCommandID(handlerName, sourceEventID, outputIndex)` (ADR
  0015) is used throughout. It is the recommended way to make subscriber
  emissions idempotent under at-least-once bus delivery.

## When to write a new recipe

Whenever your team solves a coordination pattern that other teams might
need. A good recipe:

- Names the problem clearly.
- Shows the smallest working example.
- States explicitly which framework primitives it uses and which it
  deliberately does not.
- Discusses the failure modes (what happens on retry, on partial
  failure, on bus delivery duplication).
- Is reviewable as a Markdown PR. No new framework code required.
