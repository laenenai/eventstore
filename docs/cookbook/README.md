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
| 05 | [Layered authorization](./05-layered-authz.md)                  | Wrap `aggregate.Runtime` with a Policy check; framework-shipped Cedar adapter (`adapters/authz/cedar`) plus the pattern for OPA / RBAC. |
| 06 | [Running the outbox drain](./06-running-the-drain.md)           | Five deployment patterns, plus failure handling: backoff, retries, DLQ semantics (quarantine vs auto-resume), `OutboxAdmin` for dashboards and operator replay/abandon. |
| 07 | [Read models via materialized views](./07-read-models-via-materialized-views.md) | Filtered/joined/aggregated read shapes over Tier 1 `state_cache` via Postgres MVs + scheduled REFRESH; SQLite alternatives. |
| 08 | [Rebuilding projections](./08-rebuilding-projections.md)        | Truncate-and-replay, versioned parallel rebuild for zero-downtime, and the Tier-1 `state_cache` rebuild helper. |
| 09 | [Snapshots](./09-snapshots.md)                                  | Enabling snapshots via StateSchemaVersion + SnapshotEvery; tuning cadence; schema-bump operator workflow. |
| 10 | [Restate as the event publisher](./10-restate-publisher.md)     | Wiring `restate.Publisher`, the receiver-side Restate service shape, idempotency-key semantics, deployment notes. |
| 11 | [Crypto-shredding for PII](./11-crypto-shredding.md)            | PII proto annotations, `pii_manifest.json` review, `ForgetSubject`, `RewrapDEKs` for KEK rotation, redacted reads. |
| 12 | [Linked projections / derived streams](./12-linked-projections.md) | Tier 3.5: routing (`OrderShipped` → `FulfillmentTask`) and fan-in audit ledger patterns. Idempotent emit, lineage, replay. |
| 13 | [state_stream — coalesced state-mirror delivery](./13-state-stream.md) | Mirror current aggregate state to search indexes / external read-stores. Coalesced-on-retry, cold-start backfill, no DLQ, crypto-shred runbook, schema-bump cascade. |
| 14 | [cmdworkflow deployment](./14-cmdworkflow-deployment.md) | Production wiring of the workflow-orchestrated command bus on Restate. Three-step start, topologies (sidecar / cluster / managed), idempotency at the edge, observability, common pitfalls. |
| 15 | [HTTP edge with Connect-go](./15-http-edge-with-connect.md) | Expose `cmdworkflow.Workflow` over HTTP via the `adapters/httpedge/connect` runtime helper. DTO seam, error mapping, idempotency header, async-ack pattern. |

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
