# projection

Tier-3 projector runtime. Polls events from an `es.Store`, invokes a
`Handler` per event, advances a per-(name, tenant) checkpoint. No
business logic — the handler is the application's read-model writer.

## Load-bearing primitives

- [`Runtime`](runtime.go) — the projector struct; `Run` (loop until
  ctx cancelled) and `RunOnce` (single batch, for tests + cron).
  Carries shard config, lock key, batch size, and the DLQ-on-failure
  switch.
- [`Handler`](handler.go) — `func(ctx, env) error`. At-least-once
  delivery; handlers must be idempotent.
- [`Checkpoint`](checkpoint.go) — per-(name, tenant) cursor store.
  `MemoryCheckpoint` for tests; adapters back the SQL-backed default
  via `projection_checkpoint`.
- [`Chain` / `IgnoreUnknown`](dispatcher.go) — compose multiple
  codegen-emitted dispatchers when one projection consumes events
  from more than one aggregate.
- [`WithDedup` / `DedupStore`](dedup.go) — optional per-event-id
  dedup wrapper backed by the framework-managed `processed_events`
  table; reduces duplicate side effects, not strict EOS (ADR 0020
  decision 3h).
- [`DispatcherConfig` / `ApplyOptions`](dispatcher.go) — the assembly
  surface codegen-emitted `NewProjectionDispatcher` calls into.
  Consumers use the `Option` constructors (`IgnoreUnknown()`), not
  this struct directly.

## Contract

At-least-once delivery; fail-stop with last-success checkpoint
advance by default (ADR 0020). Per-stream order preserved within a
shard (stream-sticky FNV-1a hash). `LockKey` plus
`es.ProjectionLocker` gives single-runner safety across replicas;
without a lock the runtime assumes the caller does the coordination.
With `DLQOnFailure=true` the failure semantic flips to DLQ-skip via
`projection_dlq`; requires the Store to implement
`es.ProjectionDLQWriter`. Operator surface (Reset / ResetTo /
Status / List, plus DLQ list/replay/abandon) lives on
`es.ProjectionAdmin` and `es.ProjectionDLQAdmin`.

## Where to start reading

1. [`runtime.go`](runtime.go) — `Runtime` struct fields,
   `RunOnce` is the meat.
2. [`handler.go`](handler.go) + [`checkpoint.go`](checkpoint.go) —
   the two interfaces the runtime talks to.
3. [`dispatcher.go`](dispatcher.go) — composition primitives for
   multi-aggregate projections.

## Relevant ADRs

- [0012 — Event Delivery and EventPublisher](../docs/adr/0012-event-delivery.md)
  — defines the two delivery shapes (poll-based here, push elsewhere).
- [0020 — Projections and Read Models](../docs/adr/0020-projections-and-read-models.md)
  — the bulk of the design: fail-stop, locking, sharding, DLQ.
- [0022 — Linked Projections (Tier 3.5)](../docs/adr/0022-linked-projections.md)
  — `linked` package builds on the `Handler` shape here.
