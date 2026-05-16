# state_stream

Coalesced state-mirror delivery. Subscribers are external systems
that want the latest state of every stream — search indexes,
dashboards, denormalized read stores, webhook targets. Mirrors
`outbox.Drain`'s shape so operators learn one deployment pattern.

## Load-bearing primitives

- [`Drain`](drain.go) — the drain struct; `Run` (loop) and `RunOnce`
  (single batch returning a `RunResult{Delivered, Failed}`).
- The wire shape is [`es.StateEnvelope`](../es/state_envelope.go) —
  tenant, canonical stream id, type URL, version, schema version,
  state bytes (typically protojson), and `UpdatedAt`.
- The publisher shape is [`es.StatePublisher`](../es/state_envelope.go)
  — `PublishState(ctx, env) error`. Single-method; adapters live
  under `adapters/publisher/`.
- The adapter-side contract is `es.StateStreamStore`
  (`ListStreamsBehind`, `AdvanceStateStreamPosition`) plus
  `es.StateStreamAdmin` for ops queries. Both shipped storage
  adapters implement both.

## Contract

Coalescing-on-retry: failed deliveries don't queue. The next drain
cycle delivers whatever the **current** state is, not the version
that previously failed. Receivers MUST be idempotent on
`(TenantID, StreamID, Version)` — recommended pattern is "upsert
if `incoming.Version > stored.Version`, else ignore." Sharding is
stream-sticky (FNV-1a hash of `tenant|stream_id`), so per-stream
version monotonicity holds across a multi-runner deployment.
`LockKey` plus `es.ProjectionLocker` gives single-runner safety on
the same advisory-lock primitive the outbox and projections use
(distinct keyspace prefix `state_stream.drain:`).

## Where to start reading

1. [`drain.go`](drain.go) — the whole runtime is one file.
2. [`../es/state_envelope.go`](../es/state_envelope.go) — wire
   shape and `StatePublisher` contract.
3. [Cookbook 13 — state_stream](../docs/cookbook/13-state-stream.md)
   — end-to-end deployment recipe.

## Relevant ADRs

- [0024 — state_stream](../docs/adr/0024-state-stream.md) — full
  design, including the coalescing rationale and the comparison
  against per-event delivery.
- [0023 — state_cache supersedes snapshots](../docs/adr/0023-state-cache-supersedes-snapshots.md)
  — `state_stream` reads from the same `state_cache` table.
