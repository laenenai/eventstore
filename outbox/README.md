# outbox

Outbox drain runtime. The durability seam between the writer
transaction (which appends events + an outbox row atomically) and
the `publisher.Publisher` (which fans events out to subscribers).
The outbox table itself is owned by the storage adapters; this
package is the operator-side drain that reads it.

## Load-bearing primitives

- [`Drain`](drain.go) — the drain struct; `Run` (loop) and `RunOnce`
  (single batch). Carries publisher, tenant scope, shard config,
  backoff knobs, and the DLQ-quarantine switch.
- The adapter-side contract lives in [`es.OutboxStore`](../es/outbox.go)
  (`PendingOutbox`, `QuarantinedStreams`, `MarkOutboxPublished`,
  `MarkOutboxFailed`, `CleanupPublishedOutbox`) and
  [`es.OutboxAdmin`](../es/outbox.go) (counts, list-DLQ,
  replay-DLQ, abandon).

## Contract

At-least-once delivery to the publisher. Per-stream order preserved:
if version N in stream X fails to publish, no version > N in stream X
is delivered until N succeeds or N enters DLQ. Cross-stream
interleaving is allowed. With `AutoResumeAfterDLQ=false` (default),
a DLQ'd row quarantines the entire stream until operator action
(replay or abandon). Sharding is stream-sticky (FNV-1a hash of
`tenant|stream_id` % `TotalShards`) so per-stream order survives a
multi-runner deployment. `LockKey` plus `es.DrainLocker` gives
single-runner safety on Postgres advisory locks.

## Where to start reading

1. [`drain.go`](drain.go) — `Drain` struct fields + the
   `Run` / `RunOnce` / `runBatch` triple.
2. [`../es/outbox.go`](../es/outbox.go) — the adapter contract this
   drain consumes.
3. [Cookbook 06 — Running the Drain](../docs/cookbook/06-running-the-drain.md)
   — the deployment patterns this drain is shaped for.

## Relevant ADRs

- [0012 — Event Delivery and EventPublisher](../docs/adr/0012-event-delivery.md)
  — defines the publisher seam.
- [0014 — Outbox Shape](../docs/adr/0014-outbox-shape.md) — the
  outbox table layout and semantics.
- [0020 — Projections and Read Models](../docs/adr/0020-projections-and-read-models.md)
  — same advisory-lock primitive (`es.DrainLocker` /
  `es.ProjectionLocker`).
