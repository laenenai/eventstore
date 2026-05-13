# ADR 0001: Profile A — Always-On Database Deployment

## Status

**Deferred.** Recorded for future implementation. v1 of the framework targets
Profile B (serverless scale-to-zero) only. This ADR documents how Profile A
would be built when there is a deployment target that justifies it.

## Context

The framework supports two deployment profiles for projection delivery:

- **Profile B (v1 target):** scale-to-zero databases such as Neon, Turso,
  Cloudflare D1. The DB cannot be used as the delivery mechanism because any
  continuous activity (polling, `LISTEN/NOTIFY` sessions) defeats the
  scale-to-zero cost model. Delivery moves out of the DB into an external
  durable runtime (recommended: Restate; alternatives behind the same
  `EventPublisher` interface: NATS JetStream, AWS SNS+SQS, GCP Pub/Sub,
  Cloudflare Queues).

- **Profile A (this ADR, deferred):** always-on Postgres deployments (RDS,
  Aurora, self-hosted, etc.) where polling or `LISTEN/NOTIFY` is acceptable
  because the compute is never expected to suspend. In this profile the DB
  *can* be the delivery mechanism, which removes the dependency on an external
  bus or durable runtime.

Profile B was chosen as the v1 implementation target because:

1. The stated deployment targets are serverless DBs.
2. Profile B's primitives (outbox table, inline projections, `EventPublisher`
   interface, idempotent projectors, gap detection) form a strict superset of
   what Profile A needs. Designing B first and adding A later is cheaper than
   the reverse.
3. Profile A can be expressed in Profile B's abstractions: it is simply
   "use the in-process / in-DB `EventPublisher` and skip the external bus".

## Decision

Profile A, when added, will be implemented as an additional `EventPublisher`
adapter and an in-DB projector runtime — **not** as a separate framework
mode. The aggregate runtime, codegen, envelope, schema, constraint table,
crypto-shredding, snapshots, and outbox table remain identical across both
profiles.

The Profile A additions are:

### 1. `EventPublisher` adapters

Two new reference adapters under the existing `EventPublisher` interface:

- **`pollingpublisher`** — polls the events table every `N` ms
  (`WHERE global_position > :cursor ORDER BY global_position LIMIT M`),
  dispatches to in-process subscribers. Same code path on SQLite and
  Postgres. Default `N = 100ms`.
- **`notifypublisher`** — Postgres-only optimization. On commit, the
  append path issues a `NOTIFY events_appended, '<position>'`. Subscribers
  hold a `LISTEN` connection on a non-pooled (or session-pooled) Postgres
  connection. On notification, drain immediately. Polling backstops as the
  correctness mechanism — `NOTIFY` is best-effort.

The framework selects one via configuration; the runtime API does not change.

### 2. In-process projector runtime

A `projection.Run(ctx)` helper that holds the projector goroutine, manages the
poll loop, owns the `LISTEN` connection (if used), and writes checkpoints in
the read-model database transactionally with each batch.

### 3. Same outbox, same drain, same gap detection

- The outbox table is identical to Profile B's. In Profile A it's used for
  external dispatch (Kafka, webhooks, third-party integrations), not for
  internal projection delivery.
- The scheduled drain still exists for outbox stragglers but is rarely
  needed because in-DB polling already catches up missed events.
- Gap detection in projectors uses the same `global_position` cursor logic.

### 4. Postgres connection pooler caveat

Profile A's `notifypublisher` cannot run against PgBouncer in transaction
mode. Deployment notes will require either:

- A direct connection (no pooler) for the `LISTEN` subscriber, **or**
- Session-mode pooling for the subscriber connections, **or**
- Polling-only mode (no `NOTIFY`).

This restriction is documented at the adapter level, not enforced at the
framework level.

## Consequences

### Positive

- **No mandatory external bus.** Deployments that already run always-on
  Postgres don't need to bring in Restate / Kafka / SQS just to deliver
  events to projectors.
- **Lower-latency projections** when `NOTIFY` is available (~ms vs Profile B's
  network round-trip to the durable runtime).
- **Simpler local development** for traditional Postgres users — projector
  starts in the same process as the writer.
- **Profile A is a config switch, not a divergent implementation.** Same
  schema, same codegen, same envelope, same crypto-shredding, same outbox.

### Negative

- **Adds a Postgres-specific code path** (`NOTIFY`). SQLite still uses
  polling. Two adapters to test, document, and maintain.
- **Connection pooler incompatibility.** Teams using PgBouncer in
  transaction mode (a common Neon-era default) cannot use `NOTIFY`; they
  fall back to polling and lose the latency win.
- **Polling-only is genuinely worse than Profile B's push model** for
  most metrics. Teams choosing Profile A purely to avoid Restate may find
  that a managed message bus is cheaper and lower-latency than the
  polling-plus-database load they end up paying for.
- **Two profiles to document and support** indefinitely once shipped.

### Neutral

- Profile A adds no new primitives to the aggregate, snapshot, constraint,
  or crypto-shredding layers. All behavior is local to the projection
  delivery path.

## Alternatives Considered

### A.1: Logical replication slot (rejected for v1 and Profile A both)

Use Postgres logical replication (`pgoutput` / `wal2json`) as the delivery
mechanism. Strongly ordered by commit LSN, no append serialization, no lost
events under concurrency.

Rejected because:

- Operationally heavy: slots can become bloated if a consumer dies, slot
  retention has to be managed, replication slot is a single point of
  failure unless replicated.
- Diverges materially from SQLite. Two adapter implementations would have
  almost nothing in common.
- LSN is opaque and not human-friendly for cursors.

Documented here so it isn't reconsidered in a future ADR without revisiting
this reasoning.

### A.2: `bigserial` + xmin watermark (gap-tolerant projectors)

Lock-free appends with bigserial; projectors defer reading past a position
until all transactions with `xmin <=` that position's `xmin` have
completed.

Reserved as a future option if Profile A's append throughput hits the
advisory-lock ceiling. Not selected for v1 of Profile A because the
projector watermark logic is non-trivial and the advisory lock is fast
enough for the expected use cases. Migration path: change the
`global_position` allocation strategy in the append path; projectors gain a
"stable cursor" abstraction; no schema change.

## Implementation Sketch (for when Profile A is built)

1. Add `eventstore/publisher/polling` package implementing
   `EventPublisher`. ~200 LoC, mostly the poll loop and cursor management.
2. Add `eventstore/publisher/pgnotify` package — Postgres-only — implementing
   `EventPublisher` with `LISTEN`/`NOTIFY` + polling fallback. ~400 LoC.
3. Modify the Postgres append path to emit `NOTIFY events_appended,
   '<global_position>'` after `COMMIT` (server-side trigger or
   application-side, depending on pooler constraints).
4. Add a `projection.LocalRunner` that owns the projector goroutine
   lifecycle for in-process projections.
5. Wire configuration: `publisher.type = "polling" | "pgnotify" | "restate"
   | "nats" | ...`. Codegen does not change.
6. Document the PgBouncer caveat at the adapter level.

Estimated effort when prioritized: ~1 engineer-week, plus tests.

## Open Questions for Future-Author

- Should `pgnotify` payloads carry the event (subject to NOTIFY's
  ~8000 byte limit) or only the `global_position`, forcing subscribers to
  fetch? Initial instinct: position only, since events can exceed the
  NOTIFY size limit and we want consistent semantics regardless of event
  size.
- Should we expose a deployment-mode validation step that fails fast on
  startup if the configured publisher is incompatible with the configured
  storage (e.g., `pgnotify` against SQLite)? Yes — fail loudly at config
  load.
