# ADR 0012: Event Delivery and EventPublisher

- **Status:** Accepted
- **Date:** 2026-05-13
- **Pairs with:** ADR 0001 (Profile A — Always-On Database Deployment, deferred)

## Context

v1 targets scale-to-zero databases (Neon, Turso, Cloudflare D1). In this
profile, **the database cannot be the event-delivery mechanism**:

- Continuous polling defeats scale-to-zero — the compute never gets to
  suspend.
- `LISTEN/NOTIFY` is unavailable: Turso has no equivalent, and Neon's
  default transaction-mode pooler does not support session-scoped features.
  Persistent `LISTEN` connections also keep the compute awake.
- Logical replication slots require operational infrastructure (slot
  retention, replication management) incompatible with the
  zero-infrastructure spirit of serverless databases.

The architectural shift required: **projection and saga delivery moves out
of the database** into an external durable runtime. The database becomes a
source of truth and replay mechanism only — it wakes for writes, scheduled
drain, and on-demand catch-up reads, then suspends again.

## Decision

### `EventPublisher` interface (pluggable)

The framework defines a single `EventPublisher` interface. Adapters
implement it. The framework is not tied to any specific runtime.

### Reference adapters shipped with v1

- **`restate`** — **recommended** for the serverless / scale-to-zero
  profile. First-class integration: codegen produces Restate service
  definitions for projectors and sagas. Framework documentation positions
  Restate as the path of least resistance for Neon/Turso deployments.
- **`nats`** — NATS JetStream, lightweight, self-hostable, durable.
- **`sns`** — AWS SNS+SQS, pay-per-message, managed.
- **`pubsub`** — GCP Pub/Sub, pay-per-message, managed.
- **`cfqueues`** — Cloudflare Queues, edge-friendly, pairs with Workers.
- **`inproc`** — single-process in-memory, for dev and tests only.

### Sagas and projectors are unified

Both are durable handlers reacting to events. Restate's invocation model
expresses both. The same codegen path produces both kinds of handler;
the framework maintains one runtime, not two.

### Write flow

```
1. BEGIN
2.   pg_advisory_xact_lock(...)           -- ADR 0009
3.   INSERT events ...                    -- ADR 0005
4.   INSERT unique_claims ...             -- first-class uniqueness
5.   INSERT outbox row (pending)
6.   INSERT/UPDATE inline projection updates  -- read-your-writes
7. COMMIT
8. publisher.Publish(envelope)            -- fire-and-forget
9. (on success: UPDATE outbox SET published_at = now())
```

If the writer crashes between step 7 and step 9, the outbox row remains
`pending` and is picked up by the scheduled drain.

### Inline projections

Run inside the writer transaction (step 6). Used for read models that
need read-your-writes consistency. Mandatory characteristic for the
serverless profile, but useful in any profile.

### Outbox + scheduled drain

The outbox table is the durability backstop. A scheduled job (cron,
serverless cron, EventBridge, Cloud Scheduler) wakes the database every
`N` minutes (deployment-tunable, default 5 minutes), publishes any rows
with `published_at IS NULL`, and sleeps. The database suspends between
runs.

The drain interval is the **maximum delivery delay for a stuck event**,
not the normal-case latency. Normal-case latency is the publisher,
measured in milliseconds.

### Gap detection in subscribers

Each published envelope carries its `global_position`. Subscribers track
the position they have consumed; when they receive an event whose
`global_position` skips, they wake the database on demand to fetch the
missing positions before processing further.

## Consequences

### Positive

- **The eventstore database stays asleep unless there is real write
  activity.** Scale-to-zero preserved.
- **Cost scales with writes, not with idle time.**
- **Sagas and projectors share infrastructure.** One runtime to maintain,
  test, and reason about.
- **Exactly-once handler semantics** (via Restate or equivalent
  durable runtime) remove the need for hand-rolled idempotency.
- **Pluggable publisher** means deployments not on Restate use a managed
  bus with no code change. Profile A (always-on Postgres with in-DB
  polling or `LISTEN/NOTIFY`) becomes "add a polling EventPublisher", not
  a different architecture (see ADR 0001).
- **Inline projections** give read-your-writes consistency for the hot
  path without any subscription infrastructure.

### Negative

- **External durable runtime is a hard dependency.** Restate, NATS, SQS,
  Pub/Sub, or Cloudflare Queues becomes an operational concern.
- **At-least-once delivery semantics.** Projectors must be idempotent.
  Restate's exactly-once handler semantics make this less painful, but
  the framework's contract is at-least-once at the publisher boundary.
- **Restate ecosystem is younger than Kafka/SQS.** Vendor risk exists.
  Mitigated by keeping `EventPublisher` pluggable and standardizing on
  `global_position` for ordering and dedup — migrating to a different
  publisher does not require code changes in the framework.
- **All adapters must behave equivalently on `global_position`
  ordering.** Test discipline required; the framework ships a conformance
  suite for `EventPublisher` implementations.
- **Two systems to trace through during debugging** (eventstore DB +
  publisher runtime). OTel integration helps.

## Alternatives Considered

### Database-as-bus (polling, `LISTEN/NOTIFY`, logical replication)

Rejected for v1. Defeats scale-to-zero (polling), unavailable on the
target platforms (`LISTEN/NOTIFY` on Turso, transaction-pooler on Neon),
or operationally heavy (logical replication). Documented as Profile A in
ADR 0001 for future implementation if a non-serverless deployment is
ever targeted.

### Hardcode a single bus (e.g., Kafka)

Rejected. Locks consumers to infrastructure they may not want or be able
to operate. The pluggable `EventPublisher` interface costs little and
preserves optionality.

### Skip the outbox, publish directly to the bus

Rejected. A writer crash between `COMMIT` and the publish call loses
events. The outbox is the durability backstop that makes the
fire-and-forget publish acceptable.

### Synchronous publish inside the writer transaction

Rejected. Couples the writer's latency and availability to the
publisher's. The fire-and-forget pattern with an outbox drain gives the
same durability with much better latency and fault tolerance.

### Restate as the only publisher

Rejected. Vendor lock-in. Restate is the recommended adapter for the
serverless profile, but the framework's interface and conformance suite
ensure alternatives stay viable.
