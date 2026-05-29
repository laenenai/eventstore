# ADR 0020: Projections and Read Models

- **Status:** Accepted
- **Date:** 2026-05-14
- **Pairs with:** ADR 0003 (Decider Aggregate Model), ADR 0012 (Event
  Delivery and EventPublisher), ADR 0014 (Outbox Shape)

## Context

The framework needs a coherent answer to "how do I read state, list
entities, and build derived views?" Today's surface is a single
`projection.Runtime` driving a `func(ctx, env) error` handler against
the polled event stream. That works, but leaves several decisions to
the application:

- How does a handler decode `env.Payload` to a typed event? (manual
  switch on `env.TypeURL` + codec.Decode)
- What happens when a handler fails mid-batch? (current: abort
  batch, retry from last checkpoint — re-processes already-applied
  events)
- Where does the checkpoint live? (only an in-memory implementation
  ships today)
- How do operators rebuild a projection? (undocumented)
- How does a user list "all entities of type X" without writing a
  full custom projection? (no answer)

This ADR consolidates the design for projections in the framework
and establishes a tiered model so users reach for the right tool at
the right level of complexity.

The framing exercise that informed this ADR: surveyed how other
event-sourcing stacks handle "current state per entity" — EventStoreDB
delegates entirely to application code; Marten (Postgres ES in .NET)
provides "inline projections" that run synchronously in the same
transaction as the event write; Axon and Equinox favor async event
handlers with optional snapshots. Marten's inline-projection pattern
is the closest direct precedent for the design here.

## Decision

The framework offers a **three-tier projection model**, each tier
optimized for a distinct read-model shape:

### Tier 1 — `state_cache` (sync, transactional, framework-shipped)

A single framework-managed table maintained transactionally with
event appends. Answers "what is the current state of stream X?" and
"list all entities of type X" without replay.

**Storage** (one table per adapter, framework migration):

```sql
CREATE TABLE state_cache (
    tenant_id   TEXT,
    stream_id   TEXT,
    type_url    TEXT,
    state       JSONB,
    version     BIGINT,
    terminal    BOOLEAN,
    updated_at  TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, stream_id)
);
```

**Write path.** `aggregate.Runtime.Handle` already computes the
pre-decide state and the events; we add a post-decide Evolve loop to
produce the *new* state, then pass marshaled bytes through to
`Store.Append` for in-tx persistence:

```go
type AppendParams struct {
    StreamID, ExpectedVersion, Events, Constraints  ...

    // Optional. When set, the adapter upserts the state_cache row
    // in the same transaction as the events.
    NewStateBytes []byte
    StateTypeURL  string
    Terminal      bool
}
```

The aggregate runtime gains an optional `StateCodec[S]` field. When
set, the runtime marshals the post-decide state and populates the
new `AppendParams` fields. When unset, behavior is unchanged.

In practice the framework requires `S` to satisfy `proto.Message` for
state-cache use, matching the direction the codebase has converged
to (party.State refactor, etc.). Non-proto state types continue to
work — they simply cannot use the state cache.

**Read path.** Adapters expose:

```go
// On es.Store (or a sibling es.StateCacheReader interface):
GetState(ctx, tenant, stream)                  -> bytes, typeURL, version, terminal
ListStates(ctx, tenant, typeURL, after, limit) -> []StateRow
```

**Serialization choice: JSONB.** Proto binary would be smaller but
forecloses ad-hoc operator queries against state fields (`state->>'min'`)
and functional indexes on JSONB paths — both load-bearing for the
tier-2 pattern below. The size delta is negligible for typical state
shapes and the operational ergonomics of JSONB dominate.

**Consistency.** Read-your-writes is guaranteed: the state row commits
in the same transaction as the events. No projection lag, no
checkpoint, no separate runtime.

**Rebuild.** A separate helper (`store.RebuildStateCache(ctx, tenant,
typeURL)`) reads events for the given type, folds them through
`Decider.Evolve`, and rewrites `state_cache` rows. Used after a state
proto schema change that requires reinterpreting historical events.

### Tier 2 — Postgres materialized views over `state_cache`

A large class of "list" queries that need joins, filters, or
aggregations are derivable from `state_cache` alone:

```sql
CREATE MATERIALIZED VIEW invoice_active AS
SELECT
    i.tenant_id,
    i.stream_id                                AS invoice_id,
    (i.state->>'customer_id')                  AS customer_id,
    (i.state->>'total')::numeric               AS total,
    c.state->>'name'                           AS customer_name
FROM state_cache i
LEFT JOIN state_cache c
       ON c.tenant_id = i.tenant_id
      AND c.stream_id = i.state->>'customer_stream_id'
      AND c.type_url  = 'myapp.customer.v1.State'
WHERE i.type_url  = 'myapp.invoice.v1.State'
  AND i.terminal  = false;

CREATE UNIQUE INDEX ON invoice_active (tenant_id, invoice_id);

REFRESH MATERIALIZED VIEW CONCURRENTLY invoice_active;
```

This is **not a framework feature**. The framework ships
`state_cache`; users write their own `CREATE MATERIALIZED VIEW` in
their app's migrations and trigger `REFRESH` from whatever scheduler
also runs the outbox drain. Documented in cookbook recipe 07.

The cost is refresh lag (the MV trails the cache by your refresh
interval); the benefit is plain SQL, normal indexes, and no Go code.

SQLite has no native MV. SQLite deployments either use triggers,
maintain a regular table, or fall back to a Tier 3 custom projection.

### Tier 2.5 — Streaming SQL engines (escape hatch, not framework-shipped)

When scheduled REFRESH becomes a bottleneck but the workload is still
naturally SQL, attach a streaming SQL engine (RisingWave, Materialize,
Flink SQL, ksqlDB) downstream of `state_cache` via Postgres logical
replication, or downstream of the event stream via Kafka.

Documented as a scaling pattern in cookbook recipe 07; the framework
ships nothing for this tier. Profile B (scale-to-zero) deployments
will rarely reach it.

### Tier 3 — Custom projections (async, code-driven)

Anything not derivable from current state alone: append-only ledgers,
audit logs, time-series rollups, full-text indexes, projections that
react to *events* rather than *state* (e.g., "count distinct
invoices created today"), external-service writes.

The existing `projection.Runtime` is the foundation. The decisions
below sharpen its semantics and add codegen.

#### Decision 3a — Handler API: codegen exhaustive typed interface

For each `.proto` event set, codegen emits a per-aggregate
projection interface and a constructor that returns a
`projection.Handler`:

```go
// gen/myapp/invoice/v1/invoice_projection.go (generated)
type Projection interface {
    OnCreated  (ctx context.Context, env es.Envelope, e *Created)   error
    OnPaid     (ctx context.Context, env es.Envelope, e *Paid)      error
    OnVoided   (ctx context.Context, env es.Envelope, e *Voided)    error
    OnRefunded (ctx context.Context, env es.Envelope, e *Refunded)  error
}

func NewProjectionDispatcher(p Projection, opts ...DispatcherOption) projection.Handler {
    // decode env.Payload via the generated EventCodec; type-switch
    // on the concrete variant; invoke the corresponding method.
}
```

The interface is **exhaustive**: adding a new event variant in
`.proto` produces a new method on the interface, which breaks every
implementing projection at compile time until they decide how to
handle the new variant (even if the decision is "return nil").
This is the safety property we want — it forces a deliberate choice
when the event vocabulary changes.

The cost is verbosity: a projection that only cares about one event
of an aggregate still must implement methods for the rest. That's
an acceptable trade against silent breakage.

#### Decision 3b — Unknown-TypeURL behavior: default error, opt-in skip

The generated dispatcher returns an error when it encounters a
TypeURL outside its aggregate's set. This pairs with Decision 3d
below: a batch halts and the operator investigates.

For cross-aggregate compositions, dispatchers opt into ignoring
TypeURLs they don't know about, and a small `projection.Chain`
helper composes them:

```go
handler := projection.Chain(
    invoice.NewProjectionDispatcher(&v, invoice.IgnoreUnknown()),
    customer.NewProjectionDispatcher(&v, customer.IgnoreUnknown()),
)
```

The opt-in is explicit at each dispatcher; this keeps the
"event-set-changed and my projection is missing the new variant"
safety property intact for single-aggregate projections (the 90%
case) while making composition straightforward.

#### Decision 3c — Read-model schema is application-owned

The framework codegens dispatch only. The read-model table, indexes,
upsert SQL, and migrations live in the application alongside the
projection code. Different projections need wildly different schemas
(denormalized aggregations, time-series, full-text, multi-table
joins) and schema codegen from event sets fights the use case.

This is the **Tier 3** trade: more code per projection, in exchange
for full flexibility on read-side shape. Tiers 1 and 2 absorb the
cases where this flexibility isn't needed.

#### Decision 3d — Failure semantics: fail-stop with last-success advance

When a handler returns an error mid-batch, the runtime:

1. Advances the checkpoint to the global_position of the last
   successfully-handled event in the batch.
2. Returns the error to the caller.

The next invocation resumes at the failing event. Once the handler
bug is fixed and deployed, the projection picks up exactly where it
left off — no replay of already-applied events.

Handlers MUST be idempotent. The checkpoint advance is in a separate
write from the handler's own writes (the handler operates against
the application's read-model storage, which may not even be the same
DB), so the framework cannot guarantee atomicity. If the checkpoint
write itself fails after the batch succeeds, the next run re-processes
the batch; handler idempotency makes this a no-op.

An opt-in DLQ-skip mode (drop the failing event into a
`projection_dlq` table, advance past it, continue) is deferred. The
case for it is weaker than for the outbox: outbox failures are
infrastructure flakes the operator can't fix from code, but
projection-handler failures are usually code bugs that fail-stop
already handles correctly.

#### Decision 3e — Checkpoint storage: framework default + interface override

Adapters ship a default `Checkpoint` implementation backed by a
framework-managed table:

```sql
CREATE TABLE projection_checkpoint (
    name        TEXT NOT NULL,
    tenant_id   TEXT NOT NULL DEFAULT '',  -- '' means cross-tenant
    cursor      BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (name, tenant_id)
);
```

`projection.Runtime.Checkpoint` remains an interface field: users
can swap in a Redis, DynamoDB, or in-process implementation when
needed. Same pattern as `es.Store` + `es.OutboxStore`.

Checkpoint is keyed by **(projection_name, tenant_id)**. Cursor is
the `global_position` of the last-applied event.

#### Decision 3f — Concurrency: single-instance default, opt-in `LockKey`

Mirrors the outbox drain (see cookbook 06):

- **Default**: a single runner per `(Name, Tenant)`. Operator
  ensures this via deployment pattern — scheduled trigger (Profile B),
  singleton StatefulSet, or external leader election.
- **Opt-in**: set `Runtime.LockKey`. The Postgres adapter implements
  `es.ProjectionLocker` via `pg_try_advisory_lock`. N replicas can
  call `Run` concurrently; losers exit cleanly with `(0, nil)`.
  SQLite is a no-op (file-level write lock already serializes).

Sharding (by stream-key hash, mirroring the drain's post-2026-05-14
design) is a future addition for throughput beyond what a single
runner can sustain.

#### Decision 3g — Rebuild: `ProjectionAdmin` interface + cookbook patterns

Adapters implement `es.ProjectionAdmin`:

```go
type ProjectionAdmin interface {
    Status (ctx, name, tenant)                              -> Status, error
    Reset  (ctx, name, tenant)                              -> error
    ResetTo(ctx, name, tenant, position uint64)             -> error
    List   (ctx)                                            -> []Status, error
}
```

The standard rebuild workflow (cookbook recipe 08) is:

1. Stop the runner (or leave it running and accept transient
   inconsistency).
2. App-specific: `TRUNCATE my_read_model;` (or equivalent).
3. `admin.Reset(ctx, "user-view", "tenant-x")`.
4. Runner picks up: re-reads events from gp=0, rebuilds.

The framework does not ship a one-command "rebuild" helper for
Tier 3. Read-model storage is opaque to the framework — only the
application knows what to truncate, and not all read models live in
the same Postgres/SQLite database. The application owns the
truncate; the framework owns the cursor reset.

For zero-downtime rebuilds, cookbook 08 documents the **versioned
parallel rebuild** pattern: deploy `user-view-v2` alongside
`user-view-v1`, both run, v2 catches up, atomic swap of read traffic
via a view/alias, then delete v1. This is a deployment pattern, not
a framework feature.

Tier 1 (`state_cache`) has its own narrower rebuild helper
(`store.RebuildStateCache`) because the framework *does* own that
table.

#### Decision 3h — Idempotency tooling: documented contract, no built-in

Handlers MUST be idempotent. The framework documents this as a
contract, gives concrete patterns (upserts via PK, `INSERT ... ON
CONFLICT`), and otherwise stays out of the way.

For handlers with genuinely non-idempotent side effects (sending
emails, publishing to non-dedup queues, calling payment APIs), the
standard pattern is a per-projection dedup table:

```sql
CREATE TABLE processed_events (
    projection_name  TEXT,
    tenant_id        TEXT,
    event_id         UUID,
    processed_at     TIMESTAMPTZ,
    PRIMARY KEY (projection_name, tenant_id, event_id)
);
```

A `projection.WithDedup(inner Handler, store DedupStore, ...) Handler`
wrapper is straightforward to ship later when there's concrete
demand. Not built in v1.

#### Decision 3i — Execution model: same as the drain

`projection.Runtime` already supports both `Run` (long-lived
goroutine) and `RunOnce` (one batch, returns). Cookbook recipe 06
patterns apply identically:

- **Profile B / scale-to-zero**: `RunOnce` driven by the same
  scheduled trigger that runs the outbox drain.
- **Profile A / always-on**: `Run` in a goroutine, optionally with
  `LockKey` for multi-replica safety.

## Future direction: spec-driven projections (v2)

**v2 enhancement — spec-driven projection codegen — shipped.**

The codegen ships via the `(es.v1.projection)` proto option (proto
extensions won over YAML — already-installed, no separate format
to learn). Codegen emits a per-projection typed interface +
dispatcher that handles only the listed events:

```proto
// myapp/customerview/v1/customerview.proto
message CustomerView {
  option (es.v1.projection) = {
    name: "customer-view"
    events: [
      "myapp.invoice.v1.Created",
      "myapp.invoice.v1.Paid",
      "myapp.customer.v1.NameChanged",
    ]
  };
}
```

Implementation: `cmd/protoc-gen-es-go/main.go` § `emitProjectionSpec`. Working
example: `proto/myapp/customerview/v1/` + `gen/myapp/customerview/v1/`.

This wins for:

- **Many-aggregate projections** (5+), where Chain composition gets
  unwieldy.
- **Cherry-picking** specific events from an aggregate without
  having to implement no-op methods for the rest.
- **External tooling**: dashboards, lineage tools, CI checks
  consuming the machine-readable spec.

A possible v3 (YAML format + external-tooling integration) would
be a separate ADR if it ever ships — proto extensions cover the
in-framework use cases.

Documented here so the v1 design is understood as the foundation
of a longer trajectory, not a final answer.

## Consequences

**Gained:**
- A clear answer to "give me current state of one stream" and "list
  all entities of type X" without writing code (Tier 1).
- A clear answer to "joins, filters, aggregations over state" via
  plain SQL (Tier 2).
- Type-safe custom projections with compile-time enforcement of
  event-set evolution (Tier 3 + Decision 3a).
- Cleaner failure recovery: handlers fix bugs and resume at the
  failing event (Decision 3d).
- Operator surface for projection inspection and rebuild
  (Decision 3g).
- No new runtime moving parts for the 90% case — Tier 1 piggybacks
  on the existing append transaction.

**Given up:**
- `state_cache` couples the write path slightly: one extra JSONB
  write per Append when the cache is enabled. Measurably small for
  typical state shapes; opt-out via leaving `StateCodec` unset.
- Tier 3 projections require handler idempotency; the framework
  cannot guarantee atomicity between the handler's writes and the
  checkpoint advance.
- Cross-aggregate projections (Decision 3b) require explicit
  `IgnoreUnknown()` per dispatcher — slightly verbose, but the
  verbosity is the point (makes the choice visible).
- `state_cache` lives in the same DB as events. Cross-DB
  deployments where state must live elsewhere fall back to Tier 3.

**Deferred to follow-up work** (each with an explicit sunset
criterion so the deferral has a finishing condition, not just a
"someday" hedge):

- **Tier 3 sharding by stream-key hash.** N independent runner
  instances, each owning `hash(tenant_id || stream_id) % N`.
  Sunset: when an adopter measures **≥1,000 events/sec sustained**
  arrival rate AND single-runner projection lag is growing
  unboundedly under normal load. Estimated effort: ~5 engineer-days
  (partitioning strategy, per-partition cursor table, recipe).

- **Tier 3 DLQ-skip mode** (analogue of `AutoResumeAfterDLQ` on
  the drain). Skip the failing event, record in `projection_dlq`,
  advance the cursor. Sunset: when an adopter ships a projection
  that mirrors to **an external system outside the eventstore's
  transactional boundary** (search index, OpenSearch sink,
  analytics warehouse) — or when manual `projection_dlq` table
  writes appear in production and need formalization. Estimated
  effort: ~2 engineer-days (table + queries already scaffolded;
  missing the opt-in flag, operator API, recipe section).

- **`projection.WithDedup` middleware** for non-idempotent
  handlers. Sunset: when an adopter ships a projection that needs
  at-most-once delivery AND cannot achieve idempotency at the sink
  (no UPSERT, no idempotency-key support, no natural primary key).
  Adopters whose sink _could_ UPSERT should fix the projection,
  not the framework. Estimated effort: ~3 engineer-days (interface
  + SQL impl piggybacking on `processed_events` + middleware
  wrapper + recipe).

- **Concurrent-claim drain mode** (`FOR UPDATE SKIP LOCKED`) for
  projections. Postgres-only intra-runner parallelism; the SQLite
  adapter is explicitly out of scope (single-writer can't benefit).
  Sunset: when an adopter measures projection lag that single-runner
  can't fix by handler optimization (handler profiled, optimized,
  still bottlenecked). Estimated effort: ~5 engineer-days. The
  parity break vs SQLite is a deliberate trade-off — documented in
  the future recipe.

## Alternatives considered

**Single-tier "everything is a custom projection."** Forces every
"current state" query through a hand-rolled projection. Loses
read-your-writes. Rejected.

**Polling-based state cache (the original proposal).** A projection
runtime maintains `state_cache` from events asynchronously. Equivalent
to running a projection that copies `Decider.Evolve` output to a
table. Rejected in favor of the in-tx variant: same code, strictly
better consistency, fewer moving parts.

**Bytea state_cache.** Smaller and faster than JSONB. Rejected for
Tier 2 ergonomics — JSONB enables the materialized-view pattern that
absorbs a large fraction of read-model use cases.

**Marten-style "schema generated from state type."** Codegen a
typed-column table per aggregate. Works when state is flat
primitives; falls apart on nested messages or repeated fields, and
schema evolution becomes a migration concern coupled to proto
changes. Rejected; JSONB sidesteps both.

**Bake leader election / sharding into the runtime.** The
deployment-pattern surface is identical to the outbox drain's
(scheduled trigger, advisory lock, external leader, sharding). Same
patterns, same opt-in shape, same cookbook recipe. No new framework
machinery beyond `ProjectionLocker`.

**Atomic checkpoint advance with handler writes.** Requires the
handler signature to include an adapter-specific transaction
handle. Leaky and adapter-coupled. Rejected; the idempotency
contract is cleaner.

## Reference

- ADR 0003 — Decider Aggregate Model (`Evolve` is the engine of
  Tier 1's write path)
- ADR 0012 — Event Delivery and EventPublisher (sibling of Tier 3;
  same execution patterns)
- ADR 0014 — Outbox Shape (mirror design — failure semantics,
  concurrency, admin surface)
- Cookbook recipe 06 — Running the Outbox Drain (deployment patterns
  apply identically to Tier 3)
- Cookbook recipe 07 (planned) — Read models via materialized views
- Cookbook recipe 08 (planned) — Rebuilding projections
- Prior art — Marten's "inline projections" (the Tier 1 pattern is
  the same idea, in JSONB rather than typed columns)
