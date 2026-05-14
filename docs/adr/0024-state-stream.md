# ADR 0024: state_stream — Coalesced State-Mirror Delivery

- **Status:** Accepted
- **Date:** 2026-05-14
- **Pairs with:** ADR 0012 (Event Delivery and EventPublisher),
  ADR 0014 (Outbox Table Shape), ADR 0020 (Projections and Read Models),
  ADR 0023 (state_cache subsumes snapshots)

## Context

The framework already ships two delivery mechanisms:

- **Events outbox** (ADR 0014): per-event, append-only, every event must
  be delivered. Subscribers consume `es.Envelope` and typically react to
  *transitions* — sagas, audit logs, projections that fold events.

- **Tier 1 `state_cache`** (ADR 0020, ADR 0023): in-tx synchronous write
  of the current state per stream. Backs `GetState` / `ListStates` for
  in-DB queries and Tier 2 materialized views.

Neither delivers **current state to external subscribers**. The
canonical example is a search index or denormalized read store
elsewhere (Elasticsearch, Algolia, a downstream Postgres in another
service, a webhook target) that wants to mirror "the latest state of
every aggregate of type X." Today such a subscriber has three options,
all unsatisfying:

1. **Subscribe to events + maintain its own Evolve.** Duplicates the
   Decider's fold logic in the receiver. Bug-prone, schema-coupled.
2. **Subscribe to events + call `GetState` per event.** Two round-trips
   per event, wasteful at scale.
3. **Periodically poll `ListStates`.** Latency at refresh cadence,
   wasteful when nothing changed.

`state_stream` is the framework's purpose-built answer: **coalesced,
state-shaped delivery** to external subscribers, sharing storage with
`state_cache` so there is no duplicate-write cost at Append time.

## Decision

Eight coupled design decisions, derived from the Q&A loop that produced
this ADR.

### 1. Subscriber pattern: state-mirror (coalesced)

The use case is "keep an external store in sync with current state."
The framework optimizes for this; subscribers that want *every* state
transition use the events outbox instead.

A subscriber 1 hour behind sees one delivery per stream (current
state), not 1000 (history of transitions). Coalescing is structural,
not configured.

### 2. Storage: read from state_cache, per-subscriber positions in a sibling table

No state duplication. The state bytes live exactly once, in
`state_cache`. A new table tracks per-subscriber delivery progress:

```sql
CREATE TABLE state_stream_subscribers (
    name                  TEXT        NOT NULL,
    tenant_id             TEXT        NOT NULL DEFAULT '',
    stream_id             TEXT        NOT NULL,
    last_delivered_version BIGINT     NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (name, tenant_id, stream_id)
);
```

Drain query (per subscriber):

```sql
SELECT sc.tenant_id, sc.stream_id, sc.type_url, sc.state, sc.version,
       sc.state_schema_version, sc.updated_at
FROM state_cache sc
LEFT JOIN state_stream_subscribers p
  ON p.name = $sub_name
 AND p.tenant_id = sc.tenant_id
 AND p.stream_id = sc.stream_id
WHERE COALESCE(p.last_delivered_version, 0) < sc.version
  AND ($tenant_filter = '' OR sc.tenant_id = $tenant_filter)
ORDER BY sc.updated_at
LIMIT $batch;
```

On successful delivery, `last_delivered_version` is upserted to the
state's version. On failure, the position stays put and the next drain
cycle re-attempts — picking up the *latest* state at that point, not
the version that previously failed. Coalescing-on-retry is automatic
and structural.

### 3. Delivery payload: typed `StateEnvelope`

```go
type StateEnvelope struct {
    TenantID           string
    StreamID           string  // canonical
    TypeURL            string  // proto FullName of the state
    Version            uint64  // stream version this state reflects
    StateSchemaVersion uint32  // for receiver-side upcasting
    State              []byte  // marshalled state (protojson)
    UpdatedAt          time.Time
}
```

`Version` lets receivers reject delayed/duplicate deliveries
(`if incoming.Version <= stored.Version: skip`). `TypeURL` and
`StateSchemaVersion` let cross-aggregate receivers route and evolve
gracefully.

### 4. Drain runtime + publisher interface: separate from events

Mirror the outbox shape exactly:

```go
// New publisher interface, parallel to publisher.Publisher:
type StatePublisher interface {
    PublishState(ctx context.Context, env es.StateEnvelope) error
}

// New drain runtime, parallel to outbox.Drain:
type Drain struct {
    SubscriberName string
    Store          es.Store
    Publisher      state_stream.StatePublisher
    Tenant         string   // or "" for cross-tenant
    BatchSize      int
    LockKey        string   // pg_try_advisory_lock, distinct keyspace from outbox
    Shard          int
    TotalShards    int
}
```

Both drains run independently in production: separate `LockKey`,
separate schedules (or shared scheduler firing both). Cookbook 06's
five deployment patterns apply identically. Adapters implement one or
both publisher interfaces depending on what shape they support.

### 5. Subscriber lifecycle: implicit registration, admin for replay

No explicit `Register` step. First `Drain.Run` for `name` discovers no
positions, delivers current state of every stream (full backfill),
fills the position table as deliveries succeed.

Admin interface:

```go
type StateStreamAdmin interface {
    Status(ctx, name, tenant)                              -> []StateStreamSubscriberStatus
    Reset(ctx, name, tenant)                               -> error  // delete all positions
    ResetForStream(ctx, name, tenant, streamID)            -> error  // single-stream rewind
    List(ctx)                                              -> []string  // known names
}
```

Replay from history is not supported — `state_stream` has no history,
only current state. Subscribers wanting historical transitions are
event subscribers.

### 6. Failure semantics: retry forever, observability-driven

No DLQ in v1. Drain logs failures, exposes `Status.StreamsBehind` /
`MaxLagVersion` per subscriber for alerting. Persistent failures stay
in retry; coalescing-on-retry auto-recovers from transient ones.
Per-stream isolation is structural — a single failing stream doesn't
block other streams' delivery for the same subscriber.

Future iteration may add per-stream DLQ-skip (`MaxAttempts` +
`state_stream_dlq` table mirroring outbox's design), but no concrete
use case justified it in v1.

### 7. Crypto-shred interaction: operator-driven for v1

`state_cache` stores plaintext (deliberate — JSONB queryability for
Tier 2 MVs depends on plaintext PII fields). After `ForgetSubject`:

- The DEK is destroyed (encrypted event bytes become inaccessible).
- `state_cache` rows for the shredded subject still hold plaintext PII.
- The operator runs `aggregate.RebuildStateCache` to replay events
  through `DecryptPII`; shredded fields zero out; redacted state is
  rewritten to `state_cache`.
- The operator calls `state_stream.Admin.ResetForStream` for each
  affected stream so the drain redelivers the redacted state.

This is three steps. A convenience helper bundles them:

```go
func ForgetAndPropagate[S, C, E](
    ctx context.Context,
    shredder *shred.Shredder,
    rt *aggregate.Runtime[S, C, E],
    admin StateStreamAdmin,
    tenantID, subject string,
) error
```

The compliance window between step 1 (shred) and step 3 (redeliver) is
operator-controllable but real. Cookbook recipe 13 documents this
explicitly. Automatic cascade (cascade on `ForgetSubject`, with a
`revision` column on state_cache + per-aggregate `RedactPII` codegen)
is documented as v2 work.

### 8. Relationship to events outbox: fully independent

No coordination. Different lock keys (distinct FNV prefix for the
advisory-lock derivation), different schedules, different publishers.
A subscriber registers for *one* delivery mode — event-shaped or
state-shaped, not both — because the two answer different questions.
Cookbook 13 documents the gotchas:

- Aggregates without `StateCodec` are invisible to state_stream
  subscribers (no state_cache row to deliver).
- No cross-stream ordering for state deliveries (coalesced).
- No cross-mode atomicity (events drain and state drain deliver at
  different times).

## Consequences

**Gained:**

- **One unified mechanism for "mirror this aggregate's current state
  externally."** Replaces the three unsatisfying options the user had
  before.
- **Bounded storage at any subscriber lag.** Position table grows by
  one row per (subscriber × stream), each ~50 bytes. State bytes
  aren't duplicated.
- **Coalescing-on-retry as a built-in property.** Transient failures
  auto-recover with the latest state, not the failed one.
- **Same operator pattern (cookbook 06) applies twice.** Outbox drain
  and state stream drain share lock/schedule/scaling shape.
- **Receiver-side idempotency is trivial.** Each delivery carries
  `Version`; the standard "upsert if version > stored" pattern works.
- **The state_cache table earns its dual purpose.** It's the read API
  *and* the delivery source. One table, two consumers.

**Given up:**

- **No history of state transitions.** Subscribers cannot see
  intermediate states they missed during downtime — they get the
  latest. This is the explicit Q1 tradeoff.
- **No DLQ in v1.** A subscriber permanently failing on one stream
  retries forever; observability is the only knob. Documented as
  intentional for v1; future addition tracked.
- **Crypto-shred propagation is a three-step operator runbook.**
  Compliance window between shred and propagation is operator-managed.
  Future automatic cascade is v2 work.
- **Subscribers must choose one delivery mode.** A subscriber that
  wants both event detail and state mirror must run two subscriptions
  (one of each); the framework doesn't unify them.

**Deferred to future ADRs / implementations:**

- Per-stream DLQ-skip mode (mirror events outbox's DLQ semantics).
- Automatic shred cascade with `revision` column on `state_cache` +
  per-aggregate `RedactPII` codegen on State protos.
- Per-stream backoff for persistent failures (cap retry burn rate).
- State_stream sharding by stream-key hash (same shape as outbox drain
  sharding).

## Alternatives Considered

### Append-only state_outbox table

Considered: write one row per Append into `state_outbox` (mirror events
outbox), drain reads append-only, position is a global cursor.
Rejected — duplicates state bytes for every Append; no natural
coalescing; storage grows unboundedly with throughput regardless of
subscriber state. Per-subscriber position tracking is cheaper and
gives coalescing for free.

### Coupled drains (events deliver before state for same Append)

Rejected. The two delivery modes serve disjoint subscribers by design;
coordination solves a problem real users don't have. Coupling
introduces failure-domain crossover (slow event publisher delays
unrelated state delivery).

### State_outbox with periodic compaction

Append-only table that's periodically reduced to "latest per stream
where no subscriber has caught up." Effectively reinvents Option B's
position-tracking with extra steps; no benefit over storing the state
in `state_cache` directly.

### Stream-of-transitions delivery (Option B in Q1)

Subscribers receive every state change in order, with previous/new
state. Rejected — already served by the events outbox (subscribers
that want transitions consume events and either fold them or call
GetState). No real use case for "transition delivery from a third
table."

### Halt-on-failure (Option C in Q6)

One failure halts the whole subscriber. Rejected — state_stream's
per-stream isolation makes halt-everything inappropriate. Failed
stream X doesn't poison stream Y.

### Encrypted state_cache (Option B in Q7's design space)

Encrypt PII fields in state_cache. Rejected for v1: defeats JSONB
queryability for non-PII fields and forces every Tier 2 MV to know
about decryption. The operator-driven model (A) is correct for v1;
encrypted state is a future direction if compliance pressure mandates
zero plaintext window.

## Reference

- ADR 0014 — Outbox Table Shape (shape sibling)
- ADR 0020 — Projections and Read Models (Tier 1 + Tier 3 + Tier 3.5)
- ADR 0023 — state_cache subsumes snapshots (the state_stream source)
- Cookbook recipe 06 — Running the Outbox Drain (deployment patterns
  apply to both drains)
- Cookbook recipe 13 (planned) — state_stream operator story
- [`state_stream/`](../../state_stream/) — runtime package (planned)
- [`es/state_envelope.go`](../../es/state_envelope.go) — `StateEnvelope` + `StatePublisher` (planned)
