# ADR 0029: Per-Command Subscriber Batch Delivery

- **Status:** Accepted
- **Date:** 2026-05-15
- **Pairs with:** ADR 0023 (state_cache supersedes snapshots), ADR 0025
  (workflow-orchestrated command bus), ADR 0026 (workflow adapters).

## Context

ADR 0025 introduced `cmdworkflow.Subscriber[C]` with a per-event
delivery contract: the framework calls `Handle(ctx, env)` once for
every emitted envelope that matched a subscriber's filter. The bus
loops over `[]es.Envelope` and dispatches each event individually,
treating retries and DLQ writes as per-event units.

That shape was natural for the initial Restate-style decomposition —
each step is one event, one journal entry. But it has been an
ergonomic and operational sore spot from the first real consumer.

The persistent friction lives on the read side. The most common
subscriber is "a Tier-3 projection that mirrors current aggregate
state into a query store" — read-models, search indexes, caches.
Today the subscriber receives one envelope at a time and gets to
decide between two equally-bad options:

1. **Re-derive state from the envelope payload.** Decode the event,
   run an Evolve-equivalent in projection code, write the result.
   That duplicates Decider logic — Evolve lives in the aggregate
   package, but the projection has to mirror its rules to compute the
   "current state" the read store wants. Schema evolution
   (ADR 0013) doubles the surface area: a new event or a renamed
   field has to be handled in BOTH the aggregate Evolve and every
   projection's per-event branch. Tests catch divergence late; the
   compiler doesn't.

2. **Call `Load(ctx, sid)` from the subscriber.** Round-trip to
   state_cache for every envelope. The runtime already loaded the
   same state during `Handle` — and persisted it transactionally
   into state_cache via the StateCodec — and now the subscriber
   reads it back out a few milliseconds later. The work is wasted;
   the latency adds up under fan-out. Worse: with the per-event
   loop, a command emitting N events causes N redundant Loads per
   subscriber.

The framework already has the state in memory at the exact moment a
subscriber should see it: between the Append and the fan-out, the
runtime's `Handle` produced the post-Decide state and wrote it to
state_cache in the same transaction as the events. The bus had no
reason to throw it away.

A secondary friction: the per-event loop produces N journal entries
per command in Restate (one RunAsync per (subscriber, event)).
Bounded by `MaxRetries` and subscriber count, but unnecessarily fat
for the common "Created and 0..few attribute changes in one command"
case. The unit a saga step wants to retry is "the command that
happened", not "the individual event that happened" — those are the
same thing for most commands.

## Decision

`Subscriber` becomes generic on `[S, C, E]` and receives the
**whole command-batch** in one Handle call:

```go
type Subscriber[S, C, E any] struct {
    Name           string
    Filter         EventFilter
    Mode           DeliveryMode
    MaxRetries     int
    OnExhausted    ExhaustedPolicy
    AttemptTimeout time.Duration

    Handle func(ctx context.Context, envs []es.Envelope, state S, events []E) error

    Compensate func(ctx context.Context, envs []es.Envelope, state S, events []E) (C, error)
}
```

`envs` is the filtered envelope batch from the command, `state` is
the post-Decide state (the same value the runtime's StateCodec wrote
to state_cache), and `events` is `[]E` decoded once from the
envelopes — index-aligned with `envs`. A subscriber whose filter
rejects every envelope in the batch is **not called at all** (no
journal entry, no DLQ row, no spawned workflow).

`Workflow` gains the same `E` type parameter and a new constructor
argument:

```go
func New[S, C, E any](
    runner AggregateRunner[S, C],
    store es.Store,
    wf WorkflowRuntime,
    codec aggregate.Codec[E],
) *Workflow[S, C, E]
```

The codec is required — the bus uses it to decode envelopes into
`[]E` before handing them to subscribers. The same codec the
aggregate runtime already owns.

### Why per-batch is the right unit

The Decider produces a set of events from one command **as a unit**:
the events that, applied together, take the aggregate from one
consistent state to the next. Splitting them across separate
subscriber invocations breaks that abstraction. A subscriber that
sees `(InvoiceCreated, LineItemAdded, LineItemAdded)` in three
separate calls has to reconstruct "they all came from one command"
to do anything coherent with them. The command-batch IS the unit
the Decider thought about; the framework should hand it over as a
unit.

The post-Decide state is the second piece. Every Sync subscriber
either needs current state (to write a read-model row), needs to
diff against current state (to publish a change event), or doesn't
need state at all (audit logs). The first two cases want a state
value, not a stream of events to fold themselves. The third case
ignores `state` — zero cost. By passing state in, we make the
common case trivial and the uncommon case unchanged.

The runtime already computes this state. `Runtime.Handle` runs
`Decide`, computes the post-Decide state via `Evolve`, writes it to
state_cache via `StateCodec`, then returns. The bus is one in-process
hop away — it can ship the same value to subscribers without a
second Load.

### Dispatch flow

Per HandleCmd:

1. `appendStep` — `aggregate.Runtime.Handle` produces the AppendResult.
2. `readEnvelopesStep` — fetch the envelopes with assigned
   `global_position`. Journaled.
3. **NEW: `readStateStep`** — `runner.Load(ctx, sid)` + encode via
   the runner's StateCodec. The bytes are journaled so workflow
   replay sees the same state, not whatever state_cache holds at
   replay time (a sibling command may have advanced it between the
   first run and the replay).
4. **NEW: decode events once.** `codec.Decode` per envelope yields
   `[]E`. Deterministic, not separately journaled.
5. **Per-subscriber fan-out** — filter envelopes; if any survive,
   the subscriber receives ONE Handle call with the filtered batch,
   the typed state, and the typed events. Step name keyed off the
   first event id in the batch: `<prefix><sub.Name>:cmd:<firstID>`.
6. Wait on Sync futures; apply OnExhausted per (subscriber, batch).
7. Return the state from step 3 — no second Load on the happy path.

Compensation breaks the "no second Load" rule: the compensating
HandleCmd appends new events, so the final state has advanced
beyond what step 3 captured. Detect this and Load once at the end,
only on the saga-failure path.

### Why the `E` type parameter

The cleanest alternative was "leave the bus generic over `[S, C]`
only; let subscribers decode their own envelopes." That keeps the
bus signature narrow but pushes the per-subscriber decode boilerplate
back onto every projection. The framework already has a Codec[E]
sitting in the aggregate runtime — passing it through to subscribers
costs one constructor argument and removes the boilerplate from
every projection.

The downside: applications that mix aggregates with different event
sum types in one bus are no longer expressible. They never were
sensibly expressible — one bus per aggregate has been the pattern
since ADR 0025 (`Workflow[S, C, E]` was always one aggregate's
shape). Making `E` explicit aligns the type signature with the
already-existing invariant.

### Per-batch retry and DLQ

The retry budget is per-batch: one Handle call counts as one
attempt. A subscriber whose batch contains five events and whose
MaxRetries is 3 gets four total Handle calls in the worst case, not
twenty.

The DLQ row shape becomes batch-shaped:

```sql
CREATE TABLE subscriber_dlq (
    subscriber_name TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    stream_id       TEXT        NOT NULL,
    first_event_id  TEXT        NOT NULL,
    event_ids       TEXT[]      NOT NULL,
    type_urls       TEXT[]      NOT NULL,
    last_error      TEXT        NOT NULL,
    attempts        INT         NOT NULL,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (subscriber_name, tenant_id, first_event_id)
);
```

`first_event_id` is the row's identity for replay / delete — under
OCC a single stream cannot produce overlapping command-batches, so
the first event id in the batch uniquely identifies the batch within
(subscriber, tenant). SQLite stores `event_ids` and `type_urls` as
JSON-encoded TEXT (no native array type).

Operationally a row says: "subscriber X exhausted on this set of
events; here's the full batch with type URLs for context, and the
final error message." Replay tooling reconstructs the batch from
`event_ids`, decodes via the codec, hands it back to Handle.

### Async fan-out keeps wire shape narrow

`AsyncPayload` already carried `EnvBytes` for the durable Restate /
DBOS path; the new model still ships just the envelope batch plus
the subscriber name. The dispatcher on the consumer side:

1. Decodes envelopes from `EnvBytes` via the bus's gob codec.
2. Decodes events from envelope payloads via the typed codec.
3. Re-Loads state via `runner.Load(ctx, sid)` — one state_cache read.
4. Runs the same retry + exhausted policy as Sync.

The Load is the only extra cost vs. shipping state on the wire,
and it's the same lookup the runtime would do on the next command
for the same stream. State doesn't go on the wire because there is
no portable framework-owned state codec — `S` is an arbitrary type
parameter, the runtime's StateCodec lives behind the AggregateRunner
boundary, and pushing state encoding into AsyncPayload would force
every codec to satisfy both encode/decode roundtrip AND wire
versioning. Reload is simpler and the latency cost is bounded by
one state_cache read per Async dispatch.

### Migration story

None. This ADR documents a clean break against pre-production code;
there are no production deployments to migrate. The
`subscriber_dlq` table is dropped and recreated by a new migration
(00014 in Postgres, 00013 in SQLite). The old per-event Subscriber
type, the old AsyncPayload shape, and the old DLQ row shape are
deleted, not deprecated. Codegen-emitted services are regenerated
to the new `Workflow[S, C, E]` signature.

## Consequences

### Positive

- **Projections collapse to one expression.** "Given the
  post-Decide state, store-or-delete based on terminal status." No
  per-event type switch, no Decider duplication. The inproc example's
  `ReadModel.handle` shrank from ~20 lines of switch-on-TypeURL to 5
  lines: lock, check terminal, store-or-delete.
- **One Decider, one Evolve.** Projections never re-implement Evolve
  for the common "mirror current state" case. Schema evolution stays
  in the aggregate; projections inherit it through the state they
  receive.
- **Fewer journal entries.** N matched subscribers × commands instead
  of N × M (M = events per command). The journal storage cost was
  already bounded by MaxRetries; this halves the constant.
- **Retry budget aligns with the unit of work.** Sagas retry "the
  command that failed", not "event 2 of 4 in the command that
  failed". The latter was never the actually-useful granularity.
- **State is in the right place.** The runtime already has it from
  the in-tx state_cache write. Subscribers consume it directly
  instead of re-Loading or re-deriving.

### Negative

- **Whole-batch retry granularity.** If a batch's third event is
  what trips a subscriber, the retry replays the whole batch. The
  subscriber must be idempotent across the batch — but it had to be
  idempotent on `env.EventID` anyway under at-least-once delivery,
  so per-batch idempotency is the same contract scoped to a slightly
  larger unit. Projections that UPSERT keyed on stream id satisfy
  this for free.
- **One Async-dispatch state Load.** The wire shape doesn't carry
  state, so the durable Async dispatcher re-Loads. One state_cache
  read per Async dispatch is the cost — equivalent to one
  additional lookup the runtime would do anyway for the stream's
  next command.
- **Mandatory `E` in the constructor.** Applications that wanted a
  type-erased bus need a different abstraction. The signature
  change is mechanical for everyone else: `New[S, C](rt, store, wf)`
  becomes `New[S, C, E](rt, store, wf, codec)`.
- **AggregateRunner gains EncodeState / DecodeState.** The bus needs
  a way to journal the post-Decide state bytes. The runtime already
  has a StateCodec; exposing it through the runner interface is the
  least-invasive bridge. Custom runner implementations need to
  satisfy both methods (delegating to whatever state codec they
  use); the framework's `*aggregate.Runtime[S, C, E]` does so
  natively via its `StateCodec` field.

### Neutral

- **DLQ replay semantics shift slightly.** Replay is now "rerun the
  failed batch through Handle" instead of "rerun the failed event."
  The batch is in the row's `event_ids` column; tooling
  reconstructs it from the eventstore. The shift matches what
  operators wanted anyway — the operational unit of "what failed"
  is the command, not one of its events.
- **No effect on Sync vs. Async semantics, Mode/MaxRetries/
  OnExhausted axes, or compensation.** Those policies live one
  level above the per-event vs. per-batch question and survive
  unchanged.

## Alternatives Considered

### Keep per-event, ship state alongside each call

Rejected. Adds `state` to every per-event Handle but still calls
Handle N times for N events in one command. The savings on the
"re-derive state" pain point apply, but the projection still has to
reason about "is this one of the events from the command I just
saw?" — it doesn't get the batch as a unit. Aborting half-way
through a command's fan-out also gets weird (which envelopes did
the subscriber see before the failure?). The unit cleanup matters
more than the state-passing in isolation.

### Pass state as a closure / lazy load

Rejected. The runtime might offer subscribers a `LoadState func() S`
closure. Cute, but it doesn't actually save the Load when the
subscriber needs state (which is the majority case), and it
complicates Mock'ing in tests. Direct passing is simpler.

### Make the codec optional, fall back to raw envelopes

Rejected. The constructor signature would gain a default-nil codec;
subscribers would have a `events []E` slice that's nil unless the
codec is wired. Adds a "two ways to use the framework" axis where
one would do. The codec is always available in practice (every
aggregate has one already).

### Decoder-emitted compensation plans (revival from ADR 0025)

Still deferred. Per-subscriber Compensate stays the saga shape;
ADR 0029 only changes the unit it receives. A future
Decider.Compensate emitting a multi-event plan can layer on top —
the per-batch Compensate is just the per-subscriber fallback in
that hypothetical design.

## Implementation Plan

This ADR is implemented in one PR — no phasing — because the change
is fundamentally a signature swap propagated through every layer.

1. `cmdworkflow` package — `Subscriber[S, C, E]`, `Workflow[S, C, E]`,
   `New[S, C, E](rt, store, wf, codec)`, `AggregateRunner` interface
   extended with `EncodeState` / `DecodeState`, `SubscriberDLQRow`
   with `EventIDs []string` + `TypeURLs []string`.
2. `aggregate.Runtime` — `EncodeState` / `DecodeState` methods
   delegating to `StateCodec`.
3. Storage adapters — new migration drops + recreates
   `subscriber_dlq` with array columns (Postgres) / JSON columns
   (SQLite). sqlc regenerated. Operator surface (`ListSubscriberDLQ`,
   `DeleteSubscriberDLQRow`) keyed on `first_event_id`.
4. `proto-gen` — emits `Workflow[S, C, E]` (Restate + DBOS).
5. Examples — `examples/cmdworkflow{,-dbos,-restate}` rewritten with
   the new shape; ReadModel projection shrunk to its new minimal
   form.
6. Cookbook 14 snippets refreshed to the `New[S, C, E](..., codec)`
   shape.

## See also

- ADR 0023 — state_cache supersedes snapshots; explains why the
  framework has post-Decide state in memory at fan-out time.
- ADR 0025 — workflow-orchestrated command bus; the parent ADR
  whose Subscriber contract this ADR amends.
- ADR 0026 — workflow adapters; the runtime layer that this ADR's
  changes pass through unchanged.
- `examples/cmdworkflow/subscribers.go` — minimal worked examples
  for the three subscriber kinds under the new shape.
