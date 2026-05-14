# Architecture Overview

End-to-end picture of how the framework fits together and slots into a
broader application landscape (typified here by a bank).

Diagrams reflect the architecture as decided by ADR 0011 (snapshots,
superseded), 0012 (event delivery), 0014 (outbox shape), 0020
(projections and read models), 0022 (linked projections), 0023
(state_cache subsumes snapshots), and 0024 (state_stream). Where a
diagram references a piece that isn't fully shipped yet (e.g. the
NATS adapter), it is called out inline.

## 1. Write path — one transaction across events, state_cache, outbox

The command path inside a single domain service. Everything between
the dashed lines commits atomically; there is no eventual consistency
between events, current state, and outbox enqueue.

```
                   Caller (HTTP / gRPC / in-proc)
                              │
                              ▼  command
                ┌─────────────────────────────────┐
                │      aggregate.Runtime          │
                │                                 │
                │  Load:  reads state_cache       │
                │         when StateCodec set;    │
                │         on state_schema_version │
                │         match → use as base +   │
                │         tail-replay only.       │
                │         Mismatch / no codec →   │
                │         full event replay.      │
                │                                 │
                │  Decide → new events            │
                │                                 │
                │  Handle → Store.Append          │
                └─────────────────────────────────┘
                              │
   ┌──────────────────────────┴───────────────────────────────────┐
   │      Postgres / SQLite          —— SINGLE TRANSACTION ——     │
   │                                                              │
   │   ┌──────────────┐   ┌────────────────┐   ┌──────────────┐   │
   │   │   events     │   │  state_cache   │   │    outbox    │   │
   │   │  append-only │   │     upsert     │   │    insert    │   │
   │   │              │   │                │   │              │   │
   │   │  envelope +  │   │ tenant_id,     │   │ (tenant_id,  │   │
   │   │  payload     │   │ stream_id,     │   │  global_pos, │   │
   │   │              │   │ type_url,      │   │  event_id)   │   │
   │   │  source of   │   │ state (JSONB), │   │              │   │
   │   │  truth       │   │ version,       │   │  no payload  │   │
   │   │              │   │ state_schema_  │   │  — JOIN to   │   │
   │   │              │   │ version,       │   │  events at   │   │
   │   │              │   │ terminal       │   │  publish     │   │
   │   │              │   │  (Tier 1)      │   │  (ADR 0014)  │   │
   │   └──────────────┘   └────────────────┘   └──────────────┘   │
   └──────────────────────────────────────────────────────────────┘
```

Invariants worth naming:

- `events` is canonical history. `state_cache` is canonical current
  state per stream. They never disagree because they commit together.
- `outbox` carries only a reference; the publish drain JOINs to
  `events` for the full envelope. Keeps the events table free of
  update churn.
- Snapshots are gone; `state_cache` carries `state_schema_version` and
  serves Load directly (ADR 0023).

## 2. Read & delivery paths — three tiers + two drains

Everything below the write tx runs asynchronously, on its own
schedule, with its own coordination. Multiple consumers share the
same source-of-truth tables but never block writers.

```
                       Domain DB (PG / SQLite)
                                │
        ┌───────────────────────┼─────────────────────────┐
        │                       │                         │
        ▼                       ▼                         ▼
  ┌────────────┐         ┌──────────────┐         ┌─────────────────┐
  │ Tier 1     │         │ Tier 2       │         │ Tier 3          │
  │            │         │              │         │                 │
  │ GetState,  │         │ Postgres     │         │ projection.     │
  │ ListStates │         │ MATERIALIZED │         │ Runtime         │
  │            │         │ VIEWs over   │         │                 │
  │ sync, in-  │         │ state_cache  │         │ codegen'd       │
  │ DB query   │         │              │         │ dispatcher per  │
  │ on state_  │         │ scheduled    │         │ aggregate;      │
  │ cache      │         │ REFRESH      │         │ global-cursor   │
  │            │         │ CONCURRENTLY │         │ checkpoint;     │
  │ Read-your- │         │              │         │ fail-stop on    │
  │ writes     │         │ App-owned    │         │ handler error;  │
  │ guaranteed │         │ SQL          │         │ shards + locker │
  │            │         │ (ADR 0021)   │         │                 │
  │            │         │              │         │ Tier 3.5 =      │
  │            │         │              │         │ LinkedProjection│
  │            │         │              │         │ (emits derived  │
  │            │         │              │         │  event streams; │
  │            │         │              │         │  ADR 0022)      │
  └────────────┘         └──────────────┘         └─────────┬───────┘
                                                            │
                                                            ▼
                                                   App-owned read
                                                   model tables


           Domain DB                              Domain DB
               │                                      │
               ▼                                      ▼
   ┌─────────────────────────┐            ┌─────────────────────────┐
   │     outbox.Drain        │            │   state_stream.Drain    │
   │                         │            │                         │
   │ pg_try_advisory_lock,   │            │ separate lock keyspace  │
   │ shards, scheduled       │            │ (FNV-prefixed)          │
   │                         │            │                         │
   │ • JOIN outbox → events  │            │ • SELECT state_cache    │
   │   for envelope          │            │   LEFT JOIN state_      │
   │ • one-row-per-event     │            │   stream_subscribers    │
   │ • per-event delivery    │            │   ON name = $sub        │
   │   (full history)        │            │ • WHERE last_delivered_ │
   │                         │            │   version < sc.version  │
   │ ADR 0014                │            │ • COALESCED — one       │
   │                         │            │   delivery per stream   │
   │                         │            │   regardless of how     │
   │                         │            │   many states piled up  │
   │                         │            │ ADR 0024                │
   └────────────┬────────────┘            └────────────┬────────────┘
                │ Envelope                             │ StateEnvelope
                ▼                                      ▼
   ┌─────────────────────────┐            ┌─────────────────────────┐
   │     EventPublisher      │            │     StatePublisher      │
   │                         │            │                         │
   │ Adapters:               │            │ Adapters:               │
   │  • publisher/inproc     │            │  • inproc               │
   │  • publisher/restate    │            │  • NATS (planned)       │
   │  • NATS JetStream       │            │  • downstream PG sink   │
   │    (planned)            │            │  • Elasticsearch /      │
   │  • Kafka (planned)      │            │    Algolia (planned)    │
   │                         │            │  • webhook              │
   │ Per-subscriber retry,   │            │                         │
   │ visibility, ordering    │            │ Subscribers choose ONE  │
   │ owned by the publisher  │            │ mode — event-shaped or  │
   │ runtime (ADR 0012)      │            │ state-shaped, not both  │
   └────────────┬────────────┘            └────────────┬────────────┘
                │                                      │
                └──────────────────┬───────────────────┘
                                   ▼
                          Cross-domain bus +
                          downstream sinks
```

Notes on the dual-drain design:

- **One outbox row per event, not per subscriber.** Fan-out is the
  publisher runtime's job (Restate handler retries, NATS consumer
  redelivery, etc.).
- **state_stream stores nothing extra** — it reads state from
  state_cache and tracks per-subscriber position in a sibling table.
  Coalescing is structural, not configured.
- **No cross-mode atomicity.** A subscriber that needs both event
  history and state mirror runs two subscriptions.

## 3. Multi-domain landscape — per-domain event stores, federated via broker

For a bank-shaped landscape, each bounded context (retail, cards,
payments, KYC, etc.) runs its own service with its own event store.
Cross-domain coupling is *only* through versioned integration events
on a shared broker. No shared databases, no cross-domain SQL joins.

```
   ╔══════════════════════════════════════════════════════════════════╗
   ║          NATS JetStream backbone (planned default adapter)       ║
   ║          accounts (tenancy)  +  leaf nodes (region/entity)       ║
   ║                                                                  ║
   ║          Swappable: Kafka / Restate / inproc via the same        ║
   ║          EventPublisher + StatePublisher interfaces              ║
   ╚══════════════════════════════════════════════════════════════════╝
       ▲           ▲           ▲           ▲                 │
       │ pub       │ pub       │ pub       │ pub             │ sub
       │ events    │ events    │ events    │ events          │
       │ + state   │ + state   │ + state   │ + state         │
       │           │           │           │                 │
   ┌───┴─────┐ ┌───┴─────┐ ┌───┴─────┐ ┌───┴─────┐  ┌────────▼─────┐
   │ Retail  │ │ Cards   │ │Payments │ │ KYC /   │  │ Risk /       │
   │ domain  │ │ domain  │ │ domain  │ │ AML     │  │ Analytics    │
   │  (svc)  │ │  (svc)  │ │  (svc)  │ │  (svc)  │  │  (svc)       │
   │         │ │         │ │         │ │         │  │              │
   │ aggrt.  │ │ aggrt.  │ │ aggrt.  │ │ aggrt.  │  │ broker-      │
   │ Runtime │ │ Runtime │ │ Runtime │ │ Runtime │  │ source       │
   │         │ │         │ │         │ │         │  │ projections  │
   │ Tier    │ │ Tier    │ │ Tier    │ │ Tier    │  │ (cross-      │
   │ 1/2/3   │ │ 1/2/3   │ │ 1/2/3   │ │ 1/2/3   │  │ domain)      │
   │ projs   │ │ projs   │ │ projs   │ │ projs   │  │              │
   │         │ │         │ │         │ │         │  │ own PG +     │
   │ own PG  │ │ own PG  │ │ own PG  │ │ own PG  │  │ checkpoint   │
   └─────────┘ └─────────┘ └─────────┘ └─────────┘  └──────────────┘

   Derived sinks (broker subscribers, platform-team owned):
   ┌──────────────────────────┐    ┌──────────────────────────┐
   │ Regulatory audit         │    │ Data lake / warehouse    │
   │  (WORM, jurisdiction-    │    │  (analytics, BI, ML)     │
   │   specific retention)    │    │                          │
   └──────────────────────────┘    └──────────────────────────┘

   External channels (subscribe via dedicated NATS accounts):
       customer notifications · partner APIs · vendor systems ·
       core banking integrations
```

Why per-domain, not central:

- **Blast radius.** A central event store outage stops the bank. Per-
  domain confines failures to one bounded context.
- **Schema evolution pace.** Different domains evolve on different
  cycles. Central authority becomes a coordination bottleneck.
- **Regulatory boundaries.** Data residency, separation of duties,
  audit boundaries align with legal entities and jurisdictions, not
  with a single shared database.
- **Vendor reality.** Ledger, cards, payments rails are partly
  vendor/mainframe. A central event store can never own them; per-
  domain integrates with the actual landscape.
- **The Risk / Analytics service shown above** runs the *same*
  framework — it just uses the broker-source adapter for its
  projections instead of the local-source adapter. Same code path
  for both.

## 4. Projection source-adapter — same handler, swappable source

Intra-domain and cross-domain projections share one handler shape.
The difference is one adapter: where the events come from. The handler
code, the codegen, the checkpoint mechanism, and the read-model
upsert pattern are identical.

```
                ┌────────────────────────────────────────────┐
                │  Codegen'd Projection interface (per       │
                │  aggregate)                                │
                │                                            │
                │  user implements:                          │
                │     OnCreated(ctx, env, *Created) error    │
                │     OnPaid   (ctx, env, *Paid)    error    │
                │     ...                                    │
                │                                            │
                │  identical regardless of source            │
                └────────────────────────────────────────────┘
                                  ▲
                                  │ typed events
                                  │
              ┌───────────────────┴──────────────────────┐
              │                                          │
        ┌─────┴─────────────┐                ┌───────────┴─────────┐
        │   LocalSource     │                │    BrokerSource     │
        │   (intra-domain)  │                │    (cross-domain)   │
        │                   │                │                     │
        │  reads from:      │                │  subscribes to:     │
        │   • events table  │                │   • NATS subject    │
        │     in the same   │                │     (or Kafka topic,│
        │     DB as the     │                │     Restate handler)│
        │     read model    │                │                     │
        │                   │                │  durable consumer   │
        │  → read + read-   │                │  name owned by      │
        │    model upsert + │                │  consumer service   │
        │    checkpoint ALL │                │                     │
        │    in ONE TX      │                │  → read + read-     │
        │                   │                │    model upsert +   │
        │  transactionally  │                │    checkpoint in    │
        │  exactly-once at  │                │    ONE TX in the    │
        │  the read model   │                │    consumer's DB    │
        └───────────────────┘                └─────────────────────┘
                  │                                       │
                  └────────────────────┬──────────────────┘
                                       ▼
              ┌───────────────────────────────────────────────┐
              │  Read model + projection_checkpoint           │
              │  always in the SAME database — never spans    │
              │  source DB and sink DB.                       │
              │                                               │
              │     BEGIN;                                    │
              │       UPSERT read_model ...;                  │
              │       UPDATE projection_checkpoint ...;       │
              │     COMMIT;                                   │
              └───────────────────────────────────────────────┘
```

The load-bearing property: checkpoint always commits in the same tx
as the read-model write, in the consuming service's DB. That makes
"exactly-once at the read model" achievable without exotic broker
semantics — at-least-once on the wire plus idempotent upserts is
sufficient.

## 5. Runtime topology — what runs in each pod

A domain service pod runs all of the following side by side. They
share the domain's Postgres but coordinate via advisory locks and
distinct lock keyspaces, so multi-pod deployments are safe without
external coordination.

```
   ┌──────────────────────── Domain service pod ────────────────────────┐
   │                                                                    │
   │  ┌──────────────────┐       ┌──────────────────────────┐           │
   │  │  API handlers    │──cmd──▶│   aggregate.Runtime     │           │
   │  │  (HTTP / gRPC)   │       │                          │           │
   │  └──────────────────┘       │   Load → Decide →        │           │
   │                             │   Append (one tx writes  │           │
   │                             │   events + state_cache + │           │
   │                             │   outbox)                │           │
   │                             └──────────┬───────────────┘           │
   │                                        │                           │
   │                                        ▼                           │
   │                  ┌───────────────────────────────────────────┐     │
   │                  │      Domain Postgres / SQLite             │     │
   │                  │                                           │     │
   │                  │  events / state_cache / outbox /          │     │
   │                  │  state_stream_subscribers /               │     │
   │                  │  projection_checkpoint / app read-models  │     │
   │                  └───────────────────────────────────────────┘     │
   │                          ▲      ▲       ▲       ▲                  │
   │                          │      │       │       │                  │
   │  ┌────────────────────┐  │      │       │       │                  │
   │  │ Tier 3 projection  │──┘      │       │       │                  │
   │  │ runner pool        │         │       │       │                  │
   │  │  • shard leases    │         │       │       │                  │
   │  │  • checkpoint      │         │       │       │                  │
   │  │  • DLQ-skip / dedup│         │       │       │                  │
   │  │  • LocalSource (in │         │       │       │                  │
   │  │    this pod) or    │         │       │       │                  │
   │  │    BrokerSource    │         │       │       │                  │
   │  │    (for cross-     │         │       │       │                  │
   │  │    domain projs)   │         │       │       │                  │
   │  └────────────────────┘         │       │       │                  │
   │                                 │       │       │                  │
   │  ┌────────────────────┐         │       │       │                  │
   │  │ outbox.Drain       │─────────┘       │       │                  │
   │  │  • advisory lock   │                 │       │                  │
   │  │  • shards          │   Envelope      │       │                  │
   │  │  • scheduled       │──▶ EventPublisher       │                  │
   │  └────────────────────┘    (NATS, Restate,      │                  │
   │                             Kafka, …)           │                  │
   │  ┌────────────────────┐                         │                  │
   │  │ state_stream.Drain │─────────────────────────┘                  │
   │  │  (separate lock    │                                            │
   │  │   keyspace)        │   StateEnvelope                            │
   │  │                    │──▶ StatePublisher                          │
   │  └────────────────────┘    (NATS, downstream PG,                   │
   │                             Elasticsearch, …)                      │
   │  ┌────────────────────┐                                            │
   │  │ Restate workers    │── command back into aggregate.Runtime      │
   │  │  • sagas / process │                                            │
   │  │    managers        │                                            │
   │  │  • scheduled timers│                                            │
   │  │  • durable publish │                                            │
   │  │    (acts as Event- │                                            │
   │  │    Publisher for   │                                            │
   │  │    outbox drain)   │                                            │
   │  └────────────────────┘                                            │
   │                                                                    │
   └────────────────────────────────────────────────────────────────────┘

   N pods → each runs all of: API handlers, projection runner,
            outbox.Drain, state_stream.Drain, Restate worker(s).
            Advisory locks (one keyspace per concern) ensure single-
            active-per-shard at any given time.
```

How Restate fits, precisely:

- **Restate is the orchestration / workflow runtime.** It owns durable
  execution for sagas, scheduled work, and journaled outbox publish
  (cookbook recipe 10).
- **Restate is *not* the projection runtime.** The framework's own
  `projection.Runtime` with lease + shard + checkpoint is purpose-
  built for high-throughput read-model writes against the local DB.
  Putting projection feeding through Restate would lose the
  transactional checkpoint property and add latency.
- **Restate calls aggregate.Runtime, not the database.** Saga steps
  issue commands; commands go through the same Load/Decide/Append
  path as API-triggered commands. Restate sees commands and events,
  not raw rows.
- **Restate as EventPublisher** is the high-value first integration
  even if you're not using it for sagas yet: it gives you durable,
  journaled, retried publish out of the box.

## 6. Workflow-orchestrated command bus (Profile B recommended topology)

Polling-based projection delivery (sections 2 and 5) is the right model
for Profile A — always-on Postgres where keeping a process awake is
free. Profile B (scale-to-zero DBs) wants a different shape: **the
command itself is a durable workflow, and projection subscribers
register on the workflow as fan-out steps.** No projection polling,
no outbox drain on the live path, no separate `state_stream` consumer
loop. The eventstore commits and goes to sleep; the workflow runtime
(Restate or DBOS) is the always-on layer that fans events out.

```
                            Caller
                              │
                              ▼  command (proto)
                ┌──────────────────────────────┐
                │   Connect-go service entry   │
                │   (codegen'd from .proto)    │
                └──────────────┬───────────────┘
                               │ invokes
                               ▼
   ╔══════════════════════════════════════════════════════════════╗
   ║              Durable workflow (Restate / DBOS)               ║
   ║                                                              ║
   ║   HandleCmd(ctx, streamID, cmd) :                            ║
   ║                                                              ║
   ║   ┌──────────────────────────────────────────────────┐       ║
   ║   │  Step 1:  ctx.Run("append", …)                   │       ║
   ║   │           → aggregate.Runtime.Handle             │       ║
   ║   │           → events, state, version               │       ║
   ║   │     (Postgres / SQLite wakes, commits, sleeps)   │       ║
   ║   └──────────────────────────────────────────────────┘       ║
   ║                       │                                      ║
   ║                       ▼  for each event × matched subscriber ║
   ║   ┌──────────────────────────────────────────────────┐       ║
   ║   │  Step 2..N: ctx.Run("<sub>:<event_id>", …)       │       ║
   ║   │                                                  │       ║
   ║   │  Subscriber registry (declarative):              │       ║
   ║   │   • Name        — journal id prefix              │       ║
   ║   │   • Filter      — TypeURLs / StreamGlob /        │       ║
   ║   │                   Tenants / Custom predicate     │       ║
   ║   │   • Handle      — idempotent upsert              │       ║
   ║   │   • Mode        — Sync | Async                   │       ║
   ║   │   • MaxRetries  — 0..N, or -1 (forever)          │       ║
   ║   │   • OnExhausted — DLQ | Compensate | Drop        │       ║
   ║   │                                                  │       ║
   ║   │  Filter evaluated BEFORE ctx.Run — unmatched     │       ║
   ║   │  subscribers cost zero journal entries.          │       ║
   ║   └──────────────────────────────────────────────────┘       ║
   ║                                                              ║
   ║   Returns (state, error) to caller once all Sync             ║
   ║   subscribers settle (success, DLQ, or compensation);        ║
   ║   Async subscribers continue as child workflows.             ║
   ╚══════════════════════════════════════════════════════════════╝
            │                  │                    │
            ▼ Connect-go        ▼ Connect-go         ▼ NATS / Connect
   ┌──────────────────┐ ┌──────────────────┐  ┌──────────────────┐
   │ Read-model svc   │ │ Search-index svc │  │ Cross-domain     │
   │ (own Postgres)   │ │ (Typesense)      │  │ bus (events to   │
   │                  │ │                  │  │ other services)  │
   │ idempotent       │ │ idempotent       │  │                  │
   │ UPSERT by stream │ │ upsert by stream │  │ sagas, audit,    │
   │                  │ │                  │  │ analytics        │
   └──────────────────┘ └──────────────────┘  └──────────────────┘
```

Three responsibilities, three layers — kept distinct on purpose:

| Layer | Role | Implementation |
| ----- | ---- | -------------- |
| **Transport** | Typed request/response over HTTP, browser-callable | `connect-go` services codegen'd from the same `.proto` files as commands/events |
| **Orchestration** | Durable journaled steps, retry, fan-out, sagas | Restate (default) / DBOS — code uses `ctx.Run(name, fn)` |
| **State of truth** | Append-only log + Tier-1 current state | The framework's `aggregate.Runtime` + adapters |

### Subscriber semantics — three knobs, not a binary

Each subscriber declares three independent properties at registration.
The combination encodes a wide range of intents — from "saga step the
command must wait on" to "best-effort webhook we don't care about".

| Knob | Values | Meaning |
| ---- | ------ | ------- |
| `Mode` | `Sync` / `Async` | Whether `HandleCmd` blocks on this subscriber before returning to the caller. |
| `MaxRetries` | `0..N` / `-1` | Failure budget before the subscriber is considered exhausted. `-1` = retry forever (workflow re-runs the step on every replay until it succeeds). |
| `OnExhausted` | `DLQ` / `Compensate` / `Drop` | What happens when `MaxRetries` is reached. DLQ writes a row for operator action; Compensate emits a compensating command into `HandleCmd` (saga rollback); Drop silently abandons. |

Five common combinations cover almost every real use case:

| Subscriber kind | `Mode` | `MaxRetries` | `OnExhausted` | Why |
| --------------- | ------ | ------------ | ------------- | --- |
| **Local read-model UPSERT** (own Postgres) | Sync | 3 | DLQ | Read-your-writes for this projection. UPSERT into the same DB should rarely fail; bounded retry catches transient errors. Operator triages anything that lands in DLQ. |
| **Saga step** (reserve inventory, charge card) | Sync | 5 | Compensate | Command success depends on this step. If exhausted, the workflow emits a compensating command (`Cancel`, `Refund`) back into `HandleCmd`, which appends a compensating event. Classic saga rollback. |
| **State mirror to search index** (Typesense, Algolia) | Async | -1 | DLQ | Best-effort durable. The workflow retries forever in the background; only a permanent failure (e.g., subscriber decommissioned) needs DLQ. Recovery is `state_stream.Drain` against the subscriber's target, not DLQ replay. |
| **Audit / analytics fan-out** | Async | 10 | DLQ | Fire-and-forget durable. DLQ surfaces gaps the audit pipeline must reconcile. Replay is event-shaped, since audit cares about history not current state. |
| **Nice-to-have webhook** | Async | 3 | Drop | We don't care if it's lost. Three quick tries; if the webhook target is down, move on. |

Two recovery paths drop out of this naturally:

- **DLQ replay** (event-shaped subscribers, e.g., audit) — operator inspects the DLQ rows and replays them. Same mechanism as the existing `projection_dlq` table on both adapters.
- **State refresh via `state_stream`** (state-shaped subscribers, e.g., search indexes) — operator fixes the underlying issue, then runs `state_stream.Drain` for that subscriber. The drain reads `state_cache` and pushes current state, coalescing any missed deltas into one delivery per stream (ADR 0024). DLQ entries can be cleared en masse since the state is now correct.

The Compensate path is what makes "sync subscriber + saga semantics"
clean: the framework doesn't pretend the eventstore commit can be
undone — it can't, the events are durable. Instead, the workflow
emits a *new* command that the aggregate's `Decide` translates into a
compensating event. The audit trail shows both the original action
and the compensation; you don't get the illusion that nothing
happened.

### Why this is the Profile B answer

- **DB stays asleep between commands.** The workflow runtime is what stays alive; the eventstore commits and suspends. No projection poll loops keeping it awake.
- **One generic command handler.** `HandleCmd[S, C, E]` is framework code, not per-aggregate code. Adding a new aggregate adds an entry in a router, not a new workflow.
- **Declarative subscriptions.** Adding a Typesense mirror, an audit sink, a SaaS-side webhook = `bus.Register(Subscriber{Name, Filter, Handle})`. No workflow rewrite, no new pipeline.
- **Per-subscriber durability without dual-write risk.** Each subscriber's update is an independently-journaled step. Eventstore is the source of truth; subscribers derive from it. If one is down for an hour, the workflow keeps retrying that one step until it succeeds — eventstore state is unaffected.
- **Sagas are just subscribers.** A saga is a Subscriber whose `Handle` emits more commands (back into `HandleCmd`). Same registration mechanism, same observability surface, same journal.
- **Backfill stays clean.** `state_stream.Drain` keeps its role: when a new subscriber is added, point it at `state_cache` for one-shot backfill, then let the workflow take over for live events. Section 2's drains are not deleted; they become the *catch-up* path while the workflow is the *live* path.

### Why you might NOT pick this

- **You're on Profile A.** Always-on Postgres + a single-service codebase. The polling projector (section 2 / section 5) is simpler, has no external dependency, and is plenty fast. The workflow layer is overkill.
- **You need fan-out so large that journaling every step is wasteful.** 100 subscribers × 1000 events per command = 100k journal entries per command. At that scale, push the bulk into NATS as a single workflow step and let NATS handle the fan-out.
- **Every subscriber has to declare its mode + retry + exhaustion explicitly.** That's the cost of expressing this richly: there is no "right default for everything". A team that hasn't internalized the matrix above will misclassify subscribers (treating an audit pipeline as `Drop`, or a webhook as `Sync 5x Compensate`) and feel the consequences in production. The matrix is a feature, but it has a learning surface.

### Mapping to existing framework pieces

| Concern | Where it lives in the framework |
| ------- | ------------------------------- |
| Append step | `aggregate.Runtime.Handle` — already returns events + state |
| Subscriber registration | New: framework-provided `Workflow` type wraps the workflow + registry. Codegen'd registration helpers per event type. |
| Mode / MaxRetries / OnExhausted | Framework-defined per-subscriber config; the workflow runtime enforces it via `ctx.Run` retry policies and child-workflow forking. |
| Step durability | Restate/DBOS — workflow runtime, *not* the framework |
| Compensation (saga rollback) | Subscriber declares a `Compensate(env) Command`; on exhaustion the workflow loops back into `HandleCmd` with that command. |
| DLQ for exhausted subscribers | Per-subscriber rows in a `subscriber_dlq` table (sibling to existing `projection_dlq`); replay tooling matches that pattern. |
| State refresh / recovery for state-shaped subscribers | `state_stream.Drain` — unchanged. Becomes the primary recovery path for `Async + DLQ` subscribers that mirror current state. |
| Connect-go services | Codegen extension (`protoc-gen-es-go` already generates the typed event/command code; adding Connect service definitions is a mechanical extension). |
| Cross-domain fan-out | NATS / Kafka subscriber registered as one of the `Subscriber`s. |

Status: the workflow-orchestrated command bus is **architectural intent
not yet shipped**. ADR pending. The shipped primitives (aggregate
runtime, state_cache, state_stream, outbox, projection runtime) are
each independently usable today; this section describes how they
compose under a Restate/DBOS layer when that's how you choose to
deploy.

## Reference

- ADR 0003 — Decider aggregate model
- ADR 0011 — Snapshots (superseded by ADR 0023)
- ADR 0012 — Event delivery and EventPublisher
- ADR 0014 — Outbox table shape
- ADR 0020 — Projections and read models (three-tier model)
- ADR 0021 — SQLite JSONB storage
- ADR 0022 — Linked projections (Tier 3.5)
- ADR 0023 — state_cache subsumes snapshots
- ADR 0024 — state_stream — coalesced state-mirror delivery
- Cookbook 06 — Running the drain
- Cookbook 07 — Read models via materialized views
- Cookbook 08 — Rebuilding projections
- Cookbook 10 — Restate publisher
