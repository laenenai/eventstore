# 22: Sync read models with Subscriber

When a caller needs **read-after-write** on a denormalised view —
operator search after creating a customer, dashboard refresh after
issuing a command, anywhere "I just changed X, now show me X" must
work on the same screen — the right primitive is a Sync
`cmdworkflow.Subscriber[S, C, E]`.

ADR 0029 covers the design rationale (per-command batch delivery,
state + events passed together, batch-level retry / DLQ /
Compensate). This recipe is the **applied form**: here's the table
shape, the handler shape, the upsert pattern, and the wiring.

## When this fits (and when it doesn't)

| Need | Reach for |
| --- | --- |
| Read-after-write on a denormalised view in the same Postgres | **This recipe** |
| Filtered/joined/aggregated read over `state_cache` with eventual consistency | Cookbook 07 (materialized views) |
| Mirror current state to an external system (Elasticsearch, CDC) | Cookbook 13 (state_stream) |
| Derive new events / routing one aggregate's effects to another | Cookbook 12 (linked projections) |
| Audit log — one row per event, never coalesced | Subscriber with event-iteration in Handle (see end of this recipe) |

The Subscriber's superpower for THIS recipe: it runs as a journaled
DBOS / Restate step **inside the same workflow as the command's
HandleCmd**. Caller's `HandleCmd` returns *after* the step settles.
The view has the row by the time control returns to the caller.

## The pattern, in five blocks

### Block 1 — table with a `state_version` idempotency guard

```sql
CREATE TABLE customer_search (
    tenant_id      TEXT   NOT NULL,
    public_id      TEXT   NOT NULL,
    display_name   TEXT   NOT NULL DEFAULT '',
    primary_email  TEXT   NOT NULL DEFAULT '',
    search_tsv     TSVECTOR
        GENERATED ALWAYS AS (
            to_tsvector('simple', display_name || ' ' || primary_email)
        ) STORED,
    state_version  BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, public_id)
);
CREATE INDEX customer_search_tsv_idx
    ON customer_search USING GIN (search_tsv);
CREATE INDEX customer_search_email_idx
    ON customer_search (tenant_id, primary_email);
```

The `state_version` column is the idempotency guard. At-least-once
delivery means a re-tried Handle may run twice for the same
`(stream, version)` pair. The version-guarded UPSERT (Block 4) makes
the second invocation a no-op.

### Block 2 — the Subscriber value

```go
import (
    "github.com/laenenai/eventstore/cmdworkflow"
    "github.com/laenenai/eventstore/es"
)

func SearchSubscriber(pool *pgxpool.Pool) cmdworkflow.Subscriber[
    *customerv1.Customer,
    customerv1.Command,
    customerv1.Event,
] {
    return cmdworkflow.Subscriber[
        *customerv1.Customer,
        customerv1.Command,
        customerv1.Event,
    ]{
        Name: "customer_search",
        Filter: cmdworkflow.EventFilter{
            // Only fire on events that change a searchable column.
            // Anything else skips the subscriber entirely — zero
            // journal cost when an unrelated command runs.
            TypeURLs: []string{
                "myapp.customer.v1.Created",
                "myapp.customer.v1.DisplayNameSet",
                "myapp.customer.v1.PrimaryEmailChanged",
            },
        },
        Mode:        cmdworkflow.Sync,   // HandleCmd waits
        MaxRetries:  3,
        OnExhausted: cmdworkflow.DLQ,    // surface in subscriber_dlq
        Handle:      handleSearchUpsert(pool),
    }
}
```

Three knobs that matter:

- **`Filter.TypeURLs`** — list every TypeURL whose payload (or
  whose effect on state) changes a column you read. If the
  command emits an unrelated event, the framework skips the
  subscriber: zero journal entries, zero retry budget consumed.
- **`Mode: Sync`** — what makes this "read-after-write". With
  `Async`, the subscriber runs as a spawned child workflow and
  the caller's `HandleCmd` doesn't wait.
- **`OnExhausted: DLQ`** — failed batches land in `subscriber_dlq`
  with a row carrying `event_ids[]` + `type_urls[]` for triage. See
  ADR 0029 for the DLQ schema. Alternatives are `Drop` (silent
  discard — analytics where loss is fine) and `Compensate`
  (saga-style rollback).

### Block 3 — state-based vs event-based Handle

The Subscriber's `Handle` receives both `state` and `events`. For
a denormalised view of current state, **use `state`**:

```go
func handleSearchUpsert(pool *pgxpool.Pool) func(
    ctx context.Context,
    envs []es.Envelope,
    state *customerv1.Customer,
    events []customerv1.Event,
) error {
    return func(
        ctx context.Context,
        envs []es.Envelope,
        state *customerv1.Customer,
        _ []customerv1.Event, // ignored — state is the truth
    ) error {
        if len(envs) == 0 || state == nil {
            return nil
        }
        last := envs[len(envs)-1]
        return upsertRow(ctx, pool, last, state)
    }
}
```

Why state-based: maintaining a view from deltas means duplicating
the Decider's `Evolve` inside your subscriber. That bug-prone
duplication is exactly what `state` saves you from. The Decider has
already computed the truth — you just persist a queryable shape.

Use **event-iteration** when each event matters individually:

```go
// Event-log subscriber — one row per event
Handle: func(ctx context.Context, envs []es.Envelope, _ *customerv1.Customer, events []customerv1.Event) error {
    for i, evt := range events {
        if err := writeAuditRow(ctx, pool, envs[i], evt); err != nil {
            return err
        }
    }
    return nil
}
```

Audit logs, derived-event publishers, change-log views — these need
per-event semantics. State alone won't tell you "the email was
changed at v=7".

### Block 4 — the version-guarded UPSERT

```go
func upsertRow(ctx context.Context, pool *pgxpool.Pool, env es.Envelope, state *customerv1.Customer) error {
    _, err := pool.Exec(ctx, `
        INSERT INTO customer_search
            (tenant_id, public_id, display_name, primary_email, state_version)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (tenant_id, public_id) DO UPDATE
        SET display_name  = EXCLUDED.display_name,
            primary_email = EXCLUDED.primary_email,
            state_version = EXCLUDED.state_version,
            updated_at    = NOW()
        WHERE customer_search.state_version < EXCLUDED.state_version
    `,
        env.TenantID,
        env.StreamID.ID,
        state.DisplayName,
        state.PrimaryEmail,
        int64(env.Version),
    )
    return err
}
```

The `WHERE state_version < EXCLUDED.state_version` clause is the
guard. A re-delivered older version becomes a no-op UPDATE rather
than a wrong overwrite. Cheap; expensive to debug missing.

### Block 5 — wire it onto the Workflow

```go
wf := cmdworkflow.New[*customerv1.Customer, customerv1.Command, customerv1.Event](
    runtime,                   // *aggregate.Runtime[*Customer, Command, Event]
    store,                     // es.Store
    dbosOrRestateRuntime,      // cmdworkflow.WorkflowRuntime
    customerv1.EventCodec{},   // typed event codec for envelope decode
).
    WithDLQ(store).            // store satisfies SubscriberDLQWriter
    With(SearchSubscriber(pool))
```

Order matters: `.With(...)` accepts the variadic subscriber list,
calls `Register` for each. Do it at boot, before
`dbosCtx.Launch()` — DBOS requires registration-before-launch.

## Operational properties

- **Read-after-write** for the originating call (Sync mode).
- **At-least-once** delivery: state_version guard absorbs replays.
- **Failure-isolated**: the row's UPSERT runs in its own transaction
  (not the event Append's). A projection write that fails does not
  roll back the event. The retry / DLQ policy handles it.
- **Backpressure-free**: sync subscribers don't queue. If the
  upsert is slow, the caller's HandleCmd is slow — which is the
  right signal that something is wrong.
- **Crash-recoverable**: the underlying DBOS/Restate workflow
  journal records each step. A process crash mid-step replays the
  same step on recovery; the state_version guard makes the replay
  idempotent.

## Anti-patterns

- **Multiple subscribers competing for the same row.** Two Sync
  subscribers writing to the same view's row will race under
  concurrent commands. One subscriber per table. If you need two
  consumers, split into two tables.

- **Side effects in `Handle`.** Sending emails, publishing to
  external queues, charging payments — none belong here. The
  subscriber should be a pure UPSERT-from-state. Side effects go
  through a saga step or an Async subscriber wrapping a real
  notification service.

- **Skipping `state_version`.** Tempting to leave it out for
  brevity. Don't. The at-least-once delivery contract is real;
  duplicates will arrive; without the guard, an older version
  can silently overwrite a newer one.

- **Reading the view inside `Handle`.** Reads of the projection
  inside the writer create read-write cycles that destroy the
  performance story. The projection is for *callers* to read, not
  the subscriber.

- **Sync mode for fire-and-forget work.** If the caller doesn't
  need read-after-write, the Sync wait is wasted latency. Use Async.

## Failure modes worth understanding

- **Pool exhausted under concurrent commands.** Each Sync
  subscriber's UPSERT consumes one connection from the pool while
  the command's append already holds another. Pool size must be
  large enough for both. A starved pool surfaces as Handle
  timeouts → retries → DLQ.

- **Schema migration mid-flight.** If you ALTER the projection's
  table while writes are in flight, OldHandler can run against
  NewTable or vice versa. The framework has no opinion on this —
  bake migration into your deploy story (back-compat-friendly
  ALTERs preferred; column-rename via expand-then-contract).

- **TypeURL added to Filter but no rebuild.** A new event type
  starts firing the subscriber, but historical events of that
  type already exist. Either rebuild the projection from events
  (cookbook 08) or accept that older streams stay at their last
  state until the next event triggers a fresh write.

- **The Decider's Evolve diverges from the projection's columns.**
  If you ever find yourself thinking "the projection shows X but
  state.X is Y", the bug is in the projection — state is the
  source of truth, the projection is a denormalisation. Fix the
  projection; never patch the discrepancy by reading from Evolve
  output instead of state.

## See also

- ADR 0029 — Per-command Subscriber batch delivery (the design).
- Cookbook 07 — Read models via materialized views (eventually-
  consistent alternative).
- Cookbook 08 — Rebuilding projections (truncate-and-replay).
- Cookbook 12 — Linked projections / derived streams.
- Cookbook 13 — state_stream — coalesced state-mirror delivery
  (external systems).
- Cookbook 14 — cmdworkflow deployment (where Subscriber's
  delivery semantics show in DBOS / Restate dashboards).
