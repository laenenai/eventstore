# 06: Running the Outbox Drain in Production

The outbox drain is the bridge between the writer's transactional
`Append` and the publisher (Restate / NATS / SNS / Pub-Sub / etc.).
It needs **exactly one drainer per work unit** at any moment, otherwise
you get duplicate publishes (subscribers are idempotent so it's
correctness-safe but wasteful).

Four patterns, ordered by deployment fit:

## (1) Scheduled trigger — the default for Profile B

For scale-to-zero deployments (Neon, Turso, Cloudflare D1), invoke
the drain from a single-fire scheduler. The drainer wakes the DB,
processes pending rows, exits. Nothing runs between ticks.

| Platform                | What you wire up                                |
| ----------------------- | ----------------------------------------------- |
| Cloudflare Workers      | [Cron Triggers](https://developers.cloudflare.com/workers/configuration/cron-triggers/) |
| AWS Lambda              | [EventBridge Scheduler](https://docs.aws.amazon.com/scheduler/) |
| GCP Cloud Run           | [Cloud Scheduler](https://cloud.google.com/scheduler/) |
| Kubernetes              | `CronJob` with `concurrencyPolicy: Forbid`      |
| Fly.io                  | [Scheduled machines](https://fly.io/docs/launch/cron/) |

Each tick guarantees one invocation; no concurrency problem.

```go
// Cloudflare Worker / Lambda / Cloud Run entrypoint
func DrainScheduled(ctx context.Context) error {
    drain := &outbox.Drain{
        Store:     store,
        Publisher: pub,
        Tenant:    os.Getenv("TENANT_ID"),  // or "" for cross-tenant
    }
    published, cleaned, err := drain.Run(ctx)
    log.Printf("drain: published=%d cleaned=%d err=%v", published, cleaned, err)
    return err
}
```

**Use this** for the framework's intended (serverless) deployment.

## (2) External leader election — for always-on Profile A

Your service has N replicas (k8s deployment, ECS service, fly app
scale-3, etc.) and you want exactly one of them to run the drain.
Use whatever leader-election mechanism you already operate; the
framework stays out of it.

### Kubernetes `Lease`

```go
import "k8s.io/client-go/tools/leaderelection"

lec := leaderelection.LeaderElectionConfig{
    Lock: &resourcelock.LeaseLock{
        LeaseMeta: metav1.ObjectMeta{Name: "outbox-drain", Namespace: ns},
        Client:    k8sClient.CoordinationV1(),
        LockConfig: resourcelock.ResourceLockConfig{Identity: podName},
    },
    LeaseDuration: 15 * time.Second,
    RenewDeadline: 10 * time.Second,
    RetryPeriod:   2 * time.Second,
    Callbacks: leaderelection.LeaderCallbacks{
        OnStartedLeading: func(ctx context.Context) {
            // We're the leader — run the drain in a loop.
            ticker := time.NewTicker(30 * time.Second)
            for {
                select {
                case <-ctx.Done(): return
                case <-ticker.C:
                    drain.Run(ctx)
                }
            }
        },
    },
}
leaderelection.RunOrDie(ctx, lec)
```

### Consul / etcd / Vault

Same shape: acquire a session-scoped lock, run while held, release on
shutdown. The framework's `Drain` is invoked from the leader's loop.

## (3) Postgres advisory lock — framework-supported

For deployments running multiple drainers against the same Postgres
DB (long-running processes, no external leader-election infra),
set `Drain.LockKey`. The framework's Postgres adapter implements
`es.DrainLocker` via `pg_try_advisory_lock`:

```go
drain := &outbox.Drain{
    Store:     store,
    Publisher: pub,
    Tenant:    "",
    LockKey:   "outbox-drain", // recommended: stable string per drainer purpose
}

// All N replicas can call drain.Run() concurrently.
// pg_try_advisory_lock ensures only one acquires per tick.
// Losers return (0, 0, nil) and exit cleanly.
published, cleaned, err := drain.Run(ctx)
```

How it works:

- `drain.Run` calls `pg_try_advisory_lock(fnv1a("outbox-drain"))` at
  start. Non-blocking — returns true/false immediately.
- The winner holds a dedicated connection from the pool while
  draining. Other connections in the same pool aren't affected.
- The lock auto-releases on `drain.Run` return (success or failure)
  via `pg_advisory_unlock`. If the process crashes, the lock
  releases on connection close.
- SQLite doesn't implement `DrainLocker` (file-level write lock
  already serializes), so `LockKey` is a no-op there — same code
  works on both adapters.

Per-tenant locking:

```go
LockKey: "outbox-drain:" + tenantID
```

Lets N replicas drain N tenants concurrently — one drainer per tenant
key, no cross-tenant blocking.

## (4) SELECT FOR UPDATE SKIP LOCKED — concurrent drainers

For high-throughput stores where one drainer is the bottleneck,
multiple drainers can run **concurrently** on disjoint row subsets
using Postgres's `FOR UPDATE SKIP LOCKED`:

```sql
SELECT ... FROM outbox
WHERE tenant_id = $1 AND published_at IS NULL
ORDER BY global_position
LIMIT $2
FOR UPDATE SKIP LOCKED
```

Each drainer's transaction holds locks on its claimed rows; other
drainers see the lock and skip to the next available batch. No
explicit coordination needed.

### Pattern A: transaction-wrapped (simple but holds connection)

The full read-publish-mark sequence runs inside one transaction. The
DB connection is held for the duration of the batch (including the
network call to the publisher).

```go
tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

rows := pgxQuery(tx, "SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100", ...)
for _, r := range rows {
    if err := pub.Publish(ctx, r.Envelope); err != nil { /* keep pending */ continue }
    pgxExec(tx, "UPDATE outbox SET published_at = now() WHERE ...", ...)
}
tx.Commit(ctx)
```

Pros: one transaction per batch, atomic.
Cons: slow publisher → connection held for slow time → connection
pool exhaustion under failure.

### Pattern B: claim-then-publish (two short transactions)

A short claim transaction marks rows with `claimed_at = now()` and
returns them. The publish happens outside the transaction. A second
short transaction marks them published. TTL on `claimed_at` recovers
crashed drainers.

Requires a schema change (add `claimed_at` and `claimed_by` columns
to `outbox`), so it's not the framework default. Document this
pattern; implement on demand.

### Framework support status

The framework doesn't yet ship a built-in concurrent-drain mode. The
sqlc query for `FOR UPDATE SKIP LOCKED` is straightforward to add;
the trade-off (Pattern A vs B above) is the bigger decision. Open a
follow-up if you need this in production.

## (5) Sharded drain — set-and-forget for fixed-N deployments

When you have a known-fixed number of drainer replicas and want each
to handle a disjoint slice of the work without any coordination:

```go
// Replica 0:
&outbox.Drain{Store: store, Publisher: pub, Shard: 0, TotalShards: 3}

// Replica 1:
&outbox.Drain{Store: store, Publisher: pub, Shard: 1, TotalShards: 3}

// Replica 2:
&outbox.Drain{Store: store, Publisher: pub, Shard: 2, TotalShards: 3}
```

Sharding is **stream-sticky**: the drain assigns each row to a shard
via `FNV-1a(tenant_id|stream_id) % TotalShards`. All events of a given
stream always go to the same shard. This is what makes sharding
compatible with strict per-stream ordering — if sharding were by
`global_position`, two events in the same stream could land in
different shards and arrive out of order.

Current implementation is client-side filtering (the adapter returns
all matching rows, drain filters in Go). For large pending sets,
push-down to the SQL `WHERE` clause is straightforward to add as a
follow-up.

Rebalancing implications: if you change `TotalShards`, the hash output
shifts and many streams move between shards. Coordinate the rollout so
the old shard has fully drained its slice before the new shard count
takes over (deploy with `TotalShards=0` momentarily, or scale through
zero).

### Combining sharding with locking

Sharding + LockKey gives "one drainer per shard":

```go
&outbox.Drain{
    Store: store, Publisher: pub,
    Shard:       0, TotalShards: 3,
    LockKey:     "outbox-drain:shard-0",
}
```

Now multiple replicas can be deployed per shard for redundancy; only
one of them drains at a time.

## Choosing the right pattern

| Scenario                                         | Recommended pattern |
| ------------------------------------------------ | ------------------- |
| Profile B (Neon / Turso / D1)                    | (1) scheduled trigger |
| Profile A, k8s, want zero new infra              | (3) advisory lock + `LockKey` |
| Profile A, already running Consul/etcd/k8s Lease | (2) external leader election |
| High throughput, drain is the bottleneck         | (4) FOR UPDATE SKIP LOCKED |
| Fixed-N drainers, want trivial scale-out         | (5) sharding (+ optional `LockKey`) |

## Per-stream ordering: how the drain preserves it

The drain guarantees per-stream order under all five deployment
patterns. The mechanism is two-layered:

1. **SQL head filter.** `PendingOutbox` returns at most one row per
   stream — the lowest-version unpublished row that's not in DLQ. If
   that head row is in cooldown (failed recently, `next_attempt_at` in
   the future), the stream contributes nothing this batch. So no
   matter who's draining or when, you can't pick up row N+1 before
   row N is resolved.

2. **In-Run halt map.** Within one `drain.Run` call, when a row fails
   the drain marks the stream as halted for the rest of that run — so
   the same head row isn't retried in a tight loop on the same
   invocation (relevant when `BackoffBase=0`).

Cross-stream interleaving is unaffected: stream B's events never wait
for stream A's failure. Strict ordering is per-stream, not global.

## Failure handling: backoff, retries, DLQ

When `Publisher.Publish` returns an error, the drain doesn't drop the
event — it records the failure and decides whether to retry, wait, or
quarantine. Three knobs control this:

```go
&outbox.Drain{
    Store:     store,
    Publisher: pub,

    MaxAttempts: 5,                  // retry cap — 0 = unbounded
    BackoffBase: 30 * time.Second,   // first retry delay — 0 = retry next tick
    BackoffMax:  10 * time.Minute,   // delay ceiling
}
```

### Retry timing

After each failure the drain sets `outbox.next_attempt_at` to
`now() + min(BackoffMax, BackoffBase * 2^(attempts-1))`. `PendingOutbox`
filters by `next_attempt_at <= now()`, so cooldown'd rows simply don't
appear in the work set until eligible.

| `BackoffBase` | Behavior |
| ------------- | -------- |
| `0` (default) | Retry every tick. Suitable when retries are cheap and the publisher is local. |
| `30 * time.Second` | Exponential: 30s → 60s → 120s → 240s … capped at `BackoffMax`. |
| `BackoffMax` only | Fixed delay between every retry. |

Recommended defaults for external publishers: `BackoffBase = 1s`,
`BackoffMax = 5m`.

### DLQ threshold

When a row's `attempts >= MaxAttempts`, it has reached the dead-letter
state. The drain stops retrying it (the SQL filter excludes
`attempts >= MaxAttempts`) and the row's behavior depends on
`AutoResumeAfterDLQ`:

```go
// Default: quarantine the stream until operator action.
&outbox.Drain{MaxAttempts: 5}

// Alternative: skip the DLQ'd row and keep publishing the stream.
&outbox.Drain{MaxAttempts: 5, AutoResumeAfterDLQ: true}
```

| Mode | After a row hits DLQ | Subscriber sees |
| ---- | -------------------- | --------------- |
| `AutoResumeAfterDLQ: false` (default — **fail loud**) | The entire stream stays paused. No events from that stream are delivered until an operator runs `ReplayDLQ` or `AbandonDLQ`. | Either every event in order, or nothing — never a gap. |
| `AutoResumeAfterDLQ: true` (**accept gaps**) | The DLQ'd row is skipped; later events of the same stream proceed. | Events N-1, N+1, N+2 … (N is missing). Subscriber must tolerate gaps. |

The default is "fail loud" because in event sourcing a missing event
usually corrupts downstream state silently. Only opt into gap mode
when the consumer is robust to it (e.g. an analytics pipeline doing
idempotent upserts on each event independently).

### Observing DLQ crossings

`OnDLQ` is a callback invoked once when a row crosses the threshold —
the right hook for paging on-call, writing audit events, or bumping a
metric:

```go
&outbox.Drain{
    MaxAttempts: 5,
    OnDLQ: func(row es.OutboxRow) {
        log.Error("event quarantined",
            "tenant", row.Envelope.TenantID,
            "stream", row.Envelope.StreamID.Canonical(),
            "event_id", row.Envelope.EventID,
            "attempts", row.Attempts)
        metrics.DLQCrossed.Inc()
    },
}
```

The callback runs from the drain goroutine; keep it non-blocking.

## Operator actions: inspecting and unblocking the DLQ

Postgres and SQLite adapters both implement `es.OutboxAdmin` — the
read+mutate surface for dashboards and runbooks. Typical queries:

```go
admin := store.(es.OutboxAdmin)

// Gauges (cheap, run on a schedule)
pendingTotal,   _ := admin.CountPending(ctx, tenantID)
failingNotYet,  _ := admin.CountFailing(ctx, tenantID, maxAttempts) // attempts > 0 && < max
dlqTotal,       _ := admin.CountDLQ(ctx, tenantID, maxAttempts)

// Paginated listing for a dashboard view
rows, _ := admin.ListDLQ(ctx, tenantID, maxAttempts, afterPosition, 50)
for _, r := range rows {
    fmt.Printf("%s/%s v? gp=%d attempts=%d last_error=%s\n",
        r.TenantID, r.StreamID, r.GlobalPosition, r.Attempts, r.LastError)
}
```

### Replay a DLQ'd row

Run this after fixing the root cause (publisher config, schema, dead
subscriber):

```go
if err := admin.ReplayDLQ(ctx, tenantID, globalPosition); err != nil { ... }
```

`ReplayDLQ` resets `attempts = 0`, clears `last_error` and
`next_attempt_at`, leaving the row eligible for the next drain run.
Replay is per-row — the operator names the exact `global_position`.

For bulk replay after a publisher outage:

```go
n, err := admin.ReplayAllDLQ(ctx, tenantID, maxAttempts)
log.Info("replayed DLQ", "rows", n)
```

### Abandon a row

When the event is genuinely garbage (payload corruption, schema gap
that won't be fixed, subscriber permanently removed):

```go
if err := admin.AbandonDLQ(ctx, tenantID, globalPosition); err != nil { ... }
```

`AbandonDLQ` marks the outbox row published (without delivering) and
appends `[abandoned]` to `last_error` for audit. The underlying event
remains in the `events` table per ADR 0005 (events are immutable);
only the outbox row is closed.

After abandon, the stream resumes — the row no longer blocks
`PendingOutbox`'s head filter and downstream events flow.

### Dashboards

A useful operator dashboard surfaces:

| Panel | Source |
| ----- | ------ |
| Pending count (gauge) | `CountPending` per tenant |
| Failing-not-yet-DLQ (gauge) | `CountFailing` |
| DLQ count (gauge, alerts ≥ 1) | `CountDLQ` |
| DLQ table (paginated list) | `ListDLQ` |
| Oldest pending lag (histogram) | `min(enqueued_at)` join with `now()` — separate query |
| DLQ crossings (counter) | Incremented from `OnDLQ` callback |

The `failing` and `DLQ` gauges are the two most important alerts. A
non-zero `DLQ` means a stream is paused (quarantine mode) or a
subscriber is dropping events (auto-resume mode); a rising `failing`
gauge predicts a DLQ crossing.

## How DrainLocker interacts with publisher-level idempotency

Some publishers have their own exactly-once / dedup mechanism keyed
on a message id. When your publisher adapter passes `env.EventID`
(UUIDv7) as the idempotency key, multiple drainers publishing the
same event collapse into one subscriber invocation:

| Publisher              | Correctness without `LockKey`?                                | Notes                                                |
| ---------------------- | ------------------------------------------------------------- | ---------------------------------------------------- |
| **Restate**            | ✅ Exactly-once via `IdempotencyKey = event_id`               | Recommended pattern; framework Restate adapter sets it |
| **NATS JetStream**     | ✅ With `Nats-Msg-Id` header = event_id (dedup window)        |                                                      |
| **AWS SQS (FIFO)**     | ✅ With `MessageDeduplicationId` = event_id (5 min window)    |                                                      |
| **GCP Pub/Sub**        | ⚠️ No native publish-time dedup; subscriber dedups            | Use `ordering_key = stream_id` for stream order      |
| **Cloudflare Queues**  | ⚠️ No native dedup; subscriber dedups by event_id             |                                                      |
| **HTTP webhook**       | ❌ Subscriber receives N copies; must dedup                   |                                                      |
| **inproc**             | ❌ Subscriber invoked N times in-process                      | Tests use this; production doesn't                   |

In the ✅ cases, `LockKey` becomes an **efficiency optimization** —
it avoids wasted network round-trips and redundant DB writes, but
correctness is upheld by the publisher.

In the ⚠️ / ❌ cases, `LockKey` (or scheduled trigger, or external
leader election) is part of your **correctness** story. The
subscriber's per-event_id dedup remains the second line of defense
in all cases — at-least-once delivery means subscribers always check.

**Practical implication**: if you're using Restate (the recommended
publisher per ADR 0012), feel free to skip `LockKey` for simplicity.
For everything else, set `LockKey` or use a scheduled trigger.

## Operational notes

- **Idempotency at the subscriber**. Even with exactly-one drainer,
  network retries on the publisher side can deliver an event twice.
  Subscribers must dedup (typically by `event_id`).
- **Failure mode of LockKey**. If the drainer holding the lock
  hangs (network partition, GC pause), other drainers see the lock
  as held and skip. `pg_advisory_lock` releases on connection close,
  so a hard process death restores availability; a hung process
  blocks until its TCP keepalive times out (~5–15 min by default).
  Tune `tcp_keepalives_idle` and `tcp_keepalives_interval` on the
  Postgres side or the pool client if this is a concern.
- **Observability**. The framework's `Drain.Run` returns
  `(published, cleaned, err)`. Emit these as metrics, plus the gauge
  reads from `es.OutboxAdmin`:
  - `outbox_drain_published_total` (counter)
  - `outbox_drain_cleaned_total` (counter)
  - `outbox_drain_errors_total` (counter, by error kind)
  - `outbox_drain_dlq_crossings_total` (counter, from the `OnDLQ` callback)
  - `outbox_pending` (gauge, `CountPending`)
  - `outbox_failing` (gauge, `CountFailing`) — predicts DLQ pressure
  - `outbox_dlq` (gauge, `CountDLQ`) — page-worthy when ≥ 1
- **Backpressure on publisher**. If the publisher is slow or down,
  rows accumulate in the outbox. Monitor the pending gauge; alert
  when it grows unbounded. The drain's `MarkOutboxFailed` records
  the most recent error per row — useful for diagnosis.

## Reference

- ADR 0012 — Event Delivery and EventPublisher
- ADR 0014 — Outbox Table Shape
- [`outbox/drain.go`](../../outbox/drain.go) — the runtime
- [`es/outbox.go`](../../es/outbox.go) — `OutboxStore` and `OutboxAdmin` interfaces, `OutboxRow` / `DLQRow` shapes
- [`es/drain_locker.go`](../../es/drain_locker.go) — the DrainLocker interface
- [`adapters/storage/postgres/drain_locker.go`](../../adapters/storage/postgres/drain_locker.go) — pg_try_advisory_lock implementation
- [`adapters/storage/postgres/outbox_admin.go`](../../adapters/storage/postgres/outbox_admin.go) / [`adapters/storage/sqlite/outbox_admin.go`](../../adapters/storage/sqlite/outbox_admin.go) — admin implementations
