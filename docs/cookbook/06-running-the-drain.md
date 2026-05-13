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

The framework filters to `global_position % TotalShards == Shard` per
replica. Each row is processed by exactly one shard. Doesn't need
DB-side coordination; works on any adapter.

Current implementation is client-side filtering (the adapter returns
all matching rows, drain filters in Go). For large pending sets,
push-down to the SQL `WHERE` clause is straightforward to add as a
follow-up.

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
  `(published, cleaned, err)`. Emit these as metrics:
  - `outbox_drain_published_total` (counter)
  - `outbox_drain_cleaned_total` (counter)
  - `outbox_drain_errors_total` (counter, by error kind)
  - `outbox_pending` (gauge) — separate query, run periodically
- **Backpressure on publisher**. If the publisher is slow or down,
  rows accumulate in the outbox. Monitor the pending gauge; alert
  when it grows unbounded. The drain's `MarkOutboxFailed` records
  the most recent error per row — useful for diagnosis.

## Reference

- ADR 0012 — Event Delivery and EventPublisher
- ADR 0014 — Outbox Table Shape
- [`outbox/drain.go`](../../outbox/drain.go) — the runtime
- [`es/drain_locker.go`](../../es/drain_locker.go) — the DrainLocker interface
- [`adapters/storage/postgres/drain_locker.go`](../../adapters/storage/postgres/drain_locker.go) — pg_try_advisory_lock implementation
