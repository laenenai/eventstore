# 04: Time-Based Triggers

"Cancel the order if no payment within 24h." "Retry the webhook in 5m
if it failed." "Send a reminder one week after sign-up."

Time-based behavior is application logic, not framework logic. The
framework provides primitives that work cleanly with several scheduling
mechanisms; you pick the one that fits your deployment.

## When to use this

- A future action must fire at a wall-clock time relative to an event.
- The action must survive process restarts.
- Examples: payment timeouts, retry backoff, scheduled notifications,
  TTL on temporary resources.

## What does *not* work

- **Sleeping inside an aggregate.** Aggregates' `Decide` and `Evolve`
  are pure functions; no clocks, no sleeps. Time cannot live there.
- **`time.AfterFunc` in the subscriber.** Process restarts lose the
  scheduled callback.
- **`pg_cron` / SQLite cron extensions.** Awakens the database
  continuously, defeating scale-to-zero (ADR 0001, 0012).

## Three patterns that do work

### Pattern A: Restate's durable sleep

If your `EventPublisher` is Restate (ADR 0012), Restate has a built-in
durable sleep API. A handler can sleep for hours or days; Restate
persists the suspension and resumes the handler at the appointed time.

```go
// Restate handler
func OnOrderPlaced(ctx restate.Context, env es.Envelope) error {
    placed := env.Payload.(*orderpb.OrderPlaced)

    // Wait 24 hours, durably.
    ctx.Sleep(24 * time.Hour)

    // Reload to check current state of the order.
    sid, _ := order.NewStreamID(ctx, placed.OrderId)
    state, err := orderRuntime.Load(ctx, sid)
    if err != nil { return err }

    if !state.Paid {
        cmdID := es.DeriveCommandID("order-timeout", env.EventID, 0)
        return orderRuntime.Handle(ctx, sid,
            &orderpb.Cancel{Reason: "payment timeout"},
            es.WithCommandID(cmdID),
        )
    }
    return nil
}
```

This is the simplest pattern when Restate is available. The sleep is
durable; the handler resumes even after a redeploy.

### Pattern B: Scheduled drain over a "pending timeouts" projection

For non-Restate publishers, build a small projection that tracks
pending timeouts and have a scheduled job (cron, EventBridge, Cloud
Scheduler, Cloudflare Cron Trigger) wake the database periodically to
fire any due ones.

```sql
-- Projection: pending timeouts
CREATE TABLE pending_timeouts (
    tenant_id    TEXT        NOT NULL,
    stream_id    TEXT        NOT NULL,
    timeout_kind TEXT        NOT NULL,
    fire_at      TIMESTAMPTZ NOT NULL,
    correlation_id UUID      NOT NULL,
    PRIMARY KEY (tenant_id, stream_id, timeout_kind)
);

CREATE INDEX pending_timeouts_due_idx
    ON pending_timeouts (fire_at) WHERE fire_at IS NOT NULL;
```

```go
// Projector: when an OrderPlaced arrives, schedule a timeout.
func (p *TimeoutProjector) Handle(ctx context.Context, env es.Envelope) error {
    placed, ok := env.Payload.(*orderpb.OrderPlaced)
    if !ok { return nil }
    return p.db.Exec(ctx,
        `INSERT INTO pending_timeouts(tenant_id, stream_id, timeout_kind, fire_at, correlation_id)
         VALUES ($1, $2, 'payment_timeout', $3, $4)
         ON CONFLICT DO NOTHING`,
        env.TenantID, env.StreamID, env.OccurredAt.Add(24*time.Hour), env.CorrelationID,
    )
}

// When the order is paid, clear the timeout.
func (p *TimeoutProjector) HandlePaid(ctx context.Context, env es.Envelope) error {
    paid, ok := env.Payload.(*orderpb.OrderPaid)
    if !ok { return nil }
    return p.db.Exec(ctx,
        `DELETE FROM pending_timeouts WHERE tenant_id=$1 AND stream_id=$2 AND timeout_kind=$3`,
        env.TenantID, env.StreamID, "payment_timeout",
    )
}
```

The scheduled job:

```go
// Run on a cron — every minute, every five minutes, whatever your tolerance is.
func FireDueTimeouts(ctx context.Context, db *pgxpool.Pool, rt *order.Runtime) error {
    rows, err := db.Query(ctx, `
        DELETE FROM pending_timeouts
         WHERE fire_at <= now()
        RETURNING tenant_id, stream_id, timeout_kind, correlation_id
        LIMIT 1000
    `)
    if err != nil { return err }
    defer rows.Close()

    for rows.Next() {
        var tenant, sid, kind string
        var correlationID uuid.UUID
        _ = rows.Scan(&tenant, &sid, &kind, &correlationID)

        tctx := estenant.With(ctx, tenant)
        orderID := orderIDFromStreamID(sid)
        osid, _ := order.NewStreamID(tctx, orderID)

        // Deterministic command_id from the timeout identity.
        cmdID := es.DeriveCommandIDFromString(
            fmt.Sprintf("timeout:%s:%s:%s", tenant, sid, kind),
        )

        if err := rt.Handle(tctx, osid,
            &orderpb.Cancel{Reason: "payment timeout"},
            es.WithCommandID(cmdID),
        ); err != nil {
            // Log and continue — retried on the next cron tick.
        }
    }
    return nil
}
```

The cron job is what wakes the database. The frequency is your
maximum-tolerable-firing-delay. Between ticks, the database can suspend.

### Pattern C: External scheduler service

For complex scheduling (calendar windows, custom retry backoffs), use a
dedicated scheduler — Temporal, GCP Cloud Tasks, AWS EventBridge
Scheduler — that holds the schedule and calls back into your service
at the right time. The callback handler then dispatches the
appropriate command.

This is overkill for most cases but appropriate when:
- Schedules have business-significant complexity (timezones, holidays,
  custom rules).
- The scheduling system needs its own audit trail and observability.
- You already operate one of these services for other reasons.

## Choosing between A, B, and C

- **Restate available?** Use Pattern A. It's the simplest and most
  durable.
- **No Restate, scale-to-zero DB?** Use Pattern B. The scheduled job
  is the only thing that periodically wakes the DB, which is exactly
  the right behavior for serverless.
- **Always-on DB with complex schedules?** Pattern C with a dedicated
  scheduler service may be worth the operational cost.

## Idempotency for time-fired commands

A scheduled job may fire the same timeout twice (cron overlap, retry,
crash mid-batch). The deterministic `command_id` from the timeout
identity (`timeout:tenant:stream:kind`) ensures the target aggregate
dedups. Same pattern as recipe 01.

## What about jitter and exponential backoff?

These are scheduling-policy concerns, not framework concerns. Compute
the next `fire_at` from your policy when you write the pending_timeouts
row; the cron job just fires what's due. Restate also supports
configurable retry policies on its handlers directly.

## What about cancellation?

When the event that obviated the timeout arrives (e.g., `OrderPaid`),
the same projector that scheduled it deletes the row. The cron job
will not see it next tick. In Restate (Pattern A), the handler reloads
the order state after waking — if the order is paid, it returns without
acting. Both approaches handle "this timeout no longer applies"
cleanly.
