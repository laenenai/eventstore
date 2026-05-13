# ADR 0014: Outbox Table Shape

- **Status:** Accepted
- **Date:** 2026-05-13
- **Pairs with:** ADR 0012 (Event Delivery and EventPublisher)

## Context

ADR 0012 establishes the outbox as the durability seam between the
writer transaction and the publisher: events are written to the outbox
in the same transaction as the event append, and a scheduled drain
guarantees eventual handoff to the publisher even if the writer crashes
or the publisher is temporarily unavailable.

Two concrete questions remain:

1. One outbox row per event, or one row per `(event, subscriber)` pair?
2. Inline copy of the envelope, or reference to the events table?

## Decision

### One row per event (not per subscriber)

```sql
CREATE TABLE outbox (
  global_position  BIGINT       NOT NULL,
  tenant_id        TEXT         NOT NULL,
  event_id         UUID         NOT NULL,
  enqueued_at      TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  published_at     TIMESTAMPTZ,
  attempts         INT          NOT NULL DEFAULT 0,
  last_error       TEXT,
  PRIMARY KEY (tenant_id, global_position)
) PARTITION BY HASH (tenant_id);

CREATE INDEX outbox_pending_idx
  ON outbox (tenant_id, global_position)
  WHERE published_at IS NULL;
```

The publisher fans out to all targets and marks the row published when
the bus has accepted handoff. Per-subscriber retry, redelivery, and
visibility-timeout semantics are owned by the publisher runtime
(Restate's handler retries, SQS visibility timeouts, NATS consumer
redelivery, etc.).

### Reference, not inline

The outbox row carries `(tenant_id, global_position, event_id)`. At
publish time, the drain process JOINs to the `events` table on the
primary key to fetch the envelope and payload.

### Why not a column on `events`?

The events table is otherwise insert-only — perfect for layout, vacuum,
replication, and compression. Adding a `published_at` column would
generate dead tuples on every published event, triggering vacuum churn
on the table most worth keeping clean. A separate outbox table keeps
update churn off the events partition.

### Retention and cleanup

Outbox rows are eligible for deletion once
`published_at IS NOT NULL AND published_at < now() - retention_window`.

- Default `retention_window = 7 days` — long enough to investigate "did
  we publish event X?", short enough to keep the table bounded.
- Configurable per deployment.
- The same scheduled drain that publishes pending rows also runs the
  cleanup pass — one DB wake-up, both jobs.

### Failure semantics

A publisher failure on a row:

- Increments `attempts` by 1.
- Writes the truncated error message to `last_error`.
- Leaves `published_at` NULL.
- Next drain run retries.

There is **no exponential backoff inside the outbox**. The outbox
guarantees eventual handoff; the publisher runtime owns its own retry
policy (Restate has built-in retries with backoff; SQS uses visibility
timeouts; NATS uses delivery attempts).

## Consequences

### Positive

- **Minimal storage.** Single row per event, references the source of
  truth, no envelope duplication. Steady-state outbox is mostly empty
  because rows are deleted after the retention window.
- **Events table stays insert-only.** Vacuum and replication behave
  cleanly. No layout regression for the most-written table in the
  system.
- **Drain is cheap.** Partial index on `WHERE published_at IS NULL`
  stays tiny — a few rows in normal operation, zero rows when fully
  caught up.
- **Audit window preserved.** "Did we publish this event?" answerable
  for the retention window without inspecting bus logs.
- **One wake-up serves two jobs.** The scheduled drain publishes and
  cleans up in the same run, maximally efficient for scale-to-zero.

### Negative

- **Publish-time JOIN** to `events` is required. JOIN is on PK so it's
  fast, but it's not free.
- **Per-subscriber durability is the publisher's job.** This is
  intentional — Restate, SQS, NATS, and Pub/Sub all already provide it —
  but means deployments using a non-durable publisher (e.g., the
  `inproc` dev-only adapter) lose at-least-once semantics. The contract
  is documented per adapter.

## Alternatives Considered

### One row per (event, subscriber)

Rejected. Duplicates work the bus already does, explodes the outbox row
count by the current subscriber count, and complicates "is this row
done?" when the subscriber list changes over time (adding a subscriber
should not retroactively un-publish historical rows).

### Inline copy of envelope + payload in outbox

Rejected. Doubles storage during the brief window before publish,
creates a synchronization surface (what if the values diverge?), and the
JOIN to the events PK is cheap enough that the saved query cost is not
load-bearing.

### `published_at` column on the events table

Rejected. Turns the events table from insert-only into
insert-then-update, generating dead tuples and vacuum churn on the most
write-heavy table. The separate outbox table isolates that churn.

### Per-row exponential backoff state in the outbox

Rejected. Duplicates logic the publisher runtime already implements.
Keeping the outbox semantics simple ("hand off once, retry on next
drain") avoids two competing backoff policies.

### No retention (keep outbox rows forever)

Rejected. Outbox grows unboundedly and bloats the partition. The audit
question "did we publish event X?" remains answerable via bus-side logs
for any time horizon beyond the retention window.
