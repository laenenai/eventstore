-- Outbox queries (ADR 0014).
--
-- The drain process wakes the database on a schedule, pulls pending
-- rows in global_position order, fetches the envelope+payload via a
-- JOIN to events, hands off to the EventPublisher, then marks each row
-- published. The same wake-up also runs the cleanup pass.

-- name: PendingOutboxRows :many
-- Tenant-scoped pending rows. Hot path for per-tenant publishers.
SELECT *
FROM outbox
WHERE tenant_id = @tenant_id
  AND published_at IS NULL
ORDER BY global_position
LIMIT @max_rows;

-- name: PendingOutboxRowsAllTenants :many
-- Cross-tenant pending rows. Used by the shared scheduled drain that
-- handles all tenants in one wake-up.
SELECT *
FROM outbox
WHERE published_at IS NULL
ORDER BY global_position
LIMIT @max_rows;

-- name: PendingOutboxWithEnvelope :many
-- Drain hot path: pending rows joined to their envelope+payload, ready
-- to hand to the publisher.
SELECT
    o.tenant_id,
    o.global_position,
    o.event_id,
    o.attempts,
    e.stream_id,
    e.version,
    e.type_url,
    e.schema_version,
    e.occurred_at,
    e.recorded_at,
    e.correlation_id,
    e.causation_id,
    e.command_id,
    e.actor,
    e.actor_principal,
    e.payload,
    e.encryption_key_refs
FROM outbox o
JOIN events e
  ON e.tenant_id = o.tenant_id
 AND e.event_id  = o.event_id
WHERE o.published_at IS NULL
ORDER BY o.global_position
LIMIT @max_rows;

-- name: MarkOutboxPublished :exec
UPDATE outbox
SET published_at = clock_timestamp()
WHERE tenant_id = @tenant_id
  AND global_position = @global_position;

-- name: MarkOutboxFailed :exec
-- Increment attempts and record the error. The next drain run retries;
-- exponential backoff is the publisher's concern, not the outbox's.
UPDATE outbox
SET attempts   = attempts + 1,
    last_error = @last_error
WHERE tenant_id = @tenant_id
  AND global_position = @global_position;

-- name: CleanupPublished :exec
-- Retention pruning. Deletes rows that have been published longer than
-- @older_than. Runs in the same scheduled wake-up as the publish drain.
DELETE FROM outbox
WHERE tenant_id = @tenant_id
  AND published_at IS NOT NULL
  AND published_at < @older_than;
