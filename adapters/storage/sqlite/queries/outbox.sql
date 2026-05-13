-- Outbox queries (ADR 0014).

-- name: PendingOutboxRows :many
SELECT tenant_id, global_position, event_id, enqueued_at, published_at, attempts, last_error
FROM outbox
WHERE tenant_id = ?
  AND published_at IS NULL
ORDER BY global_position
LIMIT ?;

-- name: PendingOutboxRowsAllTenants :many
SELECT tenant_id, global_position, event_id, enqueued_at, published_at, attempts, last_error
FROM outbox
WHERE published_at IS NULL
ORDER BY global_position
LIMIT ?;

-- name: PendingOutboxWithEnvelope :many
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
LIMIT ?;

-- name: MarkOutboxPublished :exec
UPDATE outbox
SET published_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE tenant_id = ?
  AND global_position = ?;

-- name: MarkOutboxFailed :exec
UPDATE outbox
SET attempts   = attempts + 1,
    last_error = ?
WHERE tenant_id = ?
  AND global_position = ?;

-- name: CleanupPublished :exec
DELETE FROM outbox
WHERE tenant_id = ?
  AND published_at IS NOT NULL
  AND published_at < ?;
