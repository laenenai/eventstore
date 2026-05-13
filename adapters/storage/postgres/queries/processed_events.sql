-- processed_events queries (ADR 0020 — WithDedup escape hatch).

-- name: HasProcessedEvent :one
-- Returns true if (projection_name, tenant_id, event_id) is already
-- recorded. The wrapper checks this before invoking the inner handler.
SELECT EXISTS (
    SELECT 1 FROM processed_events
    WHERE projection_name = @projection_name
      AND tenant_id       = @tenant_id
      AND event_id        = @event_id
);

-- name: MarkProcessedEvent :exec
-- Records that the inner handler processed this event successfully.
-- Idempotent: a duplicate primary-key insert silently succeeds (this
-- protects against the race where two replicas process the same event
-- before the projection lock is acquired).
INSERT INTO processed_events (projection_name, tenant_id, event_id, processed_at)
VALUES (@projection_name, @tenant_id, @event_id, clock_timestamp())
ON CONFLICT (projection_name, tenant_id, event_id) DO NOTHING;

-- name: CleanupProcessedEvents :execrows
-- Retention pruning. Deletes processed_events rows older than
-- @older_than. Returns the number of rows deleted. Operators run this
-- periodically since the table grows monotonically with event count.
DELETE FROM processed_events
WHERE projection_name = @projection_name
  AND processed_at < @older_than;
