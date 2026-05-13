-- processed_events queries for SQLite (ADR 0020).

-- name: HasProcessedEvent :one
SELECT EXISTS (
    SELECT 1 FROM processed_events
    WHERE projection_name = ?
      AND tenant_id       = ?
      AND event_id        = ?
);

-- name: MarkProcessedEvent :exec
INSERT INTO processed_events (projection_name, tenant_id, event_id, processed_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (projection_name, tenant_id, event_id) DO NOTHING;

-- name: CleanupProcessedEvents :execrows
DELETE FROM processed_events
WHERE projection_name = ?
  AND processed_at    < ?;
