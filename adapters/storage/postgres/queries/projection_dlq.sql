-- projection_dlq queries (ADR 0020 deferred — DLQ-skip mode).

-- name: InsertProjectionDLQ :exec
-- Capture one event that failed under DLQOnFailure. Idempotent: same
-- (projection, tenant, global_position) overwrites the existing row
-- (handler error message is the most useful version).
INSERT INTO projection_dlq (
    projection_name, tenant_id, global_position, event_id, type_url, last_error, enqueued_at
) VALUES (
    @projection_name, @tenant_id, @global_position, @event_id, @type_url, @last_error, clock_timestamp()
)
ON CONFLICT (projection_name, tenant_id, global_position) DO UPDATE SET
    last_error  = EXCLUDED.last_error,
    enqueued_at = EXCLUDED.enqueued_at;

-- name: ListProjectionDLQ :many
-- Paginated listing for an admin dashboard. afterPosition = 0 starts
-- from the beginning.
SELECT projection_name, tenant_id, global_position, event_id, type_url, last_error, enqueued_at
FROM projection_dlq
WHERE projection_name = @projection_name
  AND tenant_id       = @tenant_id
  AND global_position > @after_position
ORDER BY global_position
LIMIT @max_rows;

-- name: CountProjectionDLQ :one
SELECT COUNT(*)::bigint
FROM projection_dlq
WHERE projection_name = @projection_name
  AND tenant_id       = @tenant_id;

-- name: DeleteProjectionDLQ :exec
-- Operator action: remove a DLQ row (after Replay via ResetTo, or
-- after Abandon).
DELETE FROM projection_dlq
WHERE projection_name = @projection_name
  AND tenant_id       = @tenant_id
  AND global_position = @global_position;

-- name: AbandonAllProjectionDLQ :execrows
-- Operator action: bulk-delete all DLQ rows for one projection. Used
-- after a runbook decision to stop trying to reprocess. The events
-- themselves remain in the events table; only the DLQ markers go.
DELETE FROM projection_dlq
WHERE projection_name = @projection_name
  AND tenant_id       = @tenant_id;
