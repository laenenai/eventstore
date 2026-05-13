-- projection_dlq queries for SQLite (ADR 0020).

-- name: InsertProjectionDLQ :exec
INSERT INTO projection_dlq (
    projection_name, tenant_id, global_position, event_id, type_url, last_error, enqueued_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (projection_name, tenant_id, global_position) DO UPDATE SET
    last_error  = excluded.last_error,
    enqueued_at = excluded.enqueued_at;

-- name: ListProjectionDLQ :many
SELECT projection_name, tenant_id, global_position, event_id, type_url, last_error, enqueued_at
FROM projection_dlq
WHERE projection_name = sqlc.arg(projection_name)
  AND tenant_id       = sqlc.arg(tenant_id)
  AND global_position > sqlc.arg(after_position)
ORDER BY global_position
LIMIT sqlc.arg(max_rows);

-- name: CountProjectionDLQ :one
SELECT COUNT(*) FROM projection_dlq
WHERE projection_name = ?
  AND tenant_id       = ?;

-- name: DeleteProjectionDLQ :exec
DELETE FROM projection_dlq
WHERE projection_name = ?
  AND tenant_id       = ?
  AND global_position = ?;

-- name: AbandonAllProjectionDLQ :execrows
DELETE FROM projection_dlq
WHERE projection_name = ?
  AND tenant_id       = ?;
