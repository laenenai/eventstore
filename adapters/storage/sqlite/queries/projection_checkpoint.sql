-- projection_checkpoint queries for SQLite (ADR 0020 Tier 3).
-- See the Postgres sibling for the canonical doc comments.

-- name: LoadProjectionCheckpoint :one
SELECT cursor
FROM projection_checkpoint
WHERE name      = ?
  AND tenant_id = ?;

-- name: SaveProjectionCheckpoint :exec
INSERT INTO projection_checkpoint (name, tenant_id, cursor, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (name, tenant_id) DO UPDATE SET
    cursor     = excluded.cursor,
    updated_at = excluded.updated_at;

-- name: ResetProjectionCheckpoint :exec
INSERT INTO projection_checkpoint (name, tenant_id, cursor, updated_at)
VALUES (sqlc.arg(name), sqlc.arg(tenant_id), 0, sqlc.arg(updated_at))
ON CONFLICT (name, tenant_id) DO UPDATE SET
    cursor     = 0,
    updated_at = excluded.updated_at;

-- name: SetProjectionCheckpoint :exec
INSERT INTO projection_checkpoint (name, tenant_id, cursor, updated_at)
VALUES (sqlc.arg(name), sqlc.arg(tenant_id), sqlc.arg(cursor), sqlc.arg(updated_at))
ON CONFLICT (name, tenant_id) DO UPDATE SET
    cursor     = excluded.cursor,
    updated_at = excluded.updated_at;

-- name: ListProjectionCheckpoints :many
SELECT name, tenant_id, cursor, updated_at
FROM projection_checkpoint
ORDER BY name, tenant_id;

-- name: GetProjectionStatus :one
SELECT name, tenant_id, cursor, updated_at
FROM projection_checkpoint
WHERE name      = ?
  AND tenant_id = ?;
