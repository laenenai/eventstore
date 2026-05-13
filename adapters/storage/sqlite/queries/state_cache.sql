-- state_cache queries for SQLite (ADR 0020 Tier 1).
-- See the Postgres sibling for the canonical doc comments.

-- name: UpsertStateCache :exec
INSERT INTO state_cache (
    tenant_id, stream_id, type_url, state, version, terminal, updated_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT (tenant_id, stream_id) DO UPDATE SET
    type_url   = excluded.type_url,
    state      = excluded.state,
    version    = excluded.version,
    terminal   = excluded.terminal,
    updated_at = excluded.updated_at;

-- name: GetState :one
SELECT type_url, state, version, terminal, updated_at
FROM state_cache
WHERE tenant_id = ?
  AND stream_id = ?;

-- name: ListStates :many
SELECT tenant_id, stream_id, type_url, state, version, terminal, updated_at
FROM state_cache
WHERE tenant_id      = sqlc.arg(tenant_id)
  AND type_url       = sqlc.arg(type_url)
  AND stream_id      > sqlc.arg(after_stream_id)
ORDER BY stream_id
LIMIT sqlc.arg(max_rows);

-- name: ListStatesAll :many
SELECT tenant_id, stream_id, type_url, state, version, terminal, updated_at
FROM state_cache
WHERE type_url    = sqlc.arg(type_url)
  AND (tenant_id > sqlc.arg(after_tenant_id)
       OR (tenant_id = sqlc.arg(after_tenant_id) AND stream_id > sqlc.arg(after_stream_id)))
ORDER BY tenant_id, stream_id
LIMIT sqlc.arg(max_rows);

-- name: DeleteStateCacheForType :execrows
DELETE FROM state_cache
WHERE tenant_id = ?
  AND type_url  = ?;

-- name: DeleteStateCacheForTypeAllTenants :execrows
DELETE FROM state_cache
WHERE type_url = ?;
