-- state_cache queries for SQLite (ADR 0020 Tier 1).
-- See the Postgres sibling for the canonical doc comments.

-- name: UpsertStateCache :exec
-- The protojson bytes come in as a TEXT param; jsonb() converts them
-- to binary on insert (ADR 0021). Reads return the BLOB; protojson
-- on the Go side decodes either form interchangeably.
INSERT INTO state_cache (
    tenant_id, stream_id, type_url, state, version, terminal,
    state_schema_version, updated_at
) VALUES (
    sqlc.arg(tenant_id),
    sqlc.arg(stream_id),
    sqlc.arg(type_url),
    jsonb(sqlc.arg(state)),
    sqlc.arg(version),
    sqlc.arg(terminal),
    sqlc.arg(state_schema_version),
    sqlc.arg(updated_at)
)
ON CONFLICT (tenant_id, stream_id) DO UPDATE SET
    type_url             = excluded.type_url,
    state                = excluded.state,
    version              = excluded.version,
    terminal             = excluded.terminal,
    state_schema_version = excluded.state_schema_version,
    updated_at           = excluded.updated_at;

-- name: GetState :one
-- json(state) converts the BLOB JSONB back to text bytes so the Go
-- side can unmarshal via protojson without knowing about the binary
-- form. The framework's StateCacheReader returns these bytes as []byte.
SELECT type_url, json(state) AS state, version, terminal, state_schema_version, updated_at
FROM state_cache
WHERE tenant_id = ?
  AND stream_id = ?;

-- name: ListStates :many
SELECT tenant_id, stream_id, type_url, json(state) AS state, version, terminal, state_schema_version, updated_at
FROM state_cache
WHERE tenant_id      = sqlc.arg(tenant_id)
  AND type_url       = sqlc.arg(type_url)
  AND stream_id      > sqlc.arg(after_stream_id)
ORDER BY stream_id
LIMIT sqlc.arg(max_rows);

-- name: ListStatesAll :many
SELECT tenant_id, stream_id, type_url, json(state) AS state, version, terminal, state_schema_version, updated_at
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
