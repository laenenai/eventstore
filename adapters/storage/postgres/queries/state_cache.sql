-- state_cache queries (ADR 0020 Tier 1).
--
-- One row per (tenant_id, stream_id). Written transactionally with
-- events via UpsertStateCache (called from Append). Read via GetState /
-- ListStates / ListStatesAll.

-- name: UpsertStateCache :exec
-- Atomic upsert: insert on first append, update on subsequent. Called
-- from within the events-append transaction.
INSERT INTO state_cache (
    tenant_id, stream_id, type_url, state, version, terminal,
    state_schema_version, updated_at
) VALUES (
    @tenant_id, @stream_id, @type_url, @state, @version, @terminal,
    @state_schema_version, clock_timestamp()
)
ON CONFLICT (tenant_id, stream_id) DO UPDATE SET
    type_url             = EXCLUDED.type_url,
    state                = EXCLUDED.state,
    version              = EXCLUDED.version,
    terminal             = EXCLUDED.terminal,
    state_schema_version = EXCLUDED.state_schema_version,
    updated_at           = EXCLUDED.updated_at;

-- name: GetState :one
-- Tier 1 point lookup. Returns nothing if the stream has no cached
-- state (either never written or the aggregate is not opted in).
SELECT type_url, state, version, terminal, state_schema_version, updated_at
FROM state_cache
WHERE tenant_id = @tenant_id
  AND stream_id = @stream_id;

-- name: ListStates :many
-- Paged listing of cached states for a given type_url. Pass the
-- stream_id of the last row in the previous page as @after_stream_id
-- to fetch the next page; start with @after_stream_id = ''.
SELECT tenant_id, stream_id, type_url, state, version, terminal, state_schema_version, updated_at
FROM state_cache
WHERE tenant_id      = @tenant_id
  AND type_url       = @type_url
  AND stream_id      > @after_stream_id
ORDER BY stream_id
LIMIT @max_rows;

-- name: ListStatesAll :many
-- Cross-tenant variant. Admin/operator-scope use only.
SELECT tenant_id, stream_id, type_url, state, version, terminal, state_schema_version, updated_at
FROM state_cache
WHERE type_url      = @type_url
  AND (tenant_id, stream_id) > (@after_tenant_id, @after_stream_id)
ORDER BY tenant_id, stream_id
LIMIT @max_rows;

-- name: DeleteStateCacheForType :execrows
-- Wipe state_cache rows for a given type before a rebuild. Returns the
-- number of rows deleted.
DELETE FROM state_cache
WHERE tenant_id = @tenant_id
  AND type_url  = @type_url;

-- name: DeleteStateCacheForTypeAllTenants :execrows
-- Cross-tenant variant for full rebuild.
DELETE FROM state_cache
WHERE type_url = @type_url;
