-- Snapshot queries (ADR 0011).

-- name: GetSnapshot :one
SELECT tenant_id, stream_id, version, state_schema_version, state, created_at
FROM snapshots
WHERE tenant_id = ?
  AND stream_id = ?;

-- name: UpsertSnapshot :exec
INSERT INTO snapshots (tenant_id, stream_id, version, state_schema_version, state)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (tenant_id, stream_id) DO UPDATE SET
    version              = excluded.version,
    state_schema_version = excluded.state_schema_version,
    state                = excluded.state,
    created_at           = strftime('%Y-%m-%dT%H:%M:%fZ', 'now');

-- name: DeleteSnapshot :exec
DELETE FROM snapshots
WHERE tenant_id = ?
  AND stream_id = ?;
