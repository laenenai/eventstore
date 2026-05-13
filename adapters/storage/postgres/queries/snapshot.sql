-- Snapshot queries (ADR 0011).
--
-- Lazy snapshots: written on read after N events since the last
-- snapshot. Latest wins per stream.

-- name: GetSnapshot :one
SELECT *
FROM snapshots
WHERE tenant_id = @tenant_id
  AND stream_id = @stream_id;

-- name: UpsertSnapshot :exec
-- Write or replace the snapshot for a stream. state_schema_version is
-- the decider state's shape version; mismatches at read time are
-- silently discarded with full-replay fallback.
INSERT INTO snapshots (tenant_id, stream_id, version, state_schema_version, state)
VALUES (@tenant_id, @stream_id, @version, @state_schema_version, @state)
ON CONFLICT (tenant_id, stream_id) DO UPDATE SET
    version              = EXCLUDED.version,
    state_schema_version = EXCLUDED.state_schema_version,
    state                = EXCLUDED.state,
    created_at           = clock_timestamp();

-- name: DeleteSnapshot :exec
-- Operational tool: force a full-replay on next read.
DELETE FROM snapshots
WHERE tenant_id = @tenant_id
  AND stream_id = @stream_id;
