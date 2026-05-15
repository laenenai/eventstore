-- Read-path queries.

-- name: ReadStream :many
SELECT
    event_id, tenant_id, stream_id, version, global_position,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor, actor_principal, payload, payload_json, encryption_key_refs,
    hash, prev_hash
FROM events
WHERE tenant_id = ?
  AND stream_id = ?
ORDER BY version;

-- name: ReadStreamFromVersion :many
SELECT
    event_id, tenant_id, stream_id, version, global_position,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor, actor_principal, payload, payload_json, encryption_key_refs,
    hash, prev_hash
FROM events
WHERE tenant_id = ?
  AND stream_id = ?
  AND version > ?
ORDER BY version;

-- name: CurrentStreamVersion :one
SELECT COALESCE(MAX(version), 0) AS current_version
FROM events
WHERE tenant_id = ?
  AND stream_id = ?;

-- name: ReadAllFromPosition :many
SELECT
    event_id, tenant_id, stream_id, version, global_position,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor, actor_principal, payload, payload_json, encryption_key_refs,
    hash, prev_hash
FROM events
WHERE global_position > ?
ORDER BY global_position
LIMIT ?;

-- name: ReadAllFromPositionTenant :many
SELECT
    event_id, tenant_id, stream_id, version, global_position,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor, actor_principal, payload, payload_json, encryption_key_refs,
    hash, prev_hash
FROM events
WHERE tenant_id = ?
  AND global_position > ?
ORDER BY global_position
LIMIT ?;

-- name: GetEventByID :one
SELECT
    event_id, tenant_id, stream_id, version, global_position,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor, actor_principal, payload, payload_json, encryption_key_refs,
    hash, prev_hash
FROM events
WHERE tenant_id = ?
  AND event_id = ?;
