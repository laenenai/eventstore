-- state_stream queries for SQLite (ADR 0024).

-- name: ListStreamsBehind :many
-- json(state) converts the BLOB JSONB back to text bytes so the Go
-- side gets the protojson form (ADR 0021).
SELECT
    sc.tenant_id,
    sc.stream_id,
    sc.type_url,
    json(sc.state) AS state,
    sc.version,
    sc.state_schema_version,
    sc.updated_at,
    COALESCE(p.last_delivered_version, 0) AS last_delivered_version
FROM state_cache sc
LEFT JOIN state_stream_subscribers p
       ON p.name      = sqlc.arg(name)
      AND p.tenant_id = sc.tenant_id
      AND p.stream_id = sc.stream_id
WHERE sc.version > COALESCE(p.last_delivered_version, 0)
  AND (sqlc.arg(tenant_id) = '' OR sc.tenant_id = sqlc.arg(tenant_id))
ORDER BY sc.updated_at
LIMIT sqlc.arg(max_rows);

-- name: UpsertStateStreamPosition :exec
INSERT INTO state_stream_subscribers (
    name, tenant_id, stream_id, last_delivered_version, updated_at
) VALUES (
    sqlc.arg(name),
    sqlc.arg(tenant_id),
    sqlc.arg(stream_id),
    sqlc.arg(version),
    sqlc.arg(updated_at)
)
ON CONFLICT (name, tenant_id, stream_id) DO UPDATE SET
    last_delivered_version = MAX(state_stream_subscribers.last_delivered_version, excluded.last_delivered_version),
    updated_at             = excluded.updated_at;

-- name: ResetStateStreamSubscriber :execrows
DELETE FROM state_stream_subscribers
WHERE name      = sqlc.arg(name)
  AND tenant_id = sqlc.arg(tenant_id);

-- name: ResetStateStreamSubscriberForStream :exec
DELETE FROM state_stream_subscribers
WHERE name      = sqlc.arg(name)
  AND tenant_id = sqlc.arg(tenant_id)
  AND stream_id = sqlc.arg(stream_id);

-- name: CountStateStreamLag :one
SELECT
    COUNT(*)                          AS streams_behind,
    COALESCE(MAX(sc.version - COALESCE(p.last_delivered_version, 0)), 0) AS max_lag_versions
FROM state_cache sc
LEFT JOIN state_stream_subscribers p
       ON p.name      = sqlc.arg(name)
      AND p.tenant_id = sc.tenant_id
      AND p.stream_id = sc.stream_id
WHERE sc.version > COALESCE(p.last_delivered_version, 0)
  AND (sqlc.arg(tenant_id) = '' OR sc.tenant_id = sqlc.arg(tenant_id));

-- name: ListStateStreamSubscribers :many
SELECT DISTINCT name FROM state_stream_subscribers ORDER BY name;
