-- state_stream queries (ADR 0024).

-- name: ListStreamsBehind :many
-- Drain hot path: returns up to @max_rows streams where the named
-- subscriber's position is behind state_cache.version. The state
-- payload comes from state_cache (no duplication of state bytes).
-- Tenant filter: pass @tenant_id = '' for cross-tenant drain.
SELECT
    sc.tenant_id,
    sc.stream_id,
    sc.type_url,
    sc.state,
    sc.version,
    sc.state_schema_version,
    sc.updated_at,
    COALESCE(p.last_delivered_version, 0) AS last_delivered_version
FROM state_cache sc
LEFT JOIN state_stream_subscribers p
       ON p.name      = @name
      AND p.tenant_id = sc.tenant_id
      AND p.stream_id = sc.stream_id
WHERE sc.version > COALESCE(p.last_delivered_version, 0)
  AND (@tenant_id::text = '' OR sc.tenant_id = @tenant_id)
ORDER BY sc.updated_at
LIMIT @max_rows;

-- name: UpsertStateStreamPosition :exec
-- Advance the subscriber's position for one stream after a successful
-- delivery. Last-delivered-version is monotonically non-decreasing for
-- a given (name, tenant_id, stream_id).
INSERT INTO state_stream_subscribers (
    name, tenant_id, stream_id, last_delivered_version, updated_at
) VALUES (
    @name, @tenant_id, @stream_id, @version, clock_timestamp()
)
ON CONFLICT (name, tenant_id, stream_id) DO UPDATE SET
    last_delivered_version = GREATEST(state_stream_subscribers.last_delivered_version, EXCLUDED.last_delivered_version),
    updated_at             = clock_timestamp();

-- name: ResetStateStreamSubscriber :execrows
-- Operator action: delete every position for one subscriber. Next
-- drain will full-backfill (current state of every stream). Returns
-- the count of rows deleted.
DELETE FROM state_stream_subscribers
WHERE name      = @name
  AND tenant_id = @tenant_id;

-- name: ResetStateStreamSubscriberForStream :exec
-- Single-stream rewind: delete the position row for one stream so the
-- drain redelivers the current state on its next pass. Used after the
-- crypto-shred propagation runbook (ADR 0024 § Crypto-shred).
DELETE FROM state_stream_subscribers
WHERE name      = @name
  AND tenant_id = @tenant_id
  AND stream_id = @stream_id;

-- name: CountStateStreamLag :one
-- How many streams is the subscriber behind on, and what's the largest
-- gap? Used for operator dashboards / alerting per ADR 0024 § 6.
SELECT
    COUNT(*)::bigint                          AS streams_behind,
    COALESCE(MAX(sc.version - COALESCE(p.last_delivered_version, 0)), 0)::bigint AS max_lag_versions
FROM state_cache sc
LEFT JOIN state_stream_subscribers p
       ON p.name      = @name
      AND p.tenant_id = sc.tenant_id
      AND p.stream_id = sc.stream_id
WHERE sc.version > COALESCE(p.last_delivered_version, 0)
  AND (@tenant_id::text = '' OR sc.tenant_id = @tenant_id);

-- name: ListStateStreamSubscribers :many
-- Returns the distinct subscriber names present in the table — used
-- by Admin.List.
SELECT DISTINCT name FROM state_stream_subscribers ORDER BY name;
