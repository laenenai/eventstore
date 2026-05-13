-- Read-path queries.
--
-- Three modes:
--   * Per-stream replay  (aggregate command path, snapshot rebuild)
--   * Store-wide replay  (projection rebuild, gap-fill from bus consumers)
--   * Per-tenant replay  (projection rebuild scoped to one tenant)

-- name: ReadStream :many
-- Full per-stream replay from version 1.
SELECT *
FROM events
WHERE tenant_id = @tenant_id
  AND stream_id = @stream_id
ORDER BY version;

-- name: ReadStreamFromVersion :many
-- Per-stream replay starting after a known version (typical when a
-- snapshot exists at @after_version).
SELECT *
FROM events
WHERE tenant_id = @tenant_id
  AND stream_id = @stream_id
  AND version > @after_version
ORDER BY version;

-- name: CurrentStreamVersion :one
-- Returns the current version of a stream, or 0 if the stream is empty.
-- Used for optimistic-concurrency hints; not for primary correctness
-- (that comes from the PK conflict at append time).
SELECT COALESCE(MAX(version), 0)::bigint AS current_version
FROM events
WHERE tenant_id = @tenant_id
  AND stream_id = @stream_id;

-- name: ReadAllFromPosition :many
-- Cross-tenant catch-up read. Used by gap-fill in subscribers and by
-- admin-scope projections (billing, compliance).
SELECT *
FROM events
WHERE global_position > @after_position
ORDER BY global_position
LIMIT @max_rows;

-- name: ReadAllFromPositionTenant :many
-- Tenant-scoped catch-up read. Used by per-tenant projection rebuilds.
SELECT *
FROM events
WHERE tenant_id = @tenant_id
  AND global_position > @after_position
ORDER BY global_position
LIMIT @max_rows;

-- name: GetEventByID :one
-- Single-event lookup, typically for idempotency or dedup audits.
SELECT *
FROM events
WHERE tenant_id = @tenant_id
  AND event_id = @event_id;
