-- Outbox queries (ADR 0014).

-- name: PendingOutboxRows :many
SELECT tenant_id, global_position, event_id, enqueued_at, published_at, attempts, last_error, next_attempt_at
FROM outbox
WHERE tenant_id = ?
  AND published_at IS NULL
ORDER BY global_position
LIMIT ?;

-- name: PendingOutboxRowsAllTenants :many
SELECT tenant_id, global_position, event_id, enqueued_at, published_at, attempts, last_error, next_attempt_at
FROM outbox
WHERE published_at IS NULL
ORDER BY global_position
LIMIT ?;

-- name: PendingOutboxWithEnvelope :many
-- Drain hot path. Filters by retry-readiness, max-attempts (DLQ
-- threshold), and the per-stream "head" rule. The head-versions CTE
-- computes the lowest unpublished, not-yet-DLQ'd version per stream;
-- the outer SELECT then only emits rows that ARE the head. This
-- preserves per-stream order across leader handoffs even when backoff
-- puts the head in cooldown. DLQ'd rows (attempts >= max_attempts)
-- don't appear in the head-versions CTE, so AutoResumeAfterDLQ=true
-- can step past them. AutoResumeAfterDLQ=false uses QuarantinedStreams
-- in the drain runtime for the "DLQ also blocks" semantic.
WITH head_versions AS (
    SELECT e2.tenant_id, e2.stream_id, MIN(e2.version) AS head_version
    FROM outbox o2
    JOIN events e2
      ON e2.tenant_id = o2.tenant_id
     AND e2.event_id  = o2.event_id
    WHERE o2.published_at IS NULL
      AND o2.attempts < sqlc.arg(attempts)
    GROUP BY e2.tenant_id, e2.stream_id
)
SELECT
    o.tenant_id,
    o.global_position,
    o.event_id,
    o.attempts,
    e.stream_id,
    e.version,
    e.type_url,
    e.schema_version,
    e.occurred_at,
    e.recorded_at,
    e.correlation_id,
    e.causation_id,
    e.command_id,
    e.actor,
    e.actor_principal,
    e.payload,
    e.encryption_key_refs
FROM outbox o
JOIN events e
  ON e.tenant_id = o.tenant_id
 AND e.event_id  = o.event_id
JOIN head_versions hv
  ON hv.tenant_id   = e.tenant_id
 AND hv.stream_id   = e.stream_id
 AND hv.head_version = e.version
WHERE o.published_at IS NULL
  AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= sqlc.arg(next_attempt_at))
  AND o.attempts < sqlc.arg(attempts)
ORDER BY o.global_position
LIMIT sqlc.arg(limit);

-- name: QuarantinedStreams :many
SELECT DISTINCT o.tenant_id, e.stream_id
FROM outbox o
JOIN events e
  ON e.tenant_id = o.tenant_id
 AND e.event_id  = o.event_id
WHERE o.published_at IS NULL
  AND o.attempts >= ?
  AND o.tenant_id = ?;

-- name: QuarantinedStreamsAllTenants :many
SELECT DISTINCT o.tenant_id, e.stream_id
FROM outbox o
JOIN events e
  ON e.tenant_id = o.tenant_id
 AND e.event_id  = o.event_id
WHERE o.published_at IS NULL
  AND o.attempts >= ?;

-- name: MarkOutboxPublished :exec
UPDATE outbox
SET published_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE tenant_id = ?
  AND global_position = ?;

-- name: MarkOutboxFailed :exec
UPDATE outbox
SET attempts        = attempts + 1,
    last_error      = ?,
    next_attempt_at = ?
WHERE tenant_id      = ?
  AND global_position = ?;

-- name: CleanupPublished :exec
DELETE FROM outbox
WHERE tenant_id = ?
  AND published_at IS NOT NULL
  AND published_at < ?;

-- ===========================================================================
-- Admin / dashboard queries
-- ===========================================================================

-- name: ListDLQ :many
SELECT
    o.tenant_id,
    o.global_position,
    o.event_id,
    o.enqueued_at,
    o.attempts,
    o.last_error,
    o.next_attempt_at,
    e.stream_id,
    e.type_url,
    e.correlation_id,
    e.causation_id,
    e.command_id,
    e.actor_principal
FROM outbox o
JOIN events e
  ON e.tenant_id = o.tenant_id
 AND e.event_id  = o.event_id
WHERE o.tenant_id       = ?
  AND o.published_at   IS NULL
  AND o.attempts       >= ?
  AND o.global_position > ?
ORDER BY o.global_position
LIMIT ?;

-- name: CountDLQ :one
SELECT COUNT(*) FROM outbox
WHERE tenant_id    = ?
  AND published_at IS NULL
  AND attempts    >= ?;

-- name: CountPending :one
SELECT COUNT(*) FROM outbox
WHERE tenant_id    = ?
  AND published_at IS NULL;

-- name: CountFailing :one
SELECT COUNT(*) FROM outbox
WHERE tenant_id    = ?
  AND published_at IS NULL
  AND attempts     > 0
  AND attempts     < ?;

-- name: OldestPendingEnqueuedAt :one
SELECT MIN(enqueued_at) FROM outbox
WHERE tenant_id    = ?
  AND published_at IS NULL;

-- name: ReplayDLQ :exec
UPDATE outbox
SET attempts        = 0,
    next_attempt_at = NULL,
    last_error      = NULL
WHERE tenant_id       = ?
  AND global_position = ?;

-- name: AbandonDLQ :exec
UPDATE outbox
SET published_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    last_error   = COALESCE(last_error, '') || ' [abandoned]'
WHERE tenant_id       = ?
  AND global_position = ?
  AND published_at   IS NULL;

-- name: ReplayAllDLQ :execrows
UPDATE outbox
SET attempts        = 0,
    next_attempt_at = NULL,
    last_error      = NULL
WHERE tenant_id    = ?
  AND published_at IS NULL
  AND attempts    >= ?;
