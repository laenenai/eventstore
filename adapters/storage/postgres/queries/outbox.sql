-- Outbox queries (ADR 0014).
--
-- The drain process wakes the database on a schedule, pulls pending
-- rows in global_position order, fetches the envelope+payload via a
-- JOIN to events, hands off to the EventPublisher, then marks each row
-- published. The same wake-up also runs the cleanup pass.

-- name: PendingOutboxRows :many
-- Tenant-scoped pending rows. Hot path for per-tenant publishers.
SELECT tenant_id, global_position, event_id, enqueued_at, published_at, attempts, last_error, next_attempt_at
FROM outbox
WHERE tenant_id = @tenant_id
  AND published_at IS NULL
ORDER BY global_position
LIMIT @max_rows;

-- name: PendingOutboxRowsAllTenants :many
-- Cross-tenant pending rows. Used by the shared scheduled drain that
-- handles all tenants in one wake-up.
SELECT tenant_id, global_position, event_id, enqueued_at, published_at, attempts, last_error, next_attempt_at
FROM outbox
WHERE published_at IS NULL
ORDER BY global_position
LIMIT @max_rows;

-- name: PendingOutboxWithEnvelope :many
-- Drain hot path: pending rows joined to their envelope+payload,
-- filtered by retry-readiness, max-attempts (DLQ threshold), and the
-- per-stream "head" rule. The head rule (NOT EXISTS subquery) blocks
-- any row whose stream has a lower-version unpublished, not-yet-DLQ'd
-- row — preserving per-stream order even across leader handoffs when
-- backoff puts the head in cooldown. DLQ'd rows (attempts >=
-- @max_attempts) don't block, so the AutoResumeAfterDLQ=true path can
-- step past them. The DLQ-quarantines-stream semantic
-- (AutoResumeAfterDLQ=false) is layered on by the drain via
-- QuarantinedStreams, not by this query.
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
WHERE o.published_at IS NULL
  AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= @now)
  AND o.attempts < @max_attempts
  AND NOT EXISTS (
    SELECT 1
    FROM outbox o2
    JOIN events e2
      ON e2.tenant_id = o2.tenant_id
     AND e2.event_id  = o2.event_id
    WHERE e2.tenant_id = e.tenant_id
      AND e2.stream_id = e.stream_id
      AND e2.version   < e.version
      AND o2.published_at IS NULL
      AND o2.attempts < @max_attempts
  )
ORDER BY o.global_position
LIMIT @max_rows;

-- name: QuarantinedStreams :many
-- Returns the distinct (tenant_id, stream_id) pairs that have at
-- least one outbox row in DLQ state (attempts >= max_attempts AND
-- published_at IS NULL). The drain uses this to skip ALL rows of a
-- stream once any row enters DLQ, preserving per-stream order
-- (no gap delivery without operator intervention).
SELECT DISTINCT o.tenant_id, e.stream_id
FROM outbox o
JOIN events e
  ON e.tenant_id = o.tenant_id
 AND e.event_id  = o.event_id
WHERE o.published_at IS NULL
  AND o.attempts >= @max_attempts
  AND o.tenant_id = @tenant_id;

-- name: QuarantinedStreamsAllTenants :many
SELECT DISTINCT o.tenant_id, e.stream_id
FROM outbox o
JOIN events e
  ON e.tenant_id = o.tenant_id
 AND e.event_id  = o.event_id
WHERE o.published_at IS NULL
  AND o.attempts >= @max_attempts;

-- name: MarkOutboxPublished :exec
UPDATE outbox
SET published_at = clock_timestamp()
WHERE tenant_id = @tenant_id
  AND global_position = @global_position;

-- name: MarkOutboxFailed :exec
-- Increment attempts, record the last error, and set the next
-- retry-eligible time. The Drain computes next_attempt_at via its
-- backoff function and passes it in.
UPDATE outbox
SET attempts        = attempts + 1,
    last_error      = @last_error,
    next_attempt_at = @next_attempt_at
WHERE tenant_id      = @tenant_id
  AND global_position = @global_position;

-- name: CleanupPublished :exec
-- Retention pruning. Deletes rows that have been published longer
-- than @older_than. Runs in the same scheduled wake-up as the
-- publish drain.
DELETE FROM outbox
WHERE tenant_id = @tenant_id
  AND published_at IS NOT NULL
  AND published_at < @older_than;

-- ===========================================================================
-- Admin / dashboard queries
-- ===========================================================================

-- name: ListDLQ :many
-- Paginated listing of rows in DLQ state (attempts >= @max_attempts
-- AND not yet published). Pass the global_position of the last row
-- in the previous page as @after_position to fetch the next page;
-- start with @after_position = 0.
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
WHERE o.tenant_id      = @tenant_id
  AND o.published_at IS NULL
  AND o.attempts     >= @max_attempts
  AND o.global_position > @after_position
ORDER BY o.global_position
LIMIT @max_rows;

-- name: CountDLQ :one
-- Count of rows in DLQ state. Gauge metric.
SELECT COUNT(*)::bigint
FROM outbox
WHERE tenant_id    = @tenant_id
  AND published_at IS NULL
  AND attempts    >= @max_attempts;

-- name: CountPending :one
-- Total pending (unpublished) rows. Gauge metric.
SELECT COUNT(*)::bigint
FROM outbox
WHERE tenant_id    = @tenant_id
  AND published_at IS NULL;

-- name: CountFailing :one
-- Rows that have failed at least once but not yet hit DLQ.
-- Worth watching but not alarming.
SELECT COUNT(*)::bigint
FROM outbox
WHERE tenant_id    = @tenant_id
  AND published_at IS NULL
  AND attempts     > 0
  AND attempts     < @max_attempts;

-- name: OldestPendingEnqueuedAt :one
-- Returns the earliest enqueued_at among pending rows, or NULL if
-- the pending set is empty. Compute lag = now() - this in app code.
SELECT MIN(enqueued_at)::timestamptz
FROM outbox
WHERE tenant_id    = @tenant_id
  AND published_at IS NULL;

-- name: ReplayDLQ :exec
-- Reset a single row's attempts/backoff so the next drain run picks
-- it up. Used by operators after fixing root cause.
UPDATE outbox
SET attempts        = 0,
    next_attempt_at = NULL,
    last_error      = NULL
WHERE tenant_id      = @tenant_id
  AND global_position = @global_position;

-- name: AbandonDLQ :exec
-- Mark a DLQ'd row as published (set published_at = now) without
-- actually publishing. Use when an event is genuinely garbage. The
-- event itself stays in the events table (ADR 0005); only the
-- outbox row is closed.
UPDATE outbox
SET published_at = clock_timestamp(),
    last_error   = COALESCE(last_error, '') || ' [abandoned]'
WHERE tenant_id      = @tenant_id
  AND global_position = @global_position
  AND published_at IS NULL;

-- name: ReplayAllDLQ :execrows
-- Bulk-replay every DLQ'd row for a tenant. Useful after a
-- publisher outage recovery. Returns the number of rows reset.
UPDATE outbox
SET attempts        = 0,
    next_attempt_at = NULL,
    last_error      = NULL
WHERE tenant_id    = @tenant_id
  AND published_at IS NULL
  AND attempts    >= @max_attempts;
