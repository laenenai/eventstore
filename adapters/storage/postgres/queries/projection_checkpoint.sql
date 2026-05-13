-- projection_checkpoint queries (ADR 0020 Tier 3, decision 3e).

-- name: LoadProjectionCheckpoint :one
-- Returns the cursor for a projector. NULL row → 0 (never run).
SELECT cursor
FROM projection_checkpoint
WHERE name      = @name
  AND tenant_id = @tenant_id;

-- name: SaveProjectionCheckpoint :exec
-- Upsert the cursor. Called after each batch (or each successful
-- partial-batch, per the fail-stop-with-last-success rule).
INSERT INTO projection_checkpoint (name, tenant_id, cursor, updated_at)
VALUES (@name, @tenant_id, @cursor, clock_timestamp())
ON CONFLICT (name, tenant_id) DO UPDATE SET
    cursor     = EXCLUDED.cursor,
    updated_at = EXCLUDED.updated_at;

-- name: ResetProjectionCheckpoint :exec
-- Operator action: set cursor to 0 (or remove the row, equivalent).
-- Used as step (3) of the truncate-and-replay rebuild workflow.
INSERT INTO projection_checkpoint (name, tenant_id, cursor, updated_at)
VALUES (@name, @tenant_id, 0, clock_timestamp())
ON CONFLICT (name, tenant_id) DO UPDATE SET
    cursor     = 0,
    updated_at = clock_timestamp();

-- name: SetProjectionCheckpoint :exec
-- Operator action: set cursor to a specific position (for partial
-- replay from a known-good point).
INSERT INTO projection_checkpoint (name, tenant_id, cursor, updated_at)
VALUES (@name, @tenant_id, @cursor, clock_timestamp())
ON CONFLICT (name, tenant_id) DO UPDATE SET
    cursor     = @cursor,
    updated_at = clock_timestamp();

-- name: ListProjectionCheckpoints :many
-- Enumerate all known projectors for an ops dashboard.
SELECT name, tenant_id, cursor, updated_at
FROM projection_checkpoint
ORDER BY name, tenant_id;

-- name: GetProjectionStatus :one
-- Single-projector status row (for the admin Status method).
SELECT name, tenant_id, cursor, updated_at
FROM projection_checkpoint
WHERE name      = @name
  AND tenant_id = @tenant_id;
