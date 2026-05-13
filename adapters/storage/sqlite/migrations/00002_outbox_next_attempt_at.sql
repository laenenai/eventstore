-- +goose Up

-- next_attempt_at controls drain retry backoff. NULL = eligible
-- immediately (matches original behavior for existing rows).
ALTER TABLE outbox ADD COLUMN next_attempt_at TEXT;

-- Rebuild the partial index to include next_attempt_at so the drain
-- filter is cheap.
DROP INDEX IF EXISTS outbox_pending_idx;
CREATE INDEX outbox_pending_idx
    ON outbox (tenant_id, next_attempt_at, global_position)
    WHERE published_at IS NULL;

-- +goose Down

DROP INDEX IF EXISTS outbox_pending_idx;
CREATE INDEX outbox_pending_idx
    ON outbox (tenant_id, global_position)
    WHERE published_at IS NULL;
ALTER TABLE outbox DROP COLUMN next_attempt_at;
