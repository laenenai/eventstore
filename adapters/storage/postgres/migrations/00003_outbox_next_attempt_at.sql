-- +goose Up

-- next_attempt_at controls drain retry backoff. NULL = eligible
-- immediately (matches original behavior for existing rows).
ALTER TABLE outbox ADD COLUMN next_attempt_at TIMESTAMPTZ;

-- The drain's pending query now filters on (next_attempt_at IS NULL
-- OR next_attempt_at <= now()). Rebuild the partial index to make
-- this filter cheap. NULLS FIRST so newly-enqueued rows
-- (next_attempt_at = NULL) sort ahead of backed-off rows of equal
-- global_position.
DROP INDEX IF EXISTS outbox_pending_idx;
CREATE INDEX outbox_pending_idx
    ON outbox (tenant_id, next_attempt_at NULLS FIRST, global_position)
    WHERE published_at IS NULL;

-- +goose Down

DROP INDEX IF EXISTS outbox_pending_idx;
CREATE INDEX outbox_pending_idx
    ON outbox (tenant_id, global_position)
    WHERE published_at IS NULL;
ALTER TABLE outbox DROP COLUMN IF EXISTS next_attempt_at;
