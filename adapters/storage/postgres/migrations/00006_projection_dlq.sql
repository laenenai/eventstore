-- +goose Up

-- projection_dlq captures events whose handler failed under
-- DLQOnFailure mode (ADR 0020 deferred). The cursor advances past the
-- captured event so the projection keeps making progress; operators
-- inspect via ProjectionAdmin and decide whether to ResetTo for replay
-- or Abandon for permanent skip.
CREATE TABLE projection_dlq (
    projection_name TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    global_position BIGINT      NOT NULL,
    event_id        UUID        NOT NULL,
    type_url        TEXT        NOT NULL,
    last_error      TEXT        NOT NULL,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (projection_name, tenant_id, global_position)
);

-- Index for listing DLQ entries for one projection (admin dashboard).
CREATE INDEX projection_dlq_by_name_idx
    ON projection_dlq (projection_name, tenant_id, global_position);

-- +goose Down

DROP INDEX IF EXISTS projection_dlq_by_name_idx;
DROP TABLE IF EXISTS projection_dlq;
