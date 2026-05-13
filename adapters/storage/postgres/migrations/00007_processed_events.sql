-- +goose Up

-- processed_events backs projection.WithDedup (ADR 0020 decision 3h
-- escape hatch). One row per (projection_name, tenant_id, event_id)
-- the wrapper has already passed to the inner handler successfully.
--
-- IMPORTANT semantic note: writes to this table happen AFTER the
-- inner handler returns. A crash between handler-success and mark-
-- written re-processes that event on the next run. WithDedup reduces
-- duplicate side effects in the common path; it is NOT exactly-once.
-- For strict EOS, push idempotency into the external system being
-- written to (idempotency keys on payments, dedup IDs on queues).
CREATE TABLE processed_events (
    projection_name TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    event_id        UUID        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (projection_name, tenant_id, event_id)
);

-- +goose Down

DROP TABLE IF EXISTS processed_events;
