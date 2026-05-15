-- +goose Up

-- ADR 0029 — per-command-batch subscriber delivery changes the DLQ
-- shape. One row per (subscriber, failed command-batch) instead of
-- (subscriber, failed event). event_ids[] and type_urls[] carry the
-- whole batch the subscriber received; replay / inspection happens
-- against the batch as a unit. Clean break: there are no production
-- deployments to migrate, so drop-and-recreate is the cleanest move.

DROP INDEX IF EXISTS subscriber_dlq_by_name_idx;
DROP TABLE IF EXISTS subscriber_dlq;

CREATE TABLE subscriber_dlq (
    subscriber_name TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    stream_id       TEXT        NOT NULL,

    -- first_event_id is the leading event id from event_ids. Under OCC
    -- a stream cannot produce overlapping command-batches, so the
    -- first event id uniquely identifies the failed batch within
    -- (subscriber, tenant). Operators key replay / delete on it.
    first_event_id  TEXT        NOT NULL,

    -- The batch the subscriber received when it exhausted. Arrays
    -- stay index-aligned: type_urls[i] is the type URL of the event
    -- with id event_ids[i]. event_ids[0] mirrors first_event_id.
    event_ids       TEXT[]      NOT NULL,
    type_urls       TEXT[]      NOT NULL,

    last_error      TEXT        NOT NULL,
    attempts        INT         NOT NULL,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (subscriber_name, tenant_id, first_event_id)
);

CREATE INDEX subscriber_dlq_by_name_idx
    ON subscriber_dlq (subscriber_name, tenant_id, enqueued_at);

-- +goose Down

DROP INDEX IF EXISTS subscriber_dlq_by_name_idx;
DROP TABLE IF EXISTS subscriber_dlq;
