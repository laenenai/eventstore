-- +goose Up

-- subscriber_dlq captures envelopes whose Subscriber.Handle failed
-- past its MaxRetries with OnExhausted = DLQ (ADR 0025). One row per
-- (subscriber, tenant, event). Operator inspects via SubscriberDLQAdmin
-- and decides: replay, clear, or run state_stream.Drain to refresh
-- state-shaped subscribers from current state_cache.
CREATE TABLE subscriber_dlq (
    subscriber_name TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    event_id        TEXT        NOT NULL,
    stream_id       TEXT        NOT NULL,
    type_url        TEXT        NOT NULL,
    last_error      TEXT        NOT NULL,
    attempts        INTEGER     NOT NULL,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (subscriber_name, tenant_id, event_id)
);

-- Index for the common admin query: list this subscriber's DLQ
-- ordered by arrival.
CREATE INDEX subscriber_dlq_by_name_idx
    ON subscriber_dlq (subscriber_name, tenant_id, enqueued_at);

-- +goose Down

DROP INDEX IF EXISTS subscriber_dlq_by_name_idx;
DROP TABLE IF EXISTS subscriber_dlq;
