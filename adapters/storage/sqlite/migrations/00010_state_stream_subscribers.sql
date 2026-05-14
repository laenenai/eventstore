-- +goose Up

CREATE TABLE state_stream_subscribers (
    name                   TEXT    NOT NULL,
    tenant_id              TEXT    NOT NULL DEFAULT '',
    stream_id              TEXT    NOT NULL,
    last_delivered_version INTEGER NOT NULL,
    updated_at             TEXT    NOT NULL,

    PRIMARY KEY (name, tenant_id, stream_id)
);

CREATE INDEX state_stream_subscribers_by_name_idx
    ON state_stream_subscribers (name, tenant_id);

-- +goose Down

DROP INDEX IF EXISTS state_stream_subscribers_by_name_idx;
DROP TABLE IF EXISTS state_stream_subscribers;
