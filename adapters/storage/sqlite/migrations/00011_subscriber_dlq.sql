-- +goose Up

-- subscriber_dlq — see ADR 0025. SQLite mirror of the Postgres
-- migration; timestamps stored as TEXT (ISO 8601) per the SQLite
-- convention used elsewhere in this adapter.
CREATE TABLE subscriber_dlq (
    subscriber_name TEXT    NOT NULL,
    tenant_id       TEXT    NOT NULL,
    event_id        TEXT    NOT NULL,
    stream_id       TEXT    NOT NULL,
    type_url        TEXT    NOT NULL,
    last_error      TEXT    NOT NULL,
    attempts        INTEGER NOT NULL,
    enqueued_at     TEXT    NOT NULL,

    PRIMARY KEY (subscriber_name, tenant_id, event_id)
);

CREATE INDEX subscriber_dlq_by_name_idx
    ON subscriber_dlq (subscriber_name, tenant_id, enqueued_at);

-- +goose Down

DROP INDEX IF EXISTS subscriber_dlq_by_name_idx;
DROP TABLE IF EXISTS subscriber_dlq;
