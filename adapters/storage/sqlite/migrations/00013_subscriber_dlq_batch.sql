-- +goose Up

-- ADR 0029 — per-command-batch subscriber delivery changes the DLQ
-- shape. SQLite mirror of the Postgres migration: SQLite lacks native
-- arrays, so event_ids and type_urls are JSON-encoded text columns.
-- Clean break: no production deployments, so drop-and-recreate.

DROP INDEX IF EXISTS subscriber_dlq_by_name_idx;
DROP TABLE IF EXISTS subscriber_dlq;

CREATE TABLE subscriber_dlq (
    subscriber_name TEXT    NOT NULL,
    tenant_id       TEXT    NOT NULL,
    stream_id       TEXT    NOT NULL,

    -- first_event_id mirrors event_ids JSON array[0] — kept as a
    -- separate column so the primary key stays simple and queries
    -- don't need json_extract on the hot path.
    first_event_id  TEXT    NOT NULL,

    -- JSON arrays of strings. event_ids[i] is the event id; type_urls[i]
    -- is its type URL. The two arrays stay index-aligned.
    event_ids       TEXT    NOT NULL,
    type_urls       TEXT    NOT NULL,

    last_error      TEXT    NOT NULL,
    attempts        INTEGER NOT NULL,
    enqueued_at     TEXT    NOT NULL,

    PRIMARY KEY (subscriber_name, tenant_id, first_event_id)
);

CREATE INDEX subscriber_dlq_by_name_idx
    ON subscriber_dlq (subscriber_name, tenant_id, enqueued_at);

-- +goose Down

DROP INDEX IF EXISTS subscriber_dlq_by_name_idx;
DROP TABLE IF EXISTS subscriber_dlq;
