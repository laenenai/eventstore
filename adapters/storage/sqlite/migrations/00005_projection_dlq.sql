-- +goose Up

-- projection_dlq for SQLite. See Postgres sibling for rationale.
CREATE TABLE projection_dlq (
    projection_name TEXT    NOT NULL,
    tenant_id       TEXT    NOT NULL,
    global_position INTEGER NOT NULL,
    event_id        TEXT    NOT NULL,
    type_url        TEXT    NOT NULL,
    last_error      TEXT    NOT NULL,
    enqueued_at     TEXT    NOT NULL,

    PRIMARY KEY (projection_name, tenant_id, global_position)
);

CREATE INDEX projection_dlq_by_name_idx
    ON projection_dlq (projection_name, tenant_id, global_position);

-- +goose Down

DROP INDEX IF EXISTS projection_dlq_by_name_idx;
DROP TABLE IF EXISTS projection_dlq;
