-- +goose Up

CREATE TABLE processed_events (
    projection_name TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    processed_at    TEXT NOT NULL,

    PRIMARY KEY (projection_name, tenant_id, event_id)
);

-- +goose Down

DROP TABLE IF EXISTS processed_events;
