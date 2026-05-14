-- +goose Up

ALTER TABLE state_cache
    ADD COLUMN state_schema_version INTEGER NOT NULL DEFAULT 1;

-- +goose Down

-- SQLite < 3.35 cannot DROP COLUMN; the recreated table from the
-- jsonb migration includes the column now, so a Down here is
-- best-effort. Modern SQLite supports DROP COLUMN directly.
ALTER TABLE state_cache DROP COLUMN state_schema_version;
