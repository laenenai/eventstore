-- +goose Up

-- Implements ADR 0021: switch state_cache.state from TEXT JSON to
-- BLOB JSONB on SQLite. The jsonb() function converts existing JSON
-- text to the binary form; jsonb_extract() / jsonb_set() can then
-- query/mutate without re-parsing on every read.
--
-- Requires SQLite >= 3.45.0 (Jan 2024). modernc.org/sqlite v1.49.x
-- and libSQL recent releases both ship it.
CREATE TABLE state_cache_new (
    tenant_id   TEXT    NOT NULL,
    stream_id   TEXT    NOT NULL,
    type_url    TEXT    NOT NULL,
    state       BLOB    NOT NULL,
    version     INTEGER NOT NULL,
    terminal    INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT    NOT NULL,
    PRIMARY KEY (tenant_id, stream_id)
);

INSERT INTO state_cache_new
SELECT tenant_id, stream_id, type_url, jsonb(state), version, terminal, updated_at
FROM state_cache;

DROP TABLE state_cache;
ALTER TABLE state_cache_new RENAME TO state_cache;

CREATE INDEX state_cache_by_type_idx
    ON state_cache (tenant_id, type_url, stream_id);

-- +goose Down

CREATE TABLE state_cache_old (
    tenant_id   TEXT    NOT NULL,
    stream_id   TEXT    NOT NULL,
    type_url    TEXT    NOT NULL,
    state       TEXT    NOT NULL,
    version     INTEGER NOT NULL,
    terminal    INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT    NOT NULL,
    PRIMARY KEY (tenant_id, stream_id)
);
INSERT INTO state_cache_old
SELECT tenant_id, stream_id, type_url, json(state), version, terminal, updated_at
FROM state_cache;
DROP TABLE state_cache;
ALTER TABLE state_cache_old RENAME TO state_cache;
CREATE INDEX state_cache_by_type_idx
    ON state_cache (tenant_id, type_url, stream_id);
