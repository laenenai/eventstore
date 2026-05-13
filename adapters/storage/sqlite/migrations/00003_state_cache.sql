-- +goose Up

-- state_cache is the Tier 1 read-model: one row per stream, holding
-- the current state computed via Decider.Evolve at write time and
-- committed in the same transaction as the events. See ADR 0020.
--
-- SQLite stores JSON as TEXT; the JSON1 extension (json_extract,
-- json_set, etc.) operates on it directly.
CREATE TABLE state_cache (
    tenant_id   TEXT    NOT NULL,
    stream_id   TEXT    NOT NULL,
    type_url    TEXT    NOT NULL,
    state       TEXT    NOT NULL,
    version     INTEGER NOT NULL,
    terminal    INTEGER NOT NULL DEFAULT 0,  -- 0 / 1
    updated_at  TEXT    NOT NULL,            -- ISO-8601 UTC

    PRIMARY KEY (tenant_id, stream_id)
);

CREATE INDEX state_cache_by_type_idx
    ON state_cache (tenant_id, type_url, stream_id);

-- +goose Down

DROP INDEX IF EXISTS state_cache_by_type_idx;
DROP TABLE  IF EXISTS state_cache;
