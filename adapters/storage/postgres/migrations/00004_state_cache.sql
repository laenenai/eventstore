-- +goose Up

-- state_cache is the Tier 1 read-model: one row per stream, holding
-- the current state computed via Decider.Evolve at write time and
-- committed in the same transaction as the events. See ADR 0020.
--
-- Opt-in per aggregate via aggregate.Runtime.StateCodec; when not
-- set, this table stays empty for that aggregate.
CREATE TABLE state_cache (
    tenant_id   TEXT        NOT NULL,
    stream_id   TEXT        NOT NULL,
    type_url    TEXT        NOT NULL,
    state       JSONB       NOT NULL,
    version     BIGINT      NOT NULL,
    terminal    BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (tenant_id, stream_id)
);

-- ListStates by type_url + pagination uses this. The (tenant_id,
-- type_url, stream_id) ordering supports the canonical paged-list
-- query (after_stream_id parameter) without a separate sort.
CREATE INDEX state_cache_by_type_idx
    ON state_cache (tenant_id, type_url, stream_id);

-- +goose Down

DROP INDEX IF EXISTS state_cache_by_type_idx;
DROP TABLE  IF EXISTS state_cache;
