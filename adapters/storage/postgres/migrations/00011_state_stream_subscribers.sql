-- +goose Up

-- state_stream_subscribers tracks per-(subscriber, stream) delivery
-- position. The drain reads state_cache LEFT JOIN this table, finds
-- streams where last_delivered_version < state_cache.version, delivers,
-- and upserts the new version here. See ADR 0024.
--
-- Coalescing is structural: a subscriber that's an hour behind sees
-- one delivery per stream regardless of how many Appends happened in
-- the interim. Storage is bounded by (subscribers × streams), not
-- (events × subscribers).
CREATE TABLE state_stream_subscribers (
    name                   TEXT        NOT NULL,
    tenant_id              TEXT        NOT NULL DEFAULT '',
    stream_id              TEXT        NOT NULL,
    last_delivered_version BIGINT      NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (name, tenant_id, stream_id)
);

-- Index on (name, tenant_id) for Status/List queries that aggregate
-- across streams.
CREATE INDEX state_stream_subscribers_by_name_idx
    ON state_stream_subscribers (name, tenant_id);

-- +goose Down

DROP INDEX IF EXISTS state_stream_subscribers_by_name_idx;
DROP TABLE IF EXISTS state_stream_subscribers;
