-- +goose Up

-- projection_checkpoint is the framework's default Tier-3 checkpoint
-- store. One row per (projection_name, tenant_id). See ADR 0020.
--
-- tenant_id defaults to '' for cross-tenant projectors so a tenant-
-- scoped and a cross-tenant projector with the same name can coexist.
-- Cursor is the global_position of the last successfully-processed
-- event.
CREATE TABLE projection_checkpoint (
    name        TEXT        NOT NULL,
    tenant_id   TEXT        NOT NULL DEFAULT '',
    cursor      BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (name, tenant_id)
);

-- +goose Down

DROP TABLE IF EXISTS projection_checkpoint;
