-- +goose Up

-- projection_checkpoint is the framework's default Tier-3 checkpoint
-- store. See ADR 0020.
CREATE TABLE projection_checkpoint (
    name        TEXT    NOT NULL,
    tenant_id   TEXT    NOT NULL DEFAULT '',
    cursor      INTEGER NOT NULL,
    updated_at  TEXT    NOT NULL,  -- ISO-8601 UTC

    PRIMARY KEY (name, tenant_id)
);

-- +goose Down

DROP TABLE IF EXISTS projection_checkpoint;
