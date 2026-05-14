-- +goose Up

-- ADR 0023: state_cache absorbs the snapshot role. Add the
-- state_schema_version column so the runtime can invalidate stale
-- rows after the state proto shape changes, the same way the (now
-- deprecated) snapshots table did.
--
-- Existing rows get state_schema_version = 1, matching the runtime's
-- default StateSchemaVersion when none is explicitly set. Aggregates
-- that bump StateSchemaVersion after this migration must either run
-- aggregate.RebuildStateCache or accept full-replay until the rows
-- are overwritten organically.
ALTER TABLE state_cache
    ADD COLUMN state_schema_version INTEGER NOT NULL DEFAULT 1;

-- +goose Down

ALTER TABLE state_cache DROP COLUMN IF EXISTS state_schema_version;
