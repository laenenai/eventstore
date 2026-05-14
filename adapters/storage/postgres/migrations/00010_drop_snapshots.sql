-- +goose Up

-- ADR 0023: snapshots are superseded by state_cache. state_cache
-- carries state_schema_version since migration 00009; aggregate.Runtime
-- now reads from state_cache instead of snapshots. Drop the redundant
-- table.
DROP TABLE IF EXISTS snapshots CASCADE;

-- +goose Down

-- Re-creating the snapshots table on downgrade is ADR 0011's job; this
-- migration cannot meaningfully roll back without re-implementing the
-- old infrastructure. Operators who need to roll back can re-apply the
-- original 00001_initial_schema.sql snapshots block by hand.
SELECT 1;
