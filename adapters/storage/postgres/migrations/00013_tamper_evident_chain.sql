-- +goose Up

-- ADR 0028 — per-stream SHA-256 hash chain. Each event row carries
-- its own hash and the hash of its predecessor in the same stream.
-- 32 bytes of zero for the genesis event (version=1).
--
-- The columns are NULL during the migration window so existing rows
-- pass through; new appends populate them. A backfill helper
-- (es.RebuildStreamChain, ADR 0028 § verification) lets operators
-- chain pre-existing streams retroactively.

ALTER TABLE events ADD COLUMN hash      BYTEA;
ALTER TABLE events ADD COLUMN prev_hash BYTEA;

-- +goose Down

ALTER TABLE events DROP COLUMN prev_hash;
ALTER TABLE events DROP COLUMN hash;
