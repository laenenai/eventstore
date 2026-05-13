-- +goose Up

-- ===========================================================================
-- Hash partition children for the parent tables created in 00001.
--
-- 16 hash partitions per parent table. Operators with a small known
-- tenant set may swap this migration for a LIST-partitioning variant
-- before first deployment.
--
-- This file is read by goose but NOT by sqlc — sqlc would otherwise
-- generate redundant per-partition type structs.
-- ===========================================================================

CREATE TABLE events_p00 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE events_p01 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE events_p02 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE events_p03 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE events_p04 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE events_p05 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE events_p06 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE events_p07 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE events_p08 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE events_p09 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE events_p10 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE events_p11 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE events_p12 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE events_p13 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE events_p14 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE events_p15 PARTITION OF events FOR VALUES WITH (modulus 16, remainder 15);

CREATE TABLE unique_claims_p00 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE unique_claims_p01 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE unique_claims_p02 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE unique_claims_p03 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE unique_claims_p04 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE unique_claims_p05 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE unique_claims_p06 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE unique_claims_p07 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE unique_claims_p08 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE unique_claims_p09 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE unique_claims_p10 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE unique_claims_p11 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE unique_claims_p12 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE unique_claims_p13 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE unique_claims_p14 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE unique_claims_p15 PARTITION OF unique_claims FOR VALUES WITH (modulus 16, remainder 15);

CREATE TABLE subject_keys_p00 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE subject_keys_p01 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE subject_keys_p02 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE subject_keys_p03 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE subject_keys_p04 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE subject_keys_p05 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE subject_keys_p06 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE subject_keys_p07 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE subject_keys_p08 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE subject_keys_p09 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE subject_keys_p10 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE subject_keys_p11 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE subject_keys_p12 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE subject_keys_p13 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE subject_keys_p14 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE subject_keys_p15 PARTITION OF subject_keys FOR VALUES WITH (modulus 16, remainder 15);

CREATE TABLE snapshots_p00 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE snapshots_p01 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE snapshots_p02 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE snapshots_p03 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE snapshots_p04 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE snapshots_p05 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE snapshots_p06 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE snapshots_p07 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE snapshots_p08 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE snapshots_p09 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE snapshots_p10 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE snapshots_p11 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE snapshots_p12 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE snapshots_p13 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE snapshots_p14 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE snapshots_p15 PARTITION OF snapshots FOR VALUES WITH (modulus 16, remainder 15);

CREATE TABLE outbox_p00 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE outbox_p01 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE outbox_p02 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE outbox_p03 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE outbox_p04 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE outbox_p05 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE outbox_p06 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE outbox_p07 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE outbox_p08 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE outbox_p09 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE outbox_p10 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE outbox_p11 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE outbox_p12 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE outbox_p13 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE outbox_p14 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE outbox_p15 PARTITION OF outbox FOR VALUES WITH (modulus 16, remainder 15);

-- +goose Down

-- The 00001 down migration uses CASCADE to drop parents along with these
-- partition children, so this down is intentionally empty.
SELECT 1;
