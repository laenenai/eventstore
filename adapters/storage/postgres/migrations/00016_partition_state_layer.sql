-- +goose Up
-- +goose StatementBegin

-- ===========================================================================
-- Spike 0001 — partition the state-cache / projection layer.
--
-- The audit at docs/spikes/0001-laenen-tenancy.md §11.1 identified four
-- hot-write tables shipping today as single (unpartitioned) tables:
--
--   state_cache               — Tier-1 read model; UPSERT on every command
--   projection_checkpoint     — UPDATE on every projection batch
--   processed_events          — INSERT per (dedup-enabled projection, event)
--   state_stream_subscribers  — UPSERT per state-stream delivery
--
-- ADR 0007's partition strategy addresses the event path (events,
-- unique_claims, subject_keys, outbox — all hash(tenant_id) × 16). This
-- migration extends that strategy to the state-cache layer. After this
-- migration, every tenant-scoped table in the framework is hash-partitioned
-- on tenant_id.
--
-- # Data preservation
--
-- This migration MUST NOT lose existing data. PostgreSQL has no in-place
-- ALTER TABLE ... PARTITION BY syntax — partitioning an existing
-- non-partitioned table requires create-new + copy + rename + drop-old.
-- That is what this migration does, per table, inside a single goose
-- transaction. If the migration aborts at any point, the goose
-- transaction rolls back and the original tables are unchanged.
--
-- Sequence per table:
--   1. Disable RLS on the legacy table so the INSERT … SELECT below
--      reads every row regardless of the session role's RLS view.
--   2. Create <table>_partitioned as the new parent with the same
--      column types + PARTITION BY HASH (tenant_id).
--   3. Create 16 partition children matching the events convention.
--   4. Re-create non-PK indexes on the new parent.
--   5. Enable + force RLS on the new parent and re-create the
--      tenant_isolation policy (RLS on a parent propagates to all
--      children; the policy must be re-declared on the new parent
--      because policies are table-scoped, not name-scoped).
--   6. INSERT INTO <table>_partitioned SELECT * FROM <table>.
--   7. Row-count check: if old count != new count, raise an exception
--      that rolls back the whole migration.
--   8. DROP TABLE <table> (the legacy non-partitioned one).
--   9. ALTER TABLE <table>_partitioned RENAME TO <table>.
--  10. Rename each partition child to drop the _partitioned suffix.
--
-- Empty databases skip the data-copy step trivially (INSERT inserts
-- zero rows, count check passes 0 = 0). The migration is correct in
-- both cases without a guard.
--
-- # Class A operational tunings folded in
--
-- The same migration also lands the autovacuum + fillfactor tunings
-- the audit identified as zero-risk wins (§11.1.6 Class A):
--   - state_cache:           fillfactor=85, autovacuum 0.05
--   - projection_checkpoint: autovacuum 0.02
-- These apply to the new parent (and propagate to children).
--
-- # Compatibility note
--
-- The Go public API is unchanged. sqlc-generated query code reads
-- only the parent tables, which keep their original names after
-- the rename — no application-level changes are required.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1 of 4: state_cache
-- ---------------------------------------------------------------------------

-- Disable RLS on the legacy table so the data copy reads all rows.
ALTER TABLE state_cache NO FORCE ROW LEVEL SECURITY;
ALTER TABLE state_cache DISABLE ROW LEVEL SECURITY;

-- Storage parameters (fillfactor, autovacuum_*) cannot be set on a
-- partitioned parent — Postgres rejects them with SQLSTATE 42809. We
-- apply them to each child partition via ALTER TABLE below, after
-- creating the partition tree.
CREATE TABLE state_cache_partitioned (
    tenant_id            TEXT        NOT NULL,
    stream_id            TEXT        NOT NULL,
    type_url             TEXT        NOT NULL,
    state                JSONB       NOT NULL,
    version              BIGINT      NOT NULL,
    terminal             BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    state_schema_version INTEGER     NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, stream_id)
) PARTITION BY HASH (tenant_id);

CREATE TABLE state_cache_partitioned_p00 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE state_cache_partitioned_p01 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE state_cache_partitioned_p02 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE state_cache_partitioned_p03 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE state_cache_partitioned_p04 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE state_cache_partitioned_p05 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE state_cache_partitioned_p06 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE state_cache_partitioned_p07 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE state_cache_partitioned_p08 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE state_cache_partitioned_p09 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE state_cache_partitioned_p10 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE state_cache_partitioned_p11 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE state_cache_partitioned_p12 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE state_cache_partitioned_p13 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE state_cache_partitioned_p14 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE state_cache_partitioned_p15 PARTITION OF state_cache_partitioned FOR VALUES WITH (modulus 16, remainder 15);

-- Per-child storage tuning: fillfactor=85 enables HOT updates when
-- the JSONB column grows in place; the lower autovacuum scale factor
-- keeps dead-tuple cleanup ahead of UPSERT churn at 1M-tenant scale.
-- See spike 0001 §11.1.6 Class A.
DO $$
DECLARE
    p INT;
BEGIN
    FOR p IN 0..15 LOOP
        EXECUTE format(
            'ALTER TABLE state_cache_partitioned_p%s SET (fillfactor = 85, autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 2000)',
            lpad(p::text, 2, '0')
        );
    END LOOP;
END $$;

CREATE INDEX state_cache_partitioned_by_type_idx
    ON state_cache_partitioned (tenant_id, type_url, stream_id);

-- RLS + GRANT are conditional on the eventstore_app role existing,
-- which gates whether ADR 0032's role-separation model is in use.
-- The WithoutRLS adapter option excludes migration 00015 and so
-- never creates these roles; this migration must remain a no-op
-- for that mode.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_app') THEN
        EXECUTE 'ALTER TABLE state_cache_partitioned ENABLE ROW LEVEL SECURITY';
        EXECUTE 'ALTER TABLE state_cache_partitioned FORCE ROW LEVEL SECURITY';
        EXECUTE 'CREATE POLICY tenant_isolation ON state_cache_partitioned ' ||
                'USING      (tenant_id = current_setting(''app.tenant_id'', false)) ' ||
                'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', false))';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON state_cache_partitioned TO eventstore_app, eventstore_admin';
    END IF;
END $$;

-- Copy data. INSERT bypasses the WITH CHECK policy on the new table
-- because we're running under the admin/migration role (no
-- current_setting('app.tenant_id') is set; the policy would block writes
-- if it were enforced). Temporarily disable FORCE so the migration
-- proceeds; re-enable below after the copy.
ALTER TABLE state_cache_partitioned NO FORCE ROW LEVEL SECURITY;
INSERT INTO state_cache_partitioned (tenant_id, stream_id, type_url, state, version, terminal, updated_at, state_schema_version)
SELECT tenant_id, stream_id, type_url, state, version, terminal, updated_at, state_schema_version
FROM state_cache;
ALTER TABLE state_cache_partitioned FORCE ROW LEVEL SECURITY;

-- Row-count check.
DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT count(*) INTO old_count FROM state_cache;
    SELECT count(*) INTO new_count FROM state_cache_partitioned;
    IF old_count != new_count THEN
        RAISE EXCEPTION 'migration 00016: state_cache row count mismatch: old=%, new=%', old_count, new_count;
    END IF;
END $$;

DROP TABLE state_cache;
ALTER TABLE state_cache_partitioned RENAME TO state_cache;
ALTER INDEX state_cache_partitioned_by_type_idx RENAME TO state_cache_by_type_idx;

ALTER TABLE state_cache_partitioned_p00 RENAME TO state_cache_p00;
ALTER TABLE state_cache_partitioned_p01 RENAME TO state_cache_p01;
ALTER TABLE state_cache_partitioned_p02 RENAME TO state_cache_p02;
ALTER TABLE state_cache_partitioned_p03 RENAME TO state_cache_p03;
ALTER TABLE state_cache_partitioned_p04 RENAME TO state_cache_p04;
ALTER TABLE state_cache_partitioned_p05 RENAME TO state_cache_p05;
ALTER TABLE state_cache_partitioned_p06 RENAME TO state_cache_p06;
ALTER TABLE state_cache_partitioned_p07 RENAME TO state_cache_p07;
ALTER TABLE state_cache_partitioned_p08 RENAME TO state_cache_p08;
ALTER TABLE state_cache_partitioned_p09 RENAME TO state_cache_p09;
ALTER TABLE state_cache_partitioned_p10 RENAME TO state_cache_p10;
ALTER TABLE state_cache_partitioned_p11 RENAME TO state_cache_p11;
ALTER TABLE state_cache_partitioned_p12 RENAME TO state_cache_p12;
ALTER TABLE state_cache_partitioned_p13 RENAME TO state_cache_p13;
ALTER TABLE state_cache_partitioned_p14 RENAME TO state_cache_p14;
ALTER TABLE state_cache_partitioned_p15 RENAME TO state_cache_p15;

-- ---------------------------------------------------------------------------
-- 2 of 4: projection_checkpoint
-- ---------------------------------------------------------------------------

ALTER TABLE projection_checkpoint NO FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_checkpoint DISABLE ROW LEVEL SECURITY;

-- See state_cache section above for the storage-parameters-on-parent
-- constraint; applied to children below.
CREATE TABLE projection_checkpoint_partitioned (
    name        TEXT        NOT NULL,
    tenant_id   TEXT        NOT NULL DEFAULT '',
    cursor      BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (name, tenant_id)
) PARTITION BY HASH (tenant_id);

CREATE TABLE projection_checkpoint_partitioned_p00 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE projection_checkpoint_partitioned_p01 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE projection_checkpoint_partitioned_p02 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE projection_checkpoint_partitioned_p03 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE projection_checkpoint_partitioned_p04 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE projection_checkpoint_partitioned_p05 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE projection_checkpoint_partitioned_p06 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE projection_checkpoint_partitioned_p07 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE projection_checkpoint_partitioned_p08 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE projection_checkpoint_partitioned_p09 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE projection_checkpoint_partitioned_p10 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE projection_checkpoint_partitioned_p11 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE projection_checkpoint_partitioned_p12 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE projection_checkpoint_partitioned_p13 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE projection_checkpoint_partitioned_p14 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE projection_checkpoint_partitioned_p15 PARTITION OF projection_checkpoint_partitioned FOR VALUES WITH (modulus 16, remainder 15);

-- Per-child autovacuum tuning: more aggressive than state_cache
-- because this table is small + extremely hot (one UPDATE per
-- projection batch).
DO $$
DECLARE
    p INT;
BEGIN
    FOR p IN 0..15 LOOP
        EXECUTE format(
            'ALTER TABLE projection_checkpoint_partitioned_p%s SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_cost_limit = 2000)',
            lpad(p::text, 2, '0')
        );
    END LOOP;
END $$;

-- The RLS policy for projection_checkpoint admits the empty-string
-- tenant_id sentinel (per the original migration 00005). Cross-tenant
-- projectors set tenant_id = '' to share one checkpoint row across all
-- tenants; the policy must allow that even when app.tenant_id is set to
-- a specific tenant. The events / outbox policies don't carry this
-- complication.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_app') THEN
        EXECUTE 'ALTER TABLE projection_checkpoint_partitioned ENABLE ROW LEVEL SECURITY';
        EXECUTE 'ALTER TABLE projection_checkpoint_partitioned FORCE ROW LEVEL SECURITY';
        EXECUTE 'CREATE POLICY tenant_isolation ON projection_checkpoint_partitioned ' ||
                'USING      (tenant_id = current_setting(''app.tenant_id'', false)) ' ||
                'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', false))';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON projection_checkpoint_partitioned TO eventstore_app, eventstore_admin';
    END IF;
END $$;

ALTER TABLE projection_checkpoint_partitioned NO FORCE ROW LEVEL SECURITY;
INSERT INTO projection_checkpoint_partitioned (name, tenant_id, cursor, updated_at)
SELECT name, tenant_id, cursor, updated_at FROM projection_checkpoint;
ALTER TABLE projection_checkpoint_partitioned FORCE ROW LEVEL SECURITY;

DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT count(*) INTO old_count FROM projection_checkpoint;
    SELECT count(*) INTO new_count FROM projection_checkpoint_partitioned;
    IF old_count != new_count THEN
        RAISE EXCEPTION 'migration 00016: projection_checkpoint row count mismatch: old=%, new=%', old_count, new_count;
    END IF;
END $$;

DROP TABLE projection_checkpoint;
ALTER TABLE projection_checkpoint_partitioned RENAME TO projection_checkpoint;

ALTER TABLE projection_checkpoint_partitioned_p00 RENAME TO projection_checkpoint_p00;
ALTER TABLE projection_checkpoint_partitioned_p01 RENAME TO projection_checkpoint_p01;
ALTER TABLE projection_checkpoint_partitioned_p02 RENAME TO projection_checkpoint_p02;
ALTER TABLE projection_checkpoint_partitioned_p03 RENAME TO projection_checkpoint_p03;
ALTER TABLE projection_checkpoint_partitioned_p04 RENAME TO projection_checkpoint_p04;
ALTER TABLE projection_checkpoint_partitioned_p05 RENAME TO projection_checkpoint_p05;
ALTER TABLE projection_checkpoint_partitioned_p06 RENAME TO projection_checkpoint_p06;
ALTER TABLE projection_checkpoint_partitioned_p07 RENAME TO projection_checkpoint_p07;
ALTER TABLE projection_checkpoint_partitioned_p08 RENAME TO projection_checkpoint_p08;
ALTER TABLE projection_checkpoint_partitioned_p09 RENAME TO projection_checkpoint_p09;
ALTER TABLE projection_checkpoint_partitioned_p10 RENAME TO projection_checkpoint_p10;
ALTER TABLE projection_checkpoint_partitioned_p11 RENAME TO projection_checkpoint_p11;
ALTER TABLE projection_checkpoint_partitioned_p12 RENAME TO projection_checkpoint_p12;
ALTER TABLE projection_checkpoint_partitioned_p13 RENAME TO projection_checkpoint_p13;
ALTER TABLE projection_checkpoint_partitioned_p14 RENAME TO projection_checkpoint_p14;
ALTER TABLE projection_checkpoint_partitioned_p15 RENAME TO projection_checkpoint_p15;

-- ---------------------------------------------------------------------------
-- 3 of 4: processed_events
-- ---------------------------------------------------------------------------

ALTER TABLE processed_events NO FORCE ROW LEVEL SECURITY;
ALTER TABLE processed_events DISABLE ROW LEVEL SECURITY;

CREATE TABLE processed_events_partitioned (
    projection_name TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    event_id        UUID        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection_name, tenant_id, event_id)
) PARTITION BY HASH (tenant_id);

CREATE TABLE processed_events_partitioned_p00 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE processed_events_partitioned_p01 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE processed_events_partitioned_p02 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE processed_events_partitioned_p03 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE processed_events_partitioned_p04 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE processed_events_partitioned_p05 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE processed_events_partitioned_p06 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE processed_events_partitioned_p07 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE processed_events_partitioned_p08 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE processed_events_partitioned_p09 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE processed_events_partitioned_p10 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE processed_events_partitioned_p11 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE processed_events_partitioned_p12 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE processed_events_partitioned_p13 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE processed_events_partitioned_p14 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE processed_events_partitioned_p15 PARTITION OF processed_events_partitioned FOR VALUES WITH (modulus 16, remainder 15);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_app') THEN
        EXECUTE 'ALTER TABLE processed_events_partitioned ENABLE ROW LEVEL SECURITY';
        EXECUTE 'ALTER TABLE processed_events_partitioned FORCE ROW LEVEL SECURITY';
        EXECUTE 'CREATE POLICY tenant_isolation ON processed_events_partitioned ' ||
                'USING      (tenant_id = current_setting(''app.tenant_id'', false)) ' ||
                'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', false))';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON processed_events_partitioned TO eventstore_app, eventstore_admin';
    END IF;
END $$;

ALTER TABLE processed_events_partitioned NO FORCE ROW LEVEL SECURITY;
INSERT INTO processed_events_partitioned (projection_name, tenant_id, event_id, processed_at)
SELECT projection_name, tenant_id, event_id, processed_at FROM processed_events;
ALTER TABLE processed_events_partitioned FORCE ROW LEVEL SECURITY;

DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT count(*) INTO old_count FROM processed_events;
    SELECT count(*) INTO new_count FROM processed_events_partitioned;
    IF old_count != new_count THEN
        RAISE EXCEPTION 'migration 00016: processed_events row count mismatch: old=%, new=%', old_count, new_count;
    END IF;
END $$;

DROP TABLE processed_events;
ALTER TABLE processed_events_partitioned RENAME TO processed_events;

ALTER TABLE processed_events_partitioned_p00 RENAME TO processed_events_p00;
ALTER TABLE processed_events_partitioned_p01 RENAME TO processed_events_p01;
ALTER TABLE processed_events_partitioned_p02 RENAME TO processed_events_p02;
ALTER TABLE processed_events_partitioned_p03 RENAME TO processed_events_p03;
ALTER TABLE processed_events_partitioned_p04 RENAME TO processed_events_p04;
ALTER TABLE processed_events_partitioned_p05 RENAME TO processed_events_p05;
ALTER TABLE processed_events_partitioned_p06 RENAME TO processed_events_p06;
ALTER TABLE processed_events_partitioned_p07 RENAME TO processed_events_p07;
ALTER TABLE processed_events_partitioned_p08 RENAME TO processed_events_p08;
ALTER TABLE processed_events_partitioned_p09 RENAME TO processed_events_p09;
ALTER TABLE processed_events_partitioned_p10 RENAME TO processed_events_p10;
ALTER TABLE processed_events_partitioned_p11 RENAME TO processed_events_p11;
ALTER TABLE processed_events_partitioned_p12 RENAME TO processed_events_p12;
ALTER TABLE processed_events_partitioned_p13 RENAME TO processed_events_p13;
ALTER TABLE processed_events_partitioned_p14 RENAME TO processed_events_p14;
ALTER TABLE processed_events_partitioned_p15 RENAME TO processed_events_p15;

-- ---------------------------------------------------------------------------
-- 4 of 4: state_stream_subscribers
-- ---------------------------------------------------------------------------

ALTER TABLE state_stream_subscribers NO FORCE ROW LEVEL SECURITY;
ALTER TABLE state_stream_subscribers DISABLE ROW LEVEL SECURITY;

CREATE TABLE state_stream_subscribers_partitioned (
    name                   TEXT        NOT NULL,
    tenant_id              TEXT        NOT NULL DEFAULT '',
    stream_id              TEXT        NOT NULL,
    last_delivered_version BIGINT      NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (name, tenant_id, stream_id)
) PARTITION BY HASH (tenant_id);

CREATE TABLE state_stream_subscribers_partitioned_p00 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE state_stream_subscribers_partitioned_p01 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE state_stream_subscribers_partitioned_p02 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE state_stream_subscribers_partitioned_p03 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE state_stream_subscribers_partitioned_p04 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE state_stream_subscribers_partitioned_p05 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE state_stream_subscribers_partitioned_p06 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE state_stream_subscribers_partitioned_p07 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE state_stream_subscribers_partitioned_p08 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE state_stream_subscribers_partitioned_p09 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE state_stream_subscribers_partitioned_p10 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE state_stream_subscribers_partitioned_p11 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE state_stream_subscribers_partitioned_p12 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE state_stream_subscribers_partitioned_p13 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE state_stream_subscribers_partitioned_p14 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE state_stream_subscribers_partitioned_p15 PARTITION OF state_stream_subscribers_partitioned FOR VALUES WITH (modulus 16, remainder 15);

CREATE INDEX state_stream_subscribers_partitioned_by_name_idx
    ON state_stream_subscribers_partitioned (name, tenant_id);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_app') THEN
        EXECUTE 'ALTER TABLE state_stream_subscribers_partitioned ENABLE ROW LEVEL SECURITY';
        EXECUTE 'ALTER TABLE state_stream_subscribers_partitioned FORCE ROW LEVEL SECURITY';
        EXECUTE 'CREATE POLICY tenant_isolation ON state_stream_subscribers_partitioned ' ||
                'USING      (tenant_id = current_setting(''app.tenant_id'', false)) ' ||
                'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', false))';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON state_stream_subscribers_partitioned TO eventstore_app, eventstore_admin';
    END IF;
END $$;

ALTER TABLE state_stream_subscribers_partitioned NO FORCE ROW LEVEL SECURITY;
INSERT INTO state_stream_subscribers_partitioned (name, tenant_id, stream_id, last_delivered_version, updated_at)
SELECT name, tenant_id, stream_id, last_delivered_version, updated_at FROM state_stream_subscribers;
ALTER TABLE state_stream_subscribers_partitioned FORCE ROW LEVEL SECURITY;

DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT count(*) INTO old_count FROM state_stream_subscribers;
    SELECT count(*) INTO new_count FROM state_stream_subscribers_partitioned;
    IF old_count != new_count THEN
        RAISE EXCEPTION 'migration 00016: state_stream_subscribers row count mismatch: old=%, new=%', old_count, new_count;
    END IF;
END $$;

DROP TABLE state_stream_subscribers;
ALTER TABLE state_stream_subscribers_partitioned RENAME TO state_stream_subscribers;
ALTER INDEX state_stream_subscribers_partitioned_by_name_idx RENAME TO state_stream_subscribers_by_name_idx;

ALTER TABLE state_stream_subscribers_partitioned_p00 RENAME TO state_stream_subscribers_p00;
ALTER TABLE state_stream_subscribers_partitioned_p01 RENAME TO state_stream_subscribers_p01;
ALTER TABLE state_stream_subscribers_partitioned_p02 RENAME TO state_stream_subscribers_p02;
ALTER TABLE state_stream_subscribers_partitioned_p03 RENAME TO state_stream_subscribers_p03;
ALTER TABLE state_stream_subscribers_partitioned_p04 RENAME TO state_stream_subscribers_p04;
ALTER TABLE state_stream_subscribers_partitioned_p05 RENAME TO state_stream_subscribers_p05;
ALTER TABLE state_stream_subscribers_partitioned_p06 RENAME TO state_stream_subscribers_p06;
ALTER TABLE state_stream_subscribers_partitioned_p07 RENAME TO state_stream_subscribers_p07;
ALTER TABLE state_stream_subscribers_partitioned_p08 RENAME TO state_stream_subscribers_p08;
ALTER TABLE state_stream_subscribers_partitioned_p09 RENAME TO state_stream_subscribers_p09;
ALTER TABLE state_stream_subscribers_partitioned_p10 RENAME TO state_stream_subscribers_p10;
ALTER TABLE state_stream_subscribers_partitioned_p11 RENAME TO state_stream_subscribers_p11;
ALTER TABLE state_stream_subscribers_partitioned_p12 RENAME TO state_stream_subscribers_p12;
ALTER TABLE state_stream_subscribers_partitioned_p13 RENAME TO state_stream_subscribers_p13;
ALTER TABLE state_stream_subscribers_partitioned_p14 RENAME TO state_stream_subscribers_p14;
ALTER TABLE state_stream_subscribers_partitioned_p15 RENAME TO state_stream_subscribers_p15;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Reverse the partitioning back to a single non-partitioned table per
-- target. Same CNCD pattern in reverse. Preserves data on downgrade.

-- state_stream_subscribers ---------------------------------------------------
ALTER TABLE state_stream_subscribers NO FORCE ROW LEVEL SECURITY;
ALTER TABLE state_stream_subscribers DISABLE ROW LEVEL SECURITY;

CREATE TABLE state_stream_subscribers_unpartitioned (
    name                   TEXT        NOT NULL,
    tenant_id              TEXT        NOT NULL DEFAULT '',
    stream_id              TEXT        NOT NULL,
    last_delivered_version BIGINT      NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (name, tenant_id, stream_id)
);
CREATE INDEX state_stream_subscribers_unpartitioned_by_name_idx
    ON state_stream_subscribers_unpartitioned (name, tenant_id);

INSERT INTO state_stream_subscribers_unpartitioned
SELECT name, tenant_id, stream_id, last_delivered_version, updated_at
FROM state_stream_subscribers;

DROP TABLE state_stream_subscribers;
ALTER TABLE state_stream_subscribers_unpartitioned RENAME TO state_stream_subscribers;
ALTER INDEX state_stream_subscribers_unpartitioned_by_name_idx RENAME TO state_stream_subscribers_by_name_idx;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_app') THEN
        EXECUTE 'ALTER TABLE state_stream_subscribers ENABLE ROW LEVEL SECURITY';
        EXECUTE 'ALTER TABLE state_stream_subscribers FORCE ROW LEVEL SECURITY';
        EXECUTE 'CREATE POLICY tenant_isolation ON state_stream_subscribers ' ||
                'USING      (tenant_id = current_setting(''app.tenant_id'', false)) ' ||
                'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', false))';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON state_stream_subscribers TO eventstore_app, eventstore_admin';
    END IF;
END $$;

-- processed_events -----------------------------------------------------------
ALTER TABLE processed_events NO FORCE ROW LEVEL SECURITY;
ALTER TABLE processed_events DISABLE ROW LEVEL SECURITY;

CREATE TABLE processed_events_unpartitioned (
    projection_name TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    event_id        UUID        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection_name, tenant_id, event_id)
);
INSERT INTO processed_events_unpartitioned
SELECT projection_name, tenant_id, event_id, processed_at FROM processed_events;

DROP TABLE processed_events;
ALTER TABLE processed_events_unpartitioned RENAME TO processed_events;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_app') THEN
        EXECUTE 'ALTER TABLE processed_events ENABLE ROW LEVEL SECURITY';
        EXECUTE 'ALTER TABLE processed_events FORCE ROW LEVEL SECURITY';
        EXECUTE 'CREATE POLICY tenant_isolation ON processed_events ' ||
                'USING      (tenant_id = current_setting(''app.tenant_id'', false)) ' ||
                'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', false))';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON processed_events TO eventstore_app, eventstore_admin';
    END IF;
END $$;

-- projection_checkpoint ------------------------------------------------------
ALTER TABLE projection_checkpoint NO FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_checkpoint DISABLE ROW LEVEL SECURITY;

CREATE TABLE projection_checkpoint_unpartitioned (
    name        TEXT        NOT NULL,
    tenant_id   TEXT        NOT NULL DEFAULT '',
    cursor      BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (name, tenant_id)
);
INSERT INTO projection_checkpoint_unpartitioned
SELECT name, tenant_id, cursor, updated_at FROM projection_checkpoint;

DROP TABLE projection_checkpoint;
ALTER TABLE projection_checkpoint_unpartitioned RENAME TO projection_checkpoint;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_app') THEN
        EXECUTE 'ALTER TABLE projection_checkpoint ENABLE ROW LEVEL SECURITY';
        EXECUTE 'ALTER TABLE projection_checkpoint FORCE ROW LEVEL SECURITY';
        EXECUTE 'CREATE POLICY tenant_isolation ON projection_checkpoint ' ||
                'USING      (tenant_id = current_setting(''app.tenant_id'', false)) ' ||
                'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', false))';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON projection_checkpoint TO eventstore_app, eventstore_admin';
    END IF;
END $$;

-- state_cache ----------------------------------------------------------------
ALTER TABLE state_cache NO FORCE ROW LEVEL SECURITY;
ALTER TABLE state_cache DISABLE ROW LEVEL SECURITY;

CREATE TABLE state_cache_unpartitioned (
    tenant_id            TEXT        NOT NULL,
    stream_id            TEXT        NOT NULL,
    type_url             TEXT        NOT NULL,
    state                JSONB       NOT NULL,
    version              BIGINT      NOT NULL,
    terminal             BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    state_schema_version INTEGER     NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, stream_id)
);
CREATE INDEX state_cache_unpartitioned_by_type_idx
    ON state_cache_unpartitioned (tenant_id, type_url, stream_id);

INSERT INTO state_cache_unpartitioned
SELECT tenant_id, stream_id, type_url, state, version, terminal, updated_at, state_schema_version
FROM state_cache;

DROP TABLE state_cache;
ALTER TABLE state_cache_unpartitioned RENAME TO state_cache;
ALTER INDEX state_cache_unpartitioned_by_type_idx RENAME TO state_cache_by_type_idx;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_app') THEN
        EXECUTE 'ALTER TABLE state_cache ENABLE ROW LEVEL SECURITY';
        EXECUTE 'ALTER TABLE state_cache FORCE ROW LEVEL SECURITY';
        EXECUTE 'CREATE POLICY tenant_isolation ON state_cache ' ||
                'USING      (tenant_id = current_setting(''app.tenant_id'', false)) ' ||
                'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', false))';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON state_cache TO eventstore_app, eventstore_admin';
    END IF;
END $$;

-- +goose StatementEnd
