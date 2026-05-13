-- +goose Up

-- ===========================================================================
-- Parent tables, indices, and the global ordering sequence.
--
-- Partition children for these tables are created in 00002 so that sqlc
-- doesn't generate redundant per-partition type structs. sqlc reads only
-- this file (configured in sqlc.yaml); goose runs both.
-- ===========================================================================

-- ===========================================================================
-- Global ordering sequence (ADR 0009).
--
-- Allocated inside append transactions under pg_advisory_xact_lock. The
-- lock serializes all appends store-wide; the sequence is therefore
-- consumed in strict commit order, giving a gap-free monotonic
-- global_position.
-- ===========================================================================

CREATE SEQUENCE events_global_position_seq AS BIGINT MINVALUE 1 NO CYCLE;

-- ===========================================================================
-- events (ADR 0005).
--
-- Source of truth. Append-only. Partitioned by HASH(tenant_id) for first-
-- class multi-tenancy (ADR 0007). Partitions are created in 00002.
--
-- Note on global_position uniqueness: PostgreSQL requires UNIQUE indices
-- on partitioned tables to include the partition key. Since we want
-- global_position unique store-wide (not per-tenant), we cannot enforce
-- uniqueness with a constraint on this partitioned table. Instead,
-- uniqueness is guaranteed by construction: the advisory lock taken
-- inside the append transaction serializes all writers, so nextval is
-- consumed in commit order with no possibility of duplication.
-- ===========================================================================

CREATE TABLE events (
    event_id            UUID         NOT NULL,
    tenant_id           TEXT         NOT NULL,
    stream_id           TEXT         NOT NULL,
    version             BIGINT       NOT NULL,
    global_position     BIGINT       NOT NULL,
    type_url            TEXT         NOT NULL,
    schema_version      INTEGER      NOT NULL,
    occurred_at         TIMESTAMPTZ  NOT NULL,
    recorded_at         TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
    correlation_id      UUID         NOT NULL,
    causation_id        UUID         NOT NULL,
    command_id          UUID         NOT NULL,
    actor               JSONB        NOT NULL,
    actor_principal     TEXT         NOT NULL,
    payload             BYTEA        NOT NULL,
    payload_json        JSONB,
    encryption_key_refs JSONB,
    PRIMARY KEY (tenant_id, stream_id, version)
) PARTITION BY HASH (tenant_id);

CREATE INDEX events_global_position_idx
    ON events (global_position);

CREATE UNIQUE INDEX events_event_id_idx
    ON events (tenant_id, event_id);

CREATE INDEX events_correlation_idx
    ON events (tenant_id, correlation_id);

CREATE INDEX events_command_idx
    ON events (tenant_id, command_id);

CREATE INDEX events_actor_principal_idx
    ON events (tenant_id, actor_principal);

-- ===========================================================================
-- unique_claims (ADR 0010 — uniqueness as a first-class store capability).
--
-- A claim row is inserted in the same transaction as the event(s) that
-- produced it. The UNIQUE(tenant_id, scope, value) PK enforces the
-- uniqueness guarantee transactionally — a conflicting append fails
-- fast with a unique-violation error that the adapter translates to
-- ErrConstraintViolated.
--
-- Cross-tenant uniqueness is expressed by setting tenant_id = '__global__'
-- explicitly. This is never implicit.
-- ===========================================================================

CREATE TABLE unique_claims (
    tenant_id   TEXT         NOT NULL,
    scope       TEXT         NOT NULL,
    value       TEXT         NOT NULL,
    stream_id   TEXT         NOT NULL,
    claimed_at  TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, scope, value)
) PARTITION BY HASH (tenant_id);

CREATE INDEX unique_claims_stream_idx
    ON unique_claims (tenant_id, stream_id);

-- ===========================================================================
-- subject_keys (ADR 0010 — crypto-shredding).
--
-- One row per (tenant, subject). The dek_wrapped column holds the data
-- encryption key encrypted under the tenant's KEK; the KEK itself lives
-- in the configured KMS adapter (kms/inproc, adapters/kms/aws, etc.).
--
-- Shredding the subject sets dek_wrapped = '' and shredded_at = now().
-- The tombstone row is retained for compliance audit.
-- ===========================================================================

CREATE TABLE subject_keys (
    tenant_id    TEXT         NOT NULL,
    subject      TEXT         NOT NULL,
    dek_wrapped  BYTEA        NOT NULL,
    kek_version  INTEGER      NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
    shredded_at  TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, subject)
) PARTITION BY HASH (tenant_id);

-- ===========================================================================
-- snapshots (ADR 0011 — lazy snapshots, in eventstore DB).
--
-- One row per stream. Latest wins. Written on read after N events
-- accumulated since the last snapshot (default N = 100). Carries
-- state_schema_version for strict invalidation: if the decider's state
-- shape changes, the version bumps and stale snapshots are silently
-- discarded with the runtime falling back to full replay.
-- ===========================================================================

CREATE TABLE snapshots (
    tenant_id            TEXT         NOT NULL,
    stream_id            TEXT         NOT NULL,
    version              BIGINT       NOT NULL,
    state_schema_version INTEGER      NOT NULL,
    state                BYTEA        NOT NULL,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, stream_id)
) PARTITION BY HASH (tenant_id);

-- ===========================================================================
-- outbox (ADR 0014).
--
-- One row per event, reference-only (envelope fetched from `events` at
-- publish time). Partial index on `WHERE published_at IS NULL` keeps the
-- drain query cheap — the pending set is small in steady state.
--
-- Retention: rows are eligible for cleanup once
-- (published_at IS NOT NULL AND published_at < now() - retention_window).
-- Default retention 7 days; configurable per deployment. The same
-- scheduled job that publishes pending rows also runs the cleanup pass.
-- ===========================================================================

CREATE TABLE outbox (
    tenant_id        TEXT         NOT NULL,
    global_position  BIGINT       NOT NULL,
    event_id         UUID         NOT NULL,
    enqueued_at      TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
    published_at     TIMESTAMPTZ,
    attempts         INTEGER      NOT NULL DEFAULT 0,
    last_error       TEXT,
    PRIMARY KEY (tenant_id, global_position)
) PARTITION BY HASH (tenant_id);

CREATE INDEX outbox_pending_idx
    ON outbox (tenant_id, global_position)
    WHERE published_at IS NULL;

-- +goose Down

-- Parent tables cascade-drop their partition children (created in 00002).
DROP TABLE IF EXISTS outbox CASCADE;
DROP TABLE IF EXISTS snapshots CASCADE;
DROP TABLE IF EXISTS subject_keys CASCADE;
DROP TABLE IF EXISTS unique_claims CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP SEQUENCE IF EXISTS events_global_position_seq;
