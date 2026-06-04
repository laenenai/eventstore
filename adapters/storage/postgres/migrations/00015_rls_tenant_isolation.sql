-- +goose Up
-- +goose StatementBegin

-- ADR 0032 — Row-level security for tenant isolation.
--
-- Two roles split the trust boundary at the database layer:
--
--   eventstore_app    — used by every tenant-scoped framework call.
--                       Has no BYPASSRLS; reads `app.tenant_id` set per
--                       transaction by the adapter to decide which rows
--                       it can see.
--   eventstore_admin  — used by cross-tenant code paths (global position
--                       cursor, admin tooling, billing aggregation).
--                       Has BYPASSRLS; policies do not apply.
--
-- RLS is forced (FORCE ROW LEVEL SECURITY), so even the table owner is
-- subject to policies during routine queries — a migration that needs
-- to touch every row must either bind `app.tenant_id` per batch or run
-- under eventstore_admin. See ADR 0030 (migration discipline).
--
-- The app layer (estenant.From + leading tenant_id columns) stays
-- exactly as it is per ADR 0007. This migration adds a second line of
-- defence at the storage layer; it does not replace the first.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_admin') THEN
        CREATE ROLE eventstore_admin BYPASSRLS NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventstore_app') THEN
        CREATE ROLE eventstore_app NOLOGIN;
    END IF;
END
$$;

-- ===========================================================================
-- Grants.
--
-- eventstore_app needs DML on every tenant-scoped table plus USAGE on the
-- global_position sequence (allocated inside the append transaction).
-- eventstore_admin gets the same grants — BYPASSRLS only relaxes RLS,
-- not table privileges.
-- ===========================================================================

GRANT USAGE ON SEQUENCE events_global_position_seq TO eventstore_app, eventstore_admin;

GRANT SELECT, INSERT, UPDATE, DELETE ON
    events,
    unique_claims,
    subject_keys,
    outbox,
    state_cache,
    projection_checkpoint,
    projection_dlq,
    processed_events,
    state_stream_subscribers,
    subscriber_dlq
TO eventstore_app, eventstore_admin;

-- goose tracks its own migration state; both roles need to read it for
-- adapter.Migrate to be safe to call as either role (it's typically
-- called once at deploy time, but the framework does not enforce that).
GRANT SELECT ON goose_db_version TO eventstore_app, eventstore_admin;

-- ===========================================================================
-- Enable + force RLS, and install the tenant_isolation policy on each
-- tenant-scoped table.
--
-- The policy uses current_setting('app.tenant_id', false) — the `false`
-- second argument makes the call ERROR if the GUC is unset, which is the
-- visible signal that the adapter's tenant binding was bypassed. Loud
-- failure is preferable to silent "see nothing" or silent "see everything".
--
-- For partitioned tables (events, unique_claims, subject_keys, outbox),
-- ENABLE / FORCE / CREATE POLICY on the parent propagates to every
-- partition.
-- ===========================================================================

-- events ---------------------------------------------------------------------
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON events
    USING      (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false));

-- unique_claims --------------------------------------------------------------
-- Cross-tenant uniqueness is expressed by writing claims with
-- tenant_id = '__global__' explicitly (ADR 0007). Tenant-scoped append
-- transactions must be able to write and observe these claims, so the
-- policy admits the sentinel in addition to the bound tenant. The
-- sentinel is reserved by convention; no real tenant id may equal it.
ALTER TABLE unique_claims ENABLE ROW LEVEL SECURITY;
ALTER TABLE unique_claims FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON unique_claims
    USING      (tenant_id = current_setting('app.tenant_id', false)
             OR tenant_id = '__global__')
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false)
             OR tenant_id = '__global__');

-- subject_keys ---------------------------------------------------------------
ALTER TABLE subject_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE subject_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON subject_keys
    USING      (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false));

-- outbox ---------------------------------------------------------------------
ALTER TABLE outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON outbox
    USING      (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false));

-- state_cache ----------------------------------------------------------------
ALTER TABLE state_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE state_cache FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON state_cache
    USING      (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false));

-- projection_checkpoint ------------------------------------------------------
ALTER TABLE projection_checkpoint ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_checkpoint FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON projection_checkpoint
    USING      (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false));

-- projection_dlq -------------------------------------------------------------
ALTER TABLE projection_dlq ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_dlq FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON projection_dlq
    USING      (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false));

-- processed_events -----------------------------------------------------------
ALTER TABLE processed_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON processed_events
    USING      (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false));

-- state_stream_subscribers ---------------------------------------------------
ALTER TABLE state_stream_subscribers ENABLE ROW LEVEL SECURITY;
ALTER TABLE state_stream_subscribers FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON state_stream_subscribers
    USING      (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false));

-- subscriber_dlq -------------------------------------------------------------
ALTER TABLE subscriber_dlq ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriber_dlq FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON subscriber_dlq
    USING      (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', false));

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP POLICY IF EXISTS tenant_isolation ON subscriber_dlq;
DROP POLICY IF EXISTS tenant_isolation ON state_stream_subscribers;
DROP POLICY IF EXISTS tenant_isolation ON processed_events;
DROP POLICY IF EXISTS tenant_isolation ON projection_dlq;
DROP POLICY IF EXISTS tenant_isolation ON projection_checkpoint;
DROP POLICY IF EXISTS tenant_isolation ON state_cache;
DROP POLICY IF EXISTS tenant_isolation ON outbox;
DROP POLICY IF EXISTS tenant_isolation ON subject_keys;
DROP POLICY IF EXISTS tenant_isolation ON unique_claims;
DROP POLICY IF EXISTS tenant_isolation ON events;

ALTER TABLE subscriber_dlq            DISABLE ROW LEVEL SECURITY;
ALTER TABLE state_stream_subscribers  DISABLE ROW LEVEL SECURITY;
ALTER TABLE processed_events          DISABLE ROW LEVEL SECURITY;
ALTER TABLE projection_dlq            DISABLE ROW LEVEL SECURITY;
ALTER TABLE projection_checkpoint     DISABLE ROW LEVEL SECURITY;
ALTER TABLE state_cache               DISABLE ROW LEVEL SECURITY;
ALTER TABLE outbox                    DISABLE ROW LEVEL SECURITY;
ALTER TABLE subject_keys              DISABLE ROW LEVEL SECURITY;
ALTER TABLE unique_claims             DISABLE ROW LEVEL SECURITY;
ALTER TABLE events                    DISABLE ROW LEVEL SECURITY;

REVOKE SELECT, INSERT, UPDATE, DELETE ON
    events,
    unique_claims,
    subject_keys,
    outbox,
    state_cache,
    projection_checkpoint,
    projection_dlq,
    processed_events,
    state_stream_subscribers,
    subscriber_dlq
FROM eventstore_app, eventstore_admin;

REVOKE USAGE ON SEQUENCE events_global_position_seq FROM eventstore_app, eventstore_admin;
REVOKE SELECT ON goose_db_version FROM eventstore_app, eventstore_admin;

DROP ROLE IF EXISTS eventstore_app;
DROP ROLE IF EXISTS eventstore_admin;

-- +goose StatementEnd
