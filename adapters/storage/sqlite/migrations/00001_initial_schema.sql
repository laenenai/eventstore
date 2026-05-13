-- +goose Up

-- ===========================================================================
-- SQLite schema for the eventstore framework.
--
-- Mirrors the Postgres schema (adapters/storage/postgres/migrations) at the
-- query surface: same column names, same semantics. Differences vs Postgres:
--
--   * No partitioning. SQLite is single-writer; HASH(tenant_id) partitioning
--     is unnecessary and unsupported. The framework's recommended deployment
--     uses one DB file per tenant in production (ADR 0007 / ADR 0019).
--
--   * No advisory locks. SQLite serializes writers at the file level, so the
--     global_position sequence is gap-free by construction without explicit
--     locking. The Postgres advisory lock (ADR 0009) is a no-op on this
--     adapter.
--
--   * INTEGER PRIMARY KEY AUTOINCREMENT for global_position rather than a
--     SEQUENCE. Same monotonicity semantics, simpler implementation.
--
--   * Column types:
--       UUID         -> TEXT   (canonical 36-char form; uuid.UUID marshals)
--       BYTEA        -> BLOB
--       JSONB        -> TEXT   (SQLite has no native JSON type; JSON1
--                               extension functions operate on TEXT)
--       TIMESTAMPTZ  -> TEXT   (RFC3339Nano via Go's time.Time; SQLite has
--                               no native timestamp type either)
--       INT          -> INTEGER
--       BIGINT       -> INTEGER (SQLite INTEGER is 64-bit)
--
--   * SQLite supports INSERT ... RETURNING since 3.35 (modernc.org/sqlite and
--     libSQL both modern enough). We use it for the InsertEvent flow.
-- ===========================================================================

-- ===========================================================================
-- events (mirror of postgres events; see that file for full annotations).
-- ===========================================================================

CREATE TABLE events (
    event_id            TEXT     NOT NULL,
    tenant_id           TEXT     NOT NULL,
    stream_id           TEXT     NOT NULL,
    version             INTEGER  NOT NULL,
    global_position     INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    type_url            TEXT     NOT NULL,
    schema_version      INTEGER  NOT NULL,
    occurred_at         TEXT     NOT NULL,
    recorded_at         TEXT     NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    correlation_id      TEXT     NOT NULL,
    causation_id        TEXT     NOT NULL,
    command_id          TEXT     NOT NULL,
    actor               TEXT     NOT NULL,    -- JSON
    actor_principal     TEXT     NOT NULL,
    payload             BLOB     NOT NULL,
    payload_json        TEXT,                 -- JSON, nullable
    encryption_key_refs TEXT,                 -- JSON, nullable
    UNIQUE (tenant_id, stream_id, version),
    UNIQUE (tenant_id, event_id)
);

CREATE INDEX events_correlation_idx
    ON events (tenant_id, correlation_id);

CREATE INDEX events_command_idx
    ON events (tenant_id, command_id);

CREATE INDEX events_actor_principal_idx
    ON events (tenant_id, actor_principal);

-- ===========================================================================
-- unique_claims (mirror).
-- ===========================================================================

CREATE TABLE unique_claims (
    tenant_id   TEXT  NOT NULL,
    scope       TEXT  NOT NULL,
    value       TEXT  NOT NULL,
    stream_id   TEXT  NOT NULL,
    claimed_at  TEXT  NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (tenant_id, scope, value)
);

CREATE INDEX unique_claims_stream_idx
    ON unique_claims (tenant_id, stream_id);

-- ===========================================================================
-- subject_keys (mirror).
-- ===========================================================================

CREATE TABLE subject_keys (
    tenant_id    TEXT     NOT NULL,
    subject      TEXT     NOT NULL,
    dek_wrapped  BLOB     NOT NULL,
    kek_version  INTEGER  NOT NULL,
    created_at   TEXT     NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    shredded_at  TEXT,
    PRIMARY KEY (tenant_id, subject)
);

-- ===========================================================================
-- snapshots (mirror).
-- ===========================================================================

CREATE TABLE snapshots (
    tenant_id            TEXT     NOT NULL,
    stream_id            TEXT     NOT NULL,
    version              INTEGER  NOT NULL,
    state_schema_version INTEGER  NOT NULL,
    state                BLOB     NOT NULL,
    created_at           TEXT     NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (tenant_id, stream_id)
);

-- ===========================================================================
-- outbox (mirror).
-- ===========================================================================

CREATE TABLE outbox (
    tenant_id        TEXT     NOT NULL,
    global_position  INTEGER  NOT NULL,
    event_id         TEXT     NOT NULL,
    enqueued_at      TEXT     NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    published_at     TEXT,
    attempts         INTEGER  NOT NULL DEFAULT 0,
    last_error       TEXT,
    PRIMARY KEY (tenant_id, global_position)
);

CREATE INDEX outbox_pending_idx
    ON outbox (tenant_id, global_position)
    WHERE published_at IS NULL;

-- +goose Down

DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS snapshots;
DROP TABLE IF EXISTS subject_keys;
DROP TABLE IF EXISTS unique_claims;
DROP TABLE IF EXISTS events;
