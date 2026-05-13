-- Append-path queries for SQLite. Mirror of the Postgres adapter's
-- append.sql, with three dialect deltas:
--
--   * No AdvisoryLock. SQLite serializes writers at the file level;
--     the Append flow in the adapter code skips the AdvisoryLock step.
--
--   * global_position is INTEGER PRIMARY KEY AUTOINCREMENT, fetched
--     via database/sql's Result.LastInsertId() (`:execlastid`) rather
--     than RETURNING. This avoids a sqlc 1.30 SQLite tokenizer bug
--     that truncates trailing identifiers in RETURNING clauses.
--
--   * recorded_at is passed explicitly by the adapter (set to the
--     adapter-side clock at append time). The schema's DEFAULT remains
--     as a safety net for queries that omit it.

-- name: InsertEvent :execlastid
INSERT INTO events (
    event_id,
    tenant_id,
    stream_id,
    version,
    type_url,
    schema_version,
    occurred_at,
    recorded_at,
    correlation_id,
    causation_id,
    command_id,
    actor,
    actor_principal,
    payload,
    payload_json,
    encryption_key_refs
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: InsertOutbox :exec
INSERT INTO outbox (tenant_id, global_position, event_id)
VALUES (?, ?, ?);
