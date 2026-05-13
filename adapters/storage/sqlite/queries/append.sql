-- Append-path queries for SQLite. Mirror of the Postgres adapter's
-- append.sql, with two dialect deltas:
--
--   * No AdvisoryLock — SQLite serializes writers at the file level.
--     The "no-op" advisory lock exists in the adapter code as a stub
--     to keep the Append flow uniform across adapters.
--
--   * global_position comes from INTEGER PRIMARY KEY AUTOINCREMENT,
--     not nextval(). The INSERT omits the column; SQLite assigns it
--     and we read it back via RETURNING.
--
-- Parameter syntax: bare `?` positional. sqlc 1.30's SQLite engine
-- does not reliably handle `@name` or `sqlc.arg(name)` syntax in our
-- setup, but positional `?` works and sqlc infers parameter struct
-- field names from the column context.

-- name: InsertEvent :one
INSERT INTO events (
    event_id,
    tenant_id,
    stream_id,
    version,
    type_url,
    schema_version,
    occurred_at,
    correlation_id,
    causation_id,
    command_id,
    actor,
    actor_principal,
    payload,
    payload_json,
    encryption_key_refs
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING global_position, recorded_at;

-- name: InsertOutbox :exec
INSERT INTO outbox (tenant_id, global_position, event_id)
VALUES (?, ?, ?);
