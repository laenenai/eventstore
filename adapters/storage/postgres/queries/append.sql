-- Append-path queries. Used inside a single transaction in the
-- following sequence (see ADR 0009):
--
--   1. AdvisoryLock          (serializes all writers store-wide)
--   2. For each constraint:  ClaimUnique  (in constraint.sql)
--   3. For each event:       InsertEvent  (allocates global_position)
--   4. For each event:       InsertOutbox (durable publish backstop)
--   5. COMMIT
--
-- Optimistic concurrency is enforced by the events PRIMARY KEY
-- (tenant_id, stream_id, version) — a stale read produces a
-- unique-violation that the adapter translates to ErrConflict.
--
-- "Gap-free" precisely: the COMMITTED global_position column has no
-- late-committing predecessor below any projector's cursor — i.e.,
-- given two committed rows with positions A < B, A is visible to the
-- projector no later than B. It does NOT mean the integer sequence is
-- dense: nextval() is non-transactional, so a rolled-back append still
-- consumes its sequence value and leaves a hole in the integer column.
-- Holes between committed positions are harmless; what would break the
-- projector contract is a hole appearing BELOW a cursor that has
-- already advanced — that's what the advisory lock prevents.

-- name: AdvisoryLock :exec
-- Acquire the store-wide append lock. Auto-releases on commit/rollback.
-- The constant is the framework's reserved lock key for this table.
SELECT pg_advisory_xact_lock(@lock_key::bigint);

-- name: InsertEvent :one
-- Insert one event, allocating its global_position from the sequence
-- under the advisory lock. Returns the assigned position and the DB
-- commit timestamp so callers can echo them back on the envelope.
INSERT INTO events (
    event_id,
    tenant_id,
    stream_id,
    version,
    global_position,
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
    encryption_key_refs,
    hash,
    prev_hash
) VALUES (
    @event_id,
    @tenant_id,
    @stream_id,
    @version,
    nextval('events_global_position_seq'),
    @type_url,
    @schema_version,
    @occurred_at,
    @correlation_id,
    @causation_id,
    @command_id,
    @actor,
    @actor_principal,
    @payload,
    @payload_json,
    @encryption_key_refs,
    @hash,
    @prev_hash
)
RETURNING global_position, recorded_at;

-- name: LastStreamHash :one
-- Returns the hash of the most recent event in a stream, used to
-- chain the next append. Empty result for streams with no events.
SELECT hash FROM events
WHERE tenant_id = @tenant_id AND stream_id = @stream_id
ORDER BY version DESC
LIMIT 1;

-- name: InsertOutbox :exec
-- Insert the outbox row for an event. The drain process polls
-- outbox_pending_idx and hands rows to the configured EventPublisher.
INSERT INTO outbox (tenant_id, global_position, event_id)
VALUES (@tenant_id, @global_position, @event_id);

-- name: BackfillEventHash :execrows
-- Populate hash + prev_hash for an event whose chain columns are NULL,
-- as written by streams that existed before ADR 0028's tamper-evident
-- chain migration. The `hash IS NULL` predicate is a safety guard: if
-- the row already carries a hash, the UPDATE is a no-op (RowsAffected
-- = 0) and the Go wrapper errors out, since overwriting an existing
-- hash would silently mask tampering.
UPDATE events
SET hash = @hash, prev_hash = @prev_hash
WHERE tenant_id = @tenant_id AND event_id = @event_id AND hash IS NULL;
