-- subscriber_dlq queries (ADR 0025).

-- name: InsertSubscriberDLQ :exec
-- Capture one exhausted subscriber delivery. INSERT-on-conflict-update
-- keeps the most recent attempt info if the same (subscriber, tenant,
-- event) re-DLQs (rare — happens on replay after partial recovery).
INSERT INTO subscriber_dlq (
    subscriber_name, tenant_id, event_id, stream_id,
    type_url, last_error, attempts, enqueued_at
) VALUES (
    @subscriber_name, @tenant_id, @event_id, @stream_id,
    @type_url, @last_error, @attempts, @enqueued_at
)
ON CONFLICT (subscriber_name, tenant_id, event_id) DO UPDATE SET
    last_error  = EXCLUDED.last_error,
    attempts    = EXCLUDED.attempts,
    enqueued_at = EXCLUDED.enqueued_at;

-- name: ListSubscriberDLQ :many
-- Operator dashboard query. Tenant filter: '' = cross-tenant.
SELECT subscriber_name, tenant_id, event_id, stream_id, type_url,
       last_error, attempts, enqueued_at
FROM subscriber_dlq
WHERE subscriber_name = @subscriber_name
  AND (@tenant_id::text = '' OR tenant_id = @tenant_id)
ORDER BY enqueued_at
LIMIT @max_rows;

-- name: ClearSubscriberDLQ :execrows
-- Operator action: drop every DLQ row for one subscriber (typically
-- after a state_stream.Drain refresh has caught the subscriber up).
DELETE FROM subscriber_dlq
WHERE subscriber_name = @subscriber_name
  AND (@tenant_id::text = '' OR tenant_id = @tenant_id);

-- name: DeleteSubscriberDLQRow :exec
-- Delete a single DLQ row after operator-driven replay.
DELETE FROM subscriber_dlq
WHERE subscriber_name = @subscriber_name
  AND tenant_id       = @tenant_id
  AND event_id        = @event_id;
