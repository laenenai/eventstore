-- subscriber_dlq queries (ADR 0025, batched per ADR 0029).

-- name: InsertSubscriberDLQ :exec
-- Capture one exhausted subscriber command-batch. INSERT-on-conflict-
-- update keeps the most recent attempt info if the same (subscriber,
-- tenant, first_event_id) re-DLQs (rare — happens on replay after
-- partial recovery).
INSERT INTO subscriber_dlq (
    subscriber_name, tenant_id, stream_id,
    first_event_id, event_ids, type_urls,
    last_error, attempts, enqueued_at
) VALUES (
    @subscriber_name, @tenant_id, @stream_id,
    @first_event_id, @event_ids, @type_urls,
    @last_error, @attempts, @enqueued_at
)
ON CONFLICT (subscriber_name, tenant_id, first_event_id) DO UPDATE SET
    event_ids   = EXCLUDED.event_ids,
    type_urls   = EXCLUDED.type_urls,
    stream_id   = EXCLUDED.stream_id,
    last_error  = EXCLUDED.last_error,
    attempts    = EXCLUDED.attempts,
    enqueued_at = EXCLUDED.enqueued_at;

-- name: ListSubscriberDLQ :many
-- Operator dashboard query. Tenant filter: '' = cross-tenant.
SELECT subscriber_name, tenant_id, stream_id, first_event_id,
       event_ids, type_urls, last_error, attempts, enqueued_at
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
-- Delete a single DLQ row after operator-driven replay. Keyed by the
-- batch's first event id (uniquely identifies the failed batch).
DELETE FROM subscriber_dlq
WHERE subscriber_name = @subscriber_name
  AND tenant_id       = @tenant_id
  AND first_event_id  = @first_event_id;
