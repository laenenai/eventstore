-- subscriber_dlq queries (ADR 0025, batched per ADR 0029).

-- name: InsertSubscriberDLQ :exec
INSERT INTO subscriber_dlq (
    subscriber_name, tenant_id, stream_id,
    first_event_id, event_ids, type_urls,
    last_error, attempts, enqueued_at
) VALUES (
    sqlc.arg(subscriber_name), sqlc.arg(tenant_id), sqlc.arg(stream_id),
    sqlc.arg(first_event_id), sqlc.arg(event_ids), sqlc.arg(type_urls),
    sqlc.arg(last_error), sqlc.arg(attempts), sqlc.arg(enqueued_at)
)
ON CONFLICT (subscriber_name, tenant_id, first_event_id) DO UPDATE SET
    event_ids   = excluded.event_ids,
    type_urls   = excluded.type_urls,
    stream_id   = excluded.stream_id,
    last_error  = excluded.last_error,
    attempts    = excluded.attempts,
    enqueued_at = excluded.enqueued_at;

-- name: ListSubscriberDLQ :many
SELECT subscriber_name, tenant_id, stream_id, first_event_id,
       event_ids, type_urls, last_error, attempts, enqueued_at
FROM subscriber_dlq
WHERE subscriber_name = sqlc.arg(subscriber_name)
  AND (sqlc.arg(tenant_id) = '' OR tenant_id = sqlc.arg(tenant_id))
ORDER BY enqueued_at
LIMIT sqlc.arg(max_rows);

-- name: ClearSubscriberDLQ :execrows
DELETE FROM subscriber_dlq
WHERE subscriber_name = sqlc.arg(subscriber_name)
  AND (sqlc.arg(tenant_id) = '' OR tenant_id = sqlc.arg(tenant_id));

-- name: DeleteSubscriberDLQRow :exec
DELETE FROM subscriber_dlq
WHERE subscriber_name = sqlc.arg(subscriber_name)
  AND tenant_id       = sqlc.arg(tenant_id)
  AND first_event_id  = sqlc.arg(first_event_id);
