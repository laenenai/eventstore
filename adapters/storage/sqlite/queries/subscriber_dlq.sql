-- subscriber_dlq queries (ADR 0025).

-- name: InsertSubscriberDLQ :exec
INSERT INTO subscriber_dlq (
    subscriber_name, tenant_id, event_id, stream_id,
    type_url, last_error, attempts, enqueued_at
) VALUES (
    sqlc.arg(subscriber_name), sqlc.arg(tenant_id), sqlc.arg(event_id), sqlc.arg(stream_id),
    sqlc.arg(type_url), sqlc.arg(last_error), sqlc.arg(attempts), sqlc.arg(enqueued_at)
)
ON CONFLICT (subscriber_name, tenant_id, event_id) DO UPDATE SET
    last_error  = excluded.last_error,
    attempts    = excluded.attempts,
    enqueued_at = excluded.enqueued_at;

-- name: ListSubscriberDLQ :many
SELECT subscriber_name, tenant_id, event_id, stream_id, type_url,
       last_error, attempts, enqueued_at
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
  AND event_id        = sqlc.arg(event_id);
