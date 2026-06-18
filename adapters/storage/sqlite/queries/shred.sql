-- Crypto-shredding subject-key queries (ADR 0010).

-- name: GetSubjectKey :one
SELECT tenant_id, subject, dek_wrapped, kek_version, created_at, shredded_at
FROM subject_keys
WHERE tenant_id = ?
  AND subject = ?;

-- name: UpsertSubjectKey :exec
INSERT INTO subject_keys (tenant_id, subject, dek_wrapped, kek_version)
VALUES (?, ?, ?, ?)
ON CONFLICT (tenant_id, subject) DO UPDATE SET
    dek_wrapped = excluded.dek_wrapped,
    kek_version = excluded.kek_version;

-- name: ForgetSubject :exec
UPDATE subject_keys
SET dek_wrapped = X'',
    shredded_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE tenant_id = ?
  AND subject = ?;

-- name: ListShreddedSubjects :many
SELECT subject, shredded_at
FROM subject_keys
WHERE tenant_id = ?
  AND shredded_at IS NOT NULL
ORDER BY shredded_at DESC
LIMIT ?;

-- name: ListStaleSubjectKeys :many
SELECT tenant_id, subject, dek_wrapped, kek_version, created_at, shredded_at
FROM subject_keys
WHERE tenant_id    = sqlc.arg(tenant_id)
  AND kek_version  < sqlc.arg(current_kek_version)
  AND shredded_at IS NULL
ORDER BY subject
LIMIT sqlc.arg(max_rows);

-- name: ListSubjectsCreatedBefore :many
-- Returns non-shredded subject_keys rows whose DEK was minted before
-- the cutoff. Used by shred.RetentionWorker. SQLite stores timestamps
-- as ISO-8601 strings; lexicographic < works correctly when both
-- sides are formatted the same way.
SELECT tenant_id, subject, dek_wrapped, kek_version, created_at, shredded_at
FROM subject_keys
WHERE tenant_id    = sqlc.arg(tenant_id)
  AND shredded_at IS NULL
  AND created_at   < sqlc.arg(cutoff)
ORDER BY subject
LIMIT sqlc.arg(max_rows);
