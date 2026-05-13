-- Crypto-shredding subject-key queries (ADR 0010).
--
-- DEKs are stored wrapped by the tenant's KEK (held in the configured
-- KMS adapter). Shredding zeroes the wrapped DEK and tombstones the
-- row for compliance audit.

-- name: GetSubjectKey :one
SELECT *
FROM subject_keys
WHERE tenant_id = @tenant_id
  AND subject = @subject;

-- name: UpsertSubjectKey :exec
-- Used at first encryption for a subject (create the DEK) and during
-- KEK rotation (re-wrap the existing DEK under the new KEK version).
INSERT INTO subject_keys (tenant_id, subject, dek_wrapped, kek_version)
VALUES (@tenant_id, @subject, @dek_wrapped, @kek_version)
ON CONFLICT (tenant_id, subject) DO UPDATE SET
    dek_wrapped = EXCLUDED.dek_wrapped,
    kek_version = EXCLUDED.kek_version;

-- name: ForgetSubject :exec
-- Crypto-shred a subject. Sets the wrapped DEK to empty bytes and marks
-- the shred timestamp. The row is retained for compliance audit.
UPDATE subject_keys
SET dek_wrapped = ''::bytea,
    shredded_at = clock_timestamp()
WHERE tenant_id = @tenant_id
  AND subject = @subject;

-- name: ListShreddedSubjects :many
-- Audit query: enumerate subjects that have been crypto-shredded for a
-- tenant. Useful for compliance reports.
SELECT subject, shredded_at
FROM subject_keys
WHERE tenant_id = @tenant_id
  AND shredded_at IS NOT NULL
ORDER BY shredded_at DESC
LIMIT @max_rows;
