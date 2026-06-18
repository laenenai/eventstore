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

-- name: ListStaleSubjectKeys :many
-- Returns subject_keys rows wrapped under an older KEK version. Used
-- by shred.RewrapDEKs to migrate historical DEKs after a KEK rotation
-- (ADR 0010). Shredded rows are skipped — their DEKs are deliberately
-- inaccessible.
SELECT tenant_id, subject, dek_wrapped, kek_version, created_at, shredded_at
FROM subject_keys
WHERE tenant_id    = @tenant_id
  AND kek_version  < @current_kek_version
  AND shredded_at IS NULL
ORDER BY subject
LIMIT @max_rows;

-- name: ListSubjectsCreatedBefore :many
-- Returns non-shredded subject_keys rows whose DEK was minted before
-- the cutoff. Used by shred.RetentionWorker to identify subjects
-- eligible for retention shredding. Shredded rows are skipped — their
-- DEKs are already destroyed. Pagination by subject keeps the order
-- stable across resumed sweeps.
SELECT tenant_id, subject, dek_wrapped, kek_version, created_at, shredded_at
FROM subject_keys
WHERE tenant_id   = @tenant_id
  AND shredded_at IS NULL
  AND created_at  < @cutoff
ORDER BY subject
LIMIT @max_rows;
