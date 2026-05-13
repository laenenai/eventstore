-- Uniqueness primitives (ADR 0010). Identical semantics to Postgres;
-- SQLite UNIQUE PK gives the same fail-fast guarantee.

-- name: ClaimUnique :exec
INSERT INTO unique_claims (tenant_id, scope, value, stream_id)
VALUES (?, ?, ?, ?);

-- name: ReleaseUnique :exec
DELETE FROM unique_claims
WHERE tenant_id = ? AND scope = ? AND value = ?;

-- name: GetUniqueClaim :one
SELECT tenant_id, scope, value, stream_id, claimed_at
FROM unique_claims
WHERE tenant_id = ? AND scope = ? AND value = ?;
