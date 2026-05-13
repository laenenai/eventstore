-- First-class uniqueness primitives (ADR 0010).
--
-- A claim is inserted in the same transaction as the events that produce
-- it. The UNIQUE(tenant_id, scope, value) PK enforces transactional
-- uniqueness — a conflict fails the entire append, which the adapter
-- translates to ErrConstraintViolated.
--
-- Cross-tenant uniqueness uses tenant_id = '__global__' explicitly; the
-- framework refuses to set this implicitly.

-- name: ClaimUnique :exec
INSERT INTO unique_claims (tenant_id, scope, value, stream_id)
VALUES (@tenant_id, @scope, @value, @stream_id);

-- name: ReleaseUnique :exec
DELETE FROM unique_claims
WHERE tenant_id = @tenant_id AND scope = @scope AND value = @value;

-- name: GetUniqueClaim :one
SELECT * FROM unique_claims
WHERE tenant_id = @tenant_id AND scope = @scope AND value = @value;
