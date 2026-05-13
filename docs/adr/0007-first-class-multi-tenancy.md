# ADR 0007: First-Class Multi-Tenancy

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

Multi-tenancy can be retrofitted into an event-sourced system, but the
cost is enormous: every event ever written needs backfilling with a
tenant identifier, every index needs rebuilding with tenant as a leading
column, and every API surface needs a tenant parameter threaded through.

The framework targets SaaS deployments where multi-tenant is the default.
Single-tenant deployments are expressible as a degenerate case (one
tenant); the reverse is not true.

## Decision

Multi-tenancy is a first-class, non-optional concern.

- **Envelope:** `tenant_id` is required on every event. Empty string is
  rejected at the API.
- **API contract:** every framework call takes `ctx context.Context` and
  reads tenant via `estenant.From(ctx)`. The framework **refuses to
  operate** without a tenant in context. There is no default, no fallback,
  no "guess from the connection".
- **Postgres storage:** the `events`, `unique_claims`, `subject_keys`,
  and `snapshots` tables are all partitioned by `HASH(tenant_id)` with
  16 partitions by default. Deployments with a small known set of
  tenants may override to `LIST` partitioning at schema-generation time
  for cleaner per-tenant `pg_dump` and `DETACH PARTITION` workflows.
- **SQLite storage:** prefer one DB file per tenant in production. Clean
  isolation, trivial backup/restore, trivial offboarding. The framework
  supports multi-tenant SQLite for dev and test but biases toward
  one-file-per-tenant for prod.
- **All indices include `tenant_id` as the leading column.**
- **Constraint table:** `UNIQUE(tenant_id, scope, value)`. Uniqueness is
  scoped to a tenant by default. Cross-tenant uniqueness requires explicit
  `tenant_id = "__global__"` — never silent.
- **Global position remains store-wide monotonic**, not per-tenant.
  Cross-tenant admin subscriptions exist (billing aggregation, compliance
  export); per-tenant counters would not enable them and would introduce
  coordination cost.
- **Key custody is per-tenant.** KEK per tenant in pluggable KMS; DEKs
  per `(tenant, subject)` (see ADR 0010). A tenant offboarding flow drops
  the tenant KEK, which crypto-shreds all of the tenant's encrypted data.
- **Tenant middleware ships with the framework.** Transport handlers
  install it once at the boundary; every downstream call inherits the
  tenant from context.

## Consequences

### Positive

- **Structural isolation, not advisory.** Tenant leakage requires
  bypassing the type system and the framework's middleware — much harder
  to do by accident.
- **Per-tenant operations become first-class.** Backup, restore, export,
  rebuild, and purge all run on a single partition (Postgres) or a single
  DB file (SQLite).
- **Key custody blast radius is bounded by tenant.** A KEK compromise
  affects one tenant, not all.
- **Noisy-neighbor analysis is free.** Partition stats already isolate
  per-tenant volume and latency.

### Negative

- **Every API surface carries `ctx`** and reads tenant from it. Ergonomic
  cost is real, especially in tests. The framework provides test helpers
  that set a tenant in context.
- **Refusing to operate without a tenant is a hard error.** "I forgot to
  set the tenant" surfaces immediately rather than producing data with an
  empty or default tenant. This is by design.
- **Cross-tenant operations are explicit and loud.** This is the right
  default but occasionally annoying — for example, an admin tool listing
  all events across tenants must explicitly declare cross-tenant intent.

## Alternatives Considered

### Single-tenant only (one deployment per customer)

Rejected. The framework targets SaaS contexts where this profile is too
expensive. The retrofit cost — versioning the envelope, backfilling every
event, rebuilding every index — is enormous.

### Hybrid (tenancy optional, configured per deployment)

Rejected. Bifurcates every test, every index strategy, every runbook.
Doubles the operational complexity. Picking a side at the framework level
keeps the surface coherent.
