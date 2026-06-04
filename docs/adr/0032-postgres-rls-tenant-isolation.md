# ADR 0032: Postgres Row-Level Security for Tenant Isolation

- **Status:** Accepted
- **Date:** 2026-06-04
- **Pairs with:** ADR 0007 (first-class multi-tenancy), ADR 0008 (stream
  identity), ADR 0009 (Postgres global position).

## Context

ADR 0007 makes multi-tenancy a first-class, non-optional concern: every
event carries `tenant_id`, every framework call reads tenant from
context via `estenant.From(ctx)`, and the runtime refuses to operate
without a tenant. Every tenant-scoped table (`events`, `unique_claims`,
`subject_keys`, `outbox`, `state_cache`, `processed_events`,
`projection_checkpoint`, `projection_dlq`, `subscriber_dlq`,
`state_stream_subscribers`, the tamper-evident chain) is partitioned by
`HASH(tenant_id)` and carries `tenant_id` as the leading index column.

This is structural — but it is enforced entirely in application code.
Every sqlc-generated query takes `tenant_id` as a bind parameter, and
the runtime threads it from context. The trust boundary therefore sits
at the Go layer: any code path that constructs SQL outside the
generated queries, any ad-hoc operator query, any SQL-injection foothold
in a downstream projection, and any forgotten `WHERE tenant_id = $1`
clause in a hand-written read model can cross the tenant boundary. The
database itself does not know which tenant the current connection is
authorized to see.

For deployments where the framework's own discipline is the *only*
thing keeping tenants apart, this is acceptable. For SaaS deployments
shipping into regulated contexts (HIPAA, SOC 2, PCI, GDPR with strict
DPA terms), auditors increasingly expect a database-enforced isolation
boundary that does not rely on application-layer correctness. A second
line of defence at the storage layer also bounds the blast radius of a
class of bugs that would otherwise be silent: a missing predicate that
returns "too much" rather than failing loudly.

Postgres provides row-level security policies that evaluate against
session state. Combined with a per-transaction setting that pins the
tenant for the duration of the transaction, RLS can enforce the same
predicate that the application already includes — but at a layer the
application cannot bypass without holding a privileged role.

## Decision

Enable Postgres row-level security on every tenant-scoped table, with
policies keyed off a per-transaction session setting (`app.tenant_id`)
that the framework's pgx layer sets from the request context.

RLS is **additive defence-in-depth**, not a replacement. The
application-layer enforcement from ADR 0007 stays exactly as it is:
`estenant.From(ctx)` is still required, every generated query still
takes `tenant_id` as a bind parameter, and `ErrTenantMissing` still
fires before any SQL is issued. RLS is the second line — the line that
catches the bypass, not the line that catches the omission.

### Policy shape

Every tenant-scoped table runs:

```sql
ALTER TABLE <name> ENABLE ROW LEVEL SECURITY;
ALTER TABLE <name> FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON <name>
    USING       (tenant_id = current_setting('app.tenant_id', false))
    WITH CHECK  (tenant_id = current_setting('app.tenant_id', false));
```

`FORCE ROW LEVEL SECURITY` makes the policy apply even to the table
owner, so the role that runs migrations does not silently bypass it
during routine queries. The `false` argument to `current_setting`
causes the query to error if `app.tenant_id` is unset — a missing
binding is loud, not a silent "see everything."

### Binding the tenant per transaction

The framework's pgx layer issues `SET LOCAL app.tenant_id = $1` as the
first statement of every transaction, sourced from `estenant.From(ctx)`.
`SET LOCAL` scopes the value to the transaction; rollback and commit
both clear it. For non-transactional read paths the equivalent is
issued at the head of the query batch on a checked-out connection,
followed by `RESET app.tenant_id` on release.

This binding lives in `adapters/storage/postgres` — a single
chokepoint that every store method already goes through. It does not
require modifications to generated sqlc code. A query that runs without
the binding hits Postgres, evaluates `current_setting('app.tenant_id',
false)`, and fails with `unrecognized configuration parameter` — the
visible signal that the framework's tenant plumbing was bypassed.

### Cross-tenant operations

ADR 0007 already calls out the legitimate cross-tenant cases:
store-wide monotonic global position (ADR 0009), admin subscriptions,
billing aggregation, compliance export, and the `tenant_id =
"__global__"` constraint sentinel. These do not fit a single-tenant
RLS predicate.

The framework defines a dedicated `eventstore_admin` Postgres role
with `BYPASSRLS`. Cross-tenant code paths — the global-position cursor,
admin tooling under `cmd/esctl`, and the cross-tenant subscription API
— acquire connections under this role through a separate pool. The
role is **explicit and loud**: the connection string is distinct, the
pool is named, and reviewers can see at a glance that a piece of code
is operating outside the per-tenant boundary. There is no implicit
upgrade; tenant-scoped code paths use the tenant-scoped pool, and
their connections never gain `BYPASSRLS`.

Migrations run under the table-owning role (not `eventstore_admin`).
Because of `FORCE ROW LEVEL SECURITY`, the owner role does not bypass
RLS either; migrations that need to touch every row (backfills, data
repairs) must either set `app.tenant_id` per batch or run as
`eventstore_admin`. This is by design — a backfill that "just works"
across every tenant is exactly the kind of operation an auditor will
ask about.

### SQLite

SQLite is unaffected. ADR 0007 already biases toward one DB file per
tenant in production for SQLite deployments; a single file is a
single-tenant trust boundary by construction. The framework continues
to support multi-tenant SQLite for development and tests; RLS is a
Postgres-only feature and would not translate.

## Alternatives considered

### Per-tenant Postgres role (`SET ROLE` per request)

Each tenant gets its own Postgres role, owned by `eventstore_admin`,
with `GRANT SELECT, INSERT, UPDATE` on every tenant-scoped table.
Connections issue `SET ROLE tenant_<id>` per request; RLS policies
key off `current_user` rather than a GUC.

Stronger isolation in principle — the role itself is the credential —
but the operational story collapses past a few hundred tenants. Role
creation joins the tenant-onboarding critical path; `pg_authid` grows
without bound; connection pooling has to be per-role or has to
`RESET ROLE` on checkout (and a forgotten reset is a tenant leak in
the other direction). For a framework that targets SaaS deployments
with potentially thousands of tenants per cluster, the GUC approach
scales and the role approach does not.

### Pin tenant at connection acquisition

The pool sets `app.tenant_id` once when a connection is checked out
and resets it on release. Marginally less SQL traffic than `SET LOCAL`
per transaction.

Rejected because pgx's pool reuses connections aggressively across
goroutines and across requests; a forgotten reset on release would
leak the previous request's tenant into the next request on the same
connection. `SET LOCAL` is scoped to the transaction by Postgres
itself, with no possibility of leaking past commit or rollback. The
per-transaction cost is one extra round-trip-free protocol message —
cheap insurance against a class of bugs that would otherwise be silent.

### Status quo — application-layer enforcement only

What the framework does today. The argument for keeping it is
simplicity: one place to reason about tenant isolation, no Postgres
features to learn, no role separation, no GUC handling. The argument
against is that a single missing `WHERE` clause in a hand-written
projection — or a single SQL-injection foothold in a downstream
service that talks to the same database — defeats the entire
isolation story silently. The framework cannot enforce discipline on
code it does not write, and adopters consistently report writing
hand-tuned read queries against the events table. RLS is the line
that catches the bug the application layer did not.

## Consequences

### Positive

- **Database-enforced tenant boundary.** A query that omits
  `tenant_id` returns zero rows for the bound tenant rather than
  leaking data for another tenant. A query under the wrong tenant
  binding returns the wrong tenant's view rather than crossing the
  boundary.
- **Compliance posture improves materially.** Auditors can be shown a
  policy definition and a role-separation diagram rather than a
  promise that every code path remembers to filter.
- **Hand-written read models inherit the boundary for free.** Adopters
  writing projections against `events` no longer carry the entire
  burden of remembering the tenant predicate; their queries are
  scoped automatically by the active session setting.
- **Cross-tenant intent becomes visible.** Code running under
  `eventstore_admin` is observably different from code running under
  the tenant-scoped pool; reviewers and operators can grep for the
  privileged code paths.

### Negative

- **Every transaction pays one extra protocol round-trip-free message
  for `SET LOCAL`.** Negligible in practice but real, and worth
  noting for high-throughput command paths.
- **Two connection pools instead of one.** The framework's pgx wiring
  grows a privileged pool for `eventstore_admin` in addition to the
  tenant-scoped pool. Operators must configure two connection
  strings.
- **Cross-tenant operations are more ceremonial.** Code that legitimately
  needs to read across tenants — global-position cursors, billing
  aggregation, admin queries — must explicitly acquire from the
  admin pool. This is the right default but adds friction.
- **Migrations need a per-batch tenant binding or an explicit admin
  promotion.** A backfill across every tenant is no longer a single
  `UPDATE events SET ...`; the migration author must either iterate
  per tenant with `SET LOCAL` or document why the migration runs
  under `eventstore_admin`. The PR-template requirement from ADR
  0030 should be extended to cover this.
- **Adopters running an older Postgres role topology must migrate.**
  Existing deployments that already have a single application role
  must split it into the tenant-scoped role plus `eventstore_admin`
  and re-grant before enabling the policies. This is a one-time
  operator action and belongs in the upgrade notes.

### Migration ramp

The intended deploy sequence — apply the migration, split the role,
ship the new binary, wire up the admin pool — is hard to do
atomically. The adapter therefore exposes `WithoutRLSEnforcement()`
as a transitional opt-in. When set, cross-tenant code paths fall back
to the main pool instead of erroring with `ErrAdminPoolRequired`,
which lets operators ship the new binary *before* migration 00015
runs or before the role split is complete.

The option is safe in two states only: when migration 00015 has not
been applied (no policies to enforce), or when the main pool's role
has `BYPASSRLS` (a superuser used in development, or a transitional
grant). Once the migration is applied *and* the main pool's role is
non-privileged, the fallback silently returns zero rows from
cross-tenant queries — the outbox publisher and any other
cross-tenant consumer would stop making progress without any error
to alert the operator. The option's doc string spells this out and
the production path is to remove it as soon as `WithAdminPool` is
wired.

This is a deliberate compromise: strict-by-default behavior catches
mistakes loudly, and the escape hatch exists only so the upgrade can
happen in two steps instead of being all-or-nothing.

### Neutral but load-bearing

- **SQLite remains outside this regime.** The trust boundary on
  SQLite continues to be the file itself, per ADR 0007.
- **The application-layer enforcement is not removed.** `ErrTenantMissing`
  still fires in the runtime before any SQL is issued; the policy is
  the second line, not the only line. If the application layer is
  ever disabled or weakened in the future, that is a separate
  decision and would need its own ADR.
- **Tier under ADR 0030.** Enabling RLS is a schema-touching change
  (new migration, role topology change) but does not alter on-disk
  encodings or state shapes. The implementation PR must declare its
  tier per ADR 0030 and document the operator steps for splitting
  roles in existing deployments.
