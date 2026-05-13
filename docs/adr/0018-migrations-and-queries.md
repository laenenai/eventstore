# ADR 0018: Schema Migrations and Query Generation

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

The framework owns a small, fixed schema (events, unique_claims,
subject_keys, snapshots, outbox, projection_checkpoints, plus partition
children and indices) and a finite, static set of queries against that
schema (append, read stream, claim constraints, fetch by event id,
drain outbox, snapshot read/write, etc.). Consumer apps do not write
SQL against framework tables.

Two tooling choices follow from this shape:

1. **Migration tool.** How the framework defines, ships, and applies
   its DDL.
2. **Query implementation.** How Go code in the storage adapters
   constructs and executes SQL against framework tables.

## Decision

### Migrations: `pressly/goose` with embedded SQL per adapter

Each storage adapter owns its dialect-specific migrations, embedded
into the adapter binary via `embed.FS`:

```
adapters/storage/postgres/migrations/
  00001_initial_schema.sql       # events, unique_claims, ..., partitions
  00001_initial_schema.down.sql
  00002_...

adapters/storage/sqlite/migrations/
  00001_initial_schema.sql       # SQLite-flavored DDL
  00001_initial_schema.down.sql
  00002_...
```

Each adapter exposes a programmatic migration API:

```go
package postgres

func (a *Adapter) Migrate(ctx context.Context) error
```

`Migrate` runs `goose.Up` against the embedded migrations using a
goose state table (`goose_db_version`) in the adapter's schema.
Consumers call this from their startup path or deploy hook. No goose
CLI is required on production hosts.

Concurrency on multi-instance deploys is handled by goose's
advisory-lock acquisition (Postgres) and SQLite's single-writer
semantics — only one instance applies migrations at a time; others
no-op.

Deployments wanting `LIST` partitioning rather than the default
`HASH(16)` (ADR 0007) select an alternative initial migration variant
via an adapter configuration option. The variants ship side-by-side in
the same adapter; the active one is chosen at `Migrate()` call time.

### Queries: `sqlc` with per-adapter generation

Each storage adapter ships query files and a `sqlc.yaml`. `sqlc
generate` produces type-safe Go code for the queries used by the
adapter. Generated code is internal to the adapter (not part of the
framework's public API).

```
adapters/storage/postgres/
  sqlc.yaml
  migrations/        # shared as schema source for sqlc
  queries/
    append.sql       # ClaimAdvisoryLock, InsertUniqueClaim, InsertEvent, InsertOutbox
    read.sql         # ReadStream, ReadStreamFromVersion, ReadAllFromPosition
    snapshot.sql     # GetSnapshot, UpsertSnapshot
    shred.sql        # GetSubjectKey, UpsertSubjectKey, ForgetSubject
    outbox.sql       # PendingOutboxRows, MarkPublished, CleanupPublished
    constraint.sql   # ClaimUnique, ReleaseUnique
  internal/db/       # sqlc-generated Go (not exported)
```

Drivers:

- **Postgres:** `pgx/v5` via `sqlc`'s `pgx/v5` mode. Type-safe queries,
  zero runtime SQL parsing.
- **SQLite:** `modernc.org/sqlite` (cgo-free) via `sqlc`'s `database/sql`
  mode. Same query files where SQL is compatible; dialect-specific files
  where it isn't.

Custom type mappings configured in `sqlc.yaml`:

- `UUID` → `github.com/google/uuid.UUID` (or `pgtype.UUID` for pgx-
  native binding, decided per adapter).
- `BYTEA` / `BLOB` → `[]byte`.
- `JSONB` (Postgres) / `TEXT` JSON (SQLite) → `json.RawMessage` or a
  typed wrapper for `encryption_key_refs` and `payload_json`.
- `TIMESTAMPTZ` → `time.Time`.

### Build orchestration

CI runs two codegen steps in order:

1. `buf generate` — protos → Go types (per ADR 0016).
2. `sqlc generate` — SQL queries → Go types (this ADR), per adapter.

Both produce checked-in generated code. CI also runs:

- `buf breaking` (ADR 0013) against the previous protos.
- `sqlc diff` (or equivalent) to detect SQL files that drifted from
  generated code without regeneration.

## Consequences

### Positive

- **Type-safe SQL.** Compile-time errors on column-name typos,
  parameter-type mismatches, return-shape changes. The entire surface
  the framework cares about is verified statically.
- **Single source of truth for schema.** Migrations are the schema;
  sqlc reads them to type-check queries. Drift is impossible at the
  type level.
- **No runtime SQL parsing or query builders.** Performance is close
  to raw `database/sql` / `pgx`.
- **Programmatic migration API.** `adapter.Migrate(ctx)` is one call;
  goose does the locking, version-tracking, and idempotency.
- **Per-adapter ownership.** Postgres and SQLite each evolve their
  own schema and queries at their own pace where dialects diverge.
- **No external runtime dependency for migration.** Goose ships
  embedded; no CLI on prod.

### Negative

- **Two codegen steps in the build pipeline.** `buf generate` and
  `sqlc generate`. Both produce checked-in code; both must run
  consistently. Standard Go codegen discipline applies (CI fails if
  generated code is out of date).
- **Dynamic queries are painful with sqlc.** "Filter by 0..N optional
  conditions" doesn't model well. The framework's queries are static
  by design, so this is not a real constraint for us — but if a
  future adapter needs dynamic SQL, sqlc is not the right tool for
  that specific query and we'd fall back to handwritten SQL there.
- **Generated code is checked in.** Reviewers see large generated
  diffs on schema or query changes. Standard cost; tooling marks
  these files as generated for review tooling.

## Alternatives Considered

### Migrations: `golang-migrate/migrate`

Rejected. More CLI-shaped; programmatic API is rougher; larger
transitive dep surface than goose. Capability parity but worse fit for
our "consumer calls `adapter.Migrate(ctx)`" pattern.

### Migrations: `ariga/atlas`

Rejected. Declarative schema management is overkill for a framework-
owned, linearly-evolving schema. Atlas shines when consumers own the
schema and want diff-based migrations. We don't.

### Migrations: roll our own

Rejected. ~100 LOC of obvious code, but the edge cases (concurrent
migration on multi-instance deploys, partial DDL failure, advisory
locking, transactional DDL behavior across Postgres versions) are
exactly what goose has already solved. No reason to rebuild it.

### Queries: handwritten `database/sql` / `pgx` code

Rejected. Loses compile-time type checking against the schema. Every
column-name typo becomes a runtime error. Manual refactoring across
queries when schema changes is error-prone.

### Queries: query builder (squirrel, goqu, bun)

Rejected. Adds runtime construction cost and reads worse than SQL for
the static queries we have. Query builders earn their keep for highly
dynamic queries; ours are static.

### Queries: ORM (gorm, ent)

Rejected hard. The framework intentionally treats events and the store
as opaque byte transport (ADR 0006). An ORM would impose model layers
over rows that don't represent domain entities — the wrong abstraction
at the wrong layer.
