# ADR 0019: SQLite Driver Strategy

- **Status:** Accepted
- **Date:** 2026-05-13
- **Amends:** ADR 0017 (Module and Package Layout), ADR 0018 (Migrations
  and Queries) — specifically the points naming `modernc.org/sqlite` as
  *the* SQLite driver. Those statements are superseded by this ADR; the
  rest of those ADRs is unaffected.

## Context

A stated deployment target for the framework is Turso (and Turso's
self-hostable daemon, `sqld`). Turso runs on **libSQL**, a SQLite fork
that exposes its own network protocol (HTTP / WebSocket) for remote
access plus an embedded-replica mode that keeps a local copy in sync
with the remote.

`modernc.org/sqlite` is a **local SQLite library**. It reads and writes
local SQLite files. It does not speak the libSQL network protocol and
cannot reach a Turso or `sqld` instance.

Naming `modernc.org/sqlite` as the SQLite driver in ADRs 0017 and 0018
was a category error: SQLite-compatible SQL is not the same as "can
reach Turso". The two are decoupled — SQL stays portable, the network
transport does not.

The Turso Go ecosystem provides three drivers:

| Driver                                                     | Mode                                                  | CGO   | Reaches Turso? |
| ---------------------------------------------------------- | ----------------------------------------------------- | ----- | -------------- |
| `modernc.org/sqlite`                                       | Local SQLite file only                                | No    | No             |
| `github.com/tursodatabase/libsql-client-go/libsql`         | Remote-only (HTTP/WebSocket to sqld/Turso)            | No    | Yes (remote)   |
| `github.com/tursodatabase/go-libsql`                       | Local file + embedded replica (local cache + sync)    | Yes   | Yes (embedded) |

LibSQL is SQLite-compatible at the SQL level: same DDL, same
`INSERT ... RETURNING`, same JSON1 functions, same partial indices.
The schema, migrations, and sqlc-generated queries from ADR 0018 work
unchanged across all three drivers.

## Decision

### The SQLite adapter is driver-agnostic

The `adapters/storage/sqlite/` module imports **none** of the three
drivers. It accepts an already-opened `*sql.DB` and operates against
whatever driver the consumer registered.

```go
package sqlite

func NewAdapter(db *sql.DB, opts ...Option) (*Adapter, error)
```

Consumers register the driver they need via blank import and open the
DB themselves:

```go
import _ "modernc.org/sqlite"                                  // local
// or
import _ "github.com/tursodatabase/libsql-client-go/libsql"    // Turso remote
// or
import _ "github.com/tursodatabase/go-libsql"                  // Turso embedded

db, err := sql.Open(driverName, dsn)
adapter, err := sqlite.NewAdapter(db)
```

### Three driver patterns, documented per use case

- **Local dev, tests, self-hosted single-node:** `modernc.org/sqlite`.
  Pure Go, no CGO, fast iteration. Default for examples and the
  framework's own test suite.
- **Serverless deployment to Turso:** `libsql-client-go`. Pure Go, no
  CGO. Every query is a network round-trip; appropriate when the
  writer process is short-lived (Cloudflare Worker, AWS Lambda) and
  the per-request query count is small.
- **Long-running process targeting Turso:** `go-libsql` with embedded
  replicas. CGO required. Local reads are file-fast; writes go local
  then replicate asynchronously to Turso. Best per-query latency for
  any process that lives long enough to amortize the embedded copy.

### CI / internal tests default to modernc

The framework's own test suite and the conformance suite (the shared
contract every storage adapter must satisfy) run against
`modernc.org/sqlite` by default. Reasons:

- Pure Go — no CGO toolchain required on CI runners.
- Fast — no network. Test suites stay under the latency ceiling we
  care about.
- Deterministic — local file behavior is reproducible across CI
  environments.

### Integration tests against real sqld are opt-in, not per-PR

A separate integration-test target (`make test-integration` or
equivalent) spins up a real `sqld` instance — via Docker, Turso's
local-server CLI, or a managed Turso branch — and runs the
conformance suite against `libsql-client-go` and `go-libsql`. This
runs:

- **Pre-release** as a release gate.
- **On a nightly or weekly schedule** to catch regressions.
- **On-demand** via a label or workflow dispatch when a PR touches the
  storage path.

Per-PR `sqld` testing is rejected as too expensive in CI minutes and
too brittle (network flakes, real-infrastructure outages) for the
incremental value over modernc.

### Documentation updates

The README and quickstart show all three driver patterns side-by-side.
The "production deployment to Turso" guide explicitly steers users
between `libsql-client-go` (serverless) and `go-libsql` (long-running),
with the CGO trade-off called out.

### sqlc and migrations work unchanged

Per ADR 0018, sqlc and goose both target SQLite-compatible SQL. The
adapter's `migrations/*.sql` and `queries/*.sql` files are valid across
all three drivers without modification.

## Consequences

### Positive

- **The framework works on Turso.** This is the actual deployment
  target.
- **Consumers depend only on the driver they use.** No transitive CGO
  surprise.
- **Local dev stays cgo-free.** The fast pure-Go path remains the
  default for examples, tests, and CI.
- **One adapter codebase serves three deployment modes.** Maintenance
  burden does not multiply.
- **Driver-agnostic API generalizes.** If a future libSQL-compatible
  driver appears, the adapter picks it up for free.

### Negative

- **Slightly more setup for consumers.** They must register a driver
  and open `*sql.DB` themselves. Standard Go database pattern, but one
  more step than "the adapter handles everything".
- **Three drivers to validate.** Per-PR CI covers modernc only; full
  libsql validation runs on a slower cadence. Regressions in libsql-
  specific code paths may slip through to the integration suite rather
  than being caught at PR time.
- **CGO-or-not is now a deployment decision** the consumer makes, not
  the framework. Documentation must clearly explain when to choose
  which.

## Alternatives Considered

### Adapter imports `modernc.org/sqlite` directly (status quo before this ADR)

Rejected. Does not reach Turso. Misses the entire stated deployment
target.

### Adapter imports `go-libsql` for full libSQL support

Rejected. Forces CGO on all consumers, including local-dev users who
do not care about Turso. CGO complicates cross-compilation, container
builds, and Cloudflare Worker / similar pure-Go targets.

### Separate `adapters/storage/turso/` module distinct from `sqlite/`

Considered. The SQL is identical across drivers; the migrations are
identical; the queries are identical. Forking the adapter would duplicate
~95% of the code for a 5% delta (driver registration and DSN
handling). Driver-agnostic single adapter is the cleaner answer.

### CI runs full sqld integration on every PR

Rejected. Real-infrastructure tests are slow, flaky, and expensive.
The modernc path catches the overwhelming majority of regressions; the
libsql-specific path is a smaller surface that benefits from less
frequent but more thorough validation.
