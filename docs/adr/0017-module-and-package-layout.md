# ADR 0017: Module and Package Layout

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

The framework will ship a core library plus a growing number of
adapters (storage, publisher, KMS). The layout choice has two
load-bearing consequences:

1. **Dependency surface for consumers.** A consumer using only
   Postgres + Restate should not have AWS SDK, GCP libs, NATS,
   Cloudflare libs, or Vault in their `go.sum`.
2. **Public import paths.** The shape of imports becomes the surface
   that every consumer types every day; changing it later is a major
   breaking change.

## Decision

### Multi-module monorepo

The core lives in a single Go module. Each heavyweight adapter
(storage, publisher, KMS) is its own Go module under the same
monorepo. Standard Go pattern used by kubectl, klog, etcd,
opentelemetry-go.

Development uses `go.work` so contributors edit and test across
modules without publishing intermediate versions.

### Layout

```
eventstore/                              # repo root
├── go.mod                               # module: github.com/<org>/eventstore (CORE)
├── go.work                              # dev workspace
├── README.md
│
├── es/                                  # CORE API — every consumer imports this
│   ├── decider.go                       # Decider[S,C,E]
│   ├── envelope.go                      # Envelope, Actor
│   ├── streamid.go                      # StreamID + slug validation
│   ├── command.go                       # ConstraintOp, DeriveCommandID
│   ├── errors.go                        # ErrConflict, ErrConstraintViolated, etc.
│   ├── tenant.go                        # tenant context accessors
│   ├── store.go                         # Store interface (event store contract)
│   └── upcaster.go                      # Upcaster interface + registry
│
├── aggregate/                           # aggregate runtime (Load, Handle)
├── projection/                          # projector runtime
├── outbox/                              # outbox table + drain helpers
├── snapshot/                            # snapshot primitives
├── shred/                               # crypto-shredding logic
├── publisher/                           # Publisher interface (host)
│   └── inproc/                          # in-process publisher (no heavy deps)
├── kms/                                 # KeyStore interface (host)
│   └── inproc/                          # in-process KMS (dev / SQLite)
│
├── proto/                               # framework's own .proto files
│   └── envelope/v1/envelope.proto
├── gen/                                 # generated Go from framework protos
├── estest/                              # given/when/then test harness
│
├── cmd/
│   ├── protoc-gen-es-go/                # codegen plugin binary
│   └── esctl/                           # operational CLI
│
├── adapters/                            # each leaf is its OWN MODULE
│   ├── storage/
│   │   ├── postgres/                    # depends on pgx
│   │   └── sqlite/                      # depends on modernc.org/sqlite (cgo-free)
│   ├── publisher/
│   │   ├── restate/                     # recommended publisher
│   │   ├── nats/
│   │   ├── sns/
│   │   ├── pubsub/
│   │   └── cfqueues/
│   └── kms/
│       ├── aws/
│       ├── gcp/
│       └── vault/
│
├── examples/                            # MODULE — examples pull adapter deps
│   ├── go.mod
│   └── ...
│
└── docs/
    ├── adr/
    └── cookbook/
```

### Naming and import paths

- Core: `import "github.com/<org>/eventstore/es"`, used as `es.Decider`,
  `es.Envelope`, `es.StreamID`. Short, idiomatic, matches Go's
  convention.
- Runtime packages: `aggregate.Runtime`, `projection.Runtime`,
  `outbox.Drain` — imported by their short leaf name.
- Adapter import paths:
  - `github.com/<org>/eventstore/adapters/storage/postgres` →
    `postgres.New(...)`
  - `github.com/<org>/eventstore/adapters/publisher/restate` →
    `restate.New(...)`
  - `github.com/<org>/eventstore/adapters/kms/aws` → `aws.New(...)`

The leaf package name is always short. Long import paths are typed
once per file (the `import` block); short package names are typed
everywhere.

### Interface placement

Interfaces live where they are **consumed**, not where they are
implemented (Go idiom):

- `es.Store` interface in `es/store.go`. Both aggregate runtime and
  projector consume it.
- `publisher.Publisher` interface in `publisher/` package.
- `kms.KeyStore` interface in `kms/` package.

Adapter modules import these interfaces from the core. The core
never imports an adapter.

### Versioning

- **Synchronized releases.** All modules share the same semantic
  version. Tag `v1.2.3` on the root module; tag
  `adapters/storage/postgres/v1.2.3` on the Postgres adapter
  submodule. Standard Go submodule tagging.
- **Release tooling.** A scripted release (Makefile, `mage`, or
  similar) tags every module in one shot. Version drift is a release-
  script bug, not a daily concern.
- **Strict semver from v0.1.** No surprise breaking changes within a
  major. ADRs document any future-breaking-by-design intent.

### What `go.work` looks like

```go
// go.work
go 1.22

use (
    .
    ./adapters/storage/postgres
    ./adapters/storage/sqlite
    ./adapters/publisher/restate
    ./adapters/publisher/nats
    ./adapters/publisher/sns
    ./adapters/publisher/pubsub
    ./adapters/publisher/cfqueues
    ./adapters/kms/aws
    ./adapters/kms/gcp
    ./adapters/kms/vault
    ./examples
)
```

Adapters depend on the core via `replace` (during development) and on
published versions (in production builds). The release script
maintains both.

### What this commits us to

- Adding a new adapter means creating a new submodule (one
  `go.mod`, one directory). The release script picks it up
  automatically once added to `go.work`.
- Cross-module breaking changes (e.g., changing the `Store`
  interface) become visible at compile time across every adapter.
- Examples and integration tests live in `examples/` (its own
  module) so their heavy deps never leak into consumer builds.

## Consequences

### Positive

- **Consumers depend only on what they use.** No transitive bloat.
- **Repo stays scannable.** Top-level directories cover core concerns
  and tooling; adapter sprawl is folded under `adapters/`.
- **Standard Go monorepo pattern.** Tooling, IDEs, and contributors
  understand it without explanation.
- **Interface contracts visible in a small set of files** in `es/`,
  `publisher/`, `kms/`. Adapter authors have one place to read.
- **`go.work` removes friction during development** — no
  `replace` directives to manage by hand.

### Negative

- **Synchronized release tooling required.** Tagging N modules
  consistently is a real chore without a script. We will write the
  script; the cost is up-front and once.
- **More files at the top of every adapter module** (each one has its
  own `go.mod`, `go.sum`, README). Standard, but a cognitive cost for
  contributors who are used to single-module Go repos.
- **Cross-module refactors are slightly heavier.** Renaming a method
  on `es.Store` updates every adapter; in single-module that's one
  PR, here it's one PR but with explicit module-version coordination.

## Alternatives Considered

### Single module

Rejected. Every consumer would pull every adapter's transitive deps —
`pgx`, sqlite, restate-sdk-go, nats.go, aws-sdk-go, gcp-go, cloudflare
libs, Vault client — into their `go.sum`. The convenience of single
`go.mod` is not worth the bloat.

### Polyrepo (each adapter in its own repo)

Rejected. Maximum isolation, brutal coordination overhead. Cross-
adapter changes become multi-repo dances. Wrong shape at this stage.

### Flat adapter layout (`storage/postgres/`, `publisher/restate/` at
root instead of grouped under `adapters/`)

Considered. Slightly shorter import paths. Rejected because the repo
root already has 10+ top-level directories from core concerns;
grouping adapters under `adapters/` keeps the root readable. The
import path cost is paid in `import` blocks (typed once per file),
not at call sites (which use the leaf package name).
