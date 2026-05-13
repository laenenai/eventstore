# eventstore

An event sourcing framework for Go with first-class uniqueness, multi-
tenancy, and crypto-shredding. Targets scale-to-zero databases
(Neon, Turso) with delivery via an external durable runtime.

## Status

In development. The architectural design is complete; implementation is
in early stages.

## Design

- [Architecture Decision Records](docs/adr/) — 19 ADRs covering the
  framework's spine. Start with the [index](docs/adr/README.md).
- [Cookbook](docs/cookbook/) — patterns for application code that the
  framework deliberately does not provide (sagas, process managers,
  workflows, timeouts).

## Repository layout

```
es/               Core API (Decider, Envelope, StreamID, ...)
aggregate/        Aggregate runtime
projection/       Projector runtime
outbox/           Outbox primitives + drain helpers
snapshot/         Snapshot primitives
shred/            Crypto-shredding logic
publisher/        EventPublisher interface (+ inproc adapter)
kms/              KeyStore interface (+ inproc adapter)
estest/           Test harness (given/when/then)
proto/            Framework's own .proto files
gen/              Generated Go from framework protos
cmd/              CLI tools
  protoc-gen-es-go/   Codegen plugin (Phase 2)
  esctl/              Operational CLI
adapters/
  storage/
    postgres/     Postgres adapter (pgx)
    sqlite/       SQLite adapter (driver-agnostic; modernc / libsql)
  publisher/      External publisher adapters (Phase 2+)
  kms/            External KMS adapters (Phase 2+)
docs/
  adr/            Architecture Decision Records
  cookbook/       Application-pattern recipes
```

## Toolchain

Versions are pinned in [`.mise.toml`](.mise.toml). Install
[mise](https://mise.jdx.dev) and run:

```sh
mise install     # installs go, buf, sqlc, task at the pinned versions
```

Common workflows are defined in [`Taskfile.yml`](Taskfile.yml):

```sh
task            # list tasks
task test       # run tests across all modules
task generate   # buf + sqlc codegen
task lint:proto # buf lint + buf breaking
task vet
task build
```

## License

TBD.
