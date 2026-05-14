# CLAUDE.md

Guidance for Claude Code sessions working in this repo. Keep this file
focused on what isn't obvious from reading the code; the README, ADRs,
and cookbook carry the real reference material — point at them instead
of duplicating.

## What this is

Event-sourcing framework for Go. Aggregates are defined as proto files;
`protoc-gen-es-go` (custom codegen) emits sealed sum types, codecs, and
optional workflow handlers. Hand-written per aggregate: the
`Decider{Initial, Decide, Evolve}` and the error sentinels.

Authoritative entry points:

- [README](./README.md) — design summary + "defining an aggregate"
  walkthrough.
- [docs/adr/README.md](./docs/adr/README.md) — 26 ADRs, the **why**
  behind every load-bearing decision. Read these before challenging a
  pattern; the alternatives have usually been considered.
- [docs/cookbook/README.md](./docs/cookbook/README.md) — 15 recipes for
  application-level patterns the framework deliberately does not bake
  in (sagas, process managers, HTTP edge, schema evolution).
- [docs/architecture/overview.md](./docs/architecture/overview.md) —
  high-level shape.

## Module layout

Multi-module monorepo. Each adapter with heavy deps carries its own
`go.mod`; examples are not published. `go.work` ties everything
together for local development.

```
.                                        root module (es/, aggregate/, cmdworkflow/, ...)
adapters/storage/{postgres,sqlite}/      own go.mod (pgx / sqlite drivers)
adapters/cmdworkflow/{restate,dbos}/     own go.mod (workflow SDKs)
adapters/cmdworkflow/inproc/             part of root (no heavy deps)
adapters/authz/cedar/                    own go.mod (cedar-go)
adapters/httpedge/connect/               own go.mod (connectrpc.com/connect)
adapters/{kms,publisher}/inproc/         part of root
proto/                                   framework + example aggregate protos
gen/                                     generated Go (DO NOT hand-edit)
proto-gen/                               protoc-gen-es-go plugin source
examples/                                full worked examples (not published)
scripts/release.sh                       synchronized release across all published modules
```

When adding a new adapter that pulls heavy deps, give it its own
`go.mod` and add it to `go.work` + `scripts/release.sh`'s MODULES list.

## Codegen pipeline

1. Author proto under `proto/<domain>/<v1>/`.
2. Run `task generate` — buf invokes the standard protobuf-go plugin
   and the local `protoc-gen-es-go` plugin (resolved via `go.work`,
   no install step).
3. Generated files land in `gen/...` as `*.pb.go` (standard) and
   `*_es.pb.go` (framework-specific sum types, codecs, Restate/DBOS
   handlers). **Never hand-edit `_es.pb.go` files** — they're
   regenerated.
4. `task generate:check` is the CI gate — fails the build if generated
   code is out of date relative to source.

The codegen plugin runs in three modes via the `runtime=` option in
`buf.gen.yaml`: default (sum types + codecs), `runtime=restate`,
`runtime=dbos`. The Restate/DBOS outputs land in their respective
adapter `gen/` trees so the SDK deps stay scoped to the adapter.

## Conventions

- **Decider pattern** — `Initial / Decide / Evolve` is the only
  aggregate model (ADR 0003). No anemic models, no command handlers
  separate from state machines.
- **Sealed sum types** — Commands and events are proto containers with
  the `es.v1.sum_type` option (ADR 0004). Codegen produces the sealed
  interface; user code switches on the variant.
- **Multi-tenancy is mandatory** — every operation requires
  `es.WithTenant(ctx, tenantID)` (ADR 0007). The framework refuses to
  operate without one. `StreamID` is `tenant:type:id` canonical form
  (ADR 0008).
- **State lives in `state_cache`** — `state_cache` supersedes snapshots
  (ADR 0023). The `snapshot/` package is dead code awaiting deletion.
  Aggregate state is mirrored synchronously in transaction; read it
  via the runtime's `Load`, not via event replay.
- **PII via crypto-shredding** — proto `bytes` fields with no
  `non_pii` annotation are encrypted per-subject; `ForgetSubject`
  destroys the DEK (ADR 0010, cookbook 11).
- **Commands via the bus** — production wiring is
  `cmdworkflow.Workflow` over Restate or DBOS (ADR 0025/0026, cookbook
  14). `inproc` is for tests only.
- **HTTP edge is user-land** — `adapters/httpedge/connect` is a thin
  runtime helper, not codegen. The framework stays transport-neutral
  (cookbook 15).

## Don'ts

- **Don't hand-edit generated code** — anything in `gen/`, any
  `*_es.pb.go`. Regenerate via `task generate`.
- **Don't `git add -A` or `git add .`** — stage files by name. This
  repo has a history of binaries (`examples/connectedge/connectedge`)
  and other transient files slipping in via blanket adds.
- **Don't use `--no-verify`** — if a hook fails, fix the cause.
- **Don't bypass tenant context** — never call store methods without
  `es.WithTenant(ctx, ...)`; you'll get `ErrTenantMissing`. Don't
  paper over it with empty tenants.
- **Don't add features beyond the task** — bug fixes don't need
  surrounding cleanup; abstractions earn their place by being used
  three times.
- **Don't introduce snapshot infrastructure** — superseded by
  state_cache. The `snapshot/` package should be deleted, not extended.

## Commands

All via [Taskfile](./Taskfile.yml):

- `task test` — run unit tests across every module.
- `task vet` — `go vet` across every module.
- `task build` — `go build` across every module.
- `task generate` — buf + protoc-gen-es-go.
- `task generate:check` — fail if generated code is out of date (CI).
- `task lint:proto` — `buf lint` + `buf breaking` against `main`.
- `task release VERSION=v0.1.0` — synchronized tag across every
  published module via `scripts/release.sh`. Run from `main`, clean
  tree, in sync with origin. CI release workflow at
  `.github/workflows/release.yml` is the operator-triggered surface.

For one-off Go work in a specific module, `cd` into the module
directory first — each has its own `go.mod`.

## Working with the framework's primitives

- **Adding an aggregate** — invoke the `define-aggregate` skill;
  steps live there.
- **Adding an ADR** — invoke the `add-adr` skill.
- **Adding a cookbook recipe** — invoke the `add-recipe` skill.
- **Schema evolution** — read ADR 0013 + ADR 0023 first. The
  upcaster + `state_schema_version` bump + state_cache invalidation
  ordering matters; getting it wrong breaks replays.

## Style

- Comments: lead with the **why**. The code says what; the comment
  earns its place by explaining a constraint, invariant, or surprising
  choice. Existing files demonstrate the bar.
- ADR tone: prose, not bullets-only. A future maintainer should
  reconstruct the reasoning without asking anyone.
- Cookbook tone: problem first, then the smallest working pattern,
  then **what NOT to do** and **failure modes**.
