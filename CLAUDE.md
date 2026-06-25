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
cmd/protoc-gen-es-go/                    protoc-gen-es-go plugin (part of root)
cmd/esctl/                               operator CLI (own go.mod)
examples/                                full worked examples (not published)
scripts/release.sh                       synchronized release across all published modules
```

When adding a new adapter that pulls heavy deps, give it its own
`go.mod` and add it to `go.work` + `scripts/release.sh`'s MODULES list.

## Codegen pipeline

1. Author proto under `proto/<domain>/<v1>/`.
2. Run `task generate` — buf invokes the standard protobuf-go plugin
   and the local `protoc-gen-es-go` plugin — a command in the root
   module (`cmd/protoc-gen-es-go`), run by `go run` via its import
   path, no install step.
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
  (ADR 0023). Aggregate state is mirrored synchronously in transaction;
  read it via the runtime's `Load`, not via event replay.
- **PII via crypto-shredding** — encryption is opt-in via the
  `(es.v1.data_classification)` field option. Default
  (`DATA_CLASSIFICATION_PUBLIC` or unset) is plaintext. Declaring
  `PERSONAL`, `QUASI_IDENTIFIER`, `SENSITIVE`, `FINANCIAL`,
  `CARDHOLDER`, `CREDENTIAL`, or `UNSTRUCTURED` engages per-subject
  encryption. `INTERNAL` stays plaintext but is excluded from DSAR
  export. `SAD` (PCI sensitive auth data) is **rejected at runtime**
  — never persistable. Works on both `string` (ciphertext base64'd
  to stay UTF-8) and `bytes` (ciphertext raw) fields. `ForgetSubject`
  destroys the DEK (ADR 0010, ADR 0027, cookbook 11).
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
  state_cache (ADR 0023). The historical `snapshots` table is dropped
  by migrations 00009 (sqlite) / 00010 (postgres); don't reintroduce it.

## Commit classification

The auto-release workflow in `.github/workflows/ci.yml` cuts a new
synchronized version tag on every push to `main` whose squash
subject carries a `feat:` or `fix:` prefix. The gate is a heuristic
for "material adopter-facing change worth a release." Lean toward
the heuristic catching the right things, not toward strict
classification by code-diff shape.

- **`feat:` when adopter capability expands**, even if the code
  surface is tiny. Retracting a documented limitation, enabling a
  new deployment shape, adding a supported configuration
  combination, exposing a new test-support helper that unlocks a
  workflow — all `feat:`. Code-diff size is not the primary signal;
  adopter capability change is.
- **`fix:` for bug fixes adopters experience.**
- **`chore:` for repo-internal mechanics** with no adopter
  visibility: dependency bumps where the public API surface is
  unchanged, CI infrastructure, build tooling.
- **`docs:` for documentation-only changes**: ADRs that capture
  strategic direction without shipping code, README rewrites,
  recipe additions. If the ADR *also* retracts a limitation that
  changes what's supported, the PR landing it should be `feat:`,
  not `docs:`.

When in doubt, ask: "what can an adopter do now that they couldn't
before?" A real answer → `feat:`. Just "they have a better mental
model" → `docs:`.

The cost of misclassifying down (`feat:` → `docs:`) is a missed
release. The cost of misclassifying up (`docs:` → `feat:`) is an
extra version bump on a non-material change. The first is harder
to notice and harder to recover from. Bias toward `feat:` when
genuinely uncertain.

## Branching: trunk-based with held feature branches

The repo runs trunk-based development. Almost everything goes
straight to `main` via small PRs. There is no `develop`, no `v1`,
no long-lived integration branch. Stability commitments are
expressed via release tags (per `scripts/release.sh`), not via
branches — adopters who need a fixed surface pin a tag.

A PR may be **held** on its feature branch (open but not merged)
only when ALL of these hold:

1. There is a **documented validation gate** the PR is waiting
   on. Examples: a spike's measurement phase, an external
   dependency cutting a release, a regulator's sign-off. "We
   haven't reviewed it yet" is not a gate.
2. The gate has a **defined owner and target date**. If both
   slip indefinitely the PR closes; the work either re-opens
   later or never lands. Better to close cleanly than to
   accumulate held PRs that no one tracks.
3. The PR description names the **decision criterion** — what
   would change the merge / don't-merge call. A reviewer should
   be able to read the description and know what evidence would
   move the needle.
4. The branch is **rebased against main weekly** (or whenever
   main lands a relevant change, whichever is sooner) to
   surface drift early. A held PR that's two months behind main
   has already lost the testing it claims to have.

If a PR can't satisfy all four, it should not be held. Either
merge it (with a feature flag if necessary, but the framework
deliberately avoids flags — see the "Don'ts" section above),
close it pending a redesign, or split it into a landable
sub-change and a held remainder.

The standard escape valve for "we want to land this but it's not
ready everywhere" is **adding the capability without using it**:
ship the new code path, leave the old in place, switch over in a
follow-up. The eventstore framework's `WithoutRLS`,
`StateCodec` (opt-in), and `aggregate.Runtime.Shredder` (optional)
fields are examples of this pattern.

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
- **Any schema-touching change** — read [ADR 0030 (Schema Migration
  Discipline)](./docs/adr/0030-schema-migration-discipline.md) and
  pick a migration tier (A–F). Every PR that touches `proto/**`,
  `adapters/storage/*/migrations/`, `aggregate/runtime.go`,
  `es/envelope.go`, or `cmd/protoc-gen-es-go/emit_*.go` must declare its tier
  in the PR template. Reviewers refuse "Not applicable" claims that
  contradict the diff.

## Style

- Comments: lead with the **why**. The code says what; the comment
  earns its place by explaining a constraint, invariant, or surprising
  choice. Existing files demonstrate the bar.
- ADR tone: prose, not bullets-only. A future maintainer should
  reconstruct the reasoning without asking anyone.
- Cookbook tone: problem first, then the smallest working pattern,
  then **what NOT to do** and **failure modes**.
