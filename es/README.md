# es

Core public API. Every adapter and every consumer hits these types
first. No business logic lives here — `es` is the seam between the
framework and its adapters, and between the framework and the apps
that embed it.

## Load-bearing primitives

- [`Decider[S, C, E]`](decider.go) — the `Initial / Decide / Evolve`
  triple that defines an aggregate (ADR 0003).
- [`Envelope`](envelope.go) — Go-side wrapper around every event;
  carries hash-chain fields (ADR 0005, ADR 0028).
- [`StreamID`](streamid.go) — canonical `tenant:type:id` identity with
  slug validation (ADR 0008).
- [`Store`](store.go) — the storage contract every adapter implements
  (`Append`, `ReadStream`, `ReadAll{,ForTenant}`, `CurrentStreamVersion`,
  `GetEventByID`).
- [`AppendParams` / `ConstraintOp`](command.go) — one transactional
  append with optional uniqueness claims/releases and an in-tx
  state_cache write (ADR 0010, ADR 0023).
- [`ComputeChainHash` / `VerifyStreamChain`](chain.go) — tamper-evident
  per-stream SHA-256 chain (ADR 0028).
- [`AccessLevel` / `MinLevelFor`](access.go) — view-scope ladder
  consumed by codegen-emitted `View(level)` / `LogValue()` helpers
  (ADR 0027).
- [`WithTenant` / `RequireTenant`](tenant.go) — the mandatory-tenancy
  gate (ADR 0007).

## Contract

`es` is the framework/adapter boundary. Types here have stable wire
encodings; the `Envelope` fields drive both storage layout and the
chain hash, so any new field is at minimum a Tier B–F migration under
ADR 0030. All public errors are sentinels matched via `errors.Is`.

## Where to start reading

1. [`store.go`](store.go) — `Store` interface, `AppendParams`,
   `AppendResult`. Read this first; it dictates what an adapter does.
2. [`envelope.go`](envelope.go) — the wire shape that flows through
   every adapter.
3. [`decider.go`](decider.go) — how aggregates are written.

## Relevant ADRs

- [0003 — Decider Aggregate Model](../docs/adr/0003-decider-aggregate-model.md)
- [0005 — Event Envelope Schema](../docs/adr/0005-event-envelope-schema.md)
- [0007 — First-Class Multi-Tenancy](../docs/adr/0007-first-class-multi-tenancy.md)
- [0008 — Stream Identity](../docs/adr/0008-stream-identity.md)
- [0010 — Crypto-Shredding](../docs/adr/0010-crypto-shredding.md)
- [0014 — Outbox Shape](../docs/adr/0014-outbox-shape.md) — `OutboxStore`
  / `OutboxAdmin` interfaces live in [`outbox.go`](outbox.go).
- [0020 — Projections and Read Models](../docs/adr/0020-projections-and-read-models.md)
  — `ProjectionAdmin`, `ProjectionDLQ*` live in
  [`projection_admin.go`](projection_admin.go).
- [0023 — state_cache supersedes snapshots](../docs/adr/0023-state-cache-supersedes-snapshots.md)
  — `StateCache*` interfaces in [`state_cache.go`](state_cache.go).
- [0024 — state_stream](../docs/adr/0024-state-stream.md) — `StateEnvelope`,
  `StatePublisher` in [`state_envelope.go`](state_envelope.go).
- [0027 — Data Governance Model](../docs/adr/0027-data-governance-model.md)
  — drives `AccessLevel`.
- [0028 — Tamper-Evident Hash Chain](../docs/adr/0028-tamper-evident-chain.md)
- [0030 — Schema Migration Discipline](../docs/adr/0030-schema-migration-discipline.md)
