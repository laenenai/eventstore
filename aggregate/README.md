# aggregate

The aggregate runtime — load, decide, append in one transactionally-
coherent call against an `es.Store`. Pure orchestration; all business
logic lives in the user's `Decider`.

## Load-bearing primitives

- [`Runtime[S, C, E]`](runtime.go) — the runtime struct; `Load`,
  `Handle`, `EncodeState`, `DecodeState`. Construct by struct literal
  or via [`NewProto`](new.go) for proto-state aggregates.
- [`Codec[E]`](runtime.go) — event marshal/unmarshal contract;
  codegen-emitted per aggregate. Upcasting (ADR 0013) is the codec's
  responsibility.
- [`StateCodec[S]`](state_codec.go) + [`ProtoStateCodec`](state_codec.go)
  — state mirror for the Tier-1 state_cache write path and snapshot
  read path (ADR 0023). Default codec is protojson over the state's
  proto descriptor.
- [`RebuildStateCache`](rebuild.go) — operator helper: wipes the
  state_cache for one aggregate type, replays events, upserts fresh
  rows. Use after a state-schema change or initial enable (ADR 0023).
- [`HandleOption` family](runtime.go) — `WithCommandID`,
  `WithCorrelationID`, `WithCausationID`, `WithOccurredAt`,
  `WithActor`. Override envelope metadata per call (ADR 0015).

## Contract

`Handle` is `Load` + `Decide` + `Append` in one call. It guarantees
the post-Decide state and the events commit together (state_cache
when `StateCodec` is wired). PII fields on codegen-emitted events are
auto-encrypted on append and auto-decrypted on load when `Shredder`
is set (ADR 0010). `Decider.Evolve` MUST be pure — it runs during
replay.

## Where to start reading

1. [`runtime.go`](runtime.go) — `Runtime` struct fields and the
   `Load` / `Handle` methods.
2. [`state_codec.go`](state_codec.go) — `StateCodec` interface and the
   `ProtoStateCodec` default.
3. [`rebuild.go`](rebuild.go) — `RebuildStateCache` workflow.

## Relevant ADRs

- [0003 — Decider Aggregate Model](../docs/adr/0003-decider-aggregate-model.md)
- [0009 — Postgres Global Position](../docs/adr/0009-postgres-global-position.md)
- [0010 — Crypto-Shredding](../docs/adr/0010-crypto-shredding.md) —
  drives the `Shredder` field and the auto-encrypt/decrypt path.
- [0013 — Schema Evolution and Upcasters](../docs/adr/0013-schema-evolution-upcasters.md)
- [0015 — Decider Output Scope and Saga Boundary](../docs/adr/0015-decider-output-and-saga-scope.md)
- [0020 — Projections and Read Models](../docs/adr/0020-projections-and-read-models.md)
  — Tier-1 state_cache write happens here.
- [0023 — state_cache supersedes snapshots](../docs/adr/0023-state-cache-supersedes-snapshots.md)
- [0029 — Per-Command Subscriber Batch Delivery](../docs/adr/0029-per-command-subscriber-batch-delivery.md)
  — `EncodeState` / `DecodeState` feed the cmdworkflow journal.
