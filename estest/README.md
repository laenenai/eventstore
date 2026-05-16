# estest

Shared test machinery for adapters. The package's central deliverable
is the **storage conformance suite** — every adapter implementation
must pass the same `RunStoreConformance(t, setup)` exercise so the
framework's `es.Store` contract is uniformly enforced.

## Load-bearing primitives

- [`RunStoreConformance`](conformance.go) — drives every adapter
  test file under `adapters/storage/*/adapter_test.go`. Runs
  `AppendAndReadStream`, `OptimisticConcurrency`,
  `UniqueConstraintClaim`, `GlobalPositionMonotonic`,
  `ReadStreamFromVersion`, `GetEventByID`, `MultiEventAppend`.
- [`StoreSetup`](conformance.go) — `func() es.Store`. Adapter test
  entrypoints construct the store, hand it to the suite, and own
  cleanup (typically via `t.Cleanup`).
- [`MakeEvent`](conformance.go) — populated `es.EventToAppend` test
  fixture. Use in adapter-specific tests for the standard event
  shape.
- [`MustStream`](conformance.go) — `t.Fatal`-on-error wrapper around
  `es.NewStreamID`.

## Contract

The conformance suite is the binding shape of an adapter — passing
it is the framework's gate for "this adapter satisfies `es.Store`".
Subtests are reported as `TestConformance/<TestName>`. Each subtest
uses a fresh tenant via the package-internal counter so a shared
store can be reused without cross-test leakage.

## Where to start reading

1. [`conformance.go`](conformance.go) — the suite itself; one file.
2. `adapters/storage/sqlite/adapter_test.go` — reference invocation:
   wires the adapter, calls `RunStoreConformance`, adds adapter-
   specific extras.

## Relevant ADRs

- [0002 — Library Delivery Model](../docs/adr/0002-library-delivery-model.md)
  — conformance is the contract that lets adapters live in their own
  modules.
- [0009 — Postgres Global Position](../docs/adr/0009-postgres-global-position.md)
  — `GlobalPositionMonotonic` enforces the invariant.
- [0010 — Crypto-Shredding](../docs/adr/0010-crypto-shredding.md) —
  `UniqueConstraintClaim` covers the constraint table both adapters
  ship.
