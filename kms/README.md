# kms

Framework-defined KMS abstraction. Crypto-shredding uses envelope
encryption: per-subject DEKs wrapped by tenant KEKs held in the KMS.
KMS sees the wrap/unwrap calls; it never sees plaintext events and
rarely sees plaintext DEKs (only at initial generation and during
rotation).

## Load-bearing primitives

- [`KeyStore`](keystore.go) — three-method contract every adapter
  implements: `WrapDEK`, `UnwrapDEK`, `CurrentKEKVersion`. The
  versioned-wrap shape exists so a tenant can rotate KEKs while
  historical wrapped DEKs continue to decrypt under their stored
  version.
- [`KEKRotator`](keystore.go) — optional in-process rotation
  (`RotateKEK`). Implemented by the inproc adapter and by host-
  managed KMS adapters; external KMS providers rotate via their
  own controls instead.

## Contract

The framework's `shred.Shredder` is the only first-party caller.
Adapters MUST be safe for concurrent use across goroutines. The
returned `kekVersion` from `WrapDEK` MUST be monotonically
non-decreasing for a given tenant. `UnwrapDEK` MUST succeed for any
version the adapter previously emitted unless rotation has
explicitly retired it.

## Where to start reading

1. [`keystore.go`](keystore.go) — three-method interface plus
   `KEKRotator`. The whole package is one file plus this README.
2. `adapters/kms/inproc/` — reference implementation for tests and
   SQLite dev; the simplest path to seeing the contract exercised.

## Relevant ADRs

- [0010 — Crypto-Shredding](../docs/adr/0010-crypto-shredding.md) —
  defines envelope encryption, KEK rotation flow, and shred semantics.
