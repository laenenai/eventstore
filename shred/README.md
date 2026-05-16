# shred

Crypto-shredding runtime. Field-level AES-256-GCM under per-subject
DEKs wrapped by tenant KEKs. The `aggregate.Runtime` auto-invokes
this package whenever an event implements the codegen-emitted
`PIIEncoder` interface.

## Load-bearing primitives

- [`Shredder`](shred.go) — the orchestrator. Combines a `kms.KeyStore`
  (KEK custody) with a `SubjectStore` (wrapped-DEK persistence) and
  an in-process DEK cache. One instance per app.
- [`SubjectStore`](shred.go) — adapter contract:
  `GetSubjectKey`, `UpsertSubjectKey`, `ForgetSubject`,
  `ListStaleSubjectKeys`. Both shipped storage adapters implement it.
- [`EncryptField` / `DecryptField`](shred.go) — the per-field
  primitive. Wire format is `version(1B) | iv(12B) | ciphertext | tag(16B)`;
  `version=0x01` = AES-256-GCM.
- [`ForgetSubject`](shred.go) — the operator action. Zeroes the
  wrapped DEK, sets `shredded_at`, clears the cache. All existing
  ciphertext for that subject becomes computationally inaccessible.
- [`RewrapDEKs`](shred.go) — operator helper that migrates stale-KEK
  DEKs to the current KEK version after a rotation. Paginated and
  resumable.
- [`PIIEncoder`](shred.go) — interface codegen emits on every event
  variant that carries an encrypted field. `aggregate.Runtime`
  type-asserts and calls `EncryptPII` / `DecryptPII` automatically.
- [`RedactedFields`](shred.go) — per-event accounting of fields that
  could not be decrypted (subject shredded, KEK missing). The
  affected fields are zeroed and the event is still returned.

## Contract

Encryption is opt-in at the proto annotation level; runtime simply
honors `PIIEncoder`. `DecryptField` returns `ErrShredded` on a
crypto-shredded subject and `es.ErrCryptoIntegrity` on tag mismatch;
the framework never silently returns ciphertext or a default. DEK
cache is process-local and best-effort — `ClearCache()` is safe at
any time. `SAD` (PCI sensitive auth data) is rejected at the codec
boundary, never reaches this package.

## Where to start reading

1. [`shred.go`](shred.go) — `Shredder`, `EnsureSubjectKey`,
   `EncryptField`, `DecryptField` end-to-end.
2. [`shred.go` § `PIIEncoder`](shred.go) — the interface that wires
   into `aggregate.Runtime`.
3. [`cache.go`](cache.go) — DEK cache shape and invalidation.

## Relevant ADRs

- [0010 — Crypto-Shredding](../docs/adr/0010-crypto-shredding.md)
- [0027 — Data Governance Model](../docs/adr/0027-data-governance-model.md)
  — classifies which fields trigger encryption and which stay plain.
- [Cookbook 11 — Crypto-Shredding](../docs/cookbook/11-crypto-shredding.md)
  — end-to-end recipe.
