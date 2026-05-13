# ADR 0010: Crypto-Shredding for PII

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

Events are append-only and immutable. GDPR Article 17 ("right to be
forgotten") and equivalent regimes require effective deletion of personal
data on request. Hard-deleting events is not an option without violating
the framework's core invariant.

The standard answer is **crypto-shredding**: PII is stored encrypted under
a key tied to the data subject. "Forgetting" the subject means destroying
the key, after which the ciphertext is computationally inaccessible.

Several coupled decisions are required:

1. **Granularity.** Encrypt the whole payload, or individual fields?
2. **Default.** Are fields PII by default (opt-out) or non-PII by default
   (opt-in)?
3. **Subject model.** One subject per event, or multiple?
4. **Key custody.** Where do per-subject keys live? Same DB as events,
   external KMS, or hybrid?

## Decision

### Granularity: field-level

Codegen reads proto field options to decide what to encrypt. Encrypted
fields are wrapped in place inside the proto bytes payload; non-PII fields
remain plain.

Reason: after shredding, an event like `OrderPlaced` still surfaces
`amount`, `currency`, and `status` for ops and analytics. Whole-payload
encryption would render the entire event opaque, destroying the
observability win.

### Default: PII (encrypted), opt-out via annotation

Fields are encrypted by default. Mark known-safe fields with
`option (es.non_pii) = true`. Subject fields (`option (es.subject_field) = true`)
are implicitly non-encrypted — you would need the key to find the key.

Reason: a forgotten annotation should result in over-encryption, never a
leak. This is the secure-by-default posture.

### Codegen lint advisory

For primitive types (`int*`, `bool`, `enum`, `Timestamp`) left
default-encrypted, codegen emits a build-time **warning** suggesting
`(es.non_pii) = true` if intended. Suppressible per field with
`(es.pii_intentional) = true`. The build does not fail; this is
visibility, not enforcement.

Codegen also emits a `pii_manifest.json` per aggregate listing each field
and its classification. Checked into the repository. Diff-reviewed. Acts
as the audit artifact for privacy review.

### Subject model: multi-subject per event

Default subject inferred from `(es.subject_field) = true` on a top-level
field. Per-field override via `(es.subject) = "field_path"` for events
that touch multiple people (e.g., a transfer event encrypted under both
the sender's and recipient's keys).

Reason: simple cases get the simple path; multi-party events have a clean
expression without forcing awkward stream splitting.

### Key custody: hybrid envelope encryption

- **DEK** (data encryption key) per `(tenant, subject)`, stored encrypted
  in the `subject_keys` table.
- **KEK** (key encryption key) per tenant, held in a pluggable KMS
  (`KeyStore` interface).
- Reference KMS adapters: in-process (dev/SQLite), AWS KMS, GCP KMS,
  HashiCorp Vault.
- Hot read path: DEK fetched from `subject_keys`, unwrapped via KMS once,
  cached, used for AEAD operations.
- KMS is on the cold path (DEK wrap/unwrap, rotation). Not on the hot
  read path.

### Algorithm and wire format

- **AES-256-GCM** with random 96-bit IV per field.
- **Per-field wire format:** `version(1B) | iv(12B) | ciphertext | tag(16B)`.
  The version byte allows the algorithm to evolve later.

### Read API contract

- Decryption is transparent. The framework returns plaintext or a typed
  `RedactedValue{Subject, Reason}` sentinel where `Reason` is one of
  `"shredded"`, `"missing_key"`, `"kms_unavailable"`.
- The framework **never** returns ciphertext to the caller.
- The framework **never** silently substitutes a default value.
- Tag mismatch (corrupt or tampered ciphertext) returns typed
  `ErrCryptoIntegrity`. Hard error.
- KMS unavailable with no cached DEK returns `ErrKMSUnavailable`. No
  fallback to ciphertext or `RedactedValue`.

### Shredding contract

```
ForgetSubject(ctx, tenant, subject) error
```

Sets `dek_wrapped = empty` and `shredded_at = now()` on the
`subject_keys` row. Tombstone retained for compliance audit
("we deleted this on 2026-05-13"). Optionally nulls any `payload_json`
sidecar rows (ADR 0006) referencing that subject.

### Rotation

KEK rotates via versioned reference (`kek_version` on each
`subject_keys` row). A background job re-wraps DEKs under the new KEK.

DEKs themselves do not rotate. "Rotating a DEK" requires re-encrypting
all historical fields under it; if that is genuinely needed (e.g.,
algorithm compromise), the answer is to write new events under a new
subject identity, not background re-encryption of history.

## Consequences

### Positive

- **Effective GDPR deletion** without violating append-only.
- **Secure-by-default.** Forgetting an annotation means over-encryption,
  not a leak.
- **Tenant key isolation** bounds compromise blast radius. Dropping a
  tenant KEK crypto-shreds the entire tenant.
- **KMS only on the cold path.** Hot reads stay local and fast.
- **Audit-friendly.** `pii_manifest.json` plus shred tombstones plus KEK
  rotation logs provide a clean audit story.

### Negative

- **Codegen complexity is non-trivial.** Per-field encryption envelope
  generation, subject derivation, manifest emission, and the lint
  advisory together represent a meaningful chunk of the codegen plugin.
- **Per-field encryption overhead.** ~28 bytes per encrypted field
  (1B version + 12B IV + 16B tag).
- **Read API must handle `RedactedValue` everywhere.** Consumers cannot
  ignore it; the framework cannot pretend it isn't possible.
- **Subject-field requirement** constrains proto modeling: every event
  with PII must have an identifying field marked as the subject.

## Alternatives Considered

### Whole-payload encryption

Rejected. Post-shredding the entire event becomes opaque, destroying
observability for non-PII fields that are typically the most useful in
incident response and analytics.

### Opt-in PII (`option (es.pii) = true`)

Rejected. Forgetting an annotation results in a leak, not over-encryption.
Wrong direction for secure-by-default. The reverse audit question ("which
fields are safe?") is also easier with opt-out than with opt-in.

### One subject per event

Rejected. Multi-party events (transfers, shared resources) require either
awkward stream splitting or impossible encryption semantics. Multi-subject
adds modest codegen complexity for genuine flexibility.

### KMS-only (no DEK caching)

Rejected. KMS latency on every read is unacceptable, and KMS becomes a
hard runtime dependency in the read path.

### DEKs stored only in KMS

Rejected. Hot path becomes KMS-bound; shredding requires a KMS API call
rather than a local `DELETE`. Hybrid envelope encryption keeps the hot
path local and the cold path in KMS.

### Field-level JSONB encryption

Rejected. Requires the JSONB payload primary (ruled out by ADR 0006),
introduces brittle field-by-field encryption semantics, and complicates
schema evolution.
