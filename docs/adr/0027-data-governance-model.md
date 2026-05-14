# ADR 0027: Data Governance Model — Classification, Access Levels, and Codegen

- **Status:** Accepted
- **Date:** 2026-05-15
- **Builds on:** ADR 0010 (crypto-shredding mechanism).

## Context

ADR 0010 settled the encryption mechanism: per-subject DEKs, KEK envelope
encryption, AES-256-GCM at the field, the `Shredder` runtime and the
`ForgetSubject` semantics. The policy lever was a single bit per field
— `(es.v1.pii) = true` (later inverted to opt-out via `(es.v1.non_pii)`),
plus the secure-by-default convention that any unannotated `bytes` field
got encrypted. One annotation, one effect: "encrypt or don't".

That bit served the GDPR Article 17 right-to-be-forgotten case well and
nothing else. As soon as real domains landed, the model broke in three
directions:

1. **PCI scope is not just "encrypted".** PCI-DSS draws a sharp line
   between Cardholder Data (PAN, expiry, cardholder name on card — must
   be encrypted, audited on every read, segregated) and Sensitive
   Authentication Data (CVV, PIN, full track — **must not be persisted**
   post-authorization). A single PII bit cannot express "encrypt
   normally" versus "refuse to persist at all". Treating SAD as PII just
   meant SAD got encrypted and stored — which is itself a PCI violation.

2. **GDPR Article 9 special categories need stricter treatment than
   ordinary personal data.** Health data, biometric templates, political
   affiliations, sexual orientation, race/ethnicity — these are PII, but
   they also carry an audit-on-read requirement, a shorter default
   retention window, and an explicit consent gate before DSAR export
   includes them. The PII bit said all PII fields were equal. They are
   not.

3. **Retention is not uniform.** A FATCA TIN is personal data, but
   crypto-shredding it before the regulatory retention window has
   elapsed is a regulatory violation in many jurisdictions
   (typically 5–10 years for tax records). Authentication credentials
   (password hashes, MFA seeds) are personal data, but **must never
   appear in a DSAR export** — the subject's "data they can take with
   them" does not include the means to authenticate as them. Quasi-
   identifiers (date of birth, postal code, employer name) need
   k-anonymity treatment in analytics pipelines.

Every one of these is a per-field policy decision driven by the
regulatory regime the field falls under. A boolean cannot carry the
information. A second annotation per behavior (one for encryption, one
for DSAR scope, one for audit, one for retention class) would
combinatorially explode and produce annotation soup at the proto layer.

The pattern that works in regulated industries is the **classification
register**: one tag per field naming its regime, and a rule engine that
derives every behavior from the tag. This ADR extends ADR 0010's
encryption mechanism with that policy layer.

## Decision

### `DataClassification` enum, ten values plus UNSPECIFIED

Replace `(es.v1.pii)` / `(es.v1.non_pii)` / `(es.v1.pii_intentional)`
with a single field option `(es.v1.data_classification)` whose value is
a `DataClassification` enum. Numeric values are pinned forever; new
categories are appended.

| Value | Regime | Encryption | DSAR | Audit-on-read | Retention |
| ----- | ------ | ---------- | ---- | ------------- | --------- |
| UNSPECIFIED | default — treated as PUBLIC | none | yes | no | standard |
| PUBLIC | non-sensitive (ISO codes, enums) | none | yes | no | standard |
| INTERNAL | non-PII, not public (system flags, computed metrics) | none | **no** | no | standard |
| PERSONAL | GDPR Art 4 — directly identifies a person | per-subject | yes | no | standard |
| QUASI_IDENTIFIER | non-identifying alone, identifying in combination (HIPAA Safe Harbor lineage) | per-subject | yes | no | standard |
| SENSITIVE | GDPR Art 9 special category (health, biometric, political, sexual orientation, race) | per-subject | yes (\*) | **yes** | shorter |
| FINANCIAL | balances, transactions, source-of-funds, TINs | per-subject | yes (\*) | optional | **tax-locked** |
| CARDHOLDER | PCI-DSS CHD (PAN, expiry, cardholder name on card) | per-subject | yes | **yes** | PCI-scope |
| SAD | PCI-DSS Sensitive Authentication Data (CVV, PIN, full track) | **REJECTED** | n/a | n/a | n/a |
| CREDENTIAL | password hashes, API tokens, OAuth secrets, MFA seeds | per-subject | **never** | yes | standard |
| UNSTRUCTURED | free-form notes, descriptions, chat messages (may contain spilled PII) | per-subject | yes | no | standard |

(\*) DSAR for SENSITIVE requires an explicit per-Art-9 consent check;
FINANCIAL respects tax-retention windows before the underlying delete
can proceed.

The four behavior columns (encryption / DSAR / audit-on-read /
retention) are derived from classification by the framework. Application
teams do not annotate them independently; one annotation, four effects.

### Unannotated fields default to PUBLIC

The opt-out default of ADR 0010 ("unannotated bytes = encrypted") is
reversed. Unannotated fields default to PUBLIC — no encryption.
Encryption is **opt-in** from PERSONAL onwards.

This is a deliberate inversion. The original opt-out chose
secure-by-default for the leak case; the new model chooses
declared-by-default for the audit case. The reverse audit question
("what's encrypted?") used to require scanning every field for the
absence of an opt-out annotation; it now reads from the manifest. PRs
that introduce a new PII field surface as a manifest diff — encryption
status is one diff line away from being noticed in review, instead of
being silently inherited from a `bytes` field type.

The cost: a forgotten classification annotation on a new PII field now
results in leak-by-default, not over-encryption. The mitigation is that
the manifest is checked in and PR-reviewed; the discipline that ADR
0010 placed on the type annotation has moved to the manifest review
discipline. For domains where this trade-off feels wrong, the recipe
is to add a CI gate that fails when a manifest gains a new field
classified PUBLIC in an aggregate already containing PERSONAL fields —
the codegen plugin emits per-field structure suitable for that linter.

### SAD is rejected at write time, not at proto-define time

A `DATA_CLASSIFICATION_SAD` field is legal in a proto — it has to be,
because real PCI domains model the full authorization payload before
deciding what to persist. The framework refuses to *encrypt* it:
codegen emits an `EncryptPII` path that returns a typed error citing
the SAD classification before any bytes leave memory. The rejection is
runtime, not compile-time, because compile-time rejection would prevent
modeling the authorization message at all.

This is the strictest possible mode short of refusing to compile the
proto. Application teams who want compile-time enforcement can layer
a `buf lint` rule that forbids SAD in messages annotated as event or
aggregate-state types. The framework leaves that layer to the
application.

### Per-aggregate manifest is the authoritative record

Codegen emits one `<aggregate>_pii_manifest.json` per package next to
the generated Go. Each field record carries the four derived columns:

```json
{
  "name": "legal_name",
  "classification": "DATA_CLASSIFICATION_PERSONAL",
  "encryption": "subject_string_base64",
  "dsar_export": true,
  "audit_on_read": false,
  "retention": "standard"
}
```

The manifest is checked into the repository. PRs touching event protos
surface the manifest diff in code review. Privacy reviewers, PCI
auditors, and DSAR-exporter tooling all consume the manifest directly
— they never re-derive the rules from the enum values. If a field's
classification changes, every regulator-facing artifact sees the same
shape in one place.

The `encryption` column enumerates the wire-format choice:

- `none` — plaintext.
- `subject_bytes` — raw ciphertext, no base64. For `bytes` proto
  fields.
- `subject_string_base64` — base64'd ciphertext for `string` proto
  fields. ~33% storage overhead, UTF-8-safe.
- `rejected_sad` — SAD field; runtime rejects on encrypt.

### `AccessLevel` ladder — Public, Internal, Subject, Compliance, Operator

The classification annotation labels the data's sensitivity. The
`es.AccessLevel` ladder labels the caller's scope. The pair drives the
codegen-emitted `View(level)` helper: a deep copy with fields above the
caller's level zero-valued.

Five levels:

- **AccessLevelPublic** — PUBLIC fields only. Cross-tenant safe.
  Analytics aggregates, anonymous metrics.
- **AccessLevelInternal** — adds INTERNAL. Back-office dashboards,
  support consoles. Default for `slog.LogValue`.
- **AccessLevelSubject** — adds PERSONAL, QUASI_IDENTIFIER,
  UNSTRUCTURED. What the data subject (GDPR Article 4(1)) is entitled
  to see about themselves: DSAR exports, self-service UIs, "my account"
  screens.
- **AccessLevelCompliance** — adds SENSITIVE (Art 9), FINANCIAL,
  CARDHOLDER. Compliance officers, fraud/risk teams, audit pulls.
- **AccessLevelOperator** — adds CREDENTIAL. Full read for system
  internals. Treat as god mode; no audience above this.

The Subject name is deliberate. The framework's
`(es.v1.subject_field)` annotation already names the
encryption-subject — the natural person whose key gates the data. The
same person is the entity entitled to see their data under GDPR Art 15.
Naming the access-level "Subject" makes the two concepts share a
vocabulary. Domain-agnostic: the subject is the customer in a fintech,
the employee in HR, the patient in healthcare, the account-holder in
B2B SaaS — the framework does not care which.

Alternatives considered for the level name: "Customer", "Owner",
"Self". Customer is fintech-coloured and wrong for HR/health. Owner
implies authority over the data, which a GDPR data subject does not
have in the legal-control sense (they have rights of access, not
ownership). Self is ambiguous between "the caller" and "the subject".
Subject is the term GDPR Art 4(1) uses, the term the framework
already uses for the encryption-key handle, and the term most other
regulators use for the same concept.

A future-proof note: unknown/future classifications map to
`AccessLevelOperator` (closed-by-default). An older binary reading a
proto with a newer enum value hides the field at every level below
Operator. This is the safer failure mode — older code that doesn't
recognise a new classification can't accidentally leak the field.

### Codegen emits View, LogValue, Clone per message

Three methods land on every codegen-emitted message:

```go
func (m *Hired) Clone() *Hired
func (m *Hired) View(level es.AccessLevel) *Hired
func (m *Hired) LogValue() slog.Value
```

**`Clone`** is a deep copy specialised to the concrete type. Faster than
`proto.Clone`, no reflection, returns the right pointer type so callers
get compile-time field checking. Handles oneofs (type-switch over the
wrapper struct), maps, repeated fields, nested messages. Nil-safe.

**`View(level)`** is the access-control deep copy. Subject fields are
always visible (they are key handles, not identifying data). Every
other field is gated on `level >= MinLevelFor(classification)`. Nested
messages recurse at the same level. Returns nil for nil receiver.

For an Employee proto with PERSONAL `legal_name`, QUASI_IDENTIFIER
`date_of_birth`, INTERNAL `department`, INTERNAL `current_role`:

```go
emp.View(es.AccessLevelInternal)  // legal_name, date_of_birth zeroed
emp.View(es.AccessLevelSubject)   // all four visible
emp.View(es.AccessLevelOperator)  // identical to Subject (no CREDENTIAL here)
```

**`LogValue`** implements `slog.LogValuer` at `AccessLevelInternal`.
Fields above that level render as `[REDACTED:<CLASS>]` markers —
`[REDACTED:PERSONAL]`, `[REDACTED:CARDHOLDER]`, etc.
`slog.Info("...", "event", e)` is safe by default. This is the
defence-in-depth pairing for at-rest encryption: classification stops
a misconfigured logger from leaking; encryption stops a dropped
database from leaking. Different attackers, different defences.

### Oneofs with mixed classifications use the strictest variant

A oneof whose variants carry different classifications uses the
strictest one for the wrapper's access gate. The framework's policy:
when in doubt, hide more. Application teams who want per-variant
behavior split the oneof into separate fields.

This is not a frequent case in practice (oneofs usually carry
classification-homogeneous variants like "command oneof" or "event
oneof") but the rule has to be deterministic for codegen to emit
stable code.

## Consequences

### Positive

- **Regulator-readable manifests.** PCI auditors, GDPR DPOs, and DSAR
  exporters consume one JSON document per aggregate. No code-walking,
  no re-deriving rules. The classification → behavior table is the
  single source of truth.
- **Per-field governance.** SENSITIVE (Art 9) gets audit-on-read.
  FINANCIAL respects tax retention. CARDHOLDER lands in PCI scope.
  CREDENTIAL never DSAR-exports. SAD never persists. All from one
  annotation per field.
- **Ergonomic plaintext-string PII.** The previous bytes-only
  convention forced everything through `[]byte`, which leaked into
  every consumer (Decider state, projections, DTOs). String PII fields
  now round-trip through base64'd ciphertext at rest and remain
  natural-typed in code. Worth the ~33% storage overhead for the
  ergonomic win.
- **`LogValue` makes structured logging safe by default.** No
  call-site discipline; misconfigured slog handlers and third-party
  log shippers see redacted markers instead of cleartext.
- **`View(level)` makes DSAR export a one-liner.** No per-field
  allowlist maintained in handler code; the proto annotation is the
  allowlist.

### Negative

- **String PII has ~33% storage overhead.** Base64 envelope around the
  ciphertext. For genuinely binary PII (biometric templates, MRZ
  scans, signed PDF blobs), use the `bytes` proto type to skip the
  base64.
- **SAD is a runtime reject, not a compile-time reject.** Application
  teams who want compile-time enforcement layer a `buf lint` rule.
  The framework's position is that compile-time rejection would block
  modeling the authorization message at all.
- **Oneofs with mixed classifications use the strictest variant's
  gate.** Less precise than per-variant gating; the determinism win
  is judged worth the precision loss.
- **Unannotated fields default to PUBLIC** rather than encrypted. A
  forgotten annotation now leaks rather than over-encrypts. Mitigated
  by the checked-in manifest making the leak surface in PR review.
  Domains needing stricter default can add a CI gate that refuses
  PUBLIC siblings in an aggregate already containing PERSONAL fields.
- **No migration shim from `(es.v1.pii)` / `(es.v1.non_pii)` /
  `(es.v1.pii_intentional)`.** Those annotations are removed. Adopters
  rewrite their protos to use `(es.v1.data_classification)`. Worth
  the breakage to keep the option surface minimal — three legacy
  annotations carrying overlapping meaning never converged cleanly.

### Neutral

- **The codegen plugin grows.** Three new per-message methods (View,
  LogValue, Clone) plus the manifest enrichment. Mechanical;
  generated code is straightforward to read and the View/LogValue
  emission tracks each field's classification deterministically.
- **AccessLevel is in the public API.** Callers reference
  `es.AccessLevelSubject` etc. in handler code. The values are
  iota-stable; adding new levels must extend the ladder at the top or
  bottom, not between existing levels, or the binary compatibility
  breaks.

## Alternatives Considered

### Keep bytes-default + non_pii opt-out

Rejected. The opacity of `[]byte` leaked into every consumer of every
event. Deciders carried `[]byte` state; projections received `[]byte`
fields; DTOs serialised them as base64. The model worked for the GDPR
case and obstructed every other case. The new model puts string PII
back on `string` at the consumer and pays the base64 storage cost as
the price of ergonomic plaintext.

### Single PII bool plus separate annotations for each behavior

Rejected. `(es.v1.encrypt) = true`, `(es.v1.dsar_export) = false`,
`(es.v1.audit_on_read) = true`, `(es.v1.retention) = "tax_locked"`
would combinatorially explode into annotation soup at the proto
layer. Worse, the annotations could disagree (a field marked
`dsar_export = true` but `encrypt = false` is incoherent for
PERSONAL). The classification-register pattern forces internal
consistency by deriving the four columns from one tag.

### Deferred to a later release

Rejected. Every PII field landing now under the old single-bit model
would need migration when the classification model arrived. The
window for landing the change cheaply was open at the moment the
field count was small; in six months that window closes and the cost
of migration is paid by every adopter. Better to take the breaking
change now while adoption is contained.

### Re-use a community standard like ISO 27701 categories or NIST PII tiers

Rejected as the proto-level enum. ISO 27701 and NIST 800-122 both
classify PII at a register level (organisational policy), not a field
level. The values they propose (e.g., "PII confidentiality impact
level: low/moderate/high") don't map cleanly to per-field encryption
and DSAR behavior. The 10-value enum in this ADR is regime-specific
(GDPR Art 4 / Art 9, PCI CHD / SAD, financial tax retention,
credentials) because those are the regimes the framework's behaviors
actually engage with. The manifest can be cross-walked to ISO 27701
categories at the audit layer if needed.

## References

- ADR 0010 — Crypto-Shredding for PII (the encryption mechanism this
  ADR layers policy on top of).
- Cookbook 11 — Crypto-Shredding and Data Classification (the
  developer/operator walkthrough).
- `proto/es/v1/options.proto` — `DataClassification` enum definition.
- `es/access.go` — `AccessLevel` ladder and `MinLevelFor`.
- `proto-gen/main.go` — codegen emission for View / LogValue / Clone
  / EncryptPII / DecryptPII / manifest.
- GDPR Article 4(1) — definition of "data subject".
- GDPR Article 9 — special categories of personal data.
- GDPR Article 15 — right of access (DSAR).
- GDPR Article 17 — right to erasure ("right to be forgotten").
- PCI-DSS v4.0 — Cardholder Data vs Sensitive Authentication Data
  scope definitions.
