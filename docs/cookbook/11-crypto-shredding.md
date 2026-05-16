# 11: Crypto-Shredding and Data Classification

GDPR Article 17 requires that, on request, personal data can be made
effectively inaccessible. Hard-deleting events would violate the
framework's append-only invariant. The framework ships
**crypto-shredding** instead: classified fields are encrypted
per-subject; "forgetting" the subject destroys the key, after which
the ciphertext on disk is computationally inaccessible.

ADR 0010 specifies the encryption mechanism. ADR 0027 sits on top and
extends the single PII/non-PII bit into a 10-level
`DataClassification` enum that drives encryption, DSAR export,
audit-on-read, and retention from one annotation. This recipe is the
developer-and-operator walkthrough.

## Problem

A real domain has more than two flavours of sensitive data. An HR
aggregate has names (PERSONAL), dates of birth (QUASI_IDENTIFIER),
manager free-text notes (UNSTRUCTURED, may contain spilled PII),
salary (FINANCIAL, tax-retention-locked), and possibly a session
token cached during onboarding (CREDENTIAL, never exported via DSAR).
A fintech aggregate handles balances (FINANCIAL), PEP/sanctions hits
(SENSITIVE — GDPR Article 9), and PAN/expiry (CARDHOLDER — PCI scope);
the CVV (SAD — PCI Sensitive Authentication Data) must never reach
durable storage.

The single-bit `(es.v1.pii) = true` model treated all of these
identically: encrypted-per-subject, exported on DSAR, no audit, no
retention nuance. That model is gone. Replace it with a single
annotation per field whose value names the regulatory regime, and let
codegen + runtime derive every behavior from it.

## Pattern

### Annotate fields with `(es.v1.data_classification)`

A worked employee aggregate — the example shipping at
`examples/employee/`:

```proto
syntax = "proto3";

package myapp.employee.v1;

import "es/v1/options.proto";

option go_package = "myapp/gen/employee/v1;employeev1";

enum Status {
  STATUS_UNSPECIFIED = 0;
  STATUS_ACTIVE      = 1;
  STATUS_ON_LEAVE    = 2;
  STATUS_TERMINATED  = 3;
}

message Employee {
  option (es.v1.aggregate) = "employee";

  string employee_id   = 1 [(es.v1.subject_field) = true];
  string legal_name    = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string email         = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string date_of_birth = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_QUASI_IDENTIFIER];
  string department    = 5 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  string current_role  = 6 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  Status status        = 7 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
}

message Hired {
  string employee_id   = 1 [(es.v1.subject_field) = true];
  string legal_name    = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string email         = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string date_of_birth = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_QUASI_IDENTIFIER];
  string department    = 5 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  string initial_role  = 6 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
}

message Terminated {
  string employee_id = 1 [(es.v1.subject_field) = true];
  // Manager observations: free-form, accidental PII is the default
  // assumption.
  string reason = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
}
```

Two rules to internalise:

- **Unannotated fields default to PUBLIC**, meaning no encryption.
  Encryption is opt-in from PERSONAL onwards. This inverts the
  previous bytes-default convention — every field that needs
  encryption now declares it explicitly. The reverse audit question
  ("what's encrypted?") is the manifest, not a scan of fields without
  an opt-out.
- **Subject fields stay plaintext** regardless of classification.
  Marked with `(es.v1.subject_field) = true`. They are key handles,
  not identifying data on their own — encrypting them would require
  the key to find the key.

### What each classification means

Eleven enum values (UNSPECIFIED + ten classifications). The
authoritative table lives in `proto/es/v1/options.proto`; the short
version:

| Classification | Encrypted? | In DSAR? | Audit-on-read | Retention |
| -------------- | ---------- | -------- | ------------- | --------- |
| PUBLIC         | no         | yes      | no            | standard  |
| INTERNAL       | no         | no       | no            | standard  |
| PERSONAL       | yes        | yes      | no            | standard  |
| QUASI_IDENTIFIER | yes      | yes      | no            | standard  |
| SENSITIVE (Art 9) | yes    | yes (\*) | yes           | shorter   |
| FINANCIAL      | yes        | yes (\*) | optional      | tax-locked |
| CARDHOLDER (PCI) | yes      | yes      | yes           | PCI-scope |
| SAD (PCI)      | **REJECTED** | n/a    | n/a           | n/a       |
| CREDENTIAL     | yes        | **never** | yes          | standard  |
| UNSTRUCTURED   | yes        | yes      | no            | standard  |

(\*) SENSITIVE DSAR requires an explicit per-Art-9 consent check;
FINANCIAL is locked behind tax retention windows before delete.

The codegen reads these classifications and emits per-message helpers
(below); the per-aggregate manifest at
`gen/<pkg>/<v>/<name>_pii_manifest.json` records the derived behavior
columns so DSAR-export tooling, PCI-scope scanners, and retention
jobs can consume one source of truth.

### Wire format: string vs bytes

Both encryption envelopes resolve to the same `version(1B) | iv(12B)
| ciphertext | tag(16B)` shape (ADR 0010). They differ on the
field-type encoding:

- **`string` PII** is round-tripped through `base64.RawStdEncoding` so
  the field remains UTF-8-valid on the wire. ~33% storage overhead.
  Pick this for ordinary text PII (names, emails, free-form notes).
- **`bytes` PII** holds raw ciphertext. No base64 overhead. Pick this
  for genuinely binary PII — biometric templates, MRZ scans, signed
  document blobs.

Choose the proto type that matches the natural shape of the data;
the codegen does the right thing for either.

### The generated helpers

For every message, `protoc-gen-es-go` emits three methods alongside
the standard `*.pb.go`:

```go
// Clone — deep copy. Handles oneofs (type-switch over the wrapper),
// maps, repeated fields, and nested messages. Nil-safe.
func (m *Hired) Clone() *Hired

// View — deep copy filtered by the caller's access level. Fields
// above `level` are zero-valued; subject fields are always visible.
// Nested messages recurse at the same level.
func (m *Hired) View(level es.AccessLevel) *Hired

// LogValue — implements slog.LogValuer at AccessLevelInternal.
// PII fields render as "[REDACTED:<CLASS>]" markers so
// slog.Info("...", "event", e) is safe by default.
func (m *Hired) LogValue() slog.Value
```

For messages that have at least one encrypted field, codegen also
emits the existing `PIIFields`, `Subject`, `EncryptPII`, `DecryptPII`
methods that the framework's Shredder calls during Append/Load.

#### Using `View` in a Decider or projection

DSAR export — the data subject sees their own PERSONAL,
QUASI_IDENTIFIER, and UNSTRUCTURED fields, plus INTERNAL/PUBLIC:

```go
func (h *DSARHandler) Export(ctx context.Context, emp *employeev1.Employee) *employeev1.Employee {
    return emp.View(es.AccessLevelSubject)
}
```

Back-office dashboard — INTERNAL only, never PERSONAL:

```go
func (p *EmployeeProjection) ToOpsView(emp *employeev1.Employee) *employeev1.Employee {
    return emp.View(es.AccessLevelInternal)
}
```

Compliance officer pulling a fraud investigation — adds SENSITIVE,
FINANCIAL, CARDHOLDER on top of Subject:

```go
view := emp.View(es.AccessLevelCompliance)
```

The level ladder is in `es/access.go`: Public → Internal → Subject →
Compliance → Operator. Subject was named after GDPR Article 4(1)'s
"data subject" — the same natural person who is the encryption
subject. Operator is god-mode; treat it as such.

#### Using `LogValue` everywhere

Once a message implements `slog.LogValuer`, any `slog.Info("...",
"event", e)` automatically redacts. No call-site discipline needed:

```go
func (s *EmployeeService) Hire(ctx context.Context, cmd *employeev1.Hire) error {
    slog.InfoContext(ctx, "handling hire",
        "tenant", es.TenantFromContext(ctx),
        "cmd", cmd) // legal_name renders as [REDACTED:PERSONAL]
    // ...
}
```

The structured-logging path is now safe by default. Misconfigured
JSON handlers, third-party log shippers, and ad-hoc debug prints all
see the redacted markers instead of cleartext. This is the
defence-in-depth pairing for the at-rest encryption: classification
stops a misconfigured logger from leaking; encryption stops a dropped
database from leaking. Different attackers, different defences.

#### Clone in the Decider's Evolve

`Clone` replaces hand-rolled state copies. Worked example:

```go
Evolve: func(s *employeev1.Employee, e employeev1.Event) *employeev1.Employee {
    out := s.Clone() // deep copy, oneof-safe
    switch evt := e.(type) {
    case *employeev1.Hired:
        out.EmployeeId  = evt.EmployeeId
        out.LegalName   = evt.LegalName
        out.Email       = evt.Email
        out.DateOfBirth = evt.DateOfBirth
        out.Department  = evt.Department
        out.CurrentRole = evt.InitialRole
        out.Status      = employeev1.Status_STATUS_ACTIVE
    // ...
    }
    return out
},
```

`Clone()` returns the concrete pointer type — no `proto.Clone` cast,
no missing-field bugs after a new field lands in the proto.

### Wire the runtime

Stand up a KMS, a Shredder, and pass it to the aggregate Runtime:

```go
import (
    "github.com/laenenai/eventstore/aggregate"
    "github.com/laenenai/eventstore/kms/inproc"
    "github.com/laenenai/eventstore/shred"
    employeev1 "myapp/gen/employee/v1"
)

func newEmployeeRuntime(store es.Store) *aggregate.Runtime[*employeev1.Employee, employeev1.Command, employeev1.Event] {
    keyStore := inproc.New()
    shredder := shred.New(keyStore, store.(shred.SubjectStore))

    return &aggregate.Runtime[*employeev1.Employee, employeev1.Command, employeev1.Event]{
        Store:    store,
        Decider:  employee.Decider,
        Codec:    employeev1.EventCodec{},
        Shredder: shredder,
        OnRedacted: func(redacted shred.RedactedFields) {
            // Fires when Load hits a shredded subject. Fields are
            // zeroed; the read still succeeds. Surface to the caller
            // (audit log, UI banner, sidecar metric).
            for _, r := range redacted {
                slog.Warn("PII redacted on read",
                    "subject", r.Subject,
                    "field", r.Name,
                    "reason", r.Reason)
            }
        },
    }
}
```

What happens at write time (`Handle`):

1. The Decider produces typed events with **plaintext** in classified
   fields.
2. The runtime clones each event and calls codegen-emitted
   `EncryptPII` — fields whose classification engages encryption are
   now ciphertext (string → base64'd, bytes → raw).
3. `Codec.Encode` produces the wire payload; `Append` persists it.
   **The on-disk bytes never contain plaintext for encrypted fields.**

What happens at read time (`Load`):

1. `Codec.Decode` unmarshals the on-disk bytes — encrypted fields
   hold ciphertext.
2. The runtime calls codegen-emitted `DecryptPII`. If every subject
   is live, fields are replaced with plaintext.
3. If any subject has been shredded, those fields are **zeroed** (or
   emptied, for strings) and added to `RedactedFields`. The
   `OnRedacted` hook fires.

### Forgetting a subject — `ForgetSubject`

GDPR-style deletion is one call. Semantics are unchanged from ADR
0010:

```go
if err := shredder.ForgetSubject(ctx, "tenant-acme", "emp-42"); err != nil {
    return err
}
```

Effect:

- `subject_keys` row gets `dek_wrapped = ''` and `shredded_at = now()`.
  The DEK is gone; the row stays for compliance audit ("we deleted
  this on 2026-03-14").
- The in-process DEK cache evicts the entry.
- Subsequent `EncryptField` for this subject fails;
  `DecryptField` returns `shred.ErrShredded`.
- Historical events for this subject become unreadable for the
  encrypted fields. Events themselves stay on disk; their classified
  fields surface as `RedactedField` markers on Load.
- Fields classified PUBLIC or INTERNAL (department, status, the
  subject_field itself) remain readable forever.

Then run the state-cache rebuild for the affected stream so any
plaintext that the cache retained from a pre-shred Load is
overwritten:

```go
if err := aggregate.RebuildStateCache(ctx, runtime, streamID); err != nil {
    return err
}
```

### DSAR export and the manifest

GDPR Article 15 requires a per-subject export of their personal data.
The manifest's `dsar_export` column is the authoritative allowlist:

```json
{
  "name": "myapp.employee.v1.Hired",
  "fields": [
    {"name": "employee_id",   "classification": "DATA_CLASSIFICATION_SUBJECT_FIELD",   "encryption": "none",                  "dsar_export": true,  "audit_on_read": false, "retention": "standard"},
    {"name": "legal_name",    "classification": "DATA_CLASSIFICATION_PERSONAL",        "encryption": "subject_string_base64", "dsar_export": true,  "audit_on_read": false, "retention": "standard"},
    {"name": "email",         "classification": "DATA_CLASSIFICATION_PERSONAL",        "encryption": "subject_string_base64", "dsar_export": true,  "audit_on_read": false, "retention": "standard"},
    {"name": "date_of_birth", "classification": "DATA_CLASSIFICATION_QUASI_IDENTIFIER","encryption": "subject_string_base64", "dsar_export": true,  "audit_on_read": false, "retention": "standard"},
    {"name": "department",    "classification": "DATA_CLASSIFICATION_INTERNAL",        "encryption": "none",                  "dsar_export": false, "audit_on_read": false, "retention": "standard"},
    {"name": "initial_role",  "classification": "DATA_CLASSIFICATION_INTERNAL",        "encryption": "none",                  "dsar_export": false, "audit_on_read": false, "retention": "standard"}
  ]
}
```

DSAR-export tooling reads each manifest, joins on the in-flight
event/state types, and emits the fields where `dsar_export: true`.
CREDENTIAL fields are always `dsar_export: false` (auth tokens are
never the subject's own data they can take with them). INTERNAL is
also `false` (system-owned, not subject-owned). Everything else
PERSONAL-and-up defaults to exportable; the exporter applies the
domain-specific Art 9 consent / tax retention checks as documented in
ADR 0027.

**Check the manifest into the repo.** PRs touching event protos
surface the diff in code review; privacy reviewers see exactly which
fields change classification, switch encryption modes, or flip in/out
of DSAR scope. Any unintended downgrade (e.g., PERSONAL → INTERNAL)
is one diff line away from being caught.

## KEK rotation: `RewrapDEKs`

Per ADR 0010, **DEKs don't rotate** — rotating a DEK means
re-encrypting every historical field under it, only worth it if the
DEK itself is compromised. **KEKs do rotate** routinely.

```go
if r, ok := keyStore.(kms.KEKRotator); ok {
    newVersion, err := r.RotateKEK(ctx, "tenant-acme")
    if err != nil { return err }
    slog.Info("KEK rotated", "tenant", "tenant-acme", "new_version", newVersion)
}

n, err := shredder.RewrapDEKs(ctx, "tenant-acme", 100 /* pageSize */)
if err != nil { return err }
slog.Info("re-wrapped DEKs", "rows", n)
```

The job is idempotent and resumable; crashes are safe and the next
run picks up where it left off. Run after every KEK rotation, then
once a quarter as a defensive sweep. Schedule alongside the outbox
drain (recipe 06).

## PII shape migration (Tier D, ADR 0030)

When a PII field changes its on-disk encoding — most commonly the
`bytes` → `string` shift introduced by ADR 0027's
`DataClassification` enum, where strings now hold **base64 of the
ciphertext** instead of raw bytes — the framework's existing data
needs a migration. Per [ADR 0030](../adr/0030-schema-migration-discipline.md)
this is **Tier D**: classification or encryption shape changed.

The on-disk DEK doesn't change. The plaintext doesn't change. Only
the **wire encoding** changes — raw ciphertext bytes (proto `bytes`)
become base64-of-the-same-ciphertext (proto `string`). The
migration is a **read-time upcaster** registered against the OLD
schema_version.

### Shape before and after

**Old proto** (the shape the framework shipped with first):

```proto
message Hired {
  string employee_id = 1 [(es.v1.subject_field) = true];
  bytes  legal_name  = 2;  // implicitly encrypted via the runtime's Shredder
  bytes  email       = 3;  // raw ciphertext on disk
}
```

**New proto** (current):

```proto
message Hired {
  option (es.v1.schema_version) = 2;  // bump

  string employee_id = 1 [(es.v1.subject_field) = true];
  string legal_name  = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string email       = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
}
```

The field tags (1, 2, 3) stay the same. Only the wire type changes
(`bytes` → `string`) and the schema_version bumps.

### The upcaster

The framework's upcaster mechanism (ADR 0013) is the `Codec.Decode`
method. The codegen-emitted codec only knows the current shape; for
the migration you write a **wrapper codec** that dispatches on
`schemaVersion`:

```go
package employee

import (
    "encoding/base64"
    "fmt"

    "google.golang.org/protobuf/proto"

    "github.com/laenenai/eventstore/aggregate"
    employeev1 "github.com/laenenai/eventstore/gen/myapp/employee/v1"
    legacyv1  "github.com/laenenai/eventstore/gen/myapp/employee/v1/legacy"
)

// MigratingCodec wraps the codegen-emitted codec and upcasts
// schema_version=1 events to schema_version=2 on read. Append still
// uses the new shape; only Decode applies the migration.
type MigratingCodec struct {
    Inner employeev1.EventCodec  // codegen-emitted; handles v2 natively
}

func (c MigratingCodec) Encode(e employeev1.Event) (aggregate.EncodedEvent, error) {
    return c.Inner.Encode(e)  // always emit the current shape
}

func (c MigratingCodec) Decode(typeURL string, schemaVersion uint32, payload []byte) (employeev1.Event, error) {
    if schemaVersion >= 2 {
        return c.Inner.Decode(typeURL, schemaVersion, payload)
    }
    // Upcast schema_version=1 → 2. Only the Hired variant changed
    // shape (legal_name + email moved from bytes to string).
    switch typeURL {
    case "myapp.employee.v1.Hired":
        var old legacyv1.HiredV1
        if err := proto.Unmarshal(payload, &old); err != nil {
            return nil, fmt.Errorf("upcast Hired v1: %w", err)
        }
        return &employeev1.Hired{
            EmployeeId: old.EmployeeId,
            // Raw ciphertext bytes → base64-encoded string. Same
            // ciphertext, same DEK, same plaintext after DecryptPII.
            LegalName: base64.RawStdEncoding.EncodeToString(old.LegalName),
            Email:     base64.RawStdEncoding.EncodeToString(old.Email),
        }, nil
    }
    return c.Inner.Decode(typeURL, schemaVersion, payload)
}
```

The `legacyv1.HiredV1` is a **frozen copy** of the old proto in a
separate package — preserved specifically for the upcaster. Don't
keep it in the current namespace; freeze it as a hand-written
schema-historical artifact so the codegen plugin doesn't regenerate
it under the current rules.

```proto
// proto/myapp/employee/v1/legacy/legacy.proto
// FROZEN — pre-ADR-0027 shape of Hired, kept for the upcaster only.
// Do not modify; do not regenerate against current options.
syntax = "proto3";
package myapp.employee.v1.legacy;

message HiredV1 {
  string employee_id = 1;
  bytes  legal_name  = 2;
  bytes  email       = 3;
}
```

### Wire it on the Runtime

```go
rt := aggregate.NewProto(
    store,
    employee.Decider,
    employee.MigratingCodec{Inner: employeev1.EventCodec{}},
)
```

That's the whole migration. Every existing schema_version=1 event
decodes through the upcaster on Load. New writes use the current
shape directly. No data movement on disk; no operator runbook beyond
deploying the new code.

### Verification before deploy

The migration is read-time, but the upcaster needs at least one test
that proves the round-trip:

1. Encrypt a plaintext under the OLD shape using `Shredder.EncryptField`
   directly (bytes ciphertext) and synthesize a v1 `HiredV1` payload.
2. Run that payload through `MigratingCodec.Decode(typeURL, 1, payload)`.
3. Apply `DecryptPII` to the resulting `*Hired` (uses the same
   per-subject DEK).
4. Assert the decrypted plaintext matches the original.

Test under `gen/test/piimigration/v1/` is the natural home for this
kind of fixture; the legacy proto goes there too so it's never
confused with current-shape code.

### When NOT to do the upcaster

If you're greenfield — zero deployed data under the old shape — the
upcaster is dead code. Skip it. The new proto is the only proto.

If you're mid-deploy with some data under v1 and some under v2:
register the upcaster, ship the new code, and over time
schema_version=1 events age out (or you compact them via a separate
rewriter job — Tier F territory, separate ADR).

## Failure modes

**SAD fields at write time.** A field classified
`DATA_CLASSIFICATION_SAD` must never reach durable storage. The
framework rejects the encrypt attempt with a clear error from
`EncryptPII`; commands that produce SAD-bearing events fail fast.
SAD belongs on ephemeral channels only — authorize and discard.

**Schema evolution.** Adding a new classified field is safe (encrypts
on first write). Renaming a field is a proto-level breaking change.
Changing a classification (PERSONAL → SENSITIVE, INTERNAL →
PERSONAL) is a behavior change: events already written under the old
classification stay where they are, but the manifest now describes
the new regime. Pair the change with a `state_schema_version` bump
(ADR 0013) so projections and the state cache rebuild.

**Cross-shred lineage.** If event A in stream X references subject Y,
and Y is shredded, the reference itself (the subject identifier
field) is plaintext and remains. Only the encrypted fields go dark.
Intentional: operators need to trace "what stream did this
reference?" without holding any PII.

**State cache after shred.** Tier 1 `state_cache` (ADR 0023) holds
derived state. After `ForgetSubject`, the cache may still hold
the last-loaded plaintext until the next write rebuilds it. Force a
rebuild via `aggregate.RebuildStateCache` if zero-residue is
mandatory for compliance.

**External read stores.** Anything fanned out via projections to
external systems (Elasticsearch, BigQuery, S3) is **not**
auto-shredded. Your compliance runbook coordinates the shred call
with deletion in each external store. Document the list of external
stores per aggregate in the privacy review.

**KMS unavailability.** When the KMS is down on a read path and the
DEK isn't cached, the framework returns `es.ErrKMSUnavailable` —
hard error, not silent fallback.

## What NOT to do

- **Don't downgrade a classification to silence a test.** PERSONAL →
  INTERNAL bypasses encryption AND removes the field from DSAR
  exports. Both behaviours change at once. If the test wants
  plaintext for assertions, decrypt explicitly via `View(level)` at
  the test boundary.
- **Don't try to migrate from `(es.v1.pii)` / `(es.v1.non_pii)` /
  `(es.v1.pii_intentional)`** — those annotations are gone. There is
  no migration shim. Adopters update their protos to use
  `(es.v1.data_classification)`. Fields previously declared `bytes`
  with no annotation will become PUBLIC by default; this is the one
  case where the new model is more lax than the old, and it's
  intentional — unannotated bytes were a sharp edge of the old
  default-to-encrypted rule.
- **Don't store SAD even briefly in a stream "for debugging".**
  Codegen rejects it; bypassing the reject (writing a SAD field
  through some other code path) violates PCI scope. Use a separate
  ephemeral channel and discard after authorization.
- **Don't bypass `LogValue`.** If you stringify an event yourself
  (`fmt.Sprintf("%+v", e)` or `proto.Marshal` + base64), you lose the
  redaction. Pass the proto pointer to `slog` and let `LogValue`
  render.

## See also

- ADR 0010 — Crypto-Shredding for PII (the encryption mechanism).
- ADR 0027 — Data Governance Model (the classification policy).
- `es/access.go` — `AccessLevel` ladder.
- `proto/es/v1/options.proto` — `DataClassification` enum.
- `kms/` — KMS interface + in-process implementation.
- `shred/` — Shredder + Encrypt/Decrypt + ForgetSubject + RewrapDEKs.
- `proto-gen/main.go` — codegen for View / LogValue / Clone / PII
  methods + manifest.
- `examples/employee/` — the full worked aggregate.
- `adapters/storage/postgres/shred.go` /
  `adapters/storage/sqlite/shred.go` — adapter SubjectStore
  implementations.
