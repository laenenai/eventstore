# 11: Crypto-Shredding for PII

GDPR Article 17 requires that, on request, personal data can be made
effectively inaccessible. Hard-deleting events would violate the
framework's append-only invariant. The framework ships
**crypto-shredding** instead: PII fields are encrypted per-subject;
"forgetting" the subject destroys the key, after which the ciphertext
on disk is computationally inaccessible.

This recipe walks through the full operator + developer story:

1. Annotate PII fields in your `.proto`.
2. Review the auto-generated `pii_manifest.json` during privacy review.
3. Wire the runtime: `kms.KeyStore` + `shred.Shredder`.
4. Operate: `ForgetSubject`, `RewrapDEKs` for KEK rotation, redacted reads.

The mechanism is specified in **ADR 0010**. Here we put it to work.

## Annotate PII fields in proto

A real-world example — an HR aggregate. Sensitive fields go through
crypto-shredding; non-PII fields stay readable for ops/analytics
forever, even after the subject is forgotten.

```proto
syntax = "proto3";

package myapp.employee.v1;

import "es/v1/options.proto";

option go_package = "myapp/gen/employee/v1;employeev1";

message Employee {
  option (es.v1.aggregate) = "employee";

  string employee_id   = 1 [(es.v1.subject_field) = true];
  bytes  legal_name    = 2;  // PII by default
  bytes  email         = 3;  // PII by default
  bytes  date_of_birth = 4;  // PII by default
  string department    = 5 [(es.v1.non_pii) = true];  // non-PII, plaintext
  string status        = 6 [(es.v1.non_pii) = true];  // active / on_leave / terminated
}

message Hired {
  string employee_id   = 1 [(es.v1.subject_field) = true];
  bytes  legal_name    = 2;
  bytes  email         = 3;
  bytes  date_of_birth = 4;
  string department    = 5 [(es.v1.non_pii) = true];
}

message Promoted {
  string employee_id = 1 [(es.v1.subject_field) = true];
  string new_role    = 2 [(es.v1.non_pii) = true];
}

message Terminated {
  string employee_id = 1 [(es.v1.subject_field) = true];
  // Reason often contains free-text PII (manager notes etc.).
  bytes reason = 2;
}

message Events {
  option (es.v1.sum_type) = "Event";
  oneof variant {
    Hired      hired      = 1;
    Promoted   promoted   = 2;
    Terminated terminated = 3;
  }
}
```

Conventions enforced by the framework:

- **PII fields must be `bytes`.** The wire format wraps every
  encrypted field as `version(1B) | iv(12B) | ciphertext | tag(16B)`.
  Bytes is the only proto scalar that can carry that on the wire.
- **Subject fields stay plaintext.** "You'd need the key to find the
  key" otherwise. Mark them with `(es.v1.subject_field) = true`.
- **Default is encrypted.** Forget an annotation and the field gets
  encrypted — secure-by-default. The reverse-audit question ("which
  fields are safe?") then becomes the easier one.

## Review `pii_manifest.json`

Codegen writes one manifest per package next to the generated Go:

```
gen/myapp/employee/v1/employee.pb.go
gen/myapp/employee/v1/employee_es.pb.go
gen/myapp/employee/v1/employee_pii_manifest.json
```

The manifest for the example above:

```json
{
  "source": "myapp/employee/v1/employee.proto",
  "package": "myapp.employee.v1",
  "events": [
    {
      "name": "myapp.employee.v1.Hired",
      "fields": [
        {"name": "employee_id", "classification": "subject_field"},
        {"name": "legal_name", "classification": "pii"},
        {"name": "email", "classification": "pii"},
        {"name": "date_of_birth", "classification": "pii"},
        {"name": "department", "classification": "non_pii"}
      ]
    },
    {
      "name": "myapp.employee.v1.Promoted",
      "fields": [
        {"name": "employee_id", "classification": "subject_field"},
        {"name": "new_role", "classification": "non_pii"}
      ]
    },
    {
      "name": "myapp.employee.v1.Terminated",
      "fields": [
        {"name": "employee_id", "classification": "subject_field"},
        {"name": "reason", "classification": "pii"}
      ]
    }
  ]
}
```

**Check it in.** Treat the manifest like any other reviewed artifact:
PRs touching event protos surface the diff in code review, privacy
reviewers see exactly which fields are encrypted vs plaintext. Any
unintended switch from `pii` → `non_pii` (or vice-versa) is one diff
line away from being caught.

## Wire the runtime

Stand up a KMS (the in-process implementation is fine for tests and
single-binary deployments; AWS KMS / GCP KMS adapters slot in
later):

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
            // Logged or surfaced to the caller when Load hits a
            // shredded subject. Fields are zeroed; the read still
            // succeeds.
            for _, r := range redacted {
                log.Warn("PII redacted on read",
                    "subject", r.Subject,
                    "field", r.Name,
                    "reason", r.Reason)
            }
        },
    }
}
```

What happens at write time (Handle):

1. The Decider produces typed events with **plaintext** in PII fields.
2. The runtime clones each event and calls codegen-emitted `EncryptPII`
   — those bytes are now ciphertext.
3. Codec.Encode produces the wire payload; Append persists it. **The
   on-disk bytes never contain plaintext.**

What happens at read time (Load):

1. Codec.Decode unmarshals the on-disk bytes — PII fields hold
   ciphertext.
2. The runtime calls codegen-emitted `DecryptPII`. If every subject is
   live, fields are replaced with plaintext.
3. If any subject has been shredded, those fields are **zeroed** and
   added to `RedactedFields`. The `OnRedacted` hook fires.

## Forgetting a subject

GDPR-style deletion is one call:

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
- Any subsequent `EncryptField` or `DecryptField` for this subject
  fails — `EncryptField` returns an error ("subject has been
  shredded"), `DecryptField` returns `shred.ErrShredded`.
- All historical events for this subject become unreadable for the
  encrypted fields. The events themselves stay on disk (per
  append-only), but their PII fields are now ciphertext nobody can
  decrypt.
- Non-PII fields (department, status, the subject_field itself)
  remain readable forever. Ops, analytics, and audit can still answer
  "how many people were terminated last quarter?" without holding any
  PII.

**Operator runbook check**: after `ForgetSubject`, run the framework's
projection rebuild for any read model that mirrored the subject. The
Tier 1 `state_cache` is overwritten on the next event; old rows for
the subject's stream may still contain the encrypted bytes — these
will surface as redacted on read but won't auto-erase. To proactively
overwrite, call `aggregate.RebuildStateCache` after shredding.

## KEK rotation: `RewrapDEKs`

Per ADR 0010, **DEKs don't rotate** (rotating a DEK means
re-encrypting every historical field under it — only worth it if the
DEK itself is compromised). **KEKs do rotate** routinely (annual key
rollover, key-pinning policies).

Procedure:

```go
// 1. Tell the KMS to rotate. Inproc takes a tenantID; AWS/GCP adapters
//    typically rotate the underlying KMS key out-of-band, then the
//    next CurrentKEKVersion reflects the new version.
if r, ok := keyStore.(kms.KEKRotator); ok {
    newVersion, err := r.RotateKEK(ctx, "tenant-acme")
    if err != nil { return err }
    log.Info("KEK rotated", "tenant", "tenant-acme", "new_version", newVersion)
}

// 2. Re-wrap existing DEKs under the new KEK.
n, err := shredder.RewrapDEKs(ctx, "tenant-acme", 100 /* pageSize */)
if err != nil { return err }
log.Info("re-wrapped DEKs", "rows", n)
```

The rotation job is **idempotent and resumable**:

- Reads `subject_keys` rows with `kek_version < current` in batches.
- Skips shredded rows — their DEKs are intentionally gone.
- For each: unwrap under old KEK version, wrap under current, upsert.
- Crashes are safe; the next run picks up where it left off.

Recommended cadence: run after every KEK rotation, then once a quarter
as a defensive sweep. Schedule alongside the outbox drain (recipe 06).

## Redacted reads in handlers

When a projection or aggregate handler reads a stream containing
shredded events, the framework returns the event with PII fields
zeroed and surfaces the redaction via `OnRedacted`. Handler code
**doesn't** need to special-case missing fields beyond accepting that
they might be empty:

```go
func (p *EmployeeProjection) OnHired(ctx context.Context, env es.Envelope, e *employeev1.Hired) error {
    name := string(e.LegalName)
    if name == "" {
        // Subject shredded or KMS unavailable. Write a redacted
        // placeholder to the read model.
        name = "[redacted]"
    }
    return p.upsert(ctx, e.EmployeeId, name, e.Department)
}
```

Or, if your projection tolerates gaps, just write empty values and let
the UI/API surface redaction status via a sidecar query.

## What stays accessible after a shred

For the Employee example above, after `ForgetSubject(ctx, tenant, "emp-42")`:

| Field | Status on disk | Readable after shred? |
| ----- | -------------- | --------------------- |
| `employee_id` | plaintext (subject) | ✅ yes |
| `legal_name` | ciphertext | ❌ redacted |
| `email` | ciphertext | ❌ redacted |
| `date_of_birth` | ciphertext | ❌ redacted |
| `department` | plaintext (non_pii) | ✅ yes |
| `status` | plaintext (non_pii) | ✅ yes |

Analytics queries like "headcount per department over time" continue
to work for the shredded employee — they reference only the
plaintext fields. Queries like "show me Bob Smith's email history"
return `[redacted]`.

## Gotchas and limitations

**Schema evolution.** Adding a new PII field is safe (encrypts on
first write). Renaming a field is a proto-level breaking change —
new proto, new events; old events still decrypt under the old name
because proto's wire format keys by field number, not name.

**Cross-shred lineage.** If event A in stream X references subject Y,
and Y is shredded, the reference itself (the subject identifier
field) is plaintext and remains. Only the encrypted fields go dark.
This is intentional — operators need to be able to trace "what
stream did this reference?" without holding any PII.

**snapshots.** Tier 1 `state_cache` and Tier-1 snapshots (ADR 0011)
hold derived state. After a shred, the state cache may still hold
the last-loaded plaintext until the next write rebuilds it. Force a
rebuild via `aggregate.RebuildStateCache` if zero-residue is
mandatory for compliance.

**External read stores.** Anything you fanned out via Tier 3
projections to external systems (Elasticsearch, BigQuery, S3) is
**not** auto-shredded. Your compliance runbook must coordinate the
shred call with deletion in each external store. Document the list
of external stores per aggregate in your privacy review.

**KMS unavailability.** When the KMS is down on a read path and the
DEK isn't cached, the framework returns `es.ErrKMSUnavailable` —
hard error, not silent fallback. The decision is: "better to fail
loud than to silently substitute defaults that look like real data."

## Reference

- ADR 0010 — Crypto-Shredding for PII
- [`kms/`](../../kms/) — KMS interface + in-process implementation
- [`shred/`](../../shred/) — Shredder + Encrypt/Decrypt + ForgetSubject + RewrapDEKs
- [`cmd/protoc-gen-es-go/main.go`](../../cmd/protoc-gen-es-go/main.go) — codegen for PII methods + manifest
- [`adapters/storage/postgres/shred.go`](../../adapters/storage/postgres/shred.go) / [`adapters/storage/sqlite/shred.go`](../../adapters/storage/sqlite/shred.go) — adapter SubjectStore implementations
