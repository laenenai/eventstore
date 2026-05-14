# Example: Employee HR (Crypto-Shredding)

A worked example of the framework's **crypto-shredding** (ADR 0010,
cookbook 11) against a realistic HR domain. PII fields
(`legal_name`, `email`, `date_of_birth`, termination `reason`) are
encrypted under per-employee DEKs; non-PII fields (`department`,
`current_role`, `status`) stay plaintext forever.

After a GDPR-style `ForgetSubject` call, all encrypted fields become
inaccessible — but the audit-relevant fields still answer questions
like "how many people were in engineering last year?"

## What it demonstrates

| Feature | How |
| ------- | --- |
| PII annotations in `.proto` | `legal_name`, `email`, `date_of_birth`, `reason` carry `(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL` (or `QUASI_IDENTIFIER` for `date_of_birth`); `department`, `current_role`, `status` use `DATA_CLASSIFICATION_INTERNAL`. See ADR 0027. |
| `pii_manifest.json` audit artifact | Auto-emitted at `gen/myapp/employee/v1/employee_pii_manifest.json`. Diff-reviewed during privacy review. |
| Codegen-emitted `EncryptPII`/`DecryptPII` | The framework's `protoc-gen-es-go` plugin recognized the PII fields and produced the methods. `aggregate.Runtime.Shredder` calls them transparently. |
| `ForgetSubject` | One line of code; zeroes the DEK; all subsequent reads return redacted fields. |
| `OnRedacted` hook | Surfaces redactions during Load so the application can log / display "[redacted]" / etc. |
| `IsTerminal` after Terminate | Demonstrates that ADR 0003's terminal-stream rule composes cleanly with PII fields. |

## Run the tests

```bash
cd examples/employee
go test ./...
```

Three tests:

1. **HireAndReadRoundTrip** — encrypt on write, decrypt on read.
   Asserts that the raw payload bytes on disk DO NOT contain
   plaintext PII.
2. **ForgetSubjectRedactsOnLoad** — GDPR deletion. Asserts redacted
   fields are zeroed, non-PII fields remain readable, and the
   `OnRedacted` hook reports the redactions.
3. **TerminatedIsTerminal** — `Terminate` closes the stream; subsequent
   commands fail with `es.ErrTerminal`.

## A glance at the PII manifest

```json
{
  "name": "myapp.employee.v1.Hired",
  "fields": [
    {"name": "employee_id",  "classification": "subject_field"},
    {"name": "legal_name",   "classification": "pii"},
    {"name": "email",        "classification": "pii"},
    {"name": "date_of_birth","classification": "pii"},
    {"name": "department",   "classification": "non_pii"},
    {"name": "initial_role", "classification": "non_pii"}
  ]
}
```

This file is **checked into the repository** alongside the generated
Go code. Anyone reviewing a PR that changes the Employee proto can
see exactly which fields change classification — the audit trail
that privacy review needs.

## What survives a shred

After `shredder.ForgetSubject(ctx, tenant, "emp-99")`:

| Field | After shred |
| ----- | ----------- |
| `employee_id` | ✅ readable (subject_field, plaintext) |
| `legal_name`  | ❌ redacted |
| `email`       | ❌ redacted |
| `date_of_birth` | ❌ redacted |
| `department`  | ✅ readable (non_pii) |
| `current_role`| ✅ readable (non_pii) |
| `status`      | ✅ readable (non_pii) |

So "headcount per department over time" continues to work for the
shredded employee — it references only the plaintext fields. "Show
me Bob's email history" returns `[redacted]`.

## See also

- ADR 0010 — Crypto-Shredding for PII
- Cookbook recipe 11 — Crypto-shredding operator workflow
- [`kms/`](../../kms/) — KMS interface + in-process implementation
- [`shred/`](../../shred/) — Shredder runtime
- `gen/myapp/employee/v1/employee_pii_manifest.json` — this aggregate's manifest
