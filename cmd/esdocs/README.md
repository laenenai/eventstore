# esdocs

Regulator-facing data catalog generator for the eventstore framework.

Consumes the `*_pii_manifest.json` files emitted by `protoc-gen-es-go`
(per-package, one per `.proto` file containing aggregates / events) and
produces:

- a combined `catalog.json` summarising every aggregate, command,
  event, and field across the codebase, with per-classification and
  per-encryption-mode tallies;
- a self-contained HTML report — single file, inline CSS, vanilla JS,
  no external assets — suitable for emailing to auditors or hosting
  inside an API-docs site.

## Usage

```sh
# Build the JSON catalog (audit-of-record artifact)
esdocs catalog --gen ./gen --out catalog.json --framework-version v0.8.0

# Render an HTML view from the catalog
esdocs render --in catalog.json --out report.html

# Or do both in one step
esdocs render --gen ./gen --out report.html --framework-version v0.8.0
```

The HTML view ships with:

- A summary header (aggregates / commands / events / total fields /
  PII field count / SAD-rejected count).
- Two histograms: classification distribution and encryption-mode
  distribution.
- A filter bar (by classification, by DSAR-exportable, by PII).
- Collapsible per-message sections — commands and events are folded by
  default; click to expand.
- Per-package anchors for direct linking from other documents.

## What the manifests contain

Each `*_pii_manifest.json` (v2 schema) lists every field's:

- `classification` — one of `DATA_CLASSIFICATION_*` values per
  ADR 0027, or `SUBJECT_FIELD` for the per-subject identifier;
- `encryption` — `none`, `subject_bytes`, `subject_string_base64`, or
  `rejected_sad`;
- `dsar_export` — whether the field appears in DSAR export output;
- `audit_on_read` — whether decrypt of this field should emit an
  audit event;
- `retention` — `standard`, `shorter`, `tax_locked`, or `pci_scope`.

The manifests are checked into the repository under `gen/` and are
diff-reviewed. They are the **audit-of-record artifact** for the
framework's data classification; this tool is a presentation layer
on top.

## Compliance handover

See [`docs/compliance/regulator-handover.md`](../../docs/compliance/regulator-handover.md)
for the broader compliance architecture document.
