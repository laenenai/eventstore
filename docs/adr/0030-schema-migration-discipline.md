# ADR 0030: Schema Migration Discipline

- **Status:** Accepted
- **Date:** 2026-05-16
- **Pairs with:** ADR 0005 (envelope schema), ADR 0010 (crypto-shredding),
  ADR 0013 (schema evolution & upcasters), ADR 0023 (state_cache schema
  version), ADR 0027 (data governance), ADR 0028 (tamper-evident chain).

## Context

The framework evolved its PII model during early development: `bytes`
fields with an implicit "encrypted" semantic + `(es.v1.non_pii)`
opt-out → `string` / `bytes` fields with explicit
`(es.v1.data_classification)`. Greenfield adopters (regenerate
codegen, ship) saw no impact. Any adopter mid-deploy would have hit
a silent on-disk format change with no documented migration path —
exactly the kind of "small now, painful later" failure mode that
compounds over a framework's lifetime.

This ADR codifies a **discipline for every schema-touching change**:

1. A taxonomy of change types, each with a defined migration story.
2. A PR-template requirement so authors declare which tier a change
   is in and confirm the migration steps before merge.
3. An explicit "this is unsafe without a deliberate plan" line for
   the changes where the framework's existing mechanisms (upcasters,
   `state_schema_version`, classification) are insufficient.

The aim is **not** to slow framework work down. It is to make sure
that every change is **labelled** so adopters reading the changelog
(or maintainers running a migration audit) can answer "what do I
have to do?" without re-reading the PR diff.

## Decision

### Six migration tiers

Every schema-touching change falls into exactly one tier. The tier
determines what migration story is required.

**Tier A — Pure additive.**
A change adds a new field, message, aggregate, projection, command,
event, or option **without altering existing on-disk encodings**.
Proto3's default-value semantics make the old data forward-compatible.

- Examples: new aggregate; new optional field on a message; new
  recipe; new ADR; new cookbook item.
- Migration story: **none**. Ship.
- PR template line: `Tier A — pure additive`.

**Tier B — Semantic shift behind same wire encoding.**
The on-disk wire encoding stays the same, but the *meaning* changes:
units shift (cents → millicents), enum semantics rotate, a numeric
field's interpretation moves from "absolute" to "delta," and so on.

- Examples: change `amount_cents` semantics to `amount_microcents`
  without renaming the field; flip an enum value's interpretation.
- Migration story: **bump `(es.v1.schema_version)` on the affected
  event** and register an **upcaster** (ADR 0013) that converts
  old-schema events to the new shape on read. Existing on-disk
  events stay byte-identical.
- PR template line: `Tier B — schema_version bump + upcaster
  for old-version events`.

**Tier C — State shape changed.**
The aggregate's State proto changed in a way that means existing
`state_cache` rows would decode incorrectly under the new shape.
Note: this is *only* about the State message — Event protos use Tier
B's upcaster path.

- Examples: rename a field on State; change a field's type on State
  (string → enum); flip the "fold" of the same event into State.
- Migration story: **bump `aggregate.Runtime.StateSchemaVersion`**.
  Stale `state_cache` rows are silently discarded with full-replay
  fallback (ADR 0023 § state_schema_version invalidation). Operators
  optionally run `aggregate.RebuildStateCache` to repopulate
  proactively.
- PR template line: `Tier C — StateSchemaVersion bump; rebuild
  state_cache after deploy`.

**Tier D — Classification or encryption shape changed.**
A PII field's classification changed in a way that affects on-disk
encoding: `bytes` → `string` (raw ciphertext → base64), classification
PERSONAL → CARDHOLDER (audit-on-read becomes mandatory),
non-classified → PERSONAL (newly encrypted), PERSONAL → INTERNAL
(decrypted into plaintext).

- Examples: the bytes→string+data_classification shift that motivated
  this ADR; promoting a field from INTERNAL to PERSONAL; demoting
  a CREDENTIAL to INTERNAL after a security-team review.
- Migration story: **schema_version bump on the event + a one-time
  upcaster** that decrypts under the old shape and re-encodes under
  the new shape on read. The DEK doesn't change (still per-subject,
  same KEK); only the wire encoding shape changes.
- See the worked example in cookbook recipe 11 § PII shape migration
  (planned follow-up PR).
- PR template line: `Tier D — classification migration upcaster
  registered; document KEK / DEK invariants`.

**Tier E — Envelope hash subset changed.**
ADR 0028 fixed the envelope subset that contributes to the
tamper-evident chain at v1. Any change to that subset would
invalidate existing hashes.

- Examples: add a field to the hash subset; change the canonical
  serialization rule; remove a field from the hash subset.
- Migration story: **new ADR**. The framework has no `hash_version`
  column today; introducing one is its own design exercise.
  Tier E changes are rare by construction (the hash subset is
  pinned for a reason).
- PR template line: `Tier E — envelope hash subset change; requires
  new ADR + hash_version migration`.

**Tier F — Wire-incompatible breaking change.**
Anything that doesn't fit A–E: a column type change, a removed
event variant, a renamed message that breaks proto's wire
compatibility, a removed proto annotation that the codegen relied
on.

- Examples: delete an event variant from a Commands sum type; rename
  a top-level message; drop a column from the events table.
- Migration story: **explicit data-migration plan**. Document the
  before / after on-disk shape; ship a one-off migration script (or
  goose migration with sufficient comments) that performs the data
  transformation; include a test that runs the migration against a
  fixture.
- PR template line: `Tier F — breaking change; migration script
  included; tested against fixture`.

### The PR template requirement

A new section appears in `.github/PULL_REQUEST_TEMPLATE.md`:

```markdown
## Schema migration tier

- [ ] Tier A — pure additive (no migration)
- [ ] Tier B — schema_version bump + upcaster
- [ ] Tier C — StateSchemaVersion bump
- [ ] Tier D — classification migration upcaster
- [ ] Tier E — envelope hash subset change (NEW ADR REQUIRED)
- [ ] Tier F — breaking change (explicit migration script)
- [ ] Not applicable (no schema touched)

If Tier B, C, D, or F: link to the upcaster / migration / script.
If Tier E: link to the new ADR.
```

The checkbox is **mandatory** — reviewers refuse the PR if it's
ticked "Not applicable" but the diff touches any of:

- `proto/**/*.proto`
- `adapters/storage/*/migrations/*.sql`
- `aggregate/runtime.go` (state semantics)
- `es/envelope.go` (envelope shape)
- `proto-gen/emit_*.go` (codegen behaviour)

CI does not enforce the checkbox itself (too brittle); reviewer
discipline catches it.

### Greenfield-vs-deployed distinction

Tiers B / C / D / F have a real cost only for adopters who have
**deployed data on disk** under the old shape. For a pre-v1
framework with zero production adopters, the "migration story" is
documentation-grade — the upcaster / script must be **designed and
written**, but no live migration runs.

Once an adopter is in production, the same code provides the actual
migration path. The framework's discipline is to **write the
migration as if data exists**, even when it doesn't, so the path is
proven by the time it's needed.

### What changed retroactively

The framework's PII model shift (`bytes` + `non_pii` → `string` /
`bytes` + `(es.v1.data_classification)`) was a **Tier D** change
that landed without an upcaster. Greenfield by definition (the
framework has no production adopters), so no live data needed
migrating — but the discipline was wrong. The follow-up cookbook
recipe captures the upcaster that *would have been* required, so
the path is documented and tested.

## Consequences

**Positive:**

- Adopters can read the changelog and immediately know what work
  each release requires.
- Maintainers have a checklist that catches schema-touching changes
  before they're merged without a migration story.
- Tier D (classification shift) is now a named concept — previously
  it sat in the gap between Tier B (events change) and Tier C (state
  change), neither of which fully covered it.
- The framework establishes a vocabulary for talking about migrations
  that adopters can reuse in their own ADRs.

**Negative / costs:**

- PR overhead: every schema-touching PR has to tick a box and
  potentially write an upcaster.
- The PR template depends on reviewer discipline — CI doesn't enforce
  it. A determined author can skip the box.
- Tier E is intentionally heavy (requires a new ADR) — discourages
  envelope shape changes, which is the point, but might frustrate a
  legitimate need for an envelope evolution.

**Mitigations:**

- The PR template is the **forcing function**, not CI. Reviewers
  catch what CI can't.
- Tier D failure mode (silent decode under new shape after old-shape
  data exists) is rare for greenfield, common at scale. Writing the
  upcaster while greenfield is cheap insurance.

## Alternatives Considered

### Single tier, "always write an upcaster"

Considered. Rejected — too coarse. A purely additive change (new
aggregate, new field with proto3 default) doesn't need an upcaster
and forcing one inflates every PR without value.

### Three tiers: "no migration / upcaster / breaking"

Considered. Rejected — collapses Tier C (state schema) and Tier D
(classification shift) into the upcaster bucket, losing the
operator-facing distinction. Tier C needs
`RebuildStateCache`; Tier D needs decrypt-then-re-encrypt logic.
Different operational stories, same checkbox value.

### Codify in CLAUDE.md only, no ADR

Considered. Rejected — CLAUDE.md is for *contributor-facing
codebase conventions*. Migration discipline is a public-facing
contract with adopters that needs the longevity of an ADR.

### Enforce the PR template via CI

Considered (e.g., grep the diff for `proto/`, fail if the schema-
migration section is "Not applicable"). Rejected for v1 — too many
false positives (a comment-only change to a proto file, a Taskfile
edit that adds a generated artifact). Reviewer discipline is the
right tool.

## Reference

- ADR 0005 — Event envelope schema (the v1 hash subset Tier E protects)
- ADR 0010 — Crypto-shredding (Tier D's underlying mechanism)
- ADR 0013 — Schema evolution & upcasters (Tier B's mechanism)
- ADR 0023 — state_cache supersedes snapshots (Tier C's mechanism)
- ADR 0027 — Data governance model (Tier D's classification rules)
- ADR 0028 — Tamper-evident chain (Tier E's hash subset)
- Cookbook recipe 11 — Crypto-shredding, planned § PII shape migration

## Open Questions

- **Should the PR template's tier checkbox have a "I don't know,
  ask reviewer" option?** A maintainer who isn't sure which tier a
  change is in could fail safe — but it might encourage
  under-categorization. Initial answer: no; if you don't know,
  read the ADR and pick the closest fit. Revisit if reviewers see
  pattern of mis-categorization.

- **Should Tier D distinguish "encryption added" from "encryption
  removed"?** Removing encryption (PERSONAL → INTERNAL) is operationally
  the harder direction (decrypted plaintext written to disk; needs to
  be irreversible). Maybe split into D1 / D2 if usage shows the
  asymmetry matters.
