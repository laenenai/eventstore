# ADR 0006: Payload Format

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

The event payload — the concrete domain data per event — can be stored as:

1. Proto bytes (`BYTEA` in Postgres, `BLOB` in SQLite).
2. JSONB (Postgres) / JSON1 text (SQLite).
3. Both: proto bytes as the canonical form, JSONB as an ops sidecar.

JSONB is tempting because it makes payloads directly queryable in `psql`,
which is genuinely useful during incident debugging. Proto bytes are
smaller, faster, type-faithful, and force consumers through the projection
layer — which is the entire point of CQRS.

## Decision

- **Primary payload column:** `payload BYTEA NOT NULL`, canonical proto.
  This is the contract.
- **Optional ops sidecar:** `payload_json JSONB`. Off by default. Opt-in
  per deployment. Populated at commit time via canonical `protojson`. NULL
  for events whose payload contains crypto-shredded fields.
- **Application code is forbidden from reading `payload_json`.** It is an
  ops artifact only. The framework's read API exposes only the decoded
  Go payload (from proto bytes), never the JSONB.
- **A future `esctl` CLI** decodes events through the codec, providing
  the same ops debuggability without requiring the JSONB sidecar.

## Consequences

### Positive

- **CQRS discipline is enforced at the framework level.** No production
  code can write `WHERE payload->>'email' = ...`. Reads go through
  projections, which is the contract that makes schema evolution possible.
- **Smaller and faster.** Proto bytes are typically 3-10× smaller than
  JSON and several times faster to encode/decode.
- **Type-faithful.** `int32` vs `int64`, `bytes` vs `string`, enum identity
  are preserved exactly. JSON loses these at serialize time; JSONB cannot
  recover what was already lost.
- **Schema evolution is rigorous.** Protobuf field numbers, `reserved`,
  and backward-compatibility rules are battle-tested. JSON has no
  equivalent — it relies on convention.
- **Crypto-shredding is clean.** The payload is a single byte blob with a
  defined structure; encrypting whole fields under per-subject keys is
  well-defined. JSONB encryption either means complex per-field crypto or
  whole-blob encryption that destroys the queryability JSONB was chosen
  for.
- **Byte-stable for signing and hashing.** Canonical proto encoding is
  deterministic; JSON normalization is a famous footgun.

### Negative

- **`psql` exploration is harder.** Without the JSONB sidecar, ad-hoc
  payload queries during an incident require either the `esctl` CLI or
  loading through application code. The JSONB sidecar is the escape hatch.
- **Codegen carries the protojson path** for the sidecar. Modest
  additional complexity, paid once.
- **The JSONB sidecar can silently diverge** from the proto bytes if the
  codec changes. We mitigate by always regenerating sidecars from proto
  bytes at commit time, never editing them in place.

## Alternatives Considered

### JSONB-primary

Rejected. Loses type fidelity, loses schema discipline, makes crypto-
shredding messy, costs 3-10× more storage and CPU, and most importantly
enables direct payload queries that undermine CQRS and bind callers to
event shape.

### `google.protobuf.Any` payload

Rejected. Roughly doubles wire size (the `type_url` is embedded inside
`Any`) and forces a descriptor lookup at decode. We carry `type_url` in
the envelope ourselves and treat `payload` as raw bytes.

### Proto bytes only (no JSONB sidecar ever)

Considered. Defensible. The ops cost of forcing every payload query
through the codec is real but solvable with a CLI. We opted to ship the
opt-in JSONB sidecar as a pragmatic concession to incident debugging,
with the explicit constraint that application code cannot read it.
