# ADR 0005: Event Envelope Schema

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

The event envelope is the framework's most permanent decision. Events
written today must be readable in ten years. The envelope shape determines
the storage schema, the indexing strategy, the operational query surface,
and the contract every consumer is bound to.

Several decisions are coupled here: which identifiers exist, which
timestamps, which causality fields, what time fidelity, what is required
versus optional, and what is explicitly excluded.

## Decision

Every event envelope carries the following fields. Each is required unless
noted.

### Identity and ordering

- `event_id` — UUIDv7. Globally unique, time-sortable (the prefix encodes
  generation time at millisecond resolution). Used for dedup, signing, and
  index locality.
- `tenant_id` — non-empty (see ADR 0007 on multi-tenancy).
- `stream_id` — canonical `"<type>-<id>"` (see ADR 0008). Opaque to the
  store layer.
- `version` — per-stream, monotonic, starts at 1. Drives optimistic
  concurrency via the events PRIMARY KEY.
- `global_position` — store-wide monotonic bigint, sequence-allocated
  inside the append transaction (see ADR 0009). Drives projection cursors.

`version` and `global_position` are orthogonal: per-stream concurrency vs
store-wide ordering. Conflating them is a known anti-pattern.

### Type and schema

- `type_url` — fully-qualified proto type identifier (e.g.,
  `myapp.user.v1.UserEmailChanged`).
- `schema_version` — uint32. Upcaster dispatch key. Proto's wire-level
  backward compatibility covers field-shape changes; `schema_version`
  handles semantic changes that proto cannot detect.

### Time

- `occurred_at` — domain time, from the command. The wall-clock moment
  the user/system performed the action.
- `recorded_at` — DB time, set on commit. Monotonic relative to commit
  order within the same store.

Both are required. Incidents repeatedly show that omitting one or
substituting one for the other is a postmortem-quality failure.

### Causality and audit

- `correlation_id` — UUID; same across all events from one logical
  request or trace.
- `causation_id` — the `event_id` or `command_id` that directly caused
  this event. Distinct from `correlation_id`. Drives saga reasoning and
  lineage queries.
- `command_id` — the command that produced this event. Used for
  idempotency / dedup at the aggregate boundary.
- `actor` — structured proto message describing who or what initiated
  the action (user, system, service, on behalf of). Stored as proto bytes
  in `events.actor` plus a denormalized `actor_principal` text column for
  audit indexing.

### Payload

- `payload` — proto bytes of the concrete event (see ADR 0006).
- `encryption_key_refs` — JSONB blob listing per-field key references
  when crypto-shredding applies (see ADR 0010). NULL when no field is
  encrypted.

### Indices

- `(tenant_id, stream_id, version)` PRIMARY KEY — per-stream reads,
  optimistic concurrency.
- `(global_position)` UNIQUE — store-wide subscription cursor.
- `(event_id)` UNIQUE — dedup.
- `(tenant_id, correlation_id)` — trace queries.
- `(tenant_id, command_id)` — idempotency lookups.
- `(tenant_id, actor_principal)` — audit queries.

### Explicitly excluded

- **No free-form `metadata` map.** Bags rot. Important fields are
  promoted.
- **No `aggregate_type` column.** Implied by stream_id convention.
  Carrying both means owning the consistency between them.
- **No retention or TTL.** Events are facts, forever. Deletion semantics
  are handled by crypto-shredding (ADR 0010), not by envelope TTL.

## Consequences

### Positive

- Three distinct causality fields (correlation, causation, command) give
  postmortem clarity that retrofitting cannot.
- Strict typing — no metadata bag — means breaking changes are visible at
  schema-change time, not silent.
- Storage layout and operational query patterns fall out of this shape
  directly.
- Sub-fields like `actor` are structured rather than stringly typed; this
  pays off the moment audit queries grow beyond "who did this".

### Negative

- Envelope changes are expensive. A semantic change to any field requires
  versioning the envelope itself (new column or new table) and migrating
  consumers.
- Forty-ish bytes of metadata per event before payload. Acceptable cost
  for the observability and operational story.

## Alternatives Considered

### Free-form `metadata map<string, string>`

Rejected. Maps rot, get used for important data, and become an unmonitored
schema. Anything that matters is promoted to a field.

### Single timestamp (occurred OR recorded)

Rejected. Incidents prove both are needed: domain reports rely on
`occurred_at`, system reasoning relies on `recorded_at`.

### `google.protobuf.Any` for payload

Rejected. Doubles wire size and forces descriptor lookup at decode. We
carry `type_url` ourselves and treat the payload as opaque bytes.

### Conflating `event_id` and `global_position` into a single time-sortable ID

Rejected. They serve different purposes: identity (decentralized,
generation-time) vs ordering (DB-coordinated, commit-time). Coupling
breaks backfills, imports, and any future shard-merge scenario.
