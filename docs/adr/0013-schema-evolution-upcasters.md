# ADR 0013: Schema Evolution and Upcasters

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

Events live forever. The code that reads them does not. Over the years a
framework that hosts long-lived event-sourced systems will see:

- **Level 1, additive changes** — new optional fields, new enum values,
  new `oneof` variants. Protobuf's wire format handles these natively.
- **Level 2, semantic changes** — same wire shape, different meaning
  (units changed, an enum's interpretation shifted). Protobuf cannot see
  these.
- **Level 3, structural changes** — splits, renames, type
  re-categorizations. Neither wire format nor `schema_version` alone is
  enough.

The framework must make Level 1 trivial, Level 2 explicit and cheap, and
Level 3 possible without forcing engineers into ugly workarounds or
paralysis.

## Decision

### Strategy: read-time upcasting via a registry

Events are stored exactly as written. When historical events are read, a
registered chain of pure-function upcasters transforms each event from
its persisted shape to the shape the current code expects.

Stored bytes are never rewritten. The events table is immutable.

### Where upcasters run

Only on the **read** side of historical events:

- Aggregate command path (load events → upcast → fold via `Evolve`).
- Snapshot creation (same path).
- Projection rebuild (re-read DB into shadow table).
- Bus consumers fetching gap-fills from the store (the only place a bus
  consumer touches upcasters).

Upcasters do **not** run on:

- Inline projections in the writer transaction (events were just emitted
  in current shape).
- Bus subscribers receiving freshly-published events.

### Upcaster contract

```go
type Upcaster interface {
    From() (typeURL string, schemaVersion uint32)
    Up(env Envelope, payload proto.Message) ([]Upcast, error)
}

type Upcast struct {
    NewTypeURL       string         // may differ — enables Level 3 renames
    NewSchemaVersion uint32         // typically From().schemaVersion + 1
    NewPayload       proto.Message
}
```

Constraints, enforced by runtime and CI lint:

- **Pure.** No I/O, no clocks, no randomness. Same input → same output
  forever.
- **One-in, N-out, N ≥ 1.** Splits supported. Producing zero events is
  forbidden. To effectively drop an old event, swap it to a typed
  `Deprecated{original_type_url}` event that the decider silently
  ignores — auditable, not silent.
- **Chained.** v1 → v2 → v3 applied in sequence by the runtime. Authors
  write one hop per upcaster.
- **May change `type_url`.** Enables renames and splits.
- **Cannot change envelope facts:** `event_id`, `tenant_id`, `stream_id`,
  `version`, `global_position`, `occurred_at`, `recorded_at`,
  `correlation_id`, `causation_id`, `command_id`, `actor`. Only
  `type_url`, `schema_version`, and `payload` may change.

### Where upcasters live

Codegen scaffolds and developers fill in bodies:

```
proto/user/v1/user_events.proto     # source of truth (schema_version, type_url annotations)
gen/user/upcasters.gen.go            # generated stubs — not hand-edited
user/upcasters.go                    # handwritten bodies
```

The codegen plugin detects `schema_version` bumps and emits stub
registrations. The lint suite fails the build if a stub exists without a
body, or if a body exists without a corresponding declared upcaster.

### Forward compatibility — reading future events

If running code encounters `schema_version` newer than any upcaster
targets, the framework fails loudly with `ErrUnknownSchemaVersion`.
Reading the future is a code-version mismatch — it must surface as a
deployment problem, not silently succeed.

### Interaction with snapshots

`state_schema_version` (ADR 0011) and event `schema_version` evolve
independently. A state-schema bump invalidates snapshots → full replay.
During that replay, event upcasters run as needed. The two versioning
axes don't need to coordinate explicitly.

### Shipped lint suite

- **Proto compatibility lint:** fails if any field number is reused, any
  field is removed without `reserved`, any enum value is renumbered.
- **Schema-version monotonicity lint:** verifies `schema_version`
  annotations only increase.
- **Upcaster completeness lint:** every `(type_url, schema_version)`
  observed in the registry must have an upcaster chain that reaches the
  current version.
- **Upcaster determinism lint:** static analysis flags `time.Now()`,
  `math/rand`, file or network I/O inside upcaster bodies.

## Consequences

### Positive

- **Stored bytes are immutable forever.** Backups, signatures, hashes,
  and audits all remain stable across schema evolution.
- **Authors handle one hop at a time.** Each upcaster is a small, pure
  function with one job. Cognitive load stays bounded.
- **Levels 1 and 3 cost almost nothing.** Level 1 is proto-native (no
  upcaster needed). Level 3 is one upcaster that emits a different
  `type_url`. The hard cases are isolated to genuinely hard semantic
  shifts.
- **Codegen + lint catch the failure modes** that destroy ES projects
  silently: forgotten upcasters, non-deterministic transforms, broken
  proto compatibility, missing schema bumps.

### Negative

- **Upcasters live forever.** N versions of an event type means N-1
  upcasters in the codebase, all maintained, all tested. Storage is
  cheap; cognitive surface grows.
- **Read cost grows with chain length.** Pure transforms are fast, but a
  10-hop chain is real work per event. Snapshots and projection caches
  mitigate this in practice.
- **Forward-compat failures fail loudly.** Operationally correct (you
  want to know) but adds a deployment-ordering constraint: code must be
  upgraded before events written under the new schema can be read by
  older instances.

## Alternatives Considered

### In-place rewrite on read

Upcast then write the new payload back to the events table. **Rejected.**
Violates the immutability invariant, breaks byte stability for signing,
makes backup semantics depend on read order, and turns the events table
into "the state of the code as of last read of each row".

### Offline batch migration

Read all events of an old version, transform, rewrite. **Rejected** for
the same reasons as in-place rewrite. The events log is immutable.

### Versioned event types only — no upcasters ever

Every change introduces a new `type_url`. `Evolve` handles all versions
forever. **Rejected as a sole strategy.** Useful and supported for Level
3 (rename, split, fundamental re-categorization), but using it for
Level 2 (semantic shifts within the same event identity) bloats the
event taxonomy and dispatch tables forever, with no audit benefit over
upcasters.

### Merge upcasters (N-in, 1-out)

Combine two historical events into one. **Rejected** for v1. Hard in a
streaming model and rarely the right answer. If genuinely needed, model
it as a saga that reads both events and emits a fused event going
forward — the historical record stays as-is.

### Declarative DSL (`upcasters.yaml`)

Authors describe upcasters in a domain-specific language rather than Go.
**Rejected.** Either restricts what's expressible (and forces escape
hatches) or grows into a half-language. Go is already in the project;
the lint suite enforces the discipline a DSL would have provided.

### Tombstoning old upcaster versions (after verifying no events exist)

Eventually remove upcasters once the framework has verified no events
under the old version remain anywhere. **Deferred.** Adds a verification
flow and risks ("are you sure?") that aren't worth the small storage win
in v1. Documented for future consideration; upcasters live forever by
default.

**Sunset criterion.** Tombstoning becomes a v1.1+ feature when one of:

- A maintained aggregate accumulates **≥3 upcaster versions** on the
  same field, the original ones are confirmed unused via a per-tenant
  DB scan, and operators ask for a pruning workflow, OR
- Any adopter explicitly requests the operational story (open issue,
  RFE).

Estimated effort when triggered: ~3 engineer-days. Verification helper
(pre-flight scan refusing tombstone if any event at the target version
exists), write-side reject for the version, two-phase commit
(mark-then-delete). The `aggregate.RebuildStateCache` flow handles any
rebuild fallout.
