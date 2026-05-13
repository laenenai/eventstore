# ADR 0004: Command and Event Sum-Type Representation

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

Commands and events are protobuf messages. Protobuf has `oneof` for sum
types; Go does not have algebraic data types and represents sum types via
sealed interfaces and type switches.

The decider model (ADR 0003) expects `Decide(state, cmd)` to receive one of
several command variants and `Evolve(state, event)` to receive one of
several event variants. Each aggregate has a finite, closed set of
variants.

Three representations are possible:

1. Sealed interface with handwritten variant structs.
2. Direct use of protobuf-generated types throughout business code.
3. Hybrid: proto `oneof` on the wire, codegen-emitted clean Go types in
   business code, with a bridge at the registry boundary.

## Decision

Adopt the hybrid (option 3).

- Each aggregate has a `.proto` declaring two oneofs: `Commands` and
  `Events`.
- The codegen plugin emits:
  - Clean Go variant structs (no pointer-heavy proto noise) per variant.
  - A sealed interface per aggregate (`UserCommand`, `UserEvent`) implemented
    by each variant via a marker method.
  - A registry mapping `type_url` ↔ Go variant type ↔ proto descriptor.
  - Marshal/unmarshal bridges Go ↔ proto bytes used at the storage and
    transport boundary.
  - An exhaustiveness analyzer (`go vet`–style) that fails CI when a new
    variant is added without a matching handler in the decider or projector.

Business code reads and writes the Go types. The proto runtime is only
visible at the boundary.

## Consequences

### Positive

- **Business code is idiomatic Go.** No `GetX()` accessor noise, no
  pointer-heavy field access patterns.
- **Single source of truth.** The `.proto` defines both the wire format
  and the Go shapes.
- **Wire format is canonical proto.** Language-agnostic, schema-evolution
  rules apply (field numbers, `reserved`, backward compatibility).
- **Compile-time + lint-time enforcement.** Sealed interface marker
  methods prevent unknown variants; exhaustiveness analyzer catches missing
  handlers in CI.

### Negative

- **Codegen plugin complexity.** Maintaining two type families and a
  bridge is non-trivial. Paid once by the framework, not per consumer.
- **Two layers of types in tooling.** A debugger may show both the Go
  variant and the proto descriptor; pprof and traces must respect both.

## Alternatives Considered

### Sealed interface, handwritten variants

Idiomatic but offers no compile-time exhaustiveness check, no canonical
wire format, no schema-evolution guarantees, and each aggregate redoes the
dispatch boilerplate by hand.

### Proto-generated types used directly throughout

Eliminates the bridge but produces pointer-heavy Go code, tight coupling to
the proto runtime in business code, and awkward ergonomics when constructing
variants (every variant requires `&pb.UserEvent{Variant: &pb.UserEvent_X{X: ...}}`).
