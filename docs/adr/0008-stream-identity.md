# ADR 0008: Stream Identity

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

A stream is the unit of optimistic concurrency in the framework. Each
stream has an identifier. The shape of that identifier propagates into
every framework API, every storage column, every test fixture, and every
debugger view.

Three real options:

1. **Opaque string.** `"user-42"`. Convention encodes the aggregate type.
2. **Structured value type.** `StreamID{Tenant, Type, ID}` — a typed struct
   throughout the API. Stored serialized in the DB.
3. **Typed wrapper around a string.** `type StreamID string` with
   constructors and accessors that hide string parsing.

## Decision

Adopt option 2: a structured `es.StreamID{Tenant, Type, ID}` core type,
with codegen-emitted typed wrappers per aggregate.

```go
package es

type StreamID struct {
    Tenant string  // required, non-empty, set from ctx
    Type   string  // slug-validated, set by codegen per aggregate
    ID     string  // slug-validated, supplied by caller
}
```

- **Storage:** two columns — `tenant_id` (from multi-tenancy, ADR 0007)
  and `stream_id`. The latter stores the canonical `Type + "-" + ID` form.
  The store layer does not need to know about the `Type/ID` split.
- **Validation:** `Type` and `ID` must match a slug regex
  (`^[a-z0-9][a-z0-9_-]{0,127}$`). Enforced in the constructor. No other
  code path constructs a `StreamID`.
- **Codegen per aggregate** emits a typed wrapper:

```go
// Generated for the User aggregate
package user

type StreamID struct{ inner es.StreamID }

func NewStreamID(ctx context.Context, id string) (StreamID, error) {
    tenant, ok := estenant.From(ctx)
    if !ok { return StreamID{}, es.ErrTenantMissing }
    if !slug.Valid(id) { return StreamID{}, es.ErrInvalidStreamID }
    return StreamID{
        inner: es.StreamID{Tenant: tenant, Type: "user", ID: id},
    }, nil
}

func (s StreamID) ES() es.StreamID { return s.inner }
```

- **Public APIs** use the typed wrapper; framework internals use
  `es.StreamID`. Cross-aggregate operations must explicitly drop to
  `es.StreamID` — by design.
- **No re-parsing in business code.** Only the storage adapter splits
  the canonical form when loading.

## Consequences

### Positive

- **Cross-aggregate type errors caught at compile time.** A function
  expecting `user.StreamID` cannot be handed an `order.StreamID`.
- **Tenant binding is automatic.** Constructors read tenant from `ctx`;
  callers cannot accidentally construct a stream ID for the wrong tenant.
- **Storage stays simple.** Two columns, no schema knowledge of aggregate
  types in the store layer.
- **Constructors enforce validation.** Slug validation runs at
  construction, not at every read.

### Negative

- **Codegen complexity grows slightly.** Per-aggregate typed wrappers
  must be generated.
- **Cross-aggregate operations are slightly more verbose**, requiring an
  explicit `.ES()` to drop to the core type. This is the intended trade.

## Alternatives Considered

### Opaque string

Rejected. No compile-time guarantee that a user-handling function got a
user stream. Typos become runtime errors. Cross-stream operations re-parse
the string everywhere.

### Typed wrapper around `string` (no struct)

Rejected. Hides the `Tenant/Type/ID` split behind accessor methods that
must re-parse the string each time. Constructor logic ends up identical
to option 2 but with worse ergonomics and slower runtime behavior.
