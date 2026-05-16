# linked

Tier-3.5 derived-stream runtime. A `LinkedProjection` observes
source events and produces derived events into named streams — the
EventStoreDB `linkTo` / `emit` pattern, expressed as a normal Tier-3
projection. The derived stream is itself queryable; subscribers
can't tell whether an event was authored by a human-issued command
or by a linked projection.

## Load-bearing primitives

- [`Projection`](projection.go) — the configured linked projection;
  install via its `Handler()` on a `projection.Runtime`.
- [`Config`](projection.go) — `Name`, `Destination` store,
  optional `SourceTypeURLs` filter, the `Route` function, and
  `IdempotentEmit` (default true).
- [`Route` / `RouteFn`](projection.go) — maps one source envelope
  to a derived event (or `Skip=true`). Carries `DestinationStream`,
  `DerivedEvent` (proto.Message), `DerivedTypeURL`, `SchemaVersion`,
  `ExpectedVersion`, and optional `Actor` override.
- [`New`](projection.go) — validates a `Config` and returns
  a `*Projection`.

## Contract

Per-event idempotency is built in via a uniqueness claim keyed on
`(tenant, "linked:<Name>", source.event_id)`. On replay (cursor
reset, schema rebuild), the claim conflicts with the prior emission
and the dispatcher silently swallows — no duplicate derived event.
Disable with `IdempotentEmit=false` only when the destination
aggregate dedupes itself. Causation is preserved: derived event's
`CausationID` is the source event's id; correlation is propagated;
`OccurredAt` defaults to the source's domain time so causal timing
is preserved.

## Where to start reading

1. [`projection.go`](projection.go) — the whole runtime is one file;
   `Handler()` is the meat.
2. [Cookbook 12 — Linked Projections](../docs/cookbook/12-linked-projections.md)
   — end-to-end recipe.

## Relevant ADRs

- [0022 — Linked Projections (Tier 3.5)](../docs/adr/0022-linked-projections.md)
  — full design. Status: runtime accepted, codegen-driven destination
  aggregate emission deferred. Today, users wire `Config` by hand.
