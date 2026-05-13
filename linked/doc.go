// Package linked is the framework's Tier-3.5 derived-stream
// runtime (ADR 0022). A LinkedProjection observes source events and
// produces derived events into named streams — the EventStoreDB
// `linkTo` / `emit` pattern, expressed as a normal Tier-3 projection.
//
// The derived stream is itself a queryable event stream: subscribers
// can't tell whether an event was authored by a human-issued command
// or produced by a linked projection. Per-event idempotency uses the
// framework's first-class uniqueness constraint keyed on the source
// event id, so replays don't duplicate.
//
// v1 of this package ships the runtime helper. The proto-annotation +
// codegen path described in ADR 0022 ("destination aggregate emitted
// by codegen") is a follow-up; users today wire LinkedProjections
// programmatically. See ADR 0022 § Status.
package linked
