// Package state_stream is the coalesced state-mirror delivery runtime
// (ADR 0024). State_stream subscribers are external systems that want
// to keep a copy of the latest state of every stream in sync — search
// indexes, dashboards, denormalized read stores, webhook targets.
//
// The runtime mirrors the outbox drain's shape (cookbook recipe 06):
// configure a Drain, point it at a StatePublisher, run it on whatever
// schedule fits your deployment. Same advisory-lock pattern, same
// scaling story.
//
// Coalescing on retry is structural: failed deliveries don't queue —
// the next drain cycle delivers whatever the current state is, not
// the version that previously failed. Subscribers idempotency on
// (TenantID, StreamID, Version) absorbs the rare duplicate delivery.
package state_stream
