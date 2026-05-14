package es

import (
	"context"
	"time"
)

// StateEnvelope is the unit of delivery for state_stream subscribers
// (ADR 0024). Carries the marshalled current state of one stream plus
// the metadata a receiver needs to dedupe (Version), route (TypeURL),
// and upcast (StateSchemaVersion).
//
// Parallel to es.Envelope (used by events outbox + Publisher); kept
// distinct because the contracts differ — events are append-only and
// every event must deliver; state_stream is coalesced and only the
// latest matters.
type StateEnvelope struct {
	TenantID           string
	StreamID           string // canonical
	TypeURL            string // proto FullName of the state type
	Version            uint64 // stream version this state reflects
	StateSchemaVersion uint32 // for receiver-side upcasting
	State              []byte // marshalled state (typically protojson)
	UpdatedAt          time.Time
}

// StatePublisher delivers one StateEnvelope. Implementations vary
// (HTTP, NATS, in-process for tests). A returned error keeps the
// drain's subscriber-position row at its previous value, so the next
// drain cycle re-attempts with the latest state at that point — the
// coalescing-on-retry property documented in ADR 0024.
//
// Receivers MUST be idempotent on (TenantID, StreamID, Version):
// duplicate or out-of-order deliveries can happen on retry. The
// recommended pattern is "upsert if incoming.Version > stored.Version,
// else ignore."
type StatePublisher interface {
	PublishState(ctx context.Context, env StateEnvelope) error
}
