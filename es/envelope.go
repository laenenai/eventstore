package es

import (
	"time"

	"github.com/google/uuid"
)

// Envelope is the framework's Go-side wrapper around every event. The
// wire format uses gen/eventstore/envelope/v1.Envelope (proto), with
// conversion at the storage adapter boundary.
//
// See ADR 0005 for the field-by-field design rationale.
type Envelope struct {
	// Identity & ordering
	EventID        uuid.UUID
	TenantID       string
	StreamID       StreamID
	Version        uint64
	GlobalPosition uint64

	// Type & schema
	TypeURL       string
	SchemaVersion uint32

	// Time
	OccurredAt time.Time // domain time, from command
	RecordedAt time.Time // DB commit time, set by adapter

	// Causality & audit
	CorrelationID uuid.UUID
	CausationID   uuid.UUID
	CommandID     uuid.UUID
	Actor         Actor

	// Payload
	Payload     []byte // canonical proto bytes
	PayloadJSON []byte // optional ops sidecar; nil when not enabled or shredded
	KeyRefs     []byte // crypto-shredding per-field key references; nil when no field is encrypted

	// Tamper-evidence (ADR 0028). Computed by the storage adapter at
	// append time. Hash is SHA-256 over the deterministic proto
	// serialization of this envelope with Hash and PrevHash cleared,
	// prepended with PrevHash. Verify via VerifyStreamChain.
	Hash     []byte // 32 bytes; empty pre-append, populated on Append/Read.
	PrevHash []byte // 32 zero bytes for version 1; predecessor's Hash otherwise.
}
