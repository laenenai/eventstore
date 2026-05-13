package es

import (
	"encoding/binary"
	"time"

	"github.com/google/uuid"
)

// ConstraintOpKind discriminates between claim and release of a
// uniqueness constraint within an Append.
type ConstraintOpKind int

const (
	ClaimConstraint   ConstraintOpKind = 1
	ReleaseConstraint ConstraintOpKind = 2
)

// ConstraintOp is one uniqueness operation committed atomically with
// the events of an Append. See ADR 0010.
//
// Claim semantics: insert (tenant, scope, value, stream_id). A unique-
// violation on the PK fails the whole Append with ErrConstraintViolated.
//
// Release semantics: delete the (tenant, scope, value) row. Missing
// rows are not an error.
type ConstraintOp struct {
	Op    ConstraintOpKind
	Scope string
	Value string
}

// EventToAppend is the caller-supplied input shape for one event.
// The adapter fills in version (from ExpectedVersion + offset),
// global_position, and recorded_at.
type EventToAppend struct {
	EventID       uuid.UUID
	TypeURL       string
	SchemaVersion uint32
	OccurredAt    time.Time
	CorrelationID uuid.UUID
	CausationID   uuid.UUID
	CommandID     uuid.UUID
	Actor         Actor
	Payload       []byte
	PayloadJSON   []byte
	KeyRefs       []byte
}

// AppendParams describes one transactional Append operation.
type AppendParams struct {
	StreamID        StreamID
	ExpectedVersion uint64 // 0 for an empty/new stream
	Events          []EventToAppend
	Constraints     []ConstraintOp
}

// AppendResult reports the outcome of a successful Append.
type AppendResult struct {
	StartVersion        uint64
	EndVersion          uint64
	StartGlobalPosition uint64
	EndGlobalPosition   uint64
	RecordedAt          time.Time
}

// commandIDNamespace is the UUID v5 namespace under which
// DeriveCommandID derives deterministic command IDs.
var commandIDNamespace = uuid.MustParse("8c8b3f4e-1c0a-4c0e-9fe7-1e7c4f50c2a1")

// DeriveCommandID produces a deterministic UUIDv5 from
// (handlerName, sourceEventID, outputIndex). Same inputs always yield
// the same command_id, so target aggregates dedup under at-least-once
// bus delivery. See ADR 0015 and cookbook recipe 01.
func DeriveCommandID(handlerName string, sourceEventID uuid.UUID, outputIndex int) uuid.UUID {
	buf := make([]byte, 0, len(handlerName)+1+len(sourceEventID)+8)
	buf = append(buf, handlerName...)
	buf = append(buf, 0)
	buf = append(buf, sourceEventID[:]...)
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], uint64(outputIndex))
	buf = append(buf, idx[:]...)
	return uuid.NewSHA1(commandIDNamespace, buf)
}
