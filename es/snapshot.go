package es

import (
	"context"
	"time"
)

// Snapshot is one row of the snapshots table — the folded state of an
// aggregate at a specific version. See ADR 0011.
//
// The state bytes are produced by aggregate.StateCodec (whichever
// codec the runtime is using, typically protojson for proto.Message
// states). StateSchemaVersion is the version of the state-struct
// shape; when the consumer's current version disagrees, the snapshot
// is treated as stale and full-replay fallback kicks in.
type Snapshot struct {
	TenantID           string
	StreamID           string
	Version            uint64
	StateSchemaVersion uint32
	State              []byte
	CreatedAt          time.Time
}

// SnapshotStore is implemented by adapters that persist snapshots.
//
// Load returns ErrSnapshotNotFound when no snapshot exists for the
// (tenant, stream). The aggregate runtime treats that as a normal
// "fall back to full replay" signal — never an error.
type SnapshotStore interface {
	LoadSnapshot(ctx context.Context, tenantID, streamID string) (Snapshot, error)

	// SaveSnapshot writes a snapshot, replacing any prior row for
	// the same stream (latest-wins per ADR 0011). The runtime calls
	// this lazily on read once enough events have accumulated.
	SaveSnapshot(ctx context.Context, snap Snapshot) error

	// DeleteSnapshot lets operators force a full-replay on the next
	// read — handy after a debug session, or for one-off corruption
	// recovery. The runtime doesn't call this on its own.
	DeleteSnapshot(ctx context.Context, tenantID, streamID string) error
}
