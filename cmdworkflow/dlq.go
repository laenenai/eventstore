package cmdworkflow

import (
	"context"
	"time"
)

// SubscriberDLQRow is one quarantined delivery for a subscriber. The
// row represents a whole command-batch that exhausted its retry
// budget, not a single event — per-batch delivery (ADR 0029) means
// the unit of failure is the batch the subscriber received.
//
// EventIDs and TypeURLs are index-aligned: TypeURLs[i] is the type
// URL of the event with id EventIDs[i].
type SubscriberDLQRow struct {
	SubscriberName string
	TenantID       string
	StreamID       string

	// Batch fields — one row per (subscriber, failed command-batch).
	// EventIDs is ordered to match the envelope batch the subscriber
	// received; TypeURLs is index-aligned with EventIDs.
	EventIDs []string
	TypeURLs []string

	LastError  string
	Attempts   int
	EnqueuedAt time.Time
}

// SubscriberDLQWriter is the storage surface for capturing
// permanently-failed deliveries.
type SubscriberDLQWriter interface {
	InsertSubscriberDLQ(ctx context.Context, row SubscriberDLQRow) error
}

// SubscriberDLQAdmin is the operator-facing surface for listing,
// replaying, and clearing DLQ rows. Replay / delete keying is by the
// FIRST event id in the batch — operators identify a quarantined
// batch by the leading event id from List output.
type SubscriberDLQAdmin interface {
	ListSubscriberDLQ(ctx context.Context, subscriberName, tenant string, limit int) ([]SubscriberDLQRow, error)
	ClearSubscriberDLQ(ctx context.Context, subscriberName, tenant string) (int, error)
	DeleteSubscriberDLQRow(ctx context.Context, subscriberName, tenant, firstEventID string) error
}
