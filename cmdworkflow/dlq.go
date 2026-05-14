package cmdworkflow

import (
	"context"
	"time"
)

// SubscriberDLQRow is one quarantined delivery for a subscriber.
// Mirrors es.ProjectionDLQRow shape so operator tooling can reuse
// patterns.
type SubscriberDLQRow struct {
	SubscriberName string
	TenantID       string
	EventID        string
	StreamID       string
	TypeURL        string
	LastError      string
	Attempts       int
	EnqueuedAt     time.Time
}

// SubscriberDLQWriter is the storage surface for capturing
// permanently-failed deliveries.
type SubscriberDLQWriter interface {
	InsertSubscriberDLQ(ctx context.Context, row SubscriberDLQRow) error
}

// SubscriberDLQAdmin is the operator-facing surface for listing,
// replaying, and clearing DLQ rows.
type SubscriberDLQAdmin interface {
	ListSubscriberDLQ(ctx context.Context, subscriberName, tenant string, limit int) ([]SubscriberDLQRow, error)
	ClearSubscriberDLQ(ctx context.Context, subscriberName, tenant string) (int, error)
	DeleteSubscriberDLQRow(ctx context.Context, subscriberName, tenant, eventID string) error
}
