package es

import (
	"context"
	"time"
)

// StateStreamStore is the storage surface state_stream.Drain reads
// from. Adapters implement it by reading state_cache LEFT JOIN their
// state_stream_subscribers position table. See ADR 0024.
type StateStreamStore interface {
	// ListStreamsBehind returns up to limit streams where the named
	// subscriber's last_delivered_version is below state_cache.version.
	// Tenant filter "" means cross-tenant. The returned rows carry the
	// full state payload — drained handlers don't need a back-channel
	// fetch.
	ListStreamsBehind(ctx context.Context, subscriberName, tenantID string, limit int) ([]StateEnvelope, error)

	// AdvanceStateStreamPosition records a successful delivery: upserts
	// the subscriber's last_delivered_version for the given stream.
	// Uses GREATEST/MAX on conflict so concurrent drainers can't
	// regress positions.
	AdvanceStateStreamPosition(ctx context.Context, subscriberName, tenantID, streamID string, version uint64) error
}

// StateStreamAdmin is the operator surface for state_stream
// subscribers (ADR 0024 § 5). Method names are prefixed to coexist
// with the other admin surfaces (OutboxAdmin, ProjectionAdmin,
// ProjectionDLQAdmin) that adapters implement on the same struct.
type StateStreamAdmin interface {
	// StateStreamStatus reports per-subscriber lag for
	// monitoring/alerting. Tenant "" reports cross-tenant.
	StateStreamStatus(ctx context.Context, subscriberName, tenantID string) (StateStreamSubscriberStatus, error)

	// ResetStateStreamSubscriber deletes every position for the
	// named subscriber. The next Drain.Run sees no positions and
	// full-backfills the subscriber from current state. Returns the
	// number of position rows removed.
	ResetStateStreamSubscriber(ctx context.Context, subscriberName, tenantID string) (int64, error)

	// ResetStateStreamSubscriberForStream rewinds one stream's
	// position so the drain redelivers the current state on its
	// next pass. Used after the crypto-shred propagation runbook
	// (cookbook recipe 13).
	ResetStateStreamSubscriberForStream(ctx context.Context, subscriberName, tenantID, streamID string) error

	// ListStateStreamSubscribers enumerates the distinct subscriber
	// names known to the position table. Useful for ops dashboards.
	ListStateStreamSubscribers(ctx context.Context) ([]string, error)
}

// StateStreamSubscriberStatus summarizes one subscriber's delivery
// progress.
type StateStreamSubscriberStatus struct {
	Name           string
	TenantID       string // "" for cross-tenant queries
	StreamsBehind  int64
	MaxLagVersions uint64    // largest gap (state.version - position.last_delivered_version) across all streams
	CheckedAt      time.Time // when this snapshot was taken
}
