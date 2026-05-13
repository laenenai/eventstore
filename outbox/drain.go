// Package outbox hosts the outbox-drain helper.
//
// See ADR 0014 for the outbox semantics and ADR 0012 for how the
// drain fits into the Profile B (scale-to-zero) delivery model:
// the writer commits events + outbox rows atomically; a scheduled
// drain wakes the DB on a cadence, pulls pending rows, hands each
// to the configured EventPublisher, marks rows published.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/publisher"
)

// Drain is the outbox drain runtime. Holds a reference to an
// OutboxStore (a storage adapter implementing the outbox queries) and
// a Publisher (the configured EventPublisher).
type Drain struct {
	// Store provides the outbox queries. Implemented by storage
	// adapters (adapters/storage/sqlite, adapters/storage/postgres).
	Store es.OutboxStore

	// Publisher hands events to subscribers. Pluggable per ADR 0012:
	// inproc for tests; restate / nats / sns / pubsub / cfqueues for
	// production.
	Publisher publisher.Publisher

	// Tenant scopes the drain to a single tenant. Empty string means
	// cross-tenant (a shared scheduled drain across all tenants in
	// the database).
	Tenant string

	// BatchSize is the number of rows pulled per Run iteration.
	// Default 100. Larger batches amortize wake-up overhead;
	// smaller batches reduce blast radius on a stuck publisher.
	BatchSize int

	// CleanupRetention sets how long published rows are kept before
	// the drain's cleanup pass deletes them. Default 7 days.
	// Set to 0 to disable cleanup.
	CleanupRetention time.Duration
}

const (
	defaultBatchSize        = 100
	defaultCleanupRetention = 7 * 24 * time.Hour
)

// Run pulls all currently-pending rows in batches, publishes each via
// the configured Publisher, marks published rows, and runs the
// cleanup pass. Returns when the pending set is empty (caught up) or
// ctx is cancelled.
//
// Run is the entrypoint for a scheduled drain job. Typical deployment
// invokes Run from a cron, Lambda, Cloudflare Worker scheduled
// trigger, or similar.
//
// Returns the count of rows published and the count cleaned up.
func (d *Drain) Run(ctx context.Context) (published int, cleaned int, err error) {
	if err := d.validate(); err != nil {
		return 0, 0, err
	}

	// Drain pending rows in batches until the pending set is empty.
	for {
		if err := ctx.Err(); err != nil {
			return published, cleaned, nil
		}
		n, err := d.RunOnce(ctx)
		if err != nil {
			return published, cleaned, err
		}
		if n == 0 {
			break
		}
		published += n
	}

	// Cleanup pass.
	if d.CleanupRetention > 0 {
		cutoff := time.Now().UTC().Add(-d.cleanupRetention())
		if c, err := d.Store.CleanupPublishedOutbox(ctx, d.Tenant, cutoff); err != nil {
			return published, cleaned, fmt.Errorf("outbox cleanup: %w", err)
		} else if c > 0 {
			cleaned = c
		}
	}

	return published, cleaned, nil
}

// RunOnce processes one batch of pending rows. Returns the count
// successfully published. A zero return means the pending set is
// empty OR every row in the batch failed.
//
// Useful for callers that want fine-grained control over the drain
// cadence (e.g., wrap with custom backoff between batches, integrate
// with rate-limited downstream publishers). Run is the standard
// scheduled-drain entrypoint that calls RunOnce in a loop until
// drained.
func (d *Drain) RunOnce(ctx context.Context) (int, error) {
	if err := d.validate(); err != nil {
		return 0, err
	}
	rows, err := d.Store.PendingOutbox(ctx, d.Tenant, d.batchSize())
	if err != nil {
		return 0, fmt.Errorf("outbox: read pending: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	var published int
	for _, row := range rows {
		if err := d.Publisher.Publish(ctx, row.Envelope); err != nil {
			// Mark as failed; the next drain run will retry.
			if markErr := d.Store.MarkOutboxFailed(ctx, row.Envelope.TenantID,
				row.Envelope.GlobalPosition, err.Error()); markErr != nil {
				return published, fmt.Errorf("outbox: mark failed: %w", markErr)
			}
			// Continue with the rest of the batch — one failing row
			// shouldn't stop the others.
			continue
		}
		if err := d.Store.MarkOutboxPublished(ctx, row.Envelope.TenantID,
			row.Envelope.GlobalPosition); err != nil {
			return published, fmt.Errorf("outbox: mark published: %w", err)
		}
		published++
	}
	return published, nil
}

func (d *Drain) validate() error {
	if d.Store == nil {
		return errors.New("outbox: Store is required")
	}
	if d.Publisher == nil {
		return errors.New("outbox: Publisher is required")
	}
	return nil
}

func (d *Drain) batchSize() int {
	if d.BatchSize <= 0 {
		return defaultBatchSize
	}
	return d.BatchSize
}

func (d *Drain) cleanupRetention() time.Duration {
	if d.CleanupRetention <= 0 {
		return defaultCleanupRetention
	}
	return d.CleanupRetention
}
