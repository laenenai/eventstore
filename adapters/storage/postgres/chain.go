package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
)

// BackfillEventHash implements es.StreamChainRebuilder. It populates
// the hash and prev_hash columns on one event row whose chain columns
// are currently NULL, as written by streams that existed before the
// ADR 0028 migration introduced the columns.
//
// The underlying UPDATE has a `WHERE hash IS NULL` guard so a row that
// already carries a non-NULL hash is never overwritten. If the guard
// matches zero rows, this method returns a non-nil error so the
// caller (es.RebuildStreamChain) knows the expected write did not
// happen, rather than silently dropping the operation. The error path
// is defensive: a correct caller has already verified via ReadStream
// that the row's Hash is empty before invoking this.
func (a *Adapter) BackfillEventHash(ctx context.Context, tenantID string, eventID uuid.UUID, hash, prevHash []byte) error {
	var n int64
	err := a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		n, inner = q.BackfillEventHash(ctx, db.BackfillEventHashParams{
			Hash:     hash,
			PrevHash: prevHash,
			TenantID: tenantID,
			EventID:  eventID,
		})
		return inner
	})
	if err != nil {
		return fmt.Errorf("postgres: backfill event hash: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("postgres: backfill event hash: row tenant=%s event=%s already has a hash or does not exist", tenantID, eventID)
	}
	return nil
}
