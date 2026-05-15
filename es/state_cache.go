package es

import (
	"context"
	"iter"
	"time"
)

// StateCacheRow is the typed shape of one row in the Tier 1 state cache.
// State carries the codec-marshaled bytes; the caller unmarshals via the
// aggregate's own StateCodec to recover a typed value.
//
// StateSchemaVersion identifies the shape of the marshalled state and
// powers the same invalidation contract that previously lived on
// snapshots (ADR 0011, superseded by ADR 0023): on Load, a row whose
// stored schema version doesn't match the runtime's current
// StateSchemaVersion is silently discarded with full-replay fallback.
//
// See ADR 0020 + ADR 0023.
type StateCacheRow struct {
	TenantID           string
	StreamID           string
	TypeURL            string
	State              []byte
	Version            uint64
	Terminal           bool
	StateSchemaVersion uint32
	UpdatedAt          time.Time
}

// StateCacheReader is the read surface for the Tier 1 state cache.
// Adapters implement this in addition to Store. The cache is opt-in
// per-aggregate via aggregate.Runtime.StateCodec; rows for
// non-opted-in aggregates simply do not exist (GetState returns
// ErrStateNotFound, ListStates returns empty).
type StateCacheReader interface {
	// GetState returns the cached state for one stream, or
	// ErrStateNotFound when the stream has no cached row.
	GetState(ctx context.Context, tenantID, streamID string) (StateCacheRow, error)

	// ListStates returns up to limit cached rows for the given type
	// in a single tenant, ordered by stream_id ascending. Pass the
	// stream_id of the last row in the previous page as
	// afterStreamID to fetch the next page; the first page uses "".
	ListStates(ctx context.Context, tenantID, typeURL, afterStreamID string, limit int) ([]StateCacheRow, error)
}

// StateCacheWriter is the operator-side surface: rebuild and wipe rows
// for a given type. Adapters implement this alongside StateCacheReader.
//
// WipeStateCacheForType is the operator-issued half of a rebuild; the
// caller's rebuild helper (aggregate.RebuildStateCache) replays events
// to repopulate the cache.
type StateCacheWriter interface {
	// WipeStateCacheForType deletes every cached row matching the
	// given (tenant_id, type_url). When tenantID is "" all tenants
	// are wiped. Returns the number of rows deleted.
	WipeStateCacheForType(ctx context.Context, tenantID, typeURL string) (int64, error)
}

// ScanAllStates returns an iterator over every cached state row for
// the given (tenantID, typeURL), pulling pages of pageSize via the
// underlying ListStates cursor. The iteration ends naturally when the
// store yields a short page; an error from ListStates is yielded once
// and the iterator stops.
//
// Intended use is projection rebuild from state_cache (cookbook 08,
// Pattern 4) — O(streams) instead of O(events) when the projection is
// a pure function of current state. The typeURL is exact-match;
// cross-aggregate projections call ScanAllStates once per aggregate
// type they consume.
//
// pageSize ≤ 0 falls back to 1000. The iterator is single-pass; the
// underlying store remains read-only during iteration but is not
// frozen — concurrent writes may or may not appear in later pages
// (Postgres snapshot semantics; SQLite single-writer makes it the
// same in practice).
func ScanAllStates(
	ctx context.Context,
	r StateCacheReader,
	tenantID, typeURL string,
	pageSize int,
) iter.Seq2[StateCacheRow, error] {
	if pageSize <= 0 {
		pageSize = 1000
	}
	return func(yield func(StateCacheRow, error) bool) {
		after := ""
		for {
			if err := ctx.Err(); err != nil {
				yield(StateCacheRow{}, err)
				return
			}
			rows, err := r.ListStates(ctx, tenantID, typeURL, after, pageSize)
			if err != nil {
				yield(StateCacheRow{}, err)
				return
			}
			if len(rows) == 0 {
				return
			}
			for _, row := range rows {
				if !yield(row, nil) {
					return
				}
			}
			// A short page means we've exhausted the matching rows;
			// no point in another round-trip that would return zero.
			if len(rows) < pageSize {
				return
			}
			after = rows[len(rows)-1].StreamID
		}
	}
}

// StateCacheUpserter is the direct-write surface used by
// aggregate.RebuildStateCache. The normal write path goes through
// Store.Append (events + cache row in one tx); rebuild bypasses that
// to write only the cache row without producing new events. Adapters
// implement this alongside the rest of the state-cache surface.
type StateCacheUpserter interface {
	UpsertCachedState(ctx context.Context, row StateCacheRow) error
}
