package aggregate

import (
	"context"
	"fmt"

	"github.com/laenenai/eventstore/es"
)

// StateCacheRebuilder is the storage surface that RebuildStateCache
// needs from an adapter: the ability to wipe rows for a given type
// and the ability to upsert fresh ones. Both adapters satisfy this by
// implementing es.StateCacheWriter and es.Store.
type StateCacheRebuilder interface {
	es.Store
	es.StateCacheWriter
}

// RebuildStateCache repopulates the state_cache table for one
// aggregate type. Use after a state-proto schema change, after
// enabling the cache on an aggregate that already has historical
// events, or after operator-driven corruption recovery.
//
// Workflow:
//
//  1. Wipe existing state_cache rows for (tenant, typeURL).
//  2. Stream events for the tenant in global_position order.
//  3. Group by (tenant, stream_id), fold via Decider.Evolve.
//  4. Upsert the final per-stream state via the aggregate.Runtime so
//     the cache row is written transactionally (one tiny Append per
//     stream — empty events list, just the state). Wait — that would
//     pollute the events table. We instead use a dedicated path that
//     bypasses Append's event-insert and writes state_cache directly.
//
// In practice, this helper reads events, folds states in-memory by
// stream, then issues one cache upsert per stream via a sequence of
// trivial Appends with NewStateBytes set and an empty Events slice.
// To keep the events table untouched, the adapter's Append rejects
// empty Events with a clear error — so the helper instead uses a
// lower-level path: it streams events with ReadAllForTenant, folds,
// and calls a per-adapter rebuild upsert. That path is exposed via
// the StateCacheUpserter interface below.
//
// Pass tenantID = "" to rebuild across all tenants.
//
// The function returns the number of (tenant, stream) pairs written.
func RebuildStateCache[S, C, E any](
	ctx context.Context,
	rb StateCacheRebuilder,
	rt *Runtime[S, C, E],
	tenantID string,
) (int, error) {
	if rt.StateCodec == nil {
		return 0, fmt.Errorf("aggregate: RebuildStateCache requires Runtime.StateCodec to be set")
	}

	// Discover the target type URL by encoding the Decider's initial
	// state — the resulting typeURL is the aggregate's state name.
	initial := rt.Decider.Initial()
	_, typeURL, err := rt.StateCodec.Encode(initial)
	if err != nil {
		return 0, fmt.Errorf("aggregate: probe state type URL: %w", err)
	}

	// Wipe existing rows for this type.
	if _, err := rb.WipeStateCacheForType(ctx, tenantID, typeURL); err != nil {
		return 0, fmt.Errorf("aggregate: wipe state_cache: %w", err)
	}

	// Stream events in global_position order. We fold per-stream
	// state in memory. For tenants/aggregates with very large stream
	// counts, callers can shard the rebuild by tenant or batch.
	const pageSize = 500

	type streamState struct {
		state    S
		version  uint64
		streamID es.StreamID
	}
	streams := map[string]*streamState{} // key: tenant|stream_id

	var cursor uint64
	for {
		var (
			envs []es.Envelope
			err  error
		)
		if tenantID == "" {
			envs, err = rb.ReadAll(ctx, cursor, pageSize)
		} else {
			envs, err = rb.ReadAllForTenant(ctx, tenantID, cursor, pageSize)
		}
		if err != nil {
			return 0, fmt.Errorf("aggregate: read events: %w", err)
		}
		if len(envs) == 0 {
			break
		}
		for _, env := range envs {
			evt, err := rt.Codec.Decode(env.TypeURL, env.SchemaVersion, env.Payload)
			if err != nil {
				// Skip non-target events. A tenant's event stream
				// can contain multiple aggregate types; only the
				// ones this runtime's codec knows about apply.
				if env.GlobalPosition > cursor {
					cursor = env.GlobalPosition
				}
				continue
			}
			key := env.TenantID + "|" + env.StreamID.Canonical()
			s, ok := streams[key]
			if !ok {
				s = &streamState{
					state:    rt.Decider.Initial(),
					streamID: env.StreamID,
				}
				streams[key] = s
			}
			s.state = rt.Decider.Evolve(s.state, evt)
			s.version = env.Version
			if env.GlobalPosition > cursor {
				cursor = env.GlobalPosition
			}
		}
	}

	// Write each stream's final state via direct cache upserts. We
	// don't go through Store.Append because that path inserts events.
	// Instead the adapter exposes a tx-less UpsertCachedState method
	// via es.StateCacheUpserter (defined just below). Both adapters
	// implement it.
	upserter, ok := any(rb).(es.StateCacheUpserter)
	if !ok {
		return 0, fmt.Errorf("aggregate: adapter does not implement es.StateCacheUpserter")
	}

	written := 0
	for _, s := range streams {
		bs, tu, err := rt.StateCodec.Encode(s.state)
		if err != nil {
			return written, fmt.Errorf("aggregate: encode state for %s: %w",
				s.streamID.Canonical(), err)
		}
		terminal := false
		if rt.Decider.IsTerminal != nil {
			terminal = rt.Decider.IsTerminal(s.state)
		}
		row := es.StateCacheRow{
			TenantID: s.streamID.Tenant,
			StreamID: s.streamID.Canonical(),
			TypeURL:  tu,
			State:    bs,
			Version:  s.version,
			Terminal: terminal,
		}
		if err := upserter.UpsertCachedState(ctx, row); err != nil {
			return written, fmt.Errorf("aggregate: upsert state for %s: %w",
				s.streamID.Canonical(), err)
		}
		written++
	}
	return written, nil
}
