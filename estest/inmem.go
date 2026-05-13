package estest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// InMemoryStore is an es.Store implementation that lives entirely in
// process memory. It satisfies the same contract as the real adapters
// and is intended for two uses:
//
//  1. Unit-testing aggregate runtimes, deciders, and codecs without
//     setting up a database.
//  2. As a stand-in store in the inproc EventPublisher for
//     single-process examples.
//
// Concurrency is supported via a single mutex covering the entire
// store. The advisory-lock and PK-conflict semantics are emulated
// faithfully for the surface tested by the conformance suite, but the
// implementation is intentionally simple — not a high-throughput
// scratch implementation.
type InMemoryStore struct {
	mu             sync.Mutex
	events         []es.Envelope
	claims         map[claimKey]string // (tenant, scope, value) -> stream_id
	globalPosition atomic.Uint64
}

type claimKey struct {
	tenant, scope, value string
}

// NewInMemoryStore returns an empty in-memory event store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		claims: make(map[claimKey]string),
	}
}

// Append commits one batch of events plus any constraint operations
// atomically (under the store mutex).
func (s *InMemoryStore) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	if err := p.StreamID.Validate(); err != nil {
		return es.AppendResult{}, err
	}
	if len(p.Events) == 0 {
		return es.AppendResult{}, errors.New("inmem: append requires at least one event")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Apply constraint ops. Failure rolls everything back automatically
	// because we don't mutate state until all checks pass.
	for _, op := range p.Constraints {
		key := claimKey{p.StreamID.Tenant, op.Scope, op.Value}
		switch op.Op {
		case es.ClaimConstraint:
			if _, exists := s.claims[key]; exists {
				return es.AppendResult{}, es.ErrConstraintViolated
			}
		case es.ReleaseConstraint:
			// release is idempotent — deleting a missing key is ok
		}
	}

	// Optimistic-concurrency check: the current stream version must
	// equal ExpectedVersion.
	current := s.currentVersionLocked(p.StreamID)
	if current != p.ExpectedVersion {
		return es.AppendResult{}, es.ErrConflict
	}

	// Commit constraint changes.
	for _, op := range p.Constraints {
		key := claimKey{p.StreamID.Tenant, op.Scope, op.Value}
		switch op.Op {
		case es.ClaimConstraint:
			s.claims[key] = p.StreamID.Canonical()
		case es.ReleaseConstraint:
			delete(s.claims, key)
		}
	}

	now := time.Now().UTC()
	var startPos, endPos uint64
	for i, ev := range p.Events {
		pos := s.globalPosition.Add(1)
		env := es.Envelope{
			EventID:        ev.EventID,
			TenantID:       p.StreamID.Tenant,
			StreamID:       p.StreamID,
			Version:        p.ExpectedVersion + uint64(i) + 1,
			GlobalPosition: pos,
			TypeURL:        ev.TypeURL,
			SchemaVersion:  ev.SchemaVersion,
			OccurredAt:     ev.OccurredAt,
			RecordedAt:     now,
			CorrelationID:  ev.CorrelationID,
			CausationID:    ev.CausationID,
			CommandID:      ev.CommandID,
			Actor:          ev.Actor,
			Payload:        ev.Payload,
			PayloadJSON:    ev.PayloadJSON,
			KeyRefs:        ev.KeyRefs,
		}
		s.events = append(s.events, env)
		if i == 0 {
			startPos = pos
		}
		endPos = pos
	}

	return es.AppendResult{
		StartVersion:        p.ExpectedVersion + 1,
		EndVersion:          p.ExpectedVersion + uint64(len(p.Events)),
		StartGlobalPosition: startPos,
		EndGlobalPosition:   endPos,
		RecordedAt:          now,
	}, nil
}

// ReadStream returns events for a stream with version > fromVersion.
func (s *InMemoryStore) ReadStream(ctx context.Context, sid es.StreamID, fromVersion uint64) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []es.Envelope
	for _, env := range s.events {
		if env.TenantID == sid.Tenant &&
			env.StreamID == sid &&
			env.Version > fromVersion {
			out = append(out, env)
		}
	}
	return out, nil
}

// ReadAll returns events store-wide with global_position > fromPosition.
func (s *InMemoryStore) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.collectByPositionLocked("", fromPosition, limit), nil
}

// ReadAllForTenant is ReadAll scoped to a single tenant.
func (s *InMemoryStore) ReadAllForTenant(ctx context.Context, tenantID string, fromPosition uint64, limit int) ([]es.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.collectByPositionLocked(tenantID, fromPosition, limit), nil
}

// CurrentStreamVersion returns the highest version for a stream, or 0.
func (s *InMemoryStore) CurrentStreamVersion(ctx context.Context, sid es.StreamID) (uint64, error) {
	if err := sid.Validate(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentVersionLocked(sid), nil
}

// GetEventByID returns the event with the given id for the tenant.
func (s *InMemoryStore) GetEventByID(ctx context.Context, tenantID string, eventID uuid.UUID) (es.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, env := range s.events {
		if env.TenantID == tenantID && env.EventID == eventID {
			return env, nil
		}
	}
	return es.Envelope{}, es.ErrEventNotFound
}

// currentVersionLocked must be called with mu held.
func (s *InMemoryStore) currentVersionLocked(sid es.StreamID) uint64 {
	var max uint64
	for _, env := range s.events {
		if env.TenantID == sid.Tenant && env.StreamID == sid && env.Version > max {
			max = env.Version
		}
	}
	return max
}

// collectByPositionLocked must be called with mu held. tenant="" means
// all tenants.
func (s *InMemoryStore) collectByPositionLocked(tenant string, fromPosition uint64, limit int) []es.Envelope {
	out := make([]es.Envelope, 0)
	for _, env := range s.events {
		if env.GlobalPosition <= fromPosition {
			continue
		}
		if tenant != "" && env.TenantID != tenant {
			continue
		}
		out = append(out, env)
		if len(out) >= limit {
			break
		}
	}
	return out
}

// Ensure InMemoryStore satisfies es.Store at compile time.
var _ es.Store = (*InMemoryStore)(nil)
