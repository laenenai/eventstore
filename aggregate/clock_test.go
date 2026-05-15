package aggregate_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
)

// fakeStore is a minimal in-memory es.Store for Clock testing. It
// captures the OccurredAt the runtime stamps so the test can assert
// it tracks the runtime's Clock.
type fakeStore struct {
	captured []es.EventToAppend
	streams  map[string][]es.Envelope
}

func newFakeStore() *fakeStore {
	return &fakeStore{streams: map[string][]es.Envelope{}}
}

func (s *fakeStore) Append(_ context.Context, p es.AppendParams) (es.AppendResult, error) {
	s.captured = append(s.captured, p.Events...)
	key := p.StreamID.Canonical()
	startVersion := p.ExpectedVersion + 1
	for i, ev := range p.Events {
		s.streams[key] = append(s.streams[key], es.Envelope{
			EventID:       ev.EventID,
			TenantID:      p.StreamID.Tenant,
			StreamID:      p.StreamID,
			Version:       startVersion + uint64(i),
			TypeURL:       ev.TypeURL,
			SchemaVersion: ev.SchemaVersion,
			OccurredAt:    ev.OccurredAt,
			RecordedAt:    ev.OccurredAt,
			CorrelationID: ev.CorrelationID,
			CausationID:   ev.CausationID,
			CommandID:     ev.CommandID,
			Actor:         ev.Actor,
			Payload:       ev.Payload,
		})
	}
	return es.AppendResult{
		StartVersion: startVersion,
		EndVersion:   startVersion + uint64(len(p.Events)) - 1,
	}, nil
}

func (s *fakeStore) ReadStream(_ context.Context, sid es.StreamID, fromVersion uint64) ([]es.Envelope, error) {
	envs := s.streams[sid.Canonical()]
	if fromVersion == 0 {
		return envs, nil
	}
	var out []es.Envelope
	for _, e := range envs {
		if e.Version > fromVersion {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *fakeStore) ReadAll(_ context.Context, _ uint64, _ int) ([]es.Envelope, error) {
	return nil, nil
}
func (s *fakeStore) ReadAllForTenant(_ context.Context, _ string, _ uint64, _ int) ([]es.Envelope, error) {
	return nil, nil
}
func (s *fakeStore) CurrentStreamVersion(_ context.Context, sid es.StreamID) (uint64, error) {
	envs := s.streams[sid.Canonical()]
	if len(envs) == 0 {
		return 0, nil
	}
	return envs[len(envs)-1].Version, nil
}
func (s *fakeStore) GetEventByID(_ context.Context, _ string, _ uuid.UUID) (es.Envelope, error) {
	return es.Envelope{}, es.ErrEventNotFound
}

// fakeCodec is a no-op event codec that round-trips opaque bytes.
type fakeCodec struct{}

type stringEvent string

func (fakeCodec) Encode(e stringEvent) (aggregate.EncodedEvent, error) {
	return aggregate.EncodedEvent{
		Payload:       []byte(e),
		TypeURL:       "test.fake/v1.StringEvent",
		SchemaVersion: 1,
	}, nil
}

func (fakeCodec) Decode(_ string, _ uint32, p []byte) (stringEvent, error) {
	return stringEvent(p), nil
}

// stringCmd is the test command. The decider always emits one event.
type stringCmd struct{ value string }

func newRuntime(c es.Clock) *aggregate.Runtime[int, stringCmd, stringEvent] {
	return &aggregate.Runtime[int, stringCmd, stringEvent]{
		Store: newFakeStore(),
		Decider: es.Decider[int, stringCmd, stringEvent]{
			Initial: func() int { return 0 },
			Decide: func(_ int, c stringCmd) ([]stringEvent, []es.ConstraintOp, error) {
				return []stringEvent{stringEvent(c.value)}, nil, nil
			},
			Evolve: func(s int, _ stringEvent) int { return s + 1 },
		},
		Codec: fakeCodec{},
		Clock: c,
	}
}

func TestRuntime_Now_DefaultsToRealClock(t *testing.T) {
	rt := &aggregate.Runtime[int, stringCmd, stringEvent]{}
	before := time.Now().UTC().Add(-time.Second)
	got := rt.Now()
	after := time.Now().UTC().Add(time.Second)
	if got.Before(before) || got.After(after) {
		t.Errorf("Runtime.Now() with nil Clock should fall back to RealClock; got %v outside [%v,%v]",
			got, before, after)
	}
	if got.Location() != time.UTC {
		t.Errorf("Runtime.Now() must return UTC, got %s", got.Location())
	}
}

func TestRuntime_Now_UsesManualClock(t *testing.T) {
	fixed := time.Date(2030, 1, 15, 12, 0, 0, 0, time.UTC)
	mc := es.NewManualClock(fixed)
	rt := &aggregate.Runtime[int, stringCmd, stringEvent]{Clock: mc}
	if got := rt.Now(); !got.Equal(fixed) {
		t.Errorf("Runtime.Now() should reflect ManualClock; got %v want %v", got, fixed)
	}
	mc.Advance(48 * time.Hour)
	if got := rt.Now(); !got.Equal(fixed.Add(48 * time.Hour)) {
		t.Errorf("Runtime.Now() should track Advance; got %v", got)
	}
}

func TestRuntime_Handle_StampsOccurredAtFromClock(t *testing.T) {
	// The headline behavior: a test advances the clock, then commands
	// emitted after that point carry the new timestamp on their
	// envelopes. This is what enables exercising expiry windows
	// without sleeping.
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	mc := es.NewManualClock(t0)
	rt := newRuntime(mc)
	store := rt.Store.(*fakeStore)

	ctx := es.WithTenant(context.Background(), "t-clock")
	sid, err := es.NewStreamID("t-clock", "fake", "1")
	if err != nil {
		t.Fatalf("NewStreamID: %v", err)
	}

	if _, err := rt.Handle(ctx, sid, stringCmd{value: "first"}); err != nil {
		t.Fatalf("Handle first: %v", err)
	}
	mc.Advance(30 * 24 * time.Hour) // 30 days
	if _, err := rt.Handle(ctx, sid, stringCmd{value: "second"}); err != nil {
		t.Fatalf("Handle second: %v", err)
	}

	if len(store.captured) != 2 {
		t.Fatalf("captured events: got %d want 2", len(store.captured))
	}
	if !store.captured[0].OccurredAt.Equal(t0) {
		t.Errorf("event[0].OccurredAt: got %v want %v",
			store.captured[0].OccurredAt, t0)
	}
	want1 := t0.Add(30 * 24 * time.Hour)
	if !store.captured[1].OccurredAt.Equal(want1) {
		t.Errorf("event[1].OccurredAt: got %v want %v (30 days after t0)",
			store.captured[1].OccurredAt, want1)
	}
}

func TestRuntime_Handle_WithOccurredAtStillOverrides(t *testing.T) {
	// The Clock provides the default; explicit WithOccurredAt must
	// still win, otherwise the framework would silently override
	// domain-provided timestamps.
	mc := es.NewManualClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	rt := newRuntime(mc)
	store := rt.Store.(*fakeStore)

	ctx := es.WithTenant(context.Background(), "t-override")
	sid, _ := es.NewStreamID("t-override", "fake", "1")

	explicit := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	if _, err := rt.Handle(ctx, sid, stringCmd{value: "x"},
		aggregate.WithOccurredAt(explicit)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !store.captured[0].OccurredAt.Equal(explicit) {
		t.Errorf("WithOccurredAt didn't win: got %v want %v",
			store.captured[0].OccurredAt, explicit)
	}
}
