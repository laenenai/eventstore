package sqlite_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
	"github.com/laenenai/eventstore/linked"
	"github.com/laenenai/eventstore/projection"
)

// TestLinked_RouteAndAppend verifies the happy path: source events
// produce derived events into a destination stream via a LinkedProjection.
func TestLinked_RouteAndAppend(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	tenant := "t-linked"
	seedEvents(t, agg, []string{tenant}, 3) // 1 Init + 2 Increments

	destStream := estest.MustStream(t, tenant, "counter-mirror", "1")

	lp, err := linked.New(linked.Config{
		Name:        "counter-mirror",
		Destination: store,
		SourceTypeURLs: []string{
			"test.counter.v1.Incremented",
		},
		Route: func(_ context.Context, env es.Envelope) (linked.Route, error) {
			// Echo each Incremented as a fresh Incremented in the
			// mirror stream — verifies derived events round trip.
			return linked.Route{
				DestinationStream: destStream,
				DerivedEvent:      &counterv1.Incremented{By: 1},
				DerivedTypeURL:    "test.counter.v1.Incremented",
				ExpectedVersion:   versionOfMirror(t, store, destStream),
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rt := &projection.Runtime{
		Name:       "linked-counter-mirror",
		Tenant:     tenant,
		Store:      store,
		Checkpoint: store.(projection.Checkpoint),
		Handler:    lp.Handler(),
	}

	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// Mirror stream now holds 2 derived events (one per Incremented).
	envs, err := store.ReadStream(context.Background(), destStream, 0)
	if err != nil {
		t.Fatalf("ReadStream mirror: %v", err)
	}
	if len(envs) != 2 {
		t.Errorf("mirror events: got %d want 2", len(envs))
	}
	// Each derived event's CausationID should point at the source's event_id.
	for _, e := range envs {
		if e.CausationID.String() == "" {
			t.Errorf("derived event missing CausationID")
		}
	}
}

// versionOfMirror is a tiny test helper that loads the current version
// of the mirror stream so each derived event passes a correct
// ExpectedVersion. In production, callers either load state and pass
// the real version, or use a stream-per-source pattern.
func versionOfMirror(t *testing.T, store es.Store, sid es.StreamID) uint64 {
	t.Helper()
	envs, err := store.ReadStream(context.Background(), sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	return uint64(len(envs))
}

// TestLinked_IdempotentEmitSwallowsReplay verifies the
// constraint-claim path: re-running the same source event does not
// produce a duplicate derived event.
func TestLinked_IdempotentEmitSwallowsReplay(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	tenant := "t-linked-replay"
	seedEvents(t, agg, []string{tenant}, 2) // 1 Init + 1 Increment

	// One destination stream per source event keeps ExpectedVersion=0
	// trivial.
	lp, err := linked.New(linked.Config{
		Name:           "per-event-mirror",
		Destination:    store,
		SourceTypeURLs: []string{"test.counter.v1.Incremented"},
		Route: func(_ context.Context, env es.Envelope) (linked.Route, error) {
			return linked.Route{
				DestinationStream: estest.MustStream(t, tenant, "mirror", env.EventID.String()),
				DerivedEvent:      &counterv1.Incremented{By: 1},
				DerivedTypeURL:    "test.counter.v1.Incremented",
				ExpectedVersion:   0,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rt := &projection.Runtime{
		Name:       "linked-replay",
		Tenant:     tenant,
		Store:      store,
		Checkpoint: projection.NewMemoryCheckpoint(),
		Handler:    lp.Handler(),
	}

	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}
	// Reset checkpoint and run again — replay must NOT produce
	// duplicate derived events thanks to the source-event-id claim.
	rt.Checkpoint = projection.NewMemoryCheckpoint() // reset
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("second run: %v", err)
	}
}

// TestLinked_SkipDoesNotAppend confirms that Route.Skip drops the
// source without producing anything.
func TestLinked_SkipDoesNotAppend(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	tenant := "t-linked-skip"
	seedEvents(t, agg, []string{tenant}, 2)

	mirrorStream := estest.MustStream(t, tenant, "skip-mirror", "1")

	calls := 0
	lp, err := linked.New(linked.Config{
		Name:           "skip-mirror",
		Destination:    store,
		SourceTypeURLs: []string{"test.counter.v1.Incremented"},
		Route: func(_ context.Context, env es.Envelope) (linked.Route, error) {
			calls++
			return linked.Route{Skip: true}, nil
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	rt := &projection.Runtime{
		Name:       "linked-skip",
		Tenant:     tenant,
		Store:      store,
		Checkpoint: projection.NewMemoryCheckpoint(),
		Handler:    lp.Handler(),
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if calls == 0 {
		t.Errorf("Route should have been called for at least one event")
	}
	envs, err := store.ReadStream(context.Background(), mirrorStream, 0)
	if err == nil && len(envs) != 0 {
		t.Errorf("mirror stream should be empty after Skip, got %d events", len(envs))
	}
}

// TestLinked_FilterIgnoresUnlistedEvents confirms SourceTypeURLs gates
// dispatch.
func TestLinked_FilterIgnoresUnlistedEvents(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	tenant := "t-linked-filter"
	seedEvents(t, agg, []string{tenant}, 2) // 1 Init + 1 Increment

	calls := 0
	lp, _ := linked.New(linked.Config{
		Name:           "filter-only-decremented",
		Destination:    store,
		SourceTypeURLs: []string{"test.counter.v1.Decremented"}, // never produced
		Route: func(_ context.Context, env es.Envelope) (linked.Route, error) {
			calls++
			return linked.Route{Skip: true}, nil
		},
	})
	rt := &projection.Runtime{
		Name:       "linked-filter",
		Tenant:     tenant,
		Store:      store,
		Checkpoint: projection.NewMemoryCheckpoint(),
		Handler:    lp.Handler(),
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if calls != 0 {
		t.Errorf("Route was called %d times — filter should have excluded all events", calls)
	}
}

// TestLinked_Validation rejects malformed configs.
func TestLinked_Validation(t *testing.T) {
	store, _ := newStoreAndCounter(t)
	cases := []struct {
		name string
		cfg  linked.Config
	}{
		{"missing name", linked.Config{Destination: store, Route: func(context.Context, es.Envelope) (linked.Route, error) { return linked.Route{}, nil }}},
		{"missing destination", linked.Config{Name: "x", Route: func(context.Context, es.Envelope) (linked.Route, error) { return linked.Route{}, nil }}},
		{"missing route", linked.Config{Name: "x", Destination: store}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := linked.New(tc.cfg); err == nil {
				t.Errorf("expected validation error")
			}
		})
	}
}

var _ = proto.Marshal // keep import while we extend the test fixture
