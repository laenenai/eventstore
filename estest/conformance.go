// Package estest provides shared test machinery for the framework.
//
// The conformance suite (RunStoreConformance) exercises the public
// es.Store contract identically against every adapter. Adapter test
// files in adapters/storage/{postgres,sqlite}/adapter_test.go call
// RunStoreConformance after wiring their adapter; both must pass the
// same suite for the framework to consider an adapter compliant.
package estest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// StoreSetup returns an es.Store ready for use. The store may be
// shared across subtests; tests use unique tenant identifiers for
// isolation. Cleanup is the caller's responsibility (typically via
// t.Cleanup in the adapter's test entrypoint).
type StoreSetup func() es.Store

// RunStoreConformance runs the full conformance suite against the
// store returned by setup. Subtests are reported as
// TestConformance/<TestName>.
func RunStoreConformance(t *testing.T, setup StoreSetup) {
	t.Helper()
	s := setup()

	t.Run("AppendAndReadStream", func(t *testing.T) { testAppendAndReadStream(t, s) })
	t.Run("OptimisticConcurrency", func(t *testing.T) { testOptimisticConcurrency(t, s) })
	t.Run("UniqueConstraintClaim", func(t *testing.T) { testUniqueConstraintClaim(t, s) })
	t.Run("GlobalPositionMonotonic", func(t *testing.T) { testGlobalPositionMonotonic(t, s) })
	t.Run("ReadStreamFromVersion", func(t *testing.T) { testReadStreamFromVersion(t, s) })
	t.Run("GetEventByID", func(t *testing.T) { testGetEventByID(t, s) })
	t.Run("MultiEventAppend", func(t *testing.T) { testMultiEventAppend(t, s) })
}

// MakeEvent returns a populated EventToAppend for tests.
func MakeEvent(typeURL string) es.EventToAppend {
	return es.EventToAppend{
		EventID:       uuid.New(),
		TypeURL:       typeURL,
		SchemaVersion: 1,
		OccurredAt:    time.Now().UTC().Truncate(time.Microsecond),
		CorrelationID: uuid.New(),
		CausationID:   uuid.New(),
		CommandID:     uuid.New(),
		Actor: es.Actor{
			Kind:      es.ActorUser,
			Principal: "u:test",
		},
		Payload: []byte("test-payload"),
	}
}

// MustStream constructs a validated StreamID, failing the test on error.
func MustStream(t *testing.T, tenant, typ, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID(tenant, typ, id)
	if err != nil {
		t.Fatalf("NewStreamID(%q, %q, %q): %v", tenant, typ, id, err)
	}
	return sid
}

// tenantCounter ensures every conformance subtest uses a unique tenant,
// preventing cross-test interference when the store is shared.
var tenantCounter struct {
	sync.Mutex
	n uint64
}

func freshTenant(prefix string) string {
	tenantCounter.Lock()
	tenantCounter.n++
	n := tenantCounter.n
	tenantCounter.Unlock()
	return fmt.Sprintf("%s-%d", prefix, n)
}

// ----- Conformance tests --------------------------------------------------

func testAppendAndReadStream(t *testing.T, s es.Store) {
	tenant := freshTenant("rw")
	ctx := es.WithTenant(context.Background(), tenant)
	sid := MustStream(t, tenant, "user", "1")

	ev := MakeEvent("myapp.user.v1.UserRegistered")

	result, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{ev},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if result.StartVersion != 1 || result.EndVersion != 1 {
		t.Fatalf("versions: got %d..%d want 1..1", result.StartVersion, result.EndVersion)
	}
	if result.StartGlobalPosition == 0 {
		t.Fatalf("expected non-zero global_position")
	}

	events, err := s.ReadStream(ctx, sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	got := events[0]
	if got.EventID != ev.EventID {
		t.Errorf("EventID mismatch: got %v want %v", got.EventID, ev.EventID)
	}
	if got.Version != 1 {
		t.Errorf("Version: got %d want 1", got.Version)
	}
	if got.GlobalPosition != result.StartGlobalPosition {
		t.Errorf("GlobalPosition mismatch in read: got %d want %d",
			got.GlobalPosition, result.StartGlobalPosition)
	}
	if got.TypeURL != ev.TypeURL {
		t.Errorf("TypeURL: got %q want %q", got.TypeURL, ev.TypeURL)
	}
	if got.Actor.Principal != "u:test" {
		t.Errorf("Actor.Principal: got %q want %q", got.Actor.Principal, "u:test")
	}
	if string(got.Payload) != "test-payload" {
		t.Errorf("Payload mismatch")
	}
	if got.StreamID.Type != "user" || got.StreamID.ID != "1" {
		t.Errorf("StreamID: got %+v want type=user id=1", got.StreamID)
	}
}

func testOptimisticConcurrency(t *testing.T, s es.Store) {
	tenant := freshTenant("oc")
	ctx := es.WithTenant(context.Background(), tenant)
	sid := MustStream(t, tenant, "user", "1")

	// Seed.
	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{MakeEvent("Initial")},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// First write at v=2 succeeds.
	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Events:          []es.EventToAppend{MakeEvent("First")},
	}); err != nil {
		t.Fatalf("first concurrent append: %v", err)
	}

	// Second write also expecting v=1: conflict.
	_, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Events:          []es.EventToAppend{MakeEvent("Second")},
	})
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	events, err := s.ReadStream(ctx, sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func testUniqueConstraintClaim(t *testing.T, s es.Store) {
	tenant := freshTenant("uc")
	ctx := es.WithTenant(context.Background(), tenant)
	sid1 := MustStream(t, tenant, "user", "1")
	sid2 := MustStream(t, tenant, "user", "2")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid1,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{MakeEvent("Created")},
		Constraints: []es.ConstraintOp{
			{Op: es.ClaimConstraint, Scope: "email", Value: "a@example.com"},
		},
	}); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	_, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid2,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{MakeEvent("Created")},
		Constraints: []es.ConstraintOp{
			{Op: es.ClaimConstraint, Scope: "email", Value: "a@example.com"},
		},
	})
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("expected ErrConstraintViolated, got %v", err)
	}

	events, err := s.ReadStream(ctx, sid2, 0)
	if err != nil {
		t.Fatalf("ReadStream sid2: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events on rejected stream, got %d", len(events))
	}
}

func testGlobalPositionMonotonic(t *testing.T, s es.Store) {
	tenants := []string{freshTenant("mono-a"), freshTenant("mono-b")}
	var last uint64

	for i := 0; i < 5; i++ {
		for _, tenant := range tenants {
			ctx := es.WithTenant(context.Background(), tenant)
			sid := MustStream(t, tenant, "user", fmt.Sprintf("u%d", i))
			result, err := s.Append(ctx, es.AppendParams{
				StreamID:        sid,
				ExpectedVersion: 0,
				Events:          []es.EventToAppend{MakeEvent("Created")},
			})
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			if result.StartGlobalPosition <= last {
				t.Fatalf("global_position not monotonic: got %d, last %d",
					result.StartGlobalPosition, last)
			}
			last = result.StartGlobalPosition
		}
	}
}

func testReadStreamFromVersion(t *testing.T, s es.Store) {
	tenant := freshTenant("paginate")
	ctx := es.WithTenant(context.Background(), tenant)
	sid := MustStream(t, tenant, "user", "1")

	for i := 0; i < 3; i++ {
		if _, err := s.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i),
			Events:          []es.EventToAppend{MakeEvent("Event")},
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	events, err := s.ReadStream(ctx, sid, 2)
	if err != nil {
		t.Fatalf("ReadStream from v=2: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event after v=2, got %d", len(events))
	}
	if events[0].Version != 3 {
		t.Errorf("Version: got %d want 3", events[0].Version)
	}

	v, err := s.CurrentStreamVersion(ctx, sid)
	if err != nil {
		t.Fatalf("CurrentStreamVersion: %v", err)
	}
	if v != 3 {
		t.Errorf("CurrentStreamVersion: got %d want 3", v)
	}
}

func testGetEventByID(t *testing.T, s es.Store) {
	tenant := freshTenant("byid")
	ctx := es.WithTenant(context.Background(), tenant)
	sid := MustStream(t, tenant, "user", "1")

	ev := MakeEvent("Created")
	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{ev},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := s.GetEventByID(ctx, tenant, ev.EventID)
	if err != nil {
		t.Fatalf("GetEventByID: %v", err)
	}
	if got.EventID != ev.EventID {
		t.Errorf("EventID mismatch")
	}

	_, err = s.GetEventByID(ctx, tenant, uuid.New())
	if !errors.Is(err, es.ErrEventNotFound) {
		t.Fatalf("expected ErrEventNotFound, got %v", err)
	}
}

// testMultiEventAppend verifies a single Append committing N events
// produces consecutive versions and consecutive global positions.
func testMultiEventAppend(t *testing.T, s es.Store) {
	tenant := freshTenant("multi")
	ctx := es.WithTenant(context.Background(), tenant)
	sid := MustStream(t, tenant, "user", "1")

	events := []es.EventToAppend{
		MakeEvent("First"),
		MakeEvent("Second"),
		MakeEvent("Third"),
	}
	result, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          events,
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if result.StartVersion != 1 || result.EndVersion != 3 {
		t.Errorf("versions: got %d..%d want 1..3", result.StartVersion, result.EndVersion)
	}
	gap := result.EndGlobalPosition - result.StartGlobalPosition
	if gap != 2 {
		t.Errorf("global_position gap: got %d want 2", gap)
	}

	stored, err := s.ReadStream(ctx, sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(stored) != 3 {
		t.Fatalf("expected 3 events, got %d", len(stored))
	}
	for i, ev := range stored {
		if ev.Version != uint64(i+1) {
			t.Errorf("event %d Version: got %d want %d", i, ev.Version, i+1)
		}
	}
}
