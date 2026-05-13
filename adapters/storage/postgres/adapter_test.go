package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pgadapter "github.com/laenenai/eventstore/adapters/storage/postgres"
	"github.com/laenenai/eventstore/es"
)

// Tests in this package run against a real Postgres instance via
// testcontainers-go. Docker (or a compatible runtime) must be running
// on the host. CI uses this same setup; the conformance suite exercises
// the public es.Store contract across adapters.

var (
	adapter *pgadapter.Adapter
	pool    *pgxpool.Pool
)

func TestMain(m *testing.M) {
	if os.Getenv("EVENTSTORE_SKIP_PG_TESTS") == "1" {
		fmt.Println("skipping postgres adapter tests (EVENTSTORE_SKIP_PG_TESTS=1)")
		os.Exit(0)
	}

	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("eventstore_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		os.Exit(1)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		os.Exit(1)
	}

	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "pgxpool.New: %v\n", err)
		os.Exit(1)
	}

	adapter = pgadapter.New(pool)
	if err := adapter.Migrate(ctx); err != nil {
		pool.Close()
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	pool.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

// makeEvent returns a populated EventToAppend for tests.
func makeEvent(typeURL string) es.EventToAppend {
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

func mustStream(t *testing.T, tenant, typ, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID(tenant, typ, id)
	if err != nil {
		t.Fatalf("NewStreamID: %v", err)
	}
	return sid
}

// TestAppendAndReadStream verifies the round-trip: append one event,
// read it back via ReadStream, verify all envelope fields.
func TestAppendAndReadStream(t *testing.T) {
	ctx := es.WithTenant(context.Background(), "t-rw")
	sid := mustStream(t, "t-rw", "user", "1")

	ev := makeEvent("myapp.user.v1.UserRegistered")

	result, err := adapter.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{ev},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if result.StartVersion != 1 || result.EndVersion != 1 {
		t.Fatalf("expected versions 1..1, got %d..%d", result.StartVersion, result.EndVersion)
	}
	if result.StartGlobalPosition == 0 {
		t.Fatalf("expected non-zero global position")
	}

	events, err := adapter.ReadStream(ctx, sid, 0)
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
		t.Errorf("GlobalPosition mismatch in read: got %d want %d", got.GlobalPosition, result.StartGlobalPosition)
	}
	if got.TypeURL != ev.TypeURL {
		t.Errorf("TypeURL: got %q want %q", got.TypeURL, ev.TypeURL)
	}
	if got.Actor.Principal != "u:test" {
		t.Errorf("Actor.Principal: got %q want %q", got.Actor.Principal, "u:test")
	}
	if string(got.Payload) != "test-payload" {
		t.Errorf("Payload: got %q want %q", got.Payload, "test-payload")
	}
	if got.StreamID.Type != "user" || got.StreamID.ID != "1" {
		t.Errorf("StreamID: got %+v want type=user id=1", got.StreamID)
	}
}

// TestOptimisticConcurrency verifies two writers racing on the same
// stream: one succeeds, the other gets ErrConflict.
func TestOptimisticConcurrency(t *testing.T) {
	ctx := es.WithTenant(context.Background(), "t-oc")
	sid := mustStream(t, "t-oc", "user", "1")

	// Seed with one event.
	_, err := adapter.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{makeEvent("Initial")},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Two writers both believe the stream is at version 1, try to
	// append version 2 simultaneously. One must win, one must get
	// ErrConflict.
	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := adapter.Append(ctx, es.AppendParams{
				StreamID:        sid,
				ExpectedVersion: 1,
				Events:          []es.EventToAppend{makeEvent("Concurrent")},
			})
			results[idx] = err
		}(i)
	}
	wg.Wait()

	var successes, conflicts int
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, es.ErrConflict):
			conflicts++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected exactly 1 success and 1 conflict, got %d / %d", successes, conflicts)
	}

	// Stream should now have exactly 2 events.
	events, err := adapter.ReadStream(ctx, sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events after race, got %d", len(events))
	}
}

// TestUniqueConstraintClaim verifies that two appends claiming the same
// (scope, value) produce ErrConstraintViolated on the second.
func TestUniqueConstraintClaim(t *testing.T) {
	ctx := es.WithTenant(context.Background(), "t-uc")
	sid1 := mustStream(t, "t-uc", "user", "1")
	sid2 := mustStream(t, "t-uc", "user", "2")

	// First claim succeeds.
	_, err := adapter.Append(ctx, es.AppendParams{
		StreamID:        sid1,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{makeEvent("Created")},
		Constraints: []es.ConstraintOp{
			{Op: es.ClaimConstraint, Scope: "email", Value: "a@example.com"},
		},
	})
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	// Second claim of the same email on a different stream must fail.
	_, err = adapter.Append(ctx, es.AppendParams{
		StreamID:        sid2,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{makeEvent("Created")},
		Constraints: []es.ConstraintOp{
			{Op: es.ClaimConstraint, Scope: "email", Value: "a@example.com"},
		},
	})
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("expected ErrConstraintViolated, got %v", err)
	}

	// sid2 should have no events appended (atomic rollback).
	events, err := adapter.ReadStream(ctx, sid2, 0)
	if err != nil {
		t.Fatalf("ReadStream sid2: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events on rejected stream, got %d", len(events))
	}
}

// TestGlobalPositionMonotonic verifies that global_position increases
// monotonically across multiple appends, even across tenants.
func TestGlobalPositionMonotonic(t *testing.T) {
	tenants := []string{"t-mono-a", "t-mono-b"}
	var lastPosition uint64

	for i := 0; i < 5; i++ {
		for _, tenant := range tenants {
			ctx := es.WithTenant(context.Background(), tenant)
			sid := mustStream(t, tenant, "user", fmt.Sprintf("u%d", i))
			result, err := adapter.Append(ctx, es.AppendParams{
				StreamID:        sid,
				ExpectedVersion: 0,
				Events:          []es.EventToAppend{makeEvent("Created")},
			})
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			if result.StartGlobalPosition <= lastPosition {
				t.Fatalf("global_position not monotonic: got %d, last %d", result.StartGlobalPosition, lastPosition)
			}
			lastPosition = result.StartGlobalPosition
		}
	}
}

// TestReadStreamFromVersion verifies pagination semantics — reading
// from a known version returns only later events.
func TestReadStreamFromVersion(t *testing.T) {
	ctx := es.WithTenant(context.Background(), "t-paginate")
	sid := mustStream(t, "t-paginate", "user", "1")

	// Append 3 events one at a time so the test exercises real version
	// progression rather than batching everything in one Append.
	for i := 0; i < 3; i++ {
		_, err := adapter.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i),
			Events:          []es.EventToAppend{makeEvent("Event")},
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	events, err := adapter.ReadStream(ctx, sid, 2)
	if err != nil {
		t.Fatalf("ReadStream from v=2: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event after v=2, got %d", len(events))
	}
	if events[0].Version != 3 {
		t.Errorf("Version: got %d want 3", events[0].Version)
	}

	v, err := adapter.CurrentStreamVersion(ctx, sid)
	if err != nil {
		t.Fatalf("CurrentStreamVersion: %v", err)
	}
	if v != 3 {
		t.Errorf("CurrentStreamVersion: got %d want 3", v)
	}
}

// TestGetEventByID verifies single-event lookup and the typed
// ErrEventNotFound sentinel.
func TestGetEventByID(t *testing.T) {
	ctx := es.WithTenant(context.Background(), "t-byid")
	sid := mustStream(t, "t-byid", "user", "1")

	ev := makeEvent("Created")
	_, err := adapter.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{ev},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := adapter.GetEventByID(ctx, "t-byid", ev.EventID)
	if err != nil {
		t.Fatalf("GetEventByID: %v", err)
	}
	if got.EventID != ev.EventID {
		t.Errorf("EventID mismatch")
	}

	_, err = adapter.GetEventByID(ctx, "t-byid", uuid.New())
	if !errors.Is(err, es.ErrEventNotFound) {
		t.Fatalf("expected ErrEventNotFound, got %v", err)
	}
}
