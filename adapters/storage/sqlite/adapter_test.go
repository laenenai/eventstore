package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite" // pure-Go SQLite driver

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/es"
)

// Tests run against modernc.org/sqlite (pure Go, no CGO). Each test
// uses an isolated in-process file-backed DB so the AUTOINCREMENT
// counter starts at zero and tenant rows don't bleed across tests.

func newAdapter(t *testing.T) *sqliteadapter.Adapter {
	t.Helper()
	dir := t.TempDir()
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)",
		filepath.Join(dir, "test.db"))

	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	a := sqliteadapter.New(sqlDB)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return a
}

func makeEvent(typeURL string) es.EventToAppend {
	return es.EventToAppend{
		EventID:       uuid.New(),
		TypeURL:       typeURL,
		SchemaVersion: 1,
		OccurredAt:    time.Now().UTC().Truncate(time.Millisecond),
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

func TestAppendAndReadStream(t *testing.T) {
	a := newAdapter(t)
	ctx := es.WithTenant(context.Background(), "t-rw")
	sid := mustStream(t, "t-rw", "user", "1")

	ev := makeEvent("myapp.user.v1.UserRegistered")

	result, err := a.Append(ctx, es.AppendParams{
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

	events, err := a.ReadStream(ctx, sid, 0)
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
		t.Errorf("Payload mismatch")
	}
	if got.StreamID.Type != "user" || got.StreamID.ID != "1" {
		t.Errorf("StreamID: got %+v want type=user id=1", got.StreamID)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	a := newAdapter(t)
	ctx := es.WithTenant(context.Background(), "t-oc")
	sid := mustStream(t, "t-oc", "user", "1")

	// Seed with one event at version 1.
	_, err := a.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{makeEvent("Initial")},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// First writer succeeds at version 2.
	_, err = a.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Events:          []es.EventToAppend{makeEvent("First")},
	})
	if err != nil {
		t.Fatalf("first concurrent append: %v", err)
	}

	// Second writer believes the stream is still at version 1; conflict.
	_, err = a.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Events:          []es.EventToAppend{makeEvent("Second")},
	})
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	events, err := a.ReadStream(ctx, sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestUniqueConstraintClaim(t *testing.T) {
	a := newAdapter(t)
	ctx := es.WithTenant(context.Background(), "t-uc")
	sid1 := mustStream(t, "t-uc", "user", "1")
	sid2 := mustStream(t, "t-uc", "user", "2")

	_, err := a.Append(ctx, es.AppendParams{
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

	_, err = a.Append(ctx, es.AppendParams{
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

	events, err := a.ReadStream(ctx, sid2, 0)
	if err != nil {
		t.Fatalf("ReadStream sid2: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events on rejected stream, got %d", len(events))
	}
}

func TestGlobalPositionMonotonic(t *testing.T) {
	a := newAdapter(t)
	tenants := []string{"t-mono-a", "t-mono-b"}
	var lastPosition uint64

	for i := 0; i < 5; i++ {
		for _, tenant := range tenants {
			ctx := es.WithTenant(context.Background(), tenant)
			sid := mustStream(t, tenant, "user", fmt.Sprintf("u%d", i))
			result, err := a.Append(ctx, es.AppendParams{
				StreamID:        sid,
				ExpectedVersion: 0,
				Events:          []es.EventToAppend{makeEvent("Created")},
			})
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			if result.StartGlobalPosition <= lastPosition {
				t.Fatalf("global_position not monotonic: got %d, last %d",
					result.StartGlobalPosition, lastPosition)
			}
			lastPosition = result.StartGlobalPosition
		}
	}
}

func TestReadStreamFromVersion(t *testing.T) {
	a := newAdapter(t)
	ctx := es.WithTenant(context.Background(), "t-paginate")
	sid := mustStream(t, "t-paginate", "user", "1")

	for i := 0; i < 3; i++ {
		_, err := a.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i),
			Events:          []es.EventToAppend{makeEvent("Event")},
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	events, err := a.ReadStream(ctx, sid, 2)
	if err != nil {
		t.Fatalf("ReadStream from v=2: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event after v=2, got %d", len(events))
	}
	if events[0].Version != 3 {
		t.Errorf("Version: got %d want 3", events[0].Version)
	}

	v, err := a.CurrentStreamVersion(ctx, sid)
	if err != nil {
		t.Fatalf("CurrentStreamVersion: %v", err)
	}
	if v != 3 {
		t.Errorf("CurrentStreamVersion: got %d want 3", v)
	}
}

func TestGetEventByID(t *testing.T) {
	a := newAdapter(t)
	ctx := es.WithTenant(context.Background(), "t-byid")
	sid := mustStream(t, "t-byid", "user", "1")

	ev := makeEvent("Created")
	_, err := a.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{ev},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := a.GetEventByID(ctx, "t-byid", ev.EventID)
	if err != nil {
		t.Fatalf("GetEventByID: %v", err)
	}
	if got.EventID != ev.EventID {
		t.Errorf("EventID mismatch")
	}

	_, err = a.GetEventByID(ctx, "t-byid", uuid.New())
	if !errors.Is(err, es.ErrEventNotFound) {
		t.Fatalf("expected ErrEventNotFound, got %v", err)
	}
}
