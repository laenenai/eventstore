package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
)

// Snapshot tests cover the lazy-write + read-uses-snapshot cycle and
// the strict StateSchemaVersion invalidation (ADR 0011).

func newSnapshotRuntime(t *testing.T, every int) (*sqliteadapter.Adapter, *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	rt := &aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Store:              a,
		Decider:            counterProtoDecider,
		Codec:              counterv1.EventCodec{},
		StateCodec:         aggregate.ProtoStateCodec[*counterv1.Counter]{},
		StateSchemaVersion: 1,
		SnapshotEvery:      every,
	}
	return a, rt
}

// TestSnapshot_WrittenAfterThreshold verifies a snapshot row appears
// after Load runs through enough events.
func TestSnapshot_WrittenAfterThreshold(t *testing.T) {
	a, rt := newSnapshotRuntime(t, 5)
	tenant := "t-snap-thr"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	// Init + 6 increments = version 7. SnapshotEvery=5 → first Load
	// after enough writes should write a snapshot.
	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("init: %v", err)
	}
	for i := 0; i < 6; i++ {
		if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
			t.Fatalf("inc %d: %v", i, err)
		}
	}

	// Loading the stream should trigger snapshot write (version >= 5).
	state, version, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if version != 7 || state.Count != 6 {
		t.Errorf("state: version=%d count=%d want 7,6", version, state.Count)
	}

	// Snapshot is written by Load *before* the append in Handle, so
	// the latest snapshot is at version 5 (written when version
	// reached 5 during the 5th Handle; SnapshotEvery=5 fires once,
	// next would be at 10). That's the correct ADR 0011 cadence:
	// snapshots are a cache, not data — the version they capture is
	// "loaded before this handle" not "after this handle".
	snap, err := a.LoadSnapshot(context.Background(), tenant, sid.Canonical())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.Version != 5 {
		t.Errorf("snapshot version: got %d want 5", snap.Version)
	}
	if snap.StateSchemaVersion != 1 {
		t.Errorf("snapshot schema version: got %d want 1", snap.StateSchemaVersion)
	}
}

// TestSnapshot_LoadUsesSnapshot verifies that with a snapshot present,
// Load returns the correct state even after we hide events older
// than the snapshot. Approximation: after a snapshot is written, we
// delete the snapshot's underlying events from the events table to
// prove the snapshot alone is sufficient — except we can't safely
// mutate events from the adapter, so instead we use a sentinel
// approach: install a snapshot manually, point it at a version
// beyond the actual stream's tail, and verify Load returns the
// snapshot's state.
func TestSnapshot_LoadUsesSnapshot(t *testing.T) {
	a, rt := newSnapshotRuntime(t, 5)
	tenant := "t-snap-use"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 42}); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Manually install a snapshot at version 1 with count=999. Load
	// should pick it up and (since no events exist past version 1)
	// return count=999.
	codec := aggregate.ProtoStateCodec[*counterv1.Counter]{}
	bs, _, err := codec.Encode(&counterv1.Counter{Initialized: true, Min: 0, Max: 100, Count: 999})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := a.SaveSnapshot(context.Background(), es.Snapshot{
		TenantID:           tenant,
		StreamID:           sid.Canonical(),
		Version:            1,
		StateSchemaVersion: 1,
		State:              bs,
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	state, version, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if version != 1 {
		t.Errorf("version: got %d want 1", version)
	}
	if state.Count != 999 {
		t.Errorf("count: got %d want 999 (should come from snapshot)", state.Count)
	}
}

// TestSnapshot_SchemaMismatchDiscards verifies that a snapshot with a
// stale StateSchemaVersion is silently ignored — full replay reconstructs
// state correctly from events.
func TestSnapshot_SchemaMismatchDiscards(t *testing.T) {
	a, rt := newSnapshotRuntime(t, 5)
	tenant := "t-snap-mismatch"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 10}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 5}); err != nil {
		t.Fatalf("inc: %v", err)
	}

	// Install a snapshot with the WRONG schema version + garbage count.
	codec := aggregate.ProtoStateCodec[*counterv1.Counter]{}
	bs, _, _ := codec.Encode(&counterv1.Counter{Count: 777})
	if err := a.SaveSnapshot(context.Background(), es.Snapshot{
		TenantID:           tenant,
		StreamID:           sid.Canonical(),
		Version:            2,
		StateSchemaVersion: 99, // mismatch — runtime expects 1
		State:              bs,
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	state, version, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if version != 2 || state.Count != 15 {
		t.Errorf("state: version=%d count=%d want 2,15 (snapshot discarded, full replay used)",
			version, state.Count)
	}
}

// TestSnapshot_DisabledWhenEveryIsZero verifies no snapshot side effect
// when SnapshotEvery is unset.
func TestSnapshot_DisabledWhenEveryIsZero(t *testing.T) {
	a, rt := newSnapshotRuntime(t, 0) // disabled
	tenant := "t-snap-off"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("init: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
			t.Fatalf("inc %d: %v", i, err)
		}
	}
	if _, _, err := rt.Load(context.Background(), sid); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if _, err := a.LoadSnapshot(context.Background(), tenant, sid.Canonical()); err == nil {
		t.Errorf("expected ErrSnapshotNotFound when snapshots disabled")
	}
}
