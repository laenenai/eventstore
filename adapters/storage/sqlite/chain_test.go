package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/es"
)

// End-to-end tamper-evident chain (ADR 0028) against the live SQLite
// adapter — proves the adapter populates Hash + PrevHash on Append,
// reads them back on ReadStream, and that VerifyStreamChain rejects
// in-place row mutation.

func newAdapterForChain(t *testing.T) (*sqliteadapter.Adapter, *sql.DB) {
	t.Helper()
	d, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	a := sqliteadapter.New(d)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return a, d
}

func mustSIDForChain(t *testing.T, tenant, typ, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID(tenant, typ, id)
	if err != nil {
		t.Fatalf("NewStreamID: %v", err)
	}
	return sid
}

func mkEvent(payload byte) es.EventToAppend {
	return es.EventToAppend{
		EventID:       uuid.New(),
		TypeURL:       "myapp.test.v1.Thing",
		SchemaVersion: 1,
		OccurredAt:    time.Now().UTC(),
		Actor:         es.Actor{Kind: es.ActorUser, Principal: "alice"},
		Payload:       []byte{payload, 0xab, 0xcd},
	}
}

func TestSQLite_ChainOnAppend_GenesisAndExtension(t *testing.T) {
	a, _ := newAdapterForChain(t)
	ctx := context.Background()
	sid := mustSIDForChain(t, "t-1", "thing", "x")

	// First Append — genesis event. prev_hash should be ZeroHash.
	if _, err := a.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventToAppend{mkEvent(0x01)},
	}); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	envs, err := a.ReadStream(ctx, sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("got %d envelopes, want 1", len(envs))
	}
	if string(envs[0].PrevHash) != string(es.ZeroHash) {
		t.Errorf("genesis prev_hash should be ZeroHash, got %x", envs[0].PrevHash)
	}
	if len(envs[0].Hash) != 32 {
		t.Errorf("genesis hash length: got %d want 32", len(envs[0].Hash))
	}
	genesisHash := envs[0].Hash

	// Second Append — version 2. prev_hash should be the genesis hash.
	if _, err := a.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Events:          []es.EventToAppend{mkEvent(0x02)},
	}); err != nil {
		t.Fatalf("second Append: %v", err)
	}

	envs, _ = a.ReadStream(ctx, sid, 0)
	if len(envs) != 2 {
		t.Fatalf("got %d envelopes, want 2", len(envs))
	}
	if string(envs[1].PrevHash) != string(genesisHash) {
		t.Errorf("v2 prev_hash should equal genesis hash; got %x want %x",
			envs[1].PrevHash, genesisHash)
	}
}

func TestSQLite_ChainOnAppend_MultiEventBatch(t *testing.T) {
	a, _ := newAdapterForChain(t)
	ctx := context.Background()
	sid := mustSIDForChain(t, "t-2", "thing", "y")

	// Three events in one Append — chain inside the batch.
	if _, err := a.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventToAppend{
			mkEvent(0x01), mkEvent(0x02), mkEvent(0x03),
		},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	envs, _ := a.ReadStream(ctx, sid, 0)
	if len(envs) != 3 {
		t.Fatalf("got %d, want 3", len(envs))
	}
	if string(envs[0].PrevHash) != string(es.ZeroHash) {
		t.Error("event 1 prev_hash should be ZeroHash")
	}
	if string(envs[1].PrevHash) != string(envs[0].Hash) {
		t.Error("event 2 prev_hash should equal event 1 hash")
	}
	if string(envs[2].PrevHash) != string(envs[1].Hash) {
		t.Error("event 3 prev_hash should equal event 2 hash")
	}
}

func TestSQLite_VerifyStreamChain_HappyPath(t *testing.T) {
	a, _ := newAdapterForChain(t)
	ctx := context.Background()
	sid := mustSIDForChain(t, "t-3", "thing", "z")

	for i := byte(1); i <= 5; i++ {
		if _, err := a.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i) - 1,
			Events:          []es.EventToAppend{mkEvent(i)},
		}); err != nil {
			t.Fatalf("Append v%d: %v", i, err)
		}
	}
	if err := es.VerifyStreamChain(ctx, a, sid); err != nil {
		t.Errorf("VerifyStreamChain over 5 events: %v", err)
	}
}

func TestSQLite_VerifyStreamChain_DetectsInPlaceMutation(t *testing.T) {
	a, d := newAdapterForChain(t)
	ctx := context.Background()
	sid := mustSIDForChain(t, "t-4", "thing", "m")

	for i := byte(1); i <= 3; i++ {
		if _, err := a.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i) - 1,
			Events:          []es.EventToAppend{mkEvent(i)},
		}); err != nil {
			t.Fatalf("Append v%d: %v", i, err)
		}
	}

	// Verify clean state.
	if err := es.VerifyStreamChain(ctx, a, sid); err != nil {
		t.Fatalf("pre-tamper verify: %v", err)
	}

	// Tamper: flip a byte of payload directly in the table, simulating a
	// malicious operator with DB access.
	res, err := d.ExecContext(ctx,
		`UPDATE events SET payload = X'ff' WHERE tenant_id = ? AND stream_id = ? AND version = 2`,
		sid.Tenant, sid.Canonical())
	if err != nil {
		t.Fatalf("tamper UPDATE: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("tamper UPDATE affected %d rows, want 1", n)
	}

	err = es.VerifyStreamChain(ctx, a, sid)
	if !errors.Is(err, es.ErrChainBroken) {
		t.Fatalf("want ErrChainBroken after payload tamper, got %v", err)
	}
}

func TestSQLite_VerifyStreamChain_EmptyStream(t *testing.T) {
	a, _ := newAdapterForChain(t)
	ctx := context.Background()
	sid := mustSIDForChain(t, "t-5", "thing", "empty")
	if err := es.VerifyStreamChain(ctx, a, sid); err != nil {
		t.Errorf("verify of empty stream: %v", err)
	}
}
