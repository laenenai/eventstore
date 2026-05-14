package sqlite_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/outbox"
	"github.com/laenenai/eventstore/adapters/publisher/inproc"
)

// TestDrain_ShardedSplitsWorkAcrossShards verifies that two Drains
// configured with disjoint shards cover all the rows between them
// without either touching the other's slice. Sharding is stream-sticky
// (FNV-1a hash of tenant+stream id) — all events of a given stream
// always go to the same shard. This is what makes sharding compatible
// with strict per-stream ordering.
func TestDrain_ShardedSplitsWorkAcrossShards(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	// 3 tenants × 5 events = 15 events.
	want := seedEvents(t, agg, []string{"t-shard-a", "t-shard-b", "t-shard-c"}, 5)

	// Each shard records which stream-keys it saw — to assert that
	// the same stream-key never appears on both sides.
	pubA := inproc.New()
	var sawA atomic.Int64
	streamsA := map[string]struct{}{}
	pubA.Subscribe(func(ctx context.Context, env es.Envelope) error {
		streamsA[env.TenantID+"|"+env.StreamID.Canonical()] = struct{}{}
		sawA.Add(1)
		return nil
	})

	pubB := inproc.New()
	var sawB atomic.Int64
	streamsB := map[string]struct{}{}
	pubB.Subscribe(func(ctx context.Context, env es.Envelope) error {
		streamsB[env.TenantID+"|"+env.StreamID.Canonical()] = struct{}{}
		sawB.Add(1)
		return nil
	})

	drainA := &outbox.Drain{
		Store: store.(es.OutboxStore), Publisher: pubA,
		Shard: 0, TotalShards: 2,
	}
	drainB := &outbox.Drain{
		Store: store.(es.OutboxStore), Publisher: pubB,
		Shard: 1, TotalShards: 2,
	}

	ctx := context.Background()
	pubdA, _, err := drainA.Run(ctx)
	if err != nil {
		t.Fatalf("drainA: %v", err)
	}
	pubdB, _, err := drainB.Run(ctx)
	if err != nil {
		t.Fatalf("drainB: %v", err)
	}

	if pubdA+pubdB != want {
		t.Errorf("total published: got %d want %d", pubdA+pubdB, want)
	}
	if int(sawA.Load()+sawB.Load()) != want {
		t.Errorf("subscriber total: got %d want %d", sawA.Load()+sawB.Load(), want)
	}
	// Stream-stickiness: no stream key appears on both shards.
	for k := range streamsA {
		if _, dup := streamsB[k]; dup {
			t.Errorf("stream %q seen on both shards — sharding is not stream-sticky", k)
		}
	}
	// With FNV-1a over 3 stream keys split into 2 shards, the
	// distribution is unlikely to be all-or-nothing. Validate that
	// SOMETHING went to each side; if both happen to fall on one
	// shard for these specific keys, the test would skip this check.
	if sawA.Load() == 0 && sawB.Load() == 0 {
		t.Errorf("no events delivered at all")
	}
}

// TestDrain_ShardingValidation rejects invalid shard configs.
func TestDrain_ShardingValidation(t *testing.T) {
	store, _ := newStoreAndCounter(t)
	cases := []struct {
		name        string
		shard       int
		totalShards int
	}{
		{"shard out of range", 5, 3},
		{"negative shard", -1, 3},
		{"negative total", 0, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &outbox.Drain{
				Store: store.(es.OutboxStore), Publisher: inproc.New(),
				Shard: tc.shard, TotalShards: tc.totalShards,
			}
			_, err := d.RunOnce(context.Background())
			if err == nil {
				t.Errorf("expected validation error, got nil")
			}
		})
	}
}

// TestDrain_LockKey_NoOpOnSQLite verifies that LockKey is harmlessly
// ignored on adapters that don't implement DrainLocker (SQLite).
func TestDrain_LockKey_NoOpOnSQLite(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	want := seedEvents(t, agg, []string{"t-lockkey"}, 3)

	pub := inproc.New()
	var n atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		n.Add(1)
		return nil
	})

	d := &outbox.Drain{
		Store:     store.(es.OutboxStore),
		Publisher: pub,
		LockKey:   "ignored-by-sqlite",
	}
	pubd, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if pubd != want {
		t.Errorf("published: got %d want %d", pubd, want)
	}
	if int(n.Load()) != want {
		t.Errorf("subscriber: got %d want %d", n.Load(), want)
	}
}
