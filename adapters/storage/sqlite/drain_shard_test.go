package sqlite_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/outbox"
	"github.com/laenenai/eventstore/publisher/inproc"
)

// TestDrain_ShardedSplitsWorkAcrossShards verifies that two Drains
// configured with disjoint shards cover all the rows between them
// without either touching the other's slice.
func TestDrain_ShardedSplitsWorkAcrossShards(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	// 3 tenants × 5 events = 15 events. global_position is 1..15.
	want := seedEvents(t, agg, []string{"t-shard-a", "t-shard-b", "t-shard-c"}, 5)

	// Two shards split (pos % 2) == 0 vs == 1.
	pubA := inproc.New()
	var sawA atomic.Int64
	pubA.Subscribe(func(ctx context.Context, env es.Envelope) error {
		if env.GlobalPosition%2 != 0 {
			t.Errorf("shard 0 saw position %d (expected even)", env.GlobalPosition)
		}
		sawA.Add(1)
		return nil
	})

	pubB := inproc.New()
	var sawB atomic.Int64
	pubB.Subscribe(func(ctx context.Context, env es.Envelope) error {
		if env.GlobalPosition%2 != 1 {
			t.Errorf("shard 1 saw position %d (expected odd)", env.GlobalPosition)
		}
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
	// Reasonable split: each shard saw roughly half.
	if sawA.Load() == 0 || sawB.Load() == 0 {
		t.Errorf("expected both shards to see some events; got A=%d B=%d", sawA.Load(), sawB.Load())
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
