package sqlite_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/projection"
)

// TestProjection_ShardedSplitsWorkAcrossShards verifies that two
// runners with disjoint shards cover all events between them without
// either touching the other's slice. Mirror of the drain sharding test.
func TestProjection_ShardedSplitsWorkAcrossShards(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	tenants := []string{"t-shardP-a", "t-shardP-b", "t-shardP-c", "t-shardP-d"}
	want := seedEvents(t, agg, tenants, 4)

	cp := store.(projection.Checkpoint)

	var sawA, sawB atomic.Int64
	streamsA := map[string]struct{}{}
	streamsB := map[string]struct{}{}

	rtA := &projection.Runtime{
		Name: "shard-proj", Store: store, Checkpoint: cp,
		Shard: 0, TotalShards: 2,
		Handler: func(ctx context.Context, env es.Envelope) error {
			streamsA[env.TenantID+"|"+env.StreamID.Canonical()] = struct{}{}
			sawA.Add(1)
			return nil
		},
	}
	rtB := &projection.Runtime{
		Name: "shard-proj-b", Store: store, Checkpoint: cp,
		Shard: 1, TotalShards: 2,
		Handler: func(ctx context.Context, env es.Envelope) error {
			streamsB[env.TenantID+"|"+env.StreamID.Canonical()] = struct{}{}
			sawB.Add(1)
			return nil
		},
	}

	if _, err := rtA.RunOnce(context.Background()); err != nil {
		t.Fatalf("rtA RunOnce: %v", err)
	}
	if _, err := rtB.RunOnce(context.Background()); err != nil {
		t.Fatalf("rtB RunOnce: %v", err)
	}

	if int(sawA.Load()+sawB.Load()) != want {
		t.Errorf("total: got %d want %d", sawA.Load()+sawB.Load(), want)
	}
	for k := range streamsA {
		if _, dup := streamsB[k]; dup {
			t.Errorf("stream %q on both shards", k)
		}
	}
	if sawA.Load() == 0 && sawB.Load() == 0 {
		t.Errorf("no events processed at all")
	}
}

// TestProjection_ShardingValidation rejects invalid shard configs.
func TestProjection_ShardingValidation(t *testing.T) {
	store, _ := newStoreAndCounter(t)
	cases := []struct {
		name        string
		shard       int
		totalShards int
	}{
		{"out of range", 5, 3},
		{"negative shard", -1, 3},
		{"negative total", 0, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := &projection.Runtime{
				Name: "bad", Store: store, Checkpoint: projection.NewMemoryCheckpoint(),
				Shard: tc.shard, TotalShards: tc.totalShards,
				Handler: func(context.Context, es.Envelope) error { return nil },
			}
			_, err := rt.RunOnce(context.Background())
			if err == nil {
				t.Errorf("expected validation error, got nil")
			}
		})
	}
}
