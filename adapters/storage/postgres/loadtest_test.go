//go:build loadtest

package postgres_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
	"github.com/laenenai/eventstore/outbox"
	"github.com/laenenai/eventstore/adapters/publisher/inproc"
	"github.com/laenenai/eventstore/state_stream"
)

// Postgres load tests. Mirror of the SQLite suite — uses the same
// shared testcontainer + adapter set up in TestMain (adapter_test.go).
// Build with -tags loadtest. Set EVENTSTORE_SKIP_PG_TESTS=1 to skip
// when Docker is unavailable.
//
// Knobs (env vars):
//
//   EVENTSTORE_LOAD_WRITERS       — concurrent writer goroutines (default 32)
//   EVENTSTORE_LOAD_STREAMS       — distinct streams (default 256)
//   EVENTSTORE_LOAD_OPS_PER_STREAM— ops per stream (default 20)
//   EVENTSTORE_LOAD_HOT_OPS       — total ops in hot-stream test (default 200)
//   EVENTSTORE_LOAD_HOT_GOROUTINES— writers hammering one stream (default 32)

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// TestLoad_PG_ConcurrentDistinctStreamWriters
//
// Workload: N goroutines write Increment commands to a pool of distinct
// streams. With no OCC contention, Postgres should fan out cleanly via
// its connection pool.
func TestLoad_PG_ConcurrentDistinctStreamWriters(t *testing.T) {
	tenant := tnt(t, "load-distinct")
	ctx := es.WithTenant(context.Background(), tenant)
	rt := newCounterProtoRuntime()

	writers := envInt("EVENTSTORE_LOAD_WRITERS", 32)
	streams := envInt("EVENTSTORE_LOAD_STREAMS", 256)
	opsPerStream := envInt("EVENTSTORE_LOAD_OPS_PER_STREAM", 20)

	streamIDs := make([]es.StreamID, streams)
	for i := 0; i < streams; i++ {
		sid := estest.MustStream(t, tenant, "counter", strconv.Itoa(i))
		streamIDs[i] = sid
		if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: int64(opsPerStream * 10), Initial: 0}); err != nil {
			t.Fatalf("Init %d: %v", i, err)
		}
	}

	totalOps := streams * opsPerStream
	opsChan := make(chan int, totalOps)
	for i := 0; i < totalOps; i++ {
		opsChan <- i % streams
	}
	close(opsChan)

	var errs atomic.Int64
	var done atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for streamIdx := range opsChan {
				if err := handleWithRetry(ctx, rt, streamIDs[streamIdx],
					&counterv1.Increment{By: 1}, 50); err != nil {
					errs.Add(1)
					t.Errorf("Increment stream %d: %v", streamIdx, err)
					continue
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if errs.Load() != 0 {
		t.Fatalf("writer errors: %d", errs.Load())
	}
	if done.Load() != int64(totalOps) {
		t.Fatalf("ops done: got %d want %d", done.Load(), totalOps)
	}

	var reader es.StateCacheReader = adapter
	for i, sid := range streamIDs {
		row, err := reader.GetState(context.Background(), tenant, sid.Canonical())
		if err != nil {
			t.Fatalf("GetState stream %d: %v", i, err)
		}
		want := uint64(1 + opsPerStream)
		if row.Version != want {
			t.Errorf("stream %d version: got %d want %d", i, row.Version, want)
		}
	}

	t.Logf("PG distinct-stream load: writers=%d streams=%d ops=%d elapsed=%s throughput=%.0f ops/s",
		writers, streams, totalOps, elapsed,
		float64(totalOps)/elapsed.Seconds())
}

// TestLoad_PG_HotStreamOCC
//
// Workload: N goroutines all hammer one stream. Tests the OCC retry
// loop under contention with real Postgres serializable-ish behavior.
func TestLoad_PG_HotStreamOCC(t *testing.T) {
	tenant := tnt(t, "load-hot")
	ctx := es.WithTenant(context.Background(), tenant)
	rt := newCounterProtoRuntime()

	ops := envInt("EVENTSTORE_LOAD_HOT_OPS", 200)
	writers := envInt("EVENTSTORE_LOAD_HOT_GOROUTINES", 32)

	sid := estest.MustStream(t, tenant, "counter", "hot")
	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: int64(ops * 10), Initial: 0}); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var totalRetries atomic.Int64
	var done atomic.Int64
	work := make(chan struct{}, ops)
	for i := 0; i < ops; i++ {
		work <- struct{}{}
	}
	close(work)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				retries, err := handleWithRetryCounting(ctx, rt, sid,
					&counterv1.Increment{By: 1}, 200)
				totalRetries.Add(int64(retries))
				if err != nil {
					t.Errorf("Increment hot: %v", err)
					continue
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if done.Load() != int64(ops) {
		t.Fatalf("hot ops done: got %d want %d", done.Load(), ops)
	}

	t.Logf("PG hot-stream OCC: writers=%d ops=%d retries=%d retries/op=%.2f elapsed=%s throughput=%.0f ops/s",
		writers, ops, totalRetries.Load(),
		float64(totalRetries.Load())/float64(ops),
		elapsed, float64(ops)/elapsed.Seconds())
}

// TestLoad_PG_DrainsKeepUp
//
// Seed K streams * M events, run both drains. Verify outbox sees K*M
// rows and state_stream sees K coalesced deliveries (the coalescing
// ratio at work).
func TestLoad_PG_DrainsKeepUp(t *testing.T) {
	tenant := tnt(t, "load-drains")
	ctx := es.WithTenant(context.Background(), tenant)
	rt := newCounterProtoRuntime()

	streams := envInt("EVENTSTORE_LOAD_STREAMS", 64)
	opsPerStream := envInt("EVENTSTORE_LOAD_OPS_PER_STREAM", 10)

	for i := 0; i < streams; i++ {
		sid := estest.MustStream(t, tenant, "counter", strconv.Itoa(i))
		if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 1_000_000, Initial: 0}); err != nil {
			t.Fatalf("Init %d: %v", i, err)
		}
		for j := 0; j < opsPerStream-1; j++ {
			if err := handleWithRetry(ctx, rt, sid,
				&counterv1.Increment{By: 1}, 50); err != nil {
				t.Fatalf("Increment %d/%d: %v", i, j, err)
			}
		}
	}
	totalEvents := streams * opsPerStream

	// --- Outbox drain ---
	pub := inproc.New()
	var published atomic.Int64
	pub.Subscribe(func(_ context.Context, _ es.Envelope) error {
		published.Add(1)
		return nil
	})
	outboxDrain := &outbox.Drain{
		Store:     adapter,
		Publisher: pub,
		BatchSize: 500,
		Tenant:    tenant,
	}
	outboxStart := time.Now()
	pubCount, _, err := outboxDrain.Run(context.Background())
	if err != nil {
		t.Fatalf("outbox Run: %v", err)
	}
	outboxElapsed := time.Since(outboxStart)
	if pubCount != totalEvents {
		t.Errorf("outbox published: got %d want %d", pubCount, totalEvents)
	}

	// --- state_stream drain (coalesced) ---
	var sscount atomic.Int64
	ssRec := statePublisherFunc(func(_ context.Context, _ es.StateEnvelope) error {
		sscount.Add(1)
		return nil
	})
	ssDrain := &state_stream.Drain{
		SubscriberName: "load-test-mirror-pg",
		Tenant:         tenant,
		Store:          adapter,
		Publisher:      ssRec,
		BatchSize:      500,
	}
	ssStart := time.Now()
	delivered, err := ssDrain.Run(context.Background())
	if err != nil {
		t.Fatalf("state_stream Run: %v", err)
	}
	ssElapsed := time.Since(ssStart)
	if delivered != streams {
		t.Errorf("state_stream delivered: got %d want %d (one per stream — coalesced)",
			delivered, streams)
	}

	t.Logf("PG drains: streams=%d events=%d outbox=%s (%.0f ev/s) state_stream=%s (%.0f streams/s, coalesce ratio %.1fx)",
		streams, totalEvents,
		outboxElapsed, float64(totalEvents)/outboxElapsed.Seconds(),
		ssElapsed, float64(streams)/ssElapsed.Seconds(),
		float64(totalEvents)/float64(streams))
}

// --- helpers ---

func retryOnConflict(fn func() error, maxRetries int) (int, error) {
	var retries int
	for {
		err := fn()
		if err == nil {
			return retries, nil
		}
		if errors.Is(err, es.ErrConflict) && retries < maxRetries {
			retries++
			continue
		}
		return retries, err
	}
}

func handleWithRetry(
	ctx context.Context,
	rt *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event],
	sid es.StreamID,
	cmd counterv1.Command,
	maxRetries int,
) error {
	_, err := retryOnConflict(func() error {
		_, err := rt.Handle(ctx, sid, cmd)
		return err
	}, maxRetries)
	return err
}

func handleWithRetryCounting(
	ctx context.Context,
	rt *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event],
	sid es.StreamID,
	cmd counterv1.Command,
	maxRetries int,
) (int, error) {
	return retryOnConflict(func() error {
		_, err := rt.Handle(ctx, sid, cmd)
		return err
	}, maxRetries)
}

type statePublisherFunc func(context.Context, es.StateEnvelope) error

func (f statePublisherFunc) PublishState(ctx context.Context, env es.StateEnvelope) error {
	return f(ctx, env)
}
