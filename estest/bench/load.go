package bench

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// EventTypeURL is the synthetic event the load generator appends.
// We deliberately do NOT codegen an aggregate for this — the spike
// measures storage behaviour, not aggregate semantics. The payload
// is just an 8-byte big-endian counter so encode is constant-time.
const EventTypeURL = "bench.counter.v1.Incremented"

// StateTypeURL labels the state_cache row written alongside each
// event. Same constant-time-encoded counter.
const StateTypeURL = "bench.counter.v1.Counter"

// SeedOne appends the initial event for one tenant. Returns the
// stream id used (so the steady-state phase can address the same
// stream) and any error.
//
// Each tenant gets one stream with stream_id "counter:main". One
// stream per tenant is the load-bearing case for state_cache —
// every command UPSERTs the row.
func SeedOne(ctx context.Context, store es.Store, tenant string) (es.StreamID, error) {
	sid, err := es.NewStreamID(tenant, "counter", "main")
	if err != nil {
		return es.StreamID{}, fmt.Errorf("stream id: %w", err)
	}
	tenantCtx := es.WithTenant(ctx, tenant)
	if _, err := store.Append(tenantCtx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventToAppend{
			makeEvent(1),
		},
		NewStateBytes:      stateBytes(1),
		StateTypeURL:       StateTypeURL,
		StateSchemaVersion: 1,
	}); err != nil {
		return es.StreamID{}, fmt.Errorf("seed append: %w", err)
	}
	return sid, nil
}

// LatencySample is one (op, duration) pair recorded by the run
// phase. We keep a simple slice rather than a streaming sketch
// because the load volume in the 10K smoke is small enough for the
// sort-and-pick-percentile approach to stay sub-millisecond.
type LatencySample struct {
	Op       string
	Duration time.Duration
}

// Recorder collects latency samples concurrently. Producers Add
// without coordination; Snapshot is called once at end of run.
type Recorder struct {
	mu      sync.Mutex
	samples []LatencySample
	// success/error counts per op
	success map[string]*atomic.Int64
	failure map[string]*atomic.Int64
}

// NewRecorder allocates a recorder. The known ops list lets us
// pre-allocate atomic counters and gives a stable iteration order
// in reports.
func NewRecorder(ops ...string) *Recorder {
	r := &Recorder{
		samples: make([]LatencySample, 0, 1<<16),
		success: make(map[string]*atomic.Int64, len(ops)),
		failure: make(map[string]*atomic.Int64, len(ops)),
	}
	for _, op := range ops {
		r.success[op] = new(atomic.Int64)
		r.failure[op] = new(atomic.Int64)
	}
	return r
}

// Add records one sample. Thread-safe; the mutex covers slice
// growth, the atomic counters are lock-free.
func (r *Recorder) Add(op string, d time.Duration, ok bool) {
	r.mu.Lock()
	r.samples = append(r.samples, LatencySample{Op: op, Duration: d})
	r.mu.Unlock()
	if ok {
		if c, ok := r.success[op]; ok {
			c.Add(1)
		}
	} else {
		if c, ok := r.failure[op]; ok {
			c.Add(1)
		}
	}
}

// Snapshot returns a defensive copy of the samples + counts for the
// reporter.
func (r *Recorder) Snapshot() ([]LatencySample, map[string]int64, map[string]int64) {
	r.mu.Lock()
	samples := make([]LatencySample, len(r.samples))
	copy(samples, r.samples)
	r.mu.Unlock()
	success := make(map[string]int64, len(r.success))
	failure := make(map[string]int64, len(r.failure))
	for op, c := range r.success {
		success[op] = c.Load()
	}
	for op, c := range r.failure {
		failure[op] = c.Load()
	}
	return samples, success, failure
}

// Drain clears the in-memory samples slice without resetting the
// success/failure counters. Used by scenario C's heartbeat loop so
// each window starts with a fresh latency buffer — a 7-day soak
// would otherwise accumulate ~100M LatencySamples and exhaust
// memory. Counters stay so cumulative succ/fail across the whole
// soak is still tracked.
func (r *Recorder) Drain() {
	r.mu.Lock()
	r.samples = r.samples[:0]
	r.mu.Unlock()
}

// makeEvent fabricates one EventToAppend with the supplied
// counter value. UUIDs are random; nothing in the bench depends
// on determinism beyond the tenant population.
func makeEvent(counter uint64) es.EventToAppend {
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], counter)
	return es.EventToAppend{
		EventID:       uuid.New(),
		TypeURL:       EventTypeURL,
		SchemaVersion: 1,
		OccurredAt:    time.Now(),
		CorrelationID: uuid.New(),
		CausationID:   uuid.Nil,
		CommandID:     uuid.New(),
		Actor:         es.Actor{},
		Payload:       payload[:],
	}
}

// stateBytes returns the 8-byte JSON for one state row. We hand-
// encode rather than json.Marshal a struct because the per-Append
// allocation overhead at 1k QPS is measurable.
func stateBytes(counter uint64) []byte {
	// {"count":N} — kept ASCII to fit JSONB without quoting work.
	return fmt.Appendf(nil, `{"count":%d}`, counter)
}
