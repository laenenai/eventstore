package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/laenenai/eventstore/es"
)

// Postgres SQLSTATE for deadlock_detected. We surface this
// separately from generic Append failures because the brief's
// scenario B SLO calls out "no catastrophic locking (deadlock
// chains > 100 ms)" as a hard requirement. A count of 0 is the
// pass criterion; the run continues either way so we always
// collect a latency tail too.
const pgDeadlockDetected = "40P01"

// ScenarioBConfig knobs the burst smoke. Defaults are tuned to
// drive **above** the measured ~167–180 writes/sec advisory-lock
// ceiling so we can observe queueing, latency tail, and lock-chain
// behaviour — the whole point of the burst test. See DefaultConfigB.
type ScenarioBConfig struct {
	// TenantsTotal is the population the burst spreads across. The
	// brief calls for "100K simultaneous writes spread across as
	// many distinct tenants" — at the 10K smoke we reuse the
	// scenario A population shape (90/9/1) and pick burst targets
	// uniformly across hot+warm (cold tenants are seeded too so
	// every target is a pre-existing stream).
	TenantsTotal int

	// BurstDuration is the offered-load window. The brief says "1
	// minute"; the 10K smoke uses 10s to keep CI time under 90s
	// while still resolving the saturation tail.
	BurstDuration time.Duration

	// TargetWrites is the number of writes we offer during the
	// burst window. The brief's scenario B target is 100K writes
	// in 60s ≈ 1667/sec. The smoke shape is "above the ~180/sec
	// ceiling" so 10K writes in 10s ≈ 1000/sec gives us the same
	// saturation signature in a fraction of the wall time.
	TargetWrites int

	// WorkerConcurrency caps the in-flight Append count. At the
	// burst rate every worker spends most of its time blocked on
	// the advisory-lock-serialised append path, so a deep pool
	// just translates offered load into queue depth — which is
	// what we want to observe. Default 64; double the steady-state
	// pool to make the queue more visible.
	WorkerConcurrency int

	// SeedConcurrency mirrors scenario A's bootstrap parallelism.
	SeedConcurrency int

	// Tables names whose stats the harness snapshots before/after.
	// Same defaults as scenario A.
	Tables []string
}

// DefaultConfigB returns the smoke-at-10K burst config.
//
// The offered rate is deliberately set ABOVE the measured advisory-
// lock ceiling — that's the point of scenario B. Per §11.2.2 of the
// spike doc, the testcontainers-on-Docker M1 Max baseline sustains
// ~167–180 writes/sec; the smoke offers ~1000/sec. The gap exposes:
//
//   - queue depth: how Append callers experience saturation
//   - latency tail (p99.9): the brief's < 2 s scenario B SLO
//   - lock-chain behaviour: deadlock count is the hard fail signal
//
// Scenario A's defaults instead stay **below** the ceiling so its
// per-append latency measurements are not confounded by queueing —
// the two scenarios measure complementary things and use opposite
// pacing.
//
// 10K writes / 10s = 1000 writes/sec offered ≈ 5.5× the sustained
// ceiling. At 10K tenants total (90/9/1: 9000 cold, 900 warm, 100
// hot), each burst targets a tenant uniformly at random from the
// warm+hot slice — cold tenants are seeded for realism but skipped
// in the burst (the brief's "every active user opens the app" frame
// excludes dormant accounts by definition).
func DefaultConfigB() ScenarioBConfig {
	return ScenarioBConfig{
		TenantsTotal:      10_000,
		BurstDuration:     10 * time.Second,
		TargetWrites:      10_000,
		WorkerConcurrency: 64,
		SeedConcurrency:   32,
		Tables: []string{
			"state_cache",
			"projection_checkpoint",
			"processed_events",
			"state_stream_subscribers",
			"events",
		},
	}
}

// ScenarioBResult is the bundle the burst reporter consumes. Adds
// offered-vs-achieved framing and deadlock count on top of the
// scenario A shape — the percentile breakdown reuses LatencySummary
// (extended with P999 in scenario_a.go) so cross-scenario diffs stay
// apples-to-apples.
type ScenarioBResult struct {
	TenantsTotal int
	TenantsCold  int
	TenantsWarm  int
	TenantsHot   int

	SeedDuration  time.Duration
	BurstDuration time.Duration
	WallDuration  time.Duration // includes drain after deadline

	// TargetWrites is the nominal demand the config specified — the
	// "100K simultaneous writes" of the brief. Offered is what the
	// pacer actually managed to queue into the worker pool within
	// the burst window; when Offered < TargetWrites the system
	// rejected back-pressure on the pacer (worker pool saturated +
	// channel buffer full), which is itself the central measurement
	// of a burst-handling test. Achieved is AppendSucc + AppendFail
	// — what actually reached store.Append. Offered - Achieved is
	// the in-flight-when-shutdown gap (should be 0 since we don't
	// cancel mid-Append).
	TargetWrites int64
	Offered      int64
	Achieved     int64

	AppendSucc int64
	AppendFail int64

	// Deadlocks is the count of Append failures whose pgx error
	// carries SQLSTATE 40P01. The brief's hard SLO is 0; any
	// non-zero value is interesting data for the spike.
	Deadlocks int64

	// FailureReasons maps a coarse error label to its count.
	// "deadlock", "conflict", "context-deadline", "other" — kept
	// short so the reporter's table is readable.
	FailureReasons map[string]int64

	AppendLatencies LatencySummary

	TableStatsBefore []TableStat
	TableStatsAfter  []TableStat
}

// RunScenarioB executes the burst scenario:
//  1. Population: 90/9/1 (same as scenario A).
//  2. Seed: one Append per tenant — every burst target is a
//     pre-existing stream so we measure append-into-existing-state,
//     not first-event boot cost.
//  3. Snapshot: pg_stat_user_tables baseline.
//  4. Burst: a single pacing goroutine emits offered-rate tokens
//     for BurstDuration; a fixed worker pool drains the token
//     channel and calls store.Append. When the burst deadline
//     fires, the pacer stops and in-flight workers complete
//     naturally — context is NOT cancelled mid-Append so we
//     measure real tail latency rather than synthesised
//     cancellation errors.
//  5. Snapshot: pg_stat_user_tables endpoint.
//
// Pacing strategy: a single time.Ticker fires at the target
// interval; each tick pushes one job to a buffered channel. The
// worker pool drains it. If the channel is full (because workers
// are saturated), the pacer blocks — this is the **measurement**:
// "offered" diverges from "achieved" precisely when the system is
// the bottleneck. Fire-and-forget goroutine-per-tick was the
// rejected alternative — at 1000/sec for 10s that's 10K goroutines
// stacking up on the advisory lock, all with their own pgx
// connections, exhausting the pool and producing a
// "could not acquire connection" failure mode that says nothing
// about the storage layer's burst characteristics.
func RunScenarioB(ctx context.Context, h *Harness, cfg ScenarioBConfig) (*ScenarioBResult, error) {
	tenants := Population(cfg.TenantsTotal)

	res := &ScenarioBResult{
		TenantsTotal:   cfg.TenantsTotal,
		TargetWrites:   int64(cfg.TargetWrites),
		FailureReasons: map[string]int64{},
	}
	var targets []Tenant
	for _, t := range tenants {
		switch t.Cohort {
		case CohortCold:
			res.TenantsCold++
		case CohortWarm:
			res.TenantsWarm++
			targets = append(targets, t)
		case CohortHot:
			res.TenantsHot++
			targets = append(targets, t)
		}
	}
	if len(targets) == 0 {
		return res, errors.New("no warm+hot tenants to target — population too small")
	}

	wallStart := time.Now()

	// Phase 1: seed every tenant. We need every burst target to
	// hit a pre-existing stream, matching the brief's "every
	// active user opens the app" frame (not "every brand new
	// signup floods the system simultaneously").
	seedStart := time.Now()
	if err := seed(ctx, h.Adapter, tenants, cfg.SeedConcurrency); err != nil {
		return res, fmt.Errorf("seed: %w", err)
	}
	res.SeedDuration = time.Since(seedStart)

	// Phase 2: baseline stats.
	before, err := SampleTables(ctx, h.AdminPool, cfg.Tables)
	if err != nil {
		return res, fmt.Errorf("baseline stats: %w", err)
	}
	res.TableStatsBefore = before

	// Phase 3: burst.
	versions := make(map[string]*atomic.Uint64, len(tenants))
	for _, t := range tenants {
		v := new(atomic.Uint64)
		v.Store(1) // seed wrote v1; next is v2
		versions[t.ID] = v
	}

	recorder := NewRecorder("append")
	burstStart := time.Now()
	offered, reasons, err := burst(ctx, h.Adapter, targets, versions, cfg, recorder)
	if err != nil {
		return res, fmt.Errorf("burst: %w", err)
	}
	res.BurstDuration = time.Since(burstStart)
	res.Offered = offered

	samples, succ, fail := recorder.Snapshot()
	res.AppendSucc = succ["append"]
	res.AppendFail = fail["append"]
	res.Achieved = res.AppendSucc + res.AppendFail
	res.AppendLatencies = summarize(samples)
	res.FailureReasons = reasons
	res.Deadlocks = reasons["deadlock"]

	// Phase 4: endpoint stats.
	after, err := SampleTables(ctx, h.AdminPool, cfg.Tables)
	if err != nil {
		return res, fmt.Errorf("endpoint stats: %w", err)
	}
	res.TableStatsAfter = after

	res.WallDuration = time.Since(wallStart)
	return res, nil
}

// burst paces the offered load and drives a worker pool. Returns
// the offered count, the per-reason failure tally, and any fatal
// error (only set if the pool itself failed to start, not on
// individual Append errors which are recorded and counted).
func burst(
	ctx context.Context,
	store es.Store,
	targets []Tenant,
	versions map[string]*atomic.Uint64,
	cfg ScenarioBConfig,
	recorder *Recorder,
) (int64, map[string]int64, error) {
	if cfg.WorkerConcurrency < 1 {
		return 0, nil, errors.New("WorkerConcurrency must be >= 1")
	}
	if cfg.TargetWrites < 1 {
		return 0, nil, errors.New("TargetWrites must be >= 1")
	}
	if cfg.BurstDuration <= 0 {
		return 0, nil, errors.New("BurstDuration must be > 0")
	}

	// Compute the inter-token interval. At 10K / 10s that's 1 ms
	// per token. We use time.NewTicker rather than a sleep loop so
	// the pacer doesn't drift on slow ticks — ticker compensates by
	// dropping ticks if the consumer is slow, which here means the
	// channel send blocked because all workers are busy. Exactly
	// the saturation signal we want to record.
	interval := cfg.BurstDuration / time.Duration(cfg.TargetWrites)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	type job struct{ tenant string }
	// Buffered enough to absorb a few hundred ticks while workers
	// catch up — but not so deep we hide saturation under buffer.
	// At burst rate 1 ms a 256-deep buffer = 256 ms of buffered
	// load; beyond that the pacer blocks, which is exactly what
	// "offered exceeded what the system could absorb" looks like.
	jobs := make(chan job, 256)

	// Per-failure-reason atomic counters. Workers tag each failure
	// with one of {deadlock, conflict, context-deadline, other};
	// the snapshot at end of burst feeds the reporter.
	var nDeadlock, nConflict, nDeadline, nOther atomic.Int64

	// Deadline for the offered-load window. After the deadline
	// fires the pacer stops emitting; we then close `jobs` and the
	// workers drain. Important: we do NOT cancel ctx mid-Append —
	// in-flight appends complete or error on their own. Otherwise
	// we'd record context.Canceled errors instead of real latency.
	burstCtx, cancelBurst := context.WithTimeout(ctx, cfg.BurstDuration)
	defer cancelBurst()

	var workerWG sync.WaitGroup
	for range cfg.WorkerConcurrency {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for j := range jobs {
				v := versions[j.tenant].Add(1)
				expected := v - 1
				sid, _ := es.NewStreamID(j.tenant, "counter", "main")
				// Use the OUTER ctx, not burstCtx — burst deadline
				// gates pacing, not in-flight Append latency.
				tenantCtx := es.WithTenant(ctx, j.tenant)

				start := time.Now()
				_, err := store.Append(tenantCtx, es.AppendParams{
					StreamID:           sid,
					ExpectedVersion:    expected,
					Events:             []es.EventToAppend{makeEvent(v)},
					NewStateBytes:      stateBytes(v),
					StateTypeURL:       StateTypeURL,
					StateSchemaVersion: 1,
				})
				d := time.Since(start)

				if err != nil {
					reason := classifyErr(err)
					switch reason {
					case "deadlock":
						nDeadlock.Add(1)
					case "conflict":
						nConflict.Add(1)
					case "context-deadline":
						nDeadline.Add(1)
					default:
						nOther.Add(1)
					}
					recorder.Add("append", d, false)
					continue
				}
				recorder.Add("append", d, true)
			}
		}()
	}

	// Pacer goroutine. Single source of truth for offered count.
	var offered atomic.Int64
	pacerDone := make(chan struct{})
	go func() {
		defer close(pacerDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		emitted := 0
		for emitted < cfg.TargetWrites {
			select {
			case <-burstCtx.Done():
				return
			case <-ticker.C:
				t := targets[rand.IntN(len(targets))]
				select {
				case jobs <- job{tenant: t.ID}:
					offered.Add(1)
					emitted++
				case <-burstCtx.Done():
					return
				}
			}
		}
	}()

	<-pacerDone
	close(jobs)
	workerWG.Wait()

	reasons := map[string]int64{
		"deadlock":         nDeadlock.Load(),
		"conflict":         nConflict.Load(),
		"context-deadline": nDeadline.Load(),
		"other":            nOther.Load(),
	}
	return offered.Load(), reasons, nil
}

// classifyErr buckets one Append error for the failure-reasons
// histogram. The buckets correspond to interpretable failure modes:
//   - "deadlock": Postgres detected a lock cycle (SQLSTATE 40P01).
//     The brief's scenario B hard SLO; any non-zero is interesting.
//   - "conflict": OCC race (es.ErrConflict) — expected at high
//     concurrency on a small set of streams, not a SLO failure.
//   - "context-deadline": outer ctx cancelled — only happens if the
//     wrapping test ran out of time, not a system signal.
//   - "other": everything else; reporter surfaces a count so a
//     non-zero value is visible enough to prompt investigation.
func classifyErr(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "context-deadline"
	}
	if errors.Is(err, es.ErrConflict) {
		return "conflict"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgDeadlockDetected {
		return "deadlock"
	}
	return "other"
}
