package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/laenenai/eventstore/es"
)

// ScenarioAResult is what the smoke produces. Picked up by the
// reporter; can later be JSON-encoded for cross-run comparison.
type ScenarioAResult struct {
	TenantsTotal int
	TenantsCold  int
	TenantsWarm  int
	TenantsHot   int

	SeedDuration time.Duration
	RunDuration  time.Duration

	AppendSucc int64
	AppendFail int64

	AppendLatencies LatencySummary

	TableStatsBefore []TableStat
	TableStatsAfter  []TableStat
}

// LatencySummary is the percentile breakdown the reporter prints.
// Sub-millisecond precision is enough for our SLOs (< 20 ms p50,
// < 100 ms p99 per the spike brief); we report in microseconds to
// keep numbers readable.
type LatencySummary struct {
	Count int
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
}

func summarize(samples []LatencySample) LatencySummary {
	if len(samples) == 0 {
		return LatencySummary{}
	}
	ds := make([]time.Duration, 0, len(samples))
	for _, s := range samples {
		ds = append(ds, s.Duration)
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	idx := func(p float64) int {
		i := int(float64(len(ds)) * p)
		if i >= len(ds) {
			i = len(ds) - 1
		}
		return i
	}
	return LatencySummary{
		Count: len(ds),
		P50:   ds[idx(0.50)],
		P95:   ds[idx(0.95)],
		P99:   ds[idx(0.99)],
		Max:   ds[len(ds)-1],
	}
}

// ScenarioAConfig is the knobs the smoke runner exposes. The 10K
// smoke defaults are tuned to complete in ~2 minutes against a
// testcontainers Postgres on a developer laptop.
type ScenarioAConfig struct {
	// TenantsTotal is the population. 10_000 for the smoke; 100k,
	// 500k, 1M for the full Phase 1 tiers.
	TenantsTotal int

	// RunDuration is how long the steady-state phase runs after
	// seeding. The brief's targets (< 20 ms p50, < 100 ms p99) are
	// observable in 30-60 s of write load at our smoke volume.
	RunDuration time.Duration

	// HotWritesPerSec is the per-hot-tenant write rate during the
	// run. Hot tenants share the load uniformly. Default 1.
	HotWritesPerSec float64

	// WarmWritesPerMin is the per-warm-tenant rate during the run.
	// Default 1.
	WarmWritesPerMin float64

	// SeedConcurrency is how many goroutines parallel the seed
	// inserts. The default (32) matches the pool's MaxConns; going
	// higher just queues.
	SeedConcurrency int

	// RunConcurrency caps the writer goroutine count. Default 32.
	RunConcurrency int

	// Tables names whose stats the harness snapshots before/after.
	// The defaults are the hot tables identified in spike 0001
	// §11.1: state_cache, projection_checkpoint,
	// processed_events, state_stream_subscribers, plus events for
	// reference.
	Tables []string
}

// DefaultConfig returns the smoke-at-10K config.
func DefaultConfig() ScenarioAConfig {
	return ScenarioAConfig{
		TenantsTotal:     10_000,
		RunDuration:      60 * time.Second,
		HotWritesPerSec:  1.0,
		WarmWritesPerMin: 1.0,
		SeedConcurrency:  32,
		RunConcurrency:   32,
		Tables: []string{
			"state_cache",
			"projection_checkpoint",
			"processed_events",
			"state_stream_subscribers",
			"events",
		},
	}
}

// RunScenarioA executes the steady-state scenario:
//   1. Population: 90 % cold, 9 % warm, 1 % hot, deterministic by index.
//   2. Seed: one Append per tenant (10K appends) to bootstrap streams
//      and state_cache rows.
//   3. Snapshot: pg_stat_user_tables baseline.
//   4. Run: steady-state writes at configured rates for RunDuration.
//      Cold tenants are untouched; warm/hot drive the load.
//   5. Snapshot: pg_stat_user_tables endpoint.
//
// Results bundle is returned for the reporter.
func RunScenarioA(ctx context.Context, h *Harness, cfg ScenarioAConfig) (*ScenarioAResult, error) {
	tenants := Population(cfg.TenantsTotal)

	res := &ScenarioAResult{
		TenantsTotal: cfg.TenantsTotal,
	}
	for _, t := range tenants {
		switch t.Cohort {
		case CohortCold:
			res.TenantsCold++
		case CohortWarm:
			res.TenantsWarm++
		case CohortHot:
			res.TenantsHot++
		}
	}

	// Phase 1: seed.
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

	// Phase 3: run.
	recorder := NewRecorder("append")
	runStart := time.Now()
	if err := steadyState(ctx, h.Adapter, tenants, cfg, recorder); err != nil {
		return res, fmt.Errorf("run: %w", err)
	}
	res.RunDuration = time.Since(runStart)

	samples, succ, fail := recorder.Snapshot()
	res.AppendSucc = succ["append"]
	res.AppendFail = fail["append"]
	res.AppendLatencies = summarize(samples)

	// Phase 4: endpoint stats.
	after, err := SampleTables(ctx, h.AdminPool, cfg.Tables)
	if err != nil {
		return res, fmt.Errorf("endpoint stats: %w", err)
	}
	res.TableStatsAfter = after

	return res, nil
}

// seed parallelises the per-tenant first Append. Failures are
// surfaced via the first-error channel.
func seed(ctx context.Context, store es.Store, tenants []Tenant, concurrency int) error {
	if concurrency < 1 {
		concurrency = 1
	}
	in := make(chan Tenant, concurrency*2)
	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range in {
				if _, err := SeedOne(ctx, store, t.ID); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}
	for _, t := range tenants {
		select {
		case in <- t:
		case err := <-errCh:
			close(in)
			wg.Wait()
			return err
		}
	}
	close(in)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// steadyState runs the write load for cfg.RunDuration. Hot tenants
// each get a 1Hz-ish slot; warm tenants a 1/min-ish slot. Cold
// tenants are untouched. We pick the next write target from a
// pre-shuffled hot+warm slice each tick so the load smooths.
func steadyState(
	ctx context.Context,
	store es.Store,
	tenants []Tenant,
	cfg ScenarioAConfig,
	recorder *Recorder,
) error {
	// Build a per-tenant counter map so each Append knows the
	// next ExpectedVersion.
	versions := make(map[string]*atomic.Uint64, len(tenants))
	for _, t := range tenants {
		v := new(atomic.Uint64)
		v.Store(1) // seed wrote v1; next is v2
		versions[t.ID] = v
	}

	// Hot + warm pool of work items. Hot tenants appear once per
	// hot-slot; warm tenants once per warm-slot. The ratio of
	// slots to RunDuration determines the per-tenant rate.
	type slot struct {
		tenant Tenant
		// nominal interval between writes for this tenant
		interval time.Duration
	}
	var schedule []slot
	hotInterval := time.Duration(float64(time.Second) / cfg.HotWritesPerSec)
	warmInterval := time.Duration(float64(time.Minute) / cfg.WarmWritesPerMin)
	for _, t := range tenants {
		switch t.Cohort {
		case CohortHot:
			schedule = append(schedule, slot{tenant: t, interval: hotInterval})
		case CohortWarm:
			schedule = append(schedule, slot{tenant: t, interval: warmInterval})
		}
	}
	rand.Shuffle(len(schedule), func(i, j int) {
		schedule[i], schedule[j] = schedule[j], schedule[i]
	})

	// Each slot gets its own goroutine emitting writes at `interval`.
	// With 100 hot @ 1Hz + 900 warm @ 1/min that's 100 + 15 = ~115
	// writes/sec aggregate during the 1-minute run, well under the
	// adapter's burst capacity.
	deadline, cancel := context.WithDeadline(ctx, time.Now().Add(cfg.RunDuration))
	defer cancel()

	var wg sync.WaitGroup
	for _, s := range schedule {
		wg.Add(1)
		go func(s slot) {
			defer wg.Done()
			// Initial offset: spread starts across the first
			// interval so 100 hot tenants don't all fire at t=0.
			jitter := time.Duration(rand.Int64N(int64(s.interval)))
			select {
			case <-time.After(jitter):
			case <-deadline.Done():
				return
			}
			ticker := time.NewTicker(s.interval)
			defer ticker.Stop()
			for {
				writeOne(deadline, store, s.tenant.ID, versions[s.tenant.ID], recorder)
				select {
				case <-deadline.Done():
					return
				case <-ticker.C:
				}
			}
		}(s)
	}
	wg.Wait()
	return nil
}

// writeOne does one synthetic Append for the supplied tenant +
// version counter and records the latency. Errors are routed to
// the recorder as failures; the run keeps going on a single
// failure (an OCC race is normal at concurrent load).
func writeOne(
	ctx context.Context,
	store es.Store,
	tenant string,
	versionCounter *atomic.Uint64,
	recorder *Recorder,
) {
	v := versionCounter.Add(1)
	expected := v - 1
	sid, _ := es.NewStreamID(tenant, "counter", "main")
	tenantCtx := es.WithTenant(ctx, tenant)

	start := time.Now()
	_, err := store.Append(tenantCtx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: expected,
		Events:          []es.EventToAppend{makeEvent(v)},
		NewStateBytes:   stateBytes(v),
		StateTypeURL:    StateTypeURL,
	})
	d := time.Since(start)

	if err != nil {
		// Deadline-cancelled is the expected end of the run; don't
		// pollute latency stats with timeouts.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return
		}
		recorder.Add("append", d, false)
		return
	}
	recorder.Add("append", d, true)
}
