package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/laenenai/eventstore/es"
)

// ScenarioCConfig drives the long-soak autovacuum measurement (spike
// 0001 scenario C). The defaults target the 7-day run on Mac Studio
// M3 Ultra at 1M tenants per docs/spikes/0001-mac-studio-soak-runbook.md.
//
// Why a separate config from scenario A: scenario A is per-tenant
// scheduled (each tenant has its own ticker), which would need ~100K
// idle goroutines at 1M scale. Scenario C uses a worker-pool +
// candidate-pool design instead — workers pull a random hot/warm
// tenant each tick and drive the configured aggregate rate.
type ScenarioCConfig struct {
	// TenantsTotal is the population. 1_000_000 for the 7-day soak.
	TenantsTotal int

	// SoakDuration is how long the steady-state phase runs after
	// seeding. 168h (7 days) for the runbook's primary measurement.
	SoakDuration time.Duration

	// SustainedWritesPerSec is the aggregate offered rate during the
	// soak — divided across hot + warm tenants. Default 167 stays
	// just under the advisory-lock ceiling (~180/sec on
	// testcontainers per spike §11.2.2) so the test measures
	// per-Append cost over time, not queue depth.
	SustainedWritesPerSec float64

	// HeartbeatInterval is how often the harness snapshots latency
	// percentiles + table stats + WAL bytes and flushes one line to
	// HeartbeatPath. 30 minutes is the runbook default — fine-grained
	// enough to see autovacuum cycles land, sparse enough to keep
	// the log under a few MB over 7 days.
	HeartbeatInterval time.Duration

	// SeedConcurrency parallelises the initial per-tenant Append.
	// 32 matches the pool's MaxConns; going higher just queues.
	SeedConcurrency int

	// RunConcurrency caps the writer goroutine count during the
	// soak. A small fixed pool is enough — pacing is the bottleneck,
	// not parallelism.
	RunConcurrency int

	// Tables names whose stats the heartbeat snapshots. Defaults to
	// the same hot tables scenario A tracks.
	Tables []string

	// HeartbeatPath is the file the soak appends to on every
	// heartbeat. Defaults to "./spike-0001-soak.log" — a relative
	// path that doesn't need root. The Mac Studio runbook redirects
	// this via go test's -v output as well.
	HeartbeatPath string

	// SeedProgressInterval controls how often the seed phase logs
	// progress to stderr. 1M tenants at ~8 ms each is ~2.2 h — the
	// operator needs to see liveness signals. Default 30 s.
	SeedProgressInterval time.Duration
}

// DefaultConfigC returns the 7-day-soak-at-1M defaults.
//
// The aggregate rate (167/sec) is calibrated against scenario A's
// observation (§11.2.2): advisory-lock-serialised writes sustain
// ~167-180/sec on testcontainers Docker on M1 Max. Setting the
// target slightly under that ceiling means the soak measures
// latency drift as state grows, not saturation. On Mac Studio M3
// Ultra the same rate is well within budget but produces a
// realistic write pattern adopters would see.
//
// For a quick code-path validation (not a real soak), reduce
// SoakDuration to 30s and TenantsTotal to 1000 — see
// TestSoak_CodepathSmoke.
func DefaultConfigC() ScenarioCConfig {
	return ScenarioCConfig{
		TenantsTotal:          1_000_000,
		SoakDuration:          7 * 24 * time.Hour,
		SustainedWritesPerSec: 167,
		HeartbeatInterval:     30 * time.Minute,
		SeedConcurrency:       32,
		RunConcurrency:        16,
		Tables: []string{
			"state_cache",
			"projection_checkpoint",
			"processed_events",
			"state_stream_subscribers",
			"events",
		},
		HeartbeatPath:        "./spike-0001-soak.log",
		SeedProgressInterval: 30 * time.Second,
	}
}

// ScenarioCResult is the soak's final summary. Persisted as markdown
// by ReportC. The Heartbeats slice is the time-series record of the
// soak — every 30-minute snapshot end-to-end.
type ScenarioCResult struct {
	TenantsTotal int
	TenantsCold  int
	TenantsWarm  int
	TenantsHot   int

	SeedDuration time.Duration
	SoakDuration time.Duration // actual, not configured

	AppendSucc int64
	AppendFail int64

	// LatencyOverall summarizes the LAST window only — full-soak
	// percentiles would need ~100M samples in memory at 1M-scale,
	// which we deliberately avoid. The Heartbeats slice carries
	// per-window percentiles for the time series.
	LatencyOverall LatencySummary

	TableStatsBefore []TableStat
	TableStatsAfter  []TableStat

	WALBytesAtStart int64
	WALBytesAtEnd   int64

	Heartbeats []HeartbeatSnapshot

	// EarlyTermination is set if the soak ended before SoakDuration
	// elapsed — e.g., operator cancellation or a hard error.
	EarlyTermination string
}

// HeartbeatSnapshot is one moment-in-time observation captured every
// HeartbeatInterval during the soak. Both the in-memory result and
// the on-disk log carry the same shape so post-hoc analysis is the
// same whether the soak completed or was killed.
type HeartbeatSnapshot struct {
	At                time.Time
	ElapsedSinceStart time.Duration
	CumulativeSucc    int64
	CumulativeFail    int64
	WindowLatency     LatencySummary
	WindowAppends     int64
	Tables            []TableStat
	WALBytesCumul     int64
}

// RunScenarioC executes the long-soak measurement.
//
// Phases:
//
//  1. Population: 90/9/1 cold/warm/hot, deterministic by index.
//  2. Seed: one Append per tenant; ~2.2 h at 1M on testcontainers,
//     faster on production hardware. Progress logs every
//     SeedProgressInterval so operators see liveness.
//  3. Baseline: pg_stat_user_tables + pg_current_wal_lsn().
//  4. Soak: workers pace at SustainedWritesPerSec for SoakDuration.
//     Each worker picks a random hot/warm tenant per tick.
//     Heartbeat goroutine wakes every HeartbeatInterval, snapshots
//     latency window + tables + WAL, appends to HeartbeatPath,
//     and adds to res.Heartbeats.
//  5. Endpoint: final stats snapshot.
//
// Returns a partial result + error on cancellation; the heartbeats
// already on disk and in res.Heartbeats are still usable for
// post-mortem.
func RunScenarioC(ctx context.Context, h *Harness, cfg ScenarioCConfig) (*ScenarioCResult, error) {
	tenants := Population(cfg.TenantsTotal)

	res := &ScenarioCResult{
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

	hb, err := openHeartbeat(cfg.HeartbeatPath)
	if err != nil {
		return res, fmt.Errorf("open heartbeat log: %w", err)
	}
	defer hb.Close()
	writeHeartbeatHeader(hb, cfg)

	// Phase 1: seed.
	seedStart := time.Now()
	if err := seedWithProgress(ctx, h.Adapter, tenants, cfg.SeedConcurrency, cfg.SeedProgressInterval, hb); err != nil {
		res.EarlyTermination = "seed failed: " + err.Error()
		return res, fmt.Errorf("seed: %w", err)
	}
	res.SeedDuration = time.Since(seedStart)
	fmt.Fprintf(hb, "[seed complete] tenants=%d duration=%s\n", cfg.TenantsTotal, res.SeedDuration)
	hb.Sync()

	// Phase 2: baseline.
	before, err := SampleTables(ctx, h.AdminPool, cfg.Tables)
	if err != nil {
		res.EarlyTermination = "baseline stats failed: " + err.Error()
		return res, fmt.Errorf("baseline stats: %w", err)
	}
	res.TableStatsBefore = before
	res.WALBytesAtStart, _ = sampleWALBytes(ctx, h.AdminPool)

	// Phase 3: soak.
	soakStart := time.Now()
	if err := soakLoop(ctx, h, tenants, cfg, hb, res); err != nil {
		res.SoakDuration = time.Since(soakStart)
		res.EarlyTermination = fmt.Sprintf("soak terminated at %s: %v", res.SoakDuration, err)
	} else {
		res.SoakDuration = time.Since(soakStart)
	}

	// Phase 4: endpoint.
	after, err := SampleTables(context.Background(), h.AdminPool, cfg.Tables)
	if err == nil {
		res.TableStatsAfter = after
	}
	res.WALBytesAtEnd, _ = sampleWALBytes(context.Background(), h.AdminPool)

	// LatencyOverall reflects the most recent non-empty heartbeat
	// window. The very last heartbeat fires on deadline regardless of
	// whether any writes hit between it and the previous tick; that
	// trailing one can land with Count==0 if the recorder was drained
	// at the tick immediately before. Walk backwards to find a
	// meaningful window — that's the representative steady-state at
	// end of soak.
	for i := len(res.Heartbeats) - 1; i >= 0; i-- {
		if res.Heartbeats[i].WindowLatency.Count > 0 {
			res.LatencyOverall = res.Heartbeats[i].WindowLatency
			break
		}
	}

	fmt.Fprintf(hb, "[soak complete] duration=%s succ=%d fail=%d\n",
		res.SoakDuration, res.AppendSucc, res.AppendFail)
	hb.Sync()

	return res, nil
}

// soakLoop runs the writer pool + heartbeat goroutine for
// cfg.SoakDuration or until ctx cancels. Tenants pool (hot+warm) is
// chosen randomly per write; cold tenants are untouched per the
// scenario brief.
func soakLoop(
	ctx context.Context,
	h *Harness,
	tenants []Tenant,
	cfg ScenarioCConfig,
	hb interface {
		Write([]byte) (int, error)
		Sync() error
	},
	res *ScenarioCResult,
) error {
	versions := make(map[string]*atomic.Uint64, len(tenants))
	for _, t := range tenants {
		v := new(atomic.Uint64)
		v.Store(1) // seed wrote v1; next is v2
		versions[t.ID] = v
	}

	// Build the active candidate pool: hot + warm only.
	candidates := make([]Tenant, 0, res.TenantsHot+res.TenantsWarm)
	for _, t := range tenants {
		if t.Cohort == CohortHot || t.Cohort == CohortWarm {
			candidates = append(candidates, t)
		}
	}
	if len(candidates) == 0 {
		return errors.New("no active candidates (hot+warm pool is empty)")
	}

	// Pacing: a single token bucket emits writeReq onto a buffered
	// channel. Workers consume. When the queue fills, the bucket
	// drops ticks — that drop IS the saturation signal.
	deadline := time.Now().Add(cfg.SoakDuration)
	deadlineCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	jobs := make(chan Tenant, cfg.RunConcurrency*4)

	recorder := NewRecorder("append")
	startedAt := time.Now()

	// Heartbeat goroutine: wakes every HeartbeatInterval, snapshots
	// the recorder, drains it (so the next window is fresh), samples
	// tables, appends to log + res.Heartbeats.
	var hbWG sync.WaitGroup
	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		ticker := time.NewTicker(cfg.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-deadlineCtx.Done():
				// Last heartbeat on the way out.
				captureHeartbeat(context.Background(), h.AdminPool, cfg, recorder, startedAt, hb, res)
				return
			case <-ticker.C:
				captureHeartbeat(deadlineCtx, h.AdminPool, cfg, recorder, startedAt, hb, res)
			}
		}
	}()

	// Worker pool: each worker pulls a tenant from jobs and Appends.
	// Workers exit on deadlineCtx.Done() rather than waiting for a
	// jobs-channel close — the pacer might be mid-send when the
	// deadline fires, and closing the channel under it would panic.
	// Letting workers and pacer both observe deadlineCtx avoids that.
	var wg sync.WaitGroup
	for range cfg.RunConcurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-deadlineCtx.Done():
					return
				case t := <-jobs:
					writeOne(deadlineCtx, h.Adapter, t.ID, versions[t.ID], recorder)
				}
			}
		}()
	}

	// Pacer: emits one tenant per tick at the configured rate.
	// time.Ticker rate clamps at the goroutine scheduler's
	// resolution (~1ms); for rates > 1000 we'd need a different
	// strategy, but 167/sec is comfortable.
	interval := time.Duration(float64(time.Second) / cfg.SustainedWritesPerSec)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	pacer := time.NewTicker(interval)
	defer pacer.Stop()
	var pacerWG sync.WaitGroup
	pacerWG.Add(1)
	go func() {
		defer pacerWG.Done()
		for {
			select {
			case <-deadlineCtx.Done():
				return
			case <-pacer.C:
				idx := rand.IntN(len(candidates))
				select {
				case jobs <- candidates[idx]:
				case <-deadlineCtx.Done():
					return
				default:
					// Queue full — drop the tick. Recorded
					// implicitly by lower Cumulative counts vs.
					// expected at the deadline.
				}
			}
		}
	}()

	<-deadlineCtx.Done()
	pacerWG.Wait()
	wg.Wait()
	hbWG.Wait()

	// Final cumulative counters — assign, do NOT +=. The heartbeat
	// goroutine already kept res.AppendSucc/Fail updated; the
	// recorder's atomic counters survive Drain() so this is the
	// authoritative read.
	_, succ, fail := recorder.Snapshot()
	res.AppendSucc = succ["append"]
	res.AppendFail = fail["append"]

	if errors.Is(deadlineCtx.Err(), context.DeadlineExceeded) {
		return nil // soak completed naturally
	}
	// Parent context cancelled (operator Ctrl-C, test timeout).
	return ctx.Err()
}

// captureHeartbeat takes one snapshot of latency window + table
// stats + WAL bytes, appends a line to the heartbeat log, and
// records it in res.Heartbeats. The recorder is DRAINED so the next
// window is fresh — otherwise a 7-day in-memory sample slice would
// exhaust memory.
func captureHeartbeat(
	ctx context.Context,
	admin *pgxpool.Pool,
	cfg ScenarioCConfig,
	recorder *Recorder,
	startedAt time.Time,
	hb interface {
		Write([]byte) (int, error)
		Sync() error
	},
	res *ScenarioCResult,
) {
	samples, succ, fail := recorder.Snapshot()
	recorder.Drain()

	window := summarize(samples)
	tables, _ := SampleTables(ctx, admin, cfg.Tables)
	wal, _ := sampleWALBytes(ctx, admin)

	snap := HeartbeatSnapshot{
		At:                time.Now(),
		ElapsedSinceStart: time.Since(startedAt),
		CumulativeSucc:    succ["append"],
		CumulativeFail:    fail["append"],
		WindowLatency:     window,
		WindowAppends:     int64(len(samples)),
		Tables:            tables,
		WALBytesCumul:     wal,
	}
	res.Heartbeats = append(res.Heartbeats, snap)
	// Cumulative counters bump immediately so a mid-soak crash report
	// still shows the latest totals.
	res.AppendSucc = succ["append"]
	res.AppendFail = fail["append"]

	writeHeartbeatLine(hb, snap)
}

// seedWithProgress is seed() with an operator-visible progress feed.
// On 1M tenants the seed alone runs for hours; without progress the
// operator can't tell if the test is wedged or working.
func seedWithProgress(
	ctx context.Context,
	store es.Store,
	tenants []Tenant,
	concurrency int,
	progressInterval time.Duration,
	hb interface {
		Write([]byte) (int, error)
		Sync() error
	},
) error {
	if concurrency < 1 {
		concurrency = 1
	}
	in := make(chan Tenant, concurrency*2)
	errCh := make(chan error, concurrency)
	var done atomic.Int64
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
				done.Add(1)
			}
		}()
	}

	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	go func() {
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()
		startedAt := time.Now()
		for {
			select {
			case <-progressCtx.Done():
				return
			case <-ticker.C:
				n := done.Load()
				elapsed := time.Since(startedAt)
				rate := float64(n) / elapsed.Seconds()
				fmt.Fprintf(hb, "[seed progress] done=%d/%d elapsed=%s rate=%.1f/s\n",
					n, len(tenants), elapsed.Round(time.Second), rate)
				hb.Sync()
			}
		}
	}()

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

// sampleWALBytes returns the current WAL position as bytes (parsed
// from pg_current_wal_lsn). Used to compute generated WAL volume
// over the soak window. Returns 0 on any error — a missing pg_lsn
// is informational, not fatal.
func sampleWALBytes(ctx context.Context, admin *pgxpool.Pool) (int64, error) {
	var bytes int64
	err := admin.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::BIGINT`,
	).Scan(&bytes)
	return bytes, err
}
