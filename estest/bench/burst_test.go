package bench_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/laenenai/eventstore/estest/bench"
)

// TestBurst_10K is the scenario B smoke. 10K tenants, 10K writes
// offered over a 10s window — ~1000/sec, ~5.5× the measured
// advisory-lock ceiling — so we deliberately saturate. The point is
// to observe queue depth, latency tail (p99.9), and the deadlock
// count under burst. Skipped when EVENTSTORE_SKIP_PG_TESTS=1.
//
// Target wall time: < 90s, dominated by the 10K seed phase
// (~55s at ~5.5 ms per Append). The burst itself is 10s + drain.
func TestBurst_10K(t *testing.T) {
	runScenarioBTier(t, bench.DefaultConfigB())
}

// TestBurst_100K is the brief's nominal scenario B configuration:
// 100K writes spread across 100K tenants over 60s. ~1667/sec
// offered ≈ 9× the ceiling. ~10-15 min wall time. BENCH_TIER=100k.
func TestBurst_100K(t *testing.T) {
	gateTier(t, "100k")
	cfg := bench.DefaultConfigB()
	cfg.TenantsTotal = 100_000
	cfg.BurstDuration = 60 * time.Second
	cfg.TargetWrites = 100_000
	runScenarioBTier(t, cfg)
}

// TestBurst_500K — population scaled, burst still 100K writes/60s
// per the brief (it's a burst-over-population test, not a
// throughput test). Tests whether the larger state_cache footprint
// changes the burst's latency profile. BENCH_TIER=500k.
func TestBurst_500K(t *testing.T) {
	gateTier(t, "500k")
	cfg := bench.DefaultConfigB()
	cfg.TenantsTotal = 500_000
	cfg.BurstDuration = 60 * time.Second
	cfg.TargetWrites = 100_000
	runScenarioBTier(t, cfg)
}

// TestBurst_1M is the population-ceiling tier. Same 100K/60s burst
// against a 1M-tenant state_cache. Long wall time dominated by the
// seed; intended for the Mac Studio soak. BENCH_TIER=1m.
func TestBurst_1M(t *testing.T) {
	gateTier(t, "1m")
	cfg := bench.DefaultConfigB()
	cfg.TenantsTotal = 1_000_000
	cfg.BurstDuration = 60 * time.Second
	cfg.TargetWrites = 100_000
	runScenarioBTier(t, cfg)
}

// runScenarioBTier shares the timing + reporting body across the
// burst tiers. Pattern matches runScenarioATier from smoke_test.go.
func runScenarioBTier(t *testing.T, cfg bench.ScenarioBConfig) {
	t.Helper()
	h := bench.Setup(t)

	// Per-tier timeout: ~8 ms per seed Append + burst window + a
	// generous drain buffer. The burst can leave in-flight Appends
	// queued behind the advisory lock; 2× the burst window gives
	// the slowest tail Append time to land.
	estimatedSeed := time.Duration(cfg.TenantsTotal) * 8 * time.Millisecond
	drainBuffer := 2 * cfg.BurstDuration
	if drainBuffer < 60*time.Second {
		drainBuffer = 60 * time.Second
	}
	deadline := estimatedSeed + cfg.BurstDuration + drainBuffer
	if deadline < 5*time.Minute {
		deadline = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	res, err := bench.RunScenarioB(ctx, h, cfg)
	if err != nil {
		t.Fatalf("RunScenarioB tier=%d: %v", cfg.TenantsTotal, err)
	}

	t.Logf("tier=%d: %s", cfg.TenantsTotal, bench.CompactSummaryB(res))

	out, err := os.CreateTemp("", fmt.Sprintf("spike-0001-scenario-b-%d-*.md", cfg.TenantsTotal))
	if err != nil {
		t.Fatalf("create report file: %v", err)
	}
	defer out.Close()
	bench.ReportB(out, res)
	t.Logf("full report written to %s", out.Name())

	if os.Getenv("BENCH_VERBOSE") == "1" {
		bench.ReportB(testWriter{t}, res)
	}

	// Deadlock count is the only HARD SLO per the brief; non-zero
	// is interesting data for the spike, not a thing to paper
	// over by tuning the harness. Log loudly but do not fail the
	// test — the spike author decides whether the count crosses
	// the "catastrophic" line.
	if res.Deadlocks > 0 {
		t.Logf("⚠️  deadlocks detected: %d (brief hard SLO is 0 — interesting data for spike)",
			res.Deadlocks)
	}

	// Soft assertions on the latency SLOs. Informational only at
	// the smoke tier — the burst is deliberately above the
	// sustained ceiling so p99/p99.9 are expected to exceed the
	// brief's targets on testcontainers. The real comparison
	// happens at the 1M tier on dedicated hardware.
	if res.AppendLatencies.P99 >= 500*time.Millisecond {
		t.Logf("⚠️  p99 %s exceeds brief SLO 500ms (informational at smoke tier)",
			res.AppendLatencies.P99)
	}
	if res.AppendLatencies.P999 >= 2*time.Second {
		t.Logf("⚠️  p99.9 %s exceeds brief SLO 2s (informational at smoke tier)",
			res.AppendLatencies.P999)
	}
}
