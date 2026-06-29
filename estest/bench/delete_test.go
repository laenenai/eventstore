package bench_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/laenenai/eventstore/estest/bench"
)

// TestDelete_10K is the scenario E smoke: seed 10K tenants, then erase
// all of them in parallel, asserting zero residual rows (the hard SLO)
// and reporting per-tenant purge + cascade-verify latency. Skipped when
// EVENTSTORE_SKIP_PG_TESTS=1.
func TestDelete_10K(t *testing.T) {
	runScenarioETier(t, bench.DefaultConfigE())
}

// TestDelete_100K — next tier. ~15 min wall time, dominated by the
// 100K seed. BENCH_TIER=100k or =all.
func TestDelete_100K(t *testing.T) {
	gateTier(t, "100k")
	cfg := bench.DefaultConfigE()
	cfg.TenantsTotal = 100_000
	runScenarioETier(t, cfg)
}

// TestDelete_1M is the population-ceiling tier, intended for the Mac
// Studio setup. BENCH_TIER=1m.
func TestDelete_1M(t *testing.T) {
	gateTier(t, "1m")
	cfg := bench.DefaultConfigE()
	cfg.TenantsTotal = 1_000_000
	runScenarioETier(t, cfg)
}

// runScenarioETier shares the timing + reporting body across the
// deletion tiers. Pattern matches runScenarioATier / runScenarioBTier.
func runScenarioETier(t *testing.T, cfg bench.ScenarioEConfig) {
	t.Helper()
	h := bench.Setup(t)

	// Per-tier timeout: ~8 ms per seed Append + a purge budget. The
	// purge is parallel (DeleteConcurrency workers), each tenant a
	// short transaction; budget 5 ms/tenant/worker plus a generous
	// floor for cold-cache and autovacuum interference.
	estimatedSeed := time.Duration(cfg.TenantsTotal) * 8 * time.Millisecond
	estimatedPurge := time.Duration(cfg.TenantsTotal/max(cfg.DeleteConcurrency, 1)) * 10 * time.Millisecond
	deadline := estimatedSeed + estimatedPurge + 60*time.Second
	if deadline < 5*time.Minute {
		deadline = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	res, err := bench.RunScenarioE(ctx, h, cfg)
	if err != nil {
		t.Fatalf("RunScenarioE tier=%d: %v", cfg.TenantsTotal, err)
	}

	t.Logf("tier=%d: %s", cfg.TenantsTotal, bench.CompactSummaryE(res))

	out, err := os.CreateTemp("", fmt.Sprintf("spike-0001-scenario-e-%d-*.md", cfg.TenantsTotal))
	if err != nil {
		t.Fatalf("create report file: %v", err)
	}
	defer out.Close()
	bench.ReportE(out, res)
	t.Logf("full report written to %s", out.Name())

	if os.Getenv("BENCH_VERBOSE") == "1" {
		bench.ReportE(testWriter{t}, res)
	}

	// HARD SLO: erasure must be complete. A single orphan row in any
	// tenant-scoped table is a cascade bug — fail the test, since
	// incomplete erasure is a GDPR-compliance defect, not a perf
	// data point to log and move past.
	if res.ResidualRows != 0 {
		t.Errorf("cascade incomplete: %d residual tenant-scoped rows after purge (want 0)",
			res.ResidualRows)
	}
	if res.DeletesFail != 0 {
		t.Errorf("%d tenant purges failed (want 0)", res.DeletesFail)
	}

	// Soft SLOs — informational on testcontainers; the real gate is
	// the main-vs-PR#35 delta on dedicated hardware.
	if res.DeleteLatencies.P99 >= 5*time.Second {
		t.Logf("⚠️  per-tenant delete p99 %s exceeds brief SLO 5s (informational)",
			res.DeleteLatencies.P99)
	}
	if res.CascadeLatencies.P99 >= 30*time.Second {
		t.Logf("⚠️  cascade-verify p99 %s exceeds brief SLO 30s (informational)",
			res.CascadeLatencies.P99)
	}
}
