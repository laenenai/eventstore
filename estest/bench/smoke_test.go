package bench_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/laenenai/eventstore/estest/bench"
)

// TestSmoke_10K is the default scenario A run from spike 0001.
// Runs a 90/9/1 population of 10K tenants against testcontainers
// Postgres, drives a minute of steady-state writes, prints a
// markdown summary. Skipped when EVENTSTORE_SKIP_PG_TESTS=1.
func TestSmoke_10K(t *testing.T) {
	runScenarioATier(t, bench.DefaultConfig())
}

// TestSmoke_100K — next tier up. ~10 min wall time on a developer
// Mac (mostly seed phase). Skipped unless BENCH_TIER=100k or
// BENCH_TIER=all, since the seed at 100K is too slow to run in a
// casual `go test` invocation.
func TestSmoke_100K(t *testing.T) {
	gateTier(t, "100k")
	cfg := bench.DefaultConfig()
	cfg.TenantsTotal = 100_000
	cfg.RunDuration = 60 * time.Second
	runScenarioATier(t, cfg)
}

// TestSmoke_500K runs the 500K-tenant tier. ~45-60 min wall time
// dominated by the seed phase (advisory-lock-serialised Appends
// can't parallelise past Postgres's single-writer throughput
// ceiling per ADR 0009). BENCH_TIER=500k or =all.
func TestSmoke_500K(t *testing.T) {
	gateTier(t, "500k")
	cfg := bench.DefaultConfig()
	cfg.TenantsTotal = 500_000
	cfg.RunDuration = 60 * time.Second
	runScenarioATier(t, cfg)
}

// TestSmoke_1M is the 1M-tenant tier. Long run; intended for the
// Mac Studio M3 Ultra setup per
// docs/spikes/0001-mac-studio-soak-runbook.md. BENCH_TIER=1m to
// enable.
func TestSmoke_1M(t *testing.T) {
	gateTier(t, "1m")
	cfg := bench.DefaultConfig()
	cfg.TenantsTotal = 1_000_000
	cfg.RunDuration = 60 * time.Second
	runScenarioATier(t, cfg)
}

// runScenarioATier is the shared body. Tier-specific drivers stay
// declarative; this function carries the timing, reporting, and
// soft-SLO check logic.
func runScenarioATier(t *testing.T, cfg bench.ScenarioAConfig) {
	t.Helper()
	h := bench.Setup(t)

	// Per-tier timeout estimate: ~8 ms per seed Append (the
	// observed 10K rate was ~5.5 ms but cold-cache and larger
	// volumes slow it down) + RunDuration + 60 s buffer.
	estimatedSeed := time.Duration(cfg.TenantsTotal) * 8 * time.Millisecond
	deadline := estimatedSeed + cfg.RunDuration + 60*time.Second
	if deadline < 5*time.Minute {
		deadline = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	res, err := bench.RunScenarioA(ctx, h, cfg)
	if err != nil {
		t.Fatalf("RunScenarioA tier=%d: %v", cfg.TenantsTotal, err)
	}

	t.Logf("tier=%d: %s", cfg.TenantsTotal, bench.CompactSummary(res))

	out, err := os.CreateTemp("", fmt.Sprintf("spike-0001-scenario-a-%d-*.md", cfg.TenantsTotal))
	if err != nil {
		t.Fatalf("create report file: %v", err)
	}
	defer out.Close()
	bench.Report(out, res)
	t.Logf("full report written to %s", out.Name())

	if os.Getenv("BENCH_VERBOSE") == "1" {
		bench.Report(testWriter{t}, res)
	}

	// Soft assertions on the brief's SLOs. Informational across
	// every tier on testcontainers — the real gate is the
	// apples-to-apples DELTA between main and PR #35.
	if res.AppendLatencies.P50 >= 20*time.Millisecond {
		t.Logf("⚠️  p50 %s exceeds brief SLO 20ms (informational)",
			res.AppendLatencies.P50)
	}
	if res.AppendLatencies.P99 >= 100*time.Millisecond {
		t.Logf("⚠️  p99 %s exceeds brief SLO 100ms (informational)",
			res.AppendLatencies.P99)
	}
}

// gateTier short-circuits a tier-specific test unless the user has
// explicitly opted in via BENCH_TIER. Default `go test ./...` runs
// the 10K smoke only.
//
// BENCH_TIER values: "10k" (default; uses TestSmoke_10K which is
// always on), "100k", "500k", "1m", or "all" to run every tier.
func gateTier(t *testing.T, tier string) {
	t.Helper()
	want := os.Getenv("BENCH_TIER")
	if want == "" {
		t.Skipf("BENCH_TIER unset; skipping %s tier (set BENCH_TIER=%s or BENCH_TIER=all)",
			tier, tier)
	}
	if want != "all" && want != tier {
		t.Skipf("BENCH_TIER=%s; this test is %s", want, tier)
	}
}

// testWriter adapts *testing.T to io.Writer so the markdown report
// shows up in -v test output.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
