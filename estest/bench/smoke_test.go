package bench_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/laenenai/eventstore/estest/bench"
)

// TestSmoke_10K is the first scenario A run from spike 0001. Runs a
// 90/9/1 population of 10K tenants against testcontainers Postgres,
// drives a minute of steady-state writes, and prints a markdown
// summary of latency + table-stat deltas the spike's report
// expects.
//
// Skipped when EVENTSTORE_SKIP_PG_TESTS=1 (matches the adapter's
// integration tests). Set BENCH_VERBOSE=1 for the full markdown
// report on stdout; otherwise the test prints a one-line summary
// and writes the full report to a temp file the operator can grep.
func TestSmoke_10K(t *testing.T) {
	h := bench.Setup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := bench.DefaultConfig() // 10K tenants, 60 s run, hot 1Hz / warm 1/min

	res, err := bench.RunScenarioA(ctx, h, cfg)
	if err != nil {
		t.Fatalf("RunScenarioA: %v", err)
	}

	t.Logf("smoke: %s", bench.CompactSummary(res))

	// Always write the full report to a temp file so devs can read
	// it after the run regardless of -v / BENCH_VERBOSE.
	out, err := os.CreateTemp("", "spike-0001-scenario-a-smoke-*.md")
	if err != nil {
		t.Fatalf("create report file: %v", err)
	}
	defer out.Close()
	bench.Report(out, res)
	t.Logf("full report written to %s", out.Name())

	if os.Getenv("BENCH_VERBOSE") == "1" {
		bench.Report(testWriter{t}, res)
	}

	// Soft assertions on the brief's SLOs. We don't fail the test on
	// SLO miss at the 10K smoke — the goal is to surface numbers,
	// not gate CI on them. Phase 1 runs against managed Postgres at
	// larger tiers will be the real gate.
	if res.AppendLatencies.P50 >= 20*time.Millisecond {
		t.Logf("⚠️  p50 %s exceeds brief SLO 20ms (informational at 10K)",
			res.AppendLatencies.P50)
	}
	if res.AppendLatencies.P99 >= 100*time.Millisecond {
		t.Logf("⚠️  p99 %s exceeds brief SLO 100ms (informational at 10K)",
			res.AppendLatencies.P99)
	}
}

// testWriter adapts *testing.T to io.Writer so the markdown report
// shows up in -v test output.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
