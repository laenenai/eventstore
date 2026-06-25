//go:build soak

// Package bench_test — scenario C, the 7-day autovacuum soak.
//
// This file is build-tagged `soak` so it cannot be accidentally
// launched by a normal `go test`. The runbook
// (docs/spikes/0001-mac-studio-soak-runbook.md) is the operator
// procedure; do not run TestSoak_1M_7Day without reading it first.
package bench_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/laenenai/eventstore/estest/bench"
)

// TestSoak_1M_7Day is the scenario C measurement: 1M tenants, 168h
// sustained writes just under the advisory-lock ceiling, every 30
// minutes a heartbeat captures latency window + table stats + WAL
// bytes.
//
// Wall-time budget: 7 days + ~2-3h seed + 60s drain. Runbook says
// `-timeout 200h`. The heartbeat log lands at $SOAK_LOG (default
// ./spike-0001-soak.log).
//
// To run:
//
//	caffeinate -d -i -s -- go test -tags soak -timeout 200h \
//	  -run TestSoak_1M_7Day -v ./estest/bench/... \
//	  | tee ./spike-0001-soak-stdout.log
//
// To override defaults (e.g., for a 1-day shakeout before the full
// 7-day): set BENCH_SOAK_DURATION=24h, BENCH_SOAK_TENANTS=100000,
// BENCH_SOAK_RATE=80, BENCH_SOAK_LOG=/some/path. Empty values fall
// back to DefaultConfigC.
func TestSoak_1M_7Day(t *testing.T) {
	cfg := bench.DefaultConfigC()

	if v := os.Getenv("BENCH_SOAK_TENANTS"); v != "" {
		n, err := parseInt(v)
		if err != nil {
			t.Fatalf("BENCH_SOAK_TENANTS=%q: %v", v, err)
		}
		cfg.TenantsTotal = n
	}
	if v := os.Getenv("BENCH_SOAK_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("BENCH_SOAK_DURATION=%q: %v", v, err)
		}
		cfg.SoakDuration = d
	}
	if v := os.Getenv("BENCH_SOAK_RATE"); v != "" {
		r, err := parseFloat(v)
		if err != nil {
			t.Fatalf("BENCH_SOAK_RATE=%q: %v", v, err)
		}
		cfg.SustainedWritesPerSec = r
	}
	if v := os.Getenv("BENCH_SOAK_HEARTBEAT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("BENCH_SOAK_HEARTBEAT=%q: %v", v, err)
		}
		cfg.HeartbeatInterval = d
	}
	if v := os.Getenv("BENCH_SOAK_LOG"); v != "" {
		cfg.HeartbeatPath = v
	}

	t.Logf("scenario C config: tenants=%d duration=%s rate=%.1f/s heartbeat=%s log=%s",
		cfg.TenantsTotal, cfg.SoakDuration, cfg.SustainedWritesPerSec,
		cfg.HeartbeatInterval, cfg.HeartbeatPath)

	h := bench.Setup(t)

	// Test deadline = soak + estimated seed (8 ms/tenant) + 1 h drain.
	deadline := cfg.SoakDuration +
		time.Duration(cfg.TenantsTotal)*8*time.Millisecond +
		time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	res, err := bench.RunScenarioC(ctx, h, cfg)
	if err != nil {
		t.Errorf("RunScenarioC: %v", err)
	}

	t.Logf("scenario C result: %s", bench.CompactSummaryC(res))

	out, err := os.CreateTemp("", "spike-0001-scenario-c-*.md")
	if err != nil {
		t.Fatalf("create report file: %v", err)
	}
	defer out.Close()
	bench.ReportC(out, res)
	t.Logf("full report written to %s", out.Name())

	if os.Getenv("BENCH_VERBOSE") == "1" {
		bench.ReportC(testWriter{t}, res)
	}

	// Soft assertions. The brief's targets (autovacuum cycle < 1h,
	// no table > 24h without vacuum) are best checked by reading
	// the report — they're operationally meaningful but not
	// straightforward to gate on in a test that took 7 days.
	if res.EarlyTermination != "" {
		t.Errorf("soak ended early: %s", res.EarlyTermination)
	}
	if res.AppendSucc == 0 {
		t.Errorf("no successful appends — pacer or worker pool broken")
	}
	if len(res.Heartbeats) < 100 {
		// 168 h / 30 min = 336 heartbeats expected. Anything below
		// ~100 means the heartbeat goroutine was wedged.
		t.Errorf("only %d heartbeats captured; expected ~336 for a 7-day run",
			len(res.Heartbeats))
	}
}

func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

func parseFloat(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%g", &f)
	return f, err
}
