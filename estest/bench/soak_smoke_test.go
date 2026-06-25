package bench_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/laenenai/eventstore/estest/bench"
)

// TestSoak_CodepathSmoke exercises every phase of scenario C — seed,
// baseline, sustained writes, heartbeat capture, endpoint stats — in
// a CI-friendly window. This is NOT a soak measurement; it validates
// that the soak harness compiles and runs end-to-end against a real
// testcontainers Postgres before someone schedules the 7-day run.
//
// Wall-time budget: ~60s. 1000 tenants, 20s soak, 5s heartbeat
// interval, 10 writes/sec aggregate. Two heartbeats expected.
//
// Build-tag-gated TestSoak_1M_7Day (in soak_test.go) is the real
// thing; that one expects ~7 days wall and is excluded from `go
// test` unless `-tags soak` is set.
func TestSoak_CodepathSmoke(t *testing.T) {
	h := bench.Setup(t)

	cfg := bench.DefaultConfigC()
	cfg.TenantsTotal = 1000
	cfg.SoakDuration = 20 * time.Second
	cfg.SustainedWritesPerSec = 10
	cfg.HeartbeatInterval = 5 * time.Second
	cfg.SeedConcurrency = 32
	cfg.RunConcurrency = 4
	cfg.SeedProgressInterval = 30 * time.Second
	cfg.HeartbeatPath = filepath.Join(t.TempDir(), "smoke-soak.log")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	res, err := bench.RunScenarioC(ctx, h, cfg)
	if err != nil {
		t.Fatalf("RunScenarioC: %v", err)
	}

	t.Logf("soak-smoke: %s", bench.CompactSummaryC(res))

	if res.EarlyTermination != "" {
		t.Fatalf("unexpected early termination: %s", res.EarlyTermination)
	}
	if res.AppendSucc == 0 {
		t.Fatalf("no successful appends recorded — pacer or worker pool not wired")
	}
	if len(res.Heartbeats) == 0 {
		t.Fatalf("no heartbeats captured — interval %s vs soak %s should fire ≥2",
			cfg.HeartbeatInterval, cfg.SoakDuration)
	}
	if len(res.TableStatsBefore) == 0 || len(res.TableStatsAfter) == 0 {
		t.Fatalf("missing baseline/endpoint stats: before=%d after=%d",
			len(res.TableStatsBefore), len(res.TableStatsAfter))
	}

	// Heartbeat file should exist and have content.
	stat, err := os.Stat(cfg.HeartbeatPath)
	if err != nil {
		t.Fatalf("stat heartbeat file: %v", err)
	}
	if stat.Size() == 0 {
		t.Fatalf("heartbeat file empty at %s", cfg.HeartbeatPath)
	}

	// Final-window summary should be populated from the last heartbeat.
	if res.LatencyOverall.Count == 0 {
		t.Fatalf("LatencyOverall not populated from final heartbeat")
	}
}
