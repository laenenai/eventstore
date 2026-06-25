package bench

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// ReportB writes a human-readable summary of a scenario B run to w.
// Markdown layout mirrors Report() (scenario A) so the spike doc's
// §11.4 results table can paste both side-by-side.
//
// Brief SLOs for scenario B (per the original spike doc):
//   - Total ingest p99 latency < 500 ms
//   - Tail (p99.9)             < 2 s
//   - Deadlock chains          0  (hard fail otherwise)
//
// Projection-lag metrics are deliberately omitted — the harness
// scope decision (§11.2.6) drops the workflow runtime, so there
// are no projections to lag.
func ReportB(w io.Writer, res *ScenarioBResult) {
	fmt.Fprintln(w, "## Scenario B — mass write burst")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "**Population:** %d total — %d cold / %d warm / %d hot\n",
		res.TenantsTotal, res.TenantsCold, res.TenantsWarm, res.TenantsHot)
	fmt.Fprintf(w, "**Seed:** %s (one Append per tenant)\n", res.SeedDuration.Round(time.Millisecond))
	fmt.Fprintf(w, "**Burst window:** %s\n", res.BurstDuration.Round(time.Millisecond))
	fmt.Fprintf(w, "**Wall:** %s (seed + burst + drain)\n", res.WallDuration.Round(time.Millisecond))
	fmt.Fprintln(w)

	achievedRate := 0.0
	if res.BurstDuration > 0 {
		achievedRate = float64(res.Achieved) / res.BurstDuration.Seconds()
	}
	offeredRate := 0.0
	if res.BurstDuration > 0 {
		offeredRate = float64(res.Offered) / res.BurstDuration.Seconds()
	}

	targetRate := 0.0
	if res.BurstDuration > 0 {
		targetRate = float64(res.TargetWrites) / res.BurstDuration.Seconds()
	}

	fmt.Fprintln(w, "### Target vs offered vs achieved")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Metric | Value | Notes |")
	fmt.Fprintln(w, "| --- | --- | --- |")
	fmt.Fprintf(w, "| Target writes (config) | %d (%.0f/sec nominal) | The brief's \"simultaneous writes\" demand. |\n", res.TargetWrites, targetRate)
	fmt.Fprintf(w, "| Offered writes (queued) | %d (%.0f/sec) | Pacer back-pressured when < target — worker pool saturated. |\n", res.Offered, offeredRate)
	fmt.Fprintf(w, "| Achieved writes (Append called) | %d (%.0f/sec) | Should == Offered (we don't cancel in-flight). |\n", res.Achieved, achievedRate)
	fmt.Fprintf(w, "| Succeeded | %d | |\n", res.AppendSucc)
	fmt.Fprintf(w, "| Failed | %d | See failure-reasons table below. |\n", res.AppendFail)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "### Append latency (burst phase)")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Count | p50 | p95 | p99 | p99.9 | max |")
	fmt.Fprintln(w, "| --- | --- | --- | --- | --- | --- |")
	fmt.Fprintf(w, "| %d | %s | %s | %s | %s | %s |\n",
		res.AppendLatencies.Count,
		fmtDur(res.AppendLatencies.P50),
		fmtDur(res.AppendLatencies.P95),
		fmtDur(res.AppendLatencies.P99),
		fmtDur(res.AppendLatencies.P999),
		fmtDur(res.AppendLatencies.Max),
	)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "### Spike brief SLO check")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Metric | Measured | Target | Status |")
	fmt.Fprintln(w, "| --- | --- | --- | --- |")
	fmt.Fprintf(w, "| Append p99 | %s | < 500 ms | %s |\n",
		fmtDur(res.AppendLatencies.P99),
		passFail(res.AppendLatencies.P99 < 500*time.Millisecond))
	fmt.Fprintf(w, "| Append p99.9 | %s | < 2 s | %s |\n",
		fmtDur(res.AppendLatencies.P999),
		passFail(res.AppendLatencies.P999 < 2*time.Second))
	fmt.Fprintf(w, "| Deadlocks | %d | 0 (hard) | %s |\n",
		res.Deadlocks,
		passFail(res.Deadlocks == 0))
	fmt.Fprintln(w)

	fmt.Fprintln(w, "### Failure reasons")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Reason | Count |")
	fmt.Fprintln(w, "| --- | --- |")
	// Stable order so reports diff cleanly.
	for _, k := range []string{"deadlock", "conflict", "context-deadline", "other"} {
		fmt.Fprintf(w, "| %s | %d |\n", k, res.FailureReasons[k])
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "### Table stats — start → end")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Table | Δ live | Δ dead | Δ upd | Δ hot-upd | HOT % | Δ size KB | autovacuum n |")
	fmt.Fprintln(w, "| --- | --- | --- | --- | --- | --- | --- | --- |")
	for i := range res.TableStatsBefore {
		b := res.TableStatsBefore[i]
		a := res.TableStatsAfter[i]
		dUpd := a.NTupUpd - b.NTupUpd
		dHot := a.NTupHotUpd - b.NTupHotUpd
		hotPct := "—"
		if dUpd > 0 {
			hotPct = fmt.Sprintf("%.1f%%", 100*float64(dHot)/float64(dUpd))
		}
		fmt.Fprintf(w, "| `%s` | %+d | %+d | %+d | %+d | %s | %+d | %d |\n",
			a.Table,
			a.LiveTup-b.LiveTup,
			a.DeadTup-b.DeadTup,
			dUpd,
			dHot,
			hotPct,
			a.TotalRelSizeKB-b.TotalRelSizeKB,
			a.NAutoVacuum-b.NAutoVacuum,
		)
	}
	fmt.Fprintln(w)
}

// CompactSummaryB returns a single-line digest for `go test` output.
// Matches CompactSummary's shape so log filtering tools see the
// same format across scenarios.
func CompactSummaryB(res *ScenarioBResult) string {
	return strings.Join([]string{
		fmt.Sprintf("tenants=%d", res.TenantsTotal),
		fmt.Sprintf("burst=%s", res.BurstDuration.Round(time.Millisecond)),
		fmt.Sprintf("target=%d", res.TargetWrites),
		fmt.Sprintf("offered=%d", res.Offered),
		fmt.Sprintf("achieved=%d", res.Achieved),
		fmt.Sprintf("p50=%s", fmtDur(res.AppendLatencies.P50)),
		fmt.Sprintf("p99=%s", fmtDur(res.AppendLatencies.P99)),
		fmt.Sprintf("p99.9=%s", fmtDur(res.AppendLatencies.P999)),
		fmt.Sprintf("deadlocks=%d", res.Deadlocks),
	}, " ")
}
