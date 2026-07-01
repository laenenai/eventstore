package bench

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// ReportE writes a markdown summary of a scenario E (tenant deletion)
// run to w, shaped to drop into the spike doc's §11.3 results table.
// Mirrors Report / ReportB / ReportC.
func ReportE(w io.Writer, res ScenarioEResult) {
	fmt.Fprintln(w, "## Scenario E — tenant deletion")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "**Population:** %d tenants seeded then erased\n", res.TenantsTotal)
	fmt.Fprintf(w, "**Seed:** %s (one Append per tenant)\n", res.SeedDuration.Round(time.Millisecond))
	fmt.Fprintf(w, "**Purge:** %s wall-clock for the full parallel erasure\n",
		res.DeleteDuration.Round(time.Millisecond))
	fmt.Fprintf(w, "**Erased:** %d succeeded, %d failed\n", res.DeletesSucc, res.DeletesFail)
	fmt.Fprintf(w, "**Tables cleared per tenant:** %s\n", strings.Join(purgeTables, ", "))
	fmt.Fprintln(w)

	fmt.Fprintln(w, "### Per-tenant latency")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Op | count | p50 | p95 | p99 | max |")
	fmt.Fprintln(w, "| --- | --- | --- | --- | --- | --- |")
	for _, row := range []struct {
		op  string
		lat LatencySummary
	}{
		{"delete (purge txn)", res.DeleteLatencies},
		{"verify cascade", res.CascadeLatencies},
	} {
		fmt.Fprintf(w, "| %s | %d | %s | %s | %s | %s |\n",
			row.op, row.lat.Count,
			fmtDur(row.lat.P50), fmtDur(row.lat.P95), fmtDur(row.lat.P99), fmtDur(row.lat.Max))
	}
	fmt.Fprintln(w)

	// SLO check vs the brief. Residual is the HARD gate; the two
	// latency targets are soft (informational on testcontainers — the
	// real signal is the main-vs-PR#35 delta on dedicated hardware).
	fmt.Fprintln(w, "### Spike brief SLO check")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Metric | Measured | Target | Status |")
	fmt.Fprintln(w, "| --- | --- | --- | --- |")
	fmt.Fprintf(w, "| Per-tenant delete p99 | %s | < 5 s | %s |\n",
		fmtDur(res.DeleteLatencies.P99), passFail(res.DeleteLatencies.P99 < 5*time.Second))
	fmt.Fprintf(w, "| Cascade-verify p99 | %s | < 30 s | %s |\n",
		fmtDur(res.CascadeLatencies.P99), passFail(res.CascadeLatencies.P99 < 30*time.Second))
	fmt.Fprintf(w, "| Residual rows after purge | %d | 0 (hard) | %s |\n",
		res.ResidualRows, passFail(res.ResidualRows == 0))
	fmt.Fprintln(w)

	if len(res.TableStatsBefore) == len(res.TableStatsAfter) && len(res.TableStatsAfter) > 0 {
		fmt.Fprintln(w, "### Storage reclaim — start → end")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "| Table | Δ live | Δ dead | Δ size KB | autovacuum n |")
		fmt.Fprintln(w, "| --- | --- | --- | --- | --- |")
		for i := range res.TableStatsBefore {
			b := res.TableStatsBefore[i]
			a := res.TableStatsAfter[i]
			fmt.Fprintf(w, "| `%s` | %+d | %+d | %+d | %d |\n",
				a.Table,
				a.LiveTup-b.LiveTup,
				a.DeadTup-b.DeadTup,
				a.TotalRelSizeKB-b.TotalRelSizeKB,
				a.NAutoVacuum-b.NAutoVacuum,
			)
		}
		fmt.Fprintln(w)
	}
}

// CompactSummaryE is the one-line test-log digest, mirroring
// CompactSummary / CompactSummaryB / CompactSummaryC.
func CompactSummaryE(res ScenarioEResult) string {
	return strings.Join([]string{
		fmt.Sprintf("tenants=%d", res.TenantsTotal),
		fmt.Sprintf("seed=%s", res.SeedDuration.Round(time.Millisecond)),
		fmt.Sprintf("purge=%s", res.DeleteDuration.Round(time.Millisecond)),
		fmt.Sprintf("erased=%d/%d", res.DeletesSucc, res.DeletesSucc+res.DeletesFail),
		fmt.Sprintf("del_p99=%s", fmtDur(res.DeleteLatencies.P99)),
		fmt.Sprintf("cascade_p99=%s", fmtDur(res.CascadeLatencies.P99)),
		fmt.Sprintf("residual=%d", res.ResidualRows),
	}, " ")
}
