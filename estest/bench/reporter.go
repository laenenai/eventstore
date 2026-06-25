package bench

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// Report writes a human-readable summary of a scenario A run to w.
// The output is markdown so it can be pasted directly into the
// spike doc's §11 results tables.
func Report(w io.Writer, res *ScenarioAResult) {
	fmt.Fprintln(w, "## Scenario A — steady-state smoke")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "**Population:** %d total — %d cold / %d warm / %d hot\n",
		res.TenantsTotal, res.TenantsCold, res.TenantsWarm, res.TenantsHot)
	fmt.Fprintf(w, "**Seed:** %s (one Append per tenant)\n", res.SeedDuration.Round(time.Millisecond))
	fmt.Fprintf(w, "**Run:** %s steady-state write load\n", res.RunDuration.Round(time.Millisecond))
	fmt.Fprintln(w)

	fmt.Fprintln(w, "### Append latency (run phase)")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "| Count | p50 | p95 | p99 | max | failures |\n")
	fmt.Fprintf(w, "| --- | --- | --- | --- | --- | --- |\n")
	fmt.Fprintf(w, "| %d | %s | %s | %s | %s | %d |\n",
		res.AppendLatencies.Count,
		fmtDur(res.AppendLatencies.P50),
		fmtDur(res.AppendLatencies.P95),
		fmtDur(res.AppendLatencies.P99),
		fmtDur(res.AppendLatencies.Max),
		res.AppendFail,
	)
	fmt.Fprintln(w)

	// SLO comparison vs spike brief (< 20 ms p50, < 100 ms p99).
	fmt.Fprintln(w, "### Spike brief SLO check")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Metric | Measured | Target | Status |")
	fmt.Fprintln(w, "| --- | --- | --- | --- |")
	fmt.Fprintf(w, "| Append p50 | %s | < 20 ms | %s |\n",
		fmtDur(res.AppendLatencies.P50),
		passFail(res.AppendLatencies.P50 < 20*time.Millisecond))
	fmt.Fprintf(w, "| Append p99 | %s | < 100 ms | %s |\n",
		fmtDur(res.AppendLatencies.P99),
		passFail(res.AppendLatencies.P99 < 100*time.Millisecond))
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

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Round(time.Millisecond).String()
}

func passFail(ok bool) string {
	if ok {
		return "✅"
	}
	return "❌"
}

// CompactSummary is a one-line digest used by go test output so the
// smoke run prints its conclusions on the same line as the test
// outcome.
func CompactSummary(res *ScenarioAResult) string {
	return strings.Join([]string{
		fmt.Sprintf("tenants=%d", res.TenantsTotal),
		fmt.Sprintf("seed=%s", res.SeedDuration.Round(time.Millisecond)),
		fmt.Sprintf("appends=%d/%d", res.AppendSucc, res.AppendSucc+res.AppendFail),
		fmt.Sprintf("p50=%s", fmtDur(res.AppendLatencies.P50)),
		fmt.Sprintf("p99=%s", fmtDur(res.AppendLatencies.P99)),
	}, " ")
}
