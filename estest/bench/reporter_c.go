package bench

import (
	"fmt"
	"io"
	"os"
	"time"
)

// openHeartbeat opens the heartbeat log in append mode. Existing
// content (e.g., from an earlier soak attempt) is preserved — the
// runbook explicitly says "no resume," so a new run produces new
// lines after old, which is the right behaviour for forensic
// purposes.
func openHeartbeat(path string) (*os.File, error) {
	if path == "" {
		path = "./spike-0001-soak.log"
	}
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
}

// writeHeartbeatHeader stamps the first lines of each soak attempt.
// Lets a post-hoc reader tell where one run ends and the next
// begins.
func writeHeartbeatHeader(w io.Writer, cfg ScenarioCConfig) {
	fmt.Fprintf(w, "\n[soak start] at=%s tenants=%d duration=%s rate=%.1f/s heartbeat=%s\n",
		time.Now().Format(time.RFC3339),
		cfg.TenantsTotal,
		cfg.SoakDuration,
		cfg.SustainedWritesPerSec,
		cfg.HeartbeatInterval,
	)
}

// writeHeartbeatLine emits one parseable + scannable line per
// heartbeat. The format is a hybrid: human-readable preamble +
// compact key=value tail. Avoids JSON dependency in the harness.
func writeHeartbeatLine(w io.Writer, snap HeartbeatSnapshot) {
	fmt.Fprintf(w, "[heartbeat] at=%s elapsed=%s succ=%d fail=%d window_n=%d p50=%s p99=%s p999=%s wal_bytes=%d",
		snap.At.Format(time.RFC3339),
		snap.ElapsedSinceStart.Round(time.Second),
		snap.CumulativeSucc,
		snap.CumulativeFail,
		snap.WindowAppends,
		snap.WindowLatency.P50,
		snap.WindowLatency.P99,
		snap.WindowLatency.P999,
		snap.WALBytesCumul,
	)
	for _, t := range snap.Tables {
		fmt.Fprintf(w, " %s.live=%d %s.dead=%d %s.hot=%.0f%% %s.av=%d %s.size_kb=%d",
			t.Table, t.LiveTup,
			t.Table, t.DeadTup,
			t.Table, t.HotUpdateRatio()*100,
			t.Table, t.NAutoVacuum,
			t.Table, t.TotalRelSizeKB,
		)
	}
	fmt.Fprintln(w)
}

// ReportC writes the soak's final markdown summary. Structure
// mirrors Report (scenario A) for diff-friendliness, with an
// additional section for the heartbeat time-series and the
// autovacuum behaviour.
func ReportC(w io.Writer, res *ScenarioCResult) {
	fmt.Fprintln(w, "## Scenario C — 7-day autovacuum soak")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "**Population:** %d total — %d cold / %d warm / %d hot  \n",
		res.TenantsTotal, res.TenantsCold, res.TenantsWarm, res.TenantsHot)
	fmt.Fprintf(w, "**Seed:** %s (one Append per tenant)  \n", res.SeedDuration.Round(time.Second))
	fmt.Fprintf(w, "**Soak:** %s (target: 168h0m0s)  \n", res.SoakDuration.Round(time.Second))
	if res.EarlyTermination != "" {
		fmt.Fprintf(w, "**Early termination:** %s  \n", res.EarlyTermination)
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "**Cumulative appends:** %d succeeded, %d failed  \n",
		res.AppendSucc, res.AppendFail)
	walDelta := res.WALBytesAtEnd - res.WALBytesAtStart
	fmt.Fprintf(w, "**WAL generated:** %s (%d bytes)  \n",
		formatBytes(walDelta), walDelta)
	fmt.Fprintln(w)

	// Final-window latency (last 30 min of the soak — most representative
	// of steady-state behaviour).
	if res.LatencyOverall.Count > 0 {
		fmt.Fprintln(w, "### Append latency — final window")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "| Window n | p50 | p95 | p99 | p99.9 | max |")
		fmt.Fprintln(w, "| --- | --- | --- | --- | --- | --- |")
		fmt.Fprintf(w, "| %d | %s | %s | %s | %s | %s |\n",
			res.LatencyOverall.Count,
			res.LatencyOverall.P50.Round(time.Millisecond),
			res.LatencyOverall.P95.Round(time.Millisecond),
			res.LatencyOverall.P99.Round(time.Millisecond),
			res.LatencyOverall.P999.Round(time.Millisecond),
			res.LatencyOverall.Max.Round(time.Millisecond),
		)
		fmt.Fprintln(w)
	}

	// Heartbeat time-series: most interesting columns over time.
	if len(res.Heartbeats) > 0 {
		fmt.Fprintln(w, "### Latency trajectory (per heartbeat)")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "| Elapsed | window n | p50 | p99 | p99.9 | cumul succ | cumul fail |")
		fmt.Fprintln(w, "| --- | --- | --- | --- | --- | --- | --- |")
		for _, hb := range res.Heartbeats {
			fmt.Fprintf(w, "| %s | %d | %s | %s | %s | %d | %d |\n",
				hb.ElapsedSinceStart.Round(time.Minute),
				hb.WindowAppends,
				hb.WindowLatency.P50.Round(time.Millisecond),
				hb.WindowLatency.P99.Round(time.Millisecond),
				hb.WindowLatency.P999.Round(time.Millisecond),
				hb.CumulativeSucc, hb.CumulativeFail,
			)
		}
		fmt.Fprintln(w)
	}

	// Per-table autovacuum and bloat at start vs end.
	if len(res.TableStatsBefore) > 0 && len(res.TableStatsAfter) > 0 {
		fmt.Fprintln(w, "### Table stats — start → end")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "| Table | Δ live | Δ dead | Δ updates | Δ hot-upd | HOT % | autovacuum cycles | last_autovacuum | Δ size KB |")
		fmt.Fprintln(w, "| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
		for i, after := range res.TableStatsAfter {
			before := TableStat{}
			if i < len(res.TableStatsBefore) {
				before = res.TableStatsBefore[i]
			}
			lastAV := "—"
			if after.LastAutoVacuum != nil {
				lastAV = after.LastAutoVacuum.Format(time.RFC3339)
			}
			hotPct := "—"
			if after.NTupUpd > 0 {
				hotPct = fmt.Sprintf("%.1f %%", after.HotUpdateRatio()*100)
			}
			fmt.Fprintf(w, "| `%s` | %+d | %+d | %+d | %+d | %s | %d (Δ %+d) | %s | %+d |\n",
				after.Table,
				after.LiveTup-before.LiveTup,
				after.DeadTup-before.DeadTup,
				after.NTupUpd-before.NTupUpd,
				after.NTupHotUpd-before.NTupHotUpd,
				hotPct,
				after.NAutoVacuum, after.NAutoVacuum-before.NAutoVacuum,
				lastAV,
				after.TotalRelSizeKB-before.TotalRelSizeKB,
			)
		}
		fmt.Fprintln(w)
	}

	// Autovacuum cycle trajectory: how many cycles fired per table,
	// across the soak. The spike brief targets "< 1 h cycle on the
	// largest table" and "no table goes > 24 h without vacuum."
	if len(res.Heartbeats) > 0 {
		fmt.Fprintln(w, "### Autovacuum cycle counts (final heartbeat)")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "| Table | total cycles | last_autovacuum | bloat ratio |")
		fmt.Fprintln(w, "| --- | --- | --- | --- |")
		last := res.Heartbeats[len(res.Heartbeats)-1]
		for _, t := range last.Tables {
			lastAV := "—"
			if t.LastAutoVacuum != nil {
				lastAV = t.LastAutoVacuum.Format(time.RFC3339)
			}
			fmt.Fprintf(w, "| `%s` | %d | %s | %.2fx |\n",
				t.Table, t.NAutoVacuum, lastAV, t.BloatRatio(),
			)
		}
		fmt.Fprintln(w)
	}
}

// CompactSummaryC is the one-liner test-log surface. Mirrors
// CompactSummary's shape.
func CompactSummaryC(res *ScenarioCResult) string {
	tail := ""
	if res.EarlyTermination != "" {
		tail = fmt.Sprintf(" early=%q", res.EarlyTermination)
	}
	return fmt.Sprintf("tenants=%d seed=%s soak=%s succ=%d fail=%d heartbeats=%d%s",
		res.TenantsTotal,
		res.SeedDuration.Round(time.Second),
		res.SoakDuration.Round(time.Second),
		res.AppendSucc, res.AppendFail,
		len(res.Heartbeats),
		tail,
	)
}

// formatBytes is a small humanizer used in the report only — no
// dependency on a humanize lib.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
