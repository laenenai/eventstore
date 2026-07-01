package bench

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Scenario E — tenant deletion (spike 0001 §3 Phase 1).
//
// What it stresses: the GDPR "right to erasure" path at scale. The
// framework has no first-class DeleteTenant API — per-subject PII is
// crypto-shredded via ForgetSubject (ADR 0010/0027), but *whole-tenant
// offboarding* is a physical row purge across every tenant-scoped
// table. This scenario measures that purge: per-tenant delete latency,
// cascade completeness (no orphan rows left in any projection/index
// table), and that both stay within the brief's SLOs while many
// deletions run in parallel.
//
//	per-tenant delete time   < 5 s   (soft)
//	cascade to projections   < 30 s  (soft)
//	residual rows after purge  0     (HARD — a single orphan is a FAIL)
//
// Why the admin pool: eventstore_admin holds BYPASSRLS (migration
// 00015), so a purge crosses tenant boundaries without setting
// app.tenant_id per row. That matches the real operator offboarding
// path — a privileged batch job, not an in-tenant request.
//
// NOT yet covered here (deliberately, to keep the first cut focused):
// the "sustained throughput @ 10/s maintained" leg of the SLO, which
// needs a concurrent steady-state writer (scenario A's load) running
// while deletions proceed, to prove erasure doesn't starve live
// traffic. That is a second layer — see the TODO in RunScenarioE.

// purgeTables lists every tenant-scoped table a whole-tenant erasure
// must clear, ordered children → roots so the purge is FK-safe even
// if foreign keys are added later. `snapshots` is intentionally absent
// — it is dropped by migration 00010 (ADR 0023) and must not be
// reintroduced. The cross-tenant sentinel rows (tenant_id = '') in
// projection_checkpoint / state_stream_subscribers are untouched by a
// per-tenant WHERE clause, which is correct: a shared projector
// checkpoint does not belong to any single tenant being erased.
var purgeTables = []string{
	"processed_events",
	"projection_dlq",
	"subscriber_dlq",
	"projection_checkpoint",
	"state_stream_subscribers",
	"state_cache",
	"outbox",
	"unique_claims",
	"subject_keys",
	"events",
}

// ScenarioEConfig drives the tenant-deletion measurement.
type ScenarioEConfig struct {
	// TenantsTotal is the population seeded and then erased. 10_000
	// for the smoke; 100k+ for Phase 1 tiers.
	TenantsTotal int

	// SeedConcurrency parallelises the one-Append-per-tenant seed.
	SeedConcurrency int

	// DeleteConcurrency caps the parallel purge workers. The brief's
	// scenario name ("10K parallel") is about contention: many
	// erasures hitting the same partitions and autovacuum budget at
	// once. Default 32 matches the admin pool's MaxConns.
	DeleteConcurrency int

	// PurgeTables is the tenant-scoped table set each erasure clears.
	// Defaults to purgeTables; overridable so a branch with extra
	// tenant tables can extend it without editing the harness.
	PurgeTables []string

	// Tables names whose pg_stat the harness snapshots before/after,
	// to show the storage reclaim (live tuples dropping to the
	// sentinel-row floor, dead tuples spiking pre-autovacuum).
	Tables []string
}

// DefaultConfigE returns the 10K smoke defaults.
func DefaultConfigE() ScenarioEConfig {
	return ScenarioEConfig{
		TenantsTotal:      10_000,
		SeedConcurrency:   32,
		DeleteConcurrency: 32,
		PurgeTables:       purgeTables,
		Tables: []string{
			"state_cache",
			"projection_checkpoint",
			"processed_events",
			"state_stream_subscribers",
			"events",
		},
	}
}

// ScenarioEResult is the deletion summary. Picked up by a reporter
// (analogous to ReportC) for the §11.3 scenario-E table.
type ScenarioEResult struct {
	TenantsTotal int

	SeedDuration   time.Duration
	DeleteDuration time.Duration // wall-clock for the whole parallel purge

	DeletesSucc int64
	DeletesFail int64

	// DeleteLatencies is per-tenant purge latency (the < 5 s SLO).
	DeleteLatencies LatencySummary
	// CascadeLatencies is per-tenant residual-verification latency
	// (the < 30 s SLO is on the *cascade completing*; we measure the
	// verify pass that proves it did).
	CascadeLatencies LatencySummary

	// ResidualRows is the total tenant-scoped rows still present after
	// every purge committed. MUST be 0; any non-zero is a hard FAIL.
	ResidualRows int64

	TableStatsBefore []TableStat
	TableStatsAfter  []TableStat
}

// RunScenarioE seeds TenantsTotal tenants (one stream each), then
// erases all of them in parallel, measuring per-tenant purge +
// cascade-verification latency and asserting zero residual rows.
//
// TODO (sustained-throughput SLO): wrap a scenario-A steady-state
// writer around the purge phase — spawn the writer pool before the
// delete workers and assert its append p99 stays within target while
// erasures run. That proves erasure batches don't starve live tenants.
func RunScenarioE(ctx context.Context, h *Harness, cfg ScenarioEConfig) (ScenarioEResult, error) {
	if cfg.DeleteConcurrency < 1 {
		cfg.DeleteConcurrency = 1
	}
	if len(cfg.PurgeTables) == 0 {
		cfg.PurgeTables = purgeTables
	}

	tenants := Population(cfg.TenantsTotal)
	res := ScenarioEResult{TenantsTotal: len(tenants)}

	if before, err := SampleTables(ctx, h.AdminPool, cfg.Tables); err == nil {
		res.TableStatsBefore = before
	}

	// --- seed -----------------------------------------------------------
	seedStart := time.Now()
	if err := seed(ctx, h.Adapter, tenants, cfg.SeedConcurrency); err != nil {
		return res, fmt.Errorf("seed: %w", err)
	}
	res.SeedDuration = time.Since(seedStart)

	// --- purge in parallel ---------------------------------------------
	recorder := NewRecorder("delete", "verify_cascade")
	residualQuery := buildResidualQuery(cfg.PurgeTables)

	var residual atomic.Int64
	jobs := make(chan string, cfg.DeleteConcurrency*2)
	var wg sync.WaitGroup
	for range cfg.DeleteConcurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tenantID := range jobs {
				delDur, err := purgeTenant(ctx, h.AdminPool, tenantID, cfg.PurgeTables)
				recorder.Add("delete", delDur, err == nil)
				if err != nil {
					continue
				}
				// Cascade check: prove every tenant-scoped table is
				// clear for this tenant. We verify per-tenant rather
				// than once at the end so the latency is attributable
				// and a partial-cascade bug surfaces as a non-zero
				// residual on the offending tenant, not an aggregate.
				left, verifyDur := countResidual(ctx, h.AdminPool, residualQuery, tenantID)
				recorder.Add("verify_cascade", verifyDur, true)
				residual.Add(left)
			}
		}()
	}
	delStart := time.Now()
	for _, t := range tenants {
		jobs <- t.ID
	}
	close(jobs)
	wg.Wait()
	res.DeleteDuration = time.Since(delStart)
	res.ResidualRows = residual.Load()

	// --- summarise ------------------------------------------------------
	samples, succ, fail := recorder.Snapshot()
	res.DeletesSucc = succ["delete"]
	res.DeletesFail = fail["delete"]
	res.DeleteLatencies = summarize(filterOp(samples, "delete"))
	res.CascadeLatencies = summarize(filterOp(samples, "verify_cascade"))

	if after, err := SampleTables(ctx, h.AdminPool, cfg.Tables); err == nil {
		res.TableStatsAfter = after
	}
	return res, nil
}

// purgeTenant deletes every tenant-scoped row for one tenant inside a
// single transaction, so a tenant is atomically either fully erased or
// untouched — a crash mid-purge can't leave a half-deleted tenant.
// Returns the wall-clock latency for the SLO.
func purgeTenant(ctx context.Context, admin *pgxpool.Pool, tenantID string, tables []string) (time.Duration, error) {
	start := time.Now()
	tx, err := admin.Begin(ctx)
	if err != nil {
		return time.Since(start), fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	for _, table := range tables {
		// Table names come from a trusted in-process allowlist, never
		// user input; Sanitize is belt-and-suspenders.
		q := fmt.Sprintf("DELETE FROM %s WHERE tenant_id = $1", pgx.Identifier{table}.Sanitize())
		if _, err := tx.Exec(ctx, q, tenantID); err != nil {
			return time.Since(start), fmt.Errorf("delete %s: %w", table, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return time.Since(start), fmt.Errorf("commit: %w", err)
	}
	return time.Since(start), nil
}

// buildResidualQuery composes a single round-trip count across all
// purge tables: SELECT (count a) + (count b) + ... Avoids N queries
// per tenant. $1 is referenced once per sub-select; pgx binds the
// single parameter to every position.
func buildResidualQuery(tables []string) string {
	parts := make([]string, len(tables))
	for i, table := range tables {
		parts[i] = fmt.Sprintf("(SELECT count(*) FROM %s WHERE tenant_id = $1)", pgx.Identifier{table}.Sanitize())
	}
	return "SELECT " + strings.Join(parts, " + ")
}

// countResidual returns how many tenant-scoped rows remain for a
// tenant after its purge committed (expected 0) and the query
// latency. A query error is treated as "could not prove erasure" and
// reported as a residual of 1 so it can never silently pass.
func countResidual(ctx context.Context, admin *pgxpool.Pool, query, tenantID string) (int64, time.Duration) {
	start := time.Now()
	var n int64
	if err := admin.QueryRow(ctx, query, tenantID).Scan(&n); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, time.Since(start)
		}
		return 1, time.Since(start)
	}
	return n, time.Since(start)
}

// filterOp returns the subset of samples for one op, so a single
// recorder can carry both the delete and cascade-verify series.
func filterOp(samples []LatencySample, op string) []LatencySample {
	out := make([]LatencySample, 0, len(samples))
	for _, s := range samples {
		if s.Op == op {
			out = append(out, s)
		}
	}
	return out
}
