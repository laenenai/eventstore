package shred

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// RetentionScanner is the optional capability a SubjectStore advertises
// when it can list subjects whose DEK was created before a cutoff. The
// RetentionWorker probes for this at construction; stores that don't
// implement it can still be used for ad-hoc ForgetSubject calls but
// cannot drive a periodic retention sweep.
//
// Implementations MUST honor cancellation and return a stable order
// across paginated calls (typically by subject id) so resuming after
// a partial sweep does not skip rows.
//
// "Older than" is keyed on the row's created_at, NOT a separate
// "last activity" column. The contract is: "a subject whose DEK was
// minted more than MaxAge ago is presumed eligible for retention
// shredding." Adopters who need recency semantics (touch-on-use)
// should add a wrapper that updates created_at on read — the
// framework does not impose that overhead on every read path.
type RetentionScanner interface {
	// ListSubjectsCreatedBefore returns up to limit non-shredded
	// subject_keys rows for tenantID whose created_at < cutoff.
	// Rows are ordered by subject. Shredded subjects (those with
	// non-NULL shredded_at) are excluded.
	ListSubjectsCreatedBefore(ctx context.Context, tenantID string, cutoff time.Time, limit int) ([]SubjectKey, error)
}

// ErrRetentionScannerNotSupported is returned by RetentionWorker.Run /
// RunOnce when the configured Shredder.Store does not implement
// RetentionScanner. Adopters relying on a periodic sweep MUST either
// pick a SubjectStore implementation that supports it or write their
// own scanner against their event store and pass it to the worker
// explicitly.
var ErrRetentionScannerNotSupported = errors.New("shred: SubjectStore does not implement RetentionScanner")

// TenantSource produces the list of tenants the worker should sweep on
// each tick. The framework cannot enumerate tenants on its own — the
// concept lives in the application's user/account model, not in the
// eventstore — so the adopter supplies it. The function is invoked at
// the start of every sweep; static deployments return a constant slice,
// dynamic ones query their own user db.
//
// Returning an empty slice is a no-op (no work this tick), not an
// error.
type TenantSource func(ctx context.Context) ([]string, error)

// StaticTenants is a TenantSource that always returns the same slice.
// Convenience for single-tenant deployments and tests.
func StaticTenants(tenantIDs ...string) TenantSource {
	out := make([]string, len(tenantIDs))
	copy(out, tenantIDs)
	return func(_ context.Context) ([]string, error) { return out, nil }
}

// RetentionWorker periodically scans subject_keys for subjects whose
// DEK is older than MaxAge and crypto-shreds them via the Shredder.
//
// The worker is opt-in framework infrastructure: ADR 0027 mandates
// retention-aware classifications but leaves the *enforcement
// schedule* to the adopter. This worker is the reference enforcement
// shape — a single MaxAge per tenant. Adopters whose classifications
// have heterogeneous windows (e.g., CARDHOLDER kept 12 months,
// PERSONAL kept 7 years) run multiple workers, one per policy, with
// distinct MaxAge values and disjoint TenantSources.
//
// Concurrency: one RetentionWorker per (tenant-set × policy). Safe
// to run multiple workers in the same process. The worker is NOT
// safe to run from multiple processes against the same tenant set —
// shred operations are idempotent, but you'd burn DEK lookups
// pointlessly. Use external leader election (the same pattern as
// outbox.Drain — cookbook 06) for HA deployments.
type RetentionWorker struct {
	// Shredder is the framework-managed Shredder. The worker calls
	// Shredder.ForgetSubject on every expired row.
	Shredder *Shredder

	// Scanner produces the list of subjects to shred. If nil, the
	// worker probes Shredder.Store for the RetentionScanner interface;
	// adapters that don't implement it produce ErrRetentionScannerNotSupported.
	// Explicit Scanner overrides allow adopters to source eligibility
	// from a different system (e.g., a CRM that tracks "last activity"
	// authoritatively).
	Scanner RetentionScanner

	// Tenants enumerates the tenants the worker visits per tick.
	// Required. See StaticTenants for a constant list.
	Tenants TenantSource

	// MaxAge is the retention window. A subject whose created_at is
	// older than (now - MaxAge) is eligible. Must be > 0.
	MaxAge time.Duration

	// Interval is the sleep between sweeps when running via Run.
	// Default 1 hour. RunOnce ignores this.
	Interval time.Duration

	// BatchSize caps the number of rows fetched per Scanner call.
	// Default 100. The worker keeps fetching pages until the scanner
	// returns < BatchSize.
	BatchSize int

	// OnShredded is invoked after each successful ForgetSubject call,
	// with the (tenant, subject) pair that was shredded. Adopter use:
	// emit an audit event, write to a compliance log, increment a
	// metric. nil is a no-op.
	//
	// Errors returned from this callback do NOT roll back the
	// ForgetSubject — the DEK is already destroyed. They are
	// surfaced to the sweep loop, which logs and continues.
	OnShredded func(ctx context.Context, tenantID, subject string) error

	// Clock injects time.Now for deterministic tests. nil => time.Now.
	Clock func() time.Time
}

// RunOnce executes a single retention sweep across every tenant
// returned by Tenants. Returns the total number of subjects
// crypto-shredded and the first error encountered. Errors do not
// halt the sweep; the loop continues so a single tenant's failure
// does not block the rest.
//
// Returns (0, ErrRetentionScannerNotSupported) immediately if no
// Scanner is configured and Shredder.Store doesn't implement one.
func (w *RetentionWorker) RunOnce(ctx context.Context) (int, error) {
	if err := w.validate(); err != nil {
		return 0, err
	}
	scanner := w.resolveScanner()
	if scanner == nil {
		return 0, ErrRetentionScannerNotSupported
	}

	tenants, err := w.Tenants(ctx)
	if err != nil {
		return 0, fmt.Errorf("shred: retention worker: tenant source: %w", err)
	}

	now := w.now()
	cutoff := now.Add(-w.MaxAge)

	var (
		total    int
		firstErr error
	)
	for _, tenantID := range tenants {
		if err := ctx.Err(); err != nil {
			return total, nil
		}
		count, err := w.sweepTenant(ctx, scanner, tenantID, cutoff)
		total += count
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return total, firstErr
}

// Run loops RunOnce on a ticker until ctx is cancelled. Returns nil
// on clean cancellation, the underlying error on a non-cancellation
// failure (today: only ErrRetentionScannerNotSupported, which is a
// configuration error and not recoverable by waiting).
//
// Per-sweep errors are logged via slog.Default() and swallowed so
// transient adapter failures don't kill the worker. Pair with an
// observability surface (cookbook 06's drain pattern applies
// identically) for production deployments.
func (w *RetentionWorker) Run(ctx context.Context) error {
	if err := w.validate(); err != nil {
		return err
	}
	if w.resolveScanner() == nil {
		return ErrRetentionScannerNotSupported
	}

	interval := w.Interval
	if interval <= 0 {
		interval = time.Hour
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		count, err := w.RunOnce(ctx)
		if err != nil {
			slog.Warn("shred retention worker sweep failed",
				"err", err, "shredded_before_failure", count)
		} else if count > 0 {
			slog.Info("shred retention worker sweep complete",
				"shredded", count)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (w *RetentionWorker) sweepTenant(
	ctx context.Context,
	scanner RetentionScanner,
	tenantID string,
	cutoff time.Time,
) (int, error) {
	batchSize := w.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return total, nil
		}
		rows, err := scanner.ListSubjectsCreatedBefore(ctx, tenantID, cutoff, batchSize)
		if err != nil {
			return total, fmt.Errorf("shred: retention scan tenant=%s: %w", tenantID, err)
		}
		if len(rows) == 0 {
			return total, nil
		}
		for _, row := range rows {
			if err := w.Shredder.ForgetSubject(ctx, row.TenantID, row.Subject); err != nil {
				return total, fmt.Errorf("shred: retention forget tenant=%s subject=%s: %w",
					row.TenantID, row.Subject, err)
			}
			total++
			if w.OnShredded != nil {
				if err := w.OnShredded(ctx, row.TenantID, row.Subject); err != nil {
					slog.Warn("shred retention OnShredded callback failed (DEK already destroyed)",
						"tenant", row.TenantID, "subject", row.Subject, "err", err)
				}
			}
		}
		if len(rows) < batchSize {
			return total, nil
		}
	}
}

func (w *RetentionWorker) resolveScanner() RetentionScanner {
	if w.Scanner != nil {
		return w.Scanner
	}
	if s, ok := w.Shredder.Store.(RetentionScanner); ok {
		return s
	}
	return nil
}

func (w *RetentionWorker) validate() error {
	if w.Shredder == nil {
		return errors.New("shred: RetentionWorker.Shredder is required")
	}
	if w.Tenants == nil {
		return errors.New("shred: RetentionWorker.Tenants is required")
	}
	if w.MaxAge <= 0 {
		return errors.New("shred: RetentionWorker.MaxAge must be > 0")
	}
	return nil
}

func (w *RetentionWorker) now() time.Time {
	if w.Clock != nil {
		return w.Clock()
	}
	return time.Now()
}
