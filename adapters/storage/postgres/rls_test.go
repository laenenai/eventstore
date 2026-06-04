package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	pgadapter "github.com/laenenai/eventstore/adapters/storage/postgres"
)

// TestRLS_AppPoolBlocksCrossTenantReads verifies that a connection on
// the eventstore_app pool, bound to tenant A via SET LOCAL, cannot see
// rows belonging to tenant B even when issuing a query that explicitly
// names tenant B in its WHERE clause. The RLS policy filters the rows
// before the app-layer predicate runs (ADR 0032).
func TestRLS_AppPoolBlocksCrossTenantReads(t *testing.T) {
	ctx := context.Background()
	agg := newCounterRuntime(t)
	tA := tnt(t, "rls-a")
	tB := tnt(t, "rls-b")

	// Seed one event under each tenant via the adapter (normal write path).
	seedEvents(t, agg, []string{tA}, 1)
	seedEvents(t, agg, []string{tB}, 1)

	// Acquire a connection on the app pool, bind app.tenant_id = tA,
	// and issue a raw SQL that asks for tenant B's events. RLS must
	// filter those rows out — we expect zero rows back, not an error.
	err := pgx.BeginFunc(ctx, appPool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tA); err != nil {
			return err
		}
		var got int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM events WHERE tenant_id = $1", tB).Scan(&got); err != nil {
			return err
		}
		if got != 0 {
			t.Errorf("RLS leak: app pool bound to %q saw %d rows for %q (expected 0)", tA, got, tB)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify isolation: %v", err)
	}
}

// TestRLS_AdminPoolSeesAcrossTenants verifies the admin pool bypasses
// RLS — a query without any tenant binding returns rows for every
// tenant in the database.
func TestRLS_AdminPoolSeesAcrossTenants(t *testing.T) {
	ctx := context.Background()
	agg := newCounterRuntime(t)
	tA := tnt(t, "admin-rls-a")
	tB := tnt(t, "admin-rls-b")
	seedEvents(t, agg, []string{tA}, 1)
	seedEvents(t, agg, []string{tB}, 1)

	var got int
	err := adminPool.QueryRow(ctx,
		"SELECT count(*) FROM events WHERE tenant_id = ANY($1)",
		[]string{tA, tB},
	).Scan(&got)
	if err != nil {
		t.Fatalf("admin pool query: %v", err)
	}
	if got != 2 {
		t.Errorf("admin pool saw %d rows across {%q, %q}, expected 2", got, tA, tB)
	}
}

// TestRLS_AppPoolNoLeakageWithoutBinding verifies that an app-pool
// query with no tenant binding returns no rows for any tenant. Either
// the policy errors out (on a never-bound connection — the loudest
// signal) or it filters everything to zero (when the GUC is known to
// the session but currently empty). Both prevent leakage; the test
// accepts both outcomes.
func TestRLS_AppPoolNoLeakageWithoutBinding(t *testing.T) {
	ctx := context.Background()
	agg := newCounterRuntime(t)
	tX := tnt(t, "noleak")
	seedEvents(t, agg, []string{tX}, 1)

	// Run inside a tx so we get a stable connection scope without
	// touching SET LOCAL ourselves.
	err := pgx.BeginFunc(ctx, appPool, func(tx pgx.Tx) error {
		var n int
		err := tx.QueryRow(ctx, "SELECT count(*) FROM events WHERE tenant_id = $1", tX).Scan(&n)
		if err != nil {
			// Loud error path — RLS policy hit current_setting on a
			// virgin connection. Acceptable; signals misuse.
			return nil
		}
		if n != 0 {
			t.Errorf("unbound app-pool query saw %d rows for %q, expected 0 (or error)", n, tX)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify no leakage: %v", err)
	}
}

// TestRLS_WithoutRLSEnforcement_FallsBackToMainPool verifies the
// migration-ramp option: when WithoutRLSEnforcement is set and no
// admin pool is configured, cross-tenant operations route to the main
// pool instead of erroring with ErrAdminPoolRequired.
//
// The fixture's adminPool runs as eventstore_admin (BYPASSRLS), so
// using it as the main pool simulates the production state where the
// operator has not yet split roles — the main pool's role is
// privileged enough to bypass policies, and the binary is happy to
// drain cross-tenant work through it. This is the safe ramp state
// called out in the WithoutRLSEnforcement doc string.
func TestRLS_WithoutRLSEnforcement_FallsBackToMainPool(t *testing.T) {
	ctx := context.Background()
	agg := newCounterRuntime(t)
	tA := tnt(t, "ramp-a")
	tB := tnt(t, "ramp-b")
	// Seed via the normal adapter (writes tenant-scoped on appPool).
	seedEvents(t, agg, []string{tA, tB}, 1)

	// Construct a ramp adapter: main pool = adminPool (has BYPASSRLS),
	// no admin pool, escape hatch on.
	ramp := pgadapter.New(adminPool, pgadapter.WithoutRLSEnforcement())

	rows, err := ramp.ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ReadAll with ramp option: %v", err)
	}
	if len(rows) < 2 {
		t.Errorf("ReadAll returned %d rows; expected at least 2 (one per tenant)", len(rows))
	}

	// Cross-tenant outbox drain — the canonical case the ramp option
	// protects.
	if _, err := ramp.PendingOutbox(ctx, "", 100, 0); err != nil {
		t.Errorf("PendingOutbox(\"\") with ramp option: %v", err)
	}
}

// TestRLS_AdminPoolRequired verifies that constructing the adapter
// without WithAdminPool causes cross-tenant methods to fail with
// ErrAdminPoolRequired, rather than silently falling through to the
// tenant-scoped pool (where they would RLS-error in a less specific
// way).
func TestRLS_AdminPoolRequired(t *testing.T) {
	ctx := context.Background()
	bare := pgadapter.New(appPool) // no WithAdminPool

	_, err := bare.ReadAll(ctx, 0, 10)
	if !errors.Is(err, pgadapter.ErrAdminPoolRequired) {
		t.Errorf("ReadAll without admin pool: got err=%v want ErrAdminPoolRequired", err)
	}

	_, err = bare.WipeStateCacheForType(ctx, "", "any/type/url")
	if !errors.Is(err, pgadapter.ErrAdminPoolRequired) {
		t.Errorf("WipeStateCacheForType(tenant=\"\"): got err=%v want ErrAdminPoolRequired", err)
	}

	_, err = bare.ListStateStreamSubscribers(ctx)
	if !errors.Is(err, pgadapter.ErrAdminPoolRequired) {
		t.Errorf("ListStateStreamSubscribers without admin pool: got err=%v want ErrAdminPoolRequired", err)
	}
}

// TestRLS_TenantBindingDoesNotLeakAcrossTransactions verifies that a
// SET LOCAL binding in one transaction does not carry over into the
// next transaction on the same pooled connection. In tx 1 we bind
// tenant A and see A's rows; in tx 2 we bind tenant B and verify we
// see B's rows, not A's, even if pgx reuses the same physical conn.
func TestRLS_TenantBindingDoesNotLeakAcrossTransactions(t *testing.T) {
	ctx := context.Background()
	agg := newCounterRuntime(t)
	tA := tnt(t, "leak-a")
	tB := tnt(t, "leak-b")
	seedEvents(t, agg, []string{tA}, 1)
	seedEvents(t, agg, []string{tB}, 1)

	// Acquire a single connection from the pool so tx 1 and tx 2 are
	// guaranteed to share the same physical connection.
	conn, err := appPool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	// Tx 1: bind tA, verify only tA's row is visible.
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tA); err != nil {
			return err
		}
		var n int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM events WHERE tenant_id = $1", tA).Scan(&n); err != nil {
			return err
		}
		if n != 1 {
			t.Errorf("tx 1 bound to %q: got %d rows, want 1", tA, n)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("tx 1: %v", err)
	}

	// Tx 2 on the SAME connection, bound to tB. Should see B's row and
	// must NOT see A's row even though we never explicitly cleared the
	// previous binding — SET LOCAL is scoped to the transaction, so
	// the new binding fully replaces the old.
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tB); err != nil {
			return err
		}
		var na int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM events WHERE tenant_id = $1", tA).Scan(&na); err != nil {
			return err
		}
		if na != 0 {
			t.Errorf("tx 2 bound to %q saw %d rows for %q (binding leaked)", tB, na, tA)
		}
		var nb int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM events WHERE tenant_id = $1", tB).Scan(&nb); err != nil {
			return err
		}
		if nb != 1 {
			t.Errorf("tx 2 bound to %q: got %d rows, want 1", tB, nb)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("tx 2: %v", err)
	}
}
