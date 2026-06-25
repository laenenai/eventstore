package postgres_test

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Migration 00016 partitions four previously-unpartitioned hot-write
// tables (state_cache, projection_checkpoint, processed_events,
// state_stream_subscribers). It MUST preserve data — adopters with
// non-empty databases cannot lose state on upgrade.
//
// This test pins that invariant. It spins up its own dedicated
// PostgreSQL container, migrates up to version 15 (the unpartitioned
// state), seeds rows into each of the four tables, runs migration
// 16, and verifies every seeded row survived the transformation
// into its new partitioned shape.
//
// Why a separate container from TestMain's: TestMain migrates fully
// (through 16) into a single container reused across all conformance
// subtests. This migration test needs the *intermediate* state at
// version 15 — pre-partitioning — and would interfere with the
// shared container if it shared it.
//
// Set EVENTSTORE_SKIP_PG_TESTS=1 to skip when Docker isn't available.

//go:embed migrations/*.sql
var migrationsForTest embed.FS

func TestMigration00016_PreservesData(t *testing.T) {
	if os.Getenv("EVENTSTORE_SKIP_PG_TESTS") == "1" {
		t.Skip("EVENTSTORE_SKIP_PG_TESTS=1")
	}

	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("migration_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	bootstrap, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("bootstrap pool: %v", err)
	}
	t.Cleanup(bootstrap.Close)

	// Step 1: migrate up to and including 00015 (the unpartitioned
	// pre-fix state). Migration 00015 creates the eventstore_app /
	// eventstore_admin roles + RLS policies, so we need it applied
	// before we can take on the admin role for the seed.
	if err := gooseUpTo(ctx, bootstrap, 15); err != nil {
		t.Fatalf("migrate up to 15: %v", err)
	}
	// Grant role membership to the test superuser so the seed
	// connection can SET ROLE eventstore_admin and bypass RLS for
	// the data writes.
	for _, stmt := range []string{
		"GRANT eventstore_app TO test",
		"GRANT eventstore_admin TO test",
	} {
		if _, err := bootstrap.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// Step 2: open an admin-role pool, seed deterministic data into
	// each of the four affected tables.
	adminPool, err := openAsRole(ctx, dsn, "eventstore_admin")
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(adminPool.Close)

	seeded := seedTestData(t, ctx, adminPool)

	// Step 3: run migration 16 (the partitioning).
	if err := gooseUp(ctx, bootstrap); err != nil {
		t.Fatalf("migrate up to current (16): %v", err)
	}

	// Step 4: verify every seeded row survived.
	verifyDataPreserved(t, ctx, adminPool, seeded)

	// Step 5: verify the tables ARE now partitioned (so we know we
	// actually ran 00016, not some other no-op).
	verifyTablesPartitioned(t, ctx, adminPool, []string{
		"state_cache",
		"projection_checkpoint",
		"processed_events",
		"state_stream_subscribers",
	})
}

// gooseUpTo migrates to a specific goose version. We use the
// migrations embedded in this test file rather than the adapter's
// embedded set so the test is self-contained and doesn't depend on
// the adapter's Migrate method.
func gooseUpTo(ctx context.Context, pool *pgxpool.Pool, version int64) error {
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	if err := goose.SetDialect(string(goose.DialectPostgres)); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}
	goose.SetBaseFS(fs.FS(migrationsForTest))
	if err := goose.UpToContext(ctx, sqlDB, "migrations", version); err != nil {
		return fmt.Errorf("UpTo %d: %w", version, err)
	}
	return nil
}

// gooseUp migrates to the latest version. Reuses the same embedded
// migration set as gooseUpTo.
func gooseUp(ctx context.Context, pool *pgxpool.Pool) error {
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	if err := goose.SetDialect(string(goose.DialectPostgres)); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}
	goose.SetBaseFS(fs.FS(migrationsForTest))
	if err := goose.UpContext(ctx, sqlDB, "migrations"); err != nil {
		return fmt.Errorf("Up: %w", err)
	}
	return nil
}

// openAsRole returns a pool whose connections SET ROLE on first use.
// Mirrors newPoolAs in adapter_test.go but lives here so this test
// can run independently.
func openAsRole(ctx context.Context, dsn, roleName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET ROLE %q", roleName))
		return err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// seededData captures every row we wrote into the four affected
// tables, indexed by table for the post-migration verification.
type seededData struct {
	stateCache             []stateCacheRow
	projectionCheckpoints  []projectionCheckpointRow
	processedEvents        []processedEventRow
	stateStreamSubscribers []stateStreamSubscriberRow
}

type stateCacheRow struct {
	tenantID, streamID, typeURL string
	version                     int64
	terminal                    bool
	stateJSON                   string
}

type projectionCheckpointRow struct {
	name, tenantID string
	cursor         int64
}

type processedEventRow struct {
	projectionName, tenantID string
	eventID                  uuid.UUID
}

type stateStreamSubscriberRow struct {
	name, tenantID, streamID string
	lastDeliveredVersion     int64
}

// seedTestData inserts a deterministic mix of rows across several
// tenants. The dataset is intentionally small (handful of rows per
// table) but covers the dimensions the migration must preserve:
// multiple tenants, varying type_urls, the empty-tenant sentinel for
// projection_checkpoint, multiple streams per tenant.
func seedTestData(t *testing.T, ctx context.Context, pool *pgxpool.Pool) seededData {
	t.Helper()
	out := seededData{}

	tenants := []string{"tenant-alpha", "tenant-beta", "tenant-gamma"}

	// state_cache: 2 rows per tenant, different type_urls + streams.
	for _, tenant := range tenants {
		for i, suffix := range []string{"a", "b"} {
			row := stateCacheRow{
				tenantID:  tenant,
				streamID:  "invoice-" + suffix,
				typeURL:   "myapp.invoice.v1.Invoice",
				version:   int64(i + 1),
				terminal:  i%2 == 1,
				stateJSON: fmt.Sprintf(`{"id":%q,"total":%d}`, suffix, (i+1)*100),
			}
			out.stateCache = append(out.stateCache, row)
			_, err := pool.Exec(ctx, `
				INSERT INTO state_cache
				    (tenant_id, stream_id, type_url, state, version, terminal, state_schema_version)
				VALUES ($1, $2, $3, $4::jsonb, $5, $6, 1)`,
				row.tenantID, row.streamID, row.typeURL, row.stateJSON, row.version, row.terminal)
			if err != nil {
				t.Fatalf("seed state_cache %s/%s: %v", row.tenantID, row.streamID, err)
			}
		}
	}

	// projection_checkpoint: one row per (projection, tenant), plus
	// one empty-tenant cross-tenant entry to exercise the '' default.
	projections := []string{"invoice-read-model", "audit-log"}
	for _, p := range projections {
		for _, tenant := range tenants {
			row := projectionCheckpointRow{name: p, tenantID: tenant, cursor: int64(len(out.projectionCheckpoints) * 10)}
			out.projectionCheckpoints = append(out.projectionCheckpoints, row)
			_, err := pool.Exec(ctx, `
				INSERT INTO projection_checkpoint (name, tenant_id, cursor) VALUES ($1, $2, $3)`,
				row.name, row.tenantID, row.cursor)
			if err != nil {
				t.Fatalf("seed projection_checkpoint %s/%s: %v", row.name, row.tenantID, err)
			}
		}
	}
	// Cross-tenant entry (empty tenant_id sentinel).
	xrow := projectionCheckpointRow{name: "global-rollup", tenantID: "", cursor: 9999}
	out.projectionCheckpoints = append(out.projectionCheckpoints, xrow)
	_, err := pool.Exec(ctx, `
		INSERT INTO projection_checkpoint (name, tenant_id, cursor) VALUES ($1, $2, $3)`,
		xrow.name, xrow.tenantID, xrow.cursor)
	if err != nil {
		t.Fatalf("seed projection_checkpoint cross-tenant: %v", err)
	}

	// processed_events: 3 rows per tenant.
	for _, tenant := range tenants {
		for i := 0; i < 3; i++ {
			row := processedEventRow{
				projectionName: "invoice-read-model",
				tenantID:       tenant,
				eventID:        uuid.New(),
			}
			out.processedEvents = append(out.processedEvents, row)
			_, err := pool.Exec(ctx, `
				INSERT INTO processed_events (projection_name, tenant_id, event_id) VALUES ($1, $2, $3)`,
				row.projectionName, row.tenantID, row.eventID)
			if err != nil {
				t.Fatalf("seed processed_events %s/%s: %v", row.projectionName, row.tenantID, err)
			}
		}
	}

	// state_stream_subscribers: 2 streams per tenant for one subscriber.
	for _, tenant := range tenants {
		for _, stream := range []string{"invoice-a", "invoice-b"} {
			row := stateStreamSubscriberRow{
				name:                 "audit-mirror",
				tenantID:             tenant,
				streamID:             stream,
				lastDeliveredVersion: int64(len(out.stateStreamSubscribers) + 1),
			}
			out.stateStreamSubscribers = append(out.stateStreamSubscribers, row)
			_, err := pool.Exec(ctx, `
				INSERT INTO state_stream_subscribers (name, tenant_id, stream_id, last_delivered_version)
				VALUES ($1, $2, $3, $4)`,
				row.name, row.tenantID, row.streamID, row.lastDeliveredVersion)
			if err != nil {
				t.Fatalf("seed state_stream_subscribers %s/%s/%s: %v", row.name, row.tenantID, row.streamID, err)
			}
		}
	}

	t.Logf("seeded %d state_cache, %d projection_checkpoint, %d processed_events, %d state_stream_subscribers rows",
		len(out.stateCache), len(out.projectionCheckpoints), len(out.processedEvents), len(out.stateStreamSubscribers))
	return out
}

// verifyDataPreserved re-reads every seeded row from the post-
// migration partitioned tables and asserts identity-by-content.
// Any mismatch is a data-preservation bug in the migration.
func verifyDataPreserved(t *testing.T, ctx context.Context, pool *pgxpool.Pool, seeded seededData) {
	t.Helper()

	// state_cache row count must match.
	var stateCacheCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM state_cache").Scan(&stateCacheCount); err != nil {
		t.Fatalf("count state_cache: %v", err)
	}
	if stateCacheCount != len(seeded.stateCache) {
		t.Errorf("state_cache row count: got %d, want %d", stateCacheCount, len(seeded.stateCache))
	}
	for _, want := range seeded.stateCache {
		var (
			gotVersion  int64
			gotTerminal bool
			gotType     string
			gotState    string
		)
		err := pool.QueryRow(ctx, `
			SELECT version, terminal, type_url, state::text
			  FROM state_cache
			 WHERE tenant_id = $1 AND stream_id = $2`,
			want.tenantID, want.streamID).Scan(&gotVersion, &gotTerminal, &gotType, &gotState)
		if err != nil {
			t.Errorf("re-read state_cache %s/%s: %v", want.tenantID, want.streamID, err)
			continue
		}
		if gotVersion != want.version || gotTerminal != want.terminal || gotType != want.typeURL {
			t.Errorf("state_cache %s/%s: scalar columns drift; got v=%d t=%v type=%s; want v=%d t=%v type=%s",
				want.tenantID, want.streamID, gotVersion, gotTerminal, gotType, want.version, want.terminal, want.typeURL)
		}
		// JSONB normalization may reformat whitespace and reorder
		// keys; compare via parsed roundtrip rather than byte
		// equality to assert semantic preservation.
		var gotVal, wantVal any
		if err := json.Unmarshal([]byte(gotState), &gotVal); err != nil {
			t.Errorf("state_cache %s/%s: post-migration state not valid JSON: %v", want.tenantID, want.streamID, err)
			continue
		}
		if err := json.Unmarshal([]byte(want.stateJSON), &wantVal); err != nil {
			t.Errorf("state_cache %s/%s: seed state JSON invalid: %v", want.tenantID, want.streamID, err)
			continue
		}
		if !reflect.DeepEqual(gotVal, wantVal) {
			t.Errorf("state_cache %s/%s: state semantic drift; got %v want %v",
				want.tenantID, want.streamID, gotVal, wantVal)
		}
	}

	// projection_checkpoint
	var pcCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM projection_checkpoint").Scan(&pcCount); err != nil {
		t.Fatalf("count projection_checkpoint: %v", err)
	}
	if pcCount != len(seeded.projectionCheckpoints) {
		t.Errorf("projection_checkpoint row count: got %d, want %d", pcCount, len(seeded.projectionCheckpoints))
	}
	for _, want := range seeded.projectionCheckpoints {
		var gotCursor int64
		err := pool.QueryRow(ctx, `
			SELECT cursor FROM projection_checkpoint WHERE name = $1 AND tenant_id = $2`,
			want.name, want.tenantID).Scan(&gotCursor)
		if err != nil {
			t.Errorf("re-read projection_checkpoint %s/%s: %v", want.name, want.tenantID, err)
			continue
		}
		if gotCursor != want.cursor {
			t.Errorf("projection_checkpoint %s/%s: cursor drift; got %d want %d",
				want.name, want.tenantID, gotCursor, want.cursor)
		}
	}

	// processed_events
	var peCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM processed_events").Scan(&peCount); err != nil {
		t.Fatalf("count processed_events: %v", err)
	}
	if peCount != len(seeded.processedEvents) {
		t.Errorf("processed_events row count: got %d, want %d", peCount, len(seeded.processedEvents))
	}
	for _, want := range seeded.processedEvents {
		var exists bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1 FROM processed_events
			   WHERE projection_name = $1 AND tenant_id = $2 AND event_id = $3
			)`, want.projectionName, want.tenantID, want.eventID).Scan(&exists)
		if err != nil {
			t.Errorf("re-read processed_events %s/%s/%s: %v", want.projectionName, want.tenantID, want.eventID, err)
			continue
		}
		if !exists {
			t.Errorf("processed_events %s/%s/%s: row missing post-migration",
				want.projectionName, want.tenantID, want.eventID)
		}
	}

	// state_stream_subscribers
	var sssCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM state_stream_subscribers").Scan(&sssCount); err != nil {
		t.Fatalf("count state_stream_subscribers: %v", err)
	}
	if sssCount != len(seeded.stateStreamSubscribers) {
		t.Errorf("state_stream_subscribers row count: got %d, want %d", sssCount, len(seeded.stateStreamSubscribers))
	}
	for _, want := range seeded.stateStreamSubscribers {
		var gotVersion int64
		err := pool.QueryRow(ctx, `
			SELECT last_delivered_version FROM state_stream_subscribers
			 WHERE name = $1 AND tenant_id = $2 AND stream_id = $3`,
			want.name, want.tenantID, want.streamID).Scan(&gotVersion)
		if err != nil {
			t.Errorf("re-read state_stream_subscribers %s/%s/%s: %v",
				want.name, want.tenantID, want.streamID, err)
			continue
		}
		if gotVersion != want.lastDeliveredVersion {
			t.Errorf("state_stream_subscribers %s/%s/%s: last_delivered_version drift; got %d want %d",
				want.name, want.tenantID, want.streamID, gotVersion, want.lastDeliveredVersion)
		}
	}
}

// verifyTablesPartitioned confirms that each named table now has
// hash-partitioned children. Catches the case where the migration
// silently no-oped (e.g., because of an early-exit path we didn't
// notice).
func verifyTablesPartitioned(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tables []string) {
	t.Helper()
	for _, table := range tables {
		var partitionCount int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_inherits i
			JOIN pg_class p ON p.oid = i.inhparent
			WHERE p.relname = $1`, table).Scan(&partitionCount)
		if err != nil {
			t.Errorf("query partition count for %s: %v", table, err)
			continue
		}
		if partitionCount != 16 {
			t.Errorf("%s: expected 16 hash partitions post-migration, got %d", table, partitionCount)
		}
	}
}

// Compile-time guard that sql.DB stays in scope (referenced via
// stdlib.OpenDBFromPool but type-elided in goose helpers above).
var _ = sql.ErrNoRows
