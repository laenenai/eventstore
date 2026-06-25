package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TableStat is one row of pg_stat_user_tables we care about for
// scenario A's autovacuum + bloat questions. Captured at intervals
// during the run so the reporter can show start vs end deltas.
type TableStat struct {
	At              time.Time
	Table           string
	LiveTup         int64
	DeadTup         int64
	NTupIns         int64
	NTupUpd         int64
	NTupDel         int64
	NTupHotUpd      int64
	NAutoVacuum     int64
	LastAutoVacuum  *time.Time
	TotalRelSizeKB  int64
	HeapRelSizeKB   int64
}

// HotUpdateRatio returns the fraction of UPDATEs that completed in
// place via Postgres's HOT path. Higher is better — HOT updates
// don't churn indexes and reduce autovacuum pressure. Returns 0
// if there have been no UPDATEs yet (no denominator).
func (s TableStat) HotUpdateRatio() float64 {
	if s.NTupUpd == 0 {
		return 0
	}
	return float64(s.NTupHotUpd) / float64(s.NTupUpd)
}

// BloatRatio approximates the table-size : useful-content ratio.
// Total size / (live tuples * average row size estimate). For the
// smoke we use a coarser proxy: total_size / heap_size — a number
// > 1 indicates indexes + TOAST are absorbing space (normal); >2
// usually means an autovacuum lag or fillfactor mismatch.
//
// Not a substitute for pg_stat_user_tables n_dead_tup absolute
// values; complements them.
func (s TableStat) BloatRatio() float64 {
	if s.HeapRelSizeKB == 0 {
		return 0
	}
	return float64(s.TotalRelSizeKB) / float64(s.HeapRelSizeKB)
}

// SampleTables snapshots pg_stat_user_tables + size info for the
// supplied table names. Must run as admin (BYPASSRLS) — these
// system catalogs aren't tenant-scoped and the app role would see
// truncated results.
func SampleTables(ctx context.Context, admin *pgxpool.Pool, tables []string) ([]TableStat, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	out := make([]TableStat, 0, len(tables))
	for _, table := range tables {
		var s TableStat
		s.At = time.Now()
		s.Table = table
		// to_regclass('public.<name>') returns NULL on missing
		// tables, which COALESCE handles cleanly. Casting a TEXT
		// parameter to ::regclass directly fails parse-time
		// ("operator does not exist: name = regclass") because the
		// parameter type isn't known to the planner when the cast
		// is applied.
		err := admin.QueryRow(ctx, `
			SELECT
				COALESCE(n_live_tup, 0),
				COALESCE(n_dead_tup, 0),
				COALESCE(n_tup_ins, 0),
				COALESCE(n_tup_upd, 0),
				COALESCE(n_tup_del, 0),
				COALESCE(n_tup_hot_upd, 0),
				COALESCE(autovacuum_count, 0),
				last_autovacuum,
				COALESCE(pg_total_relation_size(to_regclass('public.' || $1)) / 1024, 0),
				COALESCE(pg_relation_size(to_regclass('public.' || $1)) / 1024, 0)
			FROM pg_stat_user_tables
			WHERE schemaname = 'public' AND relname = $1`,
			table,
		).Scan(
			&s.LiveTup, &s.DeadTup,
			&s.NTupIns, &s.NTupUpd, &s.NTupDel, &s.NTupHotUpd,
			&s.NAutoVacuum, &s.LastAutoVacuum,
			&s.TotalRelSizeKB, &s.HeapRelSizeKB,
		)
		if err != nil {
			return nil, fmt.Errorf("sample %s: %w", table, err)
		}
		out = append(out, s)
	}
	return out, nil
}

// SizeOnly returns just the total size of a table in KB, fast.
// Used for periodic checks during the run loop where we don't
// need the full stat scan.
func SizeOnly(ctx context.Context, admin *pgxpool.Pool, table string) (int64, error) {
	var kb int64
	err := admin.QueryRow(ctx,
		`SELECT COALESCE(pg_total_relation_size(to_regclass('public.' || $1)) / 1024, 0)`,
		table,
	).Scan(&kb)
	return kb, err
}
