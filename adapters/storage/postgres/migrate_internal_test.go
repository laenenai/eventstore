package postgres

import (
	"io/fs"
	"testing"
)

// TestExcludeFS_DropsRLSMigration verifies the WithoutRLS plumbing at
// the FS layer: the RLS migration is hidden from directory listings
// (so goose never collects it) while every other migration remains
// both listed and openable. This is a pure check of the mechanism —
// the privilege behaviour it enables (no CREATE ROLE) is covered by the
// integration tests.
func TestExcludeFS_DropsRLSMigration(t *testing.T) {
	full, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("read full migrations dir: %v", err)
	}

	var sawRLS bool
	for _, e := range full {
		if e.Name() == rlsMigrationFile {
			sawRLS = true
		}
	}
	if !sawRLS {
		t.Fatalf("fixture invalid: %q not present in embedded migrations", rlsMigrationFile)
	}

	filtered := excludeFS{FS: migrationsFS, exclude: map[string]bool{rlsMigrationFile: true}}

	entries, err := fs.ReadDir(filtered, "migrations")
	if err != nil {
		t.Fatalf("read filtered migrations dir: %v", err)
	}
	if len(entries) != len(full)-1 {
		t.Errorf("filtered dir has %d entries; want %d (one fewer than %d)", len(entries), len(full)-1, len(full))
	}
	for _, e := range entries {
		if e.Name() == rlsMigrationFile {
			t.Errorf("RLS migration %q still listed after exclusion", rlsMigrationFile)
		}
	}

	// Remaining files must still be openable through the wrapper.
	for _, e := range entries {
		f, err := filtered.Open("migrations/" + e.Name())
		if err != nil {
			t.Errorf("open %q through excludeFS: %v", e.Name(), err)
			continue
		}
		_ = f.Close()
	}
}
