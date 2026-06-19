package file_test

import (
	"path/filepath"
	"testing"

	"github.com/laenenai/eventstore/adapters/kms/file"
	"github.com/laenenai/eventstore/estest"
	"github.com/laenenai/eventstore/kms"
)

// TestConformance runs the framework's KMS conformance suite. The
// file adapter's persistence story is the load-bearing reason it
// exists (inproc holds KEKs in memory only), so PersistentNew is
// supplied and the round-trip-across-instances test runs.
func TestConformance(t *testing.T) {
	estest.RunKMSConformance(t, estest.KMSConformance{
		New: func(t *testing.T) kms.KeyStore {
			// Each fresh test path gets its own KEK store — no
			// state leaks across the basic subtests.
			path := filepath.Join(t.TempDir(), "kms.json")
			ks, err := file.New(path)
			if err != nil {
				t.Fatalf("file.New: %v", err)
			}
			return ks
		},

		PersistentNew: persistentNewFactory(t),
		Rotating:      true,
	})
}

// persistentNewFactory returns a closure that, within one test's
// scope, hands back KeyStores pointing at the SAME path. Each call
// loads the file fresh — that's the round-trip property the
// PersistsAcrossInstances test asserts.
//
// The path is allocated once per top-level test invocation via
// t.TempDir; subtests within that test share it. The framework's
// conformance harness only calls PersistentNew inside the
// PersistsAcrossInstances subtest, so the captured t.TempDir is
// the right scope.
func persistentNewFactory(t *testing.T) func(t *testing.T) kms.KeyStore {
	path := filepath.Join(t.TempDir(), "shared-kms.json")
	return func(t *testing.T) kms.KeyStore {
		ks, err := file.New(path)
		if err != nil {
			t.Fatalf("file.New (persistent): %v", err)
		}
		return ks
	}
}
