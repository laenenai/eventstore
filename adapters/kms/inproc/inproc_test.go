package inproc_test

import (
	"testing"

	"github.com/laenenai/eventstore/adapters/kms/inproc"
	"github.com/laenenai/eventstore/estest"
	"github.com/laenenai/eventstore/kms"
)

// TestConformance runs the framework's KMS conformance suite. inproc
// holds KEK material in process memory only and deliberately does
// not survive restarts, so PersistentNew is nil — the
// cross-instance round-trip test will skip with a documented reason.
// Adopters who need persistence reach for adapters/kms/file or a
// production HSM adapter.
func TestConformance(t *testing.T) {
	estest.RunKMSConformance(t, estest.KMSConformance{
		New: func(t *testing.T) kms.KeyStore {
			return inproc.New()
		},
		PersistentNew: nil, // deliberate — inproc is not persistent
		Rotating:      true,
	})
}
