package estest

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/laenenai/eventstore/kms"
)

// KMSConformance describes the test surface for one kms.KeyStore
// implementation. Adapter test files declare a value and call
// RunKMSConformance; both inproc and file (and any future adapter)
// must satisfy the same contract.
//
// Two factory fields shape the suite:
//
//   - New constructs an isolated, independent KeyStore. Every test
//     in the basic suite calls New to get a fresh instance — no
//     shared state between subtests. Required.
//
//   - PersistentNew opts the implementation into the
//     cross-instance round-trip test. The contract: two calls to
//     PersistentNew within the same test return KeyStore values
//     backed by the SAME persistent store. Implementations whose
//     state is process-local (inproc) leave this nil and the
//     round-trip test skips with a documented reason. Implementations
//     that persist (file, AWS) provide it.
//
//   - Rotating describes whether the implementation supports the
//     optional kms.KEKRotator interface. Some external KMS adapters
//     rotate out-of-band via the provider's own controls and don't
//     implement RotateKEK in-process; the rotation test skips for
//     them.
type KMSConformance struct {
	// New returns a fresh, independent KeyStore. Required.
	New func(t *testing.T) kms.KeyStore

	// PersistentNew, when non-nil, returns a KeyStore backed by the
	// same persistent storage as the previous PersistentNew call in
	// the same test. Implementations holding state in process
	// memory only leave this nil; cross-instance round-trip test
	// then skips.
	PersistentNew func(t *testing.T) kms.KeyStore

	// Rotating signals whether the implementation supports
	// in-process RotateKEK via kms.KEKRotator. The rotation test
	// skips if false.
	Rotating bool
}

// RunKMSConformance runs the KMS contract suite against the
// implementation described by c. Subtests are reported under
// TestConformance/KMS/<Subtest> in the calling adapter's test
// output.
func RunKMSConformance(t *testing.T, c KMSConformance) {
	t.Helper()
	if c.New == nil {
		t.Fatal("estest.RunKMSConformance: KMSConformance.New is required")
	}

	t.Run("WrapUnwrapRoundTrip", func(t *testing.T) { kmsTestWrapUnwrapRoundTrip(t, c) })
	t.Run("WrapDifferentDEKsProduceDifferentBytes", func(t *testing.T) { kmsTestWrapDistinct(t, c) })
	t.Run("UnknownVersionFails", func(t *testing.T) { kmsTestUnknownVersionFails(t, c) })
	t.Run("CurrentKEKVersionProgresses", func(t *testing.T) { kmsTestCurrentVersionProgresses(t, c) })
	t.Run("MultiTenantIsolation", func(t *testing.T) { kmsTestMultiTenantIsolation(t, c) })

	t.Run("PersistsAcrossInstances", func(t *testing.T) {
		if c.PersistentNew == nil {
			t.Skip("KMSConformance.PersistentNew is nil — this KeyStore is not persistent; round-trip across instances does not apply")
		}
		kmsTestPersistsAcrossInstances(t, c)
	})

	t.Run("RotateProducesNewVersionAndOldStillUnwraps", func(t *testing.T) {
		if !c.Rotating {
			t.Skip("KMSConformance.Rotating is false — this KeyStore does not support in-process KEK rotation")
		}
		kmsTestRotationCoexistence(t, c)
	})
}

// kmsTestWrapUnwrapRoundTrip — basic correctness: a wrap-then-unwrap
// returns the same DEK.
func kmsTestWrapUnwrapRoundTrip(t *testing.T, c KMSConformance) {
	ks := c.New(t)
	ctx := context.Background()
	tenant := "acme"
	dek := mustRandKey(t)

	wrapped, version, err := ks.WrapDEK(ctx, tenant, dek)
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	if version == 0 {
		t.Fatalf("WrapDEK returned version 0; want >= 1 (first wrap minted KEK v1)")
	}
	got, err := ks.UnwrapDEK(ctx, tenant, wrapped, version)
	if err != nil {
		t.Fatalf("UnwrapDEK: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Errorf("unwrapped DEK differs from original")
	}
}

// kmsTestWrapDistinct — two wraps of the same DEK under the same
// KEK produce different ciphertexts (AEAD nonce uniqueness). Catches
// implementations that reuse a constant nonce — a security-critical
// failure mode.
func kmsTestWrapDistinct(t *testing.T, c KMSConformance) {
	ks := c.New(t)
	ctx := context.Background()
	tenant := "acme"
	dek := mustRandKey(t)

	w1, v1, err := ks.WrapDEK(ctx, tenant, dek)
	if err != nil {
		t.Fatalf("WrapDEK (1): %v", err)
	}
	w2, v2, err := ks.WrapDEK(ctx, tenant, dek)
	if err != nil {
		t.Fatalf("WrapDEK (2): %v", err)
	}
	if v1 != v2 {
		t.Errorf("two wraps under same KEK changed version: %d -> %d", v1, v2)
	}
	if bytes.Equal(w1, w2) {
		t.Errorf("two wraps of the same DEK produced identical ciphertext — nonce reuse?")
	}
	// Both must unwrap to the original.
	for i, w := range [][]byte{w1, w2} {
		got, err := ks.UnwrapDEK(ctx, tenant, w, v1)
		if err != nil {
			t.Errorf("UnwrapDEK (%d): %v", i, err)
			continue
		}
		if !bytes.Equal(got, dek) {
			t.Errorf("UnwrapDEK (%d) returned different DEK", i)
		}
	}
}

// kmsTestUnknownVersionFails — UnwrapDEK with a version the store
// doesn't have must return an error rather than silently succeeding.
func kmsTestUnknownVersionFails(t *testing.T, c KMSConformance) {
	ks := c.New(t)
	ctx := context.Background()
	_, err := ks.UnwrapDEK(ctx, "acme", []byte("garbage-but-non-empty"), 1)
	if err == nil {
		t.Fatal("UnwrapDEK with no KEK versions should fail; got nil error")
	}
}

// kmsTestCurrentVersionProgresses — CurrentKEKVersion starts at 0,
// becomes >= 1 after the first WrapDEK.
func kmsTestCurrentVersionProgresses(t *testing.T, c KMSConformance) {
	ks := c.New(t)
	ctx := context.Background()
	tenant := "acme"

	v0, err := ks.CurrentKEKVersion(ctx, tenant)
	if err != nil {
		t.Fatalf("CurrentKEKVersion before any wrap: %v", err)
	}
	if v0 != 0 {
		t.Errorf("CurrentKEKVersion before any wrap: got %d want 0", v0)
	}

	if _, _, err := ks.WrapDEK(ctx, tenant, mustRandKey(t)); err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}

	v1, err := ks.CurrentKEKVersion(ctx, tenant)
	if err != nil {
		t.Fatalf("CurrentKEKVersion after first wrap: %v", err)
	}
	if v1 == 0 {
		t.Errorf("CurrentKEKVersion after first wrap should be >= 1, got 0")
	}
}

// kmsTestMultiTenantIsolation — DEKs wrapped under tenant A's KEK
// must not unwrap under tenant B's. This is the load-bearing
// invariant for multi-tenant SaaS deployments.
func kmsTestMultiTenantIsolation(t *testing.T, c KMSConformance) {
	ks := c.New(t)
	ctx := context.Background()
	dek := mustRandKey(t)

	wrappedA, vA, err := ks.WrapDEK(ctx, "tenant-a", dek)
	if err != nil {
		t.Fatalf("WrapDEK A: %v", err)
	}
	// Force tenant-b to mint its own KEK at the same version index.
	if _, _, err := ks.WrapDEK(ctx, "tenant-b", dek); err != nil {
		t.Fatalf("WrapDEK B: %v", err)
	}
	// Attempt to unwrap tenant-a's wrapped DEK under tenant-b's KEK.
	got, err := ks.UnwrapDEK(ctx, "tenant-b", wrappedA, vA)
	if err == nil && bytes.Equal(got, dek) {
		t.Fatalf("tenant-b unwrapped tenant-a's wrapped DEK — cross-tenant KEK leak")
	}
	// Either a hard error or a different DEK is acceptable (both
	// signal "this isn't decryptable by tenant-b"). A hard error is
	// preferable but AEAD-failure-on-decrypt is the typical shape.
}

// kmsTestPersistsAcrossInstances — the load-bearing test for
// persistent implementations. Wrap via instance 1, swap to a new
// instance against the SAME backing store, unwrap successfully.
// Proves wrappings survive process restarts, which is exactly the
// property inproc explicitly doesn't have.
func kmsTestPersistsAcrossInstances(t *testing.T, c KMSConformance) {
	first := c.PersistentNew(t)
	ctx := context.Background()
	tenant := "acme"
	dek := mustRandKey(t)

	wrapped, version, err := first.WrapDEK(ctx, tenant, dek)
	if err != nil {
		t.Fatalf("first WrapDEK: %v", err)
	}

	second := c.PersistentNew(t)
	got, err := second.UnwrapDEK(ctx, tenant, wrapped, version)
	if err != nil {
		t.Fatalf("second instance UnwrapDEK: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatalf("DEK round-trip across instances failed: bytes differ")
	}

	// Bonus: CurrentKEKVersion on the second instance must reflect
	// the first's writes.
	v, err := second.CurrentKEKVersion(ctx, tenant)
	if err != nil {
		t.Fatalf("second CurrentKEKVersion: %v", err)
	}
	if v != version {
		t.Errorf("second instance sees CurrentKEKVersion=%d, want %d", v, version)
	}
}

// kmsTestRotationCoexistence — after rotation, OLD wrappings still
// decrypt under their stored version. NEW wraps target the new
// version. This is the "rotate-without-rewriting-history" property
// shred.RewrapDEKs relies on for staged migration.
func kmsTestRotationCoexistence(t *testing.T, c KMSConformance) {
	ks := c.New(t)
	rotator, ok := ks.(kms.KEKRotator)
	if !ok {
		t.Fatalf("KMSConformance.Rotating=true but KeyStore doesn't implement kms.KEKRotator")
	}
	ctx := context.Background()
	tenant := "acme"

	oldDEK := mustRandKey(t)
	oldWrapped, oldVersion, err := ks.WrapDEK(ctx, tenant, oldDEK)
	if err != nil {
		t.Fatalf("WrapDEK pre-rotate: %v", err)
	}

	newVersion, err := rotator.RotateKEK(ctx, tenant)
	if err != nil {
		t.Fatalf("RotateKEK: %v", err)
	}
	if newVersion <= oldVersion {
		t.Fatalf("RotateKEK returned version %d, want > %d", newVersion, oldVersion)
	}

	// Old wrapping still decrypts under its stored version.
	got, err := ks.UnwrapDEK(ctx, tenant, oldWrapped, oldVersion)
	if err != nil {
		t.Fatalf("UnwrapDEK old wrapping under old version: %v", err)
	}
	if !bytes.Equal(got, oldDEK) {
		t.Errorf("old wrapping under old version returned different DEK")
	}

	// New WrapDEK calls target the new version.
	newDEK := mustRandKey(t)
	newWrapped, wrapVersion, err := ks.WrapDEK(ctx, tenant, newDEK)
	if err != nil {
		t.Fatalf("WrapDEK post-rotate: %v", err)
	}
	if wrapVersion != newVersion {
		t.Errorf("post-rotate WrapDEK targeted version %d, want %d (the new active)", wrapVersion, newVersion)
	}

	// And the new wrapping must NOT decrypt under the OLD version
	// (it would imply key reuse, which is the whole reason we
	// rotate).
	if _, err := ks.UnwrapDEK(ctx, tenant, newWrapped, oldVersion); err == nil {
		t.Errorf("new wrapping decrypted under old version — KEK reuse?")
	}
}

// mustRandKey returns a 32-byte cryptographically-random buffer or
// fails the test. Used everywhere the suite needs a fresh DEK.
func mustRandKey(t *testing.T) []byte {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

// ErrConformanceSetup is returned by helpers in this file when the
// caller's KMSConformance struct is misconfigured. Exposed so
// adapter test code can match against it via errors.Is in the rare
// case it wraps the conformance call.
var ErrConformanceSetup = errors.New("estest: KMS conformance setup error")
