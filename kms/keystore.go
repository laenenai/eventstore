package kms

import "context"

// KeyStore is the framework-defined KMS abstraction (ADR 0010).
//
// Crypto-shredding uses envelope encryption: per-subject DEKs are
// generated, encrypted under a tenant KEK held in the KMS, and stored
// alongside the encrypted data. KMS sees the KEK wrap/unwrap calls;
// it never sees plaintext data and rarely sees plaintext DEKs (only
// during initial generation and KEK rotation).
//
// Adapters live under adapters/kms/{aws,gcp,vault}. kms/inproc provides
// an in-process implementation for tests and SQLite dev.
type KeyStore interface {
	// WrapDEK encrypts a fresh DEK under the tenant's current KEK.
	// Returns the wrapped bytes and the KEK version used (so the
	// caller can store both and unwrap later even after a KEK
	// rotation).
	WrapDEK(ctx context.Context, tenantID string, dek []byte) (wrapped []byte, kekVersion uint32, err error)

	// UnwrapDEK decrypts a wrapped DEK using the named KEK version.
	// kekVersion=0 means "use the most recent" — most callers pass
	// the version stored alongside the wrapped DEK so rotations are
	// transparent.
	UnwrapDEK(ctx context.Context, tenantID string, wrapped []byte, kekVersion uint32) (dek []byte, err error)

	// CurrentKEKVersion returns the version that new wraps should
	// target. Bumped when KEK rotates.
	CurrentKEKVersion(ctx context.Context, tenantID string) (uint32, error)
}

// KEKRotator is implemented by KeyStores that support an in-process
// rotation API (creating a new KEK version usable for subsequent
// WrapDEK calls). External KMS adapters (AWS KMS, GCP KMS) typically
// rotate out-of-band via the KMS provider's own controls and won't
// implement this; the inproc implementation does, primarily for
// tests and single-binary deployments.
type KEKRotator interface {
	// RotateKEK creates a new KEK version for the tenant. Returns
	// the new version number. Existing wrapped DEKs continue to
	// decrypt under their stored kek_version; shred.RewrapDEKs
	// migrates them to the new version on demand.
	RotateKEK(ctx context.Context, tenantID string) (uint32, error)
}
