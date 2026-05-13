// Package kms hosts the KeyStore interface used by crypto-shredding
// (package shred). Adapter implementations live under
// adapters/kms/{aws,gcp,vault}. The inproc subpackage provides an
// in-process KeyStore for tests and SQLite dev.
//
// See ADR 0010.
package kms
