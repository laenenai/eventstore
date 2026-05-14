// Package kms hosts the KeyStore interface used by crypto-shredding
// (package shred). Adapter implementations live under
// adapters/kms/{inproc,aws,gcp,vault}. inproc is the in-process
// KeyStore for tests and SQLite dev.
//
// See ADR 0010.
package kms
