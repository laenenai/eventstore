// Package snapshot hosts snapshot primitives: lazy creation on read,
// proto-encoded state in the eventstore DB, strict state_schema_version
// invalidation.
//
// See ADR 0011.
package snapshot
