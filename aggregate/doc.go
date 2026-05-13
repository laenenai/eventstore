// Package aggregate hosts the aggregate runtime: load (replay + optional
// snapshot), handle command via Decider, append events with optimistic
// concurrency and constraint claims, atomically.
//
// See ADR 0003 (decider model) and ADR 0009 (Postgres global position).
package aggregate
