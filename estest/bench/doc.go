// Package bench is the benchmark/load-test harness for the framework's
// capacity spikes. Each spike's scenario lands here as a Go test
// fixture so the harness is reproducible across runs and can be
// re-run against any new schema or runtime change.
//
// Distinct from estest's conformance suite (which proves correctness)
// — bench measures performance and capacity. The two share scaffolding
// (testcontainers Postgres lifecycle) but answer different questions.
//
// See docs/spikes/0001-laenen-tenancy.md for the first user.
package bench
