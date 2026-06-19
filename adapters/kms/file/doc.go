// Package file is a kms.KeyStore that persists tenant KEKs to a
// local JSON file. It sits between the framework's inproc KeyStore
// (KEKs in memory only — fine for tests, fatal across CLI restarts)
// and the production-grade adapters/kms/aws (HSM-backed, requires
// AWS account + IAM). The file adapter is the right choice for:
//
//   - local development of an event-sourced app with persistent
//     PII (a long-running SQLite chat backend, an integration-test
//     harness that survives `go test -count=3`),
//   - single-binary deployments where the operator is content
//     managing secret material as a file (cron-job rotation, copy
//     to backup, etc.),
//   - tutorials and demos where pulling in an HSM is overkill but
//     in-memory KEKs would obscure the framework's contract.
//
// # Threat model
//
// Anyone with read access to the JSON file can decrypt every wrapped
// DEK in the event store and from there every PERSONAL field. The
// file is written with mode 0600 (owner-only) and the containing
// directory with 0700; an operator who needs stronger guarantees
// runs `adapters/kms/aws` instead.
//
// On-disk format is a single JSON object keyed by tenant; each
// value is the ordered list of KEK versions. Older versions are
// retained forever so historical wrappings stay decryptable across
// rotations.
//
// # ADR reference
//
// ADR 0010 (crypto-shredding) — the DEK/KEK design this implements.
// ADR 0027 (data governance) — the classification semantics that
// drive which fields get wrapped under these KEKs.
package file
