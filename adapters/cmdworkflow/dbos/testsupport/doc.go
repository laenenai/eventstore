// Package testsupport provides a Postgres testcontainer harness for
// the cmdworkflow/dbos adapter. Mirrors the cmdworkflow/restate
// testsupport in shape; the key difference is that DBOS is a library
// — no separate Restate container required. One Postgres instance
// hosts both the eventstore tables and the DBOS workflow journal
// (in the `dbos` schema by default).
//
// The harness is intentionally light: spin up Postgres, run the
// eventstore goose migrations, create the DBOSContext (which
// auto-migrates the dbos schema), Launch. That's it.
package testsupport
