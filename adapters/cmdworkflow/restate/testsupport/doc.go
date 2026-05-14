// Package testsupport provides a Restate testcontainer harness for
// the cmdworkflow/restate adapter. It mirrors the SDK's
// testing.Start helper but compiles against testcontainers-go v0.42+
// (the SDK helper is pinned to v0.40 and references a removed
// nat.Port.Int method).
//
// Beyond convenience, the code is intentionally shaped like a
// production self-registration: start an HTTP/2 server hosting the
// SDK handlers, register its URL with Restate's admin endpoint.
// Production deployments do the same three steps; the only
// differences are (1) the Restate cluster URL is fixed, (2) the SDK
// server's public URL is the application's externally-reachable
// hostname, and (3) registration is typically done once at deploy
// time, not per test.
package testsupport
