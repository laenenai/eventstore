module github.com/laenenai/eventstore/adapters/httpedge/connect

go 1.25.10

// Workspace points at the local checkout during development.
replace github.com/laenenai/eventstore => ../../..

// The cmdworkflow package this helper depends on has test files that
// import the sqlite adapter; go mod tidy chases transitive test deps
// and needs the sibling module wired (only used by sibling tests, not
// linked into the helper or its consumers).
replace github.com/laenenai/eventstore/adapters/storage/sqlite => ../../storage/sqlite

require (
	connectrpc.com/connect v1.18.1
	github.com/laenenai/eventstore v0.0.0-00010101000000-000000000000
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/laenenai/eventstore/adapters/storage/sqlite v0.0.0-00010101000000-000000000000 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
