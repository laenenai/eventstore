module github.com/laenenai/eventstore/adapters/authz/cedar

go 1.25.7

// Workspace points at the local checkout during development.
replace github.com/laenenai/eventstore => ../../..

require (
	github.com/cedar-policy/cedar-go v1.6.1
	github.com/laenenai/eventstore v0.0.0-00010101000000-000000000000
)

require (
	github.com/google/uuid v1.6.0 // indirect
	golang.org/x/exp v0.0.0-20260410095643-746e56fc9e2f // indirect
)
