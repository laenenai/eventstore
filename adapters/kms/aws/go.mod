module github.com/laenenai/eventstore/adapters/kms/aws

go 1.25.10

// Workspace points at the local checkout during development.
replace github.com/laenenai/eventstore => ../../..

require (
	github.com/aws/aws-sdk-go-v2 v1.32.6
	github.com/aws/aws-sdk-go-v2/service/kms v1.37.7
	github.com/aws/smithy-go v1.22.1
	github.com/laenenai/eventstore v0.0.0-00010101000000-000000000000
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.25 // indirect
)
