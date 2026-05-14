module github.com/laenenai/eventstore/examples/connectedge

go 1.25.10

require (
	connectrpc.com/connect v1.18.1
	github.com/laenenai/eventstore v0.0.0-00010101000000-000000000000
	github.com/laenenai/eventstore/adapters/httpedge/connect v0.0.0-00010101000000-000000000000
	github.com/laenenai/eventstore/adapters/storage/sqlite v0.0.0-00010101000000-000000000000
	github.com/laenenai/eventstore/examples/employee v0.0.0-00010101000000-000000000000
	modernc.org/sqlite v1.50.1
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.21 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/pressly/goose/v3 v3.27.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	modernc.org/libc v1.72.3 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/laenenai/eventstore => ../..

replace github.com/laenenai/eventstore/adapters/storage/sqlite => ../../adapters/storage/sqlite

replace github.com/laenenai/eventstore/adapters/httpedge/connect => ../../adapters/httpedge/connect

replace github.com/laenenai/eventstore/examples/employee => ../employee
