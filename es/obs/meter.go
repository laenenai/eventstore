package obs

import (
	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// Meter is the framework-wide meter. When no MeterProvider is
// registered with otel.SetMeterProvider, otel.Meter returns a noop
// Meter and instruments created from it never allocate measurements.
var Meter = otel.Meter("eventstore")

// Package-level instruments. Initialised in init() so each hot-path
// call is a simple instrument-method invocation with no per-call
// instrument lookup. Panics on init failure: instrument creation only
// fails on programmer error (e.g. invalid metric name), so it's
// preferable to fail fast at process start than silently emit nothing.
var (
	// CommandsTotal counts command dispatches through aggregate.Runtime.Handle.
	// Labelled with outcome (success / error). Dimensionally separated
	// from EventsAppendedTotal because one command can emit zero, one,
	// or many events.
	CommandsTotal otelmetric.Int64Counter

	// CommandDuration measures end-to-end Handle latency in seconds.
	// Use seconds (not ms) per OTel guidance — backends scale display
	// units but cannot scale base units.
	CommandDuration otelmetric.Float64Histogram

	// EventsAppendedTotal counts events successfully committed by the
	// storage adapter. Incremented after Append returns nil.
	EventsAppendedTotal otelmetric.Int64Counter

	// StoreAppendDuration measures storage adapter Append latency.
	StoreAppendDuration otelmetric.Float64Histogram

	// StoreReadStreamDuration measures storage adapter ReadStream
	// latency. Often dominates Handle latency for long streams; worth
	// its own series so adopters can spot slow replays distinct from
	// slow appends.
	StoreReadStreamDuration otelmetric.Float64Histogram
)

func init() {
	var err error

	CommandsTotal, err = Meter.Int64Counter(
		"eventstore.commands.total",
		otelmetric.WithDescription("Total commands dispatched through aggregate.Runtime.Handle, labelled by outcome."),
	)
	if err != nil {
		panic("eventstore/obs: create CommandsTotal: " + err.Error())
	}

	CommandDuration, err = Meter.Float64Histogram(
		"eventstore.command.duration",
		otelmetric.WithDescription("End-to-end aggregate.Runtime.Handle duration in seconds."),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		panic("eventstore/obs: create CommandDuration: " + err.Error())
	}

	EventsAppendedTotal, err = Meter.Int64Counter(
		"eventstore.events.appended.total",
		otelmetric.WithDescription("Total events successfully committed by the storage adapter."),
	)
	if err != nil {
		panic("eventstore/obs: create EventsAppendedTotal: " + err.Error())
	}

	StoreAppendDuration, err = Meter.Float64Histogram(
		"eventstore.store.append.duration",
		otelmetric.WithDescription("Storage adapter Append duration in seconds."),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		panic("eventstore/obs: create StoreAppendDuration: " + err.Error())
	}

	StoreReadStreamDuration, err = Meter.Float64Histogram(
		"eventstore.store.read_stream.duration",
		otelmetric.WithDescription("Storage adapter ReadStream duration in seconds."),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		panic("eventstore/obs: create StoreReadStreamDuration: " + err.Error())
	}
}
