package obs

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the framework-wide tracer. When no TracerProvider is
// registered with otel.SetTracerProvider, otel.Tracer returns a noop
// Tracer and every Start call is effectively free.
//
// The instrumentation name "eventstore" matches the otel.Meter name —
// adopters filter spans + metrics under one stable prefix.
var Tracer = otel.Tracer("eventstore")

// Start opens a child span on the current tracer with the supplied
// attributes attached at creation time (rather than via SetAttributes
// after the fact — attributes set at span start are cheaper for SDK
// implementations and survive sampling decisions).
//
// Callers must Span.End the returned span, typically via defer.
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// EndWithErr records err on span when non-nil and sets the span status
// to Error. When err is nil the span is left untouched (success is the
// default span status; explicitly setting Ok is discouraged by the
// OTel spec and the SDK already treats unset as Unset/OK).
//
// EndWithErr does NOT call span.End — pair it with a separate defer to
// keep the caller's control flow obvious.
func EndWithErr(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
