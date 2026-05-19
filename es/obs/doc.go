// Package obs is the framework's OpenTelemetry + slog integration
// surface. The package is intentionally tiny and direct:
//
//   - Tracer / Meter are obtained from the global providers
//     (otel.Tracer, otel.Meter). When no provider is registered, the
//     OTel SDK's built-in no-op implementation kicks in — adopters who
//     don't wire a TracerProvider/MeterProvider pay no allocations and
//     no measurable runtime overhead.
//
//   - Adopters opt in by calling otel.SetTracerProvider /
//     otel.SetMeterProvider once at process boot (typically next to
//     the existing slog.SetDefault wiring).
//
//   - The framework does NOT define a custom Tracer / Meter
//     abstraction. The OTel API is the abstraction. A custom interface
//     would add maintenance cost without protecting consumers from
//     change — the OTel v1.x API has been stable since 2021.
//
// Naming conventions:
//
//   - Tracer / Meter instrumentation name: "eventstore".
//   - Framework attributes are prefixed "eventstore." (e.g.
//     "eventstore.tenant", "eventstore.stream_id").
//   - Where a standard OTel semantic convention applies (e.g.
//     db.system for storage adapters), the standard key wins.
//   - Histogram durations are reported in seconds with metric.WithUnit("s").
//
// This package is the *only* place where the framework imports the
// OTel API directly. Hot-path code in aggregate/, adapters/storage/*,
// etc. depends on obs and not on go.opentelemetry.io/otel — keeping
// the OTel dependency surface contained to one package.
package obs
