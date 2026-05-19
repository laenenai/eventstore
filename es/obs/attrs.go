package obs

import (
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Attribute keys used across the framework. Defined as constants so
// callers cannot drift on capitalisation or pluralisation when adding
// new instrumentation.
const (
	AttrTenant     = "eventstore.tenant"
	AttrStreamID   = "eventstore.stream_id"
	AttrAggregate  = "eventstore.aggregate"
	AttrCommand    = "eventstore.command"
	AttrEventCount = "eventstore.event_count"
	AttrVersion    = "eventstore.version"
	AttrTypeURL    = "eventstore.type_url"
	AttrOutcome    = "eventstore.outcome"
)

// Outcome values for the AttrOutcome label on counters.
const (
	OutcomeSuccess = "success"
	OutcomeError   = "error"
)

// Tenant returns the tenant attribute. Multi-tenancy is mandatory in
// this framework (ADR 0007), so every instrumented call site should
// carry one.
func Tenant(t string) attribute.KeyValue {
	return attribute.String(AttrTenant, t)
}

// StreamID returns the canonical stream identifier attribute. Pass
// es.StreamID.String() (tenant/type:id), not Canonical() — the
// tenant/type:id form makes traces self-contained without joining
// against AttrTenant.
func StreamID(sid string) attribute.KeyValue {
	return attribute.String(AttrStreamID, sid)
}

// Aggregate returns the aggregate-type attribute (the "type" component
// of a StreamID — e.g. "counter", "employee"). Optional and only
// emitted when the call site has it cheaply available.
func Aggregate(name string) attribute.KeyValue {
	return attribute.String(AttrAggregate, name)
}

// Command returns the command-type attribute. For codegen-emitted
// commands this is the proto TypeURL or short variant name; for
// hand-rolled tests it's whatever the caller passes.
func Command(name string) attribute.KeyValue {
	return attribute.String(AttrCommand, name)
}

// EventCount returns the count of events emitted by a Decide call (or
// committed by an Append call). Useful for spotting commands that
// fan out unexpectedly.
func EventCount(n int) attribute.KeyValue {
	return attribute.Int(AttrEventCount, n)
}

// Version returns the post-append stream version. Recorded on Handle
// spans so traces show stream progression without trawling event rows.
func Version(v uint64) attribute.KeyValue {
	// OTel attributes don't have a uint64 variant; int64 is the closest
	// fit. Stream versions stay well within int64 range — overflow at
	// 9.2e18 is not a realistic concern.
	return attribute.Int64(AttrVersion, int64(v))
}

// TypeURL returns the event-or-command type URL attribute.
func TypeURL(u string) attribute.KeyValue {
	return attribute.String(AttrTypeURL, u)
}

// Outcome returns the outcome attribute (success / error) for counters
// that need to distinguish happy-path from failure-path increments.
func Outcome(s string) attribute.KeyValue {
	return attribute.String(AttrOutcome, s)
}

// DBSystem returns the standard semantic-convention db.system
// attribute. Storage adapters set this to "sqlite", "postgresql", etc.
// — values come from the semconv package's DBSystem constants.
func DBSystem(name string) attribute.KeyValue {
	return semconv.DBSystemKey.String(name)
}
