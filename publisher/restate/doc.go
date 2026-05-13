// Package restate is the Restate publisher adapter (ADR 0012). It
// publishes events via HTTP POST to a Restate ingress, using
// idempotency-key = event_id so Restate's durable invocation
// deduplicates at-least-once retries from the outbox drain.
//
// The adapter is intentionally thin: Restate handles durability,
// retries, and exactly-once dispatch on the receiver side. The
// framework just hands an envelope over; the receiver service (user
// code, typically a Restate handler) decodes by TypeURL.
//
// Configuration:
//
//	pub := restate.New(restate.Config{
//	    IngressURL:  "http://restate-ingress:8080",
//	    Service:     "event-dispatcher",  // your Restate service name
//	    Handler:     "OnEvent",           // handler within the service
//	})
//
// On Publish, the adapter POSTs:
//
//	POST {IngressURL}/{Service}/{Handler}
//	Content-Type: application/x-protobuf
//	Idempotency-Key: <event_id>
//	X-EventStore-Tenant: <tenant_id>
//	X-EventStore-Stream: <stream_canonical>
//	X-EventStore-Version: <version>
//	X-EventStore-Global-Position: <global_position>
//	X-EventStore-Type-URL: <event_type_url>
//	X-EventStore-Schema-Version: <event_schema_version>
//
//	Body: envelope payload bytes
//
// The receiver's contract: read the body proto using the type URL
// header (or a registered codec) to decode, return 200 on success. A
// non-2xx response causes Publish to return an error, leaving the
// outbox row unmarked for retry on the next drain cycle.
package restate
