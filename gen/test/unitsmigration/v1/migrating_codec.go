// Hand-written upcaster for the test.unitsmigration.v1.MeasurementRecorded
// event. Lives alongside the codegen-emitted EventCodec because that's
// where the fixture is targeted — see cookbook recipe 21 "Schema
// evolution — Tier-B upcaster (unit conversion)".
//
// This file is NOT generated. Do not regenerate it; do not move it.
// The codegen plugin only emits *.pb.go and *_es.pb.go; everything
// else under gen/test/unitsmigration/v1/ is fixture code.
package unitsmigrationv1

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/aggregate"
	legacyv1 "github.com/laenenai/eventstore/gen/test/unitsmigration/v1/legacy"
)

// MigratingCodec wraps the codegen-emitted EventCodec and upcasts
// schema_version=1 MeasurementRecorded events (legacy millisecond
// shape) to the current schema_version=2 shape (microseconds) at
// read time.
//
// Tier B per ADR 0030: same wire encoding, different meaning. The
// envelope's type_url stays the same; only the *interpretation* of
// the int64 changes. Existing v1 events on disk are not rewritten —
// the bytes survive verbatim, and every read flows through Decode's
// schemaVersion switch.
//
// Encode always emits the current shape; the upcaster path is
// strictly read-only. Greenfield deployments still register this
// codec defensively so adopters who roll in mid-migration get the
// path for free.
type MigratingCodec struct {
	Inner EventCodec
}

// Compile-time assertion: MigratingCodec satisfies the framework
// Codec contract for the Event sealed type.
var _ aggregate.Codec[Event] = MigratingCodec{}

// Encode delegates unchanged to the codegen-emitted codec. Writes
// always use the current shape — the upcaster path is read-only.
func (c MigratingCodec) Encode(e Event) (aggregate.EncodedEvent, error) {
	return c.Inner.Encode(e)
}

// Decode dispatches on schemaVersion. v2 (and any future version)
// falls through to the inner codec's native decode. v1 (or anything
// below 2) takes the upcaster path: unmarshal via the frozen legacy
// proto and translate the millisecond reading into microseconds
// before handing the current-shape event back to the runtime.
//
// Variants whose meaning DIDN'T change between v1 and v2 would fall
// through to the inner codec even at schemaVersion=1. This fixture
// has only one variant so the switch is exhaustive — adopters with
// more variants should match the upcaster cases narrowly and let
// untouched type_urls fall through.
//
// Unknown type URLs also pass through so the inner codec produces
// the canonical "unknown type_url" error rather than this wrapper.
// Adopters want a single, predictable error surface.
func (c MigratingCodec) Decode(typeURL string, schemaVersion uint32, payload []byte) (Event, error) {
	if schemaVersion >= 2 {
		return c.Inner.Decode(typeURL, schemaVersion, payload)
	}
	switch typeURL {
	case "test.unitsmigration.v1.MeasurementRecorded":
		var old legacyv1.MeasurementRecordedV1
		if err := proto.Unmarshal(payload, &old); err != nil {
			return nil, fmt.Errorf("MigratingCodec.Decode upcast MeasurementRecorded v1: %w", err)
		}
		// Multiply by 1000 because 1 ms = 1000 µs. The transform is
		// pure (ADR 0013) — same input always produces the same
		// output, no clocks, no I/O. Zero in → zero out preserves
		// the "field unset" semantics for free.
		return &MeasurementRecorded{
			MeasurementId: old.MeasurementId,
			LatencyUs:     msToUs(old.LatencyMs),
		}, nil
	}
	return c.Inner.Decode(typeURL, schemaVersion, payload)
}

// msToUs converts a millisecond reading to microseconds. Lifted
// into a named function so the conversion factor is the only thing
// a reviewer has to verify — and so future Tier-B-on-Tier-B
// migrations (us → ns?) have an obvious place to chain.
//
// int64 has plenty of headroom: the largest representable ms value
// in int64 is ~292 million years; multiplying by 1000 still fits
// (~292 thousand years). Real latency_ms readings are bounded by
// the timeout limits of the systems producing them.
func msToUs(ms int64) int64 { return ms * 1000 }
