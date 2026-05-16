package unitsmigrationv1_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	unitsmigrationv1 "github.com/laenenai/eventstore/gen/test/unitsmigration/v1"
	legacyv1 "github.com/laenenai/eventstore/gen/test/unitsmigration/v1/legacy"
)

// TestMigratingCodec_UpcastsLegacyMillisecondsToMicroseconds walks the
// happy-path fixture from cookbook recipe 21:
//
//  1. Marshal a v1 MeasurementRecorded (legacy shape) where
//     `latency_ms` carries 5 (i.e. 5 milliseconds).
//  2. Pipe the bytes through MigratingCodec.Decode at
//     schemaVersion=1 — dispatches to the upcaster.
//  3. Assert the decoded event is the CURRENT shape with
//     LatencyUs=5000 (5 ms = 5000 µs) and the subject identifier
//     preserved verbatim.
//
// This is the headline guarantee of the Tier-B pattern: byte-stable
// storage + value-corrected reads.
func TestMigratingCodec_UpcastsLegacyMillisecondsToMicroseconds(t *testing.T) {
	const (
		measurementID  = "measurement-42"
		legacyMs       = int64(5)
		expectedMicros = int64(5_000)
	)

	v1 := &legacyv1.MeasurementRecordedV1{
		MeasurementId: measurementID,
		LatencyMs:     legacyMs,
	}
	payload, err := proto.Marshal(v1)
	if err != nil {
		t.Fatalf("proto.Marshal MeasurementRecordedV1: %v", err)
	}

	codec := unitsmigrationv1.MigratingCodec{Inner: unitsmigrationv1.EventCodec{}}
	decoded, err := codec.Decode("test.unitsmigration.v1.MeasurementRecorded", 1, payload)
	if err != nil {
		t.Fatalf("MigratingCodec.Decode v1: %v", err)
	}
	got, ok := decoded.(*unitsmigrationv1.MeasurementRecorded)
	if !ok {
		t.Fatalf("upcaster returned wrong type: got %T want *MeasurementRecorded", decoded)
	}
	if got.MeasurementId != measurementID {
		t.Errorf("measurement_id mismatch: got %q want %q", got.MeasurementId, measurementID)
	}
	if got.LatencyUs != expectedMicros {
		t.Errorf("latency_us mismatch: got %d want %d (expected ms→us *1000)", got.LatencyUs, expectedMicros)
	}
}

// TestMigratingCodec_PassesThroughCurrentSchemaUnchanged verifies the
// upcaster only kicks in for legacy versions. At schema_version=2 the
// payload is already the current shape and Decode must delegate to
// the codegen-emitted EventCodec verbatim — no spurious *1000
// multiplication on an already-microsecond value.
func TestMigratingCodec_PassesThroughCurrentSchemaUnchanged(t *testing.T) {
	codec := unitsmigrationv1.MigratingCodec{Inner: unitsmigrationv1.EventCodec{}}

	orig := &unitsmigrationv1.MeasurementRecorded{
		MeasurementId: "measurement-7",
		LatencyUs:     12_345,
	}
	payload, err := proto.Marshal(orig)
	if err != nil {
		t.Fatalf("proto.Marshal MeasurementRecorded: %v", err)
	}

	decoded, err := codec.Decode("test.unitsmigration.v1.MeasurementRecorded", 2, payload)
	if err != nil {
		t.Fatalf("MigratingCodec.Decode v2: %v", err)
	}
	got, ok := decoded.(*unitsmigrationv1.MeasurementRecorded)
	if !ok {
		t.Fatalf("decode returned wrong type: got %T want *MeasurementRecorded", decoded)
	}
	if got.MeasurementId != orig.MeasurementId || got.LatencyUs != orig.LatencyUs {
		t.Errorf("v2 passthrough mutated event: got %+v want %+v", got, orig)
	}
}

// TestMigratingCodec_HandlesZeroValue proves the upcaster preserves
// the "field unset" / zero-value semantics. v1 events with
// latency_ms=0 must produce v2 events with latency_us=0 — multiplying
// zero by anything keeps it zero, which is exactly what we want: the
// proto3 default of 0 is indistinguishable on the wire from a field
// that was never set, and the upcaster shouldn't fabricate a non-zero
// reading.
func TestMigratingCodec_HandlesZeroValue(t *testing.T) {
	v1 := &legacyv1.MeasurementRecordedV1{
		MeasurementId: "measurement-empty",
		// LatencyMs left as 0
	}
	payload, err := proto.Marshal(v1)
	if err != nil {
		t.Fatalf("proto.Marshal MeasurementRecordedV1: %v", err)
	}

	codec := unitsmigrationv1.MigratingCodec{Inner: unitsmigrationv1.EventCodec{}}
	decoded, err := codec.Decode("test.unitsmigration.v1.MeasurementRecorded", 1, payload)
	if err != nil {
		t.Fatalf("MigratingCodec.Decode v1 zero: %v", err)
	}
	got := decoded.(*unitsmigrationv1.MeasurementRecorded)
	if got.MeasurementId != "measurement-empty" {
		t.Errorf("measurement_id mismatch: got %q want %q", got.MeasurementId, "measurement-empty")
	}
	if got.LatencyUs != 0 {
		t.Errorf("latency_us should be 0 after upcast of latency_ms=0, got %d", got.LatencyUs)
	}
}

// TestMigratingCodec_UnknownTypeURL ensures unrecognized type URLs
// fall through to the inner codec, which produces the canonical
// "unknown type_url" error. The wrapper must not swallow that path
// at either schema version — adopters depend on a single,
// predictable error surface for routing/observability.
func TestMigratingCodec_UnknownTypeURL(t *testing.T) {
	codec := unitsmigrationv1.MigratingCodec{Inner: unitsmigrationv1.EventCodec{}}

	// schema_version=1: the wrapper's switch doesn't match the
	// unknown URL and falls through to the inner codec.
	_, err := codec.Decode("test.unitsmigration.v1.Unknown", 1, []byte("ignored"))
	if err == nil {
		t.Fatal("expected error for unknown type_url at v1, got nil")
	}

	// schema_version=2: the wrapper short-circuits to the inner
	// codec; same error contract.
	_, err = codec.Decode("test.unitsmigration.v1.Unknown", 2, []byte("ignored"))
	if err == nil {
		t.Fatal("expected error for unknown type_url at v2, got nil")
	}
}
