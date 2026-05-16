// Hand-written upcaster for the test.piimigration.v1.Hired event.
// Lives alongside the codegen-emitted EventCodec because that's where
// the fixture is targeted — see cookbook recipe 11 § "PII shape
// migration (Tier D, ADR 0030)".
//
// This file is NOT generated. Do not regenerate it; do not move it.
// The codegen plugin only emits *.pb.go and *_es.pb.go; everything
// else under gen/test/piimigration/v1/ is fixture code.
package piimigrationv1

import (
	"encoding/base64"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/aggregate"
	legacyv1 "github.com/laenenai/eventstore/gen/test/piimigration/v1/legacy"
)

// MigratingCodec wraps the codegen-emitted EventCodec and upcasts
// schema_version=1 Hired events (legacy bytes-of-ciphertext shape)
// to the current schema_version=2 shape (base64-of-the-same-
// ciphertext string) at read time.
//
// Tier D per ADR 0030: classification or encryption shape changed.
// The DEK doesn't move, the plaintext doesn't change, and the
// envelope's type_url stays the same. Only the wire encoding of two
// fields shifts (proto bytes → proto string with base64). The on-
// disk DEK in subject_keys works unchanged because DecryptPII calls
// base64.RawStdEncoding.DecodeString on the string field and then
// hands the raw ciphertext back to shred.DecryptField — the exact
// same input shape the v1 EncryptField produced.
//
// Encode always emits the current shape; existing v1 events on disk
// are read through Decode's upcaster path and never rewritten in
// place. Greenfield deployments register this codec defensively so
// adopters who roll in mid-migration get the path for free.
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
// below 2) takes the upcaster path: for the variants whose wire
// type changed we unmarshal via the frozen legacy proto and re-
// project the fields into the current shape. The base64 transform
// is intentional — it converts raw ciphertext bytes into the
// UTF-8-valid encoding that the current shape's DecryptPII expects
// to find on disk. No KMS interaction happens here; ciphertext
// stays ciphertext until aggregate.Runtime calls DecryptPII on the
// returned event.
//
// Variants whose wire shape DIDN'T change between v1 and v2 fall
// through to the inner codec even when schemaVersion=1. The
// upcaster path is reserved for the shapes that actually moved.
// Unknown type URLs also pass through so the inner codec produces
// the canonical "unknown type_url" error rather than this wrapper.
func (c MigratingCodec) Decode(typeURL string, schemaVersion uint32, payload []byte) (Event, error) {
	if schemaVersion >= 2 {
		return c.Inner.Decode(typeURL, schemaVersion, payload)
	}
	switch typeURL {
	case "test.piimigration.v1.Hired":
		var old legacyv1.HiredV1
		if err := proto.Unmarshal(payload, &old); err != nil {
			return nil, fmt.Errorf("MigratingCodec.Decode upcast Hired v1: %w", err)
		}
		return &Hired{
			EmployeeId: old.EmployeeId,
			// Raw ciphertext bytes → base64-encoded string. Same
			// ciphertext, same DEK, same plaintext after DecryptPII.
			// Empty input produces an empty string (no spurious
			// base64 of zero bytes) — the codegen's DecryptPII
			// skips empty strings anyway, but matching the v1
			// "field unset" semantics keeps the upcast lossless.
			LegalName: encodeLegacyField(old.LegalName),
			Email:     encodeLegacyField(old.Email),
		}, nil
	}
	return c.Inner.Decode(typeURL, schemaVersion, payload)
}

// encodeLegacyField turns raw v1 ciphertext bytes into the v2
// base64-string form. Empty in → empty out so the round-trip
// preserves "unset" semantics. base64.RawStdEncoding matches the
// encoder that codegen-emitted EncryptPII uses on the current
// shape — keeping the two in lockstep is the whole point.
func encodeLegacyField(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return base64.RawStdEncoding.EncodeToString(b)
}
