package aggregate

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// StateCodec marshals and unmarshals aggregate state.
//
// Encode is used by the Tier 1 state_cache write path (ADR 0020 — runs
// inside Runtime.Handle between Decide and Append; the returned bytes
// commit in the same transaction as the events). Decode is used by the
// snapshot read path (ADR 0011 — Load checks the snapshot store first
// and unmarshals the bytes back into S when the schema matches).
//
// Implementations must be deterministic and side-effect-free.
type StateCodec[S any] interface {
	Encode(state S) (bytes []byte, typeURL string, err error)
	Decode(bytes []byte) (S, error)
}

// ProtoStateCodec is the default StateCodec for proto.Message states.
// Marshals via protojson — bytes are JSONB-compatible for state_cache
// queries and big enough to round-trip schema-evolving messages
// safely. Snapshot storage reuses the same encoding for simplicity.
//
// Usage:
//
//	rt := &aggregate.Runtime[*counterv1.State, counterv1.Command, counterv1.Event]{
//	    Store:      store,
//	    Decider:    counter.Decider,
//	    Codec:      counter.EventCodec{},
//	    StateCodec: aggregate.ProtoStateCodec[*counterv1.State]{},
//	}
//
// Non-proto state types are not supported by this codec; non-proto
// aggregates either skip the state cache + snapshots, or supply a
// hand-rolled StateCodec.
type ProtoStateCodec[T proto.Message] struct{}

// Encode marshals the state to JSON via protojson and derives the
// type URL from the proto descriptor.
func (ProtoStateCodec[T]) Encode(state T) ([]byte, string, error) {
	b, err := protojson.Marshal(state)
	if err != nil {
		return nil, "", err
	}
	typeURL := string(state.ProtoReflect().Descriptor().FullName())
	return b, typeURL, nil
}

// Decode unmarshals into a freshly-allocated T. Uses reflection to
// instantiate the concrete proto type from the zero value's element
// type — the standard Go-generics workaround for "T is a pointer
// type and I need a new one".
func (ProtoStateCodec[T]) Decode(data []byte) (T, error) {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil || t.Kind() != reflect.Pointer {
		return zero, fmt.Errorf("aggregate: ProtoStateCodec requires T to be a pointer type, got %T", zero)
	}
	msg := reflect.New(t.Elem()).Interface()
	out, ok := msg.(T)
	if !ok {
		return zero, fmt.Errorf("aggregate: ProtoStateCodec.Decode: type assertion failed for %T", msg)
	}
	if err := protojson.Unmarshal(data, msg.(proto.Message)); err != nil {
		return zero, err
	}
	return out, nil
}
