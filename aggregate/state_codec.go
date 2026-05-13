package aggregate

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// StateCodec marshals aggregate state into the bytes stored in the
// Tier 1 state_cache. See ADR 0020.
//
// Implementations must be deterministic and side-effect-free. Encode
// is called inside Runtime.Handle, between Decide and Append; the
// returned bytes are committed in the same transaction as the events.
//
// Decode is not part of this interface: callers read raw bytes from
// the state cache (via es.StateCacheReader) and unmarshal them
// themselves with the same library the codec used to produce them.
// For proto states, protojson.Unmarshal into a freshly-allocated
// message is the symmetric inverse of ProtoStateCodec.Encode.
type StateCodec[S any] interface {
	Encode(state S) (bytes []byte, typeURL string, err error)
}

// ProtoStateCodec is the default StateCodec for proto.Message states.
// Marshals to JSONB-compatible bytes via protojson. The TypeURL is the
// proto FullName (e.g., "myapp.counter.v1.State").
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
// Non-proto state types remain supported — they just cannot use the
// state cache. Leave StateCodec unset on those aggregates.
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
