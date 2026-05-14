package aggregate

import (
	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/es"
)

// NewProto constructs a Runtime for an aggregate whose state type is a
// proto message. Pre-wires ProtoStateCodec so the state_cache row is
// written in-tx with every Append (ADR 0020 Tier 1, read-your-writes).
//
// Equivalent to:
//
//	&Runtime[S, C, E]{
//	    Store:      store,
//	    Decider:    decider,
//	    Codec:      eventCodec,
//	    StateCodec: ProtoStateCodec[S]{},
//	}
//
// For non-proto state aggregates or aggregates that opt out of the
// state cache, use the struct literal form directly and omit the
// StateCodec field.
func NewProto[S proto.Message, C, E any](
	store es.Store,
	decider es.Decider[S, C, E],
	eventCodec Codec[E],
) *Runtime[S, C, E] {
	return &Runtime[S, C, E]{
		Store:      store,
		Decider:    decider,
		Codec:      eventCodec,
		StateCodec: ProtoStateCodec[S]{},
	}
}
