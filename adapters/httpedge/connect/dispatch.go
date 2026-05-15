package connectedge

import (
	"context"

	"connectrpc.com/connect"

	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
)

// IdempotencyKeyHeader is the request header consulted for idempotency.
// Convention: "Idempotency-Key" (IETF draft, widely used by Stripe etc.).
// When present, the value is passed to cmdworkflow.WithIdempotencyKey,
// which derives a deterministic command_id used by Restate/DBOS to dedup
// retried invocations.
const IdempotencyKeyHeader = "Idempotency-Key"

// Dispatcher is the minimal command-bus surface this helper needs.
// *cmdworkflow.Workflow[S, C, E] satisfies it for any (S, C, E) —
// the helper does not care about the event type parameter; only
// HandleCmd's signature matters.
type Dispatcher[S, C any] interface {
	HandleCmd(ctx context.Context, sid es.StreamID, cmd C, opts ...cmdworkflow.HandleCmdOption) (S, error)
}

// Decoder lifts a request message into (StreamID, sealed command sum type).
// Callers supply one per RPC. The framework intentionally does not generate
// this — see package doc for why.
type Decoder[ReqMsg, C any] func(*ReqMsg) (es.StreamID, C, error)

// Dispatch executes the bus call for one Connect RPC.
//
// It decodes the request via decode, passes any Idempotency-Key header
// through to the bus, calls HandleCmd, and returns the post-Decide
// aggregate state. The caller wraps the state into a *connect.Response[T]
// of their choosing — return the state directly, project to a DTO, or
// return a fixed ack.
//
// Errors are mapped:
//   - decode errors → connect.CodeInvalidArgument
//   - bus errors    → MapError (see errors.go)
func Dispatch[ReqMsg, S, C any](
	ctx context.Context,
	bus Dispatcher[S, C],
	req *connect.Request[ReqMsg],
	decode Decoder[ReqMsg, C],
) (S, error) {
	var zero S

	sid, cmd, err := decode(req.Msg)
	if err != nil {
		return zero, connect.NewError(connect.CodeInvalidArgument, err)
	}

	var opts []cmdworkflow.HandleCmdOption
	if key := req.Header().Get(IdempotencyKeyHeader); key != "" {
		opts = append(opts, cmdworkflow.WithIdempotencyKey(key))
	}

	state, err := bus.HandleCmd(ctx, sid, cmd, opts...)
	if err != nil {
		return zero, MapError(err)
	}
	return state, nil
}
