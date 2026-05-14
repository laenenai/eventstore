package connectedge_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	connectedge "github.com/laenenai/eventstore/adapters/httpedge/connect"
	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
)

// fakeBus is a stand-in for *cmdworkflow.Workflow[S, C]. Records the
// last call so tests can assert on what was forwarded.
type fakeBus struct {
	lastSID  es.StreamID
	lastCmd  string
	lastOpts int
	respond  string
	failWith error
}

func (b *fakeBus) HandleCmd(
	_ context.Context, sid es.StreamID, cmd string,
	opts ...cmdworkflow.HandleCmdOption,
) (string, error) {
	b.lastSID = sid
	b.lastCmd = cmd
	b.lastOpts = len(opts)
	if b.failWith != nil {
		return "", b.failWith
	}
	return b.respond, nil
}

func newRequest(t *testing.T, msg string, hdr http.Header) *connect.Request[string] {
	t.Helper()
	req := connect.NewRequest(&msg)
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header().Add(k, v)
		}
	}
	return req
}

func mustSID(t *testing.T) es.StreamID {
	t.Helper()
	sid, err := es.ParseCanonical("t-1", "employee:emp-1")
	if err != nil {
		t.Fatalf("ParseCanonical: %v", err)
	}
	return sid
}

func TestDispatch_HappyPath(t *testing.T) {
	bus := &fakeBus{respond: "ok-state"}
	wantSID := mustSID(t)

	state, err := connectedge.Dispatch(
		context.Background(), bus,
		newRequest(t, "hire-payload", nil),
		func(m *string) (es.StreamID, string, error) {
			return wantSID, "lifted:" + *m, nil
		},
	)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if state != "ok-state" {
		t.Errorf("state: got %q want ok-state", state)
	}
	if bus.lastCmd != "lifted:hire-payload" {
		t.Errorf("cmd: got %q", bus.lastCmd)
	}
	if bus.lastSID.Canonical() != wantSID.Canonical() {
		t.Errorf("sid: got %v want %v", bus.lastSID, wantSID)
	}
	if bus.lastOpts != 0 {
		t.Errorf("opts: got %d want 0", bus.lastOpts)
	}
}

func TestDispatch_IdempotencyKeyHeaderForwarded(t *testing.T) {
	bus := &fakeBus{respond: "ok"}
	hdr := http.Header{}
	hdr.Set(connectedge.IdempotencyKeyHeader, "req-42")

	_, err := connectedge.Dispatch(
		context.Background(), bus,
		newRequest(t, "x", hdr),
		func(m *string) (es.StreamID, string, error) {
			return mustSID(t), *m, nil
		},
	)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if bus.lastOpts != 1 {
		t.Errorf("expected 1 opt forwarded, got %d", bus.lastOpts)
	}
}

func TestDispatch_DecodeErrorIsInvalidArgument(t *testing.T) {
	bus := &fakeBus{}
	_, err := connectedge.Dispatch(
		context.Background(), bus,
		newRequest(t, "x", nil),
		func(m *string) (es.StreamID, string, error) {
			return es.StreamID{}, "", errors.New("bad input")
		},
	)
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("want *connect.Error, got %T: %v", err, err)
	}
	if ce.Code() != connect.CodeInvalidArgument {
		t.Errorf("code: got %v want InvalidArgument", ce.Code())
	}
}

func TestDispatch_BusErrorIsMapped(t *testing.T) {
	bus := &fakeBus{failWith: es.ErrConflict}
	_, err := connectedge.Dispatch(
		context.Background(), bus,
		newRequest(t, "x", nil),
		func(m *string) (es.StreamID, string, error) {
			return mustSID(t), *m, nil
		},
	)
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("want *connect.Error, got %T", err)
	}
	if ce.Code() != connect.CodeAborted {
		t.Errorf("ErrConflict should map to Aborted, got %v", ce.Code())
	}
}

func TestMapError_Table(t *testing.T) {
	cases := []struct {
		name string
		in   error
		want connect.Code
	}{
		{"conflict", es.ErrConflict, connect.CodeAborted},
		{"constraint", es.ErrConstraintViolated, connect.CodeAlreadyExists},
		{"terminal", es.ErrTerminal, connect.CodeFailedPrecondition},
		{"invalid-sid", es.ErrInvalidStreamID, connect.CodeInvalidArgument},
		{"no-tenant", es.ErrTenantMissing, connect.CodeUnauthenticated},
		{"stream-nf", es.ErrStreamNotFound, connect.CodeNotFound},
		{"event-nf", es.ErrEventNotFound, connect.CodeNotFound},
		{"state-nf", es.ErrStateNotFound, connect.CodeNotFound},
		{"kms", es.ErrKMSUnavailable, connect.CodeUnavailable},
		{"crypto", es.ErrCryptoIntegrity, connect.CodeDataLoss},
		{"schema", es.ErrUnknownSchemaVersion, connect.CodeInternal},
		{"unknown", errors.New("random"), connect.CodeUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := connectedge.MapError(tc.in)
			var ce *connect.Error
			if !errors.As(out, &ce) {
				t.Fatalf("want *connect.Error, got %T", out)
			}
			if ce.Code() != tc.want {
				t.Errorf("code: got %v want %v", ce.Code(), tc.want)
			}
		})
	}
}

func TestMapError_NilPassthrough(t *testing.T) {
	if connectedge.MapError(nil) != nil {
		t.Error("MapError(nil) must return nil")
	}
}

func TestMapError_PreservesExistingConnectError(t *testing.T) {
	in := connect.NewError(connect.CodePermissionDenied, errors.New("no"))
	out := connectedge.MapError(in)
	if out != in {
		t.Errorf("existing *connect.Error should pass through unchanged")
	}
}
