package projection_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/es"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
	"github.com/laenenai/eventstore/projection"
)

// counterProj is a minimal Projection impl used to verify dispatch.
type counterProj struct {
	inits, incs, decs int
}

func (c *counterProj) OnInitialized(_ context.Context, _ es.Envelope, _ *counterv1.Initialized) error {
	c.inits++
	return nil
}
func (c *counterProj) OnIncremented(_ context.Context, _ es.Envelope, _ *counterv1.Incremented) error {
	c.incs++
	return nil
}
func (c *counterProj) OnDecremented(_ context.Context, _ es.Envelope, _ *counterv1.Decremented) error {
	c.decs++
	return nil
}

func envWith(t *testing.T, e proto.Message, typeURL string) es.Envelope {
	t.Helper()
	b, err := proto.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return es.Envelope{TypeURL: typeURL, SchemaVersion: 1, Payload: b}
}

// TestDispatcher_RoutesEachVariant verifies the generated dispatcher
// invokes the correct typed method for each event.
func TestDispatcher_RoutesEachVariant(t *testing.T) {
	p := &counterProj{}
	h := counterv1.NewProjectionDispatcher(p)
	ctx := context.Background()

	if err := h(ctx, envWith(t, &counterv1.Initialized{Min: 0, Max: 10, Value: 1}, "test.counter.v1.Initialized")); err != nil {
		t.Fatalf("init dispatch: %v", err)
	}
	if err := h(ctx, envWith(t, &counterv1.Incremented{By: 1}, "test.counter.v1.Incremented")); err != nil {
		t.Fatalf("inc dispatch: %v", err)
	}
	if err := h(ctx, envWith(t, &counterv1.Decremented{By: 2}, "test.counter.v1.Decremented")); err != nil {
		t.Fatalf("dec dispatch: %v", err)
	}
	if p.inits != 1 || p.incs != 1 || p.decs != 1 {
		t.Errorf("counts: got %+v want {1,1,1}", p)
	}
}

// TestDispatcher_UnknownTypeURL_DefaultErrors verifies that without
// IgnoreUnknown, unknown TypeURLs produce an error (Q1B safety property).
func TestDispatcher_UnknownTypeURL_DefaultErrors(t *testing.T) {
	p := &counterProj{}
	h := counterv1.NewProjectionDispatcher(p)
	err := h(context.Background(), es.Envelope{TypeURL: "myapp.other.v1.Whatever"})
	if err == nil {
		t.Fatalf("expected error on unknown TypeURL, got nil")
	}
}

// TestDispatcher_UnknownTypeURL_IgnoreUnknown verifies the opt-in skip.
func TestDispatcher_UnknownTypeURL_IgnoreUnknown(t *testing.T) {
	p := &counterProj{}
	h := counterv1.NewProjectionDispatcher(p, projection.IgnoreUnknown())
	err := h(context.Background(), es.Envelope{TypeURL: "myapp.other.v1.Whatever"})
	if err != nil {
		t.Errorf("IgnoreUnknown: got err=%v want nil", err)
	}
	if p.inits != 0 || p.incs != 0 || p.decs != 0 {
		t.Errorf("counts should stay zero: %+v", p)
	}
}

// TestDispatcher_PropagatesHandlerError verifies error returns from
// the typed methods reach the caller of the dispatcher.
func TestDispatcher_PropagatesHandlerError(t *testing.T) {
	want := errors.New("boom")
	h := counterv1.NewProjectionDispatcher(&erroringProj{err: want})
	got := h(context.Background(), envWith(t, &counterv1.Initialized{}, "test.counter.v1.Initialized"))
	if !errors.Is(got, want) {
		t.Errorf("error propagation: got %v want %v", got, want)
	}
}

type erroringProj struct{ err error }

func (p *erroringProj) OnInitialized(context.Context, es.Envelope, *counterv1.Initialized) error {
	return p.err
}
func (p *erroringProj) OnIncremented(context.Context, es.Envelope, *counterv1.Incremented) error {
	return p.err
}
func (p *erroringProj) OnDecremented(context.Context, es.Envelope, *counterv1.Decremented) error {
	return p.err
}

// TestChain_RunsAllHandlers verifies that Chain dispatches each event
// to every handler and stops on the first error.
func TestChain_RunsAllHandlers(t *testing.T) {
	a, b := &counterProj{}, &counterProj{}
	chain := projection.Chain(
		counterv1.NewProjectionDispatcher(a, projection.IgnoreUnknown()),
		counterv1.NewProjectionDispatcher(b, projection.IgnoreUnknown()),
	)
	env := envWith(t, &counterv1.Initialized{}, "test.counter.v1.Initialized")
	if err := chain(context.Background(), env); err != nil {
		t.Fatalf("chain: %v", err)
	}
	if a.inits != 1 || b.inits != 1 {
		t.Errorf("chain delivery: a.inits=%d b.inits=%d", a.inits, b.inits)
	}
}

func TestChain_StopsOnFirstError(t *testing.T) {
	want := errors.New("first")
	called := 0
	first := func(context.Context, es.Envelope) error { called++; return want }
	second := func(context.Context, es.Envelope) error { called++; return nil }
	got := projection.Chain(first, second)(context.Background(), es.Envelope{})
	if !errors.Is(got, want) {
		t.Errorf("Chain return: got %v want %v", got, want)
	}
	if called != 1 {
		t.Errorf("subsequent handler ran after error: called=%d", called)
	}
}
