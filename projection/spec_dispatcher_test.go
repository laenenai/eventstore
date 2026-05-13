package projection_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/es"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
	projectionv1 "github.com/laenenai/eventstore/gen/test/projection/v1"
	"github.com/laenenai/eventstore/projection"
)

// counterTotals implements the codegen'd CounterTotalsHandler.
type counterTotals struct{ inits, incs int }

func (c *counterTotals) OnInitialized(_ context.Context, _ es.Envelope, _ *counterv1.Initialized) error {
	c.inits++
	return nil
}
func (c *counterTotals) OnIncremented(_ context.Context, _ es.Envelope, _ *counterv1.Incremented) error {
	c.incs++
	return nil
}

// TestSpecDispatcher_RoutesEachListedEvent verifies that the v2
// proto-driven dispatcher routes by TypeURL to the typed methods for
// only the events declared in the projection spec.
func TestSpecDispatcher_RoutesEachListedEvent(t *testing.T) {
	c := &counterTotals{}
	h := projectionv1.NewCounterTotalsDispatcher(c)

	initBytes, _ := proto.Marshal(&counterv1.Initialized{Min: 0, Max: 10, Value: 1})
	incBytes, _ := proto.Marshal(&counterv1.Incremented{By: 2})

	if err := h(context.Background(), es.Envelope{TypeURL: "test.counter.v1.Initialized", Payload: initBytes}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := h(context.Background(), es.Envelope{TypeURL: "test.counter.v1.Incremented", Payload: incBytes}); err != nil {
		t.Fatalf("inc: %v", err)
	}
	if c.inits != 1 || c.incs != 1 {
		t.Errorf("counts: got %+v want {1,1}", c)
	}
}

// TestSpecDispatcher_UnlistedEvent_DefaultErrors — spec only lists
// Initialized + Incremented; Decremented should produce an error.
func TestSpecDispatcher_UnlistedEvent_DefaultErrors(t *testing.T) {
	h := projectionv1.NewCounterTotalsDispatcher(&counterTotals{})
	err := h(context.Background(), es.Envelope{TypeURL: "test.counter.v1.Decremented"})
	if err == nil {
		t.Errorf("expected error for unlisted Decremented")
	}
}

// TestSpecDispatcher_UnlistedEvent_IgnoreUnknown — opt-in skip.
func TestSpecDispatcher_UnlistedEvent_IgnoreUnknown(t *testing.T) {
	h := projectionv1.NewCounterTotalsDispatcher(&counterTotals{}, projection.IgnoreUnknown())
	if err := h(context.Background(), es.Envelope{TypeURL: "test.counter.v1.Decremented"}); err != nil {
		t.Errorf("IgnoreUnknown: %v", err)
	}
}

// TestSpecDispatcher_NameConstant — verify the stable name constant.
func TestSpecDispatcher_NameConstant(t *testing.T) {
	if projectionv1.CounterTotalsName != "counter-totals" {
		t.Errorf("name constant: got %q want %q", projectionv1.CounterTotalsName, "counter-totals")
	}
}
