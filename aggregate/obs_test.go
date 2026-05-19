package aggregate_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/es/obs"
)

// obsHarness wires an in-memory span exporter + a manual metric reader
// against the global OTel providers, then restores the previous
// providers on cleanup. Tests use a single harness per t to keep
// assertions per-test isolated even though obs.Tracer / obs.Meter are
// package-level singletons.
type obsHarness struct {
	spans   *tracetest.InMemoryExporter
	tracerP *sdktrace.TracerProvider
	reader  *sdkmetric.ManualReader
	meterP  *sdkmetric.MeterProvider
}

func newObsHarness(t *testing.T) *obsHarness {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spans),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)

	// obs.Tracer / obs.Meter were resolved against the no-op
	// providers at init() time — swap them now so the call sites in
	// aggregate.Runtime.Handle reach the SDK providers we just
	// registered. Tests of the package-level singletons need this
	// indirection; production wires providers before first use, so
	// the swap is one-time.
	obs.Tracer = tp.Tracer("eventstore")
	obs.Meter = mp.Meter("eventstore")
	// Re-create instruments against the new meter so emitted samples
	// reach the manual reader.
	mustReinitInstruments(t)

	t.Cleanup(func() {
		// Restore providers and instruments. Other tests in the
		// process may rely on the no-op defaults.
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
		obs.Tracer = otel.Tracer("eventstore")
		obs.Meter = otel.Meter("eventstore")
		mustReinitInstruments(t)
	})
	return &obsHarness{spans: spans, tracerP: tp, reader: reader, meterP: mp}
}

// mustReinitInstruments recreates the package-level instruments
// against the current obs.Meter. obs.init() ran once at process boot
// against the no-op meter; tests need to rebind so emitted samples
// land in the reader they registered.
func mustReinitInstruments(t *testing.T) {
	t.Helper()
	var err error
	if obs.CommandsTotal, err = obs.Meter.Int64Counter("eventstore.commands.total"); err != nil {
		t.Fatalf("CommandsTotal: %v", err)
	}
	if obs.CommandDuration, err = obs.Meter.Float64Histogram("eventstore.command.duration"); err != nil {
		t.Fatalf("CommandDuration: %v", err)
	}
	if obs.EventsAppendedTotal, err = obs.Meter.Int64Counter("eventstore.events.appended.total"); err != nil {
		t.Fatalf("EventsAppendedTotal: %v", err)
	}
	if obs.StoreAppendDuration, err = obs.Meter.Float64Histogram("eventstore.store.append.duration"); err != nil {
		t.Fatalf("StoreAppendDuration: %v", err)
	}
	if obs.StoreReadStreamDuration, err = obs.Meter.Float64Histogram("eventstore.store.read_stream.duration"); err != nil {
		t.Fatalf("StoreReadStreamDuration: %v", err)
	}
}

func (h *obsHarness) collect(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect: %v", err)
	}
	return rm
}

// findCounter returns the sum of an int64 counter's data points
// matching the predicate, scanning the collected ResourceMetrics.
func findCounter(t *testing.T, rm metricdata.ResourceMetrics, name string, match func(metricdata.DataPoint[int64]) bool) int64 {
	t.Helper()
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s has unexpected data type %T", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if match == nil || match(dp) {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// hasOutcome returns true when the data point's attribute set contains
// eventstore.outcome=<want>. Used to split CommandsTotal by outcome.
func hasOutcome(dp metricdata.DataPoint[int64], want string) bool {
	v, ok := dp.Attributes.Value(obs.AttrOutcome)
	return ok && v.AsString() == want
}

// stringEvt is a no-op event used by the obs tests. Reusing the
// clock_test types would tangle test scopes; a separate codec keeps
// each test self-contained.
type obsEvt string
type obsCmd struct{ ok bool }

type obsCodec struct{}

func (obsCodec) Encode(e obsEvt) (aggregate.EncodedEvent, error) {
	return aggregate.EncodedEvent{Payload: []byte(e), TypeURL: "obs.test/v1.E", SchemaVersion: 1}, nil
}

func (obsCodec) Decode(_ string, _ uint32, p []byte) (obsEvt, error) {
	return obsEvt(p), nil
}

var errObsDecideFailed = errors.New("obs: decide failed")

func newObsRuntime(decideErr bool) *aggregate.Runtime[int, obsCmd, obsEvt] {
	return &aggregate.Runtime[int, obsCmd, obsEvt]{
		Store: newFakeStore(),
		Decider: es.Decider[int, obsCmd, obsEvt]{
			Initial: func() int { return 0 },
			Decide: func(_ int, c obsCmd) ([]obsEvt, []es.ConstraintOp, error) {
				if decideErr {
					return nil, nil, errObsDecideFailed
				}
				return []obsEvt{"ok"}, nil, nil
			},
			Evolve: func(s int, _ obsEvt) int { return s + 1 },
		},
		Codec: obsCodec{},
	}
}

// TestHandle_Observability_HappyPath asserts the success branch emits
// an aggregate.Handle span with no error status, plus a
// CommandsTotal{outcome=success} increment.
func TestHandle_Observability_HappyPath(t *testing.T) {
	h := newObsHarness(t)
	rt := newObsRuntime(false)
	sid, _ := es.NewStreamID("t-obs", "obs", "1")

	if _, err := rt.Handle(context.Background(), sid, obsCmd{ok: true}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Span
	spans := h.spans.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "aggregate.Handle" {
		t.Errorf("span name: got %q want %q", spans[0].Name, "aggregate.Handle")
	}
	if spans[0].Status.Code == codes.Error {
		t.Errorf("happy-path span should not have Error status, got %v", spans[0].Status)
	}

	// Metrics: CommandsTotal{outcome=success} = 1
	rm := h.collect(t)
	if got := findCounter(t, rm, "eventstore.commands.total", func(dp metricdata.DataPoint[int64]) bool {
		return hasOutcome(dp, obs.OutcomeSuccess)
	}); got != 1 {
		t.Errorf("CommandsTotal{success}: got %d want 1", got)
	}
	if got := findCounter(t, rm, "eventstore.commands.total", func(dp metricdata.DataPoint[int64]) bool {
		return hasOutcome(dp, obs.OutcomeError)
	}); got != 0 {
		t.Errorf("CommandsTotal{error}: got %d want 0 on happy path", got)
	}
}

// TestHandle_Observability_DecideError asserts the error branch emits
// CommandsTotal{outcome=error} and an error-status span.
func TestHandle_Observability_DecideError(t *testing.T) {
	h := newObsHarness(t)
	rt := newObsRuntime(true)
	sid, _ := es.NewStreamID("t-obs-err", "obs", "1")

	_, err := rt.Handle(context.Background(), sid, obsCmd{})
	if err == nil {
		t.Fatalf("expected error from decide, got nil")
	}
	if !errors.Is(err, errObsDecideFailed) {
		t.Fatalf("expected wrapped errObsDecideFailed, got %v", err)
	}

	spans := h.spans.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Status.Code != codes.Error {
		t.Errorf("error-path span status: got %v want Error", spans[0].Status.Code)
	}

	rm := h.collect(t)
	if got := findCounter(t, rm, "eventstore.commands.total", func(dp metricdata.DataPoint[int64]) bool {
		return hasOutcome(dp, obs.OutcomeError)
	}); got != 1 {
		t.Errorf("CommandsTotal{error}: got %d want 1", got)
	}
}
