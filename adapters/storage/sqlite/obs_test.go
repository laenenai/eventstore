package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/es/obs"
)

// installObsHarness wires real OTel SDK providers + rebinds the
// package-level instruments so emitted samples land in the test
// reader. obs.init() resolved Tracer/Meter against the no-op
// providers; we swap them here and restore on cleanup. The trade is
// per-test isolation vs leaving the package singletons alone — these
// tests run sequentially so the singleton swap is safe.
func installObsHarness(t *testing.T) (*tracetest.InMemoryExporter, *sdkmetric.ManualReader) {
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

	obs.Tracer = tp.Tracer("eventstore")
	obs.Meter = mp.Meter("eventstore")
	mustReinstrument(t)

	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
		obs.Tracer = otel.Tracer("eventstore")
		obs.Meter = otel.Meter("eventstore")
		mustReinstrument(t)
	})
	return spans, reader
}

func mustReinstrument(t *testing.T) {
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

// TestSQLiteAppend_Observability appends one event via the sqlite
// adapter and verifies a "store.append" span is emitted along with a
// matching increment on the EventsAppendedTotal counter.
func TestSQLiteAppend_Observability(t *testing.T) {
	spans, reader := installObsHarness(t)

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	sid, _ := es.NewStreamID("t-obs", "obs", "1")
	_, err = a.Append(context.Background(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventToAppend{{
			EventID:       uuid.Must(uuid.NewV7()),
			TypeURL:       "obs.test/v1.E",
			SchemaVersion: 1,
			Payload:       []byte("p"),
		}},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Span shape
	got := spans.GetSpans()
	if len(got) != 1 {
		t.Fatalf("expected 1 span, got %d", len(got))
	}
	if got[0].Name != "store.append" {
		t.Errorf("span name: got %q want %q", got[0].Name, "store.append")
	}

	// Counter — 1 event appended
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect: %v", err)
	}
	var appended int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "eventstore.events.appended.total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("EventsAppendedTotal has unexpected data type %T", m.Data)
			}
			for _, dp := range sum.DataPoints {
				appended += dp.Value
			}
		}
	}
	if appended != 1 {
		t.Errorf("EventsAppendedTotal: got %d want 1", appended)
	}
}
