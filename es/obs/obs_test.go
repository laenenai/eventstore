package obs_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/laenenai/eventstore/es/obs"
)

// TestPackageInit asserts package init succeeded without a registered
// provider — the OTel no-op meter/tracer satisfy instrument creation,
// so adopters who never call otel.SetMeterProvider must not panic at
// package import time.
func TestPackageInit(t *testing.T) {
	if obs.Tracer == nil {
		t.Fatalf("obs.Tracer should not be nil after init")
	}
	if obs.Meter == nil {
		t.Fatalf("obs.Meter should not be nil after init")
	}
	if obs.CommandsTotal == nil ||
		obs.CommandDuration == nil ||
		obs.EventsAppendedTotal == nil ||
		obs.StoreAppendDuration == nil ||
		obs.StoreReadStreamDuration == nil {
		t.Fatalf("obs package instruments should be initialised by init()")
	}
}

// TestNoopHotPath records measurements + opens a span without a
// registered provider — proving zero-config does not panic on the
// hot path.
func TestNoopHotPath(t *testing.T) {
	ctx := context.Background()
	ctx, span := obs.Start(ctx, "test.span", obs.Tenant("t1"))
	defer span.End()

	obs.CommandsTotal.Add(ctx, 1, otelmetric.WithAttributes(obs.Outcome(obs.OutcomeSuccess)))
	obs.CommandDuration.Record(ctx, 0.001)
	obs.EventsAppendedTotal.Add(ctx, 3)
	obs.StoreAppendDuration.Record(ctx, 0.002)
	obs.StoreReadStreamDuration.Record(ctx, 0.0005)
}

// TestEndWithErr_NilNoOp verifies that calling EndWithErr with nil is
// idempotent and safe — the runtime calls it on the success path too.
func TestEndWithErr_NilNoOp(t *testing.T) {
	_, span := obs.Start(context.Background(), "noerr")
	obs.EndWithErr(span, nil)
	span.End()
}

// TestAttrHelpers checks each helper returns the documented key + value.
func TestAttrHelpers(t *testing.T) {
	cases := []struct {
		name string
		got  attribute.KeyValue
		key  string
		want attribute.Value
	}{
		{"Tenant", obs.Tenant("tnt-1"), obs.AttrTenant, attribute.StringValue("tnt-1")},
		{"StreamID", obs.StreamID("tnt-1/counter:42"), obs.AttrStreamID, attribute.StringValue("tnt-1/counter:42")},
		{"Aggregate", obs.Aggregate("counter"), obs.AttrAggregate, attribute.StringValue("counter")},
		{"Command", obs.Command("*counter.Init"), obs.AttrCommand, attribute.StringValue("*counter.Init")},
		{"EventCount", obs.EventCount(3), obs.AttrEventCount, attribute.IntValue(3)},
		{"Version", obs.Version(7), obs.AttrVersion, attribute.Int64Value(7)},
		{"TypeURL", obs.TypeURL("type.googleapis.com/x.Y"), obs.AttrTypeURL, attribute.StringValue("type.googleapis.com/x.Y")},
		{"Outcome", obs.Outcome(obs.OutcomeError), obs.AttrOutcome, attribute.StringValue("error")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if string(tc.got.Key) != tc.key {
				t.Errorf("key: got %q want %q", tc.got.Key, tc.key)
			}
			if tc.got.Value != tc.want {
				t.Errorf("value: got %v want %v", tc.got.Value, tc.want)
			}
		})
	}
}

// TestDBSystem checks the semconv-backed db.system attribute. We
// don't pin the literal key here (semconv exports it as a constant)
// but we do require it be "db.system" — that's the stable OTel name.
func TestDBSystem(t *testing.T) {
	kv := obs.DBSystem("sqlite")
	if string(kv.Key) != "db.system" {
		t.Errorf("DBSystem key: got %q want %q", kv.Key, "db.system")
	}
	if kv.Value.AsString() != "sqlite" {
		t.Errorf("DBSystem value: got %q want %q", kv.Value.AsString(), "sqlite")
	}
}
