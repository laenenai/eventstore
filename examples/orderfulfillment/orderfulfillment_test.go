package orderfulfillment_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/orderfulfillment"
	fulfillmentv1 "github.com/laenenai/eventstore/gen/myapp/fulfillment/v1"
	orderv1 "github.com/laenenai/eventstore/gen/myapp/order/v1"
	"github.com/laenenai/eventstore/linked"
	"github.com/laenenai/eventstore/projection"
)

// End-to-end: Order is placed and shipped, the linked projection
// auto-creates a Fulfillment task, the warehouse service picks and
// ships it. Verifies the linked-projection idempotency contract by
// replaying the source events.

func newSetup(t *testing.T) (es.Store,
	*aggregate.Runtime[*orderv1.Order, orderv1.Command, orderv1.Event],
	*aggregate.Runtime[*fulfillmentv1.FulfillmentTask, fulfillmentv1.Command, fulfillmentv1.Event],
) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	orderRT := &aggregate.Runtime[*orderv1.Order, orderv1.Command, orderv1.Event]{
		Store: a, Decider: orderfulfillment.OrderDecider, Codec: orderv1.EventCodec{},
	}
	fulRT := &aggregate.Runtime[*fulfillmentv1.FulfillmentTask, fulfillmentv1.Command, fulfillmentv1.Event]{
		Store: a, Decider: orderfulfillment.FulfillmentDecider, Codec: fulfillmentv1.EventCodec{},
	}
	return a, orderRT, fulRT
}

func TestOrderToFulfillment_HappyPath(t *testing.T) {
	store, orderRT, fulRT := newSetup(t)
	tenant := "t-acme"
	ctx := es.WithTenant(context.Background(), tenant)
	now := time.Now().UnixMilli()

	// 1. Place + Ship the order.
	orderSID := mustStream(t, tenant, "order:o-001")
	if _, err := orderRT.Handle(ctx, orderSID, &orderv1.PlaceOrder{
		OrderId:    "o-001",
		CustomerId: "cust-42",
		Warehouse:  "wh-west",
		PlacedAtMs: now,
	}); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if _, err := orderRT.Handle(ctx, orderSID, &orderv1.Ship{
		Carrier: "UPS", Tracking: "1Z999", ShippedAtMs: now + 1000,
	}); err != nil {
		t.Fatalf("Ship: %v", err)
	}

	// 2. Wire the linked projection.
	lp, err := linked.New(linked.Config{
		Name:           "order-to-fulfillment",
		Destination:    store,
		SourceTypeURLs: []string{"myapp.order.v1.OrderShipped"},

		Route: func(ctx context.Context, env es.Envelope) (linked.Route, error) {
			// Decode the source event by re-using the EventCodec.
			shipped, err := decodeOrderShipped(env)
			if err != nil {
				return linked.Route{}, err
			}
			taskID := "task-" + shipped.OrderId
			destStream, err := es.ParseCanonical(env.TenantID, "fulfillment_task:"+taskID)
			if err != nil {
				return linked.Route{}, err
			}
			return linked.Route{
				DestinationStream: destStream,
				DerivedEvent: &fulfillmentv1.Created{
					TaskId:      taskID,
					OrderId:     shipped.OrderId,
					Warehouse:   shipped.Warehouse,
					CreatedAtMs: shipped.ShippedAtMs,
				},
				DerivedTypeURL:  "myapp.fulfillment.v1.Created",
				ExpectedVersion: 0,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("linked.New: %v", err)
	}

	// 3. Run the projection once to dispatch the OrderShipped event.
	rt := &projection.Runtime{
		Name:       "linked-order-fulfillment",
		Tenant:     tenant,
		Store:      store,
		Checkpoint: store.(projection.Checkpoint),
		Handler:    lp.Handler(),
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// 4. The fulfillment task exists; warehouse picks and ships it.
	taskSID := mustStream(t, tenant, "fulfillment_task:task-o-001")
	if _, err := fulRT.Handle(ctx, taskSID, &fulfillmentv1.Pick{
		Picker: "alice", PickedAtMs: now + 2000,
	}); err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if _, err := fulRT.Handle(ctx, taskSID, &fulfillmentv1.MarkShipped{
		ShippedAtMs: now + 3000,
	}); err != nil {
		t.Fatalf("MarkShipped: %v", err)
	}

	// 5. Verify final state by re-loading.
	state, _, err := fulRT.Load(context.Background(), taskSID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Status != fulfillmentv1.Status_STATUS_SHIPPED {
		t.Errorf("task status: got %v want SHIPPED", state.Status)
	}
	if state.Picker != "alice" {
		t.Errorf("picker: got %q want alice", state.Picker)
	}

	// 6. Re-run the projection — idempotent emit should swallow the
	//    replayed OrderShipped without producing a duplicate Created.
	if err := store.(es.ProjectionAdmin).Reset(context.Background(), "linked-order-fulfillment", tenant); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce replay: %v", err)
	}
	// Stream length must still be 3 (Created + Picked + Shipped) — no extra Created.
	envs, _ := store.ReadStream(context.Background(), taskSID, 0)
	if len(envs) != 3 {
		t.Errorf("task stream length after replay: got %d want 3", len(envs))
	}
}

// decodeOrderShipped is a small shim. In production the Route
// function would receive the typed event via the v2 spec-driven
// codegen (recipe 12 mentions it).
func decodeOrderShipped(env es.Envelope) (*orderv1.OrderShipped, error) {
	out, err := orderv1.EventCodec{}.Decode(env.TypeURL, env.SchemaVersion, env.Payload)
	if err != nil {
		return nil, err
	}
	return out.(*orderv1.OrderShipped), nil
}

func mustStream(t *testing.T, tenant, canonical string) es.StreamID {
	t.Helper()
	sid, err := es.ParseCanonical(tenant, canonical)
	if err != nil {
		t.Fatalf("ParseCanonical: %v", err)
	}
	return sid
}
