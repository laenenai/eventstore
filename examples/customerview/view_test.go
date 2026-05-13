package customerview_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/customerview"
	customerviewv1 "github.com/laenenai/eventstore/gen/myapp/customerview/v1"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
	orderv1 "github.com/laenenai/eventstore/gen/myapp/order/v1"
	"github.com/laenenai/eventstore/projection"
)

// Bottom-line: this test produces events from two aggregates (Order
// + Invoice), runs the codegen'd spec-driven projection dispatcher
// against them, and verifies the denormalized customer-view row
// reflects everything.

func setup(t *testing.T) (es.Store,
	*aggregate.Runtime[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event],
	*aggregate.Runtime[*orderv1.Order, orderv1.Command, orderv1.Event],
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
	invRT := &aggregate.Runtime[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Store:   a,
		Decider: invoiceDecider(),
		Codec:   invoicev1.EventCodec{},
	}
	orderRT := &aggregate.Runtime[*orderv1.Order, orderv1.Command, orderv1.Event]{
		Store:   a,
		Decider: orderDecider(),
		Codec:   orderv1.EventCodec{},
	}
	return a, invRT, orderRT
}

func mustStream(t *testing.T, tenant, canonical string) es.StreamID {
	t.Helper()
	sid, err := es.ParseCanonical(tenant, canonical)
	if err != nil {
		t.Fatalf("ParseCanonical: %v", err)
	}
	return sid
}

func TestCustomerView_AggregatesEventsAcrossAggregates(t *testing.T) {
	store, invRT, orderRT := setup(t)
	tenant := "t-acme"
	ctx := es.WithTenant(context.Background(), tenant)
	now := time.Now().UnixMilli()

	// Customer "alice" places an order AND has an invoice.
	if _, err := orderRT.Handle(ctx, mustStream(t, tenant, "order:o-1"),
		&orderv1.PlaceOrder{
			OrderId: "o-1", CustomerId: "alice", Warehouse: "wh-a",
			PlacedAtMs: now,
		}); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if _, err := orderRT.Handle(ctx, mustStream(t, tenant, "order:o-1"),
		&orderv1.Ship{Carrier: "UPS", Tracking: "1Z1", ShippedAtMs: now + 10}); err != nil {
		t.Fatalf("Ship: %v", err)
	}
	if _, err := invRT.Handle(ctx, mustStream(t, tenant, "invoice:inv-a"),
		&invoicev1.Create{
			InvoiceId: "inv-a", CustomerId: "alice", Currency: "USD",
			LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 5000}},
			CreatedAtMs: now,
		}); err != nil {
		t.Fatalf("Create invoice: %v", err)
	}

	// Customer "bob" places a different order.
	if _, err := orderRT.Handle(ctx, mustStream(t, tenant, "order:o-2"),
		&orderv1.PlaceOrder{
			OrderId: "o-2", CustomerId: "bob", Warehouse: "wh-b",
			PlacedAtMs: now,
		}); err != nil {
		t.Fatalf("PlaceOrder bob: %v", err)
	}

	// Run the spec-driven projection.
	view := customerview.NewView()
	handler := customerviewv1.NewCustomerViewDispatcher(view, projection.IgnoreUnknown())
	rt := &projection.Runtime{
		Name:       "customer-view",
		Tenant:     tenant,
		Store:      store,
		Checkpoint: store.(projection.Checkpoint),
		Handler:    handler,
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// Alice has 1 open invoice (5000c) + 1 shipped order.
	alice, ok := view.Get("alice")
	if !ok {
		t.Fatalf("no row for alice")
	}
	if alice.OpenInvoiceCount != 1 {
		t.Errorf("alice OpenInvoiceCount: got %d want 1", alice.OpenInvoiceCount)
	}
	if alice.TotalInvoiceCents != 5000 {
		t.Errorf("alice TotalInvoiceCents: got %d want 5000", alice.TotalInvoiceCents)
	}
	if alice.ShippedOrderCount != 1 || alice.OpenOrderCount != 0 {
		t.Errorf("alice orders: open=%d shipped=%d, want 0/1",
			alice.OpenOrderCount, alice.ShippedOrderCount)
	}

	// Bob has 1 open order, no invoices.
	bob, ok := view.Get("bob")
	if !ok {
		t.Fatalf("no row for bob")
	}
	if bob.OpenOrderCount != 1 {
		t.Errorf("bob OpenOrderCount: got %d want 1", bob.OpenOrderCount)
	}
	if bob.OpenInvoiceCount != 0 {
		t.Errorf("bob OpenInvoiceCount: got %d want 0", bob.OpenInvoiceCount)
	}

	// All() returns both rows.
	all := view.All()
	if len(all) != 2 {
		t.Errorf("All() rows: got %d want 2", len(all))
	}
}

func TestCustomerView_ReplayProducesSameState(t *testing.T) {
	store, invRT, orderRT := setup(t)
	tenant := "t-replay"
	ctx := es.WithTenant(context.Background(), tenant)
	now := time.Now().UnixMilli()

	if _, err := orderRT.Handle(ctx, mustStream(t, tenant, "order:o-1"),
		&orderv1.PlaceOrder{OrderId: "o-1", CustomerId: "alice", PlacedAtMs: now}); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if _, err := invRT.Handle(ctx, mustStream(t, tenant, "invoice:i-1"),
		&invoicev1.Create{
			InvoiceId: "i-1", CustomerId: "alice", Currency: "USD",
			LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
			CreatedAtMs: now,
		}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// First run.
	first := customerview.NewView()
	rt := &projection.Runtime{
		Name: "customer-view-replay", Tenant: tenant, Store: store,
		Checkpoint: projection.NewMemoryCheckpoint(),
		Handler:    customerviewv1.NewCustomerViewDispatcher(first, projection.IgnoreUnknown()),
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Second projection instance (fresh state). Same events should
	// produce the same view.
	second := customerview.NewView()
	rt2 := &projection.Runtime{
		Name: "customer-view-replay-2", Tenant: tenant, Store: store,
		Checkpoint: projection.NewMemoryCheckpoint(),
		Handler:    customerviewv1.NewCustomerViewDispatcher(second, projection.IgnoreUnknown()),
	}
	if _, err := rt2.RunOnce(context.Background()); err != nil {
		t.Fatalf("second run: %v", err)
	}

	a1, _ := first.Get("alice")
	a2, _ := second.Get("alice")
	if a1.OpenInvoiceCount != a2.OpenInvoiceCount {
		t.Errorf("inv count drift: first=%d second=%d",
			a1.OpenInvoiceCount, a2.OpenInvoiceCount)
	}
	if a1.OpenOrderCount != a2.OpenOrderCount {
		t.Errorf("order count drift: first=%d second=%d",
			a1.OpenOrderCount, a2.OpenOrderCount)
	}
	if a1.TotalInvoiceCents != a2.TotalInvoiceCents {
		t.Errorf("invoice cents drift: first=%d second=%d",
			a1.TotalInvoiceCents, a2.TotalInvoiceCents)
	}
}

// ---- Minimal deciders inlined to keep this example self-contained. ----

func invoiceDecider() es.Decider[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event] {
	return es.Decider[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Initial: func() *invoicev1.Invoice { return &invoicev1.Invoice{} },
		Decide: func(s *invoicev1.Invoice, c invoicev1.Command) ([]invoicev1.Event, []es.ConstraintOp, error) {
			cmd := c.(*invoicev1.Create)
			var total int64
			for _, li := range cmd.LineItems {
				total += li.Quantity * li.UnitCents
			}
			return []invoicev1.Event{
				&invoicev1.Created{
					InvoiceId: cmd.InvoiceId, CustomerId: cmd.CustomerId,
					Currency: cmd.Currency, TotalCents: total,
					LineItems: cmd.LineItems, CreatedAtMs: cmd.CreatedAtMs,
				},
			}, nil, nil
		},
		Evolve: func(s *invoicev1.Invoice, e invoicev1.Event) *invoicev1.Invoice {
			created := e.(*invoicev1.Created)
			return &invoicev1.Invoice{
				InvoiceId: created.InvoiceId, CustomerId: created.CustomerId,
				Currency: created.Currency, TotalCents: created.TotalCents,
				Status: invoicev1.Status_STATUS_OPEN,
			}
		},
	}
}

func orderDecider() es.Decider[*orderv1.Order, orderv1.Command, orderv1.Event] {
	return es.Decider[*orderv1.Order, orderv1.Command, orderv1.Event]{
		Initial: func() *orderv1.Order { return &orderv1.Order{} },
		Decide: func(s *orderv1.Order, c orderv1.Command) ([]orderv1.Event, []es.ConstraintOp, error) {
			switch cmd := c.(type) {
			case *orderv1.PlaceOrder:
				return []orderv1.Event{
					&orderv1.OrderPlaced{
						OrderId: cmd.OrderId, CustomerId: cmd.CustomerId,
						Warehouse: cmd.Warehouse, PlacedAtMs: cmd.PlacedAtMs,
					},
				}, nil, nil
			case *orderv1.Ship:
				return []orderv1.Event{
					&orderv1.OrderShipped{
						OrderId: s.OrderId, Warehouse: s.Warehouse,
						Carrier: cmd.Carrier, Tracking: cmd.Tracking,
						ShippedAtMs: cmd.ShippedAtMs,
					},
				}, nil, nil
			}
			return nil, nil, nil
		},
		Evolve: func(s *orderv1.Order, e orderv1.Event) *orderv1.Order {
			switch evt := e.(type) {
			case *orderv1.OrderPlaced:
				return &orderv1.Order{
					OrderId: evt.OrderId, CustomerId: evt.CustomerId,
					Warehouse: evt.Warehouse, Status: orderv1.Status_STATUS_PLACED,
				}
			case *orderv1.OrderShipped:
				out := *s
				out.Status = orderv1.Status_STATUS_SHIPPED
				out.Carrier = evt.Carrier
				out.Tracking = evt.Tracking
				return &out
			}
			return s
		},
	}
}
