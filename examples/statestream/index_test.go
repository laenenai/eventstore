package statestream_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/statestream"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
	"github.com/laenenai/eventstore/state_stream"
)

// invoiceDecider — minimal inline Decider; the canonical full version
// lives in examples/invoice.
var invoiceDecider = es.Decider[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
	Initial: func() *invoicev1.Invoice { return &invoicev1.Invoice{} },
	Decide: func(s *invoicev1.Invoice, c invoicev1.Command) ([]invoicev1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *invoicev1.Create:
			if s.InvoiceId != "" {
				return nil, nil, errors.New("invoice: already created")
			}
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
		case *invoicev1.MarkPaid:
			if s.InvoiceId == "" {
				return nil, nil, errors.New("invoice: not created")
			}
			return []invoicev1.Event{
				&invoicev1.Paid{PaymentRef: cmd.PaymentRef, PaidAtMs: cmd.PaidAtMs},
			}, nil, nil
		case *invoicev1.Void:
			if s.InvoiceId == "" {
				return nil, nil, errors.New("invoice: not created")
			}
			return []invoicev1.Event{
				&invoicev1.Voided{Reason: cmd.Reason, VoidedAtMs: cmd.VoidedAtMs},
			}, nil, nil
		}
		return nil, nil, errors.New("invoice: unknown command")
	},
	Evolve: func(s *invoicev1.Invoice, e invoicev1.Event) *invoicev1.Invoice {
		out := *s
		switch evt := e.(type) {
		case *invoicev1.Created:
			out.InvoiceId = evt.InvoiceId
			out.CustomerId = evt.CustomerId
			out.Currency = evt.Currency
			out.TotalCents = evt.TotalCents
			out.LineItems = evt.LineItems
			out.Status = invoicev1.Status_STATUS_OPEN
		case *invoicev1.Paid:
			out.Status = invoicev1.Status_STATUS_PAID
		case *invoicev1.Voided:
			out.Status = invoicev1.Status_STATUS_VOIDED
		}
		return &out
	},
	IsTerminal: func(s *invoicev1.Invoice) bool {
		return s.Status == invoicev1.Status_STATUS_PAID || s.Status == invoicev1.Status_STATUS_VOIDED
	},
}

func setup(t *testing.T) (es.Store, *aggregate.Runtime[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]) {
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
	rt := &aggregate.Runtime[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Store:      a,
		Decider:    invoiceDecider,
		Codec:      invoicev1.EventCodec{},
		StateCodec: aggregate.ProtoStateCodec[*invoicev1.Invoice]{},
	}
	return a, rt
}

func mustStream(t *testing.T, tenant, canonical string) es.StreamID {
	t.Helper()
	sid, err := es.ParseCanonical(tenant, canonical)
	if err != nil {
		t.Fatalf("ParseCanonical: %v", err)
	}
	return sid
}

// TestStateStream_ColdStartBackfill: a subscriber added after invoices
// already exist gets a delivery for every existing invoice on first
// Run.
func TestStateStream_ColdStartBackfill(t *testing.T) {
	store, rt := setup(t)
	tenant := "t-cold"
	ctx := es.WithTenant(context.Background(), tenant)
	now := time.Now().UnixMilli()

	// Create three invoices BEFORE any state_stream subscriber runs.
	for _, id := range []string{"inv-001", "inv-002", "inv-003"} {
		if _, err := rt.Handle(ctx, mustStream(t, tenant, "invoice:"+id),
			&invoicev1.Create{
				InvoiceId: id, CustomerId: "alice", Currency: "USD",
				LineItems: []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
				CreatedAtMs: now,
			}); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
	}

	// Wire the search-index subscriber and run the drain.
	index := statestream.NewIndex()
	drain := &state_stream.Drain{
		SubscriberName: "invoice-search-index",
		Tenant:         tenant,
		Store:          store,
		Publisher:      index,
	}
	delivered, err := drain.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if delivered != 3 {
		t.Errorf("cold delivered: got %d want 3", delivered)
	}

	// The index has all three invoices, queryable by customer.
	if got := index.FindByCustomer("alice"); len(got) != 3 {
		t.Errorf("by-customer index: got %d want 3", len(got))
	}
}

// TestStateStream_DeltaDelivery: after the initial backfill, subsequent
// Appends produce only the delta deliveries.
func TestStateStream_DeltaDelivery(t *testing.T) {
	store, rt := setup(t)
	tenant := "t-delta"
	ctx := es.WithTenant(context.Background(), tenant)
	now := time.Now().UnixMilli()

	index := statestream.NewIndex()
	drain := &state_stream.Drain{
		SubscriberName: "invoice-delta",
		Tenant:         tenant,
		Store:          store,
		Publisher:      index,
	}

	if _, err := rt.Handle(ctx, mustStream(t, tenant, "invoice:i-1"),
		&invoicev1.Create{
			InvoiceId: "i-1", CustomerId: "bob", Currency: "EUR",
			LineItems: []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 200}},
			CreatedAtMs: now,
		}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// First Run: delivers the new invoice.
	if n, _ := drain.Run(context.Background()); n != 1 {
		t.Errorf("first run delivered: got %d want 1", n)
	}
	row, ok := index.Lookup("invoice:i-1")
	if !ok || row.Status != invoicev1.Status_STATUS_OPEN {
		t.Errorf("first delivery: row=%+v ok=%v want open", row, ok)
	}

	// Second Run with no change: zero deliveries.
	if n, _ := drain.Run(context.Background()); n != 0 {
		t.Errorf("idle run: got %d want 0", n)
	}

	// MarkPaid → state changes → new delivery.
	if _, err := rt.Handle(ctx, mustStream(t, tenant, "invoice:i-1"),
		&invoicev1.MarkPaid{PaymentRef: "stripe_x", PaidAtMs: now + 100}); err != nil {
		t.Fatalf("MarkPaid: %v", err)
	}
	if n, _ := drain.Run(context.Background()); n != 1 {
		t.Errorf("paid run delivered: got %d want 1", n)
	}
	row, _ = index.Lookup("invoice:i-1")
	if row.Status != invoicev1.Status_STATUS_PAID {
		t.Errorf("paid status: got %v want PAID", row.Status)
	}
	if !row.Terminal {
		t.Errorf("paid row should be Terminal")
	}
}

// TestStateStream_IdempotentReceiver: the subscriber's PublishState
// dedupes by version. Re-running Drain after a Reset doesn't break
// the index — every row stays at its current version.
func TestStateStream_IdempotentReceiver(t *testing.T) {
	store, rt := setup(t)
	tenant := "t-idemp"
	ctx := es.WithTenant(context.Background(), tenant)
	now := time.Now().UnixMilli()

	// Seed + mark-paid + void → invoice goes through full lifecycle.
	if _, err := rt.Handle(ctx, mustStream(t, tenant, "invoice:i-1"),
		&invoicev1.Create{
			InvoiceId: "i-1", CustomerId: "carol", Currency: "USD",
			LineItems: []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
			CreatedAtMs: now,
		}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := rt.Handle(ctx, mustStream(t, tenant, "invoice:i-1"),
		&invoicev1.MarkPaid{PaymentRef: "x", PaidAtMs: now + 1}); err != nil {
		t.Fatalf("MarkPaid: %v", err)
	}

	index := statestream.NewIndex()
	drain := &state_stream.Drain{
		SubscriberName: "invoice-idempotent",
		Tenant:         tenant,
		Store:          store,
		Publisher:      index,
	}

	if _, err := drain.Run(context.Background()); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	finalRow, _ := index.Lookup("invoice:i-1")

	// Reset and re-Run → subscriber receives "current state" again.
	// The index's idempotent-by-version logic accepts the re-delivery
	// (incoming.Version is not less-than the stored one, depending on
	// implementation policy — for this example, equal-version is a no-op).
	admin := store.(es.StateStreamAdmin)
	if _, err := admin.ResetStateStreamSubscriber(context.Background(), "invoice-idempotent", tenant); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if _, err := drain.Run(context.Background()); err != nil {
		t.Fatalf("second Run: %v", err)
	}

	// Row should still reflect the same final state.
	row, ok := index.Lookup("invoice:i-1")
	if !ok {
		t.Fatalf("row missing after replay")
	}
	if row.Status != finalRow.Status || row.Version != finalRow.Version {
		t.Errorf("replay drift: got %+v want %+v", row, finalRow)
	}
	if index.Len() != 1 {
		t.Errorf("index size after replay: got %d want 1 (idempotent on stream_id)", index.Len())
	}
}
