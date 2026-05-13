package invoice_test

import (
	"context"
	"database/sql"
	"errors"

	"google.golang.org/protobuf/encoding/protojson"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/invoice"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// End-to-end example exercising the Invoice aggregate against a real
// SQLite store with state_cache enabled. Mirrors the operator flow:
//
//   1. Create an invoice (state_cache row appears in the same tx).
//   2. List by status via state_cache.
//   3. MarkPaid — terminal transition, next command fails with
//      ErrTerminal.

func newRuntime(t *testing.T) (es.Store, *aggregate.Runtime[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]) {
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
		Decider:    invoice.Decider,
		Codec:      invoicev1.EventCodec{},
		StateCodec: aggregate.ProtoStateCodec[*invoicev1.Invoice]{},
	}
	return a, rt
}

func mustStream(t *testing.T, tenant, id string) es.StreamID {
	t.Helper()
	sid, err := es.ParseCanonical(tenant, "invoice:"+id)
	if err != nil {
		t.Fatalf("ParseCanonical: %v", err)
	}
	return sid
}

func TestInvoice_FullLifecycle(t *testing.T) {
	store, rt := newRuntime(t)
	tenant := "t-acme"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := mustStream(t, tenant, "inv-001")
	now := time.Now().UnixMilli()

	// 1. Create the invoice.
	_, err := rt.Handle(ctx, sid, &invoicev1.Create{
		InvoiceId:  "inv-001",
		CustomerId: "cust-42",
		Currency:   "USD",
		LineItems: []*invoicev1.LineItem{
			{Sku: "WIDGET-1", Name: "Widget", Quantity: 2, UnitCents: 999},
			{Sku: "GADGET-1", Name: "Gadget", Quantity: 1, UnitCents: 4999},
		},
		CreatedAtMs: now,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// 2. state_cache row is present and reflects OPEN status (in-tx
	//    with the events — read-your-writes is guaranteed).
	cache := store.(es.StateCacheReader)
	row, err := cache.GetState(ctx, tenant, sid.Canonical())
	if err != nil {
		t.Fatalf("GetState after Create: %v", err)
	}
	live := &invoicev1.Invoice{}
	if err := protojson.Unmarshal(row.State, live); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}
	// protojson uses camelCase numerals as strings; check the typed field.
	want := int64(2*999 + 4999)
	if live.TotalCents != want {
		t.Errorf("total: got %d want %d", live.TotalCents, want)
	}

	// 3. List by type — useful for "show me all invoices" UIs.
	all, err := cache.ListStates(ctx, tenant, "myapp.invoice.v1.Invoice", "", 100)
	if err != nil {
		t.Fatalf("ListStates: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("ListStates size: got %d want 1", len(all))
	}

	// 4. MarkPaid → terminal transition.
	if _, err := rt.Handle(ctx, sid, &invoicev1.MarkPaid{
		PaymentRef: "stripe_pi_abc",
		PaidAtMs:   now + 1000,
	}); err != nil {
		t.Fatalf("MarkPaid: %v", err)
	}

	// 5. Further commands fail with ErrTerminal.
	_, err = rt.Handle(ctx, sid, &invoicev1.Void{
		Reason: "should be rejected", VoidedAtMs: now + 2000,
	})
	if !errors.Is(err, es.ErrTerminal) {
		t.Errorf("post-pay Void: got %v want ErrTerminal", err)
	}

	// 6. state_cache reflects the new status.
	row, _ = cache.GetState(ctx, tenant, sid.Canonical())
	_ = protojson.Unmarshal(row.State, live)
	if live.Status != invoicev1.Status_STATUS_PAID {
		t.Errorf("final status: got %v want PAID", live.Status)
	}
	if !row.Terminal {
		t.Errorf("state_cache.terminal: got false, want true")
	}
}

// TestInvoice_VoidPath verifies the alternative terminal transition.
func TestInvoice_VoidPath(t *testing.T) {
	_, rt := newRuntime(t)
	tenant := "t-void"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := mustStream(t, tenant, "inv-002")
	now := time.Now().UnixMilli()

	if _, err := rt.Handle(ctx, sid, &invoicev1.Create{
		InvoiceId: "inv-002", CustomerId: "cust-99", Currency: "EUR",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
		CreatedAtMs: now,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, err := rt.Handle(ctx, sid, &invoicev1.Void{
		Reason: "duplicate", VoidedAtMs: now + 100,
	}); err != nil {
		t.Fatalf("Void: %v", err)
	}

	// MarkPaid after Void must be rejected.
	_, err := rt.Handle(ctx, sid, &invoicev1.MarkPaid{PaymentRef: "x", PaidAtMs: now + 200})
	if !errors.Is(err, es.ErrTerminal) {
		t.Errorf("MarkPaid post-Void: got %v want ErrTerminal", err)
	}
}

// TestInvoice_BusinessRules tests the input validation.
func TestInvoice_BusinessRules(t *testing.T) {
	_, rt := newRuntime(t)
	tenant := "t-rules"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := mustStream(t, tenant, "inv-bad")
	now := time.Now().UnixMilli()

	// Empty line items.
	_, err := rt.Handle(ctx, sid, &invoicev1.Create{
		InvoiceId: "inv-bad", CustomerId: "cust", Currency: "USD",
		CreatedAtMs: now,
	})
	if !errors.Is(err, invoice.ErrEmptyLineItems) {
		t.Errorf("got %v want ErrEmptyLineItems", err)
	}

	// Empty currency.
	_, err = rt.Handle(ctx, sid, &invoicev1.Create{
		InvoiceId: "inv-bad", CustomerId: "cust",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 1}},
		CreatedAtMs: now,
	})
	if !errors.Is(err, invoice.ErrEmptyCurrency) {
		t.Errorf("got %v want ErrEmptyCurrency", err)
	}

	// MarkPaid on a non-existent invoice.
	_, err = rt.Handle(ctx, mustStream(t, tenant, "ghost"), &invoicev1.MarkPaid{
		PaymentRef: "x", PaidAtMs: now,
	})
	if !errors.Is(err, invoice.ErrNotCreated) {
		t.Errorf("got %v want ErrNotCreated", err)
	}
}
