package cmdworkflow_test

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/adapters/cmdworkflow/inproc"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	exampleCw "github.com/laenenai/eventstore/examples/cmdworkflow"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// setup wires the eventstore + aggregate runtime + workflow runtime
// + the three example subscribers. Returns everything needed by each
// test scenario.
// fixture demonstrates the fluent shape made possible by
// aggregate.NewProto + Workflow.With(...). Compare with the
// pre-helpers version (10+ lines of struct-literal + Register calls).
type fixture struct {
	wf      *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]
	rt      *inproc.Runtime
	read    *exampleCw.ReadModel
	search  *exampleCw.SearchIndex
	credit  *exampleCw.CreditReservation
	adapter *sqliteadapter.Adapter
}

func setup(t *testing.T) *fixture {
	t.Helper()
	// `cache=shared` makes the in-memory database visible across
	// all connections in the pool. Without it, the goroutine fan-out
	// in HandleCmd can pull a fresh connection whose private DB
	// hasn't seen migrations.
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	read := exampleCw.NewReadModel()
	search := exampleCw.NewSearchIndex()
	credit := exampleCw.NewCreditReservation()

	rt := inproc.New()
	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
		aggregate.NewProto(a, exampleCw.Decider, invoicev1.EventCodec{}), a, rt, invoicev1.EventCodec{},
	).
		WithDLQ(a).
		With(read.Subscriber(), search.Subscriber(), credit.Subscriber())

	return &fixture{
		wf: wf, rt: rt,
		read: read, search: search, credit: credit,
		adapter: a,
	}
}

// TestExample_HappyPath — invoice creation flows through all three
// subscribers cleanly. Read model has the row, search index has the
// doc, credit reservation has reserved the amount.
func TestExample_HappyPath(t *testing.T) {
	fx := setup(t)
	tenant := "acme"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "invoice", "inv-001")

	state, err := fx.wf.HandleCmd(ctx, sid, &invoicev1.Create{
		InvoiceId: "inv-001", CustomerId: "alice", Currency: "USD",
		LineItems: []*invoicev1.LineItem{{Sku: "X", Quantity: 2, UnitCents: 500}},
		CreatedAtMs: 1700000000000,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if state.Status != invoicev1.Status_STATUS_OPEN {
		t.Errorf("status: got %v want OPEN", state.Status)
	}

	// Sync subscribers ran inline — read model and credit are set.
	if row, ok := fx.read.Lookup("invoice:inv-001"); !ok || row.TotalCents != 1000 {
		t.Errorf("readmodel: %+v ok=%v", row, ok)
	}
	if amt, ok := fx.credit.ReservedFor("invoice:inv-001"); !ok || amt != 1000 {
		t.Errorf("reservation: %d ok=%v", amt, ok)
	}

	// Async subscriber: wait for the search index to settle.
	fx.rt.Wait()
	if !fx.search.Has("invoice:inv-001") {
		t.Errorf("search index missing the doc")
	}

	// DLQ untouched.
	rows, _ := fx.adapter.ListSubscriberDLQ(context.Background(), "search-index-mirror", tenant, 10)
	if len(rows) != 0 {
		t.Errorf("DLQ unexpectedly non-empty: %+v", rows)
	}
}

// TestExample_AsyncDLQ — the search index is misbehaving (failing 5
// times in a row, beyond MaxRetries=3). Async subscriber exhausts,
// row lands in DLQ; the command itself still succeeds.
func TestExample_AsyncDLQ(t *testing.T) {
	fx := setup(t)
	tenant := "acme"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "invoice", "inv-002")

	fx.search.FailNext(10) // way past MaxRetries

	state, err := fx.wf.HandleCmd(ctx, sid, &invoicev1.Create{
		InvoiceId: "inv-002", CustomerId: "bob", Currency: "EUR",
		LineItems: []*invoicev1.LineItem{{Sku: "Y", Quantity: 1, UnitCents: 2000}},
		CreatedAtMs: 1700000001000,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Sync subscribers settled normally — Async failure doesn't
	// block the command.
	if state.Status != invoicev1.Status_STATUS_OPEN {
		t.Errorf("status: got %v want OPEN", state.Status)
	}

	// Wait for the async retry loop to exhaust.
	fx.rt.Wait()

	rows, _ := fx.adapter.ListSubscriberDLQ(context.Background(), "search-index-mirror", tenant, 10)
	if len(rows) != 1 {
		t.Fatalf("DLQ rows: got %d want 1", len(rows))
	}
	if rows[0].SubscriberName != "search-index-mirror" || rows[0].Attempts != 4 {
		t.Errorf("DLQ row: %+v", rows[0])
	}
}

// TestExample_SagaCompensation — credit reservation declines all
// invoices. Sync+Compensate exhausts → Void emitted → invoice ends
// in STATUS_VOIDED. The audit trail shows Created + Voided.
func TestExample_SagaCompensation(t *testing.T) {
	fx := setup(t)
	fx.credit.SetApproveAll(false) // every reservation fails → compensate

	tenant := "acme"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "invoice", "inv-003")

	state, err := fx.wf.HandleCmd(ctx, sid, &invoicev1.Create{
		InvoiceId: "inv-003", CustomerId: "carol", Currency: "GBP",
		LineItems: []*invoicev1.LineItem{{Sku: "Z", Quantity: 1, UnitCents: 5000}},
		CreatedAtMs: 1700000002000,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Compensation runs as a Sync step within HandleCmd. By the
	// time HandleCmd returns, the compensating Void has been
	// appended and the final state reload reflects it.
	if state.Status != invoicev1.Status_STATUS_VOIDED {
		t.Errorf("post-compensation status: got %v want VOIDED", state.Status)
	}

	finalState, err := fx.adapter.ReadStream(context.Background(), sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(finalState) != 2 {
		t.Fatalf("expected Created + Voided events, got %d: %v", len(finalState), finalState)
	}
	if finalState[1].TypeURL != "myapp.invoice.v1.Voided" {
		t.Errorf("compensating event: got %s want Voided", finalState[1].TypeURL)
	}

	// Read model dropped the invoice (Voided is terminal).
	if _, ok := fx.read.Lookup("invoice:inv-003"); ok {
		t.Errorf("read model still has voided invoice")
	}
}
