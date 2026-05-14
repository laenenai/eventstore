package cwrestate_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	restatesdk "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/ingress"

	cwrestateadapter "github.com/laenenai/eventstore/adapters/cmdworkflow/restate"
	invoicev1restate "github.com/laenenai/eventstore/adapters/cmdworkflow/restate/gen/myapp/invoice/v1"
	"github.com/laenenai/eventstore/adapters/cmdworkflow/restate/testsupport"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	cwrestate "github.com/laenenai/eventstore/examples/cmdworkflow-restate"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// Setup wires everything: SQLite eventstore + cmdworkflow.Workflow
// with Sync (read-model) and Async (audit log) subscribers +
// generated RestateService + Restate testcontainer.
type fixture struct {
	env   *testsupport.Env
	read  *cwrestate.ReadModel
	audit *cwrestate.AuditLog
}

func setup(t *testing.T) *fixture {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	read := cwrestate.NewReadModel()
	audit := cwrestate.NewAuditLog()

	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command](
		aggregate.NewProto(a, cwrestate.Decider, invoicev1.EventCodec{}),
		a, cwrestateadapter.New(),
	).
		WithDLQ(a).
		With(read.Subscriber(), audit.Subscriber())

	svc := invoicev1restate.NewRestateService(wf)
	env := testsupport.Start(t, restatesdk.Reflect(svc))

	return &fixture{env: env, read: read, audit: audit}
}

// TestRestateExample_FullLifecycle: create → mark-paid via Restate
// ingress; verify the read model and audit log both saw the events,
// final state reflects the lifecycle.
func TestRestateExample_FullLifecycle(t *testing.T) {
	fx := setup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create — Sync read-model UPSERTs the row inline.
	createSvc := ingress.Service[*invoicev1.Create, *invoicev1.Invoice](
		fx.env.Ingress(), "Invoice", "Create")
	created, err := createSvc.Request(ctx, &invoicev1.Create{
		TenantId:    "acme",
		InvoiceId:   "inv-001",
		CustomerId:  "alice",
		Currency:    "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 2, UnitCents: 500}},
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Status != invoicev1.Status_STATUS_OPEN {
		t.Errorf("status after Create: got %v want OPEN", created.Status)
	}

	// Read-your-writes: Sync subscriber settled before HandleCmd
	// returned, so the read model already has the row.
	row, ok := fx.read.Lookup("invoice:inv-001")
	if !ok || row.TotalCents != 1000 {
		t.Errorf("read model: row=%+v ok=%v", row, ok)
	}

	// MarkPaid — Sync subscriber removes the row (terminal).
	paidSvc := ingress.Service[*invoicev1.MarkPaid, *invoicev1.Invoice](
		fx.env.Ingress(), "Invoice", "MarkPaid")
	paid, err := paidSvc.Request(ctx, &invoicev1.MarkPaid{
		TenantId:   "acme",
		InvoiceId:  "inv-001",
		PaymentRef: "stripe_x",
		PaidAtMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("MarkPaid: %v", err)
	}
	if paid.Status != invoicev1.Status_STATUS_PAID {
		t.Errorf("status after MarkPaid: got %v want PAID", paid.Status)
	}
	if _, stillThere := fx.read.Lookup("invoice:inv-001"); stillThere {
		t.Errorf("read model still has paid invoice")
	}

	// Async audit log eventually receives both events. Bounded poll.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && fx.audit.Calls() < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	if fx.audit.Calls() != 2 {
		t.Errorf("audit log calls: got %d want 2", fx.audit.Calls())
	}
}
