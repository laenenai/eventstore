package cwdbosex_test

import (
	"context"
	"testing"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"

	cwdbos "github.com/laenenai/eventstore/adapters/cmdworkflow/dbos"
	invoicev1dbos "github.com/laenenai/eventstore/adapters/cmdworkflow/dbos/gen/myapp/invoice/v1"
	"github.com/laenenai/eventstore/adapters/cmdworkflow/dbos/testsupport"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	cwdbosex "github.com/laenenai/eventstore/examples/cmdworkflow-dbos"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

type fixture struct {
	env   *testsupport.Env
	read  *cwdbosex.ReadModel
	audit *cwdbosex.AuditLog
	svc   *invoicev1dbos.DBOSService
}

func setup(t *testing.T) *fixture {
	t.Helper()
	env := testsupport.Start(t)

	read := cwdbosex.NewReadModel()
	audit := cwdbosex.NewAuditLog()

	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command](
		aggregate.NewProto(env.Adapter, cwdbosex.Decider, invoicev1.EventCodec{}),
		env.Adapter, cwdbos.New(),
	).
		WithDLQ(env.Adapter).
		With(read.Subscriber(), audit.Subscriber())

	svc := invoicev1dbos.NewDBOSService(wf)
	dbossdk.RegisterWorkflow(env.DCtx, svc.Create, dbossdk.WithWorkflowName("Invoice.Create"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.MarkPaid, dbossdk.WithWorkflowName("Invoice.MarkPaid"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.Void, dbossdk.WithWorkflowName("Invoice.Void"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.AsyncDispatch, dbossdk.WithWorkflowName("Invoice.AsyncDispatch"))

	if err := env.DCtx.Launch(); err != nil {
		t.Fatalf("DBOS Launch: %v", err)
	}

	return &fixture{env: env, read: read, audit: audit, svc: svc}
}

// TestDBOSExample_FullLifecycle: create → mark-paid via the
// generated DBOSService; verify Sync (read model) settles inline,
// Async (audit) catches up in the background.
func TestDBOSExample_FullLifecycle(t *testing.T) {
	fx := setup(t)

	// Create — Sync read-model UPSERTs the row inline.
	h, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.Create, &invoicev1.Create{
		TenantId:    "acme",
		InvoiceId:   "inv-001",
		CustomerId:  "alice",
		Currency:    "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 2, UnitCents: 500}},
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("RunWorkflow Create: %v", err)
	}
	created, err := h.GetResult()
	if err != nil {
		t.Fatalf("Create result: %v", err)
	}
	if created.Status != invoicev1.Status_STATUS_OPEN {
		t.Errorf("status after Create: %v", created.Status)
	}

	// Read-your-writes — Sync subscriber settled before RunWorkflow
	// returned the result.
	row, ok := fx.read.Lookup("invoice:inv-001")
	if !ok || row.TotalCents != 1000 {
		t.Errorf("read model: row=%+v ok=%v", row, ok)
	}

	// MarkPaid — terminal; row drops from active view.
	h2, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.MarkPaid, &invoicev1.MarkPaid{
		TenantId:   "acme",
		InvoiceId:  "inv-001",
		PaymentRef: "stripe_x",
		PaidAtMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("RunWorkflow MarkPaid: %v", err)
	}
	paid, err := h2.GetResult()
	if err != nil {
		t.Fatalf("MarkPaid result: %v", err)
	}
	if paid.Status != invoicev1.Status_STATUS_PAID {
		t.Errorf("status after MarkPaid: %v", paid.Status)
	}
	if _, stillThere := fx.read.Lookup("invoice:inv-001"); stillThere {
		t.Errorf("read model still has paid invoice")
	}

	// Async audit log catches up.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && fx.audit.Calls() < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	if fx.audit.Calls() != 2 {
		t.Errorf("audit log calls: got %d want 2", fx.audit.Calls())
	}

	// Cancel context (just to satisfy unused vars).
	_ = context.Background
}
