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
	"github.com/laenenai/eventstore/es"
	cwdbosex "github.com/laenenai/eventstore/examples/cmdworkflow-dbos"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

type fixture struct {
	env    *testsupport.Env
	read   *cwdbosex.ReadModel
	audit  *cwdbosex.AuditLog
	credit *cwdbosex.CreditReservation
	svc    *invoicev1dbos.DBOSService
}

// setupOpt mutates the Workflow before svc registration and Launch.
// Used by the saga test to register the optional CreditReservation
// subscriber without paying the wiring cost in the happy-path test.
type setupOpt func(wf *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event], fx *fixture)

// withCreditReservation enables the Sync+Compensate saga step. Kept
// behind an option so TestDBOSExample_FullLifecycle isn't paying for
// a subscriber it doesn't exercise.
func withCreditReservation() setupOpt {
	return func(wf *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event], fx *fixture) {
		fx.credit = cwdbosex.NewCreditReservation()
		wf.With(fx.credit.Subscriber())
	}
}

func setup(t *testing.T, opts ...setupOpt) *fixture {
	t.Helper()
	env := testsupport.Start(t)

	read := cwdbosex.NewReadModel()
	audit := cwdbosex.NewAuditLog()

	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
		aggregate.NewProto(env.Adapter, cwdbosex.Decider, invoicev1.EventCodec{}),
		env.Adapter, cwdbos.New(), invoicev1.EventCodec{},
	).
		WithDLQ(env.Adapter).
		With(read.Subscriber(), audit.Subscriber())

	fx := &fixture{env: env, read: read, audit: audit}
	for _, opt := range opts {
		opt(wf, fx)
	}

	svc := invoicev1dbos.NewDBOSService(wf)
	fx.svc = svc
	dbossdk.RegisterWorkflow(env.DCtx, svc.Create, dbossdk.WithWorkflowName("Invoice.Create"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.MarkPaid, dbossdk.WithWorkflowName("Invoice.MarkPaid"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.Void, dbossdk.WithWorkflowName("Invoice.Void"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.AsyncDispatch, dbossdk.WithWorkflowName("Invoice.AsyncDispatch"))

	if err := env.DCtx.Launch(); err != nil {
		t.Fatalf("DBOS Launch: %v", err)
	}

	return fx
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

// TestDBOSExample_SagaCompensation — Sync+Compensate saga step under
// DBOS. Mirrors the inproc TestExample_SagaCompensation
// (examples/cmdworkflow/example_test.go) so adopters can compare the
// two runtimes side by side and confirm subscriber code is unchanged.
//
// What the test asserts, in order:
//
//  1. Create primary command emits Created — appended to the stream.
//  2. The credit-reservation subscriber declines deterministically
//     (no retry-timing dependence).
//  3. The compensating Void command emits Voided — same bus, same
//     codec, same DBOS workflow runtime; runs under the same
//     DBOSContext as the original Create with step names prefixed
//     "compensate:credit-reservation:..." (see ADR 0025 § 7 +
//     adapters/cmdworkflow/dbos/dbos_test.go § TestDBOS_SyncCompensate).
//  4. Final state is STATUS_VOIDED — RunWorkflow returns the
//     post-compensation state, holding read-your-writes.
//  5. The active-invoices read model drops the voided invoice (Voided
//     is terminal; ReadModel.handle deletes the row).
//
// Why this lives in the examples module (not just the adapter): the
// adapter's TestDBOS_SyncCompensate is a unit test with an inline
// Decider and minimal subscriber. This test exercises the full
// production wiring — codegen-emitted DBOSService, registered
// workflows, the same subscribers an app would wire up.
func TestDBOSExample_SagaCompensation(t *testing.T) {
	fx := setup(t, withCreditReservation())

	// Deterministic decline — saga test must not depend on retry
	// randomness. MaxRetries=2 will exhaust on the first attempt
	// retried twice; OnExhausted=Compensate then fires Void.
	fx.credit.SetApproveAll(false)

	h, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.Create, &invoicev1.Create{
		TenantId:    "acme",
		InvoiceId:   "saga-1",
		CustomerId:  "carol",
		Currency:    "GBP",
		LineItems:   []*invoicev1.LineItem{{Sku: "Z", Quantity: 1, UnitCents: 5000}},
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("RunWorkflow Create: %v", err)
	}

	// Compensation is a Sync step — RunWorkflow's result reflects the
	// post-Void state.
	state, err := h.GetResult()
	if err != nil {
		t.Fatalf("Create result: %v", err)
	}
	if state.Status != invoicev1.Status_STATUS_VOIDED {
		t.Errorf("post-compensation status: got %v want VOIDED", state.Status)
	}

	// Audit trail: Created + Voided, in that order. Compensation is
	// not a rollback; it's a forward command producing a real event.
	sid, _ := es.NewStreamID("acme", "invoice", "saga-1")
	events, err := fx.env.Adapter.ReadStream(context.Background(), sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected Created + Voided, got %d events: %v", len(events), events)
	}
	if got, want := events[0].TypeURL, "myapp.invoice.v1.Created"; got != want {
		t.Errorf("event[0] TypeURL: got %s want %s", got, want)
	}
	if got, want := events[1].TypeURL, "myapp.invoice.v1.Voided"; got != want {
		t.Errorf("event[1] TypeURL: got %s want %s", got, want)
	}

	// Read model: Voided is terminal → row deleted.
	if _, ok := fx.read.Lookup("invoice:saga-1"); ok {
		t.Errorf("read model still has voided invoice")
	}
}
