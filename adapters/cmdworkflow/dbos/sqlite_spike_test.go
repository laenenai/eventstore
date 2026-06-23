//go:build dbos

package dbos_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"

	cwdbos "github.com/laenenai/eventstore/adapters/cmdworkflow/dbos"
	invoicev1dbos "github.com/laenenai/eventstore/adapters/cmdworkflow/dbos/gen/myapp/invoice/v1"
	"github.com/laenenai/eventstore/adapters/cmdworkflow/dbos/testsupport"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// TestDBOS_SQLiteSystemDB_BasicCreate is the validation spike for
// running the framework's DBOS adapter against a SQLite-backed DBOS
// context. ADR 0026 § 4 today says "SQLite eventstore + DBOS
// workflows is not a supported combination" because the DBOS Go SDK
// historically required Postgres for its workflow journal. v0.16.0
// landed the SqliteSystemDB Config hook; this test exists to prove
// the hook actually works end-to-end against the framework's
// adapter — DBOSContext.Launch, RegisterWorkflow, RunWorkflow,
// HandleCmd, the eventstore append, all on one SQLite file.
//
// If this test goes green, ADR 0026 § 4's caveat retracts and we
// can ship a recipe for "DBOS workflows on SQLite for local dev."
// If it fails, the caveat stands and we know precisely why.
//
// Build-tagged `dbos` to keep it out of the default unit-test run —
// not because it needs Docker (it deliberately doesn't), but to
// stay in the same opt-in surface as the other DBOS adapter tests.
func TestDBOS_SQLiteSystemDB_BasicCreate(t *testing.T) {
	env := testsupport.StartSQLite(t)

	rt := cwdbos.New()
	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
		aggregate.NewProto(env.Adapter, invoiceDecider, invoicev1.EventCodec{}),
		env.Adapter,
		rt,
		invoicev1.EventCodec{},
	).WithDLQ(env.Adapter)
	svc := invoicev1dbos.NewDBOSService(wf)

	// DBOS forbids RegisterWorkflow after Launch. Register the
	// codegen-emitted command + AsyncDispatch handlers before
	// Launch returns.
	dbossdk.RegisterWorkflow(env.DCtx, svc.Create, dbossdk.WithWorkflowName("InvoiceSqlite.Create"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.AsyncDispatch, dbossdk.WithWorkflowName("InvoiceSqlite.AsyncDispatch"))

	if err := env.DCtx.Launch(); err != nil {
		t.Fatalf("DBOS Launch (SqliteSystemDB): %v", err)
	}

	// Run a Create through the codegen DBOS service. End-to-end:
	// DBOS RunWorkflow -> service.Create handler -> framework's
	// HandleCmd -> aggregate.Runtime -> SQLite append + state_cache.
	cmd := &invoicev1.Create{
		TenantId:    "acme",
		InvoiceId:   "sqlite-spike-1",
		CustomerId:  "alice",
		Currency:    "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
		CreatedAtMs: time.Now().UnixMilli(),
	}
	h, err := dbossdk.RunWorkflow(env.DCtx, svc.Create, cmd)
	if err != nil {
		t.Fatalf("RunWorkflow Create: %v", err)
	}
	state, err := h.GetResult()
	if err != nil {
		t.Fatalf("GetResult Create: %v", err)
	}
	if state.GetInvoiceId() != "sqlite-spike-1" {
		t.Fatalf("post-decide state: got invoice_id=%q want %q",
			state.GetInvoiceId(), "sqlite-spike-1")
	}

	// Verify the event landed in the SQLite eventstore by reading
	// back through the framework's own adapter.
	sid, err := es.NewStreamID("acme", "invoice", "sqlite-spike-1")
	if err != nil {
		t.Fatalf("NewStreamID: %v", err)
	}
	envs, err := env.Adapter.ReadStream(context.Background(), sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("events: got %d want 1", len(envs))
	}
	if envs[0].TypeURL != "myapp.invoice.v1.Created" {
		t.Errorf("event TypeURL: got %q want %q",
			envs[0].TypeURL, "myapp.invoice.v1.Created")
	}
}

// TestDBOS_SQLiteSystemDB_AsyncSubscriber exercises the more
// demanding case: an Async subscriber that fires through DBOS's
// queue runner. If the Sync path is "DBOS RunWorkflow does its
// thing," the Async path is "DBOS schedules a child workflow,
// queue runner picks it up, child runs, subscriber fires."
// SQLite's single-writer semantics could trip the queue runner's
// poll loop in ways the Postgres path wouldn't. This test exists
// to surface that early.
func TestDBOS_SQLiteSystemDB_AsyncSubscriber(t *testing.T) {
	var delivered atomic.Int32

	env := testsupport.StartSQLite(t)

	rt := cwdbos.New()
	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
		aggregate.NewProto(env.Adapter, invoiceDecider, invoicev1.EventCodec{}),
		env.Adapter,
		rt,
		invoicev1.EventCodec{},
	).WithDLQ(env.Adapter)
	wf.With(cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Name:        "audit-async",
		Mode:        cmdworkflow.Async,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.Drop,
		Handle: func(_ context.Context, _ []es.Envelope, _ *invoicev1.Invoice, _ []invoicev1.Event) error {
			delivered.Add(1)
			return nil
		},
	})
	svc := invoicev1dbos.NewDBOSService(wf)

	dbossdk.RegisterWorkflow(env.DCtx, svc.Create, dbossdk.WithWorkflowName("InvoiceSqliteAsync.Create"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.AsyncDispatch, dbossdk.WithWorkflowName("InvoiceSqliteAsync.AsyncDispatch"))

	if err := env.DCtx.Launch(); err != nil {
		t.Fatalf("DBOS Launch (SqliteSystemDB): %v", err)
	}

	h, err := dbossdk.RunWorkflow(env.DCtx, svc.Create, &invoicev1.Create{
		TenantId:    "acme",
		InvoiceId:   "sqlite-async-1",
		CustomerId:  "alice",
		Currency:    "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	if _, err := h.GetResult(); err != nil {
		t.Fatalf("GetResult: %v", err)
	}

	if !waitForAsyncDelivery(&delivered, 1, asyncDeliveryTimeout()) {
		t.Fatalf("async delivered on SQLite: got %d want 1 (after %s) — the queue runner doesn't appear to be servicing SqliteSystemDB",
			delivered.Load(), asyncDeliveryTimeout())
	}
}
