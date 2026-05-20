//go:build dbos

package dbos_test

import (
	"context"
	"errors"
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

// Container-backed queue routing scenarios (build tag `dbos`).
// Standing up a real DBOS workflow journal is necessary to assert
// that async subscriber dispatch lands on the right queue — the
// workflow_status row carries the queue_name we route into.

// TestDBOS_AsyncQueueRouting — adopter declares two queues; async
// subscriber dispatch routed via cmdworkflow.WithQueue lands on the
// non-default queue. Verified by reading
// dbos.workflow_status.queue_name for the child AsyncDispatch row.
//
// Wiring order matters: NewWorkflowQueue requires a live DBOSContext,
// so queues must be created AFTER testsupport.Start but BEFORE Launch.
// The queue-aware Runtime is then assembled with the *Queue handles
// and passed to cmdworkflow.New.
func TestDBOS_AsyncQueueRouting(t *testing.T) {
	var delivered atomic.Int32

	env := testsupport.Start(t)
	highQ := dbossdk.NewWorkflowQueue(env.DCtx, "high")
	defaultQ := dbossdk.NewWorkflowQueue(env.DCtx, "default")

	rt := cwdbos.New(cwdbos.WithQueues(map[string]*dbossdk.WorkflowQueue{
		"high":    &highQ,
		"default": &defaultQ,
	}))
	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
		aggregate.NewProto(env.Adapter, invoiceDecider, invoicev1.EventCodec{}),
		env.Adapter, rt, invoicev1.EventCodec{},
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

	// DBOS forbids RegisterWorkflow after Launch. Define + register
	// the wrapper workflow alongside the other registrations so we
	// can call Launch ONCE. The closure captures `wf` (set above)
	// and `cmd` (set just below) — both live by the time the
	// wrapped workflow actually runs, after Launch returns.
	cmd := &invoicev1.Create{
		TenantId: "acme", InvoiceId: "queued-1",
		CustomerId: "alice", Currency: "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
		CreatedAtMs: time.Now().UnixMilli(),
	}
	// Wrap the DBOSContext as stdctx + attach the routing hint. The
	// codegen Service.Create reads queue from the stdctx via
	// cmdworkflow.QueueFromContext inside sendAsync's resolution.
	// Since RunWorkflow takes a DBOSContext directly, the queue hint
	// for the OUTER command isn't applied here — that's the adopter's
	// choice (dbos.WithQueue option). What we verify: the ASYNC
	// subscriber's AsyncDispatch child workflow lands on the "high"
	// queue because the workflow's context carried that hint into
	// sendAsync.
	//
	// The hint travels via the DBOSContext's stdlib values channel —
	// queue context value is propagated by Service.Create when it
	// builds stdCtx. Since codegen Service.Create builds stdCtx from
	// a fresh Background, we can't rely on the value flowing through
	// RunWorkflow's call site. Instead, run HandleCmd directly inside
	// a wrapper workflow whose stdctx we control end-to-end.
	wrappedWorkflow := func(dctx dbossdk.DBOSContext, _ struct{}) (struct{}, error) {
		stdCtx := cwdbos.WithContext(es.WithTenant(context.Background(), "acme"), dctx)
		stdCtx = cmdworkflow.WithQueue(stdCtx, "high")
		sid, _ := es.NewStreamID("acme", "invoice", "queued-1")
		_, err := wf.HandleCmd(stdCtx, sid, cmd)
		return struct{}{}, err
	}

	dbossdk.RegisterWorkflow(env.DCtx, svc.Create, dbossdk.WithWorkflowName("InvoiceQ.Create"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.AsyncDispatch, dbossdk.WithWorkflowName("InvoiceQ.AsyncDispatch"))
	dbossdk.RegisterWorkflow(env.DCtx, wrappedWorkflow, dbossdk.WithWorkflowName("WrappedCreateHigh"))
	if err := env.DCtx.Launch(); err != nil {
		t.Fatalf("DBOS Launch: %v", err)
	}

	h, err := dbossdk.RunWorkflow(env.DCtx, wrappedWorkflow, struct{}{})
	if err != nil {
		t.Fatalf("RunWorkflow wrapped: %v", err)
	}
	if _, err := h.GetResult(); err != nil {
		t.Fatalf("wrapped result: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && delivered.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if delivered.Load() != 1 {
		t.Fatalf("async delivered: got %d want 1", delivered.Load())
	}

	// Read back the child workflow's queue_name from dbos.workflow_status.
	sid, _ := es.NewStreamID("acme", "invoice", "queued-1")
	events, err := env.Adapter.ReadStream(context.Background(), sid, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("ReadStream: events=%d err=%v", len(events), err)
	}
	childWorkflowID := "invoice:audit-async:" + events[0].EventID.String()

	var queueName *string
	err = env.Pool.QueryRow(context.Background(),
		`SELECT queue_name FROM dbos.workflow_status WHERE workflow_uuid = $1`,
		childWorkflowID,
	).Scan(&queueName)
	if err != nil {
		t.Fatalf("workflow_status queue_name for %s: %v", childWorkflowID, err)
	}
	if queueName == nil {
		t.Errorf("queue_name NULL; expected 'high'")
	} else if *queueName != "high" {
		t.Errorf("queue_name = %q, want %q", *queueName, "high")
	}
}

// TestDBOS_AsyncUnknownQueueFallback — non-strict mode: an unknown
// queue name falls back to the declared "default" queue (not nil,
// since we declared it). One WARN log per unique unknown name.
func TestDBOS_AsyncUnknownQueueFallback(t *testing.T) {
	var delivered atomic.Int32
	env := testsupport.Start(t)
	defaultQ := dbossdk.NewWorkflowQueue(env.DCtx, "default")

	rt := cwdbos.New(cwdbos.WithQueues(map[string]*dbossdk.WorkflowQueue{
		"default": &defaultQ,
	}))
	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
		aggregate.NewProto(env.Adapter, invoiceDecider, invoicev1.EventCodec{}),
		env.Adapter, rt, invoicev1.EventCodec{},
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
	dbossdk.RegisterWorkflow(env.DCtx, svc.Create, dbossdk.WithWorkflowName("InvoiceQ2.Create"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.AsyncDispatch, dbossdk.WithWorkflowName("InvoiceQ2.AsyncDispatch"))

	wrappedWorkflow := func(dctx dbossdk.DBOSContext, _ struct{}) (struct{}, error) {
		stdCtx := cwdbos.WithContext(es.WithTenant(context.Background(), "acme"), dctx)
		stdCtx = cmdworkflow.WithQueue(stdCtx, "ghost")
		sid, _ := es.NewStreamID("acme", "invoice", "ghost-1")
		_, err := wf.HandleCmd(stdCtx, sid, &invoicev1.Create{
			TenantId: "acme", InvoiceId: "ghost-1",
			CustomerId: "alice", Currency: "USD",
			LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
			CreatedAtMs: time.Now().UnixMilli(),
		})
		return struct{}{}, err
	}
	dbossdk.RegisterWorkflow(env.DCtx, wrappedWorkflow, dbossdk.WithWorkflowName("WrappedCreateGhost"))
	if err := env.DCtx.Launch(); err != nil {
		t.Fatalf("DBOS Launch: %v", err)
	}

	h, err := dbossdk.RunWorkflow(env.DCtx, wrappedWorkflow, struct{}{})
	if err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	if _, err := h.GetResult(); err != nil {
		t.Fatalf("GetResult: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && delivered.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if delivered.Load() != 1 {
		t.Fatalf("async delivered: got %d want 1", delivered.Load())
	}

	// Assert the AsyncDispatch landed on "default" (the declared fallback).
	sid, _ := es.NewStreamID("acme", "invoice", "ghost-1")
	events, err := env.Adapter.ReadStream(context.Background(), sid, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("ReadStream: events=%d err=%v", len(events), err)
	}
	childWorkflowID := "invoice:audit-async:" + events[0].EventID.String()
	var queueName *string
	err = env.Pool.QueryRow(context.Background(),
		`SELECT queue_name FROM dbos.workflow_status WHERE workflow_uuid = $1`,
		childWorkflowID,
	).Scan(&queueName)
	if err != nil {
		t.Fatalf("workflow_status queue_name: %v", err)
	}
	if queueName == nil || *queueName != "default" {
		got := "<nil>"
		if queueName != nil {
			got = *queueName
		}
		t.Errorf("queue_name = %q, want %q (fallback)", got, "default")
	}
}

// TestDBOS_StrictModeUnknownQueueErrors — strict mode + unknown queue
// name surfaces an error through HandleCmd, rather than silently
// degrading. The cmd never appends events; the adopter sees the
// configuration mistake.
func TestDBOS_StrictModeUnknownQueueErrors(t *testing.T) {
	env := testsupport.Start(t)
	defaultQ := dbossdk.NewWorkflowQueue(env.DCtx, "default")

	rt := cwdbos.New(
		cwdbos.WithQueues(map[string]*dbossdk.WorkflowQueue{"default": &defaultQ}),
		cwdbos.WithStrictQueues(true),
	)
	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
		aggregate.NewProto(env.Adapter, invoiceDecider, invoicev1.EventCodec{}),
		env.Adapter, rt, invoicev1.EventCodec{},
	).WithDLQ(env.Adapter)
	// Async subscriber so sendAsync is invoked (where the strict-mode
	// check fires).
	wf.With(cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Name:        "audit-async",
		Mode:        cmdworkflow.Async,
		MaxRetries:  0,
		OnExhausted: cmdworkflow.Drop,
		Handle: func(context.Context, []es.Envelope, *invoicev1.Invoice, []invoicev1.Event) error {
			return nil
		},
	})
	svc := invoicev1dbos.NewDBOSService(wf)
	dbossdk.RegisterWorkflow(env.DCtx, svc.Create, dbossdk.WithWorkflowName("InvoiceQS.Create"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.AsyncDispatch, dbossdk.WithWorkflowName("InvoiceQS.AsyncDispatch"))

	wrappedWorkflow := func(dctx dbossdk.DBOSContext, _ struct{}) (struct{}, error) {
		stdCtx := cwdbos.WithContext(es.WithTenant(context.Background(), "acme"), dctx)
		stdCtx = cmdworkflow.WithQueue(stdCtx, "ghost")
		sid, _ := es.NewStreamID("acme", "invoice", "strict-1")
		_, err := wf.HandleCmd(stdCtx, sid, &invoicev1.Create{
			TenantId: "acme", InvoiceId: "strict-1",
			CustomerId: "alice", Currency: "USD",
			LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
			CreatedAtMs: time.Now().UnixMilli(),
		})
		return struct{}{}, err
	}
	dbossdk.RegisterWorkflow(env.DCtx, wrappedWorkflow, dbossdk.WithWorkflowName("WrappedCreateStrict"))
	if err := env.DCtx.Launch(); err != nil {
		t.Fatalf("DBOS Launch: %v", err)
	}

	h, err := dbossdk.RunWorkflow(env.DCtx, wrappedWorkflow, struct{}{})
	if err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	_, err = h.GetResult()
	if err == nil {
		t.Fatalf("expected strict-mode error, got nil")
	}
	if !errors.Is(err, cwdbos.ErrUnknownQueue) {
		// DBOS workflow-status surfaces a wrapped error; the
		// substring check is the resilient assertion.
		if !contains(err.Error(), "queue not declared") {
			t.Errorf("error = %v, want wrapped ErrUnknownQueue", err)
		}
	}
}

// contains is a substring helper — avoids pulling in strings just for
// one use inside this build-tagged file.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
