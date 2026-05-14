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

// Build-tagged `dbos` — pulls a real Postgres container via
// testcontainers + uses the DBOS Go SDK against it. Skip the default
// `go test ./...` run; opt in:
//
//	go test -tags dbos ./adapters/cmdworkflow/dbos/...

// invoiceDecider — minimal inline Invoice Decider for the DBOS smoke
// test. Same as the Restate adapter's invoiceDecider, duplicated to
// keep the test self-contained.
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
		out := &invoicev1.Invoice{
			InvoiceId: s.InvoiceId, CustomerId: s.CustomerId,
			Currency: s.Currency, TotalCents: s.TotalCents,
			Status: s.Status, LineItems: s.LineItems,
		}
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
		return out
	},
	IsTerminal: func(s *invoicev1.Invoice) bool {
		return s.Status == invoicev1.Status_STATUS_PAID || s.Status == invoicev1.Status_STATUS_VOIDED
	},
}

// Hand-written DBOS workflow function for Create. Phase 2c codegen
// will emit this pattern per command type. The signature
// `func(ctx DBOSContext, input P) (R, error)` is what
// dbos.RegisterWorkflow expects.
//
// makeCreate is a higher-order factory that captures the Workflow
// in closure so the resulting fn has the DBOS-compatible signature.


// fixture uses the codegen-emitted DBOSService to register Invoice
// command handlers — the same shape that production apps wire up.
type fixture struct {
	env *testsupport.Env
	wf  *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command]
	svc *invoicev1dbos.DBOSService
}

type fixtureOpt func(wf *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command])

func newFixture(t *testing.T, opts ...fixtureOpt) *fixture {
	t.Helper()
	env := testsupport.Start(t)

	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command](
		aggregate.NewProto(env.Adapter, invoiceDecider, invoicev1.EventCodec{}),
		env.Adapter, cwdbos.New(),
	).WithDLQ(env.Adapter)

	for _, opt := range opts {
		opt(wf)
	}

	svc := invoicev1dbos.NewDBOSService(wf)
	dbossdk.RegisterWorkflow(env.DCtx, svc.Create, dbossdk.WithWorkflowName("Invoice.Create"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.MarkPaid, dbossdk.WithWorkflowName("Invoice.MarkPaid"))
	dbossdk.RegisterWorkflow(env.DCtx, svc.Void, dbossdk.WithWorkflowName("Invoice.Void"))

	if err := env.DCtx.Launch(); err != nil {
		t.Fatalf("DBOS Launch: %v", err)
	}

	return &fixture{env: env, wf: wf, svc: svc}
}

// TestDBOS_Smoke — full lifecycle (Create → MarkPaid) via DBOS
// workflows. Verifies the bridging + the codegen-style handler
// shape work end-to-end against a real Postgres.
func TestDBOS_Smoke(t *testing.T) {
	fx := newFixture(t)


	handle, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.Create, &invoicev1.Create{
		TenantId:    "acme",
		InvoiceId:   "smoke-1",
		CustomerId:  "alice",
		Currency:    "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 2, UnitCents: 500}},
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("RunWorkflow Create: %v", err)
	}
	created, err := handle.GetResult()
	if err != nil {
		t.Fatalf("Create result: %v", err)
	}
	if created.Status != invoicev1.Status_STATUS_OPEN || created.TotalCents != 1000 {
		t.Errorf("after Create: %+v", created)
	}

	handle2, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.MarkPaid, &invoicev1.MarkPaid{
		TenantId:   "acme",
		InvoiceId:  "smoke-1",
		PaymentRef: "stripe_x",
		PaidAtMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("RunWorkflow MarkPaid: %v", err)
	}
	paid, err := handle2.GetResult()
	if err != nil {
		t.Fatalf("MarkPaid result: %v", err)
	}
	if paid.Status != invoicev1.Status_STATUS_PAID {
		t.Errorf("after MarkPaid: %+v", paid)
	}
}

// TestDBOS_SyncRetryThenSuccess — Sync subscriber fails twice then
// succeeds. The retry loop runs inside one dbos.Go future fn; DBOS
// journals exactly one step regardless of attempt count.
func TestDBOS_SyncRetryThenSuccess(t *testing.T) {
	var attempts atomic.Int32
	fx := newFixture(t, func(wf *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command]) {
		wf.With(cmdworkflow.Subscriber[invoicev1.Command]{
			Name:        "flaky-sub",
			Mode:        cmdworkflow.Sync,
			MaxRetries:  5,
			OnExhausted: cmdworkflow.DLQ,
			Handle: func(_ context.Context, _ es.Envelope) error {
				if attempts.Add(1) < 3 {
					return errors.New("transient")
				}
				return nil
			},
		})
	})


	handle, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.Create, &invoicev1.Create{
		TenantId: "acme", InvoiceId: "retry-1",
		CustomerId: "x", Currency: "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	state, err := handle.GetResult()
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if state.Status != invoicev1.Status_STATUS_OPEN {
		t.Errorf("status: %v", state.Status)
	}
	if attempts.Load() != 3 {
		t.Errorf("attempts: %d want 3", attempts.Load())
	}

	// One Created event in the eventstore.
	sid, _ := es.NewStreamID("acme", "invoice", "retry-1")
	events, err := fx.env.Adapter.ReadStream(context.Background(), sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("events: %d want 1", len(events))
	}
}

// TestDBOS_IdempotencyKey — two RunWorkflow calls with the same
// WithWorkflowID return the same result; DBOS dedupes at the
// workflow runtime layer. Only one Created event in the eventstore.
func TestDBOS_IdempotencyKey(t *testing.T) {
	fx := newFixture(t)


	cmd := &invoicev1.Create{
		TenantId: "acme", InvoiceId: "idemp-1",
		CustomerId: "alice", Currency: "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
		CreatedAtMs: time.Now().UnixMilli(),
	}
	const idemKey = "client-request-abc"

	h1, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.Create, cmd, dbossdk.WithWorkflowID(idemKey))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	state1, err := h1.GetResult()
	if err != nil {
		t.Fatalf("first result: %v", err)
	}

	h2, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.Create, cmd, dbossdk.WithWorkflowID(idemKey))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	state2, err := h2.GetResult()
	if err != nil {
		t.Fatalf("second result: %v", err)
	}
	if state1.Status != state2.Status || state1.TotalCents != state2.TotalCents {
		t.Errorf("results differ: %+v vs %+v", state1, state2)
	}

	sid, _ := es.NewStreamID("acme", "invoice", "idemp-1")
	events, _ := fx.env.Adapter.ReadStream(context.Background(), sid, 0)
	if len(events) != 1 {
		t.Errorf("events: %d want 1 (DBOS workflow-id dedup)", len(events))
	}
}

// TestDBOS_AsyncSubscriberDelivered — Async subscriber Spawn'd as a
// goroutine. HandleCmd returns immediately; subscriber settles in
// the background. Phase 2a limitation: not journaled by DBOS.
func TestDBOS_AsyncSubscriberDelivered(t *testing.T) {
	var delivered atomic.Int32
	fx := newFixture(t, func(wf *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command]) {
		wf.With(cmdworkflow.Subscriber[invoicev1.Command]{
			Name:        "async-sub",
			Mode:        cmdworkflow.Async,
			MaxRetries:  3,
			OnExhausted: cmdworkflow.Drop,
			Handle: func(_ context.Context, _ es.Envelope) error {
				delivered.Add(1)
				return nil
			},
		})
	})


	h, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.Create, &invoicev1.Create{
		TenantId: "acme", InvoiceId: "async-1",
		CustomerId: "alice", Currency: "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	if _, err := h.GetResult(); err != nil {
		t.Fatalf("GetResult: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && delivered.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if delivered.Load() != 1 {
		t.Errorf("async delivered: got %d want 1", delivered.Load())
	}
}

// TestDBOS_SyncCompensate — Sync+Compensate saga step. The framework
// applies the policy from the outer HandleCmd context (real
// DBOSContext in scope) and prefixes the compensating recursion's
// step names with "compensate:<sub>:<event>:" — matches the Restate
// fix from task #71.
func TestDBOS_SyncCompensate(t *testing.T) {
	fx := newFixture(t, func(wf *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command]) {
		wf.With(cmdworkflow.Subscriber[invoicev1.Command]{
			Name:        "credit-check",
			Filter:      cmdworkflow.EventFilter{TypeURLs: []string{"myapp.invoice.v1.Created"}},
			Mode:        cmdworkflow.Sync,
			MaxRetries:  1,
			OnExhausted: cmdworkflow.Compensate,
			Handle: func(_ context.Context, _ es.Envelope) error {
				return errors.New("credit declined")
			},
			Compensate: func(_ context.Context, env es.Envelope) (invoicev1.Command, error) {
				return &invoicev1.Void{
					TenantId:   env.TenantID,
					InvoiceId:  env.StreamID.ID,
					Reason:     "credit reservation declined",
					VoidedAtMs: time.Now().UnixMilli(),
				}, nil
			},
		})
	})


	h, err := dbossdk.RunWorkflow(fx.env.DCtx, fx.svc.Create, &invoicev1.Create{
		TenantId: "acme", InvoiceId: "comp-1",
		CustomerId: "alice", Currency: "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 1, UnitCents: 100}},
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	state, err := h.GetResult()
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if state.Status != invoicev1.Status_STATUS_VOIDED {
		t.Errorf("post-compensation status: %v want VOIDED", state.Status)
	}
}
