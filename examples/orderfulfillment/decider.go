// Package orderfulfillment is a worked example of the framework's
// Linked Projections (Tier 3.5, ADR 0022). An Order aggregate
// produces OrderShipped events; a LinkedProjection auto-creates a
// FulfillmentTask stream per shipped order; the warehouse service
// then issues Pick / MarkShipped commands against that task.
//
// Two aggregates, two Deciders, one linked projection wiring them.
package orderfulfillment

import (
	"errors"

	"github.com/laenenai/eventstore/es"
	fulfillmentv1 "github.com/laenenai/eventstore/gen/myapp/fulfillment/v1"
	orderv1 "github.com/laenenai/eventstore/gen/myapp/order/v1"
)

// ---- Order domain errors -----------------------------------------------

var (
	ErrOrderAlreadyPlaced = errors.New("order: already placed")
	ErrOrderNotPlaced     = errors.New("order: not yet placed")
	ErrOrderAlreadyShipped = errors.New("order: already shipped")
	ErrOrderNotShipped    = errors.New("order: not yet shipped")
	ErrOrderUnknownCmd    = errors.New("order: unknown command")
)

// OrderDecider drives the source aggregate.
var OrderDecider = es.Decider[*orderv1.Order, orderv1.Command, orderv1.Event]{
	Initial: func() *orderv1.Order { return &orderv1.Order{} },

	Decide: func(s *orderv1.Order, c orderv1.Command) ([]orderv1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *orderv1.PlaceOrder:
			if s.OrderId != "" {
				return nil, nil, ErrOrderAlreadyPlaced
			}
			return []orderv1.Event{
				&orderv1.OrderPlaced{
					OrderId:    cmd.OrderId,
					CustomerId: cmd.CustomerId,
					Warehouse:  cmd.Warehouse,
					PlacedAtMs: cmd.PlacedAtMs,
				},
			}, nil, nil

		case *orderv1.Ship:
			if s.OrderId == "" {
				return nil, nil, ErrOrderNotPlaced
			}
			if s.Status != orderv1.Status_STATUS_PLACED {
				return nil, nil, ErrOrderAlreadyShipped
			}
			return []orderv1.Event{
				&orderv1.OrderShipped{
					OrderId:     s.OrderId,
					Warehouse:   s.Warehouse,
					Carrier:     cmd.Carrier,
					Tracking:    cmd.Tracking,
					ShippedAtMs: cmd.ShippedAtMs,
				},
			}, nil, nil

		case *orderv1.Complete:
			if s.Status != orderv1.Status_STATUS_SHIPPED {
				return nil, nil, ErrOrderNotShipped
			}
			return []orderv1.Event{
				&orderv1.OrderCompleted{CompletedAtMs: cmd.CompletedAtMs},
			}, nil, nil
		}
		return nil, nil, ErrOrderUnknownCmd
	},

	Evolve: func(s *orderv1.Order, e orderv1.Event) *orderv1.Order {
		out := cloneOrder(s)
		switch evt := e.(type) {
		case *orderv1.OrderPlaced:
			out.OrderId = evt.OrderId
			out.CustomerId = evt.CustomerId
			out.Warehouse = evt.Warehouse
			out.PlacedAtMs = evt.PlacedAtMs
			out.Status = orderv1.Status_STATUS_PLACED
		case *orderv1.OrderShipped:
			out.Status = orderv1.Status_STATUS_SHIPPED
			out.Carrier = evt.Carrier
			out.Tracking = evt.Tracking
			out.ShippedAtMs = evt.ShippedAtMs
		case *orderv1.OrderCompleted:
			out.Status = orderv1.Status_STATUS_COMPLETED
		}
		return out
	},

	IsTerminal: func(s *orderv1.Order) bool {
		return s.Status == orderv1.Status_STATUS_COMPLETED
	},
}

// ---- Fulfillment domain errors -----------------------------------------

var (
	ErrFulfillmentNotCreated = errors.New("fulfillment: task not yet created")
	ErrFulfillmentNotPicked  = errors.New("fulfillment: task not yet picked")
	ErrFulfillmentAlreadyPicked = errors.New("fulfillment: already picked")
	ErrFulfillmentUnknownCmd = errors.New("fulfillment: unknown command")
)

// FulfillmentDecider drives the destination aggregate. The linked
// projection produces the initial Created event; warehouse staff
// then issues Pick → MarkShipped to advance the task.
//
// Note: there's no explicit `CreateTask` command in this Decider's
// Decide switch. Created events arrive as the FIRST event in the
// stream (no command produced them — they came from the linked
// projection). Evolve still folds them into state normally. If the
// warehouse service tries Pick on a non-existent task, ErrFulfillmentNotCreated
// surfaces.
var FulfillmentDecider = es.Decider[*fulfillmentv1.FulfillmentTask, fulfillmentv1.Command, fulfillmentv1.Event]{
	Initial: func() *fulfillmentv1.FulfillmentTask { return &fulfillmentv1.FulfillmentTask{} },

	Decide: func(s *fulfillmentv1.FulfillmentTask, c fulfillmentv1.Command) ([]fulfillmentv1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *fulfillmentv1.Pick:
			if s.TaskId == "" {
				return nil, nil, ErrFulfillmentNotCreated
			}
			if s.Status != fulfillmentv1.Status_STATUS_PENDING {
				return nil, nil, ErrFulfillmentAlreadyPicked
			}
			return []fulfillmentv1.Event{
				&fulfillmentv1.Picked{Picker: cmd.Picker, PickedAtMs: cmd.PickedAtMs},
			}, nil, nil

		case *fulfillmentv1.MarkShipped:
			if s.Status != fulfillmentv1.Status_STATUS_PICKED {
				return nil, nil, ErrFulfillmentNotPicked
			}
			return []fulfillmentv1.Event{
				&fulfillmentv1.Shipped{ShippedAtMs: cmd.ShippedAtMs},
			}, nil, nil
		}
		return nil, nil, ErrFulfillmentUnknownCmd
	},

	Evolve: func(s *fulfillmentv1.FulfillmentTask, e fulfillmentv1.Event) *fulfillmentv1.FulfillmentTask {
		out := cloneTask(s)
		switch evt := e.(type) {
		case *fulfillmentv1.Created:
			out.TaskId = evt.TaskId
			out.OrderId = evt.OrderId
			out.Warehouse = evt.Warehouse
			out.CreatedAtMs = evt.CreatedAtMs
			out.Status = fulfillmentv1.Status_STATUS_PENDING
		case *fulfillmentv1.Picked:
			out.Status = fulfillmentv1.Status_STATUS_PICKED
			out.Picker = evt.Picker
			out.PickedAtMs = evt.PickedAtMs
		case *fulfillmentv1.Shipped:
			out.Status = fulfillmentv1.Status_STATUS_SHIPPED
			out.ShippedAtMs = evt.ShippedAtMs
		}
		return out
	},

	IsTerminal: func(s *fulfillmentv1.FulfillmentTask) bool {
		return s.Status == fulfillmentv1.Status_STATUS_SHIPPED
	},
}

func cloneOrder(s *orderv1.Order) *orderv1.Order {
	if s == nil {
		return &orderv1.Order{}
	}
	return &orderv1.Order{
		OrderId: s.OrderId, CustomerId: s.CustomerId, Warehouse: s.Warehouse,
		Status: s.Status, PlacedAtMs: s.PlacedAtMs, ShippedAtMs: s.ShippedAtMs,
		Carrier: s.Carrier, Tracking: s.Tracking,
	}
}

func cloneTask(s *fulfillmentv1.FulfillmentTask) *fulfillmentv1.FulfillmentTask {
	if s == nil {
		return &fulfillmentv1.FulfillmentTask{}
	}
	return &fulfillmentv1.FulfillmentTask{
		TaskId: s.TaskId, OrderId: s.OrderId, Warehouse: s.Warehouse,
		Status: s.Status, CreatedAtMs: s.CreatedAtMs, PickedAtMs: s.PickedAtMs,
		ShippedAtMs: s.ShippedAtMs, Picker: s.Picker,
	}
}
