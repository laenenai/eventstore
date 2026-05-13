// Package employee is the worked example for crypto-shredding
// (ADR 0010, cookbook 11). PII fields are `bytes` and the
// codegen-emitted EncryptPII / DecryptPII methods round-trip them
// through the framework's Shredder at write and read time.
package employee

import (
	"errors"

	"github.com/laenenai/eventstore/es"
	employeev1 "github.com/laenenai/eventstore/gen/myapp/employee/v1"
)

var (
	ErrAlreadyHired = errors.New("employee: already hired")
	ErrNotHired     = errors.New("employee: not hired")
	ErrTerminated   = errors.New("employee: already terminated")
	ErrUnknownCmd   = errors.New("employee: unknown command")
)

// Decider holds the Employee state machine.
var Decider = es.Decider[*employeev1.Employee, employeev1.Command, employeev1.Event]{
	Initial: func() *employeev1.Employee { return &employeev1.Employee{} },

	Decide: func(s *employeev1.Employee, c employeev1.Command) ([]employeev1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *employeev1.Hire:
			if s.EmployeeId != "" {
				return nil, nil, ErrAlreadyHired
			}
			return []employeev1.Event{
				&employeev1.Hired{
					EmployeeId:   cmd.EmployeeId,
					LegalName:    cmd.LegalName,
					Email:        cmd.Email,
					DateOfBirth:  cmd.DateOfBirth,
					Department:   cmd.Department,
					InitialRole:  cmd.InitialRole,
				},
			}, nil, nil

		case *employeev1.Promote:
			if s.EmployeeId == "" {
				return nil, nil, ErrNotHired
			}
			if s.Status == employeev1.Status_STATUS_TERMINATED {
				return nil, nil, ErrTerminated
			}
			return []employeev1.Event{
				&employeev1.Promoted{
					EmployeeId: s.EmployeeId,
					NewRole:    cmd.NewRole,
				},
			}, nil, nil

		case *employeev1.Terminate:
			if s.EmployeeId == "" {
				return nil, nil, ErrNotHired
			}
			if s.Status == employeev1.Status_STATUS_TERMINATED {
				return nil, nil, ErrTerminated
			}
			return []employeev1.Event{
				&employeev1.Terminated{
					EmployeeId: s.EmployeeId,
					Reason:     cmd.Reason,
				},
			}, nil, nil
		}
		return nil, nil, ErrUnknownCmd
	},

	Evolve: func(s *employeev1.Employee, e employeev1.Event) *employeev1.Employee {
		out := cloneState(s)
		switch evt := e.(type) {
		case *employeev1.Hired:
			out.EmployeeId = evt.EmployeeId
			out.LegalName = evt.LegalName
			out.Email = evt.Email
			out.DateOfBirth = evt.DateOfBirth
			out.Department = evt.Department
			out.CurrentRole = evt.InitialRole
			out.Status = employeev1.Status_STATUS_ACTIVE
		case *employeev1.Promoted:
			out.CurrentRole = evt.NewRole
		case *employeev1.Terminated:
			out.Status = employeev1.Status_STATUS_TERMINATED
		}
		return out
	},

	// Terminated employees can't be re-hired through the same
	// stream. New hires get a new stream id (which means a new
	// subject id and a new DEK — clean break).
	IsTerminal: func(s *employeev1.Employee) bool {
		return s.Status == employeev1.Status_STATUS_TERMINATED
	},
}

func cloneState(s *employeev1.Employee) *employeev1.Employee {
	if s == nil {
		return &employeev1.Employee{}
	}
	out := &employeev1.Employee{
		EmployeeId:  s.EmployeeId,
		Department:  s.Department,
		CurrentRole: s.CurrentRole,
		Status:      s.Status,
	}
	if s.LegalName != nil {
		out.LegalName = append([]byte(nil), s.LegalName...)
	}
	if s.Email != nil {
		out.Email = append([]byte(nil), s.Email...)
	}
	if s.DateOfBirth != nil {
		out.DateOfBirth = append([]byte(nil), s.DateOfBirth...)
	}
	return out
}
