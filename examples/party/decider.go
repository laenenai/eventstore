package party

import (
	"strings"

	"github.com/laenenai/eventstore/es"
	partyv1 "github.com/laenenai/eventstore/gen/myapp/party/v1"
)

// uniqueScopeEmail is the constraint scope name under which email
// addresses are claimed. Decided by domain convention.
const uniqueScopeEmail = "party.email"

// Decider is the party aggregate's pure-function logic. Used by
// aggregate.Runtime to drive Load + Handle.
var Decider = es.Decider[State, partyv1.Command, partyv1.Event]{
	Initial: func() State {
		return State{
			PendingChanges: map[string]PendingChange{},
		}
	},
	Decide: decide,
	Evolve: evolve,
}

// ==========================================================================
// Decide — business rules
// ==========================================================================

func decide(s State, c partyv1.Command) ([]partyv1.Event, []es.ConstraintOp, error) {
	switch cmd := c.(type) {
	case *partyv1.Register:
		return decideRegister(s, cmd)

	case *partyv1.ProposeName:
		return decidePropose(s, cmd.ProposedBy, cmd.ChangeId, cmd.Reason,
			PendingChangeName, func() partyv1.Event {
				return &partyv1.NameChangeProposed{
					ChangeId: cmd.ChangeId, Proposed: cmd.Proposed,
					ProposedBy: cmd.ProposedBy, Reason: cmd.Reason,
				}
			})

	case *partyv1.ProposeEmail:
		if err := validateEmail(cmd.Proposed); err != nil {
			return nil, nil, err
		}
		return decidePropose(s, cmd.ProposedBy, cmd.ChangeId, cmd.Reason,
			PendingChangeEmail, func() partyv1.Event {
				return &partyv1.EmailChangeProposed{
					ChangeId: cmd.ChangeId, Proposed: cmd.Proposed,
					ProposedBy: cmd.ProposedBy, Reason: cmd.Reason,
				}
			})

	case *partyv1.ProposeDateOfBirth:
		return decidePropose(s, cmd.ProposedBy, cmd.ChangeId, cmd.Reason,
			PendingChangeDateOfBirth, func() partyv1.Event {
				return &partyv1.DateOfBirthChangeProposed{
					ChangeId: cmd.ChangeId, Proposed: cmd.Proposed,
					ProposedBy: cmd.ProposedBy, Reason: cmd.Reason,
				}
			})

	case *partyv1.Approve:
		return decideApprove(s, cmd)

	case *partyv1.Reject:
		return decideReject(s, cmd)

	case *partyv1.Withdraw:
		return decideWithdraw(s, cmd)

	case *partyv1.UpdatePhone:
		if err := requireActive(s); err != nil {
			return nil, nil, err
		}
		return []partyv1.Event{&partyv1.PhoneUpdated{
			NewPhone: cmd.NewPhone, ActorId: cmd.ActorId,
		}}, nil, nil

	case *partyv1.UpdateAddress:
		if err := requireActive(s); err != nil {
			return nil, nil, err
		}
		return []partyv1.Event{&partyv1.AddressUpdated{
			NewAddress: cmd.NewAddress, ActorId: cmd.ActorId,
		}}, nil, nil

	case *partyv1.Suspend:
		if err := requireActive(s); err != nil {
			return nil, nil, err
		}
		return []partyv1.Event{&partyv1.Suspended{ActorId: cmd.ActorId, Reason: cmd.Reason}}, nil, nil

	case *partyv1.Reactivate:
		if s.Status != StatusSuspended {
			return nil, nil, ErrNotSuspended
		}
		return []partyv1.Event{&partyv1.Reactivated{ActorId: cmd.ActorId, Comment: cmd.Comment}}, nil, nil

	case *partyv1.Close:
		if s.PartyID == "" {
			return nil, nil, ErrNotRegistered
		}
		if s.Status == StatusClosed {
			return nil, nil, ErrAlreadyClosed
		}
		// Releasing the email's unique-claim frees the address for
		// reuse on a future Register.
		return []partyv1.Event{&partyv1.Closed{ActorId: cmd.ActorId, Reason: cmd.Reason}},
			[]es.ConstraintOp{{Op: es.ReleaseConstraint, Scope: uniqueScopeEmail, Value: s.Email}}, nil
	}

	return nil, nil, ErrInvalidInput
}

func decideRegister(s State, cmd *partyv1.Register) ([]partyv1.Event, []es.ConstraintOp, error) {
	if s.PartyID != "" {
		return nil, nil, ErrAlreadyRegistered
	}
	if cmd.Name == nil || cmd.Name.First == "" || cmd.Name.Last == "" {
		return nil, nil, ErrInvalidInput
	}
	if err := validateEmail(cmd.Email); err != nil {
		return nil, nil, err
	}

	return []partyv1.Event{&partyv1.Registered{
			Name:        cmd.Name,
			Email:       cmd.Email,
			Phone:       cmd.Phone,
			Address:     cmd.Address,
			DateOfBirth: cmd.DateOfBirth,
			ActorId:     cmd.ActorId,
		}},
		[]es.ConstraintOp{{Op: es.ClaimConstraint, Scope: uniqueScopeEmail, Value: cmd.Email}}, nil
}

func decidePropose(
	s State,
	proposedBy, changeID, reason string,
	kind PendingChangeKind,
	makeEvent func() partyv1.Event,
) ([]partyv1.Event, []es.ConstraintOp, error) {
	if err := requireActive(s); err != nil {
		return nil, nil, err
	}
	if changeID == "" || proposedBy == "" {
		return nil, nil, ErrInvalidInput
	}
	if s.HasPending(kind) {
		return nil, nil, ErrPendingExists
	}
	return []partyv1.Event{makeEvent()}, nil, nil
}

func decideApprove(s State, cmd *partyv1.Approve) ([]partyv1.Event, []es.ConstraintOp, error) {
	if err := requireActive(s); err != nil {
		return nil, nil, err
	}
	pc, ok := s.PendingChanges[cmd.ChangeId]
	if !ok {
		return nil, nil, ErrNoSuchChange
	}
	if pc.ProposedBy == cmd.ApprovedBy {
		return nil, nil, ErrSelfApproval
	}

	switch pc.Kind {
	case PendingChangeName:
		return []partyv1.Event{&partyv1.NameChangeApplied{
			ChangeId: pc.ChangeID, ApprovedBy: cmd.ApprovedBy,
			NewName: nameToProto(pc.NameVal),
		}}, nil, nil

	case PendingChangeEmail:
		// Atomically release the old claim and acquire the new one.
		return []partyv1.Event{&partyv1.EmailChangeApplied{
				ChangeId: pc.ChangeID, ApprovedBy: cmd.ApprovedBy,
				OldEmail: s.Email, NewEmail: pc.EmailVal,
			}},
			[]es.ConstraintOp{
				{Op: es.ReleaseConstraint, Scope: uniqueScopeEmail, Value: s.Email},
				{Op: es.ClaimConstraint, Scope: uniqueScopeEmail, Value: pc.EmailVal},
			}, nil

	case PendingChangeDateOfBirth:
		return []partyv1.Event{&partyv1.DateOfBirthChangeApplied{
			ChangeId: pc.ChangeID, ApprovedBy: cmd.ApprovedBy,
			NewDateOfBirth: pc.DOBVal,
		}}, nil, nil
	}
	return nil, nil, ErrInvalidInput
}

func decideReject(s State, cmd *partyv1.Reject) ([]partyv1.Event, []es.ConstraintOp, error) {
	pc, ok := s.PendingChanges[cmd.ChangeId]
	if !ok {
		return nil, nil, ErrNoSuchChange
	}
	if pc.ProposedBy == cmd.RejectedBy {
		return nil, nil, ErrSelfReject
	}
	return []partyv1.Event{&partyv1.ChangeRejected{
		ChangeId: pc.ChangeID, RejectedBy: cmd.RejectedBy, Reason: cmd.Reason,
	}}, nil, nil
}

func decideWithdraw(s State, cmd *partyv1.Withdraw) ([]partyv1.Event, []es.ConstraintOp, error) {
	pc, ok := s.PendingChanges[cmd.ChangeId]
	if !ok {
		return nil, nil, ErrNoSuchChange
	}
	if pc.ProposedBy != cmd.WithdrawnBy {
		return nil, nil, ErrNotProposer
	}
	return []partyv1.Event{&partyv1.ChangeWithdrawn{
		ChangeId: pc.ChangeID, WithdrawnBy: cmd.WithdrawnBy,
	}}, nil, nil
}

// requireActive enforces "party must be registered and active" for
// commands that mutate the live record.
func requireActive(s State) error {
	if s.PartyID == "" {
		return ErrNotRegistered
	}
	if s.Status != StatusActive {
		return ErrNotActive
	}
	return nil
}

func validateEmail(email string) error {
	if email == "" || !strings.Contains(email, "@") {
		return ErrInvalidInput
	}
	return nil
}

// ==========================================================================
// Evolve — fold events into state
// ==========================================================================

func evolve(s State, e partyv1.Event) State {
	if s.PendingChanges == nil {
		s.PendingChanges = map[string]PendingChange{}
	}

	switch evt := e.(type) {
	case *partyv1.Registered:
		s.PartyID = evt.ActorId // by convention; many domains use a separate id
		s.Name = nameFromProto(evt.Name)
		s.Email = evt.Email
		s.Phone = evt.Phone
		s.Address = addressFromProto(evt.Address)
		s.DateOfBirth = evt.DateOfBirth
		s.Status = StatusActive
		s.CreatedBy = evt.ActorId

	case *partyv1.NameChangeProposed:
		s.PendingChanges[evt.ChangeId] = PendingChange{
			ChangeID:   evt.ChangeId,
			ProposedBy: evt.ProposedBy,
			Reason:     evt.Reason,
			Kind:       PendingChangeName,
			NameVal:    nameFromProto(evt.Proposed),
		}
	case *partyv1.NameChangeApplied:
		delete(s.PendingChanges, evt.ChangeId)
		s.Name = nameFromProto(evt.NewName)

	case *partyv1.EmailChangeProposed:
		s.PendingChanges[evt.ChangeId] = PendingChange{
			ChangeID:   evt.ChangeId,
			ProposedBy: evt.ProposedBy,
			Reason:     evt.Reason,
			Kind:       PendingChangeEmail,
			EmailVal:   evt.Proposed,
		}
	case *partyv1.EmailChangeApplied:
		delete(s.PendingChanges, evt.ChangeId)
		s.Email = evt.NewEmail

	case *partyv1.DateOfBirthChangeProposed:
		s.PendingChanges[evt.ChangeId] = PendingChange{
			ChangeID:   evt.ChangeId,
			ProposedBy: evt.ProposedBy,
			Reason:     evt.Reason,
			Kind:       PendingChangeDateOfBirth,
			DOBVal:     evt.Proposed,
		}
	case *partyv1.DateOfBirthChangeApplied:
		delete(s.PendingChanges, evt.ChangeId)
		s.DateOfBirth = evt.NewDateOfBirth

	case *partyv1.ChangeRejected:
		delete(s.PendingChanges, evt.ChangeId)
	case *partyv1.ChangeWithdrawn:
		delete(s.PendingChanges, evt.ChangeId)

	case *partyv1.PhoneUpdated:
		s.Phone = evt.NewPhone
	case *partyv1.AddressUpdated:
		s.Address = addressFromProto(evt.NewAddress)

	case *partyv1.Suspended:
		s.Status = StatusSuspended
	case *partyv1.Reactivated:
		s.Status = StatusActive
	case *partyv1.Closed:
		s.Status = StatusClosed
	}

	return s
}

// EventCodec is the codegen-emitted codec for the Event sum type.
// Re-exported here so consumers wire it without importing the proto
// package directly.
var EventCodec = partyv1.EventCodec{}
