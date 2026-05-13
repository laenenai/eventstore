package party

import (
	"strings"

	"github.com/laenenai/eventstore/es"
	partyv1 "github.com/laenenai/eventstore/gen/myapp/party/v1"
)

// uniqueScopeEmail is the constraint scope under which email addresses
// are claimed. Decided by domain convention.
const uniqueScopeEmail = "party.email"

// Decider is the party aggregate's pure-function logic.
var Decider = es.Decider[*State, partyv1.Command, partyv1.Event]{
	Initial: func() *State {
		return &State{}
	},
	Decide: decide,
	Evolve: evolve,
}

// ==========================================================================
// Decide — business rules
// ==========================================================================

func decide(s *State, c partyv1.Command) ([]partyv1.Event, []es.ConstraintOp, error) {
	switch cmd := c.(type) {
	case *partyv1.Register:
		return decideRegister(s, cmd)

	case *partyv1.ProposeName:
		return decidePropose(s, cmd.GetProposedBy(), cmd.GetChangeId(),
			hasPending[*partyv1.PendingChange_Name](s),
			func() partyv1.Event {
				return &partyv1.NameChangeProposed{
					ChangeId: cmd.GetChangeId(), Proposed: cmd.GetProposed(),
					ProposedBy: cmd.GetProposedBy(), Reason: cmd.GetReason(),
				}
			})

	case *partyv1.ProposeEmail:
		if err := validateEmail(cmd.GetProposed()); err != nil {
			return nil, nil, err
		}
		return decidePropose(s, cmd.GetProposedBy(), cmd.GetChangeId(),
			hasPending[*partyv1.PendingChange_Email](s),
			func() partyv1.Event {
				return &partyv1.EmailChangeProposed{
					ChangeId: cmd.GetChangeId(), Proposed: cmd.GetProposed(),
					ProposedBy: cmd.GetProposedBy(), Reason: cmd.GetReason(),
				}
			})

	case *partyv1.ProposeDateOfBirth:
		return decidePropose(s, cmd.GetProposedBy(), cmd.GetChangeId(),
			hasPending[*partyv1.PendingChange_DateOfBirth](s),
			func() partyv1.Event {
				return &partyv1.DateOfBirthChangeProposed{
					ChangeId: cmd.GetChangeId(), Proposed: cmd.GetProposed(),
					ProposedBy: cmd.GetProposedBy(), Reason: cmd.GetReason(),
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
			NewPhone: cmd.GetNewPhone(), ActorId: cmd.GetActorId(),
		}}, nil, nil

	case *partyv1.UpdateAddress:
		if err := requireActive(s); err != nil {
			return nil, nil, err
		}
		return []partyv1.Event{&partyv1.AddressUpdated{
			NewAddress: cmd.GetNewAddress(), ActorId: cmd.GetActorId(),
		}}, nil, nil

	case *partyv1.Suspend:
		if err := requireActive(s); err != nil {
			return nil, nil, err
		}
		return []partyv1.Event{&partyv1.Suspended{
			ActorId: cmd.GetActorId(), Reason: cmd.GetReason(),
		}}, nil, nil

	case *partyv1.Reactivate:
		if s.GetStatus() != partyv1.Status_STATUS_SUSPENDED {
			return nil, nil, ErrNotSuspended
		}
		return []partyv1.Event{&partyv1.Reactivated{
			ActorId: cmd.GetActorId(), Comment: cmd.GetComment(),
		}}, nil, nil

	case *partyv1.Close:
		if s.GetPartyId() == "" {
			return nil, nil, ErrNotRegistered
		}
		if s.GetStatus() == partyv1.Status_STATUS_CLOSED {
			return nil, nil, ErrAlreadyClosed
		}
		return []partyv1.Event{&partyv1.Closed{
				ActorId: cmd.GetActorId(), Reason: cmd.GetReason(),
			}},
			[]es.ConstraintOp{{
				Op: es.ReleaseConstraint, Scope: uniqueScopeEmail, Value: s.GetEmail(),
			}}, nil
	}

	return nil, nil, ErrInvalidInput
}

func decideRegister(s *State, cmd *partyv1.Register) ([]partyv1.Event, []es.ConstraintOp, error) {
	if s.GetPartyId() != "" {
		return nil, nil, ErrAlreadyRegistered
	}
	if cmd.GetName() == nil || cmd.GetName().GetFirst() == "" || cmd.GetName().GetLast() == "" {
		return nil, nil, ErrInvalidInput
	}
	if err := validateEmail(cmd.GetEmail()); err != nil {
		return nil, nil, err
	}
	return []partyv1.Event{&partyv1.Registered{
			Name:        cmd.GetName(),
			Email:       cmd.GetEmail(),
			Phone:       cmd.GetPhone(),
			Address:     cmd.GetAddress(),
			DateOfBirth: cmd.GetDateOfBirth(),
			ActorId:     cmd.GetActorId(),
		}},
		[]es.ConstraintOp{{
			Op: es.ClaimConstraint, Scope: uniqueScopeEmail, Value: cmd.GetEmail(),
		}}, nil
}

func decidePropose(
	s *State,
	proposedBy, changeID string,
	pendingExists bool,
	makeEvent func() partyv1.Event,
) ([]partyv1.Event, []es.ConstraintOp, error) {
	if err := requireActive(s); err != nil {
		return nil, nil, err
	}
	if changeID == "" || proposedBy == "" {
		return nil, nil, ErrInvalidInput
	}
	if pendingExists {
		return nil, nil, ErrPendingExists
	}
	return []partyv1.Event{makeEvent()}, nil, nil
}

func decideApprove(s *State, cmd *partyv1.Approve) ([]partyv1.Event, []es.ConstraintOp, error) {
	if err := requireActive(s); err != nil {
		return nil, nil, err
	}
	pc, _ := findPending(s, cmd.GetChangeId())
	if pc == nil {
		return nil, nil, ErrNoSuchChange
	}
	if pc.GetProposedBy() == cmd.GetApprovedBy() {
		return nil, nil, ErrSelfApproval
	}

	switch p := pc.GetProposed().(type) {
	case *partyv1.PendingChange_Name:
		return []partyv1.Event{&partyv1.NameChangeApplied{
			ChangeId: pc.GetChangeId(), ApprovedBy: cmd.GetApprovedBy(),
			NewName: p.Name,
		}}, nil, nil

	case *partyv1.PendingChange_Email:
		return []partyv1.Event{&partyv1.EmailChangeApplied{
				ChangeId: pc.GetChangeId(), ApprovedBy: cmd.GetApprovedBy(),
				OldEmail: s.GetEmail(), NewEmail: p.Email,
			}},
			[]es.ConstraintOp{
				{Op: es.ReleaseConstraint, Scope: uniqueScopeEmail, Value: s.GetEmail()},
				{Op: es.ClaimConstraint, Scope: uniqueScopeEmail, Value: p.Email},
			}, nil

	case *partyv1.PendingChange_DateOfBirth:
		return []partyv1.Event{&partyv1.DateOfBirthChangeApplied{
			ChangeId: pc.GetChangeId(), ApprovedBy: cmd.GetApprovedBy(),
			NewDateOfBirth: p.DateOfBirth,
		}}, nil, nil
	}
	return nil, nil, ErrInvalidInput
}

func decideReject(s *State, cmd *partyv1.Reject) ([]partyv1.Event, []es.ConstraintOp, error) {
	pc, _ := findPending(s, cmd.GetChangeId())
	if pc == nil {
		return nil, nil, ErrNoSuchChange
	}
	if pc.GetProposedBy() == cmd.GetRejectedBy() {
		return nil, nil, ErrSelfReject
	}
	return []partyv1.Event{&partyv1.ChangeRejected{
		ChangeId: pc.GetChangeId(), RejectedBy: cmd.GetRejectedBy(), Reason: cmd.GetReason(),
	}}, nil, nil
}

func decideWithdraw(s *State, cmd *partyv1.Withdraw) ([]partyv1.Event, []es.ConstraintOp, error) {
	pc, _ := findPending(s, cmd.GetChangeId())
	if pc == nil {
		return nil, nil, ErrNoSuchChange
	}
	if pc.GetProposedBy() != cmd.GetWithdrawnBy() {
		return nil, nil, ErrNotProposer
	}
	return []partyv1.Event{&partyv1.ChangeWithdrawn{
		ChangeId: pc.GetChangeId(), WithdrawnBy: cmd.GetWithdrawnBy(),
	}}, nil, nil
}

// requireActive enforces "party must be registered and active".
func requireActive(s *State) error {
	if s.GetPartyId() == "" {
		return ErrNotRegistered
	}
	if s.GetStatus() != partyv1.Status_STATUS_ACTIVE {
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
// Evolve — fold events into state (mutates the proto pointer in place)
// ==========================================================================

func evolve(s *State, e partyv1.Event) *State {
	switch evt := e.(type) {
	case *partyv1.Registered:
		s.PartyId = evt.GetActorId() // convention; many domains use a separate id
		s.Name = evt.GetName()
		s.Email = evt.GetEmail()
		s.Phone = evt.GetPhone()
		s.Address = evt.GetAddress()
		s.DateOfBirth = evt.GetDateOfBirth()
		s.Status = partyv1.Status_STATUS_ACTIVE
		s.CreatedBy = evt.GetActorId()

	case *partyv1.NameChangeProposed:
		s.PendingChanges = append(s.PendingChanges, &partyv1.PendingChange{
			ChangeId:   evt.GetChangeId(),
			ProposedBy: evt.GetProposedBy(),
			Reason:     evt.GetReason(),
			Proposed:   &partyv1.PendingChange_Name{Name: evt.GetProposed()},
		})
	case *partyv1.NameChangeApplied:
		if _, i := findPending(s, evt.GetChangeId()); i >= 0 {
			removePending(s, i)
		}
		s.Name = evt.GetNewName()

	case *partyv1.EmailChangeProposed:
		s.PendingChanges = append(s.PendingChanges, &partyv1.PendingChange{
			ChangeId:   evt.GetChangeId(),
			ProposedBy: evt.GetProposedBy(),
			Reason:     evt.GetReason(),
			Proposed:   &partyv1.PendingChange_Email{Email: evt.GetProposed()},
		})
	case *partyv1.EmailChangeApplied:
		if _, i := findPending(s, evt.GetChangeId()); i >= 0 {
			removePending(s, i)
		}
		s.Email = evt.GetNewEmail()

	case *partyv1.DateOfBirthChangeProposed:
		s.PendingChanges = append(s.PendingChanges, &partyv1.PendingChange{
			ChangeId:   evt.GetChangeId(),
			ProposedBy: evt.GetProposedBy(),
			Reason:     evt.GetReason(),
			Proposed:   &partyv1.PendingChange_DateOfBirth{DateOfBirth: evt.GetProposed()},
		})
	case *partyv1.DateOfBirthChangeApplied:
		if _, i := findPending(s, evt.GetChangeId()); i >= 0 {
			removePending(s, i)
		}
		s.DateOfBirth = evt.GetNewDateOfBirth()

	case *partyv1.ChangeRejected:
		if _, i := findPending(s, evt.GetChangeId()); i >= 0 {
			removePending(s, i)
		}
	case *partyv1.ChangeWithdrawn:
		if _, i := findPending(s, evt.GetChangeId()); i >= 0 {
			removePending(s, i)
		}

	case *partyv1.PhoneUpdated:
		s.Phone = evt.GetNewPhone()
	case *partyv1.AddressUpdated:
		s.Address = evt.GetNewAddress()

	case *partyv1.Suspended:
		s.Status = partyv1.Status_STATUS_SUSPENDED
	case *partyv1.Reactivated:
		s.Status = partyv1.Status_STATUS_ACTIVE
	case *partyv1.Closed:
		s.Status = partyv1.Status_STATUS_CLOSED
	}

	return s
}

// EventCodec is the codegen-emitted codec for the Event sum type,
// re-exported here for ergonomic wiring at the runtime construction site.
var EventCodec = partyv1.EventCodec{}
