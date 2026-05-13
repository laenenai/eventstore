package party

import (
	partyv1 "github.com/laenenai/eventstore/gen/myapp/party/v1"
)

// State is the folded representation of a party aggregate. It does not
// have to mirror the proto Party message exactly — the decider is
// generic over (S, C, E) and only the wire shapes (C, E) need to come
// from codegen.
type State struct {
	PartyID     string
	Name        Name
	Email       string
	Phone       string
	Address     Address
	DateOfBirth string
	Status      Status

	// Pending changes keyed by change_id for O(1) lookup on
	// Approve/Reject/Withdraw. The decider enforces "at most one
	// pending per Kind" as a domain invariant.
	PendingChanges map[string]PendingChange

	CreatedBy string
}

// Name mirrors partyv1.Name as a value-type Go struct.
type Name struct {
	First, Last string
}

// Address mirrors partyv1.Address as a value-type Go struct.
type Address struct {
	Line1, Line2, City, PostalCode, Country string
}

// Status mirrors partyv1.Status. We don't import the proto enum
// directly because the decider's logic doesn't need protobuf semantics
// — plain Go ints are clearer.
type Status int

const (
	StatusUnspecified Status = iota
	StatusActive
	StatusSuspended
	StatusClosed
)

// PendingChangeKind discriminates the oneof inside a PendingChange.
type PendingChangeKind int

const (
	PendingChangeNone PendingChangeKind = iota
	PendingChangeName
	PendingChangeEmail
	PendingChangeDateOfBirth
)

// PendingChange holds one in-flight propose-and-approve workflow.
// The Kind field discriminates which of NameVal/EmailVal/DOBVal is
// meaningful.
type PendingChange struct {
	ChangeID   string
	ProposedBy string
	Reason     string

	Kind     PendingChangeKind
	NameVal  Name   // when Kind == PendingChangeName
	EmailVal string // when Kind == PendingChangeEmail
	DOBVal   string // when Kind == PendingChangeDateOfBirth
}

// HasPending returns true if a pending change of the given kind exists.
// Used by the decider to enforce "at most one per kind".
func (s State) HasPending(kind PendingChangeKind) bool {
	for _, pc := range s.PendingChanges {
		if pc.Kind == kind {
			return true
		}
	}
	return false
}

// ----- Proto conversion helpers -------------------------------------------

func nameFromProto(p *partyv1.Name) Name {
	if p == nil {
		return Name{}
	}
	return Name{First: p.First, Last: p.Last}
}

func nameToProto(n Name) *partyv1.Name {
	return &partyv1.Name{First: n.First, Last: n.Last}
}

func addressFromProto(p *partyv1.Address) Address {
	if p == nil {
		return Address{}
	}
	return Address{
		Line1:      p.Line1,
		Line2:      p.Line2,
		City:       p.City,
		PostalCode: p.PostalCode,
		Country:    p.Country,
	}
}

func addressToProto(a Address) *partyv1.Address {
	return &partyv1.Address{
		Line1:      a.Line1,
		Line2:      a.Line2,
		City:       a.City,
		PostalCode: a.PostalCode,
		Country:    a.Country,
	}
}

func statusFromProto(p partyv1.Status) Status {
	switch p {
	case partyv1.Status_STATUS_ACTIVE:
		return StatusActive
	case partyv1.Status_STATUS_SUSPENDED:
		return StatusSuspended
	case partyv1.Status_STATUS_CLOSED:
		return StatusClosed
	default:
		return StatusUnspecified
	}
}
