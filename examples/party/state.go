package party

import (
	partyv1 "github.com/laenenai/eventstore/gen/myapp/party/v1"
)

// State is the proto-defined Party message used directly as the
// aggregate's folded state. No conversion layer; the decider mutates
// the proto pointer in place during Evolve.
//
// See examples/party/README.md for the rationale (snapshots are free,
// single source of truth, less code than a parallel Go struct).
type State = partyv1.Party

// hasPending reports whether the state holds a pending change of the
// given proto oneof variant type T. T must be one of the variant
// wrappers — *partyv1.PendingChange_Name, *partyv1.PendingChange_Email,
// or *partyv1.PendingChange_DateOfBirth — produced by protoc-gen-go.
func hasPending[T any](s *State) bool {
	for _, pc := range s.GetPendingChanges() {
		if _, ok := pc.GetProposed().(T); ok {
			return true
		}
	}
	return false
}

// findPending returns the pending change with the given change_id,
// plus its index in the slice, or (nil, -1) if not found.
func findPending(s *State, changeID string) (*partyv1.PendingChange, int) {
	for i, pc := range s.GetPendingChanges() {
		if pc.GetChangeId() == changeID {
			return pc, i
		}
	}
	return nil, -1
}

// removePending removes the pending change at index i. Used by Evolve
// on Approve / Reject / Withdraw.
func removePending(s *State, i int) {
	s.PendingChanges = append(s.PendingChanges[:i], s.PendingChanges[i+1:]...)
}
