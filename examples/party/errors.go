package party

import "errors"

// Domain errors. The decider returns these from Decide; consumers
// match via errors.Is.
//
// All errors are domain-level (business-rule violations). Framework-
// level errors (ErrConflict, ErrConstraintViolated) come from the
// runtime/store and propagate through the Decider unchanged.

var (
	// ErrAlreadyRegistered fires when Register is called on a stream
	// that already has a Registered event.
	ErrAlreadyRegistered = errors.New("party: already registered")

	// ErrNotRegistered fires when any command other than Register
	// arrives on a fresh stream.
	ErrNotRegistered = errors.New("party: not registered")

	// ErrNotActive fires when an action is attempted on a non-active
	// party (suspended or closed). Auto-apply updates, proposes, and
	// approvals all require status=ACTIVE.
	ErrNotActive = errors.New("party: not active")

	// ErrNotSuspended fires when Reactivate is called on a party not
	// currently suspended.
	ErrNotSuspended = errors.New("party: not suspended")

	// ErrAlreadyClosed fires when Close is called on an already-closed
	// party.
	ErrAlreadyClosed = errors.New("party: already closed")

	// ErrPendingExists fires when a propose command finds an existing
	// pending change of the same kind.
	ErrPendingExists = errors.New("party: pending change of same kind already exists")

	// ErrNoSuchChange fires when Approve/Reject/Withdraw references
	// a change_id that does not exist in pending_changes.
	ErrNoSuchChange = errors.New("party: no such pending change")

	// ErrSelfApproval fires when the approver is the same actor that
	// proposed the change. The decider rejects self-approval as a
	// structural rule independent of any external authz policy.
	ErrSelfApproval = errors.New("party: self-approval forbidden")

	// ErrSelfReject fires for the same reason on Reject.
	ErrSelfReject = errors.New("party: self-reject forbidden")

	// ErrNotProposer fires when Withdraw is called by someone other
	// than the original proposer.
	ErrNotProposer = errors.New("party: only the proposer can withdraw")

	// ErrInvalidInput fires for missing or malformed command fields
	// (empty name, malformed email, etc.). The decider does light
	// validation; full validation belongs in the transport layer.
	ErrInvalidInput = errors.New("party: invalid input")
)
