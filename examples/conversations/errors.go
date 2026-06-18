package conversations

import "errors"

// Error sentinels returned by the Decider. These pass through
// es.Decider.Decide back to the runtime, which wraps them with
// stream + version context. Application code switches on these via
// errors.Is to render user-facing failure modes.
var (
	ErrAlreadyStarted        = errors.New("conversation: already started")
	ErrNotStarted            = errors.New("conversation: not started")
	ErrConversationClosed    = errors.New("conversation: already closed")
	ErrEmptyMessage          = errors.New("conversation: message content cannot be empty")
	ErrModelMismatch         = errors.New("conversation: command's model does not match the started conversation")
	ErrUserMismatch          = errors.New("conversation: command's user_id does not match the started conversation")
	ErrMaxTurnsReached       = errors.New("conversation: max turns reached (auto-close)")
	ErrTokenBudgetExceeded   = errors.New("conversation: token budget exceeded")
	ErrUnknownCommand        = errors.New("conversation: unknown command variant")
)
