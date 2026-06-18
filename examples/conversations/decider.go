// Package conversations is the worked AI-backend example for
// eventstore. One stream per conversation, append-only turns,
// crypto-shredded message bodies, runnable end-to-end against Ollama
// for a local LLM.
//
// See examples/conversations/README.md for the design rationale and
// ADR cross-references.
package conversations

import (
	"strings"

	conversationv1 "github.com/laenenai/eventstore/gen/myapp/conversation/v1"

	"github.com/laenenai/eventstore/es"
)

// MaxTurns caps the dialogue length. Exists so a runaway agent loop
// cannot append events without bound, which would balloon both the
// event log and the state_cache JSONB row. Adopters tune via a
// per-aggregate configuration in production; this example uses a
// const so the Decider stays pure (ADR 0003 — no I/O, no clocks, no
// config lookups inside Decide).
const MaxTurns = 200

// MaxTokensBudget caps cumulative token spend per conversation.
// Like MaxTurns: a defensive ceiling so a failure mode in the LLM
// driver code cannot run up an unbounded bill. Set to 0 to disable.
const MaxTokensBudget int64 = 100_000

// Decider is the pure Initial/Decide/Evolve trio per ADR 0003. State
// is the proto-defined Conversation; the slice of turns lets the LLM
// driver send full history to the model without replaying events.
var Decider = es.Decider[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event]{
	Initial: func() *conversationv1.Conversation {
		return &conversationv1.Conversation{}
	},

	Decide: func(s *conversationv1.Conversation, c conversationv1.Command) ([]conversationv1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {

		case *conversationv1.Start:
			if s.ConversationId != "" {
				return nil, nil, ErrAlreadyStarted
			}
			return []conversationv1.Event{
				&conversationv1.Started{
					ConversationId: cmd.ConversationId,
					UserId:         cmd.UserId,
					Model:          cmd.Model,
					SystemPrompt:   cmd.SystemPrompt,
				},
			}, nil, nil

		case *conversationv1.AppendUserMessage:
			if err := guardStarted(s, cmd.UserId); err != nil {
				return nil, nil, err
			}
			if strings.TrimSpace(cmd.Content) == "" {
				return nil, nil, ErrEmptyMessage
			}
			if MaxTokensBudget > 0 && s.TokensInput+s.TokensOutput+cmd.Tokens > MaxTokensBudget {
				return nil, nil, ErrTokenBudgetExceeded
			}
			events := []conversationv1.Event{
				&conversationv1.UserMessageAppended{
					ConversationId: s.ConversationId,
					UserId:         s.UserId,
					Content:        cmd.Content,
					Tokens:         cmd.Tokens,
				},
			}
			if len(s.Turns)+1 >= MaxTurns {
				events = append(events, &conversationv1.Closed{
					ConversationId: s.ConversationId,
					UserId:         s.UserId,
					Reason:         "max_turns_reached",
				})
			}
			return events, nil, nil

		case *conversationv1.AppendAssistantMessage:
			if err := guardStarted(s, cmd.UserId); err != nil {
				return nil, nil, err
			}
			if strings.TrimSpace(cmd.Content) == "" {
				return nil, nil, ErrEmptyMessage
			}
			events := []conversationv1.Event{
				&conversationv1.AssistantMessageAppended{
					ConversationId: s.ConversationId,
					UserId:         s.UserId,
					Content:        cmd.Content,
					Tokens:         cmd.Tokens,
				},
			}
			if len(s.Turns)+1 >= MaxTurns {
				events = append(events, &conversationv1.Closed{
					ConversationId: s.ConversationId,
					UserId:         s.UserId,
					Reason:         "max_turns_reached",
				})
			}
			return events, nil, nil

		case *conversationv1.Close:
			if err := guardStarted(s, cmd.UserId); err != nil {
				return nil, nil, err
			}
			if s.Closed {
				// Idempotent close — re-issuing the command returns no
				// new events rather than erroring. Matches the
				// "operator hit the button twice" failure mode.
				return nil, nil, nil
			}
			reason := cmd.Reason
			if reason == "" {
				reason = "user_ended"
			}
			return []conversationv1.Event{
				&conversationv1.Closed{
					ConversationId: s.ConversationId,
					UserId:         s.UserId,
					Reason:         reason,
				},
			}, nil, nil
		}
		return nil, nil, ErrUnknownCommand
	},

	Evolve: func(s *conversationv1.Conversation, e conversationv1.Event) *conversationv1.Conversation {
		out := cloneState(s)
		switch evt := e.(type) {
		case *conversationv1.Started:
			out.ConversationId = evt.ConversationId
			out.UserId = evt.UserId
			out.Model = evt.Model
			out.SystemPrompt = evt.SystemPrompt
		case *conversationv1.UserMessageAppended:
			out.Turns = append(out.Turns, &conversationv1.Turn{
				Role:    conversationv1.Turn_ROLE_USER,
				Content: evt.Content,
				Tokens:  evt.Tokens,
			})
			out.TokensInput += evt.Tokens
		case *conversationv1.AssistantMessageAppended:
			out.Turns = append(out.Turns, &conversationv1.Turn{
				Role:    conversationv1.Turn_ROLE_ASSISTANT,
				Content: evt.Content,
				Tokens:  evt.Tokens,
			})
			out.TokensOutput += evt.Tokens
		case *conversationv1.Closed:
			out.Closed = true
			out.CloseReason = evt.Reason
		}
		return out
	},

	// A closed conversation is terminal. Subsequent commands against
	// the same stream return ErrConversationClosed; new conversations
	// get a fresh stream id (and a fresh DEK since user_id is the
	// subject_field).
	IsTerminal: func(s *conversationv1.Conversation) bool {
		return s.Closed
	},
}

// guardStarted is the shared precondition for every command that
// references an already-running conversation: it must exist, it must
// not be closed, and the caller's claimed user_id must match the
// conversation's owner. The user_id check defends against a confused
// router sending tenant-A's user-1 command to tenant-A's user-2
// conversation — which would otherwise silently mix histories.
func guardStarted(s *conversationv1.Conversation, claimedUserID string) error {
	if s.ConversationId == "" {
		return ErrNotStarted
	}
	if s.Closed {
		return ErrConversationClosed
	}
	if claimedUserID != "" && s.UserId != claimedUserID {
		return ErrUserMismatch
	}
	return nil
}

// cloneState is the manual deep copy the Evolve closure folds over.
// We can't use proto.Clone here because it allocates via reflection
// and Evolve is called per event during replay — a small per-call
// overhead becomes meaningful on a 200-turn replay path. The fields
// we copy are exactly the ones Evolve mutates plus the scalars; the
// turns slice is reassigned via append in the call sites above so a
// shallow copy of the header is correct.
func cloneState(s *conversationv1.Conversation) *conversationv1.Conversation {
	out := &conversationv1.Conversation{
		ConversationId: s.ConversationId,
		UserId:         s.UserId,
		Model:          s.Model,
		SystemPrompt:   s.SystemPrompt,
		TokensInput:    s.TokensInput,
		TokensOutput:   s.TokensOutput,
		Closed:         s.Closed,
		CloseReason:    s.CloseReason,
	}
	if len(s.Turns) > 0 {
		out.Turns = make([]*conversationv1.Turn, len(s.Turns))
		copy(out.Turns, s.Turns)
	}
	return out
}
