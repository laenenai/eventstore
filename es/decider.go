package es

// Decider is the framework's aggregate model — three pure functions
// describing how a stream's state evolves and which events a command
// produces. See ADR 0003 for the full rationale.
//
// All three functions MUST be pure:
//
//   - Initial returns the zero-value state for an unborn aggregate.
//   - Decide is the business rule: given the current state and an
//     incoming command, return the events to append plus any
//     uniqueness constraint operations. Return an error to reject
//     the command without state change.
//   - Evolve folds one event into state. This function is run during
//     replay to rebuild state from history; it must never call
//     time.Now(), generate UUIDs, perform I/O, or otherwise depend
//     on anything outside (state, event).
//   - IsTerminal (optional) signals whether the stream is closed. The
//     aggregate runtime may reject further commands on terminal
//     streams; nil means "never terminal".
//
// Type parameters:
//
//   - S is the aggregate's state type. Typically a struct of plain
//     fields; codegen emits the type from the .proto State message.
//   - C is the command sum type (sealed interface). Codegen emits the
//     interface and variants from the .proto Commands oneof (ADR 0004).
//   - E is the event sum type (sealed interface). Codegen emits the
//     interface and variants from the .proto Events oneof (ADR 0004).
type Decider[S, C, E any] struct {
	Initial    func() S
	Decide     func(state S, cmd C) (events []E, constraints []ConstraintOp, err error)
	Evolve     func(state S, event E) S
	IsTerminal func(state S) bool
}
