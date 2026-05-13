package projection

import (
	"context"

	"github.com/laenenai/eventstore/es"
)

// DispatcherOption configures a generated NewProjectionDispatcher.
// The only option in v1 is IgnoreUnknown — see ADR 0020 decision 3b.
type DispatcherOption interface {
	applyDispatcher(*DispatcherConfig)
}

// DispatcherConfig is the assembled options passed to generated
// dispatchers. Exported so codegen can reach it; users go through the
// Option constructors instead of touching this struct directly.
type DispatcherConfig struct {
	IgnoreUnknown bool
}

// ApplyOptions reduces a list of DispatcherOptions into a config.
// Codegen calls this from NewProjectionDispatcher.
func ApplyOptions(opts []DispatcherOption) DispatcherConfig {
	var cfg DispatcherConfig
	for _, opt := range opts {
		opt.applyDispatcher(&cfg)
	}
	return cfg
}

type ignoreUnknownOpt struct{}

func (ignoreUnknownOpt) applyDispatcher(c *DispatcherConfig) { c.IgnoreUnknown = true }

// IgnoreUnknown returns a DispatcherOption that makes the generated
// dispatcher silently skip events whose TypeURL is not part of this
// aggregate's event set. Use when composing dispatchers from multiple
// aggregates via Chain.
//
// Without it, an unknown TypeURL causes the dispatcher to return an
// error and the projection batch halts via the fail-stop semantics in
// ADR 0020 decision 3d — the right behavior for single-aggregate
// projections (it catches "I added an event variant but forgot to
// handle it" bugs at runtime), but wrong for cross-aggregate
// composition where each dispatcher only knows part of the stream.
func IgnoreUnknown() DispatcherOption { return ignoreUnknownOpt{} }

// Chain composes multiple Handlers into one. Each event is passed to
// every handler in sequence; if any returns an error, Chain stops and
// returns that error.
//
// Typical use: cross-aggregate projection that handles events from
// more than one aggregate. Pair each generated dispatcher with
// IgnoreUnknown() so it skips events from the other aggregate(s)
// rather than erroring out.
//
//	handler := projection.Chain(
//	    invoice.NewProjectionDispatcher(&p, projection.IgnoreUnknown()),
//	    customer.NewProjectionDispatcher(&p, projection.IgnoreUnknown()),
//	)
func Chain(handlers ...Handler) Handler {
	return func(ctx context.Context, env es.Envelope) error {
		for _, h := range handlers {
			if err := h(ctx, env); err != nil {
				return err
			}
		}
		return nil
	}
}
