// Package inproc is an in-process EventPublisher for tests and
// single-process examples. Subscribers are invoked synchronously in
// the same goroutine as Publish. Not durable; not for production.
package inproc

import (
	"context"
	"sync"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/publisher"
)

// Subscriber is invoked once per published event. Returning an error
// causes Publish to return that error to its caller (the outbox drain
// typically), which keeps the outbox row unmarked for retry.
type Subscriber func(ctx context.Context, env es.Envelope) error

// Publisher fans out each published event to every registered
// subscriber, synchronously, in registration order. Concurrency-safe.
type Publisher struct {
	mu   sync.RWMutex
	subs []Subscriber
}

// New returns a Publisher with no subscribers.
func New() *Publisher {
	return &Publisher{}
}

// Subscribe registers a subscriber. The subscriber is invoked on every
// future Publish call. There is no unsubscribe — subscriptions live
// until the Publisher is discarded.
func (p *Publisher) Subscribe(s Subscriber) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subs = append(p.subs, s)
}

// Publish invokes every subscriber in registration order. Returns the
// first error encountered (the outbox drain treats this as a failed
// publish and retries on the next run).
func (p *Publisher) Publish(ctx context.Context, env es.Envelope) error {
	p.mu.RLock()
	subs := append([]Subscriber(nil), p.subs...)
	p.mu.RUnlock()

	for _, s := range subs {
		if err := s(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

var _ publisher.Publisher = (*Publisher)(nil)
