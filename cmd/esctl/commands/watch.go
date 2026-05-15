package commands

import (
	"context"
	"time"
)

// minRefresh clamps --refresh so an accidental "10ms" doesn't hammer
// the DB. Users who genuinely need sub-100ms polling should not be
// using a CLI debug tool — they should subscribe via state_stream or
// the outbox bus instead.
const minRefresh = 100 * time.Millisecond

// Watch invokes tick once immediately, then once per refresh interval
// until ctx is cancelled or tick returns an error.
//
// Returns nil on clean context cancellation (Ctrl-C). Other errors
// propagate.
func Watch(ctx context.Context, refresh time.Duration, tick func(context.Context) error) error {
	if refresh < minRefresh {
		refresh = minRefresh
	}
	if err := tick(ctx); err != nil {
		return err
	}
	t := time.NewTicker(refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := tick(ctx); err != nil {
				return err
			}
		}
	}
}
