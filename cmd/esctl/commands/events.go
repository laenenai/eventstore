package commands

import (
	"context"
	"errors"
	"time"

	"github.com/urfave/cli/v3"
)

// EventsCommand assembles `esctl events` (store-wide read; distinct
// from the singular `event` command which fetches one by id).
func EventsCommand() *cli.Command {
	return &cli.Command{
		Name:  "events",
		Usage: "Store-wide event read",
		Commands: []*cli.Command{
			{
				Name:  "tail",
				Usage: "Stream events in global_position order. Watch by default — pass --no-watch for one-shot.",
				Flags: []cli.Flag{
					&cli.UintFlag{Name: "from-position", Usage: "Resume cursor", Value: 0},
					&cli.IntFlag{Name: "limit", Usage: "Per-tick page size", Value: 100},
					&cli.BoolFlag{Name: "watch", Aliases: []string{"w"}, Usage: "Poll continuously (default)", Value: true},
					&cli.DurationFlag{Name: "refresh", Usage: "Watch interval", Value: 2 * time.Second},
					&cli.BoolFlag{Name: "from-beginning", Usage: "Start at gp=0 instead of current head"},
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					tenant := requireTenant(c)
					if tenant == "" {
						return errors.New("--tenant is required for events tail")
					}
					r := newRenderer(c)
					return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
						cursor := uint64(c.Uint("from-position"))
						// Default "current head" — read once empty to advance the cursor.
						if cursor == 0 && !c.Bool("from-beginning") && c.Bool("watch") {
							envs, err := s.ReadAllForTenant(ctx, tenant, 0, 1<<31-1)
							if err != nil {
								return err
							}
							if len(envs) > 0 {
								cursor = envs[len(envs)-1].GlobalPosition
							}
						}
						tick := func(ctx context.Context) error {
							envs, err := s.ReadAllForTenant(ctx, tenant, cursor, int(c.Int("limit")))
							if err != nil {
								return err
							}
							for _, e := range envs {
								if err := r.Envelope(e); err != nil {
									return err
								}
								cursor = e.GlobalPosition
							}
							return nil
						}
						if c.Bool("watch") {
							return Watch(ctx, c.Duration("refresh"), tick)
						}
						return tick(ctx)
					})
				},
			},
		},
	}
}
