package commands

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/laenenai/eventstore/es"
)

// StateCommand assembles `esctl state`.
func StateCommand() *cli.Command {
	return &cli.Command{
		Name:  "state",
		Usage: "Inspect state_cache rows (current aggregate state)",
		Commands: []*cli.Command{
			{
				Name:  "get",
				Usage: "Get the cached state of one stream",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "stream", Usage: "Stream id within the tenant", Required: true},
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					tenant := requireTenant(c)
					if tenant == "" {
						return errors.New("--tenant is required for state get")
					}
					sid, err := parseStreamID(tenant, c.String("stream"))
					if err != nil {
						return err
					}
					r := newRenderer(c)
					return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
						row, err := s.GetState(ctx, tenant, sid.Canonical())
						if err != nil {
							return err
						}
						return r.StateRow(row)
					})
				},
			},
			{
				Name:  "list",
				Usage: "Page through cached states for an aggregate type",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "type", Usage: "State TypeURL", Required: true},
					&cli.IntFlag{Name: "limit", Usage: "Page size", Value: 100},
					&cli.BoolFlag{Name: "all", Usage: "Iterate every page (use ScanAllStates)"},
					&cli.BoolFlag{Name: "watch", Aliases: []string{"w"}, Usage: "Tail new/changed rows"},
					&cli.DurationFlag{Name: "refresh", Usage: "Watch poll interval", Value: 5 * time.Second},
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					tenant := requireTenant(c)
					if tenant == "" {
						return errors.New("--tenant is required for state list")
					}
					r := newRenderer(c)
					return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
						tick := func(ctx context.Context) error {
							if c.Bool("all") {
								return scanAll(ctx, r, s, tenant, c.String("type"), int(c.Int("limit")))
							}
							rows, err := s.ListStates(ctx, tenant, c.String("type"), "", int(c.Int("limit")))
							if err != nil {
								return err
							}
							for _, row := range rows {
								if err := r.StateRow(row); err != nil {
									return err
								}
							}
							if len(rows) == int(c.Int("limit")) {
								fmt.Fprintf(r.Out, "\n%s(more rows — pass --all to iterate, or --after %s)%s\n",
									r.dim(), rows[len(rows)-1].StreamID, r.reset())
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

func scanAll(ctx context.Context, r *Renderer, s Store, tenant, typeURL string, page int) error {
	for row, err := range es.ScanAllStates(ctx, s, tenant, typeURL, page) {
		if err != nil {
			return err
		}
		if err := r.StateRow(row); err != nil {
			return err
		}
	}
	return nil
}
