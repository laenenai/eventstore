package commands

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/urfave/cli/v3"
)

// EventCommand assembles `esctl event`.
func EventCommand() *cli.Command {
	return &cli.Command{
		Name:  "event",
		Usage: "Look up individual events",
		Commands: []*cli.Command{
			{
				Name:  "get",
				Usage: "Fetch one event by id",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "event-id", Usage: "Event UUID", Required: true},
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					tenant := requireTenant(c)
					if tenant == "" {
						return errors.New("--tenant is required for event get")
					}
					id, err := uuid.Parse(c.String("event-id"))
					if err != nil {
						return err
					}
					r := newRenderer(c)
					return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
						e, err := s.GetEventByID(ctx, tenant, id)
						if err != nil {
							return err
						}
						return r.Envelope(e)
					})
				},
			},
		},
	}
}
