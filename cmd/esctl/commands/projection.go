package commands

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

// ProjectionCommand assembles `esctl projection`.
func ProjectionCommand() *cli.Command {
	return &cli.Command{
		Name:  "projection",
		Usage: "Inspect projection runners and cursors",
		Commands: []*cli.Command{
			{
				Name:  "list",
				Usage: "List registered projections and their cursor positions (cross-tenant)",
				Action: func(ctx context.Context, c *cli.Command) error {
					r := newRenderer(c)
					return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
						rows, err := s.List(ctx)
						if err != nil {
							return err
						}
						if r.Format == "json" {
							return r.json(rows)
						}
						for _, p := range rows {
							fmt.Fprintf(r.Out, "%s%s%s/%s  cursor=%d  updated=%s\n",
								r.bold(), p.Name, r.reset(), p.TenantID, p.Cursor,
								p.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"))
						}
						return nil
					})
				},
			},
		},
	}
}
