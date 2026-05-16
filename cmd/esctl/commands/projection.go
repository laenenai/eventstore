package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v3"
)

// ProjectionCommand assembles `esctl projection`.
func ProjectionCommand() *cli.Command {
	return &cli.Command{
		Name:  "projection",
		Usage: "Inspect projection runners and cursors",
		Commands: []*cli.Command{
			projectionListCommand(),
			projectionResetCommand(),
			projectionResetToCommand(),
		},
	}
}

func projectionListCommand() *cli.Command {
	return &cli.Command{
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
	}
}

func projectionResetCommand() *cli.Command {
	return &cli.Command{
		Name: "reset",
		Usage: "Reset the projection cursor to 0. The runner replays from gp=0 on " +
			"next tick. The app must also TRUNCATE its read-model table — that step " +
			"is not the framework's responsibility (cookbook recipe 08).",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "name", Usage: "Projection name", Required: true},
			&cli.BoolFlag{Name: "all-tenants", Usage: "Reset cursor for every tenant that has a checkpoint row"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			name := c.String("name")
			allTenants := c.Bool("all-tenants")
			tenant := requireTenant(c)
			if !allTenants && tenant == "" {
				return errors.New("--tenant is required (or pass --all-tenants)")
			}
			args := map[string]any{"name": name}
			if allTenants {
				args["all-tenants"] = true
			}
			if !confirmed(c) {
				dryRun(c, args)
				return nil
			}
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				if allTenants {
					all, err := s.List(ctx)
					if err != nil {
						return err
					}
					n := 0
					for _, p := range all {
						if p.Name != name {
							continue
						}
						if err := s.Reset(ctx, name, p.TenantID); err != nil {
							return fmt.Errorf("reset %s/%s: %w", name, p.TenantID, err)
						}
						n++
					}
					fmt.Fprintf(dryRunOut, "OK reset %d tenant cursor(s) for projection %s\n", n, name)
					auditLog(c, "*", args)
					return nil
				}
				if err := s.Reset(ctx, name, tenant); err != nil {
					return err
				}
				fmt.Fprintf(dryRunOut, "OK reset cursor for %s/%s\n", name, tenant)
				auditLog(c, tenant, args)
				return nil
			})
		},
	}
}

func projectionResetToCommand() *cli.Command {
	return &cli.Command{
		Name: "reset-to",
		Usage: "Reset the projection cursor to a specific global_position. For partial " +
			"replay from a known-good point.",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "name", Usage: "Projection name", Required: true},
			&cli.UintFlag{Name: "position", Usage: "Target global_position", Required: true},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for projection reset-to")
			}
			name := c.String("name")
			pos := uint64(c.Uint("position"))
			args := map[string]any{"name": name, "position": pos}
			if !confirmed(c) {
				dryRun(c, args)
				return nil
			}
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				if err := s.ResetTo(ctx, name, tenant, pos); err != nil {
					return err
				}
				fmt.Fprintf(dryRunOut, "OK reset cursor for %s/%s to gp=%d\n", name, tenant, pos)
				auditLog(c, tenant, args)
				return nil
			})
		},
	}
}
