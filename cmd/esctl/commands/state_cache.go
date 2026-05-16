package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v3"
)

// StateCacheCommand assembles `esctl state-cache`. Distinct from
// `esctl state` (which inspects individual rows) because the
// operator-facing rebuild is a destructive bulk operation against the
// whole type, not a single stream.
func StateCacheCommand() *cli.Command {
	return &cli.Command{
		Name:  "state-cache",
		Usage: "Operator actions against the state_cache table",
		Commands: []*cli.Command{
			stateCacheRebuildCommand(),
		},
	}
}

func stateCacheRebuildCommand() *cli.Command {
	return &cli.Command{
		Name: "rebuild",
		Usage: "Wipe state_cache rows for one (tenant, typeURL). The next Load() on " +
			"each affected stream rebuilds via full event replay (ADR 0023). " +
			"NOTE: esctl is generic and has no compiled-in proto types, so it " +
			"cannot run the full aggregate.RebuildStateCache replay path " +
			"directly; that needs your app's typed Runtime. See cookbook " +
			"recipe 08 if you want eager rebuild instead of lazy on-Load.",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "type", Usage: "State TypeURL (e.g. myapp.employee.v1.Employee)", Required: true},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for state-cache rebuild")
			}
			typeURL := c.String("type")
			args := map[string]any{"type": typeURL}
			if !confirmed(c) {
				dryRun(c, args)
				return nil
			}
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				n, err := s.WipeStateCacheForType(ctx, tenant, typeURL)
				if err != nil {
					return err
				}
				fmt.Fprintf(dryRunOut,
					"OK wiped %d state_cache row(s) for type=%s tenant=%s. "+
						"Affected streams repopulate via full-replay on next Load.\n",
					n, typeURL, tenant)
				auditLog(c, tenant, args)
				return nil
			})
		},
	}
}
