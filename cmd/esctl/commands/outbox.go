package commands

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/urfave/cli/v3"
)

// OutboxCommand assembles `esctl outbox`.
func OutboxCommand() *cli.Command {
	return &cli.Command{
		Name:  "outbox",
		Usage: "Inspect and operate on the outbox drain queue and DLQ",
		Commands: []*cli.Command{
			outboxPendingCommand(),
			outboxDLQCommand(),
			outboxRetryCommand(),
			outboxRetryAllCommand(),
			outboxAbandonCommand(),
		},
	}
}

func outboxPendingCommand() *cli.Command {
	return &cli.Command{
		Name:  "pending",
		Usage: "Counts of pending/failing/DLQ outbox rows",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "max-attempts", Usage: "DLQ threshold (matches your drain config)", Value: 5},
			&cli.BoolFlag{Name: "watch", Aliases: []string{"w"}, Usage: "Refresh on a timer"},
			&cli.DurationFlag{Name: "refresh", Usage: "Watch interval", Value: 2 * time.Second},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for outbox pending")
			}
			r := newRenderer(c)
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				maxAttempts := int32(c.Int("max-attempts"))
				tick := func(ctx context.Context) error {
					pending, err := s.CountPending(ctx, tenant)
					if err != nil {
						return err
					}
					failing, err := s.CountFailing(ctx, tenant, maxAttempts)
					if err != nil {
						return err
					}
					dlq, err := s.CountDLQ(ctx, tenant, maxAttempts)
					if err != nil {
						return err
					}
					if r.Format == "json" {
						return r.json(map[string]int64{
							"pending": pending,
							"failing": failing,
							"dlq":     dlq,
						})
					}
					fmt.Fprintf(r.Out, "%s%-8d%s pending  %s%-8d%s failing  %s%-8d%s DLQ\n",
						r.bold(), pending, r.reset(),
						r.bold(), failing, r.reset(),
						r.bold(), dlq, r.reset())
					return nil
				}
				if c.Bool("watch") {
					return Watch(ctx, c.Duration("refresh"), tick)
				}
				return tick(ctx)
			})
		},
	}
}

func outboxRetryCommand() *cli.Command {
	return &cli.Command{
		Name: "retry",
		Usage: "Reset a single DLQ'd outbox row so the next drain run picks it up. " +
			"Use after fixing the root cause (subscriber redeploy, schema migration, ...).",
		Flags: []cli.Flag{
			&cli.UintFlag{Name: "position", Usage: "Target global_position", Required: true},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for outbox retry")
			}
			pos := uint64(c.Uint("position"))
			args := map[string]any{"position": pos}
			if !confirmed(c) {
				dryRun(c, args)
				return nil
			}
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				if err := s.ReplayDLQ(ctx, tenant, pos); err != nil {
					return err
				}
				fmt.Fprintf(dryRunOut, "OK queued gp=%d for retry (tenant=%s)\n", pos, tenant)
				auditLog(c, tenant, args)
				return nil
			})
		},
	}
}

func outboxRetryAllCommand() *cli.Command {
	return &cli.Command{
		Name: "retry-all",
		Usage: "Reset every DLQ'd row for a tenant. Useful after a publisher outage " +
			"recovery, when the whole DLQ is known to be safe to replay.",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "max-attempts", Usage: "DLQ threshold (matches your drain config)", Value: 5},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for outbox retry-all")
			}
			maxAttempts := int32(c.Int("max-attempts"))
			args := map[string]any{"max-attempts": maxAttempts}
			if !confirmed(c) {
				dryRun(c, args)
				return nil
			}
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				n, err := s.ReplayAllDLQ(ctx, tenant, maxAttempts)
				if err != nil {
					return err
				}
				fmt.Fprintf(dryRunOut, "OK queued %d DLQ row(s) for retry (tenant=%s)\n", n, tenant)
				auditLog(c, tenant, args)
				return nil
			})
		},
	}
}

func outboxAbandonCommand() *cli.Command {
	return &cli.Command{
		Name: "abandon",
		Usage: "Mark a DLQ'd row as abandoned — the publisher will NEVER deliver it. " +
			"The event itself stays in the events table (ADR 0005); only the " +
			"outbox row is closed out. Use for genuinely garbage events.",
		Flags: []cli.Flag{
			&cli.UintFlag{Name: "position", Usage: "Target global_position", Required: true},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for outbox abandon")
			}
			pos := uint64(c.Uint("position"))
			args := map[string]any{"position": pos}
			if !confirmed(c) {
				dryRun(c, args)
				return nil
			}
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				if err := s.AbandonDLQ(ctx, tenant, pos); err != nil {
					return err
				}
				fmt.Fprintf(dryRunOut, "OK abandoned gp=%d (tenant=%s)\n", pos, tenant)
				auditLog(c, tenant, args)
				return nil
			})
		},
	}
}

func outboxDLQCommand() *cli.Command {
	return &cli.Command{
		Name:  "dlq",
		Usage: "List rows in DLQ state",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "max-attempts", Usage: "DLQ threshold", Value: 5},
			&cli.IntFlag{Name: "limit", Usage: "Page size", Value: 50},
			&cli.UintFlag{Name: "after-position", Usage: "Resume cursor", Value: 0},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for outbox dlq")
			}
			r := newRenderer(c)
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				rows, err := s.ListDLQ(ctx, tenant, int32(c.Int("max-attempts")),
					uint64(c.Uint("after-position")), int(c.Int("limit")))
				if err != nil {
					return err
				}
				if r.Format == "json" {
					return r.json(rows)
				}
				for _, row := range rows {
					fmt.Fprintf(r.Out, "gp=%-8d  %s  attempts=%d  enqueued=%s  err=%q\n",
						row.GlobalPosition, row.EventID, row.Attempts,
						row.EnqueuedAt.Format(time.RFC3339),
						truncate(row.LastError, 80))
				}
				return nil
			})
		},
	}
}
