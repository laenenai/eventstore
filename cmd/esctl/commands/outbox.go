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
		Usage: "Inspect the outbox drain queue and DLQ",
		Commands: []*cli.Command{
			outboxPendingCommand(),
			outboxDLQCommand(),
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
