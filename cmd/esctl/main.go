// Command esctl is a debugging CLI for the eventstore framework.
// Read-only inspection of streams, events, state_cache, projections,
// and outbox state against any es.Store implementation.
//
// Connects directly via the framework as a library — no admin RPC
// service required. Auto-detects Postgres vs SQLite from the --db URL.
//
//	esctl --db postgres://... stream list --type myapp.employee.v1.Employee
//	esctl --db file:./events.db stream read --stream "employee:emp-42" --watch
//	esctl --db postgres://... events tail --tenant t-1 --watch
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/urfave/cli/v3"

	"github.com/laenenai/eventstore/cmd/esctl/commands"
)

func main() {
	// Load .env (current dir, then walk-up for .env.local) BEFORE the
	// CLI parses flags so env-var-sourced flags (--db, --tenant) pick
	// them up. Missing files are silently ignored — explicit flags
	// always win, then process env, then .env, then defaults.
	_ = godotenv.Load(".env.local")
	_ = godotenv.Load()

	// Cancel on SIGINT / SIGTERM so --watch loops exit cleanly.
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cmd := &cli.Command{
		Name:    "esctl",
		Usage:   "Eventstore debugging CLI — inspect streams, events, state_cache, projections, outbox",
		Version: "0.1.0",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "db",
				Usage:    "DB URL: postgres://user:pass@host/db, or file:./path.db (SQLite). Auto-detects driver from scheme.",
				Sources:  cli.EnvVars("ESCTL_DB"),
				Required: true,
			},
			&cli.StringFlag{
				Name:    "tenant",
				Aliases: []string{"t"},
				Usage:   "Default tenant for subcommands (per-command flag overrides)",
				Sources: cli.EnvVars("ESCTL_TENANT"),
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Output format: pretty, json",
				Sources: cli.EnvVars("ESCTL_OUTPUT"),
				Value:   "pretty",
			},
			&cli.BoolFlag{
				Name:    "no-color",
				Usage:   "Disable ANSI colour in pretty output",
				Sources: cli.EnvVars("ESCTL_NO_COLOR", "NO_COLOR"),
			},
		},
		Commands: []*cli.Command{
			commands.StreamCommand(),
			commands.EventCommand(),
			commands.StateCommand(),
			commands.ProjectionCommand(),
			commands.OutboxCommand(),
			commands.EventsCommand(),
		},
	}

	if err := cmd.Run(ctx, os.Args); err != nil {
		// Distinguish context-cancellation (watch exit) from real errors.
		if ctx.Err() != nil && (err == ctx.Err() || err == context.Canceled) {
			fmt.Fprintln(os.Stderr, "\nesctl: interrupted")
			os.Exit(130)
		}
		fmt.Fprintln(os.Stderr, "esctl:", err)
		os.Exit(1)
	}
}

