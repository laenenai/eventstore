package commands

import "github.com/urfave/cli/v3"

// newTestRoot mirrors the *cli.Command tree constructed in main.go.
// Kept in test-only code so it can be invoked by tests for every
// subcommand without dragging in main's signal-handling or godotenv.
func newTestRoot() *cli.Command {
	return &cli.Command{
		Name:    "esctl",
		Version: "test",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "db"},
			&cli.StringFlag{Name: "tenant", Aliases: []string{"t"}},
			&cli.StringFlag{Name: "output", Value: "pretty"},
			&cli.BoolFlag{Name: "no-color"},
			&cli.BoolFlag{Name: "yes", Aliases: []string{"y"}},
		},
		Commands: []*cli.Command{
			StreamCommand(),
			EventCommand(),
			StateCommand(),
			ProjectionCommand(),
			OutboxCommand(),
			EventsCommand(),
			StateCacheCommand(),
		},
	}
}
