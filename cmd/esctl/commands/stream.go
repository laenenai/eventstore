package commands

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/laenenai/eventstore/es"
)

// StreamCommand assembles the `esctl stream` subtree.
func StreamCommand() *cli.Command {
	return &cli.Command{
		Name:  "stream",
		Usage: "List, read, and verify event streams",
		Commands: []*cli.Command{
			streamListCommand(),
			streamReadCommand(),
			streamVerifyCommand(),
		},
	}
}

func streamListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List streams cached for an aggregate type (uses state_cache)",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "type", Usage: "Aggregate state TypeURL (e.g. myapp.employee.v1.Employee)", Required: true},
			&cli.IntFlag{Name: "limit", Usage: "Max rows", Value: 100},
			&cli.StringFlag{Name: "after", Usage: "Resume cursor (stream_id from previous page)"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			r := newRenderer(c)
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for stream list")
			}
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				rows, err := s.ListStates(ctx, tenant, c.String("type"), c.String("after"), int(c.Int("limit")))
				if err != nil {
					return err
				}
				for _, row := range rows {
					if err := r.StreamSummary(StreamSummary{
						StreamID:      row.StreamID,
						TypeURL:       row.TypeURL,
						Version:       row.Version,
						Terminal:      row.Terminal,
						SchemaVersion: row.StateSchemaVersion,
						UpdatedAt:     row.UpdatedAt,
					}); err != nil {
						return err
					}
				}
				if len(rows) == int(c.Int("limit")) {
					fmt.Fprintf(r.Out, "\n%s(next page: --after %s)%s\n",
						r.dim(), rows[len(rows)-1].StreamID, r.reset())
				}
				return nil
			})
		},
	}
}

func streamReadCommand() *cli.Command {
	return &cli.Command{
		Name:  "read",
		Usage: "Read events from one stream",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "stream", Usage: "Stream id within the tenant (e.g. employee:emp-42)", Required: true},
			&cli.UintFlag{Name: "from-version", Usage: "Start AFTER this version", Value: 0},
			&cli.BoolFlag{Name: "watch", Aliases: []string{"w"}, Usage: "Tail new events"},
			&cli.DurationFlag{Name: "refresh", Usage: "Watch poll interval (min 100ms)", Value: 2 * time.Second},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			r := newRenderer(c)
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for stream read")
			}
			sid, err := parseStreamID(tenant, c.String("stream"))
			if err != nil {
				return err
			}
			fromVersion := uint64(c.Uint("from-version"))

			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				tick := func(ctx context.Context) error {
					envs, err := s.ReadStream(ctx, sid, fromVersion)
					if err != nil {
						return err
					}
					for _, e := range envs {
						if err := r.Envelope(e); err != nil {
							return err
						}
						fromVersion = e.Version
					}
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

func streamVerifyCommand() *cli.Command {
	return &cli.Command{
		Name:  "verify",
		Usage: "Verify the tamper-evident chain (ADR 0028)",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "stream", Usage: "Stream id within the tenant", Required: true},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			r := newRenderer(c)
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for stream verify")
			}
			sid, err := parseStreamID(tenant, c.String("stream"))
			if err != nil {
				return err
			}
			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				envs, err := s.ReadStream(ctx, sid, 0)
				if err != nil {
					return err
				}
				vErr := es.VerifyStreamChain(ctx, s, sid)
				res := VerifyResult{
					StreamID:   sid.Canonical(),
					EventCount: len(envs),
				}
				if vErr == nil {
					res.OK = true
				} else {
					res.OK = false
					res.Message = vErr.Error()
					// Extract version from the wrapped ErrChainBroken if possible.
					if errors.Is(vErr, es.ErrChainBroken) {
						// VerifyStreamChain formats as "%w: version %d";
						// the next bytes after "version " hold the number.
						res.BrokenAt = extractBrokenVersion(vErr.Error())
					}
				}
				return r.Verify(res)
			})
		},
	}
}

// parseStreamID converts a "type:id" form within a tenant to es.StreamID.
func parseStreamID(tenant, streamRef string) (es.StreamID, error) {
	return es.ParseCanonical(tenant, streamRef)
}

// extractBrokenVersion pulls the integer version off a "...: version N"
// suffix produced by VerifyStreamChain. Returns 0 on parse failure.
func extractBrokenVersion(msg string) uint64 {
	const marker = "version "
	i := lastIndex(msg, marker)
	if i < 0 {
		return 0
	}
	n, _ := parseUint(msg[i+len(marker):])
	return n
}

func lastIndex(s, sub string) int {
	for i := len(s) - len(sub); i >= 0; i-- {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func parseUint(s string) (uint64, error) {
	var n uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + uint64(c-'0')
	}
	return n, nil
}

// newRenderer pulls --output / --no-color off the root command.
func newRenderer(c *cli.Command) *Renderer {
	return NewRenderer(c.Root().String("output"), c.Root().Bool("no-color"))
}

// requireTenant returns the per-command --tenant if set, else the root.
func requireTenant(c *cli.Command) string {
	if t := c.String("tenant"); t != "" {
		return t
	}
	return c.Root().String("tenant")
}
