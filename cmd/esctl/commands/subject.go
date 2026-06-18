package commands

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/shred"
)

// SubjectCommand assembles the `esctl subject` subtree — operator
// surface for the crypto-shredded subject concept.
//
// `subject export` is the scaffolded DSAR pipeline. It runs
// shred.RunSubjectExport with a built-in "raw triage" inspector that
// emits envelope metadata + base64 of the on-disk payload bytes for
// every envelope passing an optional TypeURL filter — no decoding,
// no decryption. Operators producing a real DSAR plug in an
// application-specific inspector (one per aggregate, codegen-friendly)
// via the framework's shred.SubjectInspector interface; that wiring
// lives in app code, not in this generic CLI.
//
// Use cases for the raw mode:
//   - compliance triage: "show me everything we have on tenant T,
//     within the last N records, for ad-hoc analysis"
//   - bug investigation: dump the raw bytes for a known subject
//     across all streams without standing up a Go program
//
// What raw mode does NOT do:
//   - decrypt PII fields (no per-aggregate codec; ciphertext is
//     opaque to a generic tool)
//   - filter by subject_id (without decoding, we can't reliably
//     identify which envelopes reference the subject; pipe through
//     a stream filter post-hoc — `esctl subject export ... | jq
//     'select(.stream_id | contains("'$SUBJECT'"))'` is the
//     intended pattern)
//
// Future work: ship a codegen-emitted SubjectInspector per aggregate,
// and a registry on esctl that lets operators bind a typed inspector
// to a TypeURL prefix. See examples/conversations/README.md for the
// intended shape.
func SubjectCommand() *cli.Command {
	return &cli.Command{
		Name:  "subject",
		Usage: "Subject-scoped operator commands (DSAR triage, shredding)",
		Commands: []*cli.Command{
			subjectExportCommand(),
		},
	}
}

func subjectExportCommand() *cli.Command {
	return &cli.Command{
		Name:  "export",
		Usage: "Scaffolded DSAR triage export (raw envelopes; pipe through your decoder for real DSAR)",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "subject", Usage: "Subject identifier (DSAR target). Used as a tagging hint on output records; raw mode does not filter on it.", Required: true},
			&cli.StringFlag{Name: "match", Usage: "Only include envelopes whose TypeURL contains this substring (e.g. 'conversation.v1' to scope to one aggregate)"},
			&cli.UintFlag{Name: "from-position", Usage: "Resume cursor: start AFTER this global_position", Value: 0},
			&cli.IntFlag{Name: "limit", Usage: "Max records (0 = unlimited)", Value: 0},
			&cli.IntFlag{Name: "batch-size", Usage: "Read page size", Value: 200},
			&cli.StringFlag{Name: "output-file", Aliases: []string{"f"}, Usage: "Write NDJSON to this path (default: stdout)"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			tenant := requireTenant(c)
			if tenant == "" {
				return errors.New("--tenant is required for subject export")
			}
			subject := c.String("subject")
			if subject == "" {
				return errors.New("--subject is required")
			}
			match := c.String("match")

			out := os.Stdout
			if path := c.String("output-file"); path != "" {
				f, err := os.Create(path)
				if err != nil {
					return fmt.Errorf("open output: %w", err)
				}
				defer f.Close()
				out = f
			}

			return withStore(ctx, c.Root().String("db"), func(ctx context.Context, s Store) error {
				enc := json.NewEncoder(out)

				// Streaming inspector: writes one NDJSON line per record
				// directly rather than buffering. The framework's
				// RunSubjectExport buffers into a slice, which is fine
				// for moderate-size exports but doesn't suit operator
				// triage on a million-row tenant. We hand-roll the
				// scan here using the same SubjectExportSource shape
				// the framework defines.
				inspector := rawTriageInspector{match: match}
				cursor := uint64(c.Uint("from-position"))
				batch := int(c.Int("batch-size"))
				if batch <= 0 {
					batch = 200
				}
				maxRecords := int(c.Int("limit"))
				written := 0
				var last uint64

				for {
					if err := ctx.Err(); err != nil {
						return err
					}
					envs, err := s.ReadAllForTenant(ctx, tenant, cursor, batch)
					if err != nil {
						return fmt.Errorf("read tenant=%s cursor=%d: %w", tenant, cursor, err)
					}
					if len(envs) == 0 {
						break
					}
					for _, env := range envs {
						last = env.GlobalPosition
						payload, err := inspector.Inspect(ctx, env, subject)
						if err != nil {
							return fmt.Errorf("inspect event=%s: %w", env.EventID, err)
						}
						if payload == nil {
							continue
						}
						rec := shred.SubjectExportRecord{
							EventID:        env.EventID.String(),
							TenantID:       env.TenantID,
							StreamID:       env.StreamID.Canonical(),
							TypeURL:        env.TypeURL,
							SchemaVersion:  env.SchemaVersion,
							Version:        env.Version,
							GlobalPosition: env.GlobalPosition,
							OccurredAt:     env.OccurredAt,
							RecordedAt:     env.RecordedAt,
							Payload:        payload,
						}
						if err := enc.Encode(rec); err != nil {
							return fmt.Errorf("encode record: %w", err)
						}
						written++
						if maxRecords > 0 && written >= maxRecords {
							_ = enc.Encode(map[string]any{
								"summary": map[string]any{
									"records":       written,
									"last_position": last,
									"truncated":     true,
								},
							})
							return nil
						}
					}
					cursor = last
					if len(envs) < batch {
						break
					}
				}
				return enc.Encode(map[string]any{
					"summary": map[string]any{
						"records":       written,
						"last_position": last,
						"truncated":     false,
					},
				})
			})
		},
	}
}

// rawTriageInspector emits envelope payload bytes wrapped as a JSON
// object with the payload base64-encoded. Does NOT filter by subject
// — the operator filters downstream. This is the "no codecs wired,
// give me everything for triage" mode; a real DSAR uses an
// application-supplied inspector that decodes and redacts.
type rawTriageInspector struct {
	match string
}

func (r rawTriageInspector) Inspect(_ context.Context, env es.Envelope, _ string) ([]byte, error) {
	if r.match != "" && !strings.Contains(env.TypeURL, r.match) {
		return nil, nil
	}
	out := map[string]string{
		"_note":           "raw triage payload — not decoded, not decrypted",
		"payload_base64":  base64.StdEncoding.EncodeToString(env.Payload),
	}
	return json.Marshal(out)
}
