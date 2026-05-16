package commands

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
)

// auditOut is the writer that audit log lines go to. Tests override it
// to capture stderr without racing the real process stderr.
var auditOut io.Writer = os.Stderr

// dryRunOut is the writer that DRY RUN lines go to. Tests override it
// to capture stdout output deterministically.
var dryRunOut io.Writer = os.Stdout

// confirmed returns true if the global --yes flag was set. When false,
// callers MUST short-circuit any destructive call and print a DRY RUN
// line instead.
func confirmed(c *cli.Command) bool {
	return c.Root().Bool("yes")
}

// dryRun prints "DRY RUN: would <command> <args...>" to stdout. Used by
// every write command when --yes is absent.
func dryRun(c *cli.Command, args map[string]any) {
	fmt.Fprintf(dryRunOut, "DRY RUN: would %s%s\n",
		commandPath(c), formatArgs(args))
	fmt.Fprintln(dryRunOut, "Re-run with --yes (-y) to execute.")
}

// auditLog emits a structured audit line to stderr. The "[esctl-write]"
// prefix lets operators grep for completed write actions. Called only
// AFTER a successful destructive call.
func auditLog(c *cli.Command, tenant string, args map[string]any) {
	fmt.Fprintf(auditOut, "[esctl-write] %s tenant=%s%s at=%s\n",
		commandPath(c), tenantOrAll(tenant),
		formatArgs(args),
		time.Now().UTC().Format(time.RFC3339Nano))
}

// commandPath walks up the command parents to produce e.g. "projection reset".
// Skips the root (esctl) so audit lines stay terse.
func commandPath(c *cli.Command) string {
	lineage := c.Lineage() // child -> parent -> ... -> root
	parts := make([]string, 0, len(lineage))
	for i := len(lineage) - 1; i >= 0; i-- {
		cmd := lineage[i]
		if cmd.Root() == cmd {
			continue
		}
		parts = append(parts, cmd.Name)
	}
	return strings.Join(parts, " ")
}

// formatArgs renders a stable, sorted "key=val key=val" suffix prefixed
// with a single space. Empty map yields the empty string.
func formatArgs(args map[string]any) string {
	if len(args) == 0 {
		return ""
	}
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=%v", k, args[k])
	}
	return b.String()
}

func tenantOrAll(t string) string {
	if t == "" {
		return "*"
	}
	return t
}
