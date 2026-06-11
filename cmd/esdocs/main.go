// esdocs renders the framework's per-package PII manifests into a
// combined regulator-facing data catalog (JSON) and an optional
// self-contained HTML report.
//
// Subcommands:
//
//	esdocs catalog --gen ./gen --out catalog.json
//	    Walk a directory tree for *_pii_manifest.json files, merge
//	    them into one catalog.json with summary statistics.
//
//	esdocs render --in catalog.json --out report.html
//	    Render a catalog.json into a self-contained HTML report.
//
//	esdocs render --gen ./gen --out report.html
//	    Convenience: do both in one step (no intermediate file).
//
// The tool is stdlib-only by design; it consumes the JSON artefacts
// emitted by cmd/protoc-gen-es-go and produces audit-friendly output.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]

	switch cmd {
	case "catalog":
		if err := runCatalog(args); err != nil {
			fmt.Fprintf(os.Stderr, "esdocs catalog: %v\n", err)
			os.Exit(1)
		}
	case "render":
		if err := runRender(args); err != nil {
			fmt.Fprintf(os.Stderr, "esdocs render: %v\n", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "esdocs: unknown subcommand %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `esdocs — regulator-facing data catalog for the eventstore framework

usage:
  esdocs catalog --gen DIR  --out FILE  [--framework-version VER]
  esdocs render  --in  FILE --out FILE
  esdocs render  --gen DIR  --out FILE  [--framework-version VER]

flags:
  --gen DIR               root directory to walk for *_pii_manifest.json
  --in FILE               an existing catalog.json
  --out FILE              destination (- for stdout)
  --framework-version V   embedded into the catalog (e.g. v0.8.0)
`)
}
