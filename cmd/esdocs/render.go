package main

import (
	"encoding/json"
	_ "embed"
	"flag"
	"fmt"
	"html/template"
	"io"
	"os"
	"sort"
	"strings"
)

//go:embed template/report.html.tmpl
var reportTemplate string

// runRender implements `esdocs render`. Two modes:
//
//   --in catalog.json --out report.html
//     Render an existing catalog file.
//
//   --gen DIR --out report.html
//     Walk DIR for manifests, build a catalog in memory, render.
//     Convenience for one-step generation.
func runRender(args []string) error {
	fs := flag.NewFlagSet("render", flag.ContinueOnError)
	in := fs.String("in", "", "existing catalog.json (mutually exclusive with --gen)")
	genDir := fs.String("gen", "", "directory to walk for *_pii_manifest.json (mutually exclusive with --in)")
	out := fs.String("out", "", "destination HTML file, or '-' for stdout")
	fwVersion := fs.String("framework-version", "", "framework version string (only used with --gen)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	if (*in == "" && *genDir == "") || (*in != "" && *genDir != "") {
		return fmt.Errorf("exactly one of --in or --gen must be set")
	}

	var cat Catalog
	if *in != "" {
		data, err := os.ReadFile(*in)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &cat); err != nil {
			return fmt.Errorf("parse catalog: %w", err)
		}
	} else {
		c, err := buildCatalog(*genDir, *fwVersion)
		if err != nil {
			return err
		}
		cat = c
	}

	return renderHTML(*out, cat)
}

// renderHTML executes the embedded template against the catalog and
// writes the result to out (or stdout if out=="-"). The template
// produces a single self-contained HTML file: inline CSS, vanilla
// JS, no external assets.
func renderHTML(out string, cat Catalog) error {
	tmpl, err := template.New("report").
		Funcs(templateFuncs()).
		Parse(reportTemplate)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}
	exec := func(w io.Writer) error {
		return tmpl.Execute(w, cat)
	}
	if out == "-" {
		return exec(os.Stdout)
	}
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer f.Close()
	return exec(f)
}

// templateFuncs exposes presentation helpers to the HTML template.
// All helpers are pure and side-effect-free; they handle formatting
// concerns (label shortening, severity coloring, sorting) that don't
// belong on the Catalog struct.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"shortClass":      shortClass,
		"shortEncryption": shortEncryption,
		"classColor":      classColor,
		"checkmark":       checkmark,
		"sortedKeys":      sortedKeys,
		"isPIIClass":      isPIIClass,
		"fieldCount":      func(fs []FieldSpec) int { return len(fs) },
		"piiFieldCount":   countPII,
		"percent":         percent,
		"anchor":          anchor,
	}
}

// percent maps (n, max) → integer 0–100 for use in CSS width
// attributes on histogram bars. Returns 0 when max <= 0 to avoid
// divide-by-zero on empty catalogs.
func percent(n, max int) int {
	if max <= 0 {
		return 0
	}
	return (n * 100) / max
}

// anchor turns a proto package name like "myapp.employee.v1" into an
// HTML-id-safe slug.
func anchor(s string) string {
	r := strings.NewReplacer(".", "-", "/", "-", "_", "-")
	return r.Replace(s)
}

func shortEncryption(s string) string {
	switch s {
	case "subject_string_base64":
		return "str+b64"
	case "subject_bytes":
		return "bytes"
	case "rejected_sad":
		return "REJECTED"
	case "none":
		return "—"
	default:
		return s
	}
}

// classColor maps a short classification name to a CSS class name
// used by the inline stylesheet (defined in the template).
func classColor(short string) string {
	switch short {
	case "PERSONAL", "SENSITIVE", "CARDHOLDER", "CREDENTIAL", "FINANCIAL":
		return "c-pii-high"
	case "QUASI_IDENTIFIER", "UNSTRUCTURED":
		return "c-pii-med"
	case "SAD":
		return "c-sad"
	case "INTERNAL":
		return "c-internal"
	case "PUBLIC":
		return "c-public"
	case "SUBJECT_FIELD":
		return "c-subject"
	default:
		return "c-other"
	}
}

func checkmark(b bool) string {
	if b {
		return "✓"
	}
	return "—"
}

// sortedKeys returns the keys of a string→int map in count-desc,
// name-asc order. Used to render the summary histograms with the
// largest buckets first.
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}
		return keys[i] < keys[j]
	})
	return keys
}

func isPIIClass(c string) bool {
	switch strings.TrimPrefix(c, "DATA_CLASSIFICATION_") {
	case "PERSONAL", "QUASI_IDENTIFIER", "SENSITIVE",
		"FINANCIAL", "CARDHOLDER", "CREDENTIAL", "UNSTRUCTURED":
		return true
	}
	return false
}

func countPII(fs []FieldSpec) int {
	n := 0
	for _, f := range fs {
		if isPIIClass(f.Classification) {
			n++
		}
	}
	return n
}
