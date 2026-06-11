package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// runCatalog implements `esdocs catalog`.
func runCatalog(args []string) error {
	fs := flag.NewFlagSet("catalog", flag.ContinueOnError)
	genDir := fs.String("gen", "", "root directory to walk for *_pii_manifest.json")
	out := fs.String("out", "", "destination file path, or '-' for stdout")
	fwVersion := fs.String("framework-version", "", "framework version string embedded in catalog")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *genDir == "" || *out == "" {
		return fmt.Errorf("--gen and --out are required")
	}

	cat, err := buildCatalog(*genDir, *fwVersion)
	if err != nil {
		return err
	}
	return writeCatalog(*out, cat)
}

// buildCatalog walks genDir, loads every *_pii_manifest.json found,
// and assembles a single Catalog with summary statistics.
func buildCatalog(genDir, fwVersion string) (Catalog, error) {
	cat := Catalog{
		SchemaVersion: 1,
		GeneratedAt:   time.Now().UTC().Truncate(time.Second),
		Framework: FrameworkInfo{
			Name:    "github.com/laenenai/eventstore",
			Version: fwVersion,
		},
		Summary: Summary{
			Classifications: map[string]int{},
			EncryptionModes: map[string]int{},
		},
	}

	paths, err := findManifests(genDir)
	if err != nil {
		return Catalog{}, err
	}
	if len(paths) == 0 {
		return Catalog{}, fmt.Errorf("no *_pii_manifest.json found under %s", genDir)
	}

	for _, p := range paths {
		m, err := loadManifest(p)
		if err != nil {
			return Catalog{}, fmt.Errorf("%s: %w", p, err)
		}
		if m.ManifestVersion == 0 {
			cat.Warnings = append(cat.Warnings,
				fmt.Sprintf("%s: v1 manifest (no aggregates/commands) — regenerate with current protoc-gen-es-go", p))
		}
		cat.Packages = append(cat.Packages, m)
	}

	// Stable, deterministic order — by proto package name.
	sort.Slice(cat.Packages, func(i, j int) bool {
		return cat.Packages[i].Package < cat.Packages[j].Package
	})

	cat.Summary = summarize(cat.Packages)
	return cat, nil
}

// findManifests walks root and returns every file matching
// *_pii_manifest.json. Symlinks are not followed.
func findManifests(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), "_pii_manifest.json") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func loadManifest(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("parse: %w", err)
	}
	return m, nil
}

// summarize computes summary statistics across every package.
//
// Definitions:
//   - PIIFieldCount: any field whose classification is one of the
//     encryption-engaging values (PERSONAL, QUASI_IDENTIFIER,
//     SENSITIVE, FINANCIAL, CARDHOLDER, CREDENTIAL, UNSTRUCTURED).
//     Fields with encryption=="rejected_sad" are counted both in
//     SADRejectedCount and (implicitly via classification) under SAD.
//   - Classifications + EncryptionModes: histograms over all fields
//     across all messages (state + commands + events).
func summarize(packages []Manifest) Summary {
	s := Summary{
		PackageCount:    len(packages),
		Classifications: map[string]int{},
		EncryptionModes: map[string]int{},
	}
	piiSet := map[string]bool{
		"DATA_CLASSIFICATION_PERSONAL":         true,
		"DATA_CLASSIFICATION_QUASI_IDENTIFIER": true,
		"DATA_CLASSIFICATION_SENSITIVE":        true,
		"DATA_CLASSIFICATION_FINANCIAL":        true,
		"DATA_CLASSIFICATION_CARDHOLDER":       true,
		"DATA_CLASSIFICATION_CREDENTIAL":       true,
		"DATA_CLASSIFICATION_UNSTRUCTURED":     true,
	}
	count := func(fields []FieldSpec) {
		for _, f := range fields {
			s.FieldCount++
			s.Classifications[shortClass(f.Classification)]++
			s.EncryptionModes[f.Encryption]++
			if piiSet[f.Classification] {
				s.PIIFieldCount++
			}
			if f.Encryption == "rejected_sad" {
				s.SADRejectedCount++
			}
		}
	}
	for _, p := range packages {
		s.AggregateCount += len(p.Aggregates)
		s.CommandCount += len(p.Commands)
		s.EventCount += len(p.Events)
		for _, a := range p.Aggregates {
			count(a.StateFields)
		}
		for _, m := range p.Commands {
			count(m.Fields)
		}
		for _, m := range p.Events {
			count(m.Fields)
		}
	}
	return s
}

// shortClass drops the "DATA_CLASSIFICATION_" prefix for readability
// in summary tallies. The full string is preserved on each FieldSpec.
func shortClass(s string) string {
	return strings.TrimPrefix(s, "DATA_CLASSIFICATION_")
}

func writeCatalog(out string, cat Catalog) error {
	enc := func(w io.Writer) error {
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")
		return e.Encode(cat)
	}
	if out == "-" {
		return enc(os.Stdout)
	}
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer f.Close()
	return enc(f)
}
