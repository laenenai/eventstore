// eslint-upcasters checks that every event with
// (es.v1.schema_version) > 1 has a hand-written MigratingCodec
// alongside its generated EventCodec, and that the MigratingCodec's
// Decode method explicitly switches on the event's TypeURL.
//
// Per ADR 0013: schema_version > 1 means "the wire bytes for older
// versions of this event still exist in the event log; the codec
// MUST be able to upcast them on read." The framework provides the
// pattern (a wrapper codec around the codegen-emitted EventCodec
// that dispatches on (typeURL, schemaVersion)) but cannot enforce
// it at codegen time without polluting the gen/ tree. This linter
// closes the loop: it walks the proto tree, derives the required
// upcaster set, and fails the build if any case is missing.
//
// Exit codes: 0 success, 1 violations, 2 setup error.
//
// Usage:
//
//	eslint-upcasters [-proto-root proto] [-gen-root gen]
//
// Run from the repository root. Defaults match the framework layout.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func main() {
	protoRoot := flag.String("proto-root", "proto", "root directory containing .proto files")
	genRoot := flag.String("gen-root", "gen", "root directory containing codegen output")
	flag.Parse()

	violations, err := lint(*protoRoot, *genRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "eslint-upcasters: %v\n", err)
		os.Exit(2)
	}
	if len(violations) == 0 {
		return
	}
	fmt.Fprintln(os.Stderr, "eslint-upcasters: schema-evolution discipline violations")
	for _, v := range violations {
		fmt.Fprintf(os.Stderr, "  %s\n", v)
	}
	fmt.Fprintf(os.Stderr, "\n%d violation(s). See ADR 0013 + cookbook recipe 21.\n", len(violations))
	os.Exit(1)
}

// declaration captures one event with schema_version > 1 found in
// the proto tree. The TypeURL is the wire dispatch key the codec
// will see; ExpectedMigratingPath is where the linter looks for
// the hand-written wrapper codec.
type declaration struct {
	ProtoFile            string
	Line                 int
	Package              string // proto package, e.g., "test.unitsmigration.v1"
	MessageName          string // e.g., "MeasurementRecorded"
	SchemaVersion        uint32 // > 1
	GoPackagePath        string // import path from `option go_package = ...`
	ExpectedMigratingPath string // relative path to the expected migrating_codec.go
}

func (d declaration) TypeURL() string {
	return d.Package + "." + d.MessageName
}

func (d declaration) String() string {
	return fmt.Sprintf("%s:%d %s (schema_version=%d)",
		d.ProtoFile, d.Line, d.TypeURL(), d.SchemaVersion)
}

// lint walks the proto root, finds events with schema_version > 1,
// and verifies each has a corresponding entry in a MigratingCodec
// under genRoot.
func lint(protoRoot, genRoot string) ([]string, error) {
	if _, err := os.Stat(protoRoot); err != nil {
		return nil, fmt.Errorf("proto root %q: %w", protoRoot, err)
	}

	var (
		decls []declaration
		walkErr error
	)
	walkErr = filepath.WalkDir(protoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".proto") {
			return nil
		}
		found, err := scanProto(path, genRoot)
		if err != nil {
			return fmt.Errorf("scan %s: %w", path, err)
		}
		decls = append(decls, found...)
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}

	sort.Slice(decls, func(i, j int) bool {
		if decls[i].ProtoFile != decls[j].ProtoFile {
			return decls[i].ProtoFile < decls[j].ProtoFile
		}
		return decls[i].Line < decls[j].Line
	})

	var violations []string
	for _, decl := range decls {
		if v := checkMigratingCodec(decl); v != "" {
			violations = append(violations, v)
		}
	}
	return violations, nil
}

// schemaVersionRe matches `option (es.v1.schema_version) = N;` lines.
var (
	packageRe       = regexp.MustCompile(`^\s*package\s+([\w.]+)\s*;`)
	goPackageRe     = regexp.MustCompile(`option\s+go_package\s*=\s*"([^"]+)"\s*;`)
	messageStartRe  = regexp.MustCompile(`^\s*message\s+(\w+)\s*\{`)
	schemaVersionRe = regexp.MustCompile(`option\s+\(\s*es\.v1\.schema_version\s*\)\s*=\s*(\d+)\s*;`)
)

// scanProto reads one .proto file and returns the events with
// schema_version > 1. The scanner is intentionally simple — it
// tracks brace depth to associate each message with its options.
// Comments are not stripped; both regexes anchor with whitespace +
// keyword to avoid false matches inside // or /* */ comments.
func scanProto(path, genRoot string) ([]declaration, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var (
		out          []declaration
		pkg          string
		goPackagePath string
		curMessage   string
		curMessageStartLine int
		braceDepth   int
		lineno       int
	)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lineno++
		line := sc.Text()
		// Strip line comments to reduce false positives.
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}

		if m := packageRe.FindStringSubmatch(line); m != nil {
			pkg = m[1]
		}
		if m := goPackageRe.FindStringSubmatch(line); m != nil {
			goPackagePath = m[1]
		}

		// Track brace depth to identify "I'm inside message X".
		openCount := strings.Count(line, "{")
		closeCount := strings.Count(line, "}")

		if m := messageStartRe.FindStringSubmatch(line); m != nil && braceDepth == 0 {
			curMessage = m[1]
			curMessageStartLine = lineno
		}
		braceDepth += openCount - closeCount
		if braceDepth < 0 {
			braceDepth = 0
		}

		if curMessage == "" {
			continue
		}
		if m := schemaVersionRe.FindStringSubmatch(line); m != nil {
			v, err := strconv.ParseUint(m[1], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("%s:%d: parse schema_version: %w", path, lineno, err)
			}
			if v > 1 {
				out = append(out, declaration{
					ProtoFile:     path,
					Line:          curMessageStartLine,
					Package:       pkg,
					MessageName:   curMessage,
					SchemaVersion: uint32(v),
					GoPackagePath: goPackagePath,
					ExpectedMigratingPath: migratingCodecPath(goPackagePath, genRoot),
				})
			}
		}

		// Reset curMessage when we drop back to package scope.
		if braceDepth == 0 {
			curMessage = ""
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// migratingCodecPath derives the expected location of migrating_codec.go
// from a proto's go_package option. The framework convention is
// `gen/<path>/migrating_codec.go` where <path> mirrors the proto's
// package hierarchy. The import-path-to-filesystem mapping uses
// genRoot as the override for the framework's "github.com/.../gen"
// prefix.
//
// Example:
//
//	go_package = "github.com/foo/eventstore/gen/test/unitsmigration/v1;unitsmigrationv1"
//	  → genRoot + "/test/unitsmigration/v1/migrating_codec.go"
//
// Returns "" if the import path can't be parsed.
func migratingCodecPath(goPackagePath, genRoot string) string {
	// Trim the package alias suffix ";aliasv1".
	imp := goPackagePath
	if i := strings.Index(imp, ";"); i >= 0 {
		imp = imp[:i]
	}
	idx := strings.Index(imp, "/gen/")
	if idx < 0 {
		return ""
	}
	tail := imp[idx+len("/gen/"):]
	return filepath.Join(genRoot, tail, "migrating_codec.go")
}

// checkMigratingCodec verifies that decl's migrating_codec.go exists
// and contains an explicit case for decl's TypeURL inside the Decode
// method body. Returns "" on success, an error string on violation.
func checkMigratingCodec(decl declaration) string {
	if decl.ExpectedMigratingPath == "" {
		return fmt.Sprintf("%s — cannot derive gen path from go_package %q",
			decl, decl.GoPackagePath)
	}
	contents, err := os.ReadFile(decl.ExpectedMigratingPath)
	if err != nil {
		return fmt.Sprintf("%s — missing %s (write a MigratingCodec; see cookbook 21)",
			decl, decl.ExpectedMigratingPath)
	}
	caseLiteral := `case "` + decl.TypeURL() + `":`
	if !strings.Contains(string(contents), caseLiteral) {
		return fmt.Sprintf("%s — %s does not handle %q (add `%s`)",
			decl, decl.ExpectedMigratingPath, decl.TypeURL(), caseLiteral)
	}
	return ""
}
