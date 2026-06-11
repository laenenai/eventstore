package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixtureManifest = `{
  "manifest_version": 2,
  "source": "test/widget/v1/widget.proto",
  "package": "test.widget.v1",
  "go_package": "example.com/gen/widget/v1",
  "aggregates": [
    {
      "name": "widget",
      "state_message": "test.widget.v1.Widget",
      "subject_field": "widget_id",
      "state_fields": [
        {"name": "widget_id", "proto_type": "string", "classification": "DATA_CLASSIFICATION_SUBJECT_FIELD", "encryption": "none", "dsar_export": true, "audit_on_read": false, "retention": "standard"},
        {"name": "owner_name", "proto_type": "string", "classification": "DATA_CLASSIFICATION_PERSONAL", "encryption": "subject_string_base64", "dsar_export": true, "audit_on_read": false, "retention": "standard"}
      ]
    }
  ],
  "commands": [
    {"name": "test.widget.v1.Create", "fields": [
      {"name": "widget_id", "proto_type": "string", "classification": "DATA_CLASSIFICATION_UNSPECIFIED", "encryption": "none", "dsar_export": true, "audit_on_read": false, "retention": "standard"}
    ]}
  ],
  "events": [
    {"name": "test.widget.v1.Created", "fields": [
      {"name": "widget_id", "proto_type": "string", "classification": "DATA_CLASSIFICATION_SUBJECT_FIELD", "encryption": "none", "dsar_export": true, "audit_on_read": false, "retention": "standard"},
      {"name": "secret", "proto_type": "bytes", "classification": "DATA_CLASSIFICATION_CREDENTIAL", "encryption": "subject_bytes", "dsar_export": false, "audit_on_read": true, "retention": "standard"},
      {"name": "card_pan", "proto_type": "string", "classification": "DATA_CLASSIFICATION_SAD", "encryption": "rejected_sad", "dsar_export": false, "audit_on_read": false, "retention": "standard"}
    ]}
  ]
}`

// TestBuildCatalog runs the full pipeline against a temp directory
// containing one fixture manifest. Verifies the summary tallies and
// that v2 fields round-trip into the catalog unchanged.
func TestBuildCatalog(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "widget_pii_manifest.json"),
		[]byte(fixtureManifest), 0o644); err != nil {
		t.Fatal(err)
	}

	cat, err := buildCatalog(dir, "v1.2.3")
	if err != nil {
		t.Fatalf("buildCatalog: %v", err)
	}

	if cat.Framework.Version != "v1.2.3" {
		t.Errorf("framework version: got %q, want v1.2.3", cat.Framework.Version)
	}
	if len(cat.Packages) != 1 {
		t.Fatalf("packages: got %d, want 1", len(cat.Packages))
	}
	pkg := cat.Packages[0]
	if pkg.Package != "test.widget.v1" {
		t.Errorf("package name: got %q", pkg.Package)
	}
	if pkg.ManifestVersion != 2 {
		t.Errorf("manifest version: got %d, want 2", pkg.ManifestVersion)
	}
	if len(pkg.Aggregates) != 1 || pkg.Aggregates[0].SubjectField != "widget_id" {
		t.Errorf("aggregate / subject_field decoded incorrectly: %+v", pkg.Aggregates)
	}

	s := cat.Summary
	if s.AggregateCount != 1 || s.CommandCount != 1 || s.EventCount != 1 {
		t.Errorf("counts: agg=%d cmd=%d evt=%d", s.AggregateCount, s.CommandCount, s.EventCount)
	}
	// 2 state fields + 1 command field + 3 event fields = 6 total.
	if s.FieldCount != 6 {
		t.Errorf("field count: got %d, want 6", s.FieldCount)
	}
	// PII = PERSONAL (state) + CREDENTIAL (event) = 2.
	if s.PIIFieldCount != 2 {
		t.Errorf("pii field count: got %d, want 2", s.PIIFieldCount)
	}
	if s.SADRejectedCount != 1 {
		t.Errorf("sad-rejected: got %d, want 1", s.SADRejectedCount)
	}
	if s.Classifications["PERSONAL"] != 1 || s.Classifications["CREDENTIAL"] != 1 {
		t.Errorf("classification histogram: %+v", s.Classifications)
	}
}

// TestV1ManifestProducesWarning verifies that a v1-shape manifest
// (no manifest_version field) decodes successfully but appears in
// the warnings list — auditors and operators should see that
// regeneration is needed.
func TestV1ManifestProducesWarning(t *testing.T) {
	dir := t.TempDir()
	v1 := `{"source":"foo","package":"foo.v1","events":[]}`
	if err := os.WriteFile(filepath.Join(dir, "foo_pii_manifest.json"),
		[]byte(v1), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := buildCatalog(dir, "")
	if err != nil {
		t.Fatalf("buildCatalog: %v", err)
	}
	if len(cat.Warnings) != 1 || !strings.Contains(cat.Warnings[0], "v1 manifest") {
		t.Errorf("expected v1 warning, got %+v", cat.Warnings)
	}
}

// TestEmptyDirErrors verifies a clean error when no manifests exist
// under the given root — important so the CLI doesn't silently
// produce an empty catalog.
func TestEmptyDirErrors(t *testing.T) {
	_, err := buildCatalog(t.TempDir(), "")
	if err == nil || !strings.Contains(err.Error(), "no") {
		t.Errorf("expected 'no manifests' error, got %v", err)
	}
}

// TestRenderSmoke runs the full catalog → HTML pipeline against the
// fixture and checks for the structural elements an auditor would
// expect: the package anchor, classification badges, field rows.
func TestRenderSmoke(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "widget_pii_manifest.json"),
		[]byte(fixtureManifest), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := buildCatalog(dir, "v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	outPath := filepath.Join(t.TempDir(), "report.html")
	if err := renderHTML(outPath, cat); err != nil {
		t.Fatalf("renderHTML: %v", err)
	}
	html, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, must := range []string{
		"Data Catalog",
		"id=\"pkg-test-widget-v1\"",
		"DATA_CLASSIFICATION_PERSONAL", // data-class on field row
		"REJECTED",                     // SAD rendering
		"function applyFilter()",       // filter JS present
	} {
		if !bytes.Contains(html, []byte(must)) {
			t.Errorf("rendered HTML missing %q", must)
		}
	}
}

