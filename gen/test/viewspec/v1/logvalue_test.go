package viewspecv1_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	pb "github.com/laenenai/eventstore/gen/test/viewspec/v1"
)

// LogValue tests for the codegen-emitted slog.LogValuer (ADR 0027).
// LogValue filters at AccessLevelInternal — PUBLIC, INTERNAL and the
// subject_field render with values; PERSONAL+ render as
// "[REDACTED:<CLASS>]"; collections render as count + redaction marker
// when classified above Internal; nested messages emit via slog.Any
// (which slog auto-resolves through their own LogValue).

// renderJSON runs slog.Info through a JSON handler and returns the
// emitted record's "v" group attrs as a map. This is the realistic
// path users hit when "event" is a slog argument.
func renderJSON(t *testing.T, v slog.LogValuer) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(h)
	logger.Info("test", "v", v)

	// Parse the line; pull the "v" group.
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("parse log line %q: %v", buf.String(), err)
	}
	v_, ok := line["v"].(map[string]any)
	if !ok {
		t.Fatalf("expected v group, got %T: %v", line["v"], line["v"])
	}
	return v_
}

func TestLogValue_Nil(t *testing.T) {
	// nil *ViewSpec is a tricky case — calling LogValue on a nil
	// pointer is what slog does for omitted values; the emitter
	// returns slog.GroupValue() (empty). Should not panic.
	var v *pb.ViewSpec
	got := v.LogValue()
	if got.Kind() != slog.KindGroup {
		t.Errorf("nil should produce empty Group, got kind=%v", got.Kind())
	}
	if len(got.Group()) != 0 {
		t.Errorf("nil group should be empty, got %d attrs", len(got.Group()))
	}
}

func TestLogValue_SubjectIDAlwaysEmitted(t *testing.T) {
	got := renderJSON(t, sample())
	if got["subject_id"] != "subj-1" {
		t.Errorf("subject_id should be visible: %v", got["subject_id"])
	}
}

func TestLogValue_PublicAndInternalScalarsVisible(t *testing.T) {
	got := renderJSON(t, sample())
	if got["s_public"] != "pub" {
		t.Errorf("PUBLIC visible: %v", got["s_public"])
	}
	if got["s_public_implicit"] != "pub-i" {
		t.Errorf("unannotated visible: %v", got["s_public_implicit"])
	}
	if got["s_internal"] != "int" {
		t.Errorf("INTERNAL visible: %v", got["s_internal"])
	}
	// Numeric / bool render via their proper slog helpers.
	if int64(got["i_public"].(float64)) != 1 {
		t.Errorf("int PUBLIC visible as number: %v", got["i_public"])
	}
	if got["bool_public"] != true {
		t.Errorf("bool PUBLIC visible as bool: %v", got["bool_public"])
	}
}

func TestLogValue_PersonalPlusRedacted(t *testing.T) {
	got := renderJSON(t, sample())
	cases := map[string]string{
		"s_personal":     "[REDACTED:PERSONAL]",
		"s_quasi":        "[REDACTED:QUASI_IDENTIFIER]",
		"s_sensitive":    "[REDACTED:SENSITIVE]",
		"s_financial":    "[REDACTED:FINANCIAL]",
		"s_cardholder":   "[REDACTED:CARDHOLDER]",
		"s_credential":   "[REDACTED:CREDENTIAL]",
		"s_unstructured": "[REDACTED:UNSTRUCTURED]",
		"b_personal":     "[REDACTED:PERSONAL]",
		"i_personal":     "[REDACTED:PERSONAL]",
		"bool_personal":  "[REDACTED:PERSONAL]",
	}
	for field, want := range cases {
		if got[field] != want {
			t.Errorf("%s: got %v want %q", field, got[field], want)
		}
	}
}

func TestLogValue_PIIValuesNeverAppearInOutput(t *testing.T) {
	// Stress: build a logger, dump full output, grep for known PII
	// strings. Belt-and-braces against future emitter regressions.
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, nil)
	slog.New(h).Info("test", "v", sample())
	body := buf.String()

	piiNeedles := []string{
		"\"per\"",         // s_personal value
		"\"quasi\"",       // s_quasi value
		"\"sens\"",        // s_sensitive value
		"\"fin\"",         // s_financial value
		"\"chd\"",         // s_cardholder value
		"\"cred\"",        // s_credential value
		"\"free\"",        // s_unstructured value
		"\"oneof-per\"",   // mixed oneof variant value
		"\"n-secret\"",    // nested Inner.Secret
		"\"i1-secret\"",   // nested repeated Inner.Secret
		"bytes-per",       // bytes PERSONAL value (base64-encoded in JSON)
	}
	for _, needle := range piiNeedles {
		if strings.Contains(body, needle) {
			t.Errorf("PII leaked: %q present in log output\n%s", needle, body)
		}
	}
}

func TestLogValue_CollectionsRenderAsCountPlusMarker(t *testing.T) {
	got := renderJSON(t, sample())

	// Public collection: count only, no redaction marker.
	rPublic, _ := got["r_public"].(map[string]any)
	if rPublic == nil {
		t.Fatal("r_public group missing")
	}
	if int64(rPublic["count"].(float64)) != 2 {
		t.Errorf("r_public count: %v", rPublic["count"])
	}
	if _, has := rPublic["redacted"]; has {
		t.Errorf("public collection should NOT have redacted marker")
	}

	// Personal collection: count + redacted marker.
	rPer, _ := got["r_personal"].(map[string]any)
	if rPer == nil {
		t.Fatal("r_personal group missing")
	}
	if int64(rPer["count"].(float64)) != 2 {
		t.Errorf("r_personal count: %v", rPer["count"])
	}
	if rPer["redacted"] != "[REDACTED:PERSONAL]" {
		t.Errorf("r_personal redacted marker: %v", rPer["redacted"])
	}

	// Map of PERSONAL — same shape as repeated PERSONAL.
	mPer, _ := got["m_personal"].(map[string]any)
	if mPer == nil {
		t.Fatal("m_personal group missing")
	}
	if int64(mPer["count"].(float64)) != 1 {
		t.Errorf("m_personal count: %v", mPer["count"])
	}
	if mPer["redacted"] != "[REDACTED:PERSONAL]" {
		t.Errorf("m_personal redacted marker: %v", mPer["redacted"])
	}
}

func TestLogValue_NestedRecursesViaSlogAny(t *testing.T) {
	// SNested is emitted as slog.Any — slog auto-resolves the value's
	// own LogValue, which redacts Inner.Secret. Output should contain
	// Inner.Name (PUBLIC) but not the secret.
	got := renderJSON(t, sample())

	nested, ok := got["s_nested"].(map[string]any)
	if !ok {
		t.Fatalf("s_nested should be a group, got %T: %v", got["s_nested"], got["s_nested"])
	}
	if nested["name"] != "n" {
		t.Errorf("nested PUBLIC name visible: %v", nested["name"])
	}
	if nested["secret"] != "[REDACTED:PERSONAL]" {
		t.Errorf("nested PERSONAL secret redacted: %v", nested["secret"])
	}
}

func TestLogValue_RepeatedNestedAsCountAtParentLevel(t *testing.T) {
	// Repeated messages get the count+marker treatment at the parent
	// level — slog can't recurse into a []*Inner without per-element
	// emission, which would be too verbose.
	got := renderJSON(t, sample())

	rNested, ok := got["r_nested"].(map[string]any)
	if !ok {
		t.Fatalf("r_nested should be a group: %v", got["r_nested"])
	}
	if int64(rNested["count"].(float64)) != 2 {
		t.Errorf("r_nested count: %v", rNested["count"])
	}
}

func TestLogValue_MixedOneofRedactedAsWhole(t *testing.T) {
	// mixed_variant has PERSONAL in its variant set; LogValue filters
	// at Internal, so the whole oneof is replaced with a generic
	// [REDACTED:ONEOF] marker (no variant value leaks regardless of
	// which one is set).
	got := renderJSON(t, sample())
	if got["mixed_variant"] != "[REDACTED:ONEOF]" {
		t.Errorf("mixed_variant: got %v want [REDACTED:ONEOF]", got["mixed_variant"])
	}
}

func TestLogValue_AllPublicOneofVisible(t *testing.T) {
	// allpublic_variant has only PUBLIC variants — emitted via
	// slog.Any so the wrapper struct's value is visible.
	got := renderJSON(t, sample())
	// Wrapper struct serializes as something readable; just confirm
	// it isn't redacted.
	if v := got["allpublic_variant"]; v == "[REDACTED:ONEOF]" {
		t.Errorf("all-public oneof wrongly redacted")
	}
	// Loose contract: presence of the value somewhere in the rendered form.
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("test", "v", sample())
	if !strings.Contains(buf.String(), "ap-one") {
		t.Errorf("all-public oneof value should appear in output:\n%s", buf.String())
	}
}
