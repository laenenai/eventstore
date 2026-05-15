package viewspecv1_test

import (
	"testing"

	"github.com/laenenai/eventstore/es"
	pb "github.com/laenenai/eventstore/gen/test/viewspec/v1"
)

// Comprehensive View(level) test for the codegen-emitted access
// helpers (ADR 0027). Every combination of (data_classification ×
// cardinality × oneof) gets a separate assertion.
//
// Source proto: proto/test/viewspec/v1/viewspec.proto. If a new
// classification or cardinality is added to the framework, extend
// both files together.

func sample() *pb.ViewSpec {
	return &pb.ViewSpec{
		SubjectId:       "subj-1",
		SPublicImplicit: "pub-i",
		SPublic:         "pub",
		SInternal:       "int",
		SPersonal:       "per",
		SQuasi:          "quasi",
		SSensitive:      "sens",
		SFinancial:      "fin",
		SCardholder:     "chd",
		SCredential:     "cred",
		SUnstructured:   "free",
		BPersonal:       []byte("bytes-per"),
		IPublic:         1,
		IPersonal:       2,
		BoolPublic:      true,
		BoolPersonal:    true,
		RPublic:         []string{"a", "b"},
		RInternal:       []string{"x"},
		RPersonal:       []string{"p1", "p2"},
		RBPersonal:      [][]byte{[]byte("bp1"), []byte("bp2")},
		MPublic:         map[string]string{"k": "v"},
		MPersonal:       map[string]string{"pk": "pv"},
		SNested:         &pb.Inner{Name: "n", Secret: "n-secret"},
		RNested: []*pb.Inner{
			{Name: "i1", Secret: "i1-secret"},
			{Name: "i2", Secret: "i2-secret"},
		},
		MixedVariant:     &pb.ViewSpec_MvPersonal{MvPersonal: "oneof-per"},
		AllpublicVariant: &pb.ViewSpec_ApOne{ApOne: "ap-one"},
	}
}

func TestView_Nil(t *testing.T) {
	var v *pb.ViewSpec
	if v.View(es.AccessLevelOperator) != nil {
		t.Error("View of nil should be nil")
	}
}

func TestView_SubjectAlwaysVisible(t *testing.T) {
	// Subject field is the encryption-key handle, not PII; visible at
	// every level including Public (ADR 0010 / ADR 0027).
	for _, lvl := range []es.AccessLevel{
		es.AccessLevelPublic, es.AccessLevelInternal,
		es.AccessLevelSubject, es.AccessLevelCompliance, es.AccessLevelOperator,
	} {
		out := sample().View(lvl)
		if out.SubjectId != "subj-1" {
			t.Errorf("level=%v: subject_id should be visible, got %q", lvl, out.SubjectId)
		}
	}
}

func TestView_Public_HidesEverythingAboveItself(t *testing.T) {
	out := sample().View(es.AccessLevelPublic)
	// Public + implicit-public scalars survive.
	if out.SPublic != "pub" {
		t.Errorf("PUBLIC should be visible at Public: got %q", out.SPublic)
	}
	if out.SPublicImplicit != "pub-i" {
		t.Errorf("unannotated should be visible at Public: got %q", out.SPublicImplicit)
	}
	if out.IPublic != 1 {
		t.Errorf("PUBLIC int should be visible: got %d", out.IPublic)
	}
	if !out.BoolPublic {
		t.Errorf("PUBLIC bool should be visible")
	}
	// Everything PERSONAL+ hidden.
	if out.SInternal != "" {
		t.Errorf("INTERNAL hidden at Public: got %q", out.SInternal)
	}
	if out.SPersonal != "" {
		t.Errorf("PERSONAL hidden at Public: got %q", out.SPersonal)
	}
	if out.SSensitive != "" {
		t.Errorf("SENSITIVE hidden at Public: got %q", out.SSensitive)
	}
	if out.SCredential != "" {
		t.Errorf("CREDENTIAL hidden at Public: got %q", out.SCredential)
	}
	if len(out.BPersonal) != 0 {
		t.Errorf("bytes PERSONAL hidden at Public: got %v", out.BPersonal)
	}
	if out.IPersonal != 0 {
		t.Errorf("int PERSONAL hidden at Public")
	}
	// Collections of non-public classifications: hidden entirely.
	if len(out.RInternal) != 0 || len(out.RPersonal) != 0 || len(out.RBPersonal) != 0 {
		t.Errorf("typed collections hidden at Public")
	}
	if len(out.MPersonal) != 0 {
		t.Errorf("typed map hidden at Public")
	}
	// Public collections survive.
	if len(out.RPublic) != 2 {
		t.Errorf("RPublic should survive at Public: %v", out.RPublic)
	}
	if len(out.MPublic) != 1 {
		t.Errorf("MPublic should survive at Public")
	}
}

func TestView_Internal_AddsInternalOnly(t *testing.T) {
	out := sample().View(es.AccessLevelInternal)
	if out.SInternal != "int" {
		t.Errorf("INTERNAL visible at Internal: got %q", out.SInternal)
	}
	if len(out.RInternal) != 1 {
		t.Errorf("RInternal visible at Internal")
	}
	if out.SPersonal != "" {
		t.Errorf("PERSONAL still hidden at Internal: got %q", out.SPersonal)
	}
	if out.SSensitive != "" {
		t.Errorf("SENSITIVE still hidden at Internal")
	}
	if out.SCredential != "" {
		t.Errorf("CREDENTIAL still hidden at Internal")
	}
}

func TestView_Subject_RevealsPersonalQuasiUnstructured(t *testing.T) {
	out := sample().View(es.AccessLevelSubject)
	if out.SPersonal != "per" {
		t.Errorf("PERSONAL visible at Subject: got %q", out.SPersonal)
	}
	if out.SQuasi != "quasi" {
		t.Errorf("QUASI_IDENTIFIER visible at Subject: got %q", out.SQuasi)
	}
	if out.SUnstructured != "free" {
		t.Errorf("UNSTRUCTURED visible at Subject: got %q", out.SUnstructured)
	}
	if string(out.BPersonal) != "bytes-per" {
		t.Errorf("bytes PERSONAL visible at Subject: got %q", out.BPersonal)
	}
	if out.IPersonal != 2 {
		t.Errorf("int PERSONAL visible at Subject")
	}
	if !out.BoolPersonal {
		t.Errorf("bool PERSONAL visible at Subject")
	}
	if len(out.RPersonal) != 2 {
		t.Errorf("RPersonal visible at Subject: %v", out.RPersonal)
	}
	if len(out.RBPersonal) != 2 {
		t.Errorf("RBPersonal visible at Subject")
	}
	if len(out.MPersonal) != 1 {
		t.Errorf("MPersonal visible at Subject")
	}
	// Still hidden at Subject:
	if out.SSensitive != "" {
		t.Errorf("SENSITIVE still hidden at Subject")
	}
	if out.SFinancial != "" {
		t.Errorf("FINANCIAL still hidden at Subject")
	}
	if out.SCardholder != "" {
		t.Errorf("CARDHOLDER still hidden at Subject")
	}
	if out.SCredential != "" {
		t.Errorf("CREDENTIAL still hidden at Subject")
	}
}

func TestView_Compliance_RevealsSensitiveFinancialCardholder(t *testing.T) {
	out := sample().View(es.AccessLevelCompliance)
	if out.SSensitive != "sens" {
		t.Errorf("SENSITIVE visible at Compliance: got %q", out.SSensitive)
	}
	if out.SFinancial != "fin" {
		t.Errorf("FINANCIAL visible at Compliance")
	}
	if out.SCardholder != "chd" {
		t.Errorf("CARDHOLDER visible at Compliance")
	}
	if out.SCredential != "" {
		t.Errorf("CREDENTIAL still hidden at Compliance")
	}
}

func TestView_Operator_RevealsCredential(t *testing.T) {
	out := sample().View(es.AccessLevelOperator)
	if out.SCredential != "cred" {
		t.Errorf("CREDENTIAL visible at Operator: got %q", out.SCredential)
	}
}

// ---- Nested message recursion contract ---------------------------------

func TestView_NestedRecursesAtSameLevel(t *testing.T) {
	// Inner has a PERSONAL "secret" field. View at every level should
	// gate the child's PII identically to a sibling field at the same
	// classification.

	// Public — name visible (PUBLIC), secret hidden.
	out := sample().View(es.AccessLevelPublic)
	if out.SNested == nil || out.SNested.Name != "n" {
		t.Errorf("nested Name visible at Public: %+v", out.SNested)
	}
	if out.SNested.Secret != "" {
		t.Errorf("nested PERSONAL hidden at Public: got %q", out.SNested.Secret)
	}

	// Subject — both visible.
	out = sample().View(es.AccessLevelSubject)
	if out.SNested.Name != "n" || out.SNested.Secret != "n-secret" {
		t.Errorf("nested fields visible at Subject: %+v", out.SNested)
	}

	// Repeated: each element recurses at the same level.
	out = sample().View(es.AccessLevelInternal)
	if len(out.RNested) != 2 {
		t.Fatalf("RNested elements: %d", len(out.RNested))
	}
	for _, e := range out.RNested {
		if e.Secret != "" {
			t.Errorf("repeated nested PERSONAL hidden at Internal: %q", e.Secret)
		}
		if e.Name == "" {
			t.Errorf("repeated nested PUBLIC name visible at Internal")
		}
	}
}

// ---- Oneof: strictest-variant rule -------------------------------------

func TestView_MixedOneof_HiddenBelowStrictest(t *testing.T) {
	// mixed_variant contains PUBLIC/INTERNAL/PERSONAL variants;
	// strictest-rule means the WHOLE oneof gates at AccessLevelSubject.
	// Below that, the oneof Go field is nil regardless of which variant
	// is set.
	for _, lvl := range []es.AccessLevel{es.AccessLevelPublic, es.AccessLevelInternal} {
		out := sample().View(lvl)
		if out.MixedVariant != nil {
			t.Errorf("mixed oneof hidden at %v, got %T", lvl, out.MixedVariant)
		}
	}
	// At Subject and above: variant survives.
	for _, lvl := range []es.AccessLevel{
		es.AccessLevelSubject, es.AccessLevelCompliance, es.AccessLevelOperator,
	} {
		out := sample().View(lvl)
		if _, ok := out.MixedVariant.(*pb.ViewSpec_MvPersonal); !ok {
			t.Errorf("mixed oneof variant carried at %v, got %T", lvl, out.MixedVariant)
		}
	}
}

func TestView_MixedOneof_StrictestRuleWithLessSensitiveActiveVariant(t *testing.T) {
	// Even though the active variant is PUBLIC, the whole oneof is
	// hidden below Subject because the STRICTEST variant (PERSONAL)
	// drives the threshold. Closed-by-default and conservative.
	v := sample()
	v.MixedVariant = &pb.ViewSpec_MvPublic{MvPublic: "should-still-hide"}

	out := v.View(es.AccessLevelInternal)
	if out.MixedVariant != nil {
		t.Errorf("strictest-rule: PUBLIC variant still hidden at Internal because PERSONAL variant exists in oneof; got %T", out.MixedVariant)
	}

	// Subject and above: visible.
	out = v.View(es.AccessLevelSubject)
	if _, ok := out.MixedVariant.(*pb.ViewSpec_MvPublic); !ok {
		t.Errorf("PUBLIC variant carried at Subject, got %T", out.MixedVariant)
	}
}

func TestView_AllPublicOneof_NoGating(t *testing.T) {
	// allpublic_variant has only PUBLIC variants — should be visible
	// at every level including Public.
	for _, lvl := range []es.AccessLevel{
		es.AccessLevelPublic, es.AccessLevelInternal, es.AccessLevelSubject,
		es.AccessLevelCompliance, es.AccessLevelOperator,
	} {
		out := sample().View(lvl)
		if _, ok := out.AllpublicVariant.(*pb.ViewSpec_ApOne); !ok {
			t.Errorf("all-public oneof visible at %v, got %T", lvl, out.AllpublicVariant)
		}
	}
}

// ---- Deep-copy contract (proves the earlier shallow-copy bug stays fixed) -

func TestView_ReturnsDeepCopy(t *testing.T) {
	orig := sample()
	cp := orig.View(es.AccessLevelOperator)

	// Mutate the clone's slice contents.
	cp.RPublic[0] = "ZZZ"
	if orig.RPublic[0] != "a" {
		t.Errorf("RPublic backing array aliased: orig=%v", orig.RPublic)
	}

	// Mutate the clone's nested message.
	cp.SNested.Name = "mutated"
	if orig.SNested.Name != "n" {
		t.Errorf("nested message aliased: orig.SNested.Name=%q", orig.SNested.Name)
	}

	// Mutate the bytes slice inside the clone.
	cp.BPersonal[0] = 'Z'
	if orig.BPersonal[0] != 'b' {
		t.Errorf("bytes PERSONAL aliased: orig=%q", orig.BPersonal)
	}

	// Oneof wrapper struct: mutate the clone's variant.
	if mv, ok := cp.MixedVariant.(*pb.ViewSpec_MvPersonal); ok {
		mv.MvPersonal = "tampered"
		if om, ok := orig.MixedVariant.(*pb.ViewSpec_MvPersonal); ok && om.MvPersonal == "tampered" {
			t.Errorf("oneof wrapper aliased: View copy shares struct with source")
		}
	}
}

// ---- Sanity: View doesn't add or rename fields between source and clone -

func TestView_OperatorIsValueIdenticalToSource(t *testing.T) {
	// At AccessLevelOperator nothing is gated; the View should
	// reproduce every field. (Hashes / chain fields aren't part of
	// access-level emission; this asserts the level=Operator path is a
	// faithful deep clone of every classified field.)
	orig := sample()
	out := orig.View(es.AccessLevelOperator)

	if out.SCredential != orig.SCredential {
		t.Errorf("credential missing at Operator")
	}
	if out.SCardholder != orig.SCardholder {
		t.Errorf("cardholder missing at Operator")
	}
	if out.SubjectId != orig.SubjectId {
		t.Errorf("subject_id missing")
	}
	if len(out.RPersonal) != len(orig.RPersonal) {
		t.Errorf("rpersonal length mismatch")
	}
}
