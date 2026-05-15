package clonespecv1_test

import (
	"testing"

	"github.com/laenenai/eventstore/es"
	pb "github.com/laenenai/eventstore/gen/test/clonespec/v1"
)

// Comprehensive Clone() deep-copy contract test. The codegen plugin
// emits Clone for every message; this test asserts the contract per
// cardinality × kind combination. Each test mutates the clone and
// asserts the original is unchanged.
//
// The proto source for these messages lives at
// proto/test/clonespec/v1/clonespec.proto. If a new cardinality × kind
// pair is added there, add a matching test here.

func sample() *pb.Spec {
	return &pb.Spec{
		// Singular scalars
		SString: "hello",
		SInt:    42,
		SBool:   true,
		SFloat:  3.14,
		// Singular bytes
		SBytes: []byte("abcd"),
		// Singular message
		SMsg: &pb.Inner{Name: "inner-1", Blob: []byte("ib1"), Value: 7},
		// Repeated scalars
		RStrings: []string{"a", "b"},
		RInts:    []int64{1, 2, 3},
		// Repeated bytes
		RBytes: [][]byte{[]byte("x1"), []byte("x2")},
		// Repeated messages
		RMsgs: []*pb.Inner{
			{Name: "rm-1", Blob: []byte("rm1b")},
			{Name: "rm-2", Blob: []byte("rm2b")},
		},
		// Maps
		MString: map[string]string{"k1": "v1"},
		MInt:    map[string]int64{"a": 10, "b": 20},
		MBytes:  map[string][]byte{"k": []byte("mb")},
		MMsg:    map[string]*pb.Inner{"x": {Name: "mm", Blob: []byte("mmb")}},
		// Oneof — set the bytes variant; other variants tested separately.
		Variant: &pb.Spec_OBytes{OBytes: []byte("oneof-bytes")},
	}
}

func TestClone_Nil(t *testing.T) {
	var s *pb.Spec
	if s.Clone() != nil {
		t.Error("Clone of nil should be nil")
	}
	var inner *pb.Inner
	if inner.Clone() != nil {
		t.Error("Clone of nil inner should be nil")
	}
}

func TestClone_SingularScalars(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	cp.SString = "different"
	cp.SInt = 99
	cp.SBool = false
	cp.SFloat = 9.99
	if orig.SString != "hello" || orig.SInt != 42 || !orig.SBool || orig.SFloat != 3.14 {
		t.Errorf("scalar mutation leaked: %+v", orig)
	}
}

func TestClone_SingularBytes_DeepCopy(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	// Mutate clone's backing array in place.
	cp.SBytes[0] = 'X'
	if orig.SBytes[0] != 'a' {
		t.Errorf("singular bytes ALIASED: orig=%q clone=%q", orig.SBytes, cp.SBytes)
	}
}

func TestClone_SingularBytes_NilAndEmpty(t *testing.T) {
	// Nil source → nil in clone.
	orig := &pb.Spec{}
	if got := orig.Clone().SBytes; got != nil {
		t.Errorf("nil bytes should clone to nil, got %v", got)
	}
	// Empty (len 0, non-nil) source → nil in clone (proto3-equivalent).
	orig.SBytes = []byte{}
	if got := orig.Clone().SBytes; len(got) != 0 {
		t.Errorf("empty bytes should clone to empty, got %v", got)
	}
}

func TestClone_SingularMessage_DeepCopy(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	cp.SMsg.Name = "mutated"
	cp.SMsg.Blob[0] = 'Z'
	if orig.SMsg.Name != "inner-1" {
		t.Errorf("nested message Name aliased: %q", orig.SMsg.Name)
	}
	if orig.SMsg.Blob[0] != 'i' {
		t.Errorf("nested message Blob ALIASED: %q", orig.SMsg.Blob)
	}
}

func TestClone_RepeatedScalars_DeepCopy(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	cp.RStrings[0] = "different"
	cp.RInts[0] = 999
	if orig.RStrings[0] != "a" || orig.RInts[0] != 1 {
		t.Errorf("repeated scalar ALIASED: strings=%v ints=%v", orig.RStrings, orig.RInts)
	}
}

func TestClone_RepeatedBytes_DeepCopy(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	cp.RBytes[0][0] = 'Z' // mutate inner []byte's backing array
	if orig.RBytes[0][0] != 'x' {
		t.Errorf("repeated bytes inner-slice ALIASED: orig=%q clone=%q", orig.RBytes, cp.RBytes)
	}
}

func TestClone_RepeatedMessages_DeepCopy(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	cp.RMsgs[0].Name = "mutated"
	cp.RMsgs[0].Blob[0] = 'Z'
	if orig.RMsgs[0].Name != "rm-1" {
		t.Errorf("repeated message Name aliased: %q", orig.RMsgs[0].Name)
	}
	if orig.RMsgs[0].Blob[0] != 'r' {
		t.Errorf("repeated message Blob ALIASED: %q", orig.RMsgs[0].Blob)
	}
}

func TestClone_MapScalar_DeepCopy(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	cp.MString["k1"] = "different"
	cp.MString["k2"] = "added"
	cp.MInt["a"] = 999
	if orig.MString["k1"] != "v1" {
		t.Errorf("map<string,string> ALIASED: orig=%v", orig.MString)
	}
	if _, ok := orig.MString["k2"]; ok {
		t.Errorf("map<string,string> shares backing map: orig has new key")
	}
	if orig.MInt["a"] != 10 {
		t.Errorf("map<string,int64> ALIASED: orig=%v", orig.MInt)
	}
}

func TestClone_MapBytes_DeepCopy(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	cp.MBytes["k"][0] = 'Z'
	if orig.MBytes["k"][0] != 'm' {
		t.Errorf("map<string,bytes> value ALIASED: orig=%q clone=%q",
			orig.MBytes["k"], cp.MBytes["k"])
	}
}

func TestClone_MapMessage_DeepCopy(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	cp.MMsg["x"].Name = "mutated"
	cp.MMsg["x"].Blob[0] = 'Z'
	if orig.MMsg["x"].Name != "mm" {
		t.Errorf("map<string,Message> Name aliased: %q", orig.MMsg["x"].Name)
	}
	if orig.MMsg["x"].Blob[0] != 'm' {
		t.Errorf("map<string,Message> Blob ALIASED")
	}
}

func TestClone_OneofString_DeepCopy(t *testing.T) {
	orig := &pb.Spec{Variant: &pb.Spec_OStr{OStr: "original"}}
	cp := orig.Clone()
	cp.Variant.(*pb.Spec_OStr).OStr = "mutated"
	if got := orig.Variant.(*pb.Spec_OStr).OStr; got != "original" {
		t.Errorf("oneof string aliased: %q", got)
	}
}

func TestClone_OneofBytes_DeepCopy(t *testing.T) {
	orig := &pb.Spec{Variant: &pb.Spec_OBytes{OBytes: []byte("source")}}
	cp := orig.Clone()
	cp.Variant.(*pb.Spec_OBytes).OBytes[0] = 'Z'
	if got := orig.Variant.(*pb.Spec_OBytes).OBytes[0]; got != 's' {
		t.Errorf("oneof bytes ALIASED: orig=%q", orig.Variant.(*pb.Spec_OBytes).OBytes)
	}
}

func TestClone_OneofMessage_DeepCopy(t *testing.T) {
	orig := &pb.Spec{Variant: &pb.Spec_OMsg{OMsg: &pb.Inner{Name: "n", Blob: []byte("b")}}}
	cp := orig.Clone()
	cp.Variant.(*pb.Spec_OMsg).OMsg.Name = "mutated"
	cp.Variant.(*pb.Spec_OMsg).OMsg.Blob[0] = 'Z'
	origMsg := orig.Variant.(*pb.Spec_OMsg).OMsg
	if origMsg.Name != "n" {
		t.Errorf("oneof message Name aliased: %q", origMsg.Name)
	}
	if origMsg.Blob[0] != 'b' {
		t.Errorf("oneof message Blob ALIASED")
	}
}

// ---- View() deep-copy tests --------------------------------------------
//
// View() has the same deep-copy contract as Clone(), with the added
// behavior of zero-valuing fields above the caller's access level. The
// clonespec proto has no data_classification annotations, so View at
// any level is structurally identical to Clone — these tests focus on
// the deep-copy contract, especially the oneof variants where the
// previous shallow `out.X = m.X` aliased the wrapper struct.

func TestView_SingularBytes_DeepCopy(t *testing.T) {
	orig := sample()
	v := orig.View(es.AccessLevelOperator)
	v.SBytes[0] = 'X'
	if orig.SBytes[0] != 'a' {
		t.Errorf("View singular bytes ALIASED: orig=%q", orig.SBytes)
	}
}

func TestView_RepeatedBytes_DeepCopy(t *testing.T) {
	orig := sample()
	v := orig.View(es.AccessLevelOperator)
	v.RBytes[0][0] = 'Z'
	if orig.RBytes[0][0] != 'x' {
		t.Errorf("View repeated bytes ALIASED: orig=%q", orig.RBytes[0])
	}
}

func TestView_MapBytes_DeepCopy(t *testing.T) {
	orig := sample()
	v := orig.View(es.AccessLevelOperator)
	v.MBytes["k"][0] = 'Z'
	if orig.MBytes["k"][0] != 'm' {
		t.Errorf("View map<string,bytes> ALIASED: orig=%q", orig.MBytes["k"])
	}
}

func TestView_OneofBytes_DeepCopy(t *testing.T) {
	orig := &pb.Spec{Variant: &pb.Spec_OBytes{OBytes: []byte("source")}}
	v := orig.View(es.AccessLevelOperator)
	v.Variant.(*pb.Spec_OBytes).OBytes[0] = 'Z'
	if orig.Variant.(*pb.Spec_OBytes).OBytes[0] != 's' {
		t.Errorf("View oneof bytes ALIASED: orig=%q",
			orig.Variant.(*pb.Spec_OBytes).OBytes)
	}
}

func TestView_OneofMessage_DeepCopy(t *testing.T) {
	orig := &pb.Spec{Variant: &pb.Spec_OMsg{OMsg: &pb.Inner{Name: "n", Blob: []byte("b")}}}
	v := orig.View(es.AccessLevelOperator)
	v.Variant.(*pb.Spec_OMsg).OMsg.Name = "mutated"
	v.Variant.(*pb.Spec_OMsg).OMsg.Blob[0] = 'Z'
	origMsg := orig.Variant.(*pb.Spec_OMsg).OMsg
	if origMsg.Name != "n" {
		t.Errorf("View oneof message Name aliased: %q", origMsg.Name)
	}
	if origMsg.Blob[0] != 'b' {
		t.Errorf("View oneof message Blob ALIASED")
	}
}

func TestView_OneofString_DeepCopy(t *testing.T) {
	orig := &pb.Spec{Variant: &pb.Spec_OStr{OStr: "original"}}
	v := orig.View(es.AccessLevelOperator)
	v.Variant.(*pb.Spec_OStr).OStr = "mutated"
	if got := orig.Variant.(*pb.Spec_OStr).OStr; got != "original" {
		t.Errorf("View oneof string aliased: %q", got)
	}
}

func TestClone_EquivalentValuesAfterCopy(t *testing.T) {
	orig := sample()
	cp := orig.Clone()
	// Spot-check that values are equal pre-mutation.
	if cp.SString != orig.SString || cp.SInt != orig.SInt {
		t.Error("clone scalars differ from source pre-mutation")
	}
	if string(cp.SBytes) != string(orig.SBytes) {
		t.Errorf("clone bytes differ: %q vs %q", cp.SBytes, orig.SBytes)
	}
	if cp.SMsg.Name != orig.SMsg.Name {
		t.Error("clone nested message differs from source")
	}
}
