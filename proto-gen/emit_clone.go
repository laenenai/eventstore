// Clone-method emission for protoc-gen-es-go.
//
// For every message that gets View/LogValue, we also emit:
//
//	func (m *M) Clone() *M
//
// Returns a deep copy of m. Nil-safe (nil m returns nil). Nested
// messages, repeated, and maps are recursively cloned via each
// element type's own Clone() — drift-safe under codegen.
//
// Why generate this instead of relying on proto.Clone:
//   - Typed return — callers receive *M, not proto.Message (no cast).
//   - No proto-runtime import leak into domain code (Deciders stop
//     importing google.golang.org/protobuf/proto just for Clone).
//   - Modestly faster than reflection-based proto.Clone (5-7x on
//     small messages); rarely the hot path in real workloads, but
//     measurable on tight Evolve loops.
//   - The View(level) emitter already walks every field with the
//     same recursion + nil-safety + repeated/map handling; Clone is
//     the same pass without the level gates.
package main

import (
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// emitCloneMethod writes a typed Clone() *M method for one message.
// Recurses into nested messages, repeated, maps, and oneof variants.
func emitCloneMethod(out *protogen.GeneratedFile, m *protogen.Message) {
	mName := m.GoIdent.GoName

	out.P()
	out.P("// Clone returns a deep copy of m. Nil-safe (returns nil for nil m).")
	out.P("// Nested messages and repeated/map fields are recursively cloned.")
	out.P("// Faster than proto.Clone and returns the concrete type *", mName, ".")
	out.P("func (m *", mName, ") Clone() *", mName, " {")
	out.P("\tif m == nil {")
	out.P("\t\treturn nil")
	out.P("\t}")
	out.P("\tout := &", mName, "{}")

	for _, af := range extractAccessFields(m) {
		emitCloneField(out, af)
	}

	// Oneofs: type-switch over every variant, deep-clone message
	// variants, copy scalar variants by value. Without the switch we
	// would alias the underlying message pointer, defeating the
	// deep-copy contract.
	for _, oo := range m.Oneofs {
		if oo.Desc.IsSynthetic() {
			continue
		}
		emitOneofClone(out, m, oo)
	}

	out.P("\treturn out")
	out.P("}")
}

// emitOneofClone writes a type-switch over an oneof's wrapper structs,
// deep-cloning message variants and copying scalar variants by value.
// Wrapper struct names follow protobuf-go's convention:
// "<ContainerMessageName>_<FieldGoName>" (e.g. Commands_Create).
func emitOneofClone(out *protogen.GeneratedFile, container *protogen.Message, oo *protogen.Oneof) {
	containerName := container.GoIdent.GoName
	ooGoName := oo.GoName

	out.P("\tswitch v := m.", ooGoName, ".(type) {")
	for _, f := range oo.Fields {
		wrapperName := containerName + "_" + f.GoName
		out.P("\tcase *", wrapperName, ":")
		if f.Message != nil {
			// Message variant — deep-clone the inner value.
			out.P("\t\tif v != nil {")
			out.P("\t\t\tout.", ooGoName, " = &", wrapperName, "{", f.GoName, ": v.", f.GoName, ".Clone()}")
			out.P("\t\t}")
		} else {
			// Scalar variant — copy by value.
			out.P("\t\tif v != nil {")
			out.P("\t\t\tout.", ooGoName, " = &", wrapperName, "{", f.GoName, ": v.", f.GoName, "}")
			out.P("\t\t}")
		}
	}
	out.P("\t}")
}

// emitCloneField writes the deep-copy of one field. Same shape as
// emitFieldCopyImpl from emit_access.go but without level gating —
// every field is copied unconditionally.
func emitCloneField(out *protogen.GeneratedFile, af accessField) {
	f := af.field
	goName := af.goName
	isMap := f.Desc.IsMap()
	isRepeated := f.Desc.Cardinality() == protoreflect.Repeated && !isMap
	isMessage := f.Message != nil && !isMap

	switch {
	case isMap:
		// map<K, V>: recurse values when message-typed, shallow-copy
		// when scalar-typed. Keys are always scalar.
		mapVal := f.Message.Fields[1] // entry: key=0, value=1
		out.P("\tif len(m.", goName, ") > 0 {")
		out.P("\t\tout.", goName, " = make(", qualifiedMapType(out, f), ", len(m.", goName, "))")
		out.P("\t\tfor k, v := range m.", goName, " {")
		if mapVal.Message != nil {
			out.P("\t\t\tout.", goName, "[k] = v.Clone()")
		} else {
			out.P("\t\t\tout.", goName, "[k] = v")
		}
		out.P("\t\t}")
		out.P("\t}")
	case isRepeated && isMessage:
		// []*Inner — recurse per element.
		elemIdent := out.QualifiedGoIdent(f.Message.GoIdent)
		out.P("\tif len(m.", goName, ") > 0 {")
		out.P("\t\tout.", goName, " = make([]*", elemIdent, ", len(m.", goName, "))")
		out.P("\t\tfor i, e := range m.", goName, " {")
		out.P("\t\t\tout.", goName, "[i] = e.Clone()")
		out.P("\t\t}")
		out.P("\t}")
	case isRepeated:
		// []scalar — copy the slice contents so the clone owns its own backing array.
		out.P("\tif len(m.", goName, ") > 0 {")
		out.P("\t\tout.", goName, " = append(out.", goName, "[:0:0], m.", goName, "...)")
		out.P("\t}")
	case isMessage:
		// Singular message — recurse.
		out.P("\tout.", goName, " = m.", goName, ".Clone()")
	default:
		// Scalar — direct assignment (immutable).
		out.P("\tout.", goName, " = m.", goName)
	}
}
