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
		switch {
		case f.Message != nil:
			// Message variant — deep-clone the inner value.
			out.P("\t\tif v != nil {")
			out.P("\t\t\tout.", ooGoName, " = &", wrapperName, "{", f.GoName, ": v.", f.GoName, ".Clone()}")
			out.P("\t\t}")
		case f.Desc.Kind() == protoreflect.BytesKind:
			// Bytes variant — []byte needs a deep copy; direct
			// assignment aliases the backing array.
			out.P("\t\tif v != nil {")
			out.P("\t\t\tcp := &", wrapperName, "{}")
			out.P("\t\t\tif len(v.", f.GoName, ") > 0 {")
			out.P("\t\t\t\tcp.", f.GoName, " = append([]byte(nil), v.", f.GoName, "...)")
			out.P("\t\t\t}")
			out.P("\t\t\tout.", ooGoName, " = cp")
			out.P("\t\t}")
		default:
			// Scalar variant (string/int/bool/enum/float) — copy by value.
			out.P("\t\tif v != nil {")
			out.P("\t\t\tout.", ooGoName, " = &", wrapperName, "{", f.GoName, ": v.", f.GoName, "}")
			out.P("\t\t}")
		}
	}
	out.P("\t}")
}

// emitCloneAsSumType writes a CloneSum() <SumIface> method per variant
// that returns the deep copy through the sealed sum interface. The body
// is a one-line delegation to the typed Clone() — no extra allocation,
// no reflection. The point is to give generic callers (the aggregate
// runtime in particular) a uniformly-typed clone method that satisfies
// a `Cloner[E]` interface assertion, so the runtime can sidestep
// proto.Clone for events whose codegen emits this.
//
// Method name: CloneSum (not CloneEvent / CloneCommand) — the codegen
// emits it for every sum type the same way and the runtime asserts the
// generic shape. Naming it after the specific sum would force the
// runtime to know which sum type it is dealing with, which it doesn't.
func emitCloneAsSumType(out *protogen.GeneratedFile, variant *protogen.Message, sumIface string) {
	mName := variant.GoIdent.GoName
	out.P()
	out.P("// CloneSum returns a deep copy of m typed as the sealed ", sumIface, " interface.")
	out.P("// Delegates to the typed Clone(); exists so generic callers can satisfy a")
	out.P("// `Cloner[", sumIface, "]` interface assertion (see aggregate.Runtime).")
	out.P("func (m *", mName, ") CloneSum() ", sumIface, " { return m.Clone() }")
}

// emitCloneField writes the deep-copy of one field. Same shape as
// emitFieldCopyImpl from emit_access.go but without level gating —
// every field is copied unconditionally.
//
// bytes (BytesKind) is the load-bearing edge case: a proto `bytes`
// field becomes `[]byte` in Go, so direct assignment aliases the
// backing array. Both the singular and repeated forms need an
// explicit append-into-fresh-slice. Strings stay safe (Go strings
// are immutable); other scalar kinds are value types.
func emitCloneField(out *protogen.GeneratedFile, af accessField) {
	f := af.field
	goName := af.goName
	isMap := f.Desc.IsMap()
	isRepeated := f.Desc.Cardinality() == protoreflect.Repeated && !isMap
	isMessage := f.Message != nil && !isMap
	isBytes := f.Desc.Kind() == protoreflect.BytesKind

	switch {
	case isMap:
		// map<K, V>: recurse values when message-typed; for bytes
		// values, deep-copy each []byte; otherwise shallow.
		mapVal := f.Message.Fields[1] // entry: key=0, value=1
		out.P("\tif len(m.", goName, ") > 0 {")
		out.P("\t\tout.", goName, " = make(", qualifiedMapType(out, f), ", len(m.", goName, "))")
		out.P("\t\tfor k, v := range m.", goName, " {")
		switch {
		case mapVal.Message != nil:
			out.P("\t\t\tout.", goName, "[k] = v.Clone()")
		case mapVal.Desc.Kind() == protoreflect.BytesKind:
			out.P("\t\t\tif v != nil {")
			out.P("\t\t\t\tout.", goName, "[k] = append([]byte(nil), v...)")
			out.P("\t\t\t}")
		default:
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
	case isRepeated && isBytes:
		// [][]byte — outer slice cloned + each inner []byte deep-copied.
		out.P("\tif len(m.", goName, ") > 0 {")
		out.P("\t\tout.", goName, " = make([][]byte, len(m.", goName, "))")
		out.P("\t\tfor i, b := range m.", goName, " {")
		out.P("\t\t\tif b != nil {")
		out.P("\t\t\t\tout.", goName, "[i] = append([]byte(nil), b...)")
		out.P("\t\t\t}")
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
	case isBytes:
		// Singular []byte — deep copy. Preserves nil-ness: a nil source
		// produces a nil clone field; an empty (len 0, non-nil) source
		// produces a nil clone field, which is equivalent on the wire
		// (proto3 emits neither).
		out.P("\tif len(m.", goName, ") > 0 {")
		out.P("\t\tout.", goName, " = append([]byte(nil), m.", goName, "...)")
		out.P("\t}")
	default:
		// Scalar (string/int/bool/enum/float) — direct assignment.
		// Strings are immutable in Go; numeric/bool/enum are value types.
		out.P("\tout.", goName, " = m.", goName)
	}
}
