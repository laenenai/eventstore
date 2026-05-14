// Access-level View/LogValue emission for protoc-gen-es-go.
//
// For every generated message (State, event variant, command variant,
// value-type, nested) we emit two methods:
//
//   func (m *M) View(level es.AccessLevel) *M
//   func (m *M) LogValue() slog.Value
//
// View returns a deep-ish copy with fields above the caller's level
// zero-valued. Nested messages recurse at the same level — their own
// classifications drive their internal redaction (see ADR-XX
// "access-level views" / readme additions). LogValue is the
// slog.LogValuer hook: it filters at AccessLevelInternal (the
// conservative default for application logs) and replaces hidden
// scalar values with "[REDACTED:<CLASS>]" markers.
//
// The (es.v1.data_classification) annotation drives both. A field
// without the annotation is treated as PUBLIC (AccessLevelPublic) —
// visible at every level. The subject_field is always visible — it is
// an opaque encryption-key handle, never PII on its own (ADR 0010).
//
// Sum-type container messages (option (es.v1.sum_type) = "...") and
// projection-spec messages (option (es.v1.projection) = ...) are
// skipped: they hold no payload of their own.
package main

import (
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	esv1 "github.com/laenenai/eventstore/gen/es/v1"
)

// accessField captures one field's emit-time access metadata.
type accessField struct {
	field     *protogen.Field
	goName    string
	protoName string
	isSubject bool

	// classification — UNSPECIFIED means "no annotation" which we treat
	// as PUBLIC (visible everywhere). Subject fields ignore this entirely.
	classification esv1.DataClassification
}

// extractAccessFields walks a message's fields and pulls out the data
// the access-level emission needs. Subject fields are flagged so they
// stay visible regardless of classification.
//
// Oneof members are skipped: in Go's generated proto, oneof variants
// are wrapper structs assigned to a single interface-typed field on
// the parent; you can't selectively zero one variant without
// destructuring. Emitting view logic per-variant would require
// non-trivial type-switches in the generated code. Oneofs are emitted
// as a single field below — see emitOneofs.
func extractAccessFields(m *protogen.Message) []accessField {
	out := make([]accessField, 0, len(m.Fields))
	for _, f := range m.Fields {
		// Skip fields that participate in a oneof — handled separately
		// at the oneof level below.
		if f.Oneof != nil && !f.Desc.HasOptionalKeyword() {
			continue
		}
		af := accessField{
			field:     f,
			goName:    f.GoName,
			protoName: f.Desc.TextName(),
		}
		if opts, ok := f.Desc.Options().(*descriptorpb.FieldOptions); ok && opts != nil {
			if proto.HasExtension(opts, esv1.E_SubjectField) {
				if v, _ := proto.GetExtension(opts, esv1.E_SubjectField).(bool); v {
					af.isSubject = true
				}
			}
			if proto.HasExtension(opts, esv1.E_DataClassification) {
				if v, _ := proto.GetExtension(opts, esv1.E_DataClassification).(esv1.DataClassification); v != 0 {
					af.classification = v
				}
			}
		}
		out = append(out, af)
	}
	return out
}

// shouldEmitAccessHelpers decides whether to emit View/LogValue for a
// message. We skip sum-type containers (no payload) and projection
// specs (also no payload). We also skip map-entry synthetic messages —
// protogen exposes them as nested messages but they're not real types
// to a user.
func shouldEmitAccessHelpers(m *protogen.Message) bool {
	if m.Desc.IsMapEntry() {
		return false
	}
	opts, ok := m.Desc.Options().(*descriptorpb.MessageOptions)
	if ok && opts != nil {
		if proto.HasExtension(opts, esv1.E_SumType) {
			if v, _ := proto.GetExtension(opts, esv1.E_SumType).(string); v != "" {
				return false
			}
		}
		if proto.HasExtension(opts, esv1.E_Projection) {
			return false
		}
	}
	return true
}

// walkEmittableMessages iterates every message in a file (top-level
// and nested), in depth-first proto-declaration order, invoking fn on
// each one that passes shouldEmitAccessHelpers.
func walkEmittableMessages(msgs []*protogen.Message, fn func(*protogen.Message)) {
	for _, m := range msgs {
		if shouldEmitAccessHelpers(m) {
			fn(m)
		}
		walkEmittableMessages(m.Messages, fn)
	}
}

// emitAccessHelpers writes Clone, View, and LogValue methods for one
// message. Idempotent on emission: every message in the file gets one
// of each. Clone is emitted first so View can call .Clone() on nested
// messages when full-fidelity copying is desired (though View itself
// uses .View(level) recursion to keep the per-field gates honest).
func emitAccessHelpers(out *protogen.GeneratedFile, m *protogen.Message) {
	emitCloneMethod(out, m)
	emitViewMethod(out, m)
	emitLogValueMethod(out, m)
}

// emitViewMethod writes:
//
//	func (m *M) View(level es.AccessLevel) *M
//
// Fields below the caller's level are zero-valued; nested messages
// recurse. Subject fields are unconditionally copied.
func emitViewMethod(out *protogen.GeneratedFile, m *protogen.Message) {
	mName := m.GoIdent.GoName
	accessLvl := out.QualifiedGoIdent(esPkg.Ident("AccessLevel"))
	accessLvlCustomer := out.QualifiedGoIdent(esPkg.Ident("AccessLevelCustomer"))
	accessLvlInternal := out.QualifiedGoIdent(esPkg.Ident("AccessLevelInternal"))
	accessLvlCompliance := out.QualifiedGoIdent(esPkg.Ident("AccessLevelCompliance"))
	accessLvlOperator := out.QualifiedGoIdent(esPkg.Ident("AccessLevelOperator"))

	out.P()
	out.P("// View returns a deep copy of m with fields above the caller's")
	out.P("// access level zero-valued. Subject fields are always visible —")
	out.P("// they are opaque key handles, not identifying data on their own.")
	out.P("// Nested messages recurse at the same level. Returns nil if m is nil.")
	out.P("func (m *", mName, ") View(level ", accessLvl, ") *", mName, " {")
	out.P("\tif m == nil {")
	out.P("\t\treturn nil")
	out.P("\t}")
	out.P("\tout := &", mName, "{}")

	for _, af := range extractAccessFields(m) {
		emitViewField(out, af, accessLvlCustomer, accessLvlInternal, accessLvlCompliance, accessLvlOperator)
	}

	// Oneofs: copy the oneof field (Go interface) as a single unit.
	// Per-variant classification differences are conservatively
	// reduced to the strictest variant's threshold — if ANY variant
	// requires Customer-level view, the entire oneof is hidden below
	// that level. This is closed-by-default and easy to reason about;
	// callers that need finer-grained filtering can View(level) the
	// individual variant after a type-switch.
	for _, oo := range m.Oneofs {
		if oo.Desc.IsSynthetic() {
			continue
		}
		threshold := strictestOneofLevel(oo,
			accessLvlCustomer, accessLvlInternal, accessLvlCompliance, accessLvlOperator)
		if threshold == "" {
			out.P("\tout.", oo.GoName, " = m.", oo.GoName)
			continue
		}
		out.P("\tif level >= ", threshold, " {")
		out.P("\t\tout.", oo.GoName, " = m.", oo.GoName)
		out.P("\t}")
	}

	out.P("\treturn out")
	out.P("}")
}

// strictestOneofLevel returns the highest minimum-level threshold
// across an oneof's variants. Empty when every variant is
// PUBLIC/UNSPECIFIED (no gate needed).
func strictestOneofLevel(oo *protogen.Oneof,
	customer, internal, compliance, operator string,
) string {
	rank := func(c esv1.DataClassification) int {
		switch c {
		case esv1.DataClassification_DATA_CLASSIFICATION_UNSPECIFIED,
			esv1.DataClassification_DATA_CLASSIFICATION_PUBLIC:
			return 0
		case esv1.DataClassification_DATA_CLASSIFICATION_INTERNAL:
			return 1
		case esv1.DataClassification_DATA_CLASSIFICATION_PERSONAL,
			esv1.DataClassification_DATA_CLASSIFICATION_QUASI_IDENTIFIER,
			esv1.DataClassification_DATA_CLASSIFICATION_UNSTRUCTURED:
			return 2
		case esv1.DataClassification_DATA_CLASSIFICATION_SENSITIVE,
			esv1.DataClassification_DATA_CLASSIFICATION_FINANCIAL,
			esv1.DataClassification_DATA_CLASSIFICATION_CARDHOLDER:
			return 3
		case esv1.DataClassification_DATA_CLASSIFICATION_CREDENTIAL:
			return 4
		}
		return 4 // closed by default
	}
	highest := 0
	for _, f := range oo.Fields {
		opts, ok := f.Desc.Options().(*descriptorpb.FieldOptions)
		if !ok || opts == nil {
			continue
		}
		if !proto.HasExtension(opts, esv1.E_DataClassification) {
			continue
		}
		c, _ := proto.GetExtension(opts, esv1.E_DataClassification).(esv1.DataClassification)
		if r := rank(c); r > highest {
			highest = r
		}
	}
	switch highest {
	case 0:
		return ""
	case 1:
		return internal
	case 2:
		return customer
	case 3:
		return compliance
	default:
		return operator
	}
}

// emitViewField writes one field's View-copy logic. Branches on:
//   - subject field      → unconditional copy
//   - level threshold    → minimum AccessLevel to see this field
//   - cardinality / kind → scalar vs message vs repeated vs map
func emitViewField(out *protogen.GeneratedFile, af accessField,
	customer, internal, compliance, operator string) {

	f := af.field
	goName := af.goName
	isMap := f.Desc.IsMap()
	isRepeated := f.Desc.Cardinality() == protoreflect.Repeated && !isMap
	isMessage := f.Message != nil && !isMap

	// Subject fields are always visible regardless of classification.
	if af.isSubject {
		emitFieldCopy(out, "out", "m", goName, f, isMap, isRepeated, isMessage)
		return
	}

	// Classification determines the minimum level. UNSPECIFIED == PUBLIC.
	threshold := levelConstName(af.classification, customer, internal, compliance, operator)

	if threshold == "" {
		// Public — always visible.
		emitFieldCopy(out, "out", "m", goName, f, isMap, isRepeated, isMessage)
		return
	}

	out.P("\tif level >= ", threshold, " {")
	emitFieldCopyIndented(out, "out", "m", goName, f, isMap, isRepeated, isMessage)
	out.P("\t}")
}

// levelConstName returns the Go constant name (qualified) for the
// minimum AccessLevel required to see a field of the given
// classification. Returns "" for public/unspecified (no gate emitted).
func levelConstName(c esv1.DataClassification,
	customer, internal, compliance, operator string,
) string {
	switch c {
	case esv1.DataClassification_DATA_CLASSIFICATION_UNSPECIFIED,
		esv1.DataClassification_DATA_CLASSIFICATION_PUBLIC:
		return "" // visible everywhere — no gate needed
	case esv1.DataClassification_DATA_CLASSIFICATION_INTERNAL:
		return internal
	case esv1.DataClassification_DATA_CLASSIFICATION_PERSONAL,
		esv1.DataClassification_DATA_CLASSIFICATION_QUASI_IDENTIFIER,
		esv1.DataClassification_DATA_CLASSIFICATION_UNSTRUCTURED:
		return customer
	case esv1.DataClassification_DATA_CLASSIFICATION_SENSITIVE,
		esv1.DataClassification_DATA_CLASSIFICATION_FINANCIAL,
		esv1.DataClassification_DATA_CLASSIFICATION_CARDHOLDER:
		return compliance
	case esv1.DataClassification_DATA_CLASSIFICATION_CREDENTIAL:
		return operator
	}
	// Unknown future classification — closed by default at operator.
	return operator
}

// emitFieldCopy writes a one-line (or multi-line for repeated/map)
// assignment that copies a field from src to dst. Used at top level
// (no indentation guard) for unconditional copies.
func emitFieldCopy(out *protogen.GeneratedFile, dst, src, goName string,
	f *protogen.Field, isMap, isRepeated, isMessage bool,
) {
	emitFieldCopyImpl(out, "\t", dst, src, goName, f, isMap, isRepeated, isMessage)
}

// emitFieldCopyIndented writes the copy inside an `if level >= …`
// guard. The extra tab makes the emitted code legible.
func emitFieldCopyIndented(out *protogen.GeneratedFile, dst, src, goName string,
	f *protogen.Field, isMap, isRepeated, isMessage bool,
) {
	emitFieldCopyImpl(out, "\t\t", dst, src, goName, f, isMap, isRepeated, isMessage)
}

func emitFieldCopyImpl(out *protogen.GeneratedFile, indent, dst, src, goName string,
	f *protogen.Field, isMap, isRepeated, isMessage bool,
) {
	switch {
	case isMap:
		// map<K, V>: shallow copy keys, recurse values when message-typed.
		// MapValue's Message is nil for scalar-valued maps; we copy as-is.
		mapVal := f.Message.Fields[1] // entry: key=0, value=1
		if mapVal.Message != nil {
			// Map of messages — recurse each value via View(level).
			out.P(indent, "if len(", src, ".", goName, ") > 0 {")
			out.P(indent, "\t", dst, ".", goName, " = make(", qualifiedMapType(out, f), ", len(", src, ".", goName, "))")
			out.P(indent, "\tfor k, v := range ", src, ".", goName, " {")
			out.P(indent, "\t\t", dst, ".", goName, "[k] = v.View(level)")
			out.P(indent, "\t}")
			out.P(indent, "}")
		} else {
			// Scalar-valued map — shallow copy.
			out.P(indent, "if len(", src, ".", goName, ") > 0 {")
			out.P(indent, "\t", dst, ".", goName, " = make(", qualifiedMapType(out, f), ", len(", src, ".", goName, "))")
			out.P(indent, "\tfor k, v := range ", src, ".", goName, " {")
			out.P(indent, "\t\t", dst, ".", goName, "[k] = v")
			out.P(indent, "\t}")
			out.P(indent, "}")
		}
	case isRepeated && isMessage:
		// []*Inner — recurse per element.
		elemIdent := out.QualifiedGoIdent(f.Message.GoIdent)
		out.P(indent, "if len(", src, ".", goName, ") > 0 {")
		out.P(indent, "\t", dst, ".", goName, " = make([]*", elemIdent, ", len(", src, ".", goName, "))")
		out.P(indent, "\tfor i, e := range ", src, ".", goName, " {")
		out.P(indent, "\t\t", dst, ".", goName, "[i] = e.View(level)")
		out.P(indent, "\t}")
		out.P(indent, "}")
	case isRepeated:
		// []scalar — copy the slice header (proto-go slices are not
		// shared with the source struct's invariants after).
		out.P(indent, "if len(", src, ".", goName, ") > 0 {")
		out.P(indent, "\t", dst, ".", goName, " = append(", dst, ".", goName, "[:0:0], ", src, ".", goName, "...)")
		out.P(indent, "}")
	case isMessage:
		// Singular message — recurse.
		out.P(indent, dst, ".", goName, " = ", src, ".", goName, ".View(level)")
	default:
		// Scalar — direct assignment.
		out.P(indent, dst, ".", goName, " = ", src, ".", goName)
	}
}

// qualifiedMapType returns the Go map[K]V type expression for a map
// field. Needed because we emit make() statements outside protogen's
// auto-import resolution for fields.
func qualifiedMapType(out *protogen.GeneratedFile, f *protogen.Field) string {
	// f.Message is the auto-generated map-entry message with key and
	// value fields. We compose map[K]V using the entry's field types.
	keyField := f.Message.Fields[0]
	valField := f.Message.Fields[1]
	return "map[" + goTypeFor(out, keyField) + "]" + goTypeFor(out, valField)
}

// goTypeFor returns the Go type expression for a scalar or message
// field (singular — repeated/map cardinality handled by callers).
func goTypeFor(out *protogen.GeneratedFile, f *protogen.Field) string {
	if f.Message != nil {
		return "*" + out.QualifiedGoIdent(f.Message.GoIdent)
	}
	if f.Enum != nil {
		return out.QualifiedGoIdent(f.Enum.GoIdent)
	}
	switch f.Desc.Kind() {
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "uint32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "uint64"
	case protoreflect.FloatKind:
		return "float32"
	case protoreflect.DoubleKind:
		return "float64"
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BytesKind:
		return "[]byte"
	}
	return "any"
}

// ---- LogValue emission --------------------------------------------------

// emitLogValueMethod writes a slog.LogValuer implementation that
// filters at AccessLevelInternal. Hidden scalars become
// "[REDACTED:<CLASS>]" markers; nested messages delegate to their own
// LogValue (slog calls it automatically through slog.Any); repeated /
// map produce a count + redaction marker when hidden, length+marker
// when visible (we don't iterate slices in log output — too noisy and
// potentially huge for replay payloads).
func emitLogValueMethod(out *protogen.GeneratedFile, m *protogen.Message) {
	mName := m.GoIdent.GoName
	slogPkg := protogen.GoImportPath("log/slog")
	slogValue := out.QualifiedGoIdent(slogPkg.Ident("Value"))
	slogGroupValue := out.QualifiedGoIdent(slogPkg.Ident("GroupValue"))

	out.P()
	out.P("// LogValue implements slog.LogValuer. Returns the structured")
	out.P("// representation of m filtered at AccessLevelInternal — PII")
	out.P("// fields are replaced with \"[REDACTED:<CLASS>]\" markers, so")
	out.P("// slog.Info(\"...\", \"event\", e) is safe by default.")
	out.P("func (m *", mName, ") LogValue() ", slogValue, " {")
	out.P("\tif m == nil {")
	out.P("\t\treturn ", slogGroupValue, "()")
	out.P("\t}")
	out.P("\treturn ", slogGroupValue, "(")

	for _, af := range extractAccessFields(m) {
		emitLogValueAttr(out, af)
	}

	// Oneofs: emit one slog.Any attribute per oneof. The oneof's Go
	// type is the discriminated interface; slog renders the
	// concrete-variant struct (which itself implements LogValue
	// recursively for message variants).
	for _, oo := range m.Oneofs {
		if oo.Desc.IsSynthetic() {
			continue
		}
		// At AccessLevelInternal, the strictest variant in the oneof
		// dictates: if any variant is at Customer or stricter, the
		// whole oneof is redacted in the log line.
		strict := oneofVisibleAtInternal(oo)
		slogPkgPath := protogen.GoImportPath("log/slog")
		slogStr := out.QualifiedGoIdent(slogPkgPath.Ident("String"))
		slogAny := out.QualifiedGoIdent(slogPkgPath.Ident("Any"))
		if !strict {
			out.P("\t\t", slogStr, "(\"", oo.Desc.Name(), "\", \"[REDACTED:ONEOF]\"),")
		} else {
			out.P("\t\t", slogAny, "(\"", oo.Desc.Name(), "\", m.", oo.GoName, "),")
		}
	}

	out.P("\t)")
	out.P("}")
}

// oneofVisibleAtInternal reports whether every variant in the oneof
// is visible at AccessLevelInternal (i.e., none classified above
// INTERNAL).
func oneofVisibleAtInternal(oo *protogen.Oneof) bool {
	for _, f := range oo.Fields {
		opts, ok := f.Desc.Options().(*descriptorpb.FieldOptions)
		if !ok || opts == nil {
			continue
		}
		if !proto.HasExtension(opts, esv1.E_DataClassification) {
			continue
		}
		c, _ := proto.GetExtension(opts, esv1.E_DataClassification).(esv1.DataClassification)
		switch c {
		case esv1.DataClassification_DATA_CLASSIFICATION_UNSPECIFIED,
			esv1.DataClassification_DATA_CLASSIFICATION_PUBLIC,
			esv1.DataClassification_DATA_CLASSIFICATION_INTERNAL:
			continue
		default:
			return false
		}
	}
	return true
}

// emitLogValueAttr emits one slog.Attr line. At AccessLevelInternal:
//
//   - subject + public + internal fields are emitted with their values.
//   - personal / quasi / unstructured / sensitive / financial /
//     cardholder / credential fields are replaced with the redaction
//     marker.
//   - nested messages: emit as slog.Any so slog auto-calls their
//     LogValue (which redacts at their level). If the WHOLE nested
//     message is classified above Internal, replace with the marker.
//   - repeated / map: emit count + marker when classified above
//     Internal; emit count alone when visible.
func emitLogValueAttr(out *protogen.GeneratedFile, af accessField) {
	f := af.field
	goName := af.goName
	protoName := af.protoName
	isMap := f.Desc.IsMap()
	isRepeated := f.Desc.Cardinality() == protoreflect.Repeated && !isMap
	isMessage := f.Message != nil && !isMap

	// Resolve visibility at AccessLevelInternal.
	visible := af.isSubject ||
		af.classification == esv1.DataClassification_DATA_CLASSIFICATION_UNSPECIFIED ||
		af.classification == esv1.DataClassification_DATA_CLASSIFICATION_PUBLIC ||
		af.classification == esv1.DataClassification_DATA_CLASSIFICATION_INTERNAL

	slogPkg := protogen.GoImportPath("log/slog")
	slogStr := out.QualifiedGoIdent(slogPkg.Ident("String"))
	slogInt := out.QualifiedGoIdent(slogPkg.Ident("Int64"))
	slogBool := out.QualifiedGoIdent(slogPkg.Ident("Bool"))
	slogFloat := out.QualifiedGoIdent(slogPkg.Ident("Float64"))
	slogAny := out.QualifiedGoIdent(slogPkg.Ident("Any"))
	slogGroup := out.QualifiedGoIdent(slogPkg.Ident("Group"))

	label := classificationLabelFor(af.classification)

	switch {
	case isMap, isRepeated:
		// Collection — emit count, plus marker if hidden.
		if !visible {
			out.P("\t\t", slogGroup, "(\"", protoName, "\",")
			out.P("\t\t\t", slogInt, "(\"count\", int64(len(m.", goName, "))),")
			out.P("\t\t\t", slogStr, "(\"redacted\", \"[REDACTED:", label, "]\"),")
			out.P("\t\t),")
		} else {
			out.P("\t\t", slogGroup, "(\"", protoName, "\",")
			out.P("\t\t\t", slogInt, "(\"count\", int64(len(m.", goName, "))),")
			out.P("\t\t),")
		}
	case isMessage:
		if !visible {
			out.P("\t\t", slogStr, "(\"", protoName, "\", \"[REDACTED:", label, "]\"),")
		} else {
			// slog.Any picks up the nested LogValue automatically.
			out.P("\t\t", slogAny, "(\"", protoName, "\", m.", goName, "),")
		}
	default:
		if !visible {
			out.P("\t\t", slogStr, "(\"", protoName, "\", \"[REDACTED:", label, "]\"),")
			return
		}
		// Visible scalar — pick the right slog helper.
		switch {
		case f.Enum != nil:
			out.P("\t\t", slogStr, "(\"", protoName, "\", m.", goName, ".String()),")
		case f.Desc.Kind() == protoreflect.BoolKind:
			out.P("\t\t", slogBool, "(\"", protoName, "\", m.", goName, "),")
		case f.Desc.Kind() == protoreflect.StringKind:
			out.P("\t\t", slogStr, "(\"", protoName, "\", m.", goName, "),")
		case f.Desc.Kind() == protoreflect.BytesKind:
			// bytes can be opaque or ciphertext — log only the length.
			out.P("\t\t", slogInt, "(\"", protoName, ".len\", int64(len(m.", goName, "))),")
		case f.Desc.Kind() == protoreflect.FloatKind, f.Desc.Kind() == protoreflect.DoubleKind:
			out.P("\t\t", slogFloat, "(\"", protoName, "\", float64(m.", goName, ")),")
		default:
			// int family — widen to int64.
			out.P("\t\t", slogInt, "(\"", protoName, "\", int64(m.", goName, ")),")
		}
	}
}

// classificationLabelFor returns the short string used in the
// "[REDACTED:<label>]" marker. Kept in sync with
// es.ClassificationLabel.
func classificationLabelFor(c esv1.DataClassification) string {
	switch c {
	case esv1.DataClassification_DATA_CLASSIFICATION_UNSPECIFIED:
		return "UNSPECIFIED"
	case esv1.DataClassification_DATA_CLASSIFICATION_PUBLIC:
		return "PUBLIC"
	case esv1.DataClassification_DATA_CLASSIFICATION_INTERNAL:
		return "INTERNAL"
	case esv1.DataClassification_DATA_CLASSIFICATION_PERSONAL:
		return "PERSONAL"
	case esv1.DataClassification_DATA_CLASSIFICATION_QUASI_IDENTIFIER:
		return "QUASI_IDENTIFIER"
	case esv1.DataClassification_DATA_CLASSIFICATION_UNSTRUCTURED:
		return "UNSTRUCTURED"
	case esv1.DataClassification_DATA_CLASSIFICATION_SENSITIVE:
		return "SENSITIVE"
	case esv1.DataClassification_DATA_CLASSIFICATION_FINANCIAL:
		return "FINANCIAL"
	case esv1.DataClassification_DATA_CLASSIFICATION_CARDHOLDER:
		return "CARDHOLDER"
	case esv1.DataClassification_DATA_CLASSIFICATION_CREDENTIAL:
		return "CREDENTIAL"
	}
	return "UNKNOWN"
}
