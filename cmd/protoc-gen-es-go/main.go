// protoc-gen-es-go is the framework's codegen plugin (ADR 0016).
//
// Invoked by `buf generate`, it produces framework-specific Go code
// from .proto files. This iteration emits sealed-interface sum types
// and Codec implementations driven by the es.v1.sum_type option.
// Typed StreamIDs and decider stubs arrive in subsequent commits.
package main

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	esv1 "github.com/laenenai/eventstore/gen/es/v1"
)

// Import paths the generated code refers to.
var (
	aggregatePkg  = protogen.GoImportPath("github.com/laenenai/eventstore/aggregate")
	protoPkg      = protogen.GoImportPath("google.golang.org/protobuf/proto")
	fmtPkg        = protogen.GoImportPath("fmt")
	contextPkg    = protogen.GoImportPath("context")
	esPkg         = protogen.GoImportPath("github.com/laenenai/eventstore/es")
	projectionPkg = protogen.GoImportPath("github.com/laenenai/eventstore/projection")
)

func main() {
	var opts protogen.Options
	opts.ParamFunc = func(name, value string) error {
		// Accept plugin params from buf.gen.yaml's opt: [k=v] entries.
		// Currently used: runtime=restate / runtime=dbos. Empty (the
		// default) means "emit core codegen".
		return nil
	}
	opts.Run(func(plugin *protogen.Plugin) error {
		// `runtime=restate` (or `=dbos`) selects an alternate emission
		// path. Parsed from CodeGeneratorRequest.Parameter directly so
		// we don't need bespoke flag parsing.
		runtime := pluginParam(plugin, "runtime")

		// Build a cross-file message registry so projection specs
		// (ADR 0020 v2 codegen) can resolve referenced event types
		// against their declaring files. Only needed in core mode.
		var registry map[string]*protogen.Message
		if runtime == "" {
			registry = buildMessageRegistry(plugin)
		}

		for _, file := range plugin.Files {
			if !file.Generate {
				continue
			}
			switch runtime {
			case "":
				if err := generateFile(plugin, file, registry); err != nil {
					return err
				}
			case "restate":
				if err := generateRestateFile(plugin, file); err != nil {
					return err
				}
			case "dbos":
				if err := generateDBOSFile(plugin, file); err != nil {
					return err
				}
			default:
				return fmt.Errorf("protoc-gen-es-go: unknown runtime=%q (supported: restate, dbos)", runtime)
			}
		}
		return nil
	})
}

// isFrameworkPackage reports whether the proto package is one of the
// framework's own type spaces — the View/LogValue emitter must skip
// these to avoid an import cycle against the `es` package (which
// imports gen/es/v1 itself).
func isFrameworkPackage(file *protogen.File) bool {
	pkg := string(file.Desc.Package())
	switch pkg {
	case "es.v1", "eventstore.envelope.v1":
		return true
	}
	return false
}

// pluginParam parses the plugin's `,key=value,…` request parameter
// string. Returns the value for key, or "" when absent.
func pluginParam(plugin *protogen.Plugin, key string) string {
	raw := plugin.Request.GetParameter()
	for _, p := range splitParams(raw) {
		if eq := indexByte(p, '='); eq >= 0 && p[:eq] == key {
			return p[eq+1:]
		}
	}
	return ""
}

func splitParams(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// buildMessageRegistry indexes every message across plugin.Files by
// its proto FullName. Used by emitProjectionSpec to resolve event
// names declared in the (es.v1.projection) option to their
// *protogen.Message — which carries the GoIdent + GoImportPath the
// generated dispatcher needs to reference.
func buildMessageRegistry(plugin *protogen.Plugin) map[string]*protogen.Message {
	reg := map[string]*protogen.Message{}
	for _, f := range plugin.Files {
		walkMessages(f.Messages, reg)
	}
	return reg
}

func walkMessages(msgs []*protogen.Message, reg map[string]*protogen.Message) {
	for _, m := range msgs {
		reg[string(m.Desc.FullName())] = m
		walkMessages(m.Messages, reg) // nested
	}
}

// sumType captures one message annotated `option (es.v1.sum_type) = "X"`,
// holding a oneof whose variants become the sealed-interface variants.
type sumType struct {
	interfaceName string             // e.g., "Command"
	container     *protogen.Message  // wrapper message (e.g., Commands)
	oneof         *protogen.Oneof    // the oneof inside
	variants      []*protogen.Message
}

func generateFile(plugin *protogen.Plugin, file *protogen.File, registry map[string]*protogen.Message) error {
	sumTypes, err := findSumTypes(file)
	if err != nil {
		return err
	}
	projectionSpecs, err := findProjectionSpecs(file, registry)
	if err != nil {
		return err
	}
	// Count messages eligible for access-helper emission (every non-
	// sum-type, non-projection-spec, non-map-entry message). When a
	// file has only value types — neither sum types nor projection
	// specs nor aggregates — we still emit a _es.pb.go for the
	// View/LogValue helpers on those value types.
	//
	// Framework-internal packages (es.v1, eventstore.envelope.v1) are
	// skipped: the helpers reference es.AccessLevel, so emitting them
	// inside the framework's own gen tree would produce an import
	// cycle.
	var accessMsgCount int
	if !isFrameworkPackage(file) {
		walkEmittableMessages(file.Messages, func(*protogen.Message) {
			accessMsgCount++
		})
	}

	if len(sumTypes) == 0 && len(projectionSpecs) == 0 && accessMsgCount == 0 {
		return nil
	}

	out := plugin.NewGeneratedFile(
		file.GeneratedFilenamePrefix+"_es.pb.go",
		file.GoImportPath,
	)
	out.P("// Code generated by protoc-gen-es-go. DO NOT EDIT.")
	out.P("// source: ", file.Desc.Path())
	out.P()
	out.P("package ", file.GoPackageName)
	out.P()

	var eventVariants []*protogen.Message
	for _, st := range sumTypes {
		emitSumType(out, st)
		// Per ADR 0020 decision 3a: emit a Projection interface and
		// dispatcher only for the Event sum type. The Command sum
		// type isn't a projection target.
		if st.interfaceName == "Event" {
			emitProjection(out, st)
			// ADR 0010: crypto-shredding only applies to events
			// (state stays plaintext — readable for "current state"
			// even after shred).
			for _, v := range st.variants {
				if err := emitPIIMethods(out, v); err != nil {
					return err
				}
			}
			eventVariants = append(eventVariants, st.variants...)
		}
	}
	for _, ps := range projectionSpecs {
		emitProjectionSpec(out, ps)
	}

	// Access-level View/LogValue helpers — emitted for every non-sum-
	// type, non-projection-spec message in this file. Framework-
	// internal packages are skipped to avoid an es-package import
	// cycle (see isFrameworkPackage).
	if !isFrameworkPackage(file) {
		walkEmittableMessages(file.Messages, func(m *protogen.Message) {
			emitAccessHelpers(out, m)
		})
	}

	// ADR 0010: emit pii_manifest.json — the audit artifact listing
	// every event's PII classification. Diff-reviewed; acts as the
	// proof of "what's encrypted vs what's not" for privacy review.
	if len(eventVariants) > 0 {
		if err := emitPIIManifest(plugin, file, eventVariants); err != nil {
			return err
		}
	}
	return nil
}

// findSumTypes walks the file's messages and returns those annotated
// with `option (es.v1.sum_type) = "Name"`. Returns an error for
// malformed annotations (multiple oneofs, non-message variants).
func findSumTypes(file *protogen.File) ([]*sumType, error) {
	var out []*sumType
	for _, msg := range file.Messages {
		name, ok := extractSumTypeName(msg)
		if !ok {
			continue
		}
		if len(msg.Oneofs) != 1 {
			return nil, fmt.Errorf(
				"%s: es.v1.sum_type requires exactly one oneof, found %d",
				msg.GoIdent.GoName, len(msg.Oneofs),
			)
		}
		st := &sumType{
			interfaceName: name,
			container:     msg,
			oneof:         msg.Oneofs[0],
		}
		for _, field := range msg.Oneofs[0].Fields {
			if field.Message == nil {
				return nil, fmt.Errorf(
					"%s.%s: sum_type oneof variants must be message types",
					msg.GoIdent.GoName, field.GoName,
				)
			}
			st.variants = append(st.variants, field.Message)
		}
		out = append(out, st)
	}
	return out, nil
}

// extractSumTypeName returns the value of (es.v1.sum_type) if set, "" otherwise.
func extractSumTypeName(msg *protogen.Message) (string, bool) {
	opts, ok := msg.Desc.Options().(*descriptorpb.MessageOptions)
	if !ok || opts == nil {
		return "", false
	}
	if !proto.HasExtension(opts, esv1.E_SumType) {
		return "", false
	}
	name, _ := proto.GetExtension(opts, esv1.E_SumType).(string)
	return name, name != ""
}

// getSchemaVersion returns the value of (es.v1.schema_version) on a
// variant message; defaults to 1.
func getSchemaVersion(msg *protogen.Message) uint32 {
	opts, ok := msg.Desc.Options().(*descriptorpb.MessageOptions)
	if !ok || opts == nil {
		return 1
	}
	if !proto.HasExtension(opts, esv1.E_SchemaVersion) {
		return 1
	}
	v, _ := proto.GetExtension(opts, esv1.E_SchemaVersion).(uint32)
	if v == 0 {
		return 1
	}
	return v
}

// emitSumType writes the sealed interface, marker methods, and Codec
// for one sum type.
func emitSumType(out *protogen.GeneratedFile, st *sumType) {
	iface := st.interfaceName
	marker := "is" + iface

	// ----- Sealed interface ---------------------------------------------
	out.P("// ", iface, " is the sealed interface for the ", st.container.GoIdent.GoName, " sum type.")
	out.P("// Variants implement this via the unexported ", marker, "() marker method.")
	out.P("type ", iface, " interface {")
	out.P("\t", marker, "()")
	out.P("}")
	out.P()

	// ----- Variant marker methods ---------------------------------------
	for _, v := range st.variants {
		out.P("func (*", v.GoIdent.GoName, ") ", marker, "() {}")
	}
	out.P()

	// ----- CloneSum() <iface> ------------------------------------------
	// One-line wrappers around each variant's typed Clone(). Lets the
	// aggregate runtime use a `Cloner[E]` interface assertion to invoke
	// the codegen-emitted deep-copy without going through reflection-
	// based proto.Clone. Strictly additive; the typed Clone() *T method
	// remains the primary API.
	for _, v := range st.variants {
		emitCloneAsSumType(out, v, iface)
	}
	out.P()

	// ----- Action() string ----------------------------------------------
	// Stable per-variant identifier — the variant's full proto type
	// name (e.g., "myapp.party.v1.Approve"). Useful for authz wrappers,
	// metrics labels, trace span names, audit log enrichment, and
	// anywhere else a stable per-command-type identifier is needed.
	// Emitted for events too; semantically it's the event's type name.
	out.P("// Stable per-variant identifiers — full proto type names.")
	out.P("// Useful for authz, metrics, tracing, and audit annotation.")
	for _, v := range st.variants {
		out.P("func (*", v.GoIdent.GoName, ") Action() string { return \"", v.Desc.FullName(), "\" }")
	}
	out.P()

	// ----- Codec --------------------------------------------------------
	codec := iface + "Codec"
	encodedEvent := out.QualifiedGoIdent(aggregatePkg.Ident("EncodedEvent"))
	codecIface := out.QualifiedGoIdent(aggregatePkg.Ident("Codec"))
	protoMarshal := out.QualifiedGoIdent(protoPkg.Ident("Marshal"))
	protoUnmarshal := out.QualifiedGoIdent(protoPkg.Ident("Unmarshal"))
	fmtErrorf := out.QualifiedGoIdent(fmtPkg.Ident("Errorf"))

	out.P("// ", codec, " encodes and decodes ", iface, " variants on the wire.")
	out.P("// Implements aggregate.Codec[", iface, "].")
	out.P("type ", codec, " struct{}")
	out.P()
	out.P("// Compile-time assertion that the codec satisfies the framework contract.")
	out.P("var _ ", codecIface, "[", iface, "] = ", codec, "{}")
	out.P()

	// Encode
	out.P("// Encode marshals v as proto bytes and produces an EncodedEvent")
	out.P("// tagged with the variant's full proto type name.")
	out.P("func (", codec, ") Encode(v ", iface, ") (", encodedEvent, ", error) {")
	out.P("\tswitch x := v.(type) {")
	for _, v := range st.variants {
		out.P("\tcase *", v.GoIdent.GoName, ":")
		out.P("\t\tb, err := ", protoMarshal, "(x)")
		out.P("\t\tif err != nil {")
		out.P("\t\t\treturn ", encodedEvent, "{}, err")
		out.P("\t\t}")
		out.P("\t\treturn ", encodedEvent, "{")
		out.P("\t\t\tPayload:       b,")
		out.P("\t\t\tTypeURL:       \"", v.Desc.FullName(), "\",")
		out.P("\t\t\tSchemaVersion: ", getSchemaVersion(v), ",")
		out.P("\t\t}, nil")
	}
	out.P("\t}")
	out.P("\treturn ", encodedEvent, "{}, ", fmtErrorf, "(\"", iface, "Codec.Encode: unknown variant type %T\", v)")
	out.P("}")
	out.P()

	// Decode
	out.P("// Decode unmarshals the proto payload back into the variant identified")
	out.P("// by typeURL. The schemaVersion is passed through for upcaster")
	out.P("// dispatch (ADR 0013); this generated codec does not perform upcasting.")
	out.P("func (", codec, ") Decode(typeURL string, _ uint32, payload []byte) (", iface, ", error) {")
	out.P("\tswitch typeURL {")
	for _, v := range st.variants {
		out.P("\tcase \"", v.Desc.FullName(), "\":")
		out.P("\t\tx := &", v.GoIdent.GoName, "{}")
		out.P("\t\tif err := ", protoUnmarshal, "(payload, x); err != nil {")
		out.P("\t\t\treturn nil, err")
		out.P("\t\t}")
		out.P("\t\treturn x, nil")
	}
	out.P("\t}")
	out.P("\treturn nil, ", fmtErrorf, "(\"", iface, "Codec.Decode: unknown type_url %q\", typeURL)")
	out.P("}")
	out.P()
}

// emitProjection writes the Projection interface and dispatcher for an
// Event sum type. See ADR 0020 decision 3a (exhaustive typed interface)
// and 3b (default-error, IgnoreUnknown opt-in).
func emitProjection(out *protogen.GeneratedFile, st *sumType) {
	contextType := out.QualifiedGoIdent(contextPkg.Ident("Context"))
	envelope := out.QualifiedGoIdent(esPkg.Ident("Envelope"))
	handler := out.QualifiedGoIdent(projectionPkg.Ident("Handler"))
	dispOption := out.QualifiedGoIdent(projectionPkg.Ident("DispatcherOption"))
	applyOptions := out.QualifiedGoIdent(projectionPkg.Ident("ApplyOptions"))
	fmtErrorf := out.QualifiedGoIdent(fmtPkg.Ident("Errorf"))
	codec := st.interfaceName + "Codec"

	out.P("// Projection is the typed handler interface for the events in")
	out.P("// this package. Implementations must provide a method for every")
	out.P("// variant — adding a new event will break implementations at")
	out.P("// compile time until they decide how to handle it (return nil")
	out.P("// is fine for variants the projection deliberately ignores).")
	out.P("// See ADR 0020 decisions 3a + 3b.")
	out.P("type Projection interface {")
	for _, v := range st.variants {
		out.P("\tOn", v.GoIdent.GoName, "(ctx ", contextType, ", env ", envelope, ", e *", v.GoIdent.GoName, ") error")
	}
	out.P("}")
	out.P()

	out.P("// NewProjectionDispatcher returns a projection.Handler that decodes")
	out.P("// envelopes carrying this package's events and dispatches to the")
	out.P("// typed Projection interface. By default an event whose TypeURL is")
	out.P("// outside this aggregate's event set causes a non-nil error; use")
	out.P("// projection.IgnoreUnknown() when composing across aggregates via")
	out.P("// projection.Chain.")
	out.P("func NewProjectionDispatcher(p Projection, opts ...", dispOption, ") ", handler, " {")
	out.P("\tcfg := ", applyOptions, "(opts)")
	out.P("\tvar codec ", codec)
	out.P("\treturn func(ctx ", contextType, ", env ", envelope, ") error {")
	out.P("\t\tif !isOurType(env.TypeURL) {")
	out.P("\t\t\tif cfg.IgnoreUnknown {")
	out.P("\t\t\t\treturn nil")
	out.P("\t\t\t}")
	out.P("\t\t\treturn ", fmtErrorf, "(\"projection: unknown TypeURL %q for package ", st.container.Desc.ParentFile().Package(), "\", env.TypeURL)")
	out.P("\t\t}")
	out.P("\t\tevt, err := codec.Decode(env.TypeURL, env.SchemaVersion, env.Payload)")
	out.P("\t\tif err != nil {")
	out.P("\t\t\treturn ", fmtErrorf, "(\"projection: decode %s: %w\", env.TypeURL, err)")
	out.P("\t\t}")
	out.P("\t\tswitch e := evt.(type) {")
	for _, v := range st.variants {
		out.P("\t\tcase *", v.GoIdent.GoName, ":")
		out.P("\t\t\treturn p.On", v.GoIdent.GoName, "(ctx, env, e)")
	}
	out.P("\t\t}")
	out.P("\t\treturn ", fmtErrorf, "(\"projection: unreachable variant %T\", evt)")
	out.P("\t}")
	out.P("}")
	out.P()

	// isOurType — package-level helper used by the dispatcher to
	// recognize TypeURLs belonging to this aggregate. Emitted as a
	// switch over the known variants.
	out.P("// isOurType reports whether the given TypeURL is one of this")
	out.P("// package's event variants. Used by the dispatcher to decide")
	out.P("// between dispatch and (skip|error) for unknown events.")
	out.P("func isOurType(typeURL string) bool {")
	out.P("\tswitch typeURL {")
	for _, v := range st.variants {
		out.P("\tcase \"", v.Desc.FullName(), "\":")
		out.P("\t\treturn true")
	}
	out.P("\t}")
	out.P("\treturn false")
	out.P("}")
	out.P()
}

// projectionSpec is one (es.v1.projection)-annotated message. It binds
// a stable projection name + a list of events to a Go interface +
// dispatcher emitted in the annotated message's package.
type projectionSpec struct {
	host   *protogen.Message   // the annotated message
	name   string              // value of Projection.name
	events []*protogen.Message // resolved variant types
}

// findProjectionSpecs scans the file for messages carrying the
// (es.v1.projection) option, resolves each event name against the
// cross-file registry, and returns the validated specs.
//
// Errors:
//   - empty name
//   - empty events list
//   - event name not present in plugin.Files (typo, missing import)
//   - duplicate method names after derivation (resolve in proto)
func findProjectionSpecs(file *protogen.File, registry map[string]*protogen.Message) ([]*projectionSpec, error) {
	var out []*projectionSpec
	for _, msg := range file.Messages {
		opts, ok := msg.Desc.Options().(*descriptorpb.MessageOptions)
		if !ok || opts == nil {
			continue
		}
		if !proto.HasExtension(opts, esv1.E_Projection) {
			continue
		}
		raw := proto.GetExtension(opts, esv1.E_Projection)
		spec, _ := raw.(*esv1.Projection)
		if spec == nil {
			continue
		}
		if spec.Name == "" {
			return nil, fmt.Errorf("%s: (es.v1.projection).name is required",
				msg.GoIdent.GoName)
		}
		if len(spec.Events) == 0 {
			return nil, fmt.Errorf("%s: (es.v1.projection).events list cannot be empty",
				msg.GoIdent.GoName)
		}
		ps := &projectionSpec{host: msg, name: spec.Name}
		seenMethod := map[string]string{} // methodName -> originating event
		for _, evtName := range spec.Events {
			eventMsg, found := registry[evtName]
			if !found {
				return nil, fmt.Errorf(
					"%s: (es.v1.projection).events references unknown type %q "+
						"(missing import in %s?)",
					msg.GoIdent.GoName, evtName, file.Desc.Path())
			}
			methodName := "On" + eventMsg.GoIdent.GoName
			if prev, dup := seenMethod[methodName]; dup {
				return nil, fmt.Errorf(
					"%s: projection method name %q collides — produced by both %q and %q. "+
						"Rename one of the proto types or split into separate projections.",
					msg.GoIdent.GoName, methodName, prev, evtName)
			}
			seenMethod[methodName] = evtName
			ps.events = append(ps.events, eventMsg)
		}
		out = append(out, ps)
	}
	return out, nil
}

// emitProjectionSpec writes a typed Projection interface and dispatcher
// for one (es.v1.projection) annotation. Referenced event types may
// live in other Go packages — protogen's QualifiedGoIdent handles the
// import generation automatically.
func emitProjectionSpec(out *protogen.GeneratedFile, ps *projectionSpec) {
	hostName := ps.host.GoIdent.GoName
	ifaceName := hostName + "Handler" // host message struct already takes hostName
	contextType := out.QualifiedGoIdent(contextPkg.Ident("Context"))
	envelope := out.QualifiedGoIdent(esPkg.Ident("Envelope"))
	handler := out.QualifiedGoIdent(projectionPkg.Ident("Handler"))
	dispOption := out.QualifiedGoIdent(projectionPkg.Ident("DispatcherOption"))
	applyOptions := out.QualifiedGoIdent(projectionPkg.Ident("ApplyOptions"))
	protoUnmarshal := out.QualifiedGoIdent(protoPkg.Ident("Unmarshal"))
	fmtErrorf := out.QualifiedGoIdent(fmtPkg.Ident("Errorf"))

	// Name constant — stable string for projection.Runtime.Name.
	out.P("// ", hostName, "Name is the stable projection name from the")
	out.P("// (es.v1.projection) annotation on ", hostName, ".")
	out.P("const ", hostName, "Name = \"", ps.name, "\"")
	out.P()

	// Interface
	out.P("// ", ifaceName, " is the typed handler for projection \"", ps.name, "\".")
	out.P("// Implementations provide a method for each event in the spec —")
	out.P("// adding or removing an event in the .proto produces a compile-")
	out.P("// time gap. See ADR 0020 (v2 proto-driven codegen).")
	out.P("type ", ifaceName, " interface {")
	for _, evt := range ps.events {
		methodName := "On" + evt.GoIdent.GoName
		evtIdent := out.QualifiedGoIdent(evt.GoIdent)
		out.P("\t", methodName, "(ctx ", contextType, ", env ", envelope, ", e *", evtIdent, ") error")
	}
	out.P("}")
	out.P()

	// Dispatcher — constructor name matches the host message, not the
	// interface, so users write New<MessageName>Dispatcher rather than
	// New<MessageName>HandlerDispatcher.
	out.P("// New", hostName, "Dispatcher returns a projection.Handler that decodes")
	out.P("// the events declared in this projection's spec and dispatches to")
	out.P("// the typed ", ifaceName, " interface. Unknown TypeURLs cause an")
	out.P("// error by default; pass projection.IgnoreUnknown() to skip them.")
	out.P("func New", hostName, "Dispatcher(p ", ifaceName, ", opts ...", dispOption, ") ", handler, " {")
	out.P("\tcfg := ", applyOptions, "(opts)")
	out.P("\treturn func(ctx ", contextType, ", env ", envelope, ") error {")
	out.P("\t\tswitch env.TypeURL {")
	for _, evt := range ps.events {
		methodName := "On" + evt.GoIdent.GoName
		evtIdent := out.QualifiedGoIdent(evt.GoIdent)
		out.P("\t\tcase \"", evt.Desc.FullName(), "\":")
		out.P("\t\t\te := &", evtIdent, "{}")
		out.P("\t\t\tif err := ", protoUnmarshal, "(env.Payload, e); err != nil {")
		out.P("\t\t\t\treturn ", fmtErrorf, "(\"projection %s decode %s: %w\", ", hostName, "Name, env.TypeURL, err)")
		out.P("\t\t\t}")
		out.P("\t\t\treturn p.", methodName, "(ctx, env, e)")
	}
	out.P("\t\t}")
	out.P("\t\tif cfg.IgnoreUnknown {")
	out.P("\t\t\treturn nil")
	out.P("\t\t}")
	out.P("\t\treturn ", fmtErrorf, "(\"projection %s: unknown TypeURL %q\", ", hostName, "Name, env.TypeURL)")
	out.P("\t}")
	out.P("}")
	out.P()
}

// piiField captures the codegen analysis of one field on an event
// variant as it relates to data classification (ADR 0010 + ADR 0027).
type piiField struct {
	goName         string                  // Go struct field name (e.g. "Email")
	protoName      string                  // proto field name (e.g. "email")
	isSubject      bool                    // marked (es.v1.subject_field) = true
	subjectField   string                  // (es.v1.subject) = "other_field" override; default ""
	classification esv1.DataClassification // (es.v1.data_classification) value, UNSPECIFIED if absent
	kind           piiKind                 // how this field is encoded for encryption
}

// piiKind selects the encrypt/decrypt code path emitted by codegen.
type piiKind int

const (
	piiKindNone     piiKind = iota // not encrypted — no encryption code emitted
	piiKindBytes                   // bytes field, encrypted in place as raw ciphertext
	piiKindString                  // string field, base64-encoded ciphertext for UTF-8 safety
	piiKindRejected                // SAD — must not be persisted; codegen emits a runtime-reject EncryptPII
)

// isEncryptedClassification returns true when the classification means
// the field must be encrypted per-subject. PUBLIC, INTERNAL,
// UNSPECIFIED stay plaintext; SAD is rejected (handled separately).
func isEncryptedClassification(c esv1.DataClassification) bool {
	switch c {
	case esv1.DataClassification_DATA_CLASSIFICATION_PERSONAL,
		esv1.DataClassification_DATA_CLASSIFICATION_QUASI_IDENTIFIER,
		esv1.DataClassification_DATA_CLASSIFICATION_SENSITIVE,
		esv1.DataClassification_DATA_CLASSIFICATION_FINANCIAL,
		esv1.DataClassification_DATA_CLASSIFICATION_CARDHOLDER,
		esv1.DataClassification_DATA_CLASSIFICATION_CREDENTIAL,
		esv1.DataClassification_DATA_CLASSIFICATION_UNSTRUCTURED:
		return true
	}
	return false
}

// classifyFields walks a message's fields and derives each field's
// encryption code path from its (es.v1.data_classification). The
// boolean result reports whether the variant has at least one field
// that produces emit-able encryption code — when false, codegen omits
// the EncryptPII/DecryptPII methods entirely.
func classifyFields(v *protogen.Message) ([]piiField, bool, error) {
	var (
		fields []piiField
		hasPII bool
	)
	for _, f := range v.Fields {
		pf := piiField{
			goName:    f.GoName,
			protoName: f.Desc.TextName(),
		}
		if opts, ok := f.Desc.Options().(*descriptorpb.FieldOptions); ok && opts != nil {
			if proto.HasExtension(opts, esv1.E_SubjectField) {
				if val, _ := proto.GetExtension(opts, esv1.E_SubjectField).(bool); val {
					pf.isSubject = true
				}
			}
			if proto.HasExtension(opts, esv1.E_Subject) {
				if val, _ := proto.GetExtension(opts, esv1.E_Subject).(string); val != "" {
					pf.subjectField = val
				}
			}
			if proto.HasExtension(opts, esv1.E_DataClassification) {
				if val, _ := proto.GetExtension(opts, esv1.E_DataClassification).(esv1.DataClassification); val != 0 {
					pf.classification = val
				}
			}
		}

		// Subject fields are auto-non-encrypted per ADR 0010 ("you
		// would need the key to find the key"). Setting a classification
		// on a subject_field is a no-op for encryption (the manifest
		// still records the declared classification for audit).
		switch {
		case pf.isSubject:
			pf.kind = piiKindNone
		case pf.classification == esv1.DataClassification_DATA_CLASSIFICATION_SAD:
			pf.kind = piiKindRejected
		case isEncryptedClassification(pf.classification):
			switch f.Desc.Kind() {
			case protoreflect.BytesKind:
				pf.kind = piiKindBytes
			case protoreflect.StringKind:
				pf.kind = piiKindString
			default:
				// Non-string/bytes field with a PII classification: the
				// classification still drives View() / LogValue() access
				// scoping (e.g. an int32 date_of_birth_year classified
				// QUASI_IDENTIFIER is hidden below AccessLevelSubject),
				// but the field cannot be encrypted at the wire-format
				// boundary — there's no per-field crypto envelope for
				// fixed-width primitives. Emit no encryption code; the
				// access helpers continue to honour the classification.
				//
				// Repeated/map/message fields with a PII classification
				// land here too; they encode their PII through their
				// element/value types' own classifications, not at the
				// container level.
				pf.kind = piiKindNone
			}
		default:
			pf.kind = piiKindNone
		}

		fields = append(fields, pf)
		if pf.kind != piiKindNone {
			hasPII = true
		}
	}
	return fields, hasPII, nil
}

// emitPIIMethods writes PIIFields/Subject/EncryptPII/DecryptPII on one
// event variant. No-op for variants without PII fields.
func emitPIIMethods(out *protogen.GeneratedFile, v *protogen.Message) error {
	fields, hasPII, err := classifyFields(v)
	if err != nil {
		return err
	}
	if !hasPII {
		return nil
	}

	vName := v.GoIdent.GoName

	// Collect just the PII fields.
	var piiFields []piiField
	for _, f := range fields {
		if f.kind != piiKindNone {
			piiFields = append(piiFields, f)
		}
	}

	// SAD detection up-front. A variant with any SAD field emits a
	// short-circuit EncryptPII / DecryptPII that returns a typed error
	// before touching anything else (ADR 0027). When the variant is
	// SAD-only the per-field loop never runs, so we must NOT register
	// the imports that loop would have needed — protogen marks every
	// QualifiedGoIdent call as a used import, and unused imports fail
	// the generated package to compile.
	hasSAD := false
	for _, pf := range piiFields {
		if pf.kind == piiKindRejected {
			hasSAD = true
			break
		}
	}

	shredPkg := protogen.GoImportPath("github.com/laenenai/eventstore/shred")
	contextType := out.QualifiedGoIdent(contextPkg.Ident("Context"))
	shredderType := out.QualifiedGoIdent(shredPkg.Ident("Shredder"))
	redactedType := out.QualifiedGoIdent(shredPkg.Ident("RedactedFields"))
	fmtErrorf := out.QualifiedGoIdent(fmtPkg.Ident("Errorf"))

	// Imports used only by the per-field encrypt/decrypt loops; suppress
	// when SAD reject preempts those loops, otherwise the generated file
	// has unused imports.
	var (
		redactedField string
		errShredded   string
		errorsIs      string
		b64Pkg        string
	)
	if !hasSAD {
		redactedField = out.QualifiedGoIdent(shredPkg.Ident("RedactedField"))
		errShredded = out.QualifiedGoIdent(shredPkg.Ident("ErrShredded"))
		errorsIs = out.QualifiedGoIdent(protogen.GoImportPath("errors").Ident("Is"))
		// base64 import is only needed when at least one field is string-PII.
		for _, pf := range piiFields {
			if pf.kind == piiKindString {
				b64Pkg = out.QualifiedGoIdent(protogen.GoImportPath("encoding/base64").Ident("RawStdEncoding"))
				break
			}
		}
	}
	// Subject field, if any.
	var subjectGoName, subjectProtoName string
	for _, f := range fields {
		if f.isSubject {
			subjectGoName = f.goName
			subjectProtoName = f.protoName
			break
		}
	}

	out.P()
	out.P("// ---- Crypto-shredding for ", vName, " (ADR 0010) ----")
	out.P()

	// PIIFields()
	out.P("// PIIFields returns the proto field names of ", vName, "'s")
	out.P("// encrypted fields. Stable across regenerations.")
	out.P("func (*", vName, ") PIIFields() []string {")
	out.P("\treturn []string{")
	for _, pf := range piiFields {
		out.P("\t\t\"", pf.protoName, "\",")
	}
	out.P("\t}")
	out.P("}")
	out.P()

	// Subject()
	out.P("// Subject returns the default subject id used to key the DEK")
	out.P("// for this event's PII fields. Empty when the variant has no")
	out.P("// (es.v1.subject_field) — caller falls back to the StreamID.")
	out.P("func (e *", vName, ") Subject() string {")
	_ = subjectProtoName // tracked but not emitted to avoid unreachable-code error
	if subjectGoName != "" {
		out.P("\treturn e.Get", subjectGoName, "()")
	} else {
		out.P("\treturn \"\"")
	}
	out.P("}")
	out.P()

	// Names of every SAD field on this variant. Listed in the reject
	// error so operators see which fields tripped the guard — important
	// when a mixed-classification message is a design bug masquerading
	// as one bad annotation.
	var sadFieldNames []string
	for _, pf := range piiFields {
		if pf.kind == piiKindRejected {
			sadFieldNames = append(sadFieldNames, pf.protoName)
		}
	}

	// EncryptPII
	out.P("// EncryptPII encrypts every classification-PERSONAL+ field in")
	out.P("// place using s. Called by aggregate.Runtime before Codec.Encode")
	out.P("// when Runtime.Shredder is configured. The framework passes the")
	out.P("// resolved subject id; per-field (es.v1.subject) overrides are")
	out.P("// honored below.")
	out.P("//")
	out.P("// `bytes` fields hold raw ciphertext after encrypt; `string` fields")
	out.P("// classified PERSONAL+ hold base64-encoded ciphertext so the field")
	out.P("// remains UTF-8-valid. Fields classified")
	out.P("// (es.v1.data_classification) = DATA_CLASSIFICATION_SAD cause this")
	out.P("// method to return an error before any field is touched — SAD MUST")
	out.P("// NOT be persisted (PCI-DSS §3.2). See ADR 0027.")
	out.P("func (e *", vName, ") EncryptPII(ctx ", contextType, ", s *", shredderType, ", tenantID, subject string) error {")
	if hasSAD {
		// Refuse before touching any field: a message that mixes SAD
		// with PERSONAL is a design bug, and a half-encrypted message
		// on the way to storage is strictly worse than a clean reject.
		out.P("\treturn ", fmtErrorf, "(\"SAD MUST NOT be persisted: ", vName, " contains DATA_CLASSIFICATION_SAD field(s) %q\", []string{")
		for _, name := range sadFieldNames {
			out.P("\t\t\"", name, "\",")
		}
		out.P("\t})")
		out.P("}")
		out.P()
	} else {
		for _, pf := range piiFields {
			subjExpr := "subject"
			if pf.subjectField != "" {
				subjExpr = `e.Get` + upperGoFieldName(pf.subjectField) + `()`
			}
			switch pf.kind {
			case piiKindBytes:
				out.P("\tif len(e.", pf.goName, ") > 0 {")
				out.P("\t\tsealed, err := s.EncryptField(ctx, tenantID, ", subjExpr, ", e.", pf.goName, ")")
				out.P("\t\tif err != nil {")
				out.P("\t\t\treturn ", fmtErrorf, "(\"", vName, ".EncryptPII ", pf.protoName, ": %w\", err)")
				out.P("\t\t}")
				out.P("\t\te.", pf.goName, " = sealed")
				out.P("\t}")
			case piiKindString:
				out.P("\tif e.", pf.goName, " != \"\" {")
				out.P("\t\tsealed, err := s.EncryptField(ctx, tenantID, ", subjExpr, ", []byte(e.", pf.goName, "))")
				out.P("\t\tif err != nil {")
				out.P("\t\t\treturn ", fmtErrorf, "(\"", vName, ".EncryptPII ", pf.protoName, ": %w\", err)")
				out.P("\t\t}")
				out.P("\t\te.", pf.goName, " = ", b64Pkg, ".EncodeToString(sealed)")
				out.P("\t}")
			}
		}
		out.P("\treturn nil")
		out.P("}")
		out.P()
	}

	// DecryptPII
	out.P("// DecryptPII reverses EncryptPII. Per-field shred → RedactedField;")
	out.P("// base64 decode failure on string PII fields counts as a corrupt")
	out.P("// payload and aborts; other errors abort. Reading a SAD-classified")
	out.P("// field from persistence is itself corruption (it should never have")
	out.P("// been written) and returns the same SAD reject error.")
	out.P("func (e *", vName, ") DecryptPII(ctx ", contextType, ", s *", shredderType, ", tenantID, subject string) (", redactedType, ", error) {")
	if hasSAD {
		// Mirror Encrypt: if a SAD payload reached storage anyway, the
		// stream is in a regulator-reportable state and we fail loudly
		// rather than decoding it.
		out.P("\treturn nil, ", fmtErrorf, "(\"SAD MUST NOT be persisted: ", vName, " contains DATA_CLASSIFICATION_SAD field(s) %q\", []string{")
		for _, name := range sadFieldNames {
			out.P("\t\t\"", name, "\",")
		}
		out.P("\t})")
		out.P("}")
		out.P()
		return nil
	}
	out.P("\tvar redacted ", redactedType)
	for _, pf := range piiFields {
		subjExpr := "subject"
		if pf.subjectField != "" {
			subjExpr = `e.Get` + upperGoFieldName(pf.subjectField) + `()`
		}
		switch pf.kind {
		case piiKindBytes:
			out.P("\tif len(e.", pf.goName, ") > 0 {")
			out.P("\t\tpt, err := s.DecryptField(ctx, tenantID, ", subjExpr, ", e.", pf.goName, ")")
			out.P("\t\tswitch {")
			out.P("\t\tcase err == nil:")
			out.P("\t\t\te.", pf.goName, " = pt")
			out.P("\t\tcase ", errorsIs, "(err, ", errShredded, "):")
			out.P("\t\t\te.", pf.goName, " = nil")
			out.P("\t\t\tredacted = append(redacted, ", redactedField, "{Name: \"", pf.protoName, "\", Subject: ", subjExpr, ", Reason: \"shredded\"})")
			out.P("\t\tdefault:")
			out.P("\t\t\treturn redacted, ", fmtErrorf, "(\"", vName, ".DecryptPII ", pf.protoName, ": %w\", err)")
			out.P("\t\t}")
			out.P("\t}")
		case piiKindString:
			out.P("\tif e.", pf.goName, " != \"\" {")
			out.P("\t\tsealed, b64err := ", b64Pkg, ".DecodeString(e.", pf.goName, ")")
			out.P("\t\tif b64err != nil {")
			out.P("\t\t\treturn redacted, ", fmtErrorf, "(\"", vName, ".DecryptPII ", pf.protoName, " base64: %w\", b64err)")
			out.P("\t\t}")
			out.P("\t\tpt, err := s.DecryptField(ctx, tenantID, ", subjExpr, ", sealed)")
			out.P("\t\tswitch {")
			out.P("\t\tcase err == nil:")
			out.P("\t\t\te.", pf.goName, " = string(pt)")
			out.P("\t\tcase ", errorsIs, "(err, ", errShredded, "):")
			out.P("\t\t\te.", pf.goName, " = \"\"")
			out.P("\t\t\tredacted = append(redacted, ", redactedField, "{Name: \"", pf.protoName, "\", Subject: ", subjExpr, ", Reason: \"shredded\"})")
			out.P("\t\tdefault:")
			out.P("\t\t\treturn redacted, ", fmtErrorf, "(\"", vName, ".DecryptPII ", pf.protoName, ": %w\", err)")
			out.P("\t\t}")
			out.P("\t}")
		}
	}
	out.P("\treturn redacted, nil")
	out.P("}")
	out.P()
	return nil
}

// manifestAttrs renders one field entry of the pii_manifest.json as
// a single deterministic JSON object literal. Captures classification
// + derived behaviors (encryption, DSAR export, audit-on-read,
// retention class) so audit / DSAR-exporter / PCI-scope tooling can
// consume the manifest without re-implementing the rules.
func manifestAttrs(pf piiField) string {
	classification := pf.classification.String()
	if pf.isSubject {
		classification = "DATA_CLASSIFICATION_SUBJECT_FIELD"
	}

	var encryption string
	switch pf.kind {
	case piiKindNone:
		encryption = "none"
	case piiKindBytes:
		encryption = "subject_bytes"
	case piiKindString:
		encryption = "subject_string_base64"
	case piiKindRejected:
		encryption = "rejected_sad"
	}

	dsarExport := true
	auditOnRead := false
	retention := "standard"
	switch pf.classification {
	case esv1.DataClassification_DATA_CLASSIFICATION_INTERNAL:
		dsarExport = false
	case esv1.DataClassification_DATA_CLASSIFICATION_SENSITIVE:
		auditOnRead = true
		retention = "shorter"
	case esv1.DataClassification_DATA_CLASSIFICATION_FINANCIAL:
		retention = "tax_locked"
	case esv1.DataClassification_DATA_CLASSIFICATION_CARDHOLDER:
		auditOnRead = true
		retention = "pci_scope"
	case esv1.DataClassification_DATA_CLASSIFICATION_CREDENTIAL:
		dsarExport = false
		auditOnRead = true
	case esv1.DataClassification_DATA_CLASSIFICATION_SAD:
		dsarExport = false
	}
	if pf.isSubject {
		// Subject ids are the look-up handles for the DEK and are not
		// themselves PII (per ADR 0010); they remain plaintext and
		// always DSAR-exportable as an entity identifier.
		dsarExport = true
		auditOnRead = false
		retention = "standard"
	}

	subjectAttr := ""
	if pf.subjectField != "" {
		subjectAttr = fmt.Sprintf(`, "subject_field_override": %q`, pf.subjectField)
	}

	return fmt.Sprintf(
		`{"name": %q, "classification": %q, "encryption": %q, "dsar_export": %t, "audit_on_read": %t, "retention": %q%s}`,
		pf.protoName, classification, encryption, dsarExport, auditOnRead, retention, subjectAttr,
	)
}

// upperGoFieldName converts a proto field name (snake_case) to its
// Go struct field name (UpperCamelCase). Matches the convention
// protoc-gen-go uses for accessor names.
func upperGoFieldName(protoName string) string {
	out := make([]byte, 0, len(protoName))
	upper := true
	for i := 0; i < len(protoName); i++ {
		c := protoName[i]
		if c == '_' {
			upper = true
			continue
		}
		if upper && c >= 'a' && c <= 'z' {
			c -= 32
		}
		out = append(out, c)
		upper = false
	}
	return string(out)
}

// emitPIIManifest writes pii_manifest.json next to the generated Go
// code: one entry per event variant, each field classified per ADR
// 0010 (subject_field / non_pii / pii_intentional / pii). The
// document is JSON for ergonomic diff review and machine consumption
// by privacy-audit tooling.
func emitPIIManifest(plugin *protogen.Plugin, file *protogen.File, variants []*protogen.Message) error {
	out := plugin.NewGeneratedFile(
		file.GeneratedFilenamePrefix+"_pii_manifest.json",
		file.GoImportPath,
	)

	// Hand-rolled JSON so the output is deterministic (key order)
	// without pulling encoding/json into the plugin. Two-space
	// indent, stable variant order (proto declaration order).
	out.P("{")
	out.P(`  "source": "`, file.Desc.Path(), `",`)
	out.P(`  "package": "`, file.Desc.Package(), `",`)
	out.P(`  "events": [`)
	for i, v := range variants {
		fields, _, err := classifyFields(v)
		if err != nil {
			return err
		}
		comma := ","
		if i == len(variants)-1 {
			comma = ""
		}
		out.P(`    {`)
		out.P(`      "name": "`, v.Desc.FullName(), `",`)
		out.P(`      "fields": [`)
		for j, pf := range fields {
			fcomma := ","
			if j == len(fields)-1 {
				fcomma = ""
			}
			attrs := manifestAttrs(pf)
			out.P(`        `, attrs, fcomma)
		}
		out.P(`      ]`)
		out.P(`    }`, comma)
	}
	out.P(`  ]`)
	out.P("}")
	return nil
}
