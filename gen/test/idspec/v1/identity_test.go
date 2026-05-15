package idspecv1_test

import (
	"reflect"
	"testing"

	pb "github.com/laenenai/eventstore/gen/test/idspec/v1"
)

// Tests for the codegen-emitted per-variant identity helpers:
//
//   - Sealed-interface marker methods: every variant satisfies its sum
//     interface, Command and Event interfaces stay disjoint.
//   - Action() returns the variant's full proto type name, stable
//     across regenerations, no collisions.
//   - Subject() on events: returns (es.v1.subject_field) value when
//     declared, "" otherwise.
//   - PIIFields() on events: returns proto field names in declaration
//     order, emitted ONLY for variants with at least one PERSONAL+
//     classified field.
//
// Source proto: proto/test/idspec/v1/idspec.proto.

// ---- Sealed-interface marker methods ---------------------------------

func TestSealedMarkers_CommandsImplementCommand(t *testing.T) {
	// Compile-time + assignability check: every command variant
	// satisfies the sealed Command interface.
	cmds := []pb.Command{
		&pb.CreateCmd{},
		&pb.UpdateCmd{},
	}
	if len(cmds) != 2 {
		t.Fatalf("variant count drifted: got %d want 2", len(cmds))
	}
}

func TestSealedMarkers_EventsImplementEvent(t *testing.T) {
	evs := []pb.Event{
		&pb.CreatedEv{},
		&pb.RenamedEv{},
		&pb.CleanedEv{},
		&pb.SubjectLessEv{},
	}
	if len(evs) != 4 {
		t.Fatalf("variant count drifted: got %d want 4", len(evs))
	}
}

// TestSealedMarkers_CommandAndEventDisjoint pins the contract that a
// Command variant doesn't accidentally satisfy Event (and vice versa).
// The marker methods are unexported (isCommand / isEvent), so the
// sealed-interface property is enforced at the package boundary —
// only types declared in this package can satisfy the interfaces.
// Cross-interface satisfaction would mean codegen emitted both
// markers on one variant, which is structurally wrong.
func TestSealedMarkers_CommandAndEventDisjoint(t *testing.T) {
	var cmd pb.Command = &pb.CreateCmd{}
	var ev pb.Event = &pb.CreatedEv{}

	if _, ok := any(cmd).(pb.Event); ok {
		t.Errorf("command variant *CreateCmd should NOT satisfy Event")
	}
	if _, ok := any(ev).(pb.Command); ok {
		t.Errorf("event variant *CreatedEv should NOT satisfy Command")
	}
}

// ---- Action() --------------------------------------------------------

func TestAction_StableProtoTypeName(t *testing.T) {
	cases := []struct {
		got, want string
	}{
		{(&pb.CreateCmd{}).Action(), "test.idspec.v1.CreateCmd"},
		{(&pb.UpdateCmd{}).Action(), "test.idspec.v1.UpdateCmd"},
		{(&pb.CreatedEv{}).Action(), "test.idspec.v1.CreatedEv"},
		{(&pb.RenamedEv{}).Action(), "test.idspec.v1.RenamedEv"},
		{(&pb.CleanedEv{}).Action(), "test.idspec.v1.CleanedEv"},
		{(&pb.SubjectLessEv{}).Action(), "test.idspec.v1.SubjectLessEv"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("Action(): got %q want %q", c.got, c.want)
		}
	}
}

func TestAction_NoCollisions(t *testing.T) {
	// Each variant's Action() must be globally unique across the file's
	// sum types. If the codegen emitted the message name instead of the
	// fully-qualified proto name, two messages with the same local name
	// across packages would collide — pin that the FQN is what's used.
	seen := map[string]string{}
	type withAction interface{ Action() string }
	all := []withAction{
		&pb.CreateCmd{}, &pb.UpdateCmd{},
		&pb.CreatedEv{}, &pb.RenamedEv{}, &pb.CleanedEv{}, &pb.SubjectLessEv{},
	}
	for _, v := range all {
		action := v.Action()
		typeName := reflectTypeName(v)
		if prev, hit := seen[action]; hit {
			t.Errorf("Action collision: %q used by both %s and %s", action, prev, typeName)
		}
		seen[action] = typeName
	}
}

func reflectTypeName(v any) string {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Name()
}

func TestAction_NilSafe(t *testing.T) {
	// Action takes a pointer receiver but doesn't dereference m — it
	// returns a compile-time string literal. Calling on a typed-nil
	// pointer should not panic.
	var c *pb.CreateCmd
	if got := c.Action(); got != "test.idspec.v1.CreateCmd" {
		t.Errorf("Action() on typed-nil: got %q", got)
	}
	var e *pb.CreatedEv
	if got := e.Action(); got != "test.idspec.v1.CreatedEv" {
		t.Errorf("Action() on typed-nil event: got %q", got)
	}
}

// ---- Subject() -------------------------------------------------------

func TestSubject_FromSubjectField(t *testing.T) {
	e := &pb.CreatedEv{
		Id:    "user-42",
		Name:  "Alice",
		Email: "alice@example.com",
	}
	if got := e.Subject(); got != "user-42" {
		t.Errorf("Subject(): got %q want %q", got, "user-42")
	}

	r := &pb.RenamedEv{Id: "user-99", NewName: "Bob"}
	if got := r.Subject(); got != "user-99" {
		t.Errorf("RenamedEv Subject(): got %q want %q", got, "user-99")
	}
}

func TestSubject_EmptyWhenNoSubjectField(t *testing.T) {
	// SubjectLessEv has a PII field (note) but no (es.v1.subject_field).
	// Subject() is still emitted (PIIFields() drives emission, not
	// presence of subject_field), and it returns "" so callers know to
	// fall back to the StreamID.
	e := &pb.SubjectLessEv{
		TraceId: "trace-1",
		Note:    "free-form",
	}
	if got := e.Subject(); got != "" {
		t.Errorf("SubjectLessEv Subject(): got %q want empty", got)
	}
}

func TestSubject_OnEmptyMessageReturnsEmpty(t *testing.T) {
	// CreatedEv with no Id set: GetId() returns "" (proto3 default).
	// Subject() should propagate that.
	e := &pb.CreatedEv{}
	if got := e.Subject(); got != "" {
		t.Errorf("Subject() on empty message: got %q", got)
	}
}

// ---- PIIFields() -----------------------------------------------------

func TestPIIFields_ProtoNamesInDeclarationOrder(t *testing.T) {
	// CreatedEv declares: id (subject_field, not PII), name (PERSONAL),
	// email (PERSONAL), region (INTERNAL — NOT PII).
	// PIIFields() returns the PERSONAL+ classified fields only, by
	// proto name (snake_case), in declaration order.
	got := (&pb.CreatedEv{}).PIIFields()
	want := []string{"name", "email"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("CreatedEv PIIFields: got %v want %v", got, want)
	}
}

func TestPIIFields_SingleField(t *testing.T) {
	got := (&pb.RenamedEv{}).PIIFields()
	want := []string{"new_name"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("RenamedEv PIIFields: got %v want %v", got, want)
	}
}

func TestPIIFields_OnVariantWithNoSubjectField(t *testing.T) {
	// SubjectLessEv has note (PERSONAL). PIIFields() should include
	// it regardless of subject_field presence.
	got := (&pb.SubjectLessEv{}).PIIFields()
	want := []string{"note"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SubjectLessEv PIIFields: got %v want %v", got, want)
	}
}

// TestPIIFields_NotEmittedForCleanEvents pins the contract that
// CleanedEv (no PII at all) does NOT get a PIIFields() method emitted.
// We assert via an interface check — variants WITH PIIFields satisfy
// it, CleanedEv does not.
func TestPIIFields_NotEmittedForCleanEvents(t *testing.T) {
	type piiBearer interface {
		PIIFields() []string
		Subject() string
	}

	// CleanedEv (no PII) must NOT satisfy piiBearer.
	var cleaned pb.Event = &pb.CleanedEv{}
	if _, ok := any(cleaned).(piiBearer); ok {
		t.Errorf("CleanedEv has no PII but emits PIIFields/Subject; codegen drifted")
	}

	// Every event with PII satisfies it.
	withPII := []pb.Event{
		&pb.CreatedEv{},
		&pb.RenamedEv{},
		&pb.SubjectLessEv{},
	}
	for _, e := range withPII {
		if _, ok := any(e).(piiBearer); !ok {
			t.Errorf("%T has PII but doesn't satisfy piiBearer (PIIFields/Subject)", e)
		}
	}
}

func TestPIIFields_NilSafe(t *testing.T) {
	// Same as Action(): pointer receiver, no dereference.
	var e *pb.CreatedEv
	got := e.PIIFields()
	want := []string{"name", "email"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PIIFields() on typed-nil: got %v want %v", got, want)
	}
}

// ---- Sum-type oneof: variant carriage -----------------------------------

// TestSumType_OneofVariantCarriage ensures the codegen-emitted wrapper
// structs around oneof variants are themselves assignable to the sum
// interface. Subtle but matters: if codegen ever emitted the marker
// method on the wrapper instead of the bare variant, type switches
// would break.
func TestSumType_OneofVariantCarriage(t *testing.T) {
	// Construct an Events oneof with a CreatedEv variant.
	all := pb.Events{
		Variant: &pb.Events_Created{
			Created: &pb.CreatedEv{Id: "user-1", Name: "Alice"},
		},
	}
	switch v := all.Variant.(type) {
	case *pb.Events_Created:
		// The inner *CreatedEv (not the *Events_Created wrapper) is what
		// implements Event.
		var _ pb.Event = v.Created
		if v.Created.Subject() != "user-1" {
			t.Errorf("nested variant Subject(): got %q", v.Created.Subject())
		}
	default:
		t.Errorf("unexpected variant: %T", v)
	}
}
