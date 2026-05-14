// Access-level view scopes used by codegen-emitted View / LogValue
// helpers. The annotation (es.v1.data_classification) on each field
// labels its sensitivity; AccessLevel labels the caller's scope. The
// generated View(level) returns a deep copy with fields above the
// caller's level zero-valued; LogValue() implements slog.LogValuer at
// AccessLevelInternal with [REDACTED:<class>] markers in place of the
// hidden fields.
//
// This pair (annotation + level) is deliberately orthogonal to the
// crypto-shredding model: (es.v1.non_pii) controls what is encrypted
// AT REST (ADR 0010); data_classification controls what may leak
// through LOGS/UIs/EXPORTS even when the bytes are in plaintext in
// memory. A field can be both encrypted and classified — encryption
// stops a dropped database from leaking, classification stops a
// misconfigured slog handler from leaking. Different attackers,
// different defenses.
package es

import (
	esv1 "github.com/laenenai/eventstore/gen/es/v1"
)

// AccessLevel is a view scope. Levels are ordered: a higher level
// subsumes every field visible at lower levels. The codegen-emitted
// View(level) method on each generated message returns a deep copy
// with fields above level zero-valued.
//
// The five levels correspond to typical audiences:
//
//   - Public: cross-tenant safe (analytics, billing aggregates).
//   - Internal: same-org ops dashboards (back-office, support tools).
//   - Customer: DSAR exports, customer-facing UIs after auth.
//   - Compliance: compliance officers, fraud/risk teams.
//   - Operator: system internals, full read.
type AccessLevel int

const (
	// AccessLevelPublic — PUBLIC-classified fields only. Cross-tenant
	// safe. Use for telemetry aggregates, anonymous analytics, public
	// metrics.
	AccessLevelPublic AccessLevel = iota

	// AccessLevelInternal — adds INTERNAL. Same-org back-office
	// dashboards, support consoles. Default for slog.LogValue.
	AccessLevelInternal

	// AccessLevelCustomer — adds PERSONAL, QUASI_IDENTIFIER, and
	// UNSTRUCTURED. DSAR exports under GDPR Article 15 and customer
	// self-service screens belong here.
	AccessLevelCustomer

	// AccessLevelCompliance — adds SENSITIVE (GDPR Article 9),
	// FINANCIAL, and CARDHOLDER. Compliance officers, fraud
	// investigators, audit pulls.
	AccessLevelCompliance

	// AccessLevelOperator — adds CREDENTIAL. Full read for system
	// internals, debugging in a controlled environment. There is no
	// audience above this; treat it as god mode.
	AccessLevelOperator
)

// String renders the level in a stable form suitable for log fields
// and authz traces.
func (l AccessLevel) String() string {
	switch l {
	case AccessLevelPublic:
		return "public"
	case AccessLevelInternal:
		return "internal"
	case AccessLevelCustomer:
		return "customer"
	case AccessLevelCompliance:
		return "compliance"
	case AccessLevelOperator:
		return "operator"
	}
	return "unknown"
}

// MinLevelFor returns the minimum AccessLevel required to view a
// field of the given classification. Unknown/future classifications
// map to AccessLevelOperator (closed-by-default: hide what you don't
// understand). Subject fields are independently visible at every
// level regardless of this function — they are encryption-key handles,
// not identifying data on their own; the codegen wires that exception
// directly.
func MinLevelFor(c esv1.DataClassification) AccessLevel {
	switch c {
	case esv1.DataClassification_DATA_CLASSIFICATION_UNSPECIFIED,
		esv1.DataClassification_DATA_CLASSIFICATION_PUBLIC:
		return AccessLevelPublic
	case esv1.DataClassification_DATA_CLASSIFICATION_INTERNAL:
		return AccessLevelInternal
	case esv1.DataClassification_DATA_CLASSIFICATION_PERSONAL,
		esv1.DataClassification_DATA_CLASSIFICATION_QUASI_IDENTIFIER,
		esv1.DataClassification_DATA_CLASSIFICATION_UNSTRUCTURED:
		return AccessLevelCustomer
	case esv1.DataClassification_DATA_CLASSIFICATION_SENSITIVE,
		esv1.DataClassification_DATA_CLASSIFICATION_FINANCIAL,
		esv1.DataClassification_DATA_CLASSIFICATION_CARDHOLDER:
		return AccessLevelCompliance
	case esv1.DataClassification_DATA_CLASSIFICATION_CREDENTIAL:
		return AccessLevelOperator
	}
	// Closed-by-default: an unrecognised classification (e.g., a
	// newer proto enum value being read by older code) hides at
	// every level below Operator.
	return AccessLevelOperator
}

// ClassificationLabel returns the short form of a classification
// suitable for log redaction markers ("PERSONAL", "FINANCIAL", ...).
// Used by codegen-emitted LogValue methods to render
// "[REDACTED:<label>]" markers.
func ClassificationLabel(c esv1.DataClassification) string {
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
