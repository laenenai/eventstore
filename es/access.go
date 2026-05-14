// Access-level view scopes used by codegen-emitted View / LogValue
// helpers. The annotation (es.v1.data_classification) on each field
// labels its sensitivity (PERSONAL / SENSITIVE / FINANCIAL / etc.);
// AccessLevel labels the caller's scope. The generated View(level)
// returns a deep copy with fields above the caller's level zero-
// valued; LogValue() implements slog.LogValuer at AccessLevelInternal
// with [REDACTED:<class>] markers in place of the hidden fields.
//
// The same classification drives crypto-shredding at the wire-format
// boundary (ADR 0010 + ADR 0027): View / LogValue / Clone all operate
// on plaintext values once the codec has decrypted them at unmarshal
// time. Encryption stops a dropped database from leaking;
// classification + AccessLevel stop a misconfigured slog handler or
// over-permissive UI from leaking. Different attackers, different
// defenses, single annotation.
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
//   - Subject: the data subject viewing their own data — DSAR exports
//     (GDPR Article 15), self-service UIs, customer/employee/patient/
//     account-owner screens depending on the domain. Aligns with the
//     framework's existing (es.v1.subject_field) annotation: the same
//     natural person is the encryption-subject AND the access-subject.
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

	// AccessLevelSubject — adds PERSONAL, QUASI_IDENTIFIER, and
	// UNSTRUCTURED. What the data subject (GDPR Art 4(1)) is entitled
	// to see about themselves: DSAR exports under GDPR Article 15,
	// self-service screens, "my account" pages. Domain-agnostic: the
	// subject is the customer in a fintech, the employee in an HR
	// system, the patient in a healthcare system, the account-holder
	// in a B2B SaaS.
	AccessLevelSubject

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
	case AccessLevelSubject:
		return "subject"
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
		return AccessLevelSubject
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
