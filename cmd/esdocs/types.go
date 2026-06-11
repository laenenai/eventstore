// Package main — shared types for the esdocs CLI.
//
// These types mirror the on-disk shape of the v2 PII manifest emitted
// by cmd/protoc-gen-es-go. The producer hand-rolls the JSON for
// deterministic key order; the consumer (this package) uses
// encoding/json normally. Round-trip equality is not required —
// only structural compatibility.
package main

import "time"

// Manifest is one per-package PII manifest as emitted by codegen
// (cmd/protoc-gen-es-go/main.go emitPIIManifest, schema v2).
//
// v1 manifests are tolerated: missing fields decode to zero values
// (ManifestVersion=0, Aggregates/Commands empty, Events present).
// The catalog command surfaces this in its warnings list.
type Manifest struct {
	ManifestVersion int             `json:"manifest_version"`
	Source          string          `json:"source"`
	Package         string          `json:"package"`
	GoPackage       string          `json:"go_package,omitempty"`
	Aggregates      []AggregateSpec `json:"aggregates,omitempty"`
	Commands        []MessageSpec   `json:"commands,omitempty"`
	Events          []MessageSpec   `json:"events"`
}

// AggregateSpec describes a State-bearing aggregate.
type AggregateSpec struct {
	Name         string      `json:"name"`
	StateMessage string      `json:"state_message"`
	SubjectField string      `json:"subject_field"`
	StateFields  []FieldSpec `json:"state_fields"`
}

// MessageSpec describes one command or event message.
type MessageSpec struct {
	Name   string      `json:"name"`
	Fields []FieldSpec `json:"fields"`
}

// FieldSpec captures everything an audit / DSAR / catalog consumer
// needs to render one field without re-reading the .proto source.
type FieldSpec struct {
	Name             string `json:"name"`
	ProtoType        string `json:"proto_type,omitempty"`
	Classification   string `json:"classification"`
	Encryption       string `json:"encryption"`
	DSARExport       bool   `json:"dsar_export"`
	AuditOnRead      bool   `json:"audit_on_read"`
	Retention        string `json:"retention"`
	SubjectOverride  string `json:"subject_override,omitempty"`
}

// Catalog is the combined regulator-facing document: every package's
// manifest, plus summary statistics computed at catalog time.
type Catalog struct {
	SchemaVersion int        `json:"schema_version"`
	GeneratedAt   time.Time  `json:"generated_at"`
	Framework     FrameworkInfo `json:"framework"`
	Packages      []Manifest `json:"packages"`
	Summary       Summary    `json:"summary"`
	Warnings      []string   `json:"warnings,omitempty"`
}

// FrameworkInfo identifies the framework version the catalog was
// generated against. Populated from --framework-version on the CLI;
// the tool does not try to autodetect (release flow already pins it).
type FrameworkInfo struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// Summary tallies are derived at catalog time. They are not stored in
// the per-package manifests because the totals only make sense over a
// chosen set of packages.
type Summary struct {
	PackageCount      int            `json:"package_count"`
	AggregateCount    int            `json:"aggregate_count"`
	CommandCount      int            `json:"command_count"`
	EventCount        int            `json:"event_count"`
	FieldCount        int            `json:"field_count"`
	PIIFieldCount     int            `json:"pii_field_count"`
	SADRejectedCount  int            `json:"sad_rejected_count"`
	Classifications   map[string]int `json:"classifications"`
	EncryptionModes   map[string]int `json:"encryption_modes"`
}
