// Package outbox hosts the outbox table primitives and drain helpers.
// The outbox is the durability seam between the writer transaction and
// the EventPublisher.
//
// See ADR 0014.
package outbox
