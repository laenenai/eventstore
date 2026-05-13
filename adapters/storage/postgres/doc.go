// Package postgres is the Postgres storage adapter, satisfying the
// es.Store contract. Uses pgx/v5 and sqlc-generated queries.
//
// See ADR 0009 (advisory-lock + sequence ordering), ADR 0017 (module
// layout), ADR 0018 (migrations + queries).
package postgres
