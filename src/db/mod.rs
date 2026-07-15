//! Persistence layer: optional `sqlx`/PostgreSQL storage for the
//! journal, executions, and venue configuration. Fully in-memory when
//! `DATABASE_URL` is unset.
//!
//! Governed by `docs/06-deployment.md`.
