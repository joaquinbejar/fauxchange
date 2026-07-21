//! The typed database boundary error, [`DbError`], and its mappings.
//!
//! `sqlx::Error` **never** leaves the [`crate::db`] module through a `pub`
//! signature (`rules/global_rules.md` *SQL & Persistence*): every driver failure
//! is mapped here to a [`DbError`] carrying only a **non-secret** operation label,
//! and the underlying `sqlx::Error` is logged **server-side** at the repository
//! boundary — never returned to a client and never carrying the `DATABASE_URL`
//! (which `sqlx` masks and which this module never logs, [08 §7](../../docs/08-threat-model.md#7-secrets-handling)).
//!
//! `DbError` folds outward on two boundaries:
//!
//! - into [`StoreError::Backend`] — so the durable
//!   [`PgExecutionsStore`](crate::db::PgExecutionsStore) reports through the
//!   **same** #008 store contract as the in-memory backend;
//! - into [`VenueError`] — the domain boundary the bootstrap / handler paths
//!   translate through (a redacted internal `500`).

use crate::error::VenueError;
use crate::exchange::StoreError;

/// A failure at the durable-persistence boundary — the typed error the
/// [`crate::db`] repositories return **instead of** leaking `sqlx::Error`.
///
/// Every variant carries only a **non-secret** label (a `&'static str` operation
/// name or field), never the query text, the row data, or the `DATABASE_URL`. The
/// full driver cause is logged server-side where the error is first mapped
/// (`rules/global_rules.md` *Security*, [08 §7](../../docs/08-threat-model.md#7-secrets-handling)).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DbError {
    /// The `PgPool` could not be opened from the configured `DATABASE_URL`
    /// (connection refused, auth failure, TLS failure, or an acquire timeout).
    /// The cause is logged server-side; this carries no connection detail.
    #[error("database connection failed")]
    Connect,
    /// Applying the embedded `migrations/` at boot failed (a schema conflict, an
    /// unreachable DB). The cause is logged server-side.
    #[error("database migration failed")]
    Migrate,
    /// A query against the durable store failed. Carries only the **static**
    /// operation label (e.g. `"record execution"`), never the SQL or row data.
    #[error("database query failed: {operation}")]
    Query {
        /// The non-secret operation label naming the failed repository call.
        operation: &'static str,
    },
    /// A stored value did not fit the venue's integer domain on read/write — a
    /// `BIGINT` outside the `u64`/`i64` range the field maps to. Unreachable for
    /// the venue's admission-bounded cents/quantities; the operation fails loudly
    /// rather than silently truncate. Carries only the **static** field label.
    #[error("database value out of range for field: {field}")]
    ValueRange {
        /// The non-secret field label whose value left its integer domain.
        field: &'static str,
    },
    /// The persistence layer was asked to run without an open pool (a wiring
    /// bug) — the DB-less path must use the in-memory backend, never this one.
    #[error("database backend unavailable: no open pool")]
    Unavailable,
}

impl From<DbError> for StoreError {
    /// Folds a durable-backend failure onto the #008 store contract's reserved
    /// [`StoreError::Backend`] variant — so a [`PgExecutionsStore`] failure is
    /// reported through the **same** trait the in-memory backend implements, and
    /// the gateway never learns which backend produced it. The carried string is
    /// the non-secret [`DbError`] `Display` (an operation/field label), never a
    /// driver detail or secret.
    ///
    /// [`PgExecutionsStore`]: crate::db::PgExecutionsStore
    #[inline]
    fn from(error: DbError) -> Self {
        StoreError::Backend(error.to_string())
    }
}

impl From<DbError> for VenueError {
    /// Folds a durable-backend failure into the domain boundary as a **redacted
    /// internal** failure ([`VenueError::JournalUnavailable`], HTTP `500`, cause
    /// redacted on every client surface). A DB failure is an operational/internal
    /// condition, never a client-input error, so it never surfaces its detail.
    #[cold]
    #[inline]
    fn from(error: DbError) -> Self {
        // The non-secret label stays in `Display`/`source` for server-side logs;
        // the client sees only the generic redacted internal message via
        // `VenueError::JournalUnavailable`.
        tracing::debug!(error = %error, "mapping a database error to a redacted internal failure");
        VenueError::JournalUnavailable
    }
}
