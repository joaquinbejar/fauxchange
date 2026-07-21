//! [`PgVenueJournal`] — the durable PostgreSQL backend for the venue's
//! write-ahead command/event journal, behind the **same**
//! [`VenueJournal`](crate::exchange::VenueJournal) trait shape as the in-memory
//! [`InMemoryVenueJournal`](crate::exchange::InMemoryVenueJournal) (#029,
//! [ADR-0006 §3](../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
//! [02 §6](../../docs/02-matching-architecture.md)).
//!
//! ## Only the store is swapped — the contract is unchanged
//!
//! This store implements the identical **synchronous** `VenueJournal` trait the
//! actor already writes through (`append` / `read_from` / `last_sequence` /
//! `header`). The single-writer actor's turn discipline — write-ahead append
//! before execute, `checked_add` sequence advance, `Ambiguous` → durable
//! tail-read-back, post-mutation append failure → seal — is **untouched**: those
//! mechanics live in [`crate::exchange::actor`] and drive against whichever store
//! is wired. This store only changes *where* the records land, mapping the three
//! append outcomes onto durable storage:
//!
//! | Actor expectation ([ADR-0006 §3](../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)) | Durable mechanic |
//! |---|---|
//! | **confirmed failure** → reuse `N` | a query error that definitely did not commit → [`JournalError::AppendFailed`] |
//! | **ambiguous** → durable tail read-back | a connection-lost error of unknown outcome → [`JournalError::Ambiguous`]; the actor then calls [`contains`](crate::exchange::VenueJournal) (a `SELECT`) |
//! | **idempotent re-append** → no-op | `INSERT … ON CONFLICT (underlying, underlying_sequence, kind) DO NOTHING`, then an equality check on the stored payload |
//!
//! ## The paired-record physical schema (`migrations/…_journal.sql`)
//!
//! One append-only stream per underlying: `journal_records` keyed
//! `(underlying, underlying_sequence, kind)` UNIQUE, so a command is never appended
//! twice; `journal_headers` carries the run `lineage_id` + envelope
//! `schema_version` per stream. `payload` stores the **exact `venue.v1`
//! `serde_json` bytes** of the [`JournalRecord`] envelope — the journal is a
//! durable, versioned, venue-owned wire contract, so the envelope is persisted
//! verbatim and the projected `kind` / `underlying_sequence` columns are only the
//! routing + unique-key index.
//!
//! ## The sync→async bridge
//!
//! The `VenueJournal` contract is synchronous (the actor drives it inside a
//! lock-free turn); `sqlx` is async. Each method bridges via
//! [`tokio::task::block_in_place`] + [`Handle::block_on`], which requires a
//! **multi-threaded** runtime — the binary's `#[tokio::main]` default and the
//! integration test's `flavor = "multi_thread"`. The durable append is
//! **write-ahead on the synchronous critical path** (HP-5), not a background flush
//! — a correctness requirement, measured separately from the in-memory HP-1 path
//! (#035). This store never regresses the in-memory default: it is used only when
//! `DATABASE_URL` is set.
//!
//! ## Parameterised queries only, no leaked driver error
//!
//! Every query is a compile-time-checked `sqlx::query!` / `query_scalar!` with
//! bound parameters (`$1, $2, …`); no value or identifier is ever interpolated. The
//! `sqlx::Error` is **never** returned through a `pub` signature — it is logged
//! server-side at the boundary and mapped to a typed [`JournalError`] carrying only
//! a non-secret label, and the `DATABASE_URL` is never logged
//! (`rules/global_rules.md` *SQL & Persistence*, *Security*).

use sqlx::PgPool;
use tokio::runtime::Handle;

use crate::db::error::DbError;
use crate::db::pool::DatabasePool;
use crate::exchange::event::SequenceNumber;
use crate::exchange::identity::{JournalHeader, LineageId};
use crate::exchange::journal::{JournalError, JournalRecord, RecordKind, VenueJournal};

// ============================================================================
// The durable store
// ============================================================================

/// The durable PostgreSQL [`VenueJournal`] for one underlying's paired
/// command/event stream.
///
/// Cloning the [`PgPool`] is cheap (it is an `Arc` internally). Constructed at boot
/// from an open [`DatabasePool`] within a multi-threaded tokio runtime.
#[derive(Clone)]
pub struct PgVenueJournal {
    pool: PgPool,
    handle: Handle,
    underlying: String,
    header: JournalHeader,
}

impl std::fmt::Debug for PgVenueJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgVenueJournal")
            .field("underlying", &self.underlying)
            .field("schema_version", &self.header.schema_version)
            .finish_non_exhaustive()
    }
}

impl PgVenueJournal {
    /// Opens the durable journal for an underlying on the **live write path**,
    /// ensuring the stream's header row exists for the run `header`
    /// (`INSERT … ON CONFLICT (underlying) DO NOTHING`, idempotent), **reading the
    /// persisted header back**, and verifying it equals `header` before caching it.
    ///
    /// This is the [`crate::state::AppState`] boot entry point (a fresh venue that
    /// persists durably going forward). Resuming a **non-empty** durable journal by
    /// re-executing it into a running venue is the boot-time replay driver (#030);
    /// the `(underlying, N, kind)` UNIQUE key makes an accidental resume fail
    /// **loud and safe** (a conflicting append is refused, never a silent overwrite),
    /// never a silent divergence.
    ///
    /// The read-back + compare closes the silent-mismatch gap (#112): a fresh run
    /// `lineage_id` (or a differing envelope `schema_version`) opened over a
    /// **pre-existing** durable stream would otherwise cache the *caller's* header
    /// while the stored records belong to the *old* lineage — corrupting
    /// replay/recovery identity (ids are namespaced by `lineage_id`). Instead this
    /// **refuses to open** with [`DbError::HeaderMismatch`], never caching a header
    /// that disagrees with the persisted stream. A first-time open (no prior header)
    /// persists the caller's header and reads it straight back, so it proceeds
    /// normally.
    ///
    /// # Errors
    ///
    /// [`DbError::Unavailable`] if constructed outside a tokio runtime (the
    /// sync→async bridge needs a runtime handle); [`DbError::Query`] if the header
    /// row cannot be ensured or read back; [`DbError::HeaderMismatch`] if the
    /// persisted header's `lineage_id` / `schema_version` disagrees with `header`.
    pub fn open(
        db: &DatabasePool,
        underlying: impl Into<String>,
        header: JournalHeader,
    ) -> Result<Self, DbError> {
        let handle = Handle::try_current().map_err(|_| DbError::Unavailable)?;
        let underlying = underlying.into();
        let pool = db.pool().clone();
        // Ensure the header row for THIS run's lineage (idempotent no-op if present),
        // then read it back and verify it matches — both inside ONE transaction, so
        // the check is atomic and the actor never starts against a stream whose
        // persisted identity disagrees with the header we would cache (#112).
        let stored = bridge(
            &handle,
            ensure_and_verify_header(&pool, &underlying, &header),
        )?;
        tracing::info!(underlying = %underlying, "durable journal opened (live write path)");
        Ok(Self {
            pool,
            handle,
            underlying,
            // Cache the STORED header: on a first-time open it equals `header`; on a
            // resume it is the persisted header we just verified equals `header`.
            header: stored,
        })
    }

    /// Opens the durable journal for **recovery**, reading the STORED header back so
    /// recovery rehydrates the run `lineage_id` and can refuse a newer-than-binary
    /// schema ([`crate::exchange::recover`]).
    ///
    /// Unlike [`open`](Self::open) this never writes: it reads the header the prior
    /// run persisted. The recovery reducer then checks
    /// [`JournalHeader::is_current_schema`] and either replays or refuses.
    ///
    /// # Errors
    ///
    /// [`DbError::Unavailable`] outside a tokio runtime; [`DbError::Query`] on a read
    /// failure; [`DbError::ValueRange`] with field `"journal header"` when no header
    /// row exists for `underlying` (nothing to recover).
    pub fn open_for_recovery(
        db: &DatabasePool,
        underlying: impl Into<String>,
    ) -> Result<Self, DbError> {
        let handle = Handle::try_current().map_err(|_| DbError::Unavailable)?;
        let underlying = underlying.into();
        let pool = db.pool().clone();
        let header = bridge(&handle, read_header(&pool, &underlying))?;
        tracing::info!(underlying = %underlying, "durable journal opened (recovery read path)");
        Ok(Self {
            pool,
            handle,
            underlying,
            header,
        })
    }

    /// The underlying ticker this journal serves.
    #[must_use]
    #[inline]
    pub fn underlying(&self) -> &str {
        &self.underlying
    }

    /// Bridges a synchronous `VenueJournal` call onto async `sqlx`. Requires a
    /// multi-threaded runtime (documented on the type).
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future,
    {
        tokio::task::block_in_place(|| self.handle.block_on(future))
    }

    /// The async body behind the synchronous [`VenueJournal::append`].
    async fn append_async(&self, record: &JournalRecord) -> Result<(), JournalError> {
        let sequence = record.sequence();
        let kind = record.kind();
        // Checked u64 → BIGINT: an out-of-domain sequence is a confirmed no-write.
        let seq_i64 = i64::try_from(sequence.get()).map_err(|_| {
            JournalError::AppendFailed("underlying_sequence out of BIGINT range".to_string())
        })?;
        let kind_token = kind_to_str(kind);
        let payload = serde_json::to_string(record)
            .map_err(|_| JournalError::AppendFailed("journal record serialise".to_string()))?;

        // Write-ahead insert: DO NOTHING on the (underlying, N, kind) key so a
        // re-append of the SAME record is a no-op (idempotent), never a duplicate.
        let result = sqlx::query!(
            r#"
            INSERT INTO journal_records (underlying, underlying_sequence, kind, payload)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (underlying, underlying_sequence, kind) DO NOTHING
            "#,
            self.underlying,
            seq_i64,
            kind_token,
            payload,
        )
        .execute(&self.pool)
        .await
        .map_err(|error| classify_append_error(&error))?;

        if result.rows_affected() == 1 {
            // The write-ahead command / paired event committed at N.
            return Ok(());
        }

        // Zero rows changed: a record already exists at (underlying, N, kind). Read
        // it back — an identical payload is the idempotent no-op the ambiguous /
        // reuse recovery paths depend on; a differing payload is an integrity
        // violation the append refuses rather than overwrite.
        let existing = sqlx::query_scalar!(
            r#"
            SELECT payload FROM journal_records
            WHERE underlying = $1 AND underlying_sequence = $2 AND kind = $3
            "#,
            self.underlying,
            seq_i64,
            kind_token,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "durable journal conflict read-back failed");
            JournalError::Backend {
                operation: "journal conflict read-back",
            }
        })?;

        match existing {
            Some(stored) if stored == payload => Ok(()),
            _ => Err(JournalError::Conflict { sequence, kind }),
        }
    }

    /// The async body behind the synchronous [`VenueJournal::read_from`].
    async fn read_from_async(
        &self,
        from: SequenceNumber,
    ) -> Result<Vec<JournalRecord>, JournalError> {
        let from_i64 = i64::try_from(from.get()).map_err(|_| JournalError::Backend {
            operation: "journal read_from bound out of range",
        })?;
        let payloads = sqlx::query_scalar!(
            r#"
            SELECT payload FROM journal_records
            WHERE underlying = $1 AND underlying_sequence >= $2
            ORDER BY id
            "#,
            self.underlying,
            from_i64,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "durable journal read_from failed");
            JournalError::Backend {
                operation: "journal read_from",
            }
        })?;

        payloads
            .into_iter()
            .map(|payload| {
                serde_json::from_str::<JournalRecord>(&payload).map_err(|error| {
                    tracing::error!(error = %error, "durable journal record decode failed");
                    JournalError::Backend {
                        operation: "journal record decode",
                    }
                })
            })
            .collect()
    }

    /// The async body behind the synchronous [`VenueJournal::last_sequence`].
    async fn last_sequence_async(&self) -> Result<Option<SequenceNumber>, JournalError> {
        let max = sqlx::query_scalar!(
            r#"
            SELECT max(underlying_sequence) AS "max_seq" FROM journal_records
            WHERE underlying = $1
            "#,
            self.underlying,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "durable journal last_sequence failed");
            JournalError::Backend {
                operation: "journal last_sequence",
            }
        })?;

        match max {
            Some(value) => {
                let seq = u64::try_from(value).map_err(|_| JournalError::Backend {
                    operation: "journal last_sequence out of range",
                })?;
                Ok(Some(SequenceNumber::new(seq)))
            }
            None => Ok(None),
        }
    }
}

impl VenueJournal for PgVenueJournal {
    #[inline]
    fn header(&self) -> &JournalHeader {
        &self.header
    }

    fn append(&mut self, record: JournalRecord) -> Result<(), JournalError> {
        self.block_on(self.append_async(&record))
    }

    fn read_from(&self, from: SequenceNumber) -> Result<Vec<JournalRecord>, JournalError> {
        self.block_on(self.read_from_async(from))
    }

    fn last_sequence(&self) -> Option<SequenceNumber> {
        // The trait's `last_sequence` is infallible; a durable read failure has no
        // error channel here, so it is logged and reported as `None` (a defensive,
        // never-corrupting fallback). The actor sets its start sequence at
        // construction and never depends on this on the turn path — the fallible
        // `read_from` / `contains` carry the real error for the tail read-back.
        match self.block_on(self.last_sequence_async()) {
            Ok(last) => last,
            Err(error) => {
                tracing::error!(error = %error, "durable journal last_sequence failed; reporting None");
                None
            }
        }
    }
}

// ============================================================================
// Header helpers
// ============================================================================

/// Ensures the header row for `underlying` exists for this run's `supplied` header,
/// then reads the **persisted** header back and verifies it equals `supplied` — the
/// ensure and the read-back run in **one transaction**, so the check is atomic
/// against a concurrent open and the caller never caches a header that disagrees
/// with the stored stream (#112).
///
/// On a first-time open the supplied header is inserted and read straight back
/// (equal by construction). On a resume over a **pre-existing** stream the persisted
/// header is returned; if its `lineage_id` or `schema_version` disagrees with
/// `supplied` the open is refused with [`DbError::HeaderMismatch`] rather than
/// caching a header that disagrees with the durably-stored records.
async fn ensure_and_verify_header(
    pool: &PgPool,
    underlying: &str,
    supplied: &JournalHeader,
) -> Result<JournalHeader, DbError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(query_err("begin journal header txn"))?;

    // Idempotent no-op if a header is already present for the underlying.
    sqlx::query!(
        r#"
        INSERT INTO journal_headers (underlying, lineage_id, schema_version)
        VALUES ($1, $2, $3)
        ON CONFLICT (underlying) DO NOTHING
        "#,
        underlying,
        supplied.lineage_id.as_str(),
        supplied.schema_version,
    )
    .execute(&mut *tx)
    .await
    .map_err(query_err("ensure journal header"))?;

    // Read the persisted header back inside the SAME transaction (read-your-writes:
    // whether we just inserted it or it pre-existed, a row is present here).
    let row = sqlx::query!(
        r#"
        SELECT lineage_id, schema_version FROM journal_headers WHERE underlying = $1
        "#,
        underlying,
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(query_err("read journal header"))?;

    let stored = match row {
        Some(row) => JournalHeader {
            schema_version: row.schema_version,
            lineage_id: LineageId::new(row.lineage_id),
        },
        // The INSERT above guarantees a row within this transaction; its absence is a
        // backend integrity fault, not a normal state — fail loudly, never proceed.
        None => {
            return Err(DbError::Query {
                operation: "verify journal header",
            });
        }
    };

    // `JournalHeader` holds exactly `lineage_id` + `schema_version`, so this equality
    // is precisely the (lineage, schema) comparison — a mismatch is refused.
    if stored != *supplied {
        // Non-secret venue identity only (run lineage + envelope schema) — logged
        // server-side for the operator; the domain boundary redacts it to a `500`.
        tracing::error!(
            underlying = %underlying,
            stored_lineage = %stored.lineage_id.as_str(),
            supplied_lineage = %supplied.lineage_id.as_str(),
            stored_schema = %stored.schema_version,
            supplied_schema = %supplied.schema_version,
            "durable journal header mismatch on open; refusing to open a fresh lineage/schema over a pre-existing stream"
        );
        // A mismatch means the row pre-existed (a matching INSERT would have made
        // `stored == supplied`), so the ON CONFLICT DO NOTHING wrote nothing; dropping
        // the transaction unwritten rolls back to exactly the prior durable state.
        return Err(DbError::HeaderMismatch {
            stored: describe_header(&stored),
            supplied: describe_header(supplied),
        });
    }

    tx.commit()
        .await
        .map_err(query_err("commit journal header txn"))?;
    Ok(stored)
}

/// Reads the stored header for `underlying`, or [`DbError::ValueRange`] with field
/// `"journal header"` when no stream exists (nothing to recover).
async fn read_header(pool: &PgPool, underlying: &str) -> Result<JournalHeader, DbError> {
    let row = sqlx::query!(
        r#"
        SELECT lineage_id, schema_version FROM journal_headers WHERE underlying = $1
        "#,
        underlying,
    )
    .fetch_optional(pool)
    .await
    .map_err(query_err("read journal header"))?;

    match row {
        Some(row) => Ok(JournalHeader {
            schema_version: row.schema_version,
            lineage_id: LineageId::new(row.lineage_id),
        }),
        None => Err(DbError::ValueRange {
            field: "journal header",
        }),
    }
}

// ============================================================================
// Conversions + error classification (non-secret labels)
// ============================================================================

/// Bridges a `DatabasePool`-constructing async body onto the captured runtime — the
/// sync path used at `open` time (before the store owns its own `block_on`).
fn bridge<F>(handle: &Handle, future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::task::block_in_place(|| handle.block_on(future))
}

/// A **non-secret** one-line description of a journal header — the run lineage + the
/// envelope schema — for the [`DbError::HeaderMismatch`] payload and the operator log
/// on a refused open. This is an error/log string, never SQL, so `format!` is
/// correct here (no query text is built from it).
#[must_use]
fn describe_header(header: &JournalHeader) -> String {
    format!(
        "lineage={} schema={}",
        header.lineage_id.as_str(),
        header.schema_version
    )
}

/// The wire-cased DB token for a [`RecordKind`] — the projected `kind` column and
/// the third component of the `(underlying, N, kind)` unique key.
#[inline]
const fn kind_to_str(kind: RecordKind) -> &'static str {
    match kind {
        RecordKind::Command => "command",
        RecordKind::Event => "event",
        RecordKind::Epoch => "epoch",
    }
}

/// Classifies a `sqlx::Error` from the write-ahead append into the write-ahead
/// protocol's two failure modes, logging the driver cause **server-side** (never
/// leaked, never carrying the `DATABASE_URL`):
///
/// - a **connection-lost** error whose commit outcome is genuinely unknown →
///   [`JournalError::Ambiguous`] (the actor resolves it by a durable tail
///   read-back, which is idempotent either way);
/// - every other error (a pool-acquire timeout that never sent the statement, a
///   constraint or protocol error) definitely did **not** commit →
///   [`JournalError::AppendFailed`] (the actor reuses `N`, book untouched).
fn classify_append_error(error: &sqlx::Error) -> JournalError {
    tracing::error!(error = %error, "durable journal write-ahead append failed");
    match error {
        // A mid-flight I/O failure may have committed on the server before the
        // connection dropped — the outcome is unknown, so it is AMBIGUOUS.
        sqlx::Error::Io(_) => {
            JournalError::Ambiguous("durable journal append (connection lost)".to_string())
        }
        // Everything else (pool timeout before send, a database/constraint/protocol
        // error) is a CONFIRMED no-write.
        _ => JournalError::AppendFailed("durable journal append".to_string()),
    }
}

/// Maps a `sqlx::Error` to a typed [`DbError::Query`] with a non-secret operation
/// label, logging the driver cause **server-side**.
fn query_err(operation: &'static str) -> impl FnOnce(sqlx::Error) -> DbError {
    move |error| {
        tracing::error!(operation, error = %error, "durable journal header query failed");
        DbError::Query { operation }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pure projection/mapping unit tests — no DB, so they run in the default
    // `cargo test` without Docker. The real-Postgres round-trip is the
    // `testcontainers` integration test (`tests/integration.rs`), run in the CI
    // migrations job.

    #[test]
    fn test_kind_token_maps_every_record_kind() {
        assert_eq!(kind_to_str(RecordKind::Command), "command");
        assert_eq!(kind_to_str(RecordKind::Event), "event");
        assert_eq!(kind_to_str(RecordKind::Epoch), "epoch");
    }

    #[test]
    fn test_classify_append_error_pool_timeout_is_confirmed_not_committed() {
        // A pool-acquire timeout never sent the statement → confirmed no-write →
        // the actor reuses N.
        match classify_append_error(&sqlx::Error::PoolTimedOut) {
            JournalError::AppendFailed(_) => {}
            other => panic!("expected AppendFailed for a pool timeout, got {other:?}"),
        }
    }

    #[test]
    fn test_describe_header_names_lineage_and_schema_non_secret() {
        // The `HeaderMismatch` payload / operator log carries only the venue's own
        // run lineage + envelope schema — never a secret, never row data.
        let described = describe_header(&JournalHeader::new(LineageId::new("run-7")));
        assert!(described.contains("run-7"), "names the run lineage");
        assert!(described.contains("venue.v1"), "names the envelope schema");
    }

    #[test]
    fn test_header_mismatch_display_carries_both_headers() {
        // The typed refusal names both sides so an operator can see WHICH lineage the
        // stored records belong to versus what this run tried to open with.
        let err = DbError::HeaderMismatch {
            stored: describe_header(&JournalHeader::new(LineageId::new("run-1"))),
            supplied: describe_header(&JournalHeader::new(LineageId::new("run-2"))),
        };
        let rendered = err.to_string();
        assert!(rendered.contains("run-1"), "names the stored lineage");
        assert!(rendered.contains("run-2"), "names the supplied lineage");
    }

    #[test]
    fn test_classify_append_error_io_is_ambiguous() {
        // A mid-flight I/O failure may or may not have committed → ambiguous → the
        // actor resolves it by a durable tail read-back.
        let io = sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        match classify_append_error(&io) {
            JournalError::Ambiguous(_) => {}
            other => panic!("expected Ambiguous for a connection-lost I/O error, got {other:?}"),
        }
    }
}
