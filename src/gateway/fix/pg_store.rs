//! [`PgFixSessionStore`] — the durable PostgreSQL backend for the acceptor's
//! account-keyed FIX session state, behind the **same** [`FixSessionStore`] trait
//! as the in-memory [`InMemoryFixSessionStore`] (#095, #038,
//! [ADR-0010](../../../docs/adr/0010-fix-session-account-binding.md),
//! [03 §5.2](../../../docs/03-protocol-surfaces.md#52-session-management--the-acceptor-fsm-and-checked-counters)).
//!
//! ## Only the store is swapped — the contract is unchanged
//!
//! This store implements the identical **synchronous** [`FixSessionStore`] trait
//! the session FSM already calls (`load_counters` / `save_counters` /
//! `store_outbound` / `outbound_range` / `record_reset` / `reset_events`), so the
//! acceptor is backend-agnostic: [`select_fix_session_store`] wires the durable
//! backend when `DATABASE_URL` is set and the in-memory one otherwise — exactly the
//! #029 [`PgVenueJournal`](crate::db::PgVenueJournal) selection pattern. The only
//! thing that changes is **where** the counters / resend log / reset audit land, so
//! a *process restart* (not just a reconnect) resumes each
//! `(account_id, comp_id_tuple)` session from its persisted state.
//!
//! ## Layering: this lives in the transport layer, not `src/db`
//!
//! Unlike [`PgVenueJournal`](crate::db::PgVenueJournal) /
//! [`PgExecutionsStore`](crate::db::PgExecutionsStore) — which implement **domain**
//! (`src/exchange`) traits and so live in `src/db` — [`FixSessionStore`] is a
//! **transport** trait ([`super::store`]). Persistence must never import the
//! transport layer, so its durable backend lives **here** in the gateway and
//! depends *inward* on [`crate::db`] ([`DatabasePool`] / [`DbError`]) — the allowed
//! transport → persistence direction (`CLAUDE.md` *Module Boundaries*).
//!
//! ## Faithful to the in-memory semantics
//!
//! The [`fix_session_counters`](../../../migrations/20260716120600_fix_sessions.sql)
//! table doubles as the **key registry** — a row (created on first touch with the
//! `1`-based defaults) means the key is "known", exactly like an in-memory `Slot`,
//! so the [`MAX_SESSION_KEYS`] keyspace bound is enforced by a `count(*)` of that
//! table. The resend log **appends** (no unique key on `seq`) and **evicts the
//! oldest** past the count / byte bounds, matching the in-memory `Vec` push +
//! front-eviction; a range read is `ORDER BY seq ASC, id ASC` (the in-memory stable
//! `sort_by_key(seq)`). The reset audit is a bounded, append-only ring
//! (`ORDER BY id ASC`, oldest first). This is transport session state, **not** the
//! sequenced determinism path — but the resend log is faithful to the byte-exact
//! frame at its `MsgSeqNum`.
//!
//! ## The sync→async bridge
//!
//! [`FixSessionStore`] is synchronous (the FSM calls it inside a session task);
//! `sqlx` is async. Each method bridges via [`tokio::task::block_in_place`] +
//! [`Handle::block_on`], which requires a **multi-threaded** runtime — the binary's
//! `#[tokio::main]` default and the integration test's `flavor = "multi_thread"`.
//! The session task is the **sole writer for its own key** while live, so there is
//! no cross-writer contention on a key.
//!
//! ## Parameterised queries only, no leaked driver error, no logged frame
//!
//! Every query is a compile-time-checked `sqlx::query!` / `query_scalar!` with
//! bound parameters (`$1, $2, …`); no value or identifier is ever interpolated. The
//! `sqlx::Error` is **never** returned through a `pub` signature — it is logged
//! server-side and mapped to a typed [`SessionStoreError::Backend`] carrying only a
//! non-secret `&'static str` label, the `DATABASE_URL` is never logged, and the
//! stored **frame bytes are never logged** (they may carry secrets)
//! (`rules/global_rules.md` *SQL & Persistence*, *Security*).

use std::sync::Arc;

use sqlx::PgPool;
use tokio::runtime::Handle;

use crate::db::error::DbError;
use crate::db::pool::DatabasePool;

use super::store::{
    FixSessionStore, InMemoryFixSessionStore, MAX_RESET_EVENTS_PER_KEY, MAX_SESSION_KEYS,
    MAX_STORED_OUTBOUND_BYTES_PER_KEY, MAX_STORED_OUTBOUND_PER_KEY, ResetTrigger,
    SequenceResetEvent, SessionCounters, SessionKey, SessionStoreError, StoredOutbound,
};

// ============================================================================
// Backend selection (the #029 DATABASE_URL-set / -unset pattern)
// ============================================================================

/// Resolves a FIX session-store backend behind the [`FixSessionStore`] contract:
/// the durable [`PgFixSessionStore`] when a [`DatabasePool`] is open, else the
/// in-memory [`InMemoryFixSessionStore`] — so the acceptor is backend-agnostic and
/// the same trait calls yield the same observable behavior on either.
///
/// This mirrors [`select_executions_store`](crate::db::select_executions_store):
/// pass `Some(pool)` on the durable path (`DATABASE_URL` set), `None` for the
/// in-memory venue.
///
/// # Errors
///
/// [`DbError::Unavailable`] if a durable store is requested outside a tokio runtime
/// (the sync→async bridge needs a runtime handle). The in-memory path is
/// infallible.
pub fn select_fix_session_store(
    db: Option<&DatabasePool>,
) -> Result<Arc<dyn FixSessionStore>, DbError> {
    match db {
        Some(pool) => {
            let store = PgFixSessionStore::new(pool)?;
            tracing::info!("fix session store: durable postgres");
            Ok(Arc::new(store))
        }
        None => {
            tracing::info!("fix session store: in-memory (no DATABASE_URL)");
            Ok(Arc::new(InMemoryFixSessionStore::new()))
        }
    }
}

// ============================================================================
// The durable store
// ============================================================================

/// The durable PostgreSQL [`FixSessionStore`].
///
/// Cloning the [`PgPool`] is cheap (it is an `Arc` internally). Constructed at boot
/// from an open [`DatabasePool`] within a multi-threaded tokio runtime.
#[derive(Clone)]
pub struct PgFixSessionStore {
    pool: PgPool,
    handle: Handle,
}

impl std::fmt::Debug for PgFixSessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgFixSessionStore")
            .field("pool_size", &self.pool.size())
            .finish_non_exhaustive()
    }
}

impl PgFixSessionStore {
    /// Wires a durable FIX session store over an open [`DatabasePool`].
    ///
    /// Captures the current runtime [`Handle`] so the synchronous trait methods can
    /// bridge onto async `sqlx`.
    ///
    /// # Errors
    ///
    /// [`DbError::Unavailable`] if constructed outside a tokio runtime (the
    /// sync→async bridge needs a runtime handle). Constructed at boot / in tests,
    /// both within a runtime.
    pub fn new(db: &DatabasePool) -> Result<Self, DbError> {
        let handle = Handle::try_current().map_err(|_| DbError::Unavailable)?;
        Ok(Self {
            pool: db.pool().clone(),
            handle,
        })
    }

    /// Bridges a synchronous [`FixSessionStore`] call onto async `sqlx`. Requires a
    /// multi-threaded runtime (documented on the type).
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future,
    {
        tokio::task::block_in_place(|| self.handle.block_on(future))
    }

    /// The async body behind [`FixSessionStore::load_counters`].
    async fn load_counters_async(
        &self,
        key: &SessionKey,
    ) -> Result<SessionCounters, SessionStoreError> {
        let row = sqlx::query!(
            r#"
            SELECT next_sender_seq, next_target_seq
            FROM fix_session_counters
            WHERE account_id = $1 AND sender_comp_id = $2 AND target_comp_id = $3
            "#,
            key.account.as_str(),
            key.sender_comp_id,
            key.target_comp_id,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend_err("load fix session counters"))?;

        match row {
            // An unknown key is a fresh session — both counters at FIRST_SEQ_NUM.
            None => Ok(SessionCounters::default()),
            Some(row) => Ok(SessionCounters {
                next_sender_seq: i64_to_u64(row.next_sender_seq, "next_sender_seq")?,
                next_target_seq: i64_to_u64(row.next_target_seq, "next_target_seq")?,
            }),
        }
    }

    /// The async body behind [`FixSessionStore::save_counters`].
    ///
    /// A single atomic upsert that ALSO enforces the [`MAX_SESSION_KEYS`] keyspace
    /// bound: an existing key always updates; a **new** key inserts only while the
    /// registry is under the ceiling. `rows_affected == 0` therefore means a new key
    /// was refused at the cap → [`SessionStoreError::KeyspaceFull`].
    async fn save_counters_async(
        &self,
        key: &SessionKey,
        counters: SessionCounters,
    ) -> Result<(), SessionStoreError> {
        let next_sender = u64_to_i64(counters.next_sender_seq, "next_sender_seq")?;
        let next_target = u64_to_i64(counters.next_target_seq, "next_target_seq")?;
        let keyspace = keyspace_ceiling();

        let result = sqlx::query!(
            r#"
            INSERT INTO fix_session_counters
                (account_id, sender_comp_id, target_comp_id, next_sender_seq, next_target_seq)
            SELECT $1, $2, $3, $4, $5
            WHERE (
                EXISTS (
                    SELECT 1 FROM fix_session_counters
                    WHERE account_id = $1 AND sender_comp_id = $2 AND target_comp_id = $3
                )
                OR (SELECT count(*) FROM fix_session_counters) < $6
            )
            ON CONFLICT (account_id, sender_comp_id, target_comp_id) DO UPDATE SET
                next_sender_seq = EXCLUDED.next_sender_seq,
                next_target_seq = EXCLUDED.next_target_seq,
                updated_at = now()
            "#,
            key.account.as_str(),
            key.sender_comp_id,
            key.target_comp_id,
            next_sender,
            next_target,
            keyspace,
        )
        .execute(&self.pool)
        .await
        .map_err(backend_err("save fix session counters"))?;

        if result.rows_affected() == 0 {
            return Err(SessionStoreError::KeyspaceFull {
                max: MAX_SESSION_KEYS,
            });
        }
        Ok(())
    }

    /// The async body behind [`FixSessionStore::store_outbound`].
    ///
    /// Registers the key (cap-guarded), appends the frame, then evicts the oldest
    /// frames past the count / byte bounds — all in one transaction, so a resend
    /// read never sees a partially-bounded log.
    async fn store_outbound_async(
        &self,
        key: &SessionKey,
        seq: u64,
        frame: &[u8],
    ) -> Result<(), SessionStoreError> {
        let seq = u64_to_i64(seq, "outbound_seq")?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(backend_err("begin fix store_outbound txn"))?;

        ensure_registered(&mut tx, key).await?;

        sqlx::query!(
            r#"
            INSERT INTO fix_session_outbound
                (account_id, sender_comp_id, target_comp_id, seq, frame)
            VALUES ($1, $2, $3, $4, $5)
            "#,
            key.account.as_str(),
            key.sender_comp_id,
            key.target_comp_id,
            seq,
            frame,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend_err("insert fix outbound frame"))?;

        // Evict the OLDEST frames until BOTH the count and byte bounds hold — the
        // surviving suffix is the newest rows whose rank <= count-cap AND whose
        // cumulative bytes (from newest) <= byte-cap, exactly the in-memory
        // push-then-evict-front semantics.
        sqlx::query!(
            r#"
            DELETE FROM fix_session_outbound
            WHERE id IN (
                SELECT id FROM (
                    SELECT
                        id,
                        row_number() OVER (ORDER BY id DESC) AS rn,
                        sum(octet_length(frame)) OVER (ORDER BY id DESC ROWS UNBOUNDED PRECEDING)
                            AS cum_bytes
                    FROM fix_session_outbound
                    WHERE account_id = $1 AND sender_comp_id = $2 AND target_comp_id = $3
                ) ranked
                WHERE ranked.rn > $4 OR ranked.cum_bytes > $5
            )
            "#,
            key.account.as_str(),
            key.sender_comp_id,
            key.target_comp_id,
            outbound_count_ceiling(),
            outbound_bytes_ceiling(),
        )
        .execute(&mut *tx)
        .await
        .map_err(backend_err("evict fix outbound frames"))?;

        tx.commit()
            .await
            .map_err(backend_err("commit fix store_outbound txn"))?;
        Ok(())
    }

    /// The async body behind [`FixSessionStore::outbound_range`].
    async fn outbound_range_async(
        &self,
        key: &SessionKey,
        begin: u64,
        end: u64,
    ) -> Result<Vec<StoredOutbound>, SessionStoreError> {
        // `begin` / `end` are client-controlled resend bounds; the stored `seq` is a
        // non-negative BIGINT (<= i64::MAX), so a bound above i64::MAX cannot match a
        // stored seq. Clamp the range FILTER bound (never a counter mutation) to the
        // representable domain rather than error on a valid-but-large `u64`. `end`'s
        // `0` sentinel ("to the latest") survives (`0` is in range).
        let begin = i64::try_from(begin).unwrap_or(i64::MAX);
        let end = i64::try_from(end).unwrap_or(i64::MAX);

        let rows = sqlx::query!(
            r#"
            SELECT seq, frame
            FROM fix_session_outbound
            WHERE account_id = $1 AND sender_comp_id = $2 AND target_comp_id = $3
              AND seq >= $4
              AND ($5::bigint = 0 OR seq <= $5::bigint)
            ORDER BY seq ASC, id ASC
            "#,
            key.account.as_str(),
            key.sender_comp_id,
            key.target_comp_id,
            begin,
            end,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend_err("read fix outbound range"))?;

        rows.into_iter()
            .map(|row| {
                Ok(StoredOutbound {
                    seq: i64_to_u64(row.seq, "outbound_seq")?,
                    frame: row.frame,
                })
            })
            .collect()
    }

    /// The async body behind [`FixSessionStore::record_reset`].
    ///
    /// Registers the key (cap-guarded), appends the audit event, evicts the oldest
    /// past the ring cap, and persists the new counters — all atomically, so a
    /// reconnect resumes from the reset state and the reset survives an audit.
    async fn record_reset_async(
        &self,
        key: &SessionKey,
        event: SequenceResetEvent,
        counters: SessionCounters,
    ) -> Result<(), SessionStoreError> {
        let at_ms = u64_to_i64(event.at_ms, "reset_at_ms")?;
        let old_sender = u64_to_i64(event.old_next_sender_seq, "old_next_sender_seq")?;
        let old_target = u64_to_i64(event.old_next_target_seq, "old_next_target_seq")?;
        let new_sender = u64_to_i64(event.new_next_sender_seq, "new_next_sender_seq")?;
        let new_target = u64_to_i64(event.new_next_target_seq, "new_next_target_seq")?;
        let counter_sender = u64_to_i64(counters.next_sender_seq, "next_sender_seq")?;
        let counter_target = u64_to_i64(counters.next_target_seq, "next_target_seq")?;
        let trigger = trigger_to_str(event.trigger);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(backend_err("begin fix record_reset txn"))?;

        ensure_registered(&mut tx, key).await?;

        sqlx::query!(
            r#"
            INSERT INTO fix_session_resets (
                account_id, sender_comp_id, target_comp_id, at_ms, trigger,
                old_next_sender_seq, old_next_target_seq, new_next_sender_seq, new_next_target_seq
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
            key.account.as_str(),
            key.sender_comp_id,
            key.target_comp_id,
            at_ms,
            trigger,
            old_sender,
            old_target,
            new_sender,
            new_target,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend_err("insert fix reset event"))?;

        // Bounded audit ring: evict the oldest events past the per-key cap.
        sqlx::query!(
            r#"
            DELETE FROM fix_session_resets
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, row_number() OVER (ORDER BY id DESC) AS rn
                    FROM fix_session_resets
                    WHERE account_id = $1 AND sender_comp_id = $2 AND target_comp_id = $3
                ) ranked
                WHERE ranked.rn > $4
            )
            "#,
            key.account.as_str(),
            key.sender_comp_id,
            key.target_comp_id,
            reset_ring_ceiling(),
        )
        .execute(&mut *tx)
        .await
        .map_err(backend_err("evict fix reset events"))?;

        // Persist the post-reset counters (the key row exists via ensure_registered).
        sqlx::query!(
            r#"
            UPDATE fix_session_counters
            SET next_sender_seq = $4, next_target_seq = $5, updated_at = now()
            WHERE account_id = $1 AND sender_comp_id = $2 AND target_comp_id = $3
            "#,
            key.account.as_str(),
            key.sender_comp_id,
            key.target_comp_id,
            counter_sender,
            counter_target,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend_err("persist fix reset counters"))?;

        tx.commit()
            .await
            .map_err(backend_err("commit fix record_reset txn"))?;
        Ok(())
    }

    /// The async body behind [`FixSessionStore::reset_events`].
    async fn reset_events_async(
        &self,
        key: &SessionKey,
    ) -> Result<Vec<SequenceResetEvent>, SessionStoreError> {
        let rows = sqlx::query!(
            r#"
            SELECT at_ms, trigger, old_next_sender_seq, old_next_target_seq,
                   new_next_sender_seq, new_next_target_seq
            FROM fix_session_resets
            WHERE account_id = $1 AND sender_comp_id = $2 AND target_comp_id = $3
            ORDER BY id ASC
            "#,
            key.account.as_str(),
            key.sender_comp_id,
            key.target_comp_id,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend_err("read fix reset events"))?;

        rows.into_iter()
            .map(|row| {
                Ok(SequenceResetEvent {
                    at_ms: i64_to_u64(row.at_ms, "reset_at_ms")?,
                    trigger: trigger_from_str(&row.trigger)?,
                    old_next_sender_seq: i64_to_u64(
                        row.old_next_sender_seq,
                        "old_next_sender_seq",
                    )?,
                    old_next_target_seq: i64_to_u64(
                        row.old_next_target_seq,
                        "old_next_target_seq",
                    )?,
                    new_next_sender_seq: i64_to_u64(
                        row.new_next_sender_seq,
                        "new_next_sender_seq",
                    )?,
                    new_next_target_seq: i64_to_u64(
                        row.new_next_target_seq,
                        "new_next_target_seq",
                    )?,
                })
            })
            .collect()
    }
}

impl FixSessionStore for PgFixSessionStore {
    fn load_counters(&self, key: &SessionKey) -> Result<SessionCounters, SessionStoreError> {
        self.block_on(self.load_counters_async(key))
    }

    fn save_counters(
        &self,
        key: &SessionKey,
        counters: SessionCounters,
    ) -> Result<(), SessionStoreError> {
        self.block_on(self.save_counters_async(key, counters))
    }

    fn store_outbound(
        &self,
        key: &SessionKey,
        seq: u64,
        frame: &[u8],
    ) -> Result<(), SessionStoreError> {
        self.block_on(self.store_outbound_async(key, seq, frame))
    }

    fn outbound_range(
        &self,
        key: &SessionKey,
        begin: u64,
        end: u64,
    ) -> Result<Vec<StoredOutbound>, SessionStoreError> {
        self.block_on(self.outbound_range_async(key, begin, end))
    }

    fn record_reset(
        &self,
        key: &SessionKey,
        event: SequenceResetEvent,
        counters: SessionCounters,
    ) -> Result<(), SessionStoreError> {
        self.block_on(self.record_reset_async(key, event, counters))
    }

    fn reset_events(&self, key: &SessionKey) -> Result<Vec<SequenceResetEvent>, SessionStoreError> {
        self.block_on(self.reset_events_async(key))
    }
}

// ============================================================================
// Helpers (keyspace registry, ceilings, conversions, error mapping)
// ============================================================================

/// Ensures `key`'s registry row exists in `fix_session_counters` (the unified key
/// registry, created on first touch with the `1`-based defaults — an in-memory
/// `Slot`'s default counters), enforcing the [`MAX_SESSION_KEYS`] keyspace bound on
/// a **new** key.
///
/// The session task is the **sole writer for its own key** (documented on
/// [`FixSessionStore`]), so a "not present, then insert affected 0 rows" outcome for
/// a given key can only mean the keyspace is at its ceiling (never a same-key race)
/// — [`SessionStoreError::KeyspaceFull`]. Different keys inserting concurrently can
/// overshoot the ceiling by at most the (acceptor-bounded) live-session concurrency,
/// a benign, bounded overshoot for a DoS ceiling.
async fn ensure_registered(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &SessionKey,
) -> Result<(), SessionStoreError> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM fix_session_counters
            WHERE account_id = $1 AND sender_comp_id = $2 AND target_comp_id = $3
        ) AS "exists!"
        "#,
        key.account.as_str(),
        key.sender_comp_id,
        key.target_comp_id,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(backend_err("check fix session key registered"))?;

    if exists {
        return Ok(());
    }

    let inserted = sqlx::query!(
        r#"
        INSERT INTO fix_session_counters
            (account_id, sender_comp_id, target_comp_id, next_sender_seq, next_target_seq)
        SELECT $1, $2, $3, 1, 1
        WHERE (SELECT count(*) FROM fix_session_counters) < $4
        ON CONFLICT (account_id, sender_comp_id, target_comp_id) DO NOTHING
        "#,
        key.account.as_str(),
        key.sender_comp_id,
        key.target_comp_id,
        keyspace_ceiling(),
    )
    .execute(&mut **tx)
    .await
    .map_err(backend_err("register fix session key"))?;

    if inserted.rows_affected() == 0 {
        return Err(SessionStoreError::KeyspaceFull {
            max: MAX_SESSION_KEYS,
        });
    }
    Ok(())
}

/// The [`MAX_SESSION_KEYS`] ceiling as a bound `BIGINT`. The constant is far below
/// `i64::MAX`; the total conversion falls back to `i64::MAX` rather than panic.
#[inline]
fn keyspace_ceiling() -> i64 {
    i64::try_from(MAX_SESSION_KEYS).unwrap_or(i64::MAX)
}

/// The [`MAX_STORED_OUTBOUND_PER_KEY`] resend-log count ceiling as a bound `BIGINT`.
#[inline]
fn outbound_count_ceiling() -> i64 {
    i64::try_from(MAX_STORED_OUTBOUND_PER_KEY).unwrap_or(i64::MAX)
}

/// The [`MAX_STORED_OUTBOUND_BYTES_PER_KEY`] resend-log byte ceiling as a bound
/// `BIGINT`.
#[inline]
fn outbound_bytes_ceiling() -> i64 {
    i64::try_from(MAX_STORED_OUTBOUND_BYTES_PER_KEY).unwrap_or(i64::MAX)
}

/// The [`MAX_RESET_EVENTS_PER_KEY`] audit-ring ceiling as a bound `BIGINT`.
#[inline]
fn reset_ring_ceiling() -> i64 {
    i64::try_from(MAX_RESET_EVENTS_PER_KEY).unwrap_or(i64::MAX)
}

/// The wire-cased DB token for a [`ResetTrigger`] — the `trigger` column value,
/// pinned by the migration's CHECK constraint.
#[inline]
const fn trigger_to_str(trigger: ResetTrigger) -> &'static str {
    match trigger {
        ResetTrigger::LogonReset => "logon_reset",
        ResetTrigger::SequenceReset => "sequence_reset",
    }
}

/// Parses a stored `trigger` token, or a typed [`SessionStoreError::Backend`] on an
/// out-of-vocabulary value (the CHECK constraint makes this unreachable).
#[inline]
fn trigger_from_str(token: &str) -> Result<ResetTrigger, SessionStoreError> {
    match token {
        "logon_reset" => Ok(ResetTrigger::LogonReset),
        "sequence_reset" => Ok(ResetTrigger::SequenceReset),
        _ => Err(SessionStoreError::Backend("unknown fix reset trigger")),
    }
}

/// Narrows a venue `u64` (a checked, non-wrapping FIX `MsgSeqNum` / venue-clock ms)
/// to a `BIGINT` `i64`, checked — an out-of-range value is a typed
/// [`SessionStoreError::Backend`], never a silent truncation.
#[inline]
fn u64_to_i64(value: u64, field: &'static str) -> Result<i64, SessionStoreError> {
    i64::try_from(value).map_err(|_| {
        tracing::error!(
            field,
            "fix session store value out of BIGINT range on write"
        );
        SessionStoreError::Backend("fix session value out of range")
    })
}

/// Widens a stored `BIGINT` back to a venue `u64`, checked (a negative stored value
/// — which the CHECK constraints make unreachable — is a typed
/// [`SessionStoreError::Backend`]).
#[inline]
fn i64_to_u64(value: i64, field: &'static str) -> Result<u64, SessionStoreError> {
    u64::try_from(value).map_err(|_| {
        tracing::error!(field, "fix session store value out of u64 range on read");
        SessionStoreError::Backend("fix session value out of range")
    })
}

/// Maps a `sqlx::Error` to a typed [`SessionStoreError::Backend`] with a non-secret
/// `&'static str` label, logging the driver cause **server-side** (never leaked to
/// a client, never carrying the `DATABASE_URL`, never carrying a stored frame).
fn backend_err(operation: &'static str) -> impl FnOnce(sqlx::Error) -> SessionStoreError {
    move |error| {
        tracing::error!(operation, error = %error, "durable fix session store query failed");
        SessionStoreError::Backend(operation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pure projection/mapping unit tests — no DB, so they run in the default
    // `cargo test` without Docker. The real-Postgres round-trip / restart-durability
    // / parity tests are the `#[ignore]`-gated integration tests in `tests/db.rs`
    // (the CI migrations job).

    #[test]
    fn test_trigger_token_round_trips() {
        for trigger in [ResetTrigger::LogonReset, ResetTrigger::SequenceReset] {
            match trigger_from_str(trigger_to_str(trigger)) {
                Ok(parsed) => assert_eq!(parsed, trigger),
                Err(e) => panic!("trigger round-trip failed: {e}"),
            }
        }
    }

    #[test]
    fn test_unknown_trigger_token_is_backend_error() {
        match trigger_from_str("gap_fill") {
            Err(SessionStoreError::Backend(label)) => {
                assert!(label.contains("trigger"), "names the trigger vocabulary");
            }
            other => panic!("expected a Backend error, got {other:?}"),
        }
    }

    #[test]
    fn test_u64_i64_round_trip_and_range_check() {
        for value in [0_u64, 1, 4_096, u64::try_from(i64::MAX).unwrap_or(0)] {
            let narrowed = u64_to_i64(value, "seq").expect("in range");
            assert_eq!(i64_to_u64(narrowed, "seq").expect("in range"), value);
        }
        // A `u64` above `i64::MAX` is a typed range error, never a truncation.
        let over = (i64::MAX as u64) + 1;
        match u64_to_i64(over, "seq") {
            Err(SessionStoreError::Backend(_)) => {}
            other => panic!("expected a Backend range error, got {other:?}"),
        }
        // A negative `BIGINT` widening is a typed range error.
        match i64_to_u64(-1, "seq") {
            Err(SessionStoreError::Backend(_)) => {}
            other => panic!("expected a Backend range error, got {other:?}"),
        }
    }

    #[test]
    fn test_ceilings_match_the_in_memory_bounds() {
        // The durable eviction ceilings are the SAME constants the in-memory store
        // bounds on — parity of the DoS bounds across both backends.
        assert_eq!(
            outbound_count_ceiling(),
            i64::try_from(MAX_STORED_OUTBOUND_PER_KEY).unwrap_or(i64::MAX)
        );
        assert_eq!(
            outbound_bytes_ceiling(),
            i64::try_from(MAX_STORED_OUTBOUND_BYTES_PER_KEY).unwrap_or(i64::MAX)
        );
        assert_eq!(
            reset_ring_ceiling(),
            i64::try_from(MAX_RESET_EVENTS_PER_KEY).unwrap_or(i64::MAX)
        );
        assert_eq!(
            keyspace_ceiling(),
            i64::try_from(MAX_SESSION_KEYS).unwrap_or(i64::MAX)
        );
    }
}
