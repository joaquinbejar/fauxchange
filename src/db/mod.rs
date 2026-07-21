//! Persistence layer: **optional** `sqlx`/PostgreSQL storage for the executions
//! log, the venue config tables, and the account registry. The venue runs fully
//! **in-memory** when `DATABASE_URL` is unset ([06 §6](../../docs/06-deployment.md#6-persistence)).
//!
//! ## Optional, runtime-selected by `DATABASE_URL`
//!
//! `sqlx` is a **normal (non-feature-gated)** dependency selected at **runtime**,
//! not by a cargo feature: the in-memory backend is always compiled and serves
//! when `DATABASE_URL` is unset; the [`DatabasePool`] opens (and the embedded
//! `migrations/` run) only when it is set — one binary, both modes, one image.
//! [`select_executions_store`] resolves **either** backend behind the **same**
//! #008 [`ExecutionsStore`] contract.
//!
//! ## The durable store is NOT yet the live fan-out backend (#023)
//!
//! **This is documented, not silently implied.** In #023 [`select_executions_store`]
//! is **not yet invoked by [`AppState`](crate::state::AppState)**: the live actor
//! fan-out (the write path) and `AppState::executions()` (the read path) both still
//! use the in-memory [`InMemoryExecutionsStore`], unconditionally. So with
//! `DATABASE_URL` set the [`DatabasePool`] opens and the migrations run at boot,
//! but **live fills do NOT persist to Postgres today**. The durable
//! [`PgExecutionsStore`] is migration-verified and proven at parity through the
//! #008 [`ExecutionsStore`] contract (`tests/db.rs`); wiring it as the live
//! fan-out backend lands with the sync→async single-writer rewire in v0.3 (#029).
//!
//! ## What the durable schema covers (and what it does NOT)
//!
//! The `migrations/` cover `executions` (the authoritative fill log — the only
//! table with a repository, [`PgExecutionsStore`], in #023), plus the
//! `underlying_prices` / `market_maker_configs` / `system_control` / `accounts`
//! **schema skeletons** (their read/write code lands with the surfaces that own
//! them; #024 provisions the `accounts` table)
//! ([06 §6](../../docs/06-deployment.md#6-persistence)). Positions are a derived
//! fold — **not** persisted (no PG positions store).
//!
//! ## The command journal is NOT built here (v0.3, #029)
//!
//! The durable `VenueEvent` envelope journal and journal-backed recovery are v0.3
//! (#029). This layer supplies a durable executions store (proven at parity, not
//! yet wired to the live fan-out — see above) and the config/account tables, but
//! it does **not** recover book state, the position fold, or the idempotency map on
//! restart. Until journal-backed recovery lands, **a restart without an admin
//! snapshot is a fresh venue** ([06 §6](../../docs/06-deployment.md#6-persistence)).
//!
//! ## Security
//!
//! Every query is a compile-time-checked `sqlx::query!` / `query_as!` with bound
//! parameters — no value or identifier is ever interpolated. `sqlx::Error` is
//! never leaked through a `pub` signature (it is mapped to [`DbError`] carrying
//! only a non-secret label), and the `DATABASE_URL` is never logged
//! (`rules/global_rules.md` *SQL & Persistence*, *Security*;
//! [08 §7](../../docs/08-threat-model.md#7-secrets-handling)).

pub mod error;
pub mod executions;
pub mod pool;

use std::sync::Arc;

use crate::exchange::{ExecutionsStore, InMemoryExecutionsStore};

pub use self::error::DbError;
pub use self::executions::PgExecutionsStore;
pub use self::pool::{DatabasePool, DbPoolConfig};

/// Resolves an executions backend behind the #008 [`ExecutionsStore`] contract:
/// the durable [`PgExecutionsStore`] when a [`DatabasePool`] is open, else the
/// in-memory [`InMemoryExecutionsStore`] — so the caller is backend-agnostic.
///
/// **Not yet wired into [`AppState`](crate::state::AppState).** In #023 this
/// selector is called **only** by the parity integration test (`tests/db.rs`),
/// which proves the two backends serve identical reads; `AppState` still uses the
/// in-memory store for both the live fan-out (write) and `executions()` (read), so
/// **live fills do not persist to Postgres today** (see the module docs). This is
/// the seam the v0.3 single-writer rewire (#029) invokes to make the durable store
/// live; pass `None` for the DB-less path (the venue's default), or an open pool
/// for the durable path.
///
/// # Errors
///
/// [`DbError::Unavailable`] if a durable store is requested outside a tokio
/// runtime (the sync→async bridge needs a runtime handle). The in-memory path is
/// infallible.
pub fn select_executions_store(
    db: Option<&DatabasePool>,
) -> Result<Arc<dyn ExecutionsStore>, DbError> {
    match db {
        Some(pool) => {
            let store = PgExecutionsStore::new(pool)?;
            tracing::info!("executions backend: durable postgres");
            Ok(Arc::new(store))
        }
        None => {
            tracing::info!("executions backend: in-memory (no DATABASE_URL)");
            Ok(Arc::new(InMemoryExecutionsStore::new()))
        }
    }
}
