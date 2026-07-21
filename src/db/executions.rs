//! [`PgExecutionsStore`] — the durable PostgreSQL backend for the authoritative
//! executions log, behind the **same** #008 [`ExecutionsStore`] contract as the
//! in-memory store.
//!
//! ## One contract, two backends (the key deliverable)
//!
//! This store implements the identical synchronous [`ExecutionsStore`] trait the
//! in-memory [`InMemoryExecutionsStore`](crate::exchange::InMemoryExecutionsStore)
//! implements, so a future [`AppState`](crate::state::AppState) can hold whichever
//! backend it selects and a gateway read never learns which
//! ([06 §6](../../docs/06-deployment.md#6-persistence),
//! [008](../../milestones/v0.1-backend-core/008-executions-positions-stores.md)).
//! **In #023 that wiring is deferred** — `AppState` still uses the in-memory store
//! for the live fan-out and reads, so this durable store is exercised only through
//! the parity test (`tests/db.rs`), not by live fills (see [`crate::db`] docs). The
//! persisted records round-trip **identically** to the in-memory backend's:
//! the `BIGSERIAL id` is the durable home of the in-memory monotonic `ord`, so
//! `ORDER BY id` reproduces the same journal-ordered listing, and the
//! `(execution_id, liquidity)` upsert preserves that id on a re-record, matching
//! the in-memory store's "keep the original `ord`" semantics exactly.
//!
//! ## Integer cents at the DB boundary (lossless)
//!
//! Cents are `BIGINT` (`i64`) — lossless because the venue-owned
//! [`MAX_PRICE_CENTS`](crate::MAX_PRICE_CENTS) bounds every price/fee/theo inside
//! `i64` ([05 §4.1](../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable),
//! [governance-precedence §2.1](../../docs/governance-precedence.md#21-cents-at-the-database-boundary-lossless-encoding)).
//! Money is integer cents, never `f64`. The `u64 → i64` narrowing on write and
//! `i64 → u64` widening on read are **checked**; an out-of-domain value is a typed
//! [`DbError::ValueRange`], never a silent truncation.
//!
//! ## The sync→async bridge
//!
//! The #008 contract is **synchronous** (it is called on the actor fan-out and in
//! sync trait calls), but `sqlx` is async. Each trait method bridges via
//! [`tokio::task::block_in_place`] + [`Handle::block_on`], which requires a
//! **multi-threaded** runtime — the binary's `#[tokio::main]` default, and the DB
//! integration test's `flavor = "multi_thread"`. A blocking DB round-trip on the
//! actor's writer thread is a real cost; wiring this store as the LIVE fan-out
//! backend (vs. a fully-tested second backend proven at parity here) is coupled to
//! the durable journal + recovery (v0.3, #029) and is deferred.
//!
//! ## Scope: no positions store, no recovery
//!
//! [06 §6](../../docs/06-deployment.md#6-persistence) covers `executions` but
//! **not positions** — positions are a derived fold, so there is no PG positions
//! store; the fold stays in-memory. The durable command journal and
//! journal-backed recovery are v0.3 (#029): this store is the durable **home** for
//! the fill log (not yet fed by live fills, see above), and book/fold state is not
//! recovered on restart — a restart without an admin snapshot is a fresh venue.
//!
//! ## Parameterised queries only
//!
//! Every query is a compile-time-checked `sqlx::query!` / `query_as!` with bound
//! parameters (`$1, $2, …`); no value or identifier is ever interpolated into SQL
//! (`rules/global_rules.md` *SQL & Persistence*).

use sqlx::PgPool;
use tokio::runtime::Handle;

use crate::db::error::DbError;
use crate::db::pool::DatabasePool;
use crate::exchange::{
    Cents, EventTimestamp, ExecutionFilter, ExecutionsStore, SequenceNumber, SignedCents,
    StoreError, Symbol,
};
use crate::models::{AccountId, ExecutionId, ExecutionRecord, LiquidityFlag, Side, VenueOrderId};

// ============================================================================
// The durable row entity
// ============================================================================

/// The durable `executions` row — the `#[derive(sqlx::FromRow)]` entity kept in
/// lockstep with the migration's column shape. Read back and projected into an
/// authoritative [`ExecutionRecord`] by [`row_to_record`].
///
/// Cents/quantities/sequences are `BIGINT` (`i64`) on the wire from Postgres; the
/// projection re-establishes the venue's `u64`/`Cents` domain with **checked**
/// conversions.
#[derive(Debug, Clone, sqlx::FromRow)]
struct ExecutionRow {
    execution_id: String,
    order_id: String,
    account: String,
    symbol: String,
    instrument: String,
    side: String,
    liquidity: String,
    quantity: i64,
    price_cents: i64,
    fee_cents: i64,
    theo_value_cents: i64,
    edge_cents: i64,
    underlying_sequence: i64,
    latency_us: i64,
    executed_at_ms: i64,
}

// ============================================================================
// The store
// ============================================================================

/// The durable PostgreSQL [`ExecutionsStore`].
///
/// Cloning is cheap ([`PgPool`] is an `Arc` internally). Constructed at boot from
/// an open [`DatabasePool`] within a tokio runtime.
#[derive(Clone)]
pub struct PgExecutionsStore {
    pool: PgPool,
    handle: Handle,
}

impl std::fmt::Debug for PgExecutionsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgExecutionsStore")
            .field("pool_size", &self.pool.size())
            .finish_non_exhaustive()
    }
}

impl PgExecutionsStore {
    /// Wires a durable executions store over an open [`DatabasePool`].
    ///
    /// Captures the current runtime [`Handle`] so the synchronous trait methods
    /// can bridge onto async `sqlx`.
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

    /// Bridges the synchronous #008 store contract onto async `sqlx`: hands the
    /// current worker to the blocking pool and drives the query to completion.
    ///
    /// Requires a multi-threaded runtime (documented on the type).
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future,
    {
        tokio::task::block_in_place(|| self.handle.block_on(future))
    }

    /// Records (upserts) one fill leg — the async body behind the sync
    /// [`ExecutionsStore::record`].
    async fn record_async(&self, record: &ExecutionRecord) -> Result<(), DbError> {
        let side = side_to_str(record.side);
        let liquidity = liquidity_to_str(record.liquidity);
        let quantity = u64_to_i64(record.quantity, "quantity")?;
        let price_cents = u64_to_i64(record.price_cents.get(), "price_cents")?;
        let theo_value_cents = u64_to_i64(record.theo_value_cents.get(), "theo_value_cents")?;
        let underlying_sequence =
            u64_to_i64(record.underlying_sequence.get(), "underlying_sequence")?;
        let latency_us = u64_to_i64(record.latency_us, "latency_us")?;
        let executed_at_ms = u64_to_i64(record.executed_at.get(), "executed_at_ms")?;

        // Idempotent upsert on the (execution_id, liquidity) leg key: a re-record
        // updates the value columns in place and PRESERVES the row's `id`, so the
        // list order is identical to the in-memory store's "keep the original ord".
        sqlx::query!(
            r#"
            INSERT INTO executions (
                execution_id, liquidity, order_id, account, symbol, instrument, side,
                quantity, price_cents, fee_cents, theo_value_cents, edge_cents,
                underlying_sequence, latency_us, executed_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (execution_id, liquidity) DO UPDATE SET
                order_id = EXCLUDED.order_id,
                account = EXCLUDED.account,
                symbol = EXCLUDED.symbol,
                instrument = EXCLUDED.instrument,
                side = EXCLUDED.side,
                quantity = EXCLUDED.quantity,
                price_cents = EXCLUDED.price_cents,
                fee_cents = EXCLUDED.fee_cents,
                theo_value_cents = EXCLUDED.theo_value_cents,
                edge_cents = EXCLUDED.edge_cents,
                underlying_sequence = EXCLUDED.underlying_sequence,
                latency_us = EXCLUDED.latency_us,
                executed_at_ms = EXCLUDED.executed_at_ms
            "#,
            record.execution_id.as_str(),
            liquidity,
            record.order_id.as_str(),
            record.account.as_str(),
            record.symbol,
            record.instrument.as_str(),
            side,
            quantity,
            price_cents,
            record.fee_cents.get(),
            theo_value_cents,
            record.edge_cents.get(),
            underlying_sequence,
            latency_us,
            executed_at_ms,
        )
        .execute(&self.pool)
        .await
        .map_err(query_err("record execution"))?;
        Ok(())
    }

    /// Fetches the account-owned leg of `execution_id` (taker-first) — the async
    /// body behind the sync [`ExecutionsStore::get`].
    async fn get_async(
        &self,
        execution_id: &ExecutionId,
        account: &AccountId,
    ) -> Result<Option<ExecutionRecord>, DbError> {
        // Taker first: for a same-account self-trade (both legs share the account)
        // the aggressor (taker) leg wins, matching the in-memory store.
        let row = sqlx::query_as!(
            ExecutionRow,
            r#"
            SELECT
                execution_id, order_id, account, symbol, instrument, side, liquidity,
                quantity, price_cents, fee_cents, theo_value_cents, edge_cents,
                underlying_sequence, latency_us, executed_at_ms
            FROM executions
            WHERE execution_id = $1 AND account = $2
            ORDER BY CASE liquidity WHEN 'taker' THEN 0 ELSE 1 END
            LIMIT 1
            "#,
            execution_id.as_str(),
            account.as_str(),
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(query_err("get execution"))?;

        row.map(row_to_record).transpose()
    }

    /// Lists an account's legs in journal (`id`) order with the filter applied —
    /// the async body behind the sync [`ExecutionsStore::list`].
    async fn list_async(
        &self,
        account: &AccountId,
        filter: &ExecutionFilter,
    ) -> Result<Vec<ExecutionRecord>, DbError> {
        // The optional underlying filter and limit are BOUND parameters, never
        // interpolated. `LIMIT NULL` (a `None` limit) returns all rows in Postgres.
        let underlying = filter.underlying.clone();
        let limit = match filter.limit {
            Some(limit) => Some(usize_to_i64(limit, "limit")?),
            None => None,
        };
        let rows = sqlx::query_as!(
            ExecutionRow,
            r#"
            SELECT
                execution_id, order_id, account, symbol, instrument, side, liquidity,
                quantity, price_cents, fee_cents, theo_value_cents, edge_cents,
                underlying_sequence, latency_us, executed_at_ms
            FROM executions
            WHERE account = $1
              AND ($2::text IS NULL OR symbol = $2)
            ORDER BY id
            LIMIT $3
            "#,
            account.as_str(),
            underlying,
            limit,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(query_err("list executions"))?;

        rows.into_iter().map(row_to_record).collect()
    }

    /// Counts the recorded legs — the async body behind the sync
    /// [`ExecutionsStore::len`].
    async fn len_async(&self) -> Result<usize, DbError> {
        let count = sqlx::query_scalar!(r#"SELECT count(*) AS "count!" FROM executions"#)
            .fetch_one(&self.pool)
            .await
            .map_err(query_err("count executions"))?;
        // `count(*)` is a non-negative `BIGINT`; the widening to `usize` is checked.
        usize::try_from(count).map_err(|_| DbError::ValueRange { field: "count" })
    }
}

impl ExecutionsStore for PgExecutionsStore {
    fn record(&self, record: ExecutionRecord) -> Result<(), StoreError> {
        self.block_on(self.record_async(&record))
            .map_err(StoreError::from)
    }

    fn get(
        &self,
        execution_id: &ExecutionId,
        account: &AccountId,
    ) -> Result<Option<ExecutionRecord>, StoreError> {
        self.block_on(self.get_async(execution_id, account))
            .map_err(StoreError::from)
    }

    fn list(
        &self,
        account: &AccountId,
        filter: &ExecutionFilter,
    ) -> Result<Vec<ExecutionRecord>, StoreError> {
        self.block_on(self.list_async(account, filter))
            .map_err(StoreError::from)
    }

    fn len(&self) -> usize {
        // The #008 `len` is infallible by contract; a durable-count failure has no
        // error channel here, so it is logged and reported as `0` (a defensive,
        // never-corrupting fallback). `len` is an observability/test read, never on
        // the hot path — the fallible reads (`get`/`list`) carry the real error.
        match self.block_on(self.len_async()) {
            Ok(count) => count,
            Err(error) => {
                tracing::error!(error = %error, "durable executions count failed; reporting 0");
                0
            }
        }
    }
}

// ============================================================================
// Conversions (checked, non-secret error labels)
// ============================================================================

/// Maps a `sqlx::Error` to a typed [`DbError::Query`] with a non-secret operation
/// label, logging the underlying driver cause **server-side** (never leaked to a
/// client, never carrying the `DATABASE_URL`).
fn query_err(operation: &'static str) -> impl FnOnce(sqlx::Error) -> DbError {
    move |error| {
        tracing::error!(operation, error = %error, "durable executions query failed");
        DbError::Query { operation }
    }
}

/// The wire-cased DB token for an order [`Side`] (`'buy'` / `'sell'`).
#[inline]
const fn side_to_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

/// Parses a stored side token, or a typed [`DbError::ValueRange`] on an
/// out-of-vocabulary value (the CHECK constraint makes this unreachable).
#[inline]
fn side_from_str(token: &str) -> Result<Side, DbError> {
    match token {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(DbError::ValueRange { field: "side" }),
    }
}

/// The wire-cased DB token for a [`LiquidityFlag`] (`'maker'` / `'taker'`).
#[inline]
const fn liquidity_to_str(liquidity: LiquidityFlag) -> &'static str {
    match liquidity {
        LiquidityFlag::Maker => "maker",
        LiquidityFlag::Taker => "taker",
    }
}

/// Parses a stored liquidity token, or a typed [`DbError::ValueRange`] (the CHECK
/// constraint makes this unreachable).
#[inline]
fn liquidity_from_str(token: &str) -> Result<LiquidityFlag, DbError> {
    match token {
        "maker" => Ok(LiquidityFlag::Maker),
        "taker" => Ok(LiquidityFlag::Taker),
        _ => Err(DbError::ValueRange { field: "liquidity" }),
    }
}

/// Narrows a venue `u64` to a `BIGINT` `i64`, checked — an out-of-range value is a
/// typed [`DbError::ValueRange`], never a silent truncation.
#[inline]
fn u64_to_i64(value: u64, field: &'static str) -> Result<i64, DbError> {
    i64::try_from(value).map_err(|_| DbError::ValueRange { field })
}

/// Narrows a `usize` (a query limit) to a `BIGINT` `i64`, checked.
#[inline]
fn usize_to_i64(value: usize, field: &'static str) -> Result<i64, DbError> {
    i64::try_from(value).map_err(|_| DbError::ValueRange { field })
}

/// Widens a `BIGINT` `i64` back to a venue `u64`, checked (a negative stored value
/// is a typed [`DbError::ValueRange`]; the CHECK constraints make it unreachable).
#[inline]
fn i64_to_u64(value: i64, field: &'static str) -> Result<u64, DbError> {
    u64::try_from(value).map_err(|_| DbError::ValueRange { field })
}

/// Re-establishes the non-negative [`Cents`] domain from a stored `BIGINT`.
#[inline]
fn cents_from_i64(value: i64, field: &'static str) -> Result<Cents, DbError> {
    Cents::try_new(value).map_err(|_| DbError::ValueRange { field })
}

/// Projects a durable [`ExecutionRow`] into an authoritative [`ExecutionRecord`],
/// re-establishing the venue's `u64` / `Cents` / `Symbol` domain with checked
/// conversions.
fn row_to_record(row: ExecutionRow) -> Result<ExecutionRecord, DbError> {
    Ok(ExecutionRecord {
        execution_id: ExecutionId::new(row.execution_id),
        order_id: VenueOrderId::new(row.order_id),
        account: AccountId::new(row.account),
        symbol: row.symbol,
        instrument: Symbol::parse(&row.instrument).map_err(|_| DbError::ValueRange {
            field: "instrument",
        })?,
        side: side_from_str(&row.side)?,
        liquidity: liquidity_from_str(&row.liquidity)?,
        quantity: i64_to_u64(row.quantity, "quantity")?,
        price_cents: cents_from_i64(row.price_cents, "price_cents")?,
        fee_cents: SignedCents::new(row.fee_cents),
        theo_value_cents: cents_from_i64(row.theo_value_cents, "theo_value_cents")?,
        edge_cents: SignedCents::new(row.edge_cents),
        underlying_sequence: SequenceNumber::new(i64_to_u64(
            row.underlying_sequence,
            "underlying_sequence",
        )?),
        latency_us: i64_to_u64(row.latency_us, "latency_us")?,
        executed_at: EventTimestamp::new(i64_to_u64(row.executed_at_ms, "executed_at_ms")?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // These are PURE conversion/mapping unit tests — no DB, so they run in the
    // default `cargo test` suite WITHOUT Docker. The real-Postgres round-trip is
    // the `#[ignore]`-gated integration test in `tests/db.rs` (the CI migrations
    // job).

    #[test]
    fn test_side_token_round_trips() {
        for side in [Side::Buy, Side::Sell] {
            match side_from_str(side_to_str(side)) {
                Ok(parsed) => assert_eq!(parsed, side),
                Err(e) => panic!("side round-trip failed: {e}"),
            }
        }
    }

    #[test]
    fn test_liquidity_token_round_trips() {
        for liquidity in [LiquidityFlag::Maker, LiquidityFlag::Taker] {
            match liquidity_from_str(liquidity_to_str(liquidity)) {
                Ok(parsed) => assert_eq!(parsed, liquidity),
                Err(e) => panic!("liquidity round-trip failed: {e}"),
            }
        }
    }

    #[test]
    fn test_unknown_side_token_is_value_range() {
        assert_eq!(
            side_from_str("hold"),
            Err(DbError::ValueRange { field: "side" })
        );
    }

    #[test]
    fn test_u64_i64_round_trip_and_range_check() {
        // In-domain values round-trip exactly.
        for value in [0_u64, 1, 50_000, u64::try_from(i64::MAX).unwrap_or(0)] {
            let narrowed = u64_to_i64(value, "x").expect("in range");
            assert_eq!(i64_to_u64(narrowed, "x").expect("in range"), value);
        }
        // A `u64` above `i64::MAX` is a typed range error, never a truncation.
        let over = (i64::MAX as u64) + 1;
        assert_eq!(
            u64_to_i64(over, "quantity"),
            Err(DbError::ValueRange { field: "quantity" })
        );
        // A negative `BIGINT` widening is a typed range error.
        assert_eq!(
            i64_to_u64(-1, "quantity"),
            Err(DbError::ValueRange { field: "quantity" })
        );
    }

    #[test]
    fn test_cents_from_i64_rejects_negative() {
        assert_eq!(cents_from_i64(500, "price_cents").map(|c| c.get()), Ok(500));
        assert_eq!(
            cents_from_i64(-1, "price_cents"),
            Err(DbError::ValueRange {
                field: "price_cents"
            })
        );
    }

    #[test]
    fn test_db_error_maps_to_store_backend() {
        // The durable failure folds onto the #008 `StoreError::Backend` contract,
        // carrying only the non-secret label.
        let store_error = StoreError::from(DbError::Query {
            operation: "list executions",
        });
        match store_error {
            StoreError::Backend(detail) => assert!(detail.contains("list executions")),
            other => panic!("expected StoreError::Backend, got {other:?}"),
        }
    }

    #[test]
    fn test_db_error_maps_to_redacted_internal_venue_error() {
        use crate::error::{REDACTED_INTERNAL_MESSAGE, VenueError};
        let venue: VenueError = DbError::Connect.into();
        // A DB failure is a redacted internal 500 on every client surface.
        assert_eq!(venue.machine_code(), "internal");
        assert_eq!(venue.redacted_message(), REDACTED_INTERNAL_MESSAGE);
    }
}
