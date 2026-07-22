//! The in-memory **executions** and **positions** stores and their
//! backend-agnostic contract — the authoritative fill log and the per-`(account,
//! symbol)` position fold, both derived from committed [`VenueEvent`] fills via
//! the actor's post-journal [`FanOut`] seam
//! ([008](../../../milestones/v0.1-backend-core/008-executions-positions-stores.md),
//! [01 §7](../../../docs/01-domain-model.md),
//! [02 §6](../../../docs/02-matching-architecture.md)).
//!
//! ## Event-sourced, so the executions log is deterministic
//!
//! Fills are the [`VenueEvent`] outcome, so the executions log is a
//! **deterministic function of the journal** — the same journal yields the same
//! executions ([02 §6](../../../docs/02-matching-architecture.md)). Each fill
//! **leg** is recorded as an authoritative [`ExecutionRecord`]: a match produces
//! two linked legs (maker + taker) sharing one `execution_id`, each with its own
//! account and fee. Positions are a fold over those legs per `(account, symbol)`.
//!
//! ## Live-only mark, never journaled
//!
//! The fold state — `net_quantity`, `avg_price` (volume-weighted entry), and
//! `realized_pnl` — is journal-derived and exact. `unrealized_pnl` is marked at
//! **read time** against a [`MarkSource`] (in production the upstream
//! [`option_chain_orderbook::MarkPriceCalculator`], wrapped by [`MarkPriceBook`]).
//! It is a **live-only** projection: not journaled, not asserted equal across
//! replays — replay reconstructs order-book state, not historical marks
//! ([02 §5.5](../../../docs/02-matching-architecture.md),
//! [01 §7](../../../docs/01-domain-model.md)). The read API takes the mark as an
//! explicit argument precisely to keep that separation visible.
//!
//! ## Backend-agnostic contract (the key deliverable)
//!
//! [`ExecutionsStore`] and [`PositionsStore`] are the read/write contract the
//! REST executions/positions routes (#013) call. The in-memory implementations
//! here and the future PostgreSQL implementations (#023) satisfy the **same**
//! traits, so only the backend swaps — the REST reads never change. The
//! in-memory executions insertion order is a monotonic surrogate for the durable
//! `SERIAL`/`BIGSERIAL` primary key an SQL store would `ORDER BY`, so listing is
//! identically ordered on either backend.
//!
//! ## Exact integer-cents P&L
//!
//! Every monetary quantity is integer cents; the fold accumulates in `i128` with
//! **checked** arithmetic (never `saturating_*` / `wrapping_*`,
//! [rules/global_rules.md] Arithmetic) and narrows to `i64`/`u64` at the DTO
//! boundary with a typed [`StoreError::Arithmetic`] on overflow (unreachable for
//! the venue's admission-bounded cents,
//! [governance-precedence §2.1](../../../docs/governance-precedence.md)). The
//! realized/unrealized split is the standard identity
//! `realized + unrealized == net_cash − fees + net_quantity × mark`, which holds
//! **exactly** because both halves are computed from one exact cost basis.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use option_chain_orderbook::MarkPriceCalculator;

use crate::exchange::actor::{FanOut, FanOutSealed};
use crate::exchange::boundary::{Side as SeamSide, SymbolParser};
use crate::exchange::envelope::{AddOutcome, Fill, VenueCommand, VenueEvent, VenueOutcome};
use crate::exchange::money::{Cents, SignedCents};
use crate::exchange::snapshot::{ExecutionCapture, PositionCapture, SnapshotError};
use crate::exchange::symbol::Symbol;
use crate::models::{AccountId, ExecutionId, ExecutionRecord, LiquidityFlag, Position, Side};

// ============================================================================
// Store error
// ============================================================================

/// A typed store failure ([01 §11](../../../docs/01-domain-model.md)).
///
/// The in-memory stores here only ever raise [`StoreError::Arithmetic`] (and
/// only on the unreachable overflow path of the P&L fold); the
/// [`StoreError::Backend`] variant fixes the contract now for the durable
/// PostgreSQL store (#023), which reports I/O / driver failures through it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// A checked cents/quantity operation in the position fold overflowed its
    /// integer width — never a wrap or a saturate. Unreachable for the venue's
    /// admission-bounded cents; the operation fails loudly rather than corrupt
    /// P&L.
    #[error("store arithmetic overflow in the position fold")]
    Arithmetic,
    /// The durable backend reported a failure (reserved for the PostgreSQL store,
    /// #023). Never constructed by the in-memory stores.
    #[error("store backend failure: {0}")]
    Backend(String),
}

// ============================================================================
// Mark source — the live-only unrealized-P&L input
// ============================================================================

/// A live per-symbol mark-price source — the read-time input to
/// `unrealized_pnl` ([01 §7](../../../docs/01-domain-model.md)).
///
/// This is deliberately a **read-time argument**, never journaled: the mark is a
/// derived, live-only value and is recomputed rather than replayed
/// ([02 §5.5](../../../docs/02-matching-architecture.md)). A `None` mark means
/// the symbol is unpriced, and the position is reported without
/// `current_price` / `unrealized_pnl` (never as zero).
pub trait MarkSource: Send + Sync {
    /// The current mark for `symbol` in **cents**, or `None` when unpriced.
    #[must_use]
    fn mark(&self, symbol: &Symbol) -> Option<Cents>;
}

/// A per-symbol book of upstream [`MarkPriceCalculator`]s — the production
/// [`MarkSource`].
///
/// It wraps the upstream calculator (verified present in the locked
/// `option-chain-orderbook` 0.7.0), feeding it the venue's own trade prints and
/// (later) index/spot updates, and reading back the committed mark. The mark is
/// **live-only**: it is never journaled and its value is not part of the
/// determinism oracle, so feeding it on the (deterministic) fan-out is safe —
/// its output is simply not asserted across replays.
#[derive(Debug, Default)]
pub struct MarkPriceBook {
    calculators: DashMap<Symbol, MarkPriceCalculator>,
}

impl MarkPriceBook {
    /// Builds an empty mark-price book.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            calculators: DashMap::new(),
        }
    }

    /// Feeds a trade print for `symbol` and advances its mark one dampening step
    /// (the fan-out path). Prices are integer cents (one venue tick = one cent).
    pub fn on_trade(&self, symbol: &Symbol, price: Cents) {
        let calc = self
            .calculators
            .entry(symbol.clone())
            .or_insert_with(MarkPriceCalculator::with_default_config);
        calc.update_last_trade_price(price.get());
        // `advance_mark` is the mutating per-update tick; the result is read back
        // via `current_mark_price` in `mark`. Synchronous — no await under the
        // DashMap guard.
        let _ = calc.advance_mark();
    }

    /// Sets an external index/spot price for `symbol` and advances its mark
    /// (reserved for the price-feed wiring; unused on the #008 fan-out).
    pub fn set_index_price(&self, symbol: &Symbol, price: Cents) {
        let calc = self
            .calculators
            .entry(symbol.clone())
            .or_insert_with(MarkPriceCalculator::with_default_config);
        calc.update_index_price(price.get());
        let _ = calc.advance_mark();
    }
}

impl MarkSource for MarkPriceBook {
    #[inline]
    fn mark(&self, symbol: &Symbol) -> Option<Cents> {
        self.calculators
            .get(symbol)
            .and_then(|calc| calc.current_mark_price())
            .map(Cents::new)
    }
}

/// An unpriced [`MarkSource`] — every symbol is unpriced, so positions render
/// without `current_price` / `unrealized_pnl`. Useful when no mark feed is
/// wired.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoMarks;

impl MarkSource for NoMarks {
    #[inline]
    fn mark(&self, _symbol: &Symbol) -> Option<Cents> {
        None
    }
}

/// Whether an executions leg belongs to `underlying` — the **single** ownership
/// predicate the per-underlying partition is scoped by. An [`ExecutionRecord`]'s
/// `symbol` field carries the underlying ticker (the same field
/// [`InMemoryExecutionsStore::capture_for`] filters on). Capture, restore, and
/// restore-validation all consult this one rule, so the partition can never
/// diverge into a second, subtly different predicate.
#[inline]
#[must_use]
fn execution_leg_belongs(record: &ExecutionRecord, underlying: &str) -> bool {
    record.symbol.as_str() == underlying
}

// ============================================================================
// Executions store contract
// ============================================================================

/// Filters for an [`ExecutionsStore::list`] query — the deterministic subset of
/// the REST `ExecutionsQuery` the store applies (the `from`/`to` date filters are
/// translated by the REST handler, #013).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionFilter {
    /// Restrict to a single underlying ticker (e.g. `"BTC"`).
    pub underlying: Option<String>,
    /// Cap the number of returned records (applied after ordering).
    pub limit: Option<usize>,
}

/// The authoritative executions log contract ([01 §7](../../../docs/01-domain-model.md)).
///
/// Every committed fill **leg** is recorded as an [`ExecutionRecord`]; the REST
/// `/api/v1/executions` reads go through [`get`](Self::get) / [`list`](Self::list).
/// The in-memory store here and the durable PostgreSQL store (#023) implement
/// this **same** trait, so the read surface is backend-independent.
pub trait ExecutionsStore: Send + Sync {
    /// Records one fill leg. Called on the post-journal fan-out (step 5); the two
    /// legs of one match are two calls sharing one `execution_id`, distinguished
    /// by their [`LiquidityFlag`]. Re-recording the same `(execution_id,
    /// liquidity)` overwrites in place (idempotent), so a fan-out replay is safe.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backend cannot record the leg (never for
    /// the in-memory store).
    fn record(&self, record: ExecutionRecord) -> Result<(), StoreError>;

    /// Fetches the leg of `execution_id` owned by `account` — the account-scoped
    /// `GET /executions/{id}` read. For a same-account self-trade (both legs share
    /// the account) the aggressor (taker) leg is returned.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] on a backend failure (never for the in-memory
    /// store).
    fn get(
        &self,
        execution_id: &ExecutionId,
        account: &AccountId,
    ) -> Result<Option<ExecutionRecord>, StoreError>;

    /// Lists `account`'s execution legs in insertion (journal) order, filtered by
    /// [`ExecutionFilter`] — the account-scoped `GET /executions` read.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] on a backend failure (never for the in-memory
    /// store).
    fn list(
        &self,
        account: &AccountId,
        filter: &ExecutionFilter,
    ) -> Result<Vec<ExecutionRecord>, StoreError>;

    /// The total number of recorded legs (both legs of each match counted).
    #[must_use]
    fn len(&self) -> usize;

    /// Whether no legs have been recorded.
    #[must_use]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ============================================================================
// In-memory executions store
// ============================================================================

/// The `(execution_id, liquidity)` key uniquely identifying one recorded leg.
///
/// `execution_id` is shared by the two legs of a match; `liquidity` splits them,
/// so the key stays unique even for a same-account self-trade (STP-`None`), where
/// both legs carry the same account and `execution_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExecKey {
    execution_id: ExecutionId,
    liquidity: LiquidityFlag,
}

/// One stored leg with its insertion-order surrogate.
#[derive(Debug, Clone)]
struct StoredExecution {
    /// The monotonic insertion index — the in-memory surrogate for the durable
    /// `SERIAL` primary key, giving a deterministic `ORDER BY` for [`list`].
    ///
    /// [`list`]: ExecutionsStore::list
    ord: u64,
    record: ExecutionRecord,
}

/// The in-memory [`ExecutionsStore`] — a concurrent map keyed by
/// `(execution_id, liquidity)`.
///
/// `DashMap` (over `Arc<RwLock<HashMap<>>>`, per rules Concurrency) gives
/// lock-free point reads/writes; listing snapshots and sorts by the insertion
/// surrogate for a deterministic, journal-ordered result.
#[derive(Debug, Default)]
pub struct InMemoryExecutionsStore {
    records: DashMap<ExecKey, StoredExecution>,
    /// The next insertion index. Not a venue `underlying_sequence`, id, or money
    /// value — a purely internal ordering surrogate for the durable `SERIAL` id —
    /// so the checked-sequence rule does not apply; `u64::MAX` is unreachable
    /// (2^64 legs).
    next_ord: AtomicU64,
}

impl InMemoryExecutionsStore {
    /// Builds an empty executions store.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Captures **only** `underlying`'s slice of the shared venue-wide log — the
    /// legs whose [`ExecutionRecord::symbol`](crate::models::ExecutionRecord)
    /// underlying ticker matches — sorted by `ord` so the cut is deterministic
    /// regardless of map iteration order (#009).
    ///
    /// The store is shared by every per-underlying actor, but each actor writes
    /// only its own underlying's legs, so this slice is exactly the (quiesced)
    /// capturing actor's fills — a **consistent cut for that underlying**, never a
    /// torn read of another underlying's concurrent writes
    /// ([02 §9](../../../docs/02-matching-architecture.md)). This scoping is what
    /// keeps a per-underlying snapshot from ever capturing (or, on restore,
    /// erasing) another underlying's data.
    #[must_use]
    pub fn capture_for(&self, underlying: &str) -> Vec<ExecutionCapture> {
        let mut captures: Vec<ExecutionCapture> = self
            .records
            .iter()
            .filter(|entry| execution_leg_belongs(&entry.record, underlying))
            .map(|entry| ExecutionCapture {
                ord: entry.ord,
                record: entry.record.clone(),
            })
            .collect();
        captures.sort_by_key(|capture| capture.ord);
        captures
    }

    /// Validates that **every** leg in a restore cut belongs to `underlying`,
    /// **without** mutating the store — the pre-mutation, all-or-nothing ownership
    /// gate the actor's restore choreography runs in its preparation phase, before
    /// the epoch marker is journaled and before any store is swapped
    /// ([02 §9](../../../docs/02-matching-architecture.md)).
    ///
    /// The executions store is shared across every per-underlying actor and only
    /// the target actor is quiesced during a restore, so a cut carrying another
    /// underlying's legs is a **corrupt snapshot** that would otherwise inject or
    /// overwrite a live underlying's data. It is refused **wholesale** here rather
    /// than partially applied — a mixed-underlying cut is corruption, not a
    /// partial-apply situation.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::RebuildFailed`] if any leg's underlying ticker is
    /// not `underlying`.
    pub fn validate_restore(
        &self,
        underlying: &str,
        captures: &[ExecutionCapture],
    ) -> Result<(), SnapshotError> {
        let foreign = captures
            .iter()
            .filter(|capture| !execution_leg_belongs(&capture.record, underlying))
            .count();
        if foreign > 0 {
            return Err(SnapshotError::RebuildFailed(format!(
                "executions restore cut for {underlying} carries {foreign} leg(s) for another underlying"
            )));
        }
        Ok(())
    }

    /// Restores **only** `underlying`'s slice from a cut, replacing exactly that
    /// underlying's legs in place and leaving every other underlying's legs
    /// untouched — so restoring `BTC` never erases `ETH`'s (possibly newer)
    /// executions ([02 §9](../../../docs/02-matching-architecture.md)). Runs under
    /// the capturing actor's quiescence; other underlyings' actors need not quiesce
    /// because their legs occupy a disjoint keyspace.
    ///
    /// **Fail-closed on ownership**: because the store is shared venue-wide and only
    /// the target actor is quiesced, a leg belonging to another underlying is
    /// **never** inserted — it is skipped and the drop is logged. The actor path
    /// already refuses a mixed-underlying cut wholesale via [`validate_restore`]
    /// before the marker is journaled; this skip is the last-line defence for any
    /// direct caller, so foreign data can never land regardless of entry point.
    ///
    /// The insertion surrogate is a **shared, globally monotonic** counter, so it
    /// is advanced past the restored maximum (never reset — a reset would regress
    /// it below another underlying's newer legs); `checked_add` keeps the crate
    /// free of `saturating_*`, and `2^64` legs is unreachable.
    ///
    /// [`validate_restore`]: Self::validate_restore
    pub fn restore_for(&self, underlying: &str, captures: Vec<ExecutionCapture>) {
        // Drop only this underlying's slice; every other underlying's legs stay.
        self.records
            .retain(|_, stored| !execution_leg_belongs(&stored.record, underlying));
        let mut max_ord: u64 = 0;
        let mut skipped: usize = 0;
        for capture in captures {
            if !execution_leg_belongs(&capture.record, underlying) {
                // A foreign leg (a corrupt or hostile cut for another underlying)
                // is never inserted — it cannot overwrite a live underlying's data.
                skipped += 1;
                continue;
            }
            max_ord = max_ord.max(capture.ord);
            let key = ExecKey {
                execution_id: capture.record.execution_id.clone(),
                liquidity: capture.record.liquidity,
            };
            self.records.insert(
                key,
                StoredExecution {
                    ord: capture.ord,
                    record: capture.record,
                },
            );
        }
        if skipped > 0 {
            tracing::warn!(
                underlying,
                skipped,
                "executions restore cut carried leg(s) for another underlying; skipped (foreign data never lands)"
            );
        }
        // Keep the shared surrogate strictly ahead of every stored `ord` without
        // regressing it below another underlying's newer legs (checked, never
        // wrapping; `2^64` legs is unreachable).
        if let Some(next) = max_ord.checked_add(1) {
            self.next_ord.fetch_max(next, Ordering::Relaxed);
        }
    }
}

impl ExecutionsStore for InMemoryExecutionsStore {
    fn record(&self, record: ExecutionRecord) -> Result<(), StoreError> {
        use dashmap::mapref::entry::Entry;

        let key = ExecKey {
            execution_id: record.execution_id.clone(),
            liquidity: record.liquidity,
        };
        // Assign the insertion surrogate only on first insert; a re-record of the
        // same `(execution_id, liquidity)` keeps its original `ord`, so the list
        // order is identical to a SQL `ON CONFLICT (execution_id, liquidity) DO
        // UPDATE` (which preserves the row's `SERIAL` id) — bit-identical backends
        // even under a double-emit (which the single-writer actor never does).
        match self.records.entry(key) {
            Entry::Occupied(mut existing) => {
                let ord = existing.get().ord;
                existing.insert(StoredExecution { ord, record });
            }
            Entry::Vacant(slot) => {
                let ord = self.next_ord.fetch_add(1, Ordering::Relaxed);
                slot.insert(StoredExecution { ord, record });
            }
        }
        Ok(())
    }

    fn get(
        &self,
        execution_id: &ExecutionId,
        account: &AccountId,
    ) -> Result<Option<ExecutionRecord>, StoreError> {
        // Taker first: for a same-account self-trade, the aggressor leg wins.
        for liquidity in [LiquidityFlag::Taker, LiquidityFlag::Maker] {
            let key = ExecKey {
                execution_id: execution_id.clone(),
                liquidity,
            };
            if let Some(stored) = self.records.get(&key)
                && stored.record.account == *account
            {
                return Ok(Some(stored.record.clone()));
            }
        }
        Ok(None)
    }

    fn list(
        &self,
        account: &AccountId,
        filter: &ExecutionFilter,
    ) -> Result<Vec<ExecutionRecord>, StoreError> {
        let mut matched: Vec<(u64, ExecutionRecord)> = self
            .records
            .iter()
            .filter(|entry| entry.record.account == *account)
            .filter(|entry| match &filter.underlying {
                Some(underlying) => &entry.record.symbol == underlying,
                None => true,
            })
            .map(|entry| (entry.ord, entry.record.clone()))
            .collect();
        matched.sort_by_key(|(ord, _)| *ord);
        let mut out: Vec<ExecutionRecord> = matched.into_iter().map(|(_, record)| record).collect();
        if let Some(limit) = filter.limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    #[inline]
    fn len(&self) -> usize {
        self.records.len()
    }
}

/// Whether a position fold belongs to `underlying` — the **single** ownership
/// predicate the per-underlying partition is scoped by. A fold carries its
/// underlying ticker in its `underlying` field (the same field
/// [`InMemoryPositionsStore::capture_for`] filters on, surfaced on a capture as
/// [`PositionCapture::underlying`]). Capture, restore, and restore-validation all
/// consult this one rule so the partition can never diverge.
#[inline]
#[must_use]
fn position_fold_belongs(fold_underlying: &str, underlying: &str) -> bool {
    fold_underlying == underlying
}

// ============================================================================
// Positions store contract
// ============================================================================

/// One fill leg fed into the position fold — the account-attributed inputs plus
/// the symbol context the internal [`Fill`] does not carry.
///
/// Borrowed so the fan-out projects each leg without cloning; the store copies
/// only what it retains.
#[derive(Debug, Clone, Copy)]
pub struct PositionLeg<'a> {
    /// The account whose position this leg folds into.
    pub account: &'a AccountId,
    /// The canonical contract symbol.
    pub symbol: &'a Symbol,
    /// The underlying ticker (e.g. `"BTC"`).
    pub underlying: &'a str,
    /// This leg's side (upstream matching-seam [`SeamSide`]).
    pub side: SeamSide,
    /// Executed quantity in **contracts**.
    pub quantity: u64,
    /// Execution price in **cents**.
    pub price: Cents,
    /// This leg's fee in **cents** — a maker rebate is negative.
    pub fee: SignedCents,
}

/// The per-`(account, symbol)` position contract ([01 §7](../../../docs/01-domain-model.md)).
///
/// [`apply`](Self::apply) folds a committed fill leg; the reads project the fold
/// marked against a caller-supplied [`MarkSource`] (the live-only
/// `unrealized_pnl`). The in-memory store here and the durable store (#023)
/// implement this same trait.
pub trait PositionsStore: Send + Sync {
    /// Folds one fill leg into its `(account, symbol)` position. Called on the
    /// post-journal fan-out. The fold is **not** idempotent (it accumulates), so
    /// a re-fold would double-count; recovery (#017) rebuilds from an empty store
    /// by replaying each leg exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Arithmetic`] if the checked cents/quantity fold
    /// overflows (unreachable for admission-bounded cents).
    fn apply(&self, leg: &PositionLeg<'_>) -> Result<(), StoreError>;

    /// The folded position for `(account, symbol)` marked at `mark` (a live-only
    /// input; `None` ⇒ unpriced), or `None` when the account holds no fold for the
    /// symbol.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Arithmetic`] if the projection's checked arithmetic
    /// overflows the DTO integer width.
    fn get(
        &self,
        account: &AccountId,
        symbol: &Symbol,
        mark: Option<Cents>,
    ) -> Result<Option<Position>, StoreError>;

    /// All of `account`'s positions, each marked via `marks`, in a deterministic
    /// (symbol-ordered) sequence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Arithmetic`] if a projection's checked arithmetic
    /// overflows the DTO integer width.
    fn list(
        &self,
        account: &AccountId,
        marks: &dyn MarkSource,
    ) -> Result<Vec<Position>, StoreError>;
}

// ============================================================================
// In-memory positions store
// ============================================================================

/// The exact integer-cents fold state for one `(account, symbol)` position.
///
/// The three accumulators are the exact substrate for both the realized and the
/// unrealized split:
/// `realized_pnl = cash_ex_fee − fees + basis` and
/// `unrealized_pnl = net_quantity × mark − basis`, so
/// `realized + unrealized = cash_ex_fee − fees + net_quantity × mark` **exactly**
/// (both halves read the same `basis`). `avg_price` is the truncated display
/// derivation `|basis| / |net_quantity|`; the P&L never uses the truncated value,
/// so integer-cents rounding never leaks into the totals.
#[derive(Debug, Clone)]
struct PositionState {
    underlying: String,
    /// Net position in **signed contracts** (positive = long).
    net_quantity: i64,
    /// Signed cost basis of the **open** position in cents (same sign as
    /// `net_quantity`).
    basis: i128,
    /// Running `−Σ(signed_qty × price)` in cents (cash paid is negative).
    cash_ex_fee: i128,
    /// Running `Σ(fee)` in cents (a maker rebate is negative).
    fees: i128,
}

impl PositionState {
    fn new(underlying: impl Into<String>) -> Self {
        Self {
            underlying: underlying.into(),
            net_quantity: 0,
            basis: 0,
            cash_ex_fee: 0,
            fees: 0,
        }
    }

    /// Folds one leg into the state with checked `i128` arithmetic.
    fn fold(
        &mut self,
        side: SeamSide,
        quantity: u64,
        price: Cents,
        fee: SignedCents,
    ) -> Result<(), StoreError> {
        let qty = i64::try_from(quantity).map_err(|_| StoreError::Arithmetic)?;
        let dq = match side {
            SeamSide::Buy => qty,
            SeamSide::Sell => qty.checked_neg().ok_or(StoreError::Arithmetic)?,
        };
        let price_i = i128::from(price.get());
        let dqp = i128::from(dq)
            .checked_mul(price_i)
            .ok_or(StoreError::Arithmetic)?;

        self.fees = self
            .fees
            .checked_add(i128::from(fee.get()))
            .ok_or(StoreError::Arithmetic)?;
        self.cash_ex_fee = self
            .cash_ex_fee
            .checked_sub(dqp)
            .ok_or(StoreError::Arithmetic)?;

        let net = self.net_quantity;
        let increasing = net == 0 || (net > 0) == (dq > 0);
        if increasing {
            // Opening or adding in the same direction: the cost basis grows.
            self.basis = self.basis.checked_add(dqp).ok_or(StoreError::Arithmetic)?;
            self.net_quantity = net.checked_add(dq).ok_or(StoreError::Arithmetic)?;
        } else {
            let net_abs = i128::from(net.unsigned_abs());
            let dq_abs = i128::from(dq.unsigned_abs());
            if dq_abs <= net_abs {
                // Reduce / close without flipping: remove the proportional cost.
                let cost_removed = self
                    .basis
                    .checked_mul(dq_abs)
                    .ok_or(StoreError::Arithmetic)?
                    .checked_div(net_abs)
                    .ok_or(StoreError::Arithmetic)?;
                self.basis = self
                    .basis
                    .checked_sub(cost_removed)
                    .ok_or(StoreError::Arithmetic)?;
                self.net_quantity = net.checked_add(dq).ok_or(StoreError::Arithmetic)?;
                if self.net_quantity == 0 {
                    // A full close leaves no open cost basis.
                    self.basis = 0;
                }
            } else {
                // Flip: close the whole position, open the remainder at `price`.
                self.net_quantity = net.checked_add(dq).ok_or(StoreError::Arithmetic)?;
                self.basis = i128::from(self.net_quantity)
                    .checked_mul(price_i)
                    .ok_or(StoreError::Arithmetic)?;
            }
        }
        Ok(())
    }

    /// Captures the exact fold accumulators for a snapshot cut (#009) — the
    /// integer-cents state, never the derived mark / unrealised projection.
    fn to_capture(&self, account: &AccountId, symbol: &Symbol) -> PositionCapture {
        PositionCapture {
            account: account.clone(),
            symbol: symbol.clone(),
            underlying: self.underlying.clone(),
            net_quantity: self.net_quantity,
            basis: self.basis,
            cash_ex_fee: self.cash_ex_fee,
            fees: self.fees,
        }
    }

    /// Rebuilds a fold state from a captured cut — the restore side of
    /// [`to_capture`](Self::to_capture).
    fn from_capture(capture: &PositionCapture) -> Self {
        Self {
            underlying: capture.underlying.clone(),
            net_quantity: capture.net_quantity,
            basis: capture.basis,
            cash_ex_fee: capture.cash_ex_fee,
            fees: capture.fees,
        }
    }

    /// Projects the fold into a DTO [`Position`] marked at `mark`.
    fn project(
        &self,
        account: &AccountId,
        symbol: &Symbol,
        mark: Option<Cents>,
    ) -> Result<Position, StoreError> {
        let net = self.net_quantity;
        let avg_price = if net == 0 {
            Cents::new(0)
        } else {
            let magnitude = self.basis.unsigned_abs() / u128::from(net.unsigned_abs());
            Cents::new(u64::try_from(magnitude).map_err(|_| StoreError::Arithmetic)?)
        };

        // realized = cash_ex_fee − fees + basis (exact).
        let realized_i = self
            .cash_ex_fee
            .checked_sub(self.fees)
            .and_then(|v| v.checked_add(self.basis))
            .ok_or(StoreError::Arithmetic)?;
        let realized_pnl =
            SignedCents::new(i64::try_from(realized_i).map_err(|_| StoreError::Arithmetic)?);

        let (current_price, unrealized_pnl) = match mark {
            Some(mark) => {
                // unrealized = net × mark − basis (exact).
                let unrealized_i = i128::from(net)
                    .checked_mul(i128::from(mark.get()))
                    .and_then(|v| v.checked_sub(self.basis))
                    .ok_or(StoreError::Arithmetic)?;
                let unrealized_pnl = SignedCents::new(
                    i64::try_from(unrealized_i).map_err(|_| StoreError::Arithmetic)?,
                );
                (Some(mark), Some(unrealized_pnl))
            }
            None => (None, None),
        };

        Ok(Position {
            account: account.clone(),
            symbol: symbol.clone(),
            underlying: self.underlying.clone(),
            net_quantity: net,
            avg_price,
            current_price,
            realized_pnl,
            unrealized_pnl,
            // Delta exposure needs Greeks (`optionstratlib`), not wired in #008;
            // it defaults to `0.0` until the Greeks path lands.
            delta_exposure: 0.0,
        })
    }
}

/// The in-memory [`PositionsStore`] — a concurrent map keyed by
/// `(account, symbol)`, each holding an exact integer-cents fold.
///
/// `DashMap` (over `Arc<RwLock<HashMap<>>>`, per rules Concurrency) gives
/// lock-free point access; the mark is applied at read time, never stored, so the
/// fold state stays a pure function of the journal.
#[derive(Debug, Default)]
pub struct InMemoryPositionsStore {
    positions: DashMap<(AccountId, Symbol), PositionState>,
}

impl InMemoryPositionsStore {
    /// Builds an empty positions store.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Captures **only** `underlying`'s slice of the shared venue-wide fold — the
    /// `(account, symbol)` folds whose [`PositionCapture::underlying`] matches —
    /// sorted by `(account, symbol)` so the cut is deterministic regardless of map
    /// iteration order (#009).
    ///
    /// Like [`InMemoryExecutionsStore::capture_for`], the slice is exactly the
    /// (quiesced) capturing actor's folds — a **consistent cut for that
    /// underlying**, never a torn read of another underlying's concurrent writes
    /// ([02 §9](../../../docs/02-matching-architecture.md)).
    #[must_use]
    pub fn capture_for(&self, underlying: &str) -> Vec<PositionCapture> {
        let mut captures: Vec<PositionCapture> = self
            .positions
            .iter()
            .filter(|entry| position_fold_belongs(entry.value().underlying.as_str(), underlying))
            .map(|entry| {
                let (account, symbol) = entry.key();
                entry.value().to_capture(account, symbol)
            })
            .collect();
        captures.sort_by(|a, b| {
            a.account
                .as_str()
                .cmp(b.account.as_str())
                .then_with(|| a.symbol.as_str().cmp(b.symbol.as_str()))
        });
        captures
    }

    /// Validates that **every** fold in a restore cut belongs to `underlying`,
    /// **without** mutating the store — the pre-mutation, all-or-nothing ownership
    /// gate the actor's restore choreography runs in its preparation phase, before
    /// the epoch marker is journaled and before any store is swapped
    /// ([02 §9](../../../docs/02-matching-architecture.md)).
    ///
    /// The positions store is shared across every per-underlying actor and only the
    /// target actor is quiesced during a restore, so a cut carrying another
    /// underlying's folds is a **corrupt snapshot** that would otherwise inject or
    /// overwrite a live underlying's data. It is refused **wholesale** here rather
    /// than partially applied.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::RebuildFailed`] if any fold's underlying ticker is
    /// not `underlying`.
    pub fn validate_restore(
        &self,
        underlying: &str,
        captures: &[PositionCapture],
    ) -> Result<(), SnapshotError> {
        let foreign = captures
            .iter()
            .filter(|capture| !position_fold_belongs(capture.underlying.as_str(), underlying))
            .count();
        if foreign > 0 {
            return Err(SnapshotError::RebuildFailed(format!(
                "positions restore cut for {underlying} carries {foreign} fold(s) for another underlying"
            )));
        }
        Ok(())
    }

    /// Restores **only** `underlying`'s slice from a cut, replacing exactly that
    /// underlying's folds in place and leaving every other underlying's folds
    /// untouched — so restoring `BTC` never erases `ETH`'s (possibly newer)
    /// positions ([02 §9](../../../docs/02-matching-architecture.md)). Runs under
    /// the capturing actor's quiescence; other underlyings' actors need not quiesce
    /// because their folds occupy a disjoint keyspace.
    ///
    /// **Fail-closed on ownership**: because the store is shared venue-wide and only
    /// the target actor is quiesced, a fold belonging to another underlying is
    /// **never** inserted — it is skipped and the drop is logged. The actor path
    /// already refuses a mixed-underlying cut wholesale via [`validate_restore`]
    /// before the marker is journaled; this skip is the last-line defence for any
    /// direct caller, so foreign data can never land regardless of entry point.
    ///
    /// [`validate_restore`]: Self::validate_restore
    pub fn restore_for(&self, underlying: &str, captures: Vec<PositionCapture>) {
        // Drop only this underlying's slice; every other underlying's folds stay.
        self.positions
            .retain(|_, state| !position_fold_belongs(state.underlying.as_str(), underlying));
        let mut skipped: usize = 0;
        for capture in captures {
            if !position_fold_belongs(capture.underlying.as_str(), underlying) {
                // A foreign fold (a corrupt or hostile cut for another underlying)
                // is never inserted — it cannot overwrite a live underlying's data.
                skipped += 1;
                continue;
            }
            let key = (capture.account.clone(), capture.symbol.clone());
            self.positions
                .insert(key, PositionState::from_capture(&capture));
        }
        if skipped > 0 {
            tracing::warn!(
                underlying,
                skipped,
                "positions restore cut carried fold(s) for another underlying; skipped (foreign data never lands)"
            );
        }
    }
}

impl PositionsStore for InMemoryPositionsStore {
    fn apply(&self, leg: &PositionLeg<'_>) -> Result<(), StoreError> {
        let key = (leg.account.clone(), leg.symbol.clone());
        let mut state = self
            .positions
            .entry(key)
            .or_insert_with(|| PositionState::new(leg.underlying));
        state.fold(leg.side, leg.quantity, leg.price, leg.fee)
    }

    fn get(
        &self,
        account: &AccountId,
        symbol: &Symbol,
        mark: Option<Cents>,
    ) -> Result<Option<Position>, StoreError> {
        match self.positions.get(&(account.clone(), symbol.clone())) {
            Some(state) => Ok(Some(state.project(account, symbol, mark)?)),
            None => Ok(None),
        }
    }

    fn list(
        &self,
        account: &AccountId,
        marks: &dyn MarkSource,
    ) -> Result<Vec<Position>, StoreError> {
        let mut positions: Vec<Position> = Vec::new();
        for entry in self.positions.iter() {
            let (entry_account, symbol) = entry.key();
            if entry_account != account {
                continue;
            }
            let mark = marks.mark(symbol);
            positions.push(entry.value().project(account, symbol, mark)?);
        }
        // Deterministic, symbol-ordered output regardless of map iteration order.
        positions.sort_by(|a, b| a.symbol.as_str().cmp(b.symbol.as_str()));
        Ok(positions)
    }
}

// ============================================================================
// Fan-out into the stores (the FanOut seam #006 left open)
// ============================================================================

/// The fill-consuming [`FanOut`] the actor calls **after** a [`VenueEvent`] is
/// journaled (step 5) — the #008 replacement for `NoopFanOut`.
///
/// It projects each committed fill leg into the [`ExecutionsStore`] (an
/// authoritative [`ExecutionRecord`]) and the [`PositionsStore`] (the fold), and
/// feeds each trade print into the [`MarkPriceBook`]. Because it runs only on
/// journaled events, the executions log stays a deterministic function of the
/// journal; the mark it feeds is live-only and excluded from the oracle.
///
/// The stores are held behind `Arc` so the same instances are shared with the
/// REST read handlers (wired through `AppState`, #013); cloning an `Arc` handle
/// out before constructing the fan-out is the intended pattern.
///
/// ## Fan-out seal (projection integrity)
///
/// The executions log and the positions fold are **two authoritative
/// projections of the same journal**. If either write fails, continuing would
/// let them diverge from each other and from the committed journal. The fan-out
/// therefore **seals** on the first projection failure — it logs at `ERROR`,
/// stops projecting (no further leg, mark, or event), and reports the seal
/// through [`is_sealed`](Self::is_sealed) — mirroring the actor's
/// seal-on-post-mutation-failure fail-stop. Recovery rebuilds both projections
/// from the authoritative journal, so a sealed fan-out is the observable signal
/// that a journal-backed rebuild is required, never a silent divergence.
pub struct StoreFanOut<E, P> {
    executions: Arc<E>,
    positions: Arc<P>,
    marks: Arc<MarkPriceBook>,
    /// Set once a projection write fails: the fan-out is fail-stopped and drops
    /// every subsequent event until the projections are rebuilt from the journal.
    sealed: bool,
}

impl<E, P> StoreFanOut<E, P>
where
    E: ExecutionsStore,
    P: PositionsStore,
{
    /// Wires a fan-out over shared executions / positions stores and a mark book.
    #[must_use]
    #[inline]
    pub fn new(executions: Arc<E>, positions: Arc<P>, marks: Arc<MarkPriceBook>) -> Self {
        Self {
            executions,
            positions,
            marks,
            sealed: false,
        }
    }

    /// Whether a projection failure has sealed the fan-out. A sealed fan-out has
    /// stopped projecting; the executions / positions stores must be rebuilt from
    /// the journal (recovery) before they are authoritative again.
    #[must_use]
    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Seals the fan-out on a projection failure: records the seal, logs the cause
    /// at `ERROR` (observable), and — from here on — [`emit`](FanOut::emit) drops
    /// every event. `projection` names which store failed.
    fn seal(
        &mut self,
        event: &VenueEvent,
        projection: &'static str,
        error: &StoreError,
    ) -> FanOutSealed {
        self.sealed = true;
        tracing::error!(
            sequence = event.underlying_sequence.get(),
            projection,
            error = %error,
            "store projection failed; sealing the fan-out — the executions/positions \
             projections diverged from the journal and must be rebuilt from it"
        );
        FanOutSealed {
            projection,
            detail: error.to_string(),
        }
    }

    /// The shared executions store handle — the snapshot cut reads/replaces it
    /// through here (#009).
    #[must_use]
    #[inline]
    pub fn executions(&self) -> &Arc<E> {
        &self.executions
    }

    /// The shared positions store handle — the snapshot cut reads/replaces it
    /// through here (#009).
    #[must_use]
    #[inline]
    pub fn positions(&self) -> &Arc<P> {
        &self.positions
    }
}

impl<E, P> FanOut for StoreFanOut<E, P>
where
    E: ExecutionsStore,
    P: PositionsStore,
{
    fn emit(&mut self, event: &VenueEvent) -> Result<(), FanOutSealed> {
        // A prior projection failure fail-stopped the fan-out: the projections
        // diverged from the journal and must be rebuilt from it, so refuse every
        // further event (the actor is already sealed) rather than compound it.
        if self.sealed {
            return Err(FanOutSealed {
                projection: "store",
                detail: "fan-out already sealed by a prior projection failure".to_string(),
            });
        }
        let Some(symbol) = command_symbol(&event.command) else {
            return Ok(());
        };
        let fills = outcome_fills(&event.outcome);
        if fills.is_empty() {
            return Ok(());
        }
        let Some(underlying) = underlying_of(symbol) else {
            // The symbol was already validated on the way in, so this is
            // unreachable; fail safe (drop the fan-out for this event) rather
            // than record an unattributable leg.
            tracing::error!(
                symbol = symbol.as_str(),
                sequence = event.underlying_sequence.get(),
                "could not resolve the underlying from a validated symbol; skipping fan-out"
            );
            return Ok(());
        };

        // Project each leg into BOTH authoritative stores. On the FIRST failure,
        // seal and stop — never silently continue into the sibling projection or
        // the next leg, which is what would let the two stores (and the journal)
        // diverge. The mark is advanced only after every leg projected cleanly.
        for fill in fills {
            let record = project_execution(event, symbol, &underlying, fill);
            if let Err(error) = self.executions.record(record) {
                return Err(self.seal(event, "executions", &error));
            }
            let leg = PositionLeg {
                account: &fill.account,
                symbol,
                underlying: &underlying,
                side: fill.side,
                quantity: fill.quantity,
                price: fill.price,
                fee: fill.fee,
            };
            if let Err(error) = self.positions.apply(&leg) {
                return Err(self.seal(event, "positions", &error));
            }
        }

        // Advance the dampened mark ONCE per `execution_id`: a match's two linked
        // legs share one id (and one price), so the trade prints once — not once
        // per account leg, which would double-advance the mark for a single trade.
        // Deduped over the ordered `fills` (membership only; no map-iteration
        // order), and the mark is live-only, excluded from the determinism oracle.
        let mut printed: HashSet<&ExecutionId> = HashSet::new();
        for fill in fills {
            if printed.insert(&fill.execution_id) {
                self.marks.on_trade(symbol, fill.price);
            }
        }
        Ok(())
    }
}

// ============================================================================
// Projection helpers
// ============================================================================

/// The DTO [`Side`] for an upstream matching-seam [`SeamSide`].
#[inline]
const fn dto_side(side: SeamSide) -> Side {
    match side {
        SeamSide::Buy => Side::Buy,
        SeamSide::Sell => Side::Sell,
    }
}

/// The contract symbol a command targets, when it can produce fills.
#[inline]
fn command_symbol(command: &VenueCommand) -> Option<&Symbol> {
    match command {
        VenueCommand::AddOrder { symbol, .. } | VenueCommand::Replace { symbol, .. } => {
            Some(symbol)
        }
        _ => None,
    }
}

/// The captured fill legs of an outcome (empty for a non-filling outcome).
#[inline]
fn outcome_fills(outcome: &VenueOutcome) -> &[Fill] {
    match outcome {
        VenueOutcome::Added { fills, .. } | VenueOutcome::Market { fills, .. } => fills,
        VenueOutcome::Replace { add, .. } => match add {
            AddOutcome::Filled { fills, .. } | AddOutcome::Rested { fills, .. } => fills,
            AddOutcome::Rejected { .. } => &[],
        },
        _ => &[],
    }
}

/// The underlying ticker of a validated symbol, via the upstream
/// [`SymbolParser`] (never hand-parsed).
#[inline]
fn underlying_of(symbol: &Symbol) -> Option<String> {
    SymbolParser::parse(symbol.as_str())
        .ok()
        .map(|parsed| parsed.underlying().to_string())
}

/// Projects one internal [`Fill`] leg + its enclosing [`VenueEvent`] into an
/// authoritative account-scoped [`ExecutionRecord`].
///
/// The journal-derived fields (ids, account, side, liquidity, quantity, price,
/// fee, sequence, timestamp) are deterministic. `theo_value_cents`,
/// `edge_cents`, and `latency_us` are live-only analytics that need a pricer /
/// latency injector not wired in #008: `theo_value_cents` defaults to the fill
/// price so `edge_cents` is `0`, and `latency_us` is `0`. Later issues supply
/// real values without changing the wire shape ([01 §7](../../../docs/01-domain-model.md)).
#[must_use]
fn project_execution(
    event: &VenueEvent,
    symbol: &Symbol,
    underlying: &str,
    fill: &Fill,
) -> ExecutionRecord {
    ExecutionRecord {
        execution_id: fill.execution_id.clone(),
        order_id: fill.order_id.clone(),
        account: fill.account.clone(),
        symbol: underlying.to_string(),
        instrument: symbol.clone(),
        side: dto_side(fill.side),
        liquidity: fill.liquidity,
        quantity: fill.quantity,
        price_cents: fill.price,
        fee_cents: fill.fee,
        theo_value_cents: fill.price,
        edge_cents: SignedCents::new(0),
        underlying_sequence: event.underlying_sequence,
        latency_us: 0,
        executed_at: event.venue_ts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::boundary::{Hash32, STPMode, TimeInForce};
    use crate::exchange::event::{EventTimestamp, SequenceNumber};
    use crate::exchange::identity::LineageId;
    use crate::models::OrderType;

    const UNDERLYING: &str = "BTC";

    fn sym() -> Symbol {
        match Symbol::parse("BTC-20240329-50000-C") {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
        }
    }

    fn lineage() -> LineageId {
        LineageId::new("run-1")
    }

    /// Builds the two linked legs (maker + taker) of one match at `sequence`,
    /// sharing one execution id, each with its own account / side / fee.
    fn match_legs(sequence: u64, price: u64, quantity: u64) -> (Fill, Fill) {
        let lineage = lineage();
        let seq = SequenceNumber::new(sequence);
        let execution_id = lineage.execution_id(UNDERLYING, seq, 0);
        let maker = Fill {
            execution_id: execution_id.clone(),
            order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0),
            account: AccountId::new("maker"),
            owner: Hash32([0x11; 32]),
            side: SeamSide::Sell,
            liquidity: LiquidityFlag::Maker,
            price: Cents::new(price),
            quantity,
            fee: SignedCents::new(-10),
        };
        let taker = Fill {
            execution_id,
            order_id: lineage.venue_order_id(UNDERLYING, seq, 0),
            account: AccountId::new("taker"),
            owner: Hash32([0x22; 32]),
            side: SeamSide::Buy,
            liquidity: LiquidityFlag::Taker,
            price: Cents::new(price),
            quantity,
            fee: SignedCents::new(15),
        };
        (maker, taker)
    }

    fn added_event(sequence: u64, price: u64, quantity: u64) -> VenueEvent {
        let (maker, taker) = match_legs(sequence, price, quantity);
        let command = VenueCommand::AddOrder {
            symbol: sym(),
            order_id: taker.order_id.clone(),
            account: taker.account.clone(),
            owner: taker.owner,
            client_order_id: None,
            side: SeamSide::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        };
        VenueEvent::new(
            SequenceNumber::new(sequence),
            EventTimestamp::new(1_700_000_000_000),
            command,
            VenueOutcome::Added {
                fills: vec![maker, taker],
                resting_quantity: 0,
                stp_cancelled: vec![],
            },
        )
    }

    // ---- executions: both legs of a match ---------------------------------

    #[test]
    fn test_executions_store_records_both_legs_of_a_match() {
        let store = InMemoryExecutionsStore::new();
        let event = added_event(1, 50_000, 2);
        let mut fan = StoreFanOut::new(
            Arc::new(store),
            Arc::new(InMemoryPositionsStore::new()),
            Arc::new(MarkPriceBook::new()),
        );
        let _ = fan.emit(&event);

        let exec = &fan.executions;
        assert_eq!(exec.len(), 2, "one match records two legs");

        let execution_id = lineage().execution_id(UNDERLYING, SequenceNumber::new(1), 0);
        let maker = match exec.get(&execution_id, &AccountId::new("maker")) {
            Ok(Some(record)) => record,
            other => panic!("expected the maker leg, got {other:?}"),
        };
        let taker = match exec.get(&execution_id, &AccountId::new("taker")) {
            Ok(Some(record)) => record,
            other => panic!("expected the taker leg, got {other:?}"),
        };
        // Both legs share one execution id but keep distinct accounts / fees.
        assert_eq!(maker.execution_id, taker.execution_id);
        assert_eq!(maker.liquidity, LiquidityFlag::Maker);
        assert_eq!(taker.liquidity, LiquidityFlag::Taker);
        assert_eq!(maker.account, AccountId::new("maker"));
        assert_eq!(taker.account, AccountId::new("taker"));
        assert_eq!(maker.fee_cents, SignedCents::new(-10));
        assert_eq!(taker.fee_cents, SignedCents::new(15));
        // The journal-derived join keys and money project through.
        assert_eq!(maker.underlying_sequence, SequenceNumber::new(1));
        assert_eq!(taker.price_cents, Cents::new(50_000));
        assert_eq!(taker.symbol, "BTC");
        assert_eq!(taker.instrument, sym());
        assert_eq!(maker.side, Side::Sell);
        assert_eq!(taker.side, Side::Buy);
        // No pricer / latency wired: theo defaults to the fill price (edge 0).
        assert_eq!(taker.theo_value_cents, Cents::new(50_000));
        assert_eq!(taker.edge_cents, SignedCents::new(0));
        assert_eq!(taker.latency_us, 0);
    }

    #[test]
    fn test_executions_list_is_account_scoped_and_ordered() {
        let store = InMemoryExecutionsStore::new();
        let mut fan = StoreFanOut::new(
            Arc::new(store),
            Arc::new(InMemoryPositionsStore::new()),
            Arc::new(MarkPriceBook::new()),
        );
        let _ = fan.emit(&added_event(1, 50_000, 2));
        let _ = fan.emit(&added_event(2, 50_100, 1));

        let taker_list = match fan
            .executions
            .list(&AccountId::new("taker"), &ExecutionFilter::default())
        {
            Ok(list) => list,
            Err(e) => panic!("list failed: {e}"),
        };
        assert_eq!(taker_list.len(), 2, "the taker has a leg in each match");
        // Insertion (journal) order: sequence 1 before sequence 2.
        assert_eq!(taker_list[0].underlying_sequence, SequenceNumber::new(1));
        assert_eq!(taker_list[1].underlying_sequence, SequenceNumber::new(2));

        // The underlying filter and limit apply.
        let filtered = match fan.executions.list(
            &AccountId::new("taker"),
            &ExecutionFilter {
                underlying: Some("ETH".to_string()),
                limit: None,
            },
        ) {
            Ok(list) => list,
            Err(e) => panic!("list failed: {e}"),
        };
        assert!(filtered.is_empty(), "no BTC leg matches an ETH filter");
    }

    // ---- positions fold ----------------------------------------------------

    fn leg<'a>(
        account: &'a AccountId,
        symbol: &'a Symbol,
        side: SeamSide,
        quantity: u64,
        price: u64,
        fee: i64,
    ) -> PositionLeg<'a> {
        PositionLeg {
            account,
            symbol,
            underlying: UNDERLYING,
            side,
            quantity,
            price: Cents::new(price),
            fee: SignedCents::new(fee),
        }
    }

    #[test]
    fn test_positions_fold_signs_net_quantity_by_side() {
        let store = InMemoryPositionsStore::new();
        let account = AccountId::new("acct-1");
        let symbol = sym();
        // Buy 3, then sell 1 → net +2.
        store
            .apply(&leg(&account, &symbol, SeamSide::Buy, 3, 50_000, 0))
            .expect("apply buy");
        store
            .apply(&leg(&account, &symbol, SeamSide::Sell, 1, 50_500, 0))
            .expect("apply sell");
        let position = match store.get(&account, &symbol, None) {
            Ok(Some(p)) => p,
            other => panic!("expected a position, got {other:?}"),
        };
        assert_eq!(position.net_quantity, 2);
        assert_eq!(position.avg_price, Cents::new(50_000));
        // Reduced a long by 1 at +500 over cost → realized +500.
        assert_eq!(position.realized_pnl, SignedCents::new(500));
        // Unpriced: no mark, no unrealized.
        assert_eq!(position.current_price, None);
        assert_eq!(position.unrealized_pnl, None);
    }

    #[test]
    fn test_positions_avg_price_is_volume_weighted() {
        let store = InMemoryPositionsStore::new();
        let account = AccountId::new("acct-1");
        let symbol = sym();
        // Buy 2 @ 100, buy 3 @ 200 → avg (2*100 + 3*200) / 5 = 160.
        store
            .apply(&leg(&account, &symbol, SeamSide::Buy, 2, 100, 0))
            .expect("apply");
        store
            .apply(&leg(&account, &symbol, SeamSide::Buy, 3, 200, 0))
            .expect("apply");
        let position = match store.get(&account, &symbol, None) {
            Ok(Some(p)) => p,
            other => panic!("expected a position, got {other:?}"),
        };
        assert_eq!(position.net_quantity, 5);
        assert_eq!(position.avg_price, Cents::new(160));
    }

    #[test]
    fn test_positions_fold_short_with_mark_matches_golden_scenario() {
        let store = InMemoryPositionsStore::new();
        let account = AccountId::new("acct-1");
        let symbol = sym();
        // Sell 8 @ 50_000 (open short), buy 3 @ 49_600 (partial close).
        store
            .apply(&leg(&account, &symbol, SeamSide::Sell, 8, 50_000, 0))
            .expect("apply");
        store
            .apply(&leg(&account, &symbol, SeamSide::Buy, 3, 49_600, 0))
            .expect("apply");
        let mark = Cents::new(50_500);
        let position = match store.get(&account, &symbol, Some(mark)) {
            Ok(Some(p)) => p,
            other => panic!("expected a position, got {other:?}"),
        };
        assert_eq!(position.net_quantity, -5);
        assert_eq!(position.avg_price, Cents::new(50_000));
        assert_eq!(position.realized_pnl, SignedCents::new(1_200));
        assert_eq!(position.current_price, Some(mark));
        assert_eq!(position.unrealized_pnl, Some(SignedCents::new(-2_500)));
        // The consistency identity holds exactly.
        let realized = position.realized_pnl.get();
        let unrealized = position.unrealized_pnl.map(SignedCents::get).unwrap_or(0);
        // cash_ex_fee: +400_000 (sold 8) − 148_800 (bought 3) = 251_200; fees 0.
        let cash_ex_fee: i128 = 251_200;
        let expected = cash_ex_fee + i128::from(position.net_quantity) * i128::from(mark.get());
        assert_eq!(i128::from(realized) + i128::from(unrealized), expected);
    }

    #[test]
    fn test_positions_flip_resets_basis_to_new_open() {
        let store = InMemoryPositionsStore::new();
        let account = AccountId::new("acct-1");
        let symbol = sym();
        // Long 2 @ 100, then sell 5 @ 300 → flip to short 3 opened at 300.
        store
            .apply(&leg(&account, &symbol, SeamSide::Buy, 2, 100, 0))
            .expect("apply");
        store
            .apply(&leg(&account, &symbol, SeamSide::Sell, 5, 300, 0))
            .expect("apply");
        let position = match store.get(&account, &symbol, Some(Cents::new(300))) {
            Ok(Some(p)) => p,
            other => panic!("expected a position, got {other:?}"),
        };
        assert_eq!(position.net_quantity, -3);
        assert_eq!(position.avg_price, Cents::new(300));
        // Closed the long of 2 at +200 each → realized +400; the new short is at
        // the mark, so unrealized 0.
        assert_eq!(position.realized_pnl, SignedCents::new(400));
        assert_eq!(position.unrealized_pnl, Some(SignedCents::new(0)));
    }

    #[test]
    fn test_positions_both_accounts_fold_from_one_match() {
        let positions = Arc::new(InMemoryPositionsStore::new());
        let mut fan = StoreFanOut::new(
            Arc::new(InMemoryExecutionsStore::new()),
            Arc::clone(&positions),
            Arc::new(MarkPriceBook::new()),
        );
        let _ = fan.emit(&added_event(1, 50_000, 2));

        let symbol = sym();
        let maker = match positions.get(&AccountId::new("maker"), &symbol, None) {
            Ok(Some(p)) => p,
            other => panic!("expected the maker position, got {other:?}"),
        };
        let taker = match positions.get(&AccountId::new("taker"), &symbol, None) {
            Ok(Some(p)) => p,
            other => panic!("expected the taker position, got {other:?}"),
        };
        // The maker sold (short), the taker bought (long); each holds its own leg.
        assert_eq!(maker.net_quantity, -2);
        assert_eq!(taker.net_quantity, 2);
        assert_eq!(maker.avg_price, Cents::new(50_000));
        assert_eq!(taker.avg_price, Cents::new(50_000));
        // Fees are realized immediately: maker rebate +10, taker fee −15.
        assert_eq!(maker.realized_pnl, SignedCents::new(10));
        assert_eq!(taker.realized_pnl, SignedCents::new(-15));
    }

    #[test]
    fn test_store_fanout_skips_an_idempotent_duplicate() {
        // A #099 idempotent retry surfaces `VenueOutcome::Duplicate`; the store
        // fan-out MUST fold nothing — the original placement's legs were already
        // folded at first placement, so re-folding would double-count both the
        // executions and the positions.
        let executions = Arc::new(InMemoryExecutionsStore::new());
        let positions = Arc::new(InMemoryPositionsStore::new());
        let mut fan = StoreFanOut::new(
            Arc::clone(&executions),
            Arc::clone(&positions),
            Arc::new(MarkPriceBook::new()),
        );

        // First placement folds both legs once.
        let _ = fan.emit(&added_event(1, 50_000, 2));
        assert_eq!(executions.len(), 2, "the fresh placement records two legs");

        // The retry event carries the SAME command with a `Duplicate` outcome.
        let first = added_event(1, 50_000, 2);
        let duplicate = VenueEvent::new(
            SequenceNumber::new(2),
            EventTimestamp::new(1_700_000_000_000),
            first.command.clone(),
            VenueOutcome::Duplicate {
                original_order_id: lineage().venue_order_id(UNDERLYING, SequenceNumber::new(1), 0),
                original_sequence: SequenceNumber::new(1),
                terminal: Box::new(first.outcome.clone()),
            },
        );
        let _ = fan.emit(&duplicate);

        assert_eq!(
            executions.len(),
            2,
            "a Duplicate records no additional execution leg"
        );
        let symbol = sym();
        let taker = match positions.get(&AccountId::new("taker"), &symbol, None) {
            Ok(Some(p)) => p,
            other => panic!("expected the taker position, got {other:?}"),
        };
        assert_eq!(
            taker.net_quantity, 2,
            "the taker position stayed folded exactly once (no double-count)"
        );
    }

    #[test]
    fn test_positions_list_is_symbol_ordered() {
        let store = InMemoryPositionsStore::new();
        let account = AccountId::new("acct-1");
        let call = sym();
        let put = match Symbol::parse("BTC-20240329-50000-P") {
            Ok(s) => s,
            Err(e) => panic!("fixture parse failed: {e:?}"),
        };
        store
            .apply(&leg(&account, &put, SeamSide::Buy, 1, 100, 0))
            .expect("apply");
        store
            .apply(&leg(&account, &call, SeamSide::Buy, 1, 100, 0))
            .expect("apply");
        let list = match store.list(&account, &NoMarks) {
            Ok(l) => l,
            Err(e) => panic!("list failed: {e}"),
        };
        assert_eq!(list.len(), 2);
        // `...-C` sorts before `...-P`.
        assert_eq!(list[0].symbol, call);
        assert_eq!(list[1].symbol, put);
    }

    // ---- mark price book (upstream MarkPriceCalculator wiring) --------------

    #[test]
    fn test_mark_price_book_yields_last_trade_as_first_mark() {
        let book = MarkPriceBook::new();
        let symbol = sym();
        // Unpriced before any trade.
        assert_eq!(book.mark(&symbol), None);
        // The first advance from a zero mark returns the raw input undampened, so
        // a single trade print becomes the mark.
        book.on_trade(&symbol, Cents::new(50_000));
        assert_eq!(book.mark(&symbol), Some(Cents::new(50_000)));
    }

    // ---- non-filling events do not touch the stores ------------------------

    #[test]
    fn test_fan_out_ignores_events_without_fills() {
        let executions = Arc::new(InMemoryExecutionsStore::new());
        let positions = Arc::new(InMemoryPositionsStore::new());
        let mut fan = StoreFanOut::new(
            Arc::clone(&executions),
            Arc::clone(&positions),
            Arc::new(MarkPriceBook::new()),
        );
        // A resting-only add (no fills) records nothing.
        let lineage = lineage();
        let command = VenueCommand::AddOrder {
            symbol: sym(),
            order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0),
            account: AccountId::new("acct-1"),
            owner: Hash32([0x11; 32]),
            client_order_id: None,
            side: SeamSide::Sell,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 3,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        };
        let event = VenueEvent::new(
            SequenceNumber::new(0),
            EventTimestamp::new(1),
            command,
            VenueOutcome::Added {
                fills: vec![],
                resting_quantity: 3,
                stp_cancelled: vec![],
            },
        );
        let _ = fan.emit(&event);
        assert!(executions.is_empty());
        assert!(
            positions
                .get(&AccountId::new("acct-1"), &sym(), None)
                .expect("get")
                .is_none()
        );
    }

    // ---- mark dedup: one print per match, not per leg ----------------------

    #[test]
    fn test_fan_out_advances_mark_once_per_match_not_per_leg() {
        // A match's two linked legs (maker + taker) share one `execution_id` and
        // one price, so the trade must print to the mark ONCE — not once per
        // account leg, which would double-advance the dampened mark for one trade.
        let symbol = sym();
        // Seed a starting mark different from the match price. The upstream
        // dampening (default factor 0.01 → max ±ceil(prev*0.01) per step) makes
        // one advance distinguishable from two, so the assertion is not vacuous.
        let marks = Arc::new(MarkPriceBook::new());
        marks.on_trade(&symbol, Cents::new(40_000));

        let mut fan = StoreFanOut::new(
            Arc::new(InMemoryExecutionsStore::new()),
            Arc::new(InMemoryPositionsStore::new()),
            Arc::clone(&marks),
        );
        // One 2-leg match at 50_000 — a single `execution_id`.
        let _ = fan.emit(&added_event(1, 50_000, 2));

        // Reference books built from the SAME seed: one advance vs two advances.
        let once = MarkPriceBook::new();
        once.on_trade(&symbol, Cents::new(40_000));
        once.on_trade(&symbol, Cents::new(50_000));
        let twice = MarkPriceBook::new();
        twice.on_trade(&symbol, Cents::new(40_000));
        twice.on_trade(&symbol, Cents::new(50_000));
        twice.on_trade(&symbol, Cents::new(50_000));
        // Guard: the dampening genuinely distinguishes one advance from two, so a
        // double-advance bug cannot slip past this test as a false pass.
        assert_ne!(
            once.mark(&symbol),
            twice.mark(&symbol),
            "the dampening must distinguish one advance from two"
        );
        // The fan-out advanced the mark exactly once for the single match.
        assert_eq!(
            marks.mark(&symbol),
            once.mark(&symbol),
            "one mark print per execution_id (per match), not per account leg"
        );
    }

    // ---- projection-failure seal (fan-out integrity) -----------------------

    /// A fault-injecting [`ExecutionsStore`] that fails `record` on demand and
    /// otherwise delegates — the store-side stand-in for a durable backend failure
    /// (the in-memory store never fails in practice).
    struct FaultyExecutions {
        inner: InMemoryExecutionsStore,
        fail_record: bool,
    }

    impl ExecutionsStore for FaultyExecutions {
        fn record(&self, record: ExecutionRecord) -> Result<(), StoreError> {
            if self.fail_record {
                return Err(StoreError::Backend(
                    "injected executions failure".to_string(),
                ));
            }
            self.inner.record(record)
        }
        fn get(
            &self,
            execution_id: &ExecutionId,
            account: &AccountId,
        ) -> Result<Option<ExecutionRecord>, StoreError> {
            self.inner.get(execution_id, account)
        }
        fn list(
            &self,
            account: &AccountId,
            filter: &ExecutionFilter,
        ) -> Result<Vec<ExecutionRecord>, StoreError> {
            self.inner.list(account, filter)
        }
        fn len(&self) -> usize {
            self.inner.len()
        }
    }

    /// A fault-injecting [`PositionsStore`] that fails `apply` on demand and
    /// otherwise delegates.
    struct FaultyPositions {
        inner: InMemoryPositionsStore,
        fail_apply: bool,
    }

    impl PositionsStore for FaultyPositions {
        fn apply(&self, leg: &PositionLeg<'_>) -> Result<(), StoreError> {
            if self.fail_apply {
                return Err(StoreError::Backend(
                    "injected positions failure".to_string(),
                ));
            }
            self.inner.apply(leg)
        }
        fn get(
            &self,
            account: &AccountId,
            symbol: &Symbol,
            mark: Option<Cents>,
        ) -> Result<Option<Position>, StoreError> {
            self.inner.get(account, symbol, mark)
        }
        fn list(
            &self,
            account: &AccountId,
            marks: &dyn MarkSource,
        ) -> Result<Vec<Position>, StoreError> {
            self.inner.list(account, marks)
        }
    }

    #[test]
    fn test_fan_out_seals_on_executions_failure_without_diverging() {
        // The executions write fails FIRST, so the sibling positions fold is never
        // reached: the two authoritative projections do not diverge (both are
        // consistently missing the fill), and the fan-out seals — the observable
        // signal that a journal-backed rebuild is required.
        let executions = Arc::new(FaultyExecutions {
            inner: InMemoryExecutionsStore::new(),
            fail_record: true,
        });
        let positions = Arc::new(InMemoryPositionsStore::new());
        let mut fan = StoreFanOut::new(
            Arc::clone(&executions),
            Arc::clone(&positions),
            Arc::new(MarkPriceBook::new()),
        );
        let sealed = fan.emit(&added_event(1, 50_000, 2));
        assert_eq!(
            sealed.err().map(|s| s.projection),
            Some("executions"),
            "the sealing emit surfaces the failed projection to the actor (#131)"
        );

        assert!(fan.is_sealed(), "a projection failure seals the fan-out");
        assert!(
            executions.inner.is_empty(),
            "the failed executions write recorded nothing"
        );
        assert!(
            positions
                .get(&AccountId::new("taker"), &sym(), None)
                .expect("get")
                .is_none(),
            "positions was never folded (no divergence between the two projections)"
        );

        // A subsequent healthy event is dropped while sealed: positions would fold
        // cleanly if the loop were reached, but the seal short-circuits emit (still
        // erroring), so the projection never advances — no compounding divergence.
        assert!(
            fan.emit(&added_event(2, 50_100, 1)).is_err(),
            "a sealed fan-out keeps erroring so the actor stays fail-stopped"
        );
        assert!(
            positions
                .get(&AccountId::new("taker"), &sym(), None)
                .expect("get")
                .is_none(),
            "the sealed fan-out projects nothing further"
        );
    }

    #[test]
    fn test_fan_out_seals_on_positions_failure() {
        // The positions fold fails after the leg was recorded. That single leg is a
        // residual divergence the journal-backed rebuild reconciles; the seal makes
        // it observable and stops any further (divergent) writes.
        let executions = Arc::new(InMemoryExecutionsStore::new());
        let positions = Arc::new(FaultyPositions {
            inner: InMemoryPositionsStore::new(),
            fail_apply: true,
        });
        let mut fan = StoreFanOut::new(
            Arc::clone(&executions),
            Arc::clone(&positions),
            Arc::new(MarkPriceBook::new()),
        );
        assert!(
            fan.emit(&added_event(1, 50_000, 2)).is_err(),
            "a positions-fold failure surfaces a seal to the actor"
        );

        assert!(
            fan.is_sealed(),
            "a positions-fold failure also seals the fan-out"
        );
        // Exactly the one pre-seal leg was recorded before the fold failed.
        assert_eq!(executions.len(), 1, "only the pre-seal leg was recorded");

        // The sealed fan-out drops every subsequent event.
        assert!(fan.emit(&added_event(2, 50_100, 1)).is_err());
        assert_eq!(
            executions.len(),
            1,
            "the sealed fan-out records nothing after the seal"
        );
    }

    // ---- snapshot capture / restore (#009) -------------------------------

    #[test]
    fn test_executions_capture_and_restore_round_trip() {
        let mut fan = StoreFanOut::new(
            Arc::new(InMemoryExecutionsStore::new()),
            Arc::new(InMemoryPositionsStore::new()),
            Arc::new(MarkPriceBook::new()),
        );
        let _ = fan.emit(&added_event(1, 50_000, 2));
        let _ = fan.emit(&added_event(2, 50_100, 1));
        let captured = fan.executions().capture_for("BTC");
        assert_eq!(captured.len(), 4, "two matches record four legs");

        let restored = InMemoryExecutionsStore::new();
        restored.restore_for("BTC", captured);
        // The taker's list is identical (same legs, same journal order) on the
        // restored store.
        let taker = AccountId::new("taker");
        let original = fan
            .executions()
            .list(&taker, &ExecutionFilter::default())
            .expect("list");
        let after = restored
            .list(&taker, &ExecutionFilter::default())
            .expect("list");
        assert_eq!(original, after);
    }

    #[test]
    fn test_positions_capture_and_restore_round_trip() {
        let store = InMemoryPositionsStore::new();
        let account = AccountId::new("acct-1");
        let symbol = sym();
        store
            .apply(&leg(&account, &symbol, SeamSide::Sell, 8, 50_000, 0))
            .expect("apply");
        store
            .apply(&leg(&account, &symbol, SeamSide::Buy, 3, 49_600, 0))
            .expect("apply");
        let mark = Some(Cents::new(50_500));
        let before = store
            .get(&account, &symbol, mark)
            .expect("get")
            .expect("a position");

        let captured = store.capture_for("BTC");
        assert_eq!(captured.len(), 1);
        let restored = InMemoryPositionsStore::new();
        restored.restore_for("BTC", captured);
        let after = restored
            .get(&account, &symbol, mark)
            .expect("get")
            .expect("a position");
        // The exact accumulators round-trip, so the marked projection is identical.
        assert_eq!(before, after);
    }

    #[test]
    fn test_restore_for_replaces_the_underlyings_prior_slice() {
        // A restore of an underlying's slice is a wholesale replace of THAT
        // underlying's legs, not a merge: the prior BTC contents are dropped so a
        // post-restore read of BTC is the cut, exactly.
        let store = InMemoryExecutionsStore::new();
        let mut fan = StoreFanOut::new(
            Arc::new(InMemoryExecutionsStore::new()),
            Arc::new(InMemoryPositionsStore::new()),
            Arc::new(MarkPriceBook::new()),
        );
        let _ = fan.emit(&added_event(1, 50_000, 2));
        let cut = fan.executions().capture_for("BTC");

        // A different store whose only content is other BTC legs is fully replaced
        // by the BTC cut (both events are BTC, so the whole store is the slice).
        let mut other = StoreFanOut::new(
            Arc::new(store),
            Arc::new(InMemoryPositionsStore::new()),
            Arc::new(MarkPriceBook::new()),
        );
        let _ = other.emit(&added_event(9, 40_000, 5));
        other.executions().restore_for("BTC", cut);
        assert_eq!(
            other.executions().len(),
            2,
            "only the cut's two BTC legs remain"
        );
    }

    /// Builds one authoritative [`ExecutionRecord`] directly for `underlying` — the
    /// venue-wide store is keyed by `(execution_id, liquidity)` and filtered by the
    /// `symbol` underlying ticker, so this stands in for a fan-out leg without a
    /// full match.
    fn exec_record(underlying: &str, instrument: &Symbol, exec_tag: &str) -> ExecutionRecord {
        let lineage = lineage();
        ExecutionRecord {
            execution_id: ExecutionId::new(exec_tag),
            order_id: lineage.venue_order_id(underlying, SequenceNumber::new(0), 0),
            account: AccountId::new("acct-1"),
            symbol: underlying.to_string(),
            instrument: instrument.clone(),
            side: Side::Buy,
            liquidity: LiquidityFlag::Taker,
            quantity: 1,
            price_cents: Cents::new(50_000),
            fee_cents: SignedCents::new(0),
            theo_value_cents: Cents::new(50_000),
            edge_cents: SignedCents::new(0),
            underlying_sequence: SequenceNumber::new(0),
            latency_us: 0,
            executed_at: EventTimestamp::new(1_700_000_000_000),
        }
    }

    #[test]
    fn test_restore_for_one_underlying_preserves_another_underlyings_newer_data() {
        // One shared venue-wide pair of stores, as `AppState` wires them across the
        // per-underlying actors: each underlying writes only its own legs/folds.
        let executions = InMemoryExecutionsStore::new();
        let positions = InMemoryPositionsStore::new();

        let btc = sym(); // BTC-20240329-50000-C
        let eth = match Symbol::parse("ETH-20240329-3000-C") {
            Ok(s) => s,
            Err(e) => panic!("ETH fixture parse failed: {e:?}"),
        };
        let account = AccountId::new("acct-1");

        // BTC records one leg/fold, then BTC's (quiesced) actor captures its slice.
        executions
            .record(exec_record("BTC", &btc, "e-btc"))
            .expect("record BTC");
        positions
            .apply(&leg(&account, &btc, SeamSide::Buy, 2, 50_000, 0))
            .expect("apply BTC");
        let btc_exec_cut = executions.capture_for("BTC");
        let btc_pos_cut = positions.capture_for("BTC");
        assert_eq!(btc_exec_cut.len(), 1, "the BTC cut is scoped to BTC");
        assert_eq!(btc_pos_cut.len(), 1);

        // AFTER BTC's capture, ETH's independent actor writes NEWER executions and
        // positions into the SAME shared stores.
        executions
            .record(exec_record("ETH", &eth, "e-eth"))
            .expect("record ETH");
        positions
            .apply(&PositionLeg {
                account: &account,
                symbol: &eth,
                underlying: "ETH",
                side: SeamSide::Buy,
                quantity: 5,
                price: Cents::new(3_000),
                fee: SignedCents::new(0),
            })
            .expect("apply ETH");

        // BTC restores its (older) snapshot — this must touch ONLY BTC's slice.
        executions.restore_for("BTC", btc_exec_cut);
        positions.restore_for("BTC", btc_pos_cut);

        // ETH's newer executions and positions SURVIVE the BTC restore.
        let eth_execs = executions
            .list(
                &account,
                &ExecutionFilter {
                    underlying: Some("ETH".to_string()),
                    limit: None,
                },
            )
            .expect("list ETH");
        assert_eq!(
            eth_execs.len(),
            1,
            "ETH's newer execution must survive a BTC restore"
        );
        let eth_pos = positions
            .get(&account, &eth, None)
            .expect("get ETH")
            .expect("ETH position survives");
        assert_eq!(
            eth_pos.net_quantity, 5,
            "ETH's newer position must survive a BTC restore"
        );

        // BTC's slice is exactly the restored cut (unchanged, not erased).
        let btc_execs = executions
            .list(
                &account,
                &ExecutionFilter {
                    underlying: Some("BTC".to_string()),
                    limit: None,
                },
            )
            .expect("list BTC");
        assert_eq!(btc_execs.len(), 1, "BTC's slice is the restored cut");
        let btc_pos = positions
            .get(&account, &btc, None)
            .expect("get BTC")
            .expect("BTC position present");
        assert_eq!(btc_pos.net_quantity, 2);
    }

    #[test]
    fn test_restore_for_rejects_foreign_underlying_records() {
        // A malformed or hostile snapshot cut for BTC that also carries an ETH leg
        // must NEVER inject or overwrite ETH's rows — the store is shared across the
        // per-underlying actors and only the BTC actor is quiesced during a restore.
        let btc = sym(); // BTC-20240329-50000-C
        let eth = match Symbol::parse("ETH-20240329-3000-C") {
            Ok(s) => s,
            Err(e) => panic!("ETH fixture parse failed: {e:?}"),
        };
        let mixed = vec![
            ExecutionCapture {
                ord: 0,
                record: exec_record("BTC", &btc, "e-btc"),
            },
            ExecutionCapture {
                ord: 1,
                record: exec_record("ETH", &eth, "e-eth"),
            },
        ];

        // The actor-facing gate refuses the mixed cut wholesale (all-or-nothing),
        // before any mutation.
        let store = InMemoryExecutionsStore::new();
        match store.validate_restore("BTC", &mixed) {
            Err(SnapshotError::RebuildFailed(_)) => {}
            other => panic!("expected a foreign-leg rejection, got {other:?}"),
        }
        assert!(
            store.is_empty(),
            "validate_restore must not mutate the store"
        );

        // A direct restore is fail-closed: the foreign ETH leg is skipped, only the
        // BTC leg lands.
        store.restore_for("BTC", mixed);
        assert_eq!(store.len(), 1, "the foreign ETH leg must not be inserted");
        let account = AccountId::new("acct-1");
        let eth_execs = store
            .list(
                &account,
                &ExecutionFilter {
                    underlying: Some("ETH".to_string()),
                    limit: None,
                },
            )
            .expect("list ETH");
        assert!(
            eth_execs.is_empty(),
            "no ETH leg may be injected by a BTC restore"
        );
        let btc_execs = store
            .list(
                &account,
                &ExecutionFilter {
                    underlying: Some("BTC".to_string()),
                    limit: None,
                },
            )
            .expect("list BTC");
        assert_eq!(btc_execs.len(), 1, "the BTC leg of the cut still lands");
    }

    #[test]
    fn test_positions_restore_for_rejects_foreign_underlying_folds() {
        // The same partition guarantee for the positions fold: a BTC cut carrying an
        // ETH fold must never inject or overwrite ETH's position.
        let btc = sym();
        let eth = match Symbol::parse("ETH-20240329-3000-C") {
            Ok(s) => s,
            Err(e) => panic!("ETH fixture parse failed: {e:?}"),
        };
        let account = AccountId::new("acct-1");
        let btc_cap = PositionCapture {
            account: account.clone(),
            symbol: btc.clone(),
            underlying: "BTC".to_string(),
            net_quantity: 2,
            basis: 100_000,
            cash_ex_fee: -100_000,
            fees: 0,
        };
        let eth_cap = PositionCapture {
            account: account.clone(),
            symbol: eth.clone(),
            underlying: "ETH".to_string(),
            net_quantity: 5,
            basis: 15_000,
            cash_ex_fee: -15_000,
            fees: 0,
        };
        let mixed = vec![btc_cap, eth_cap];

        let store = InMemoryPositionsStore::new();
        match store.validate_restore("BTC", &mixed) {
            Err(SnapshotError::RebuildFailed(_)) => {}
            other => panic!("expected a foreign-fold rejection, got {other:?}"),
        }

        store.restore_for("BTC", mixed);
        assert!(
            store.get(&account, &eth, None).expect("get ETH").is_none(),
            "no ETH fold may be injected by a BTC restore"
        );
        let btc_pos = store
            .get(&account, &btc, None)
            .expect("get BTC")
            .expect("the BTC fold of the cut still lands");
        assert_eq!(btc_pos.net_quantity, 2);
    }
}
