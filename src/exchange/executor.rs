//! The real [`CommandExecutor`] — the **step 3–4 seam** #006 left open: route a
//! sequenced [`VenueCommand`] onto the upstream `option-chain-orderbook` matching
//! **unchanged** and capture the lossless [`VenueOutcome`]
//! ([007](../../../milestones/v0.1-backend-core/007-order-path-onto-matching.md),
//! [02 §4–§5](../../../docs/02-matching-architecture.md),
//! [ADR-0009](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
//!
//! ## What it wraps (never forks)
//!
//! Matching, fills, fees, and self-trade prevention live upstream. The executor
//! owns one per-underlying [`UnderlyingOrderBook`] and, per command:
//!
//! - **vivifies the target leaf** through the hierarchy's idempotent
//!   `get_or_create_*` path — the same pure-function-of-the-symbol resolution the
//!   upstream `SequencedUnderlyingOrderBook` uses, so replay rebuilds the identical
//!   structural state (`sequencer.rs::find_or_create_book_by_symbol`,
//!   0.7.0 registry);
//! - drives the **account-preserving** `_full` leaf
//!   (`OptionOrderBook::add_limit_order_with_tif_and_user_full` → `TradeResult`,
//!   `book.rs:1204`, 0.7.0 registry) for a limit add, and the **true non-resting
//!   market primitive** (`orderbook_rs::OrderBook::submit_market_order_with_user`
//!   reached through `OptionOrderBook::inner()`, `operations.rs:398`,
//!   orderbook-rs 0.10.5) for a market order — never a marketable-limit
//!   substitute; and
//! - captures the two linked legs of every match, the resting remainder, and the
//!   self-trade-prevention removals into the [`VenueOutcome`].
//!
//! ## Lossless capture, including the error path
//!
//! The `_full` leaf returns its own [`TradeResult`] on `Ok`. On an
//! **error-after-fills** path (an unfillable `Ioc` remainder, or a
//! self-trade-prevention cancel after earlier non-self fills) the typed `Err` is
//! returned and the executed fills reach **only** the installed trade listener
//! ([`OptionOrderBook::add_limit_order_with_tif_and_user_full`] docs, 0.7.0). The
//! executor arms that listener ([`OptionOrderBook::arm_trade_capture`]) and, on
//! `Err`, recovers the fills by a **before/after diff** of the book's single
//! last-write-wins capture slot (`last_trade_result()`, keyed on the strictly
//! monotonic `engine_seq`). The diff is correct **only because the actor is the
//! single writer** — no concurrent submit can overwrite the slot between the
//! before-read and the after-read (upstream Option-Chain-OrderBook#148:
//! last-write-wins, no `take`/`clear`). So a command that executed fills is
//! **never** captured as a bare `Rejected`.
//!
//! ## Determinism ([02 §5](../../../docs/02-matching-architecture.md))
//!
//! Execution consults no wall-clock, no RNG, and no map-iteration order:
//!
//! - the engine order id is **assigned deterministically** as
//!   `OrderId::sequential(underlying_sequence)` so the engine never RNG-mints a
//!   `Uuid` on the sequenced path, and it is unique within the underlying
//!   (sequences are per-underlying monotonic);
//! - the resting **maker** leg's venue identity (its venue order id, account, and
//!   STP owner) is recovered from a registry folded deterministically from the
//!   **journaled** add commands, never from live book state
//!   ([ADR-0009 §2](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md));
//! - the STP-cancellation and fill capture iterate ordered structures and the
//!   emitted `stp_cancelled` list is **sorted** by venue order id; and
//! - the engine's process-local trade ids and wall-clock trade timestamps are
//!   **excluded from the oracle** and never surfaced or journaled
//!   ([02 §5.5b](../../../docs/02-matching-architecture.md)).
//!
//! Expiries are `ExpirationDate::DateTime` only — enforced upstream of this seam
//! by [`crate::exchange::Symbol`] / [`crate::exchange::validate_venue_expiry`], so
//! every book this executor vivifies is keyed on a canonical absolute instant.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use option_chain_orderbook::{
    FeeSchedule, OptionOrderBook, SymbolParser, TradeResult, UnderlyingOrderBook,
};
use tokio::task::JoinHandle;

use crate::error::REDACTED_INTERNAL_MESSAGE;
use crate::exchange::actor::{
    ActorConfig, ActorHandle, CommandExecutor, ExecutionContext, FanOut, VenueClock,
    spawn_underlying_actor,
};
use crate::exchange::boundary::{Hash32, OptionStyle, OrderId, STPMode, Side, TimeInForce};
use crate::exchange::envelope::{
    AddOutcome, CancelReason, CancelledLeg, Fill, VenueCommand, VenueOutcome,
};
use crate::exchange::event::SequenceNumber;
use crate::exchange::journal::VenueJournal;
use crate::exchange::money::{Cents, MoneyError, SignedCents};
use crate::exchange::symbol::Symbol;
use crate::models::{AccountId, LiquidityFlag, OrderType, VenueOrderId};

// ============================================================================
// Top-of-book projection (the determinism oracle's read surface)
// ============================================================================

/// The per-contract top-of-book projection — the artifact the sequenced-path
/// determinism test asserts equal across a replay
/// ([02 §5](../../../docs/02-matching-architecture.md)). Prices are integer
/// [`Cents`]; sizes are aggregate resting depth in contracts. Derived mark prices
/// are **not** part of it (they are recomputed, never journaled).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TopOfBook {
    /// Best bid price in cents, if the bid side has depth.
    pub best_bid: Option<Cents>,
    /// Best ask price in cents, if the ask side has depth.
    pub best_ask: Option<Cents>,
    /// Total resting bid depth, in contracts.
    pub bid_depth: u64,
    /// Total resting ask depth, in contracts.
    pub ask_depth: u64,
}

// ============================================================================
// Resting-order registry entry
// ============================================================================

/// A resting order's venue identity, keyed by its engine [`OrderId`] so a match's
/// maker leg is attributed from the **journaled** add command, not live book
/// state ([ADR-0009 §2](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
#[derive(Debug, Clone)]
struct RestingRecord {
    venue_order_id: VenueOrderId,
    account: AccountId,
    owner: Hash32,
}

/// The captured legs of one engine match result, plus the taker's total filled
/// quantity and the resting makers the match fully consumed (to prune).
struct CapturedFills {
    fills: Vec<Fill>,
    taker_filled: u64,
    filled_maker_ids: Vec<OrderId>,
}

/// The outcome of the add half of a placement, before it is classified into a
/// top-level [`VenueOutcome::Added`] / [`VenueOutcome::Rejected`] or a replace
/// leg's [`AddOutcome`].
struct AddResult {
    fills: Vec<Fill>,
    resting_quantity: u64,
    stp_cancelled: Vec<CancelledLeg>,
    reason: String,
}

// ============================================================================
// The executor
// ============================================================================

/// The real [`CommandExecutor`]: drives one underlying's upstream hierarchy and
/// captures the lossless [`VenueOutcome`] (see the module docs).
///
/// It is the **sole** writer to its [`UnderlyingOrderBook`] (the actor guarantees
/// single-writer discipline), so it holds no lock and its before/after capture
/// diffs are race-free. The registry maps are point-lookup only; they are never
/// iterated for order-affecting logic.
pub struct MatchingExecutor {
    /// The upstream per-underlying hierarchy root this executor drives.
    underlying_book: UnderlyingOrderBook,
    /// Engine order id → resting order identity (maker-leg recovery + pruning).
    resting: HashMap<OrderId, RestingRecord>,
    /// Venue order id → engine order id (cancel / replace resolution).
    venue_to_engine: HashMap<VenueOrderId, OrderId>,
}

impl MatchingExecutor {
    /// Builds an executor for one underlying, with an empty hierarchy that
    /// vivifies leaf books lazily on first use.
    #[must_use]
    pub fn new(underlying: impl Into<String>) -> Self {
        Self {
            underlying_book: UnderlyingOrderBook::new(underlying),
            resting: HashMap::new(),
            venue_to_engine: HashMap::new(),
        }
    }

    /// The underlying ticker this executor serves.
    #[must_use]
    #[inline]
    pub fn underlying(&self) -> &str {
        self.underlying_book.underlying()
    }

    /// The current top-of-book for `symbol`, or an empty book when the contract
    /// has not been vivified. Uses **non-creating** lookups so a read never
    /// mutates hierarchy structure.
    #[must_use]
    pub fn top_of_book(&self, symbol: &Symbol) -> TopOfBook {
        match self.resolve_leaf_read(symbol) {
            Some(leaf) => TopOfBook {
                best_bid: leaf
                    .best_bid()
                    .and_then(|price| u64::try_from(price).ok())
                    .map(Cents::new),
                best_ask: leaf
                    .best_ask()
                    .and_then(|price| u64::try_from(price).ok())
                    .map(Cents::new),
                bid_depth: leaf.total_bid_depth(),
                ask_depth: leaf.total_ask_depth(),
            },
            None => TopOfBook::default(),
        }
    }

    // ---- leaf resolution -------------------------------------------------

    /// Resolves — vivifying if needed — the leaf book for `symbol`, mirroring the
    /// upstream `find_or_create_book_by_symbol` (idempotent `get_or_create_*`).
    fn resolve_leaf_vivify(&self, symbol: &Symbol) -> Result<Arc<OptionOrderBook>, String> {
        let parsed = SymbolParser::parse(symbol.as_str()).map_err(|e| e.to_string())?;
        let expected = self.underlying_book.underlying();
        if parsed.underlying() != expected {
            return Err(format!(
                "cross-underlying symbol '{symbol}' does not match underlying '{expected}'"
            ));
        }
        let expiration = self
            .underlying_book
            .get_or_create_expiration(*parsed.expiration());
        let strike = expiration.get_or_create_strike(parsed.strike());
        Ok(match parsed.option_style() {
            OptionStyle::Call => strike.call_arc(),
            OptionStyle::Put => strike.put_arc(),
        })
    }

    /// Resolves the leaf book for `symbol` **without** creating it — `None` when
    /// the contract does not exist or the underlying does not match.
    fn resolve_leaf_read(&self, symbol: &Symbol) -> Option<Arc<OptionOrderBook>> {
        let parsed = SymbolParser::parse(symbol.as_str()).ok()?;
        if parsed.underlying() != self.underlying_book.underlying() {
            return None;
        }
        let expiration = self
            .underlying_book
            .get_expiration(parsed.expiration())
            .ok()?;
        let strike = expiration.get_strike(parsed.strike()).ok()?;
        Some(match parsed.option_style() {
            OptionStyle::Call => strike.call_arc(),
            OptionStyle::Put => strike.put_arc(),
        })
    }

    // ---- registry --------------------------------------------------------

    /// Registers a newly-resting order so a future match can recover its maker
    /// identity and a cancel/replace can find its engine id.
    fn register_resting(
        &mut self,
        engine_id: OrderId,
        venue_order_id: VenueOrderId,
        account: AccountId,
        owner: Hash32,
    ) {
        self.venue_to_engine
            .insert(venue_order_id.clone(), engine_id);
        self.resting.insert(
            engine_id,
            RestingRecord {
                venue_order_id,
                account,
                owner,
            },
        );
    }

    /// Removes an order from the registry (it was filled or cancelled), keeping
    /// both maps in lockstep.
    fn remove_resting(&mut self, engine_id: OrderId) {
        if let Some(record) = self.resting.remove(&engine_id) {
            self.venue_to_engine.remove(&record.venue_order_id);
        }
    }

    // ---- command handlers -----------------------------------------------

    /// Routes an `AddOrder` onto the limit or market leaf path.
    #[allow(clippy::too_many_arguments)]
    fn execute_add_order(
        &mut self,
        ctx: &ExecutionContext<'_>,
        symbol: &Symbol,
        venue_order_id: &VenueOrderId,
        account: &AccountId,
        owner: Hash32,
        side: Side,
        order_type: OrderType,
        limit_price: Option<Cents>,
        quantity: u64,
        time_in_force: TimeInForce,
    ) -> VenueOutcome {
        let leaf = match self.resolve_leaf_vivify(symbol) {
            Ok(leaf) => leaf,
            Err(reason) => return VenueOutcome::Rejected { reason },
        };

        match order_type {
            OrderType::Market => self.execute_market(
                ctx,
                leaf.as_ref(),
                venue_order_id,
                account,
                owner,
                side,
                quantity,
            ),
            OrderType::Limit => {
                let price = match limit_price {
                    Some(price) => price,
                    None => {
                        return VenueOutcome::Rejected {
                            reason: "limit order requires a limit price".to_string(),
                        };
                    }
                };
                let add = self.run_add(
                    ctx,
                    leaf.as_ref(),
                    venue_order_id,
                    account,
                    owner,
                    side,
                    price,
                    quantity,
                    time_in_force,
                );
                if add_has_mutation(&add) {
                    VenueOutcome::Added {
                        fills: add.fills,
                        resting_quantity: add.resting_quantity,
                        stp_cancelled: add.stp_cancelled,
                    }
                } else {
                    VenueOutcome::Rejected {
                        reason: reject_reason(add.reason),
                    }
                }
            }
        }
    }

    /// Drives the account-preserving `_full` limit leaf and captures its fills,
    /// resting remainder, and STP removals — on both the `Ok` and the
    /// error-after-fills `Err` path.
    #[allow(clippy::too_many_arguments)]
    fn run_add(
        &mut self,
        ctx: &ExecutionContext<'_>,
        leaf: &OptionOrderBook,
        venue_order_id: &VenueOrderId,
        account: &AccountId,
        owner: Hash32,
        side: Side,
        limit_price: Cents,
        quantity: u64,
        time_in_force: TimeInForce,
    ) -> AddResult {
        leaf.arm_trade_capture(true);
        let engine_id = engine_order_id(ctx.sequence);
        let stp_active = leaf.stp_mode() != STPMode::None;
        let owner_before = if stp_active {
            owner_resting_ids(leaf, owner)
        } else {
            HashSet::new()
        };
        let before_seq = last_trade_seq(leaf);
        let fee_schedule = leaf.fee_schedule();

        let result = leaf.add_limit_order_with_tif_and_user_full(
            engine_id,
            side,
            limit_price.as_u128(),
            quantity,
            time_in_force,
            owner,
        );
        let succeeded = result.is_ok();

        let captured = match &result {
            Ok(trade_result) => self.build_fills(
                ctx,
                trade_result,
                venue_order_id,
                account,
                owner,
                fee_schedule.as_ref(),
            ),
            // Error-after-fills: the executed fills reached only the armed trade
            // listener, recovered here by the single-writer-safe slot diff.
            Err(_) => match slot_trade_after(leaf, before_seq) {
                Some(trade_result) => self.build_fills(
                    ctx,
                    &trade_result,
                    venue_order_id,
                    account,
                    owner,
                    fee_schedule.as_ref(),
                ),
                None => Ok(CapturedFills {
                    fills: Vec::new(),
                    taker_filled: 0,
                    filled_maker_ids: Vec::new(),
                }),
            },
        };
        let reason = result.err().map(|e| e.to_string()).unwrap_or_default();

        let CapturedFills {
            fills,
            taker_filled,
            filled_maker_ids,
        } = match captured {
            Ok(captured) => captured,
            // Unreachable for bounded venue prices/quantities (every price is a
            // `u64` `Cents` the engine echoes back in range): fail safe rather
            // than fabricate a saturated price/fee.
            Err(error) => {
                tracing::error!(
                    underlying = ctx.underlying,
                    error = %error,
                    "fill capture arithmetic overflow; failing the command safe"
                );
                return AddResult {
                    fills: Vec::new(),
                    resting_quantity: 0,
                    stp_cancelled: Vec::new(),
                    reason: REDACTED_INTERNAL_MESSAGE.to_string(),
                };
            }
        };

        for id in &filled_maker_ids {
            self.remove_resting(*id);
        }

        let stp_cancelled = if stp_active {
            let owner_after = owner_resting_ids(leaf, owner);
            self.stp_cancelled_diff(&owner_before, &owner_after, &filled_maker_ids, engine_id)
        } else {
            Vec::new()
        };

        // `Ioc` / `Fok` never rest a remainder; an `Err` never rests either.
        // `taker_filled <= quantity` by construction (a taker cannot fill more
        // than it submitted); the defensive floor is on a contract count, not
        // money / a sequence / an id — checked_sub keeps the crate free of
        // saturating_* per governance O-4.
        let rests = succeeded && tif_rests(time_in_force);
        // governance O-4 forbids `saturating_sub`; this checked equivalent is
        // the same value (`taker_filled <= quantity` invariant), so the clippy
        // manual-saturating suggestion is intentionally overridden.
        #[allow(clippy::manual_saturating_arithmetic)]
        let resting_quantity = if rests {
            quantity.checked_sub(taker_filled).unwrap_or(0)
        } else {
            0
        };
        if resting_quantity > 0 {
            self.register_resting(engine_id, venue_order_id.clone(), account.clone(), owner);
        }

        AddResult {
            fills,
            resting_quantity,
            stp_cancelled,
            reason,
        }
    }

    /// Drives the upstream true non-resting market primitive and captures its
    /// fills — empty-book (zero fill) and thin-book (partial) alike. Never rests
    /// and never invents a price.
    #[allow(clippy::too_many_arguments)]
    fn execute_market(
        &mut self,
        ctx: &ExecutionContext<'_>,
        leaf: &OptionOrderBook,
        venue_order_id: &VenueOrderId,
        account: &AccountId,
        owner: Hash32,
        side: Side,
        quantity: u64,
    ) -> VenueOutcome {
        if !leaf.status().is_accepting_orders() {
            return VenueOutcome::Rejected {
                reason: "instrument is not accepting orders".to_string(),
            };
        }
        leaf.arm_trade_capture(true);

        // Empty-book fast path (single-writer safe): no opposite-side depth means
        // the true market primitive fills nothing — zero fills, fully unfilled.
        let opposite_depth = match side {
            Side::Buy => leaf.total_ask_depth(),
            Side::Sell => leaf.total_bid_depth(),
        };
        if opposite_depth == 0 {
            return VenueOutcome::Market {
                fills: Vec::new(),
                unfilled_quantity: quantity,
                stp_cancelled: Vec::new(),
            };
        }

        let engine_id = engine_order_id(ctx.sequence);
        let stp_active = leaf.stp_mode() != STPMode::None;
        let owner_before = if stp_active {
            owner_resting_ids(leaf, owner)
        } else {
            HashSet::new()
        };
        let before_seq = last_trade_seq(leaf);
        let fee_schedule = leaf.fee_schedule();

        let outcome = leaf
            .inner()
            .submit_market_order_with_user(engine_id, quantity, side, owner);
        let captured = match outcome {
            Ok(match_result) => {
                let trade_result = TradeResult::new(leaf.symbol().to_string(), match_result);
                self.build_fills(
                    ctx,
                    &trade_result,
                    venue_order_id,
                    account,
                    owner,
                    fee_schedule.as_ref(),
                )
            }
            // STP cancelled the taker (with or without earlier fills): the fills
            // reached only the armed listener; recover them by the slot diff.
            Err(_) => match slot_trade_after(leaf, before_seq) {
                Some(trade_result) => self.build_fills(
                    ctx,
                    &trade_result,
                    venue_order_id,
                    account,
                    owner,
                    fee_schedule.as_ref(),
                ),
                None => Ok(CapturedFills {
                    fills: Vec::new(),
                    taker_filled: 0,
                    filled_maker_ids: Vec::new(),
                }),
            },
        };

        let CapturedFills {
            fills,
            taker_filled,
            filled_maker_ids,
        } = match captured {
            Ok(captured) => captured,
            Err(error) => {
                tracing::error!(
                    underlying = ctx.underlying,
                    error = %error,
                    "market fill capture arithmetic overflow; failing the command safe"
                );
                return VenueOutcome::Market {
                    fills: Vec::new(),
                    unfilled_quantity: quantity,
                    stp_cancelled: Vec::new(),
                };
            }
        };

        for id in &filled_maker_ids {
            self.remove_resting(*id);
        }
        let stp_cancelled = if stp_active {
            let owner_after = owner_resting_ids(leaf, owner);
            self.stp_cancelled_diff(&owner_before, &owner_after, &filled_maker_ids, engine_id)
        } else {
            Vec::new()
        };

        // governance O-4 forbids `saturating_sub`; this checked equivalent is
        // the same value (`taker_filled <= quantity` invariant), so the clippy
        // manual-saturating suggestion is intentionally overridden.
        #[allow(clippy::manual_saturating_arithmetic)]
        let unfilled_quantity = quantity.checked_sub(taker_filled).unwrap_or(0);
        VenueOutcome::Market {
            fills,
            unfilled_quantity,
            stp_cancelled,
        }
    }

    /// Cancels a resting order by its venue order id.
    fn execute_cancel(&mut self, symbol: &Symbol, venue_order_id: &VenueOrderId) -> VenueOutcome {
        let leaf = match self.resolve_leaf_read(symbol) {
            Some(leaf) => leaf,
            None => {
                return VenueOutcome::Rejected {
                    reason: "order not found".to_string(),
                };
            }
        };
        match self.venue_to_engine.get(venue_order_id).copied() {
            Some(engine_id) => match leaf.cancel_order(engine_id) {
                Ok(true) => {
                    self.remove_resting(engine_id);
                    VenueOutcome::Cancelled {
                        order_id: venue_order_id.clone(),
                    }
                }
                Ok(false) => VenueOutcome::Rejected {
                    reason: "order is not resting".to_string(),
                },
                Err(error) => VenueOutcome::Rejected {
                    reason: error.to_string(),
                },
            },
            None => VenueOutcome::Rejected {
                reason: "order not found".to_string(),
            },
        }
    }

    /// Executes a **non-atomic** replace as cancel-then-add in one turn, recorded
    /// as one [`VenueOutcome::Replace`] at one sequence — no rollback if the add
    /// leg is rejected after the cancel succeeded.
    #[allow(clippy::too_many_arguments)]
    fn execute_replace(
        &mut self,
        ctx: &ExecutionContext<'_>,
        symbol: &Symbol,
        order_id: &VenueOrderId,
        new_order_id: &VenueOrderId,
        account: &AccountId,
        side: Side,
        limit_price: Option<Cents>,
        quantity: u64,
        time_in_force: TimeInForce,
    ) -> VenueOutcome {
        let leaf = match self.resolve_leaf_vivify(symbol) {
            Ok(leaf) => leaf,
            Err(reason) => return VenueOutcome::Rejected { reason },
        };

        // Cancel leg. The replacement inherits the cancelled order's STP owner
        // (the `Replace` command carries no owner of its own).
        let (cancelled, owner) = match self.venue_to_engine.get(order_id).copied() {
            Some(engine_id) => {
                let owner = self
                    .resting
                    .get(&engine_id)
                    .map(|record| record.owner)
                    .unwrap_or(Hash32([0u8; 32]));
                let cancelled = matches!(leaf.cancel_order(engine_id), Ok(true));
                if cancelled {
                    self.remove_resting(engine_id);
                }
                (cancelled, owner)
            }
            None => (false, Hash32([0u8; 32])),
        };

        // Add leg (never rolled back if it is rejected).
        let add = match limit_price {
            Some(price) => {
                let result = self.run_add(
                    ctx,
                    leaf.as_ref(),
                    new_order_id,
                    account,
                    owner,
                    side,
                    price,
                    quantity,
                    time_in_force,
                );
                classify_add_leg(result)
            }
            None => AddOutcome::Rejected {
                reason: "replacement add requires a limit price".to_string(),
            },
        };

        VenueOutcome::Replace { cancelled, add }
    }

    // ---- fill + STP capture ---------------------------------------------

    /// Builds the two linked legs (maker + taker, shared `execution_id`, per-leg
    /// account / owner / fee) for every match in `trade_result`, plus the taker's
    /// total filled quantity and the makers the match fully consumed.
    fn build_fills(
        &self,
        ctx: &ExecutionContext<'_>,
        trade_result: &TradeResult,
        taker_order_id: &VenueOrderId,
        taker_account: &AccountId,
        taker_owner: Hash32,
        fee_schedule: Option<&FeeSchedule>,
    ) -> Result<CapturedFills, MoneyError> {
        let match_result = &trade_result.match_result;
        let trades = match_result.trades().as_vec();
        // Two legs per trade; checked_mul keeps the crate free of saturating_*
        // per governance O-4 (the capacity hint falls back to a large bound),
        // so the clippy manual-saturating suggestion is intentionally overridden.
        #[allow(clippy::manual_saturating_arithmetic)]
        let capacity = trades.len().checked_mul(2).unwrap_or(usize::MAX);
        let mut fills = Vec::with_capacity(capacity);
        let mut taker_filled: u64 = 0;

        for (index, trade) in trades.iter().enumerate() {
            let fill_index = u32::try_from(index).map_err(|_| MoneyError::Overflow)?;
            let execution_id =
                ctx.lineage_id
                    .execution_id(ctx.underlying, ctx.sequence, fill_index);

            let price_ticks = trade.price().as_u128();
            let price = Cents::new(u64::try_from(price_ticks).map_err(|_| MoneyError::Overflow)?);
            let quantity = trade.quantity().as_u64();
            taker_filled = taker_filled
                .checked_add(quantity)
                .ok_or(MoneyError::Overflow)?;

            let (maker_order_id, maker_account, maker_owner) =
                match self.resting.get(&trade.maker_order_id()) {
                    Some(record) => (
                        record.venue_order_id.clone(),
                        record.account.clone(),
                        record.owner,
                    ),
                    // Invariant: a maker was rested by a prior journaled add and is
                    // registered. This defensive arm never fires under the single
                    // writer; it keeps the leg attributable rather than lost.
                    None => {
                        tracing::error!(
                            underlying = ctx.underlying,
                            maker_engine_id = %trade.maker_order_id(),
                            "resting maker missing from the venue registry"
                        );
                        (
                            VenueOrderId::new(trade.maker_order_id().to_string()),
                            AccountId::new("unknown"),
                            Hash32([0u8; 32]),
                        )
                    }
                };

            let maker_fee = per_leg_fee(fee_schedule, price_ticks, quantity, true)?;
            let taker_fee = per_leg_fee(fee_schedule, price_ticks, quantity, false)?;

            // Maker leg then taker leg — a deterministic per-match order.
            fills.push(Fill {
                execution_id: execution_id.clone(),
                order_id: maker_order_id,
                account: maker_account,
                owner: maker_owner,
                side: trade.maker_side(),
                liquidity: LiquidityFlag::Maker,
                price,
                quantity,
                fee: maker_fee,
            });
            fills.push(Fill {
                execution_id,
                order_id: taker_order_id.clone(),
                account: taker_account.clone(),
                owner: taker_owner,
                side: trade.taker_side(),
                liquidity: LiquidityFlag::Taker,
                price,
                quantity,
                fee: taker_fee,
            });
        }

        Ok(CapturedFills {
            fills,
            taker_filled,
            filled_maker_ids: match_result.filled_order_ids().to_vec(),
        })
    }

    /// Records the same-owner resting makers self-trade prevention removed in this
    /// turn — the ones present before the add, gone after, and not consumed by a
    /// fill — as [`CancelReason::SelfTradePrevention`] legs, sorted by venue order
    /// id for a deterministic sweep order, then prunes them from the registry.
    fn stp_cancelled_diff(
        &mut self,
        before: &HashSet<OrderId>,
        after: &HashSet<OrderId>,
        filled: &[OrderId],
        incoming: OrderId,
    ) -> Vec<CancelledLeg> {
        let removed: Vec<OrderId> = before
            .iter()
            .copied()
            .filter(|id| *id != incoming && !after.contains(id) && !filled.contains(id))
            .collect();

        let mut legs: Vec<CancelledLeg> = removed
            .iter()
            .filter_map(|id| {
                self.resting.get(id).map(|record| CancelledLeg {
                    order_id: record.venue_order_id.clone(),
                    owner: record.owner,
                    reason: CancelReason::SelfTradePrevention,
                })
            })
            .collect();
        legs.sort_by(|a, b| a.order_id.as_str().cmp(b.order_id.as_str()));

        for id in removed {
            self.remove_resting(id);
        }
        legs
    }
}

impl CommandExecutor for MatchingExecutor {
    fn execute(&mut self, context: ExecutionContext<'_>) -> VenueOutcome {
        match context.command {
            VenueCommand::AddOrder {
                symbol,
                order_id,
                account,
                owner,
                side,
                order_type,
                limit_price,
                quantity,
                time_in_force,
                ..
            } => self.execute_add_order(
                &context,
                symbol,
                order_id,
                account,
                *owner,
                *side,
                *order_type,
                *limit_price,
                *quantity,
                *time_in_force,
            ),
            VenueCommand::CancelOrder {
                symbol, order_id, ..
            } => self.execute_cancel(symbol, order_id),
            VenueCommand::Replace {
                symbol,
                order_id,
                new_order_id,
                account,
                side,
                limit_price,
                quantity,
                time_in_force,
                ..
            } => self.execute_replace(
                &context,
                symbol,
                order_id,
                new_order_id,
                account,
                *side,
                *limit_price,
                *quantity,
                *time_in_force,
            ),
            // Control-plane commands have no leaf effect on this path; their
            // derived effects are journaled as their own sequenced commands.
            VenueCommand::MarketMakerControl { .. }
            | VenueCommand::Clock { .. }
            | VenueCommand::SimStep { .. } => VenueOutcome::ControlApplied,
            // Hierarchy-sweep / lifecycle commands are routed by later issues; the
            // #007 order path does not execute them.
            VenueCommand::MassCancel { .. }
            | VenueCommand::SetInstrumentStatus { .. }
            | VenueCommand::EvictExpiredOrders { .. } => VenueOutcome::Rejected {
                reason: "command not routed by the order path".to_string(),
            },
        }
    }
}

// ============================================================================
// Free helpers
// ============================================================================

/// The deterministic engine order id for a command: a sequential id keyed on the
/// per-underlying sequence, so the engine never RNG-mints a `Uuid` and the id is
/// unique within the underlying.
#[inline]
fn engine_order_id(sequence: SequenceNumber) -> OrderId {
    OrderId::sequential(sequence.get())
}

/// Whether a time-in-force rests its unfilled remainder (`Gtc` / `Gtd` / `Day`)
/// rather than cancelling it (`Ioc` / `Fok`).
#[inline]
fn tif_rests(time_in_force: TimeInForce) -> bool {
    matches!(
        time_in_force,
        TimeInForce::Gtc | TimeInForce::Gtd(_) | TimeInForce::Day
    )
}

/// Whether the add leg mutated the book (filled, rested, or removed an STP maker)
/// — the discriminator between an `Added` outcome and a genuine `Rejected` no-op.
#[inline]
fn add_has_mutation(add: &AddResult) -> bool {
    !add.fills.is_empty() || add.resting_quantity > 0 || !add.stp_cancelled.is_empty()
}

/// A non-empty reject reason, defaulting when the leaf produced none (a `Fok`
/// kill, or an `Ioc` that could not fill).
#[inline]
fn reject_reason(reason: String) -> String {
    if reason.is_empty() {
        "order was not fillable and did not rest".to_string()
    } else {
        reason
    }
}

/// Classifies a completed add into a replace leg's [`AddOutcome`].
fn classify_add_leg(add: AddResult) -> AddOutcome {
    if !add_has_mutation(&add) {
        return AddOutcome::Rejected {
            reason: reject_reason(add.reason),
        };
    }
    if add.resting_quantity > 0 {
        AddOutcome::Rested {
            fills: add.fills,
            resting_quantity: add.resting_quantity,
            stp_cancelled: add.stp_cancelled,
        }
    } else {
        AddOutcome::Filled {
            fills: add.fills,
            stp_cancelled: add.stp_cancelled,
        }
    }
}

/// The set of a user's currently-resting order ids on a leaf (the STP diff basis).
fn owner_resting_ids(leaf: &OptionOrderBook, owner: Hash32) -> HashSet<OrderId> {
    leaf.orders_by_user(owner)
        .into_iter()
        .map(|(id, _status)| id)
        .collect()
}

/// The engine sequence of the leaf's last captured trade, or `None` if the
/// capture slot is empty — the before-image for the error-path fill diff.
fn last_trade_seq(leaf: &OptionOrderBook) -> Option<u64> {
    leaf.last_trade_result().map(|trade| trade.engine_seq)
}

/// The trade captured in the leaf's slot **since** `before_seq`, or `None` if the
/// slot did not advance (no new fills). Correct only under the single writer.
fn slot_trade_after(leaf: &OptionOrderBook, before_seq: Option<u64>) -> Option<TradeResult> {
    let trade = leaf.last_trade_result()?;
    match before_seq {
        None => Some(trade),
        Some(before) if trade.engine_seq > before => Some(trade),
        _ => None,
    }
}

/// The per-leg fee for a transaction, computed from the leaf's configured
/// [`FeeSchedule`] (zero when none is configured). A maker rebate is negative.
///
/// The upstream `calculate_fee` saturates its own internal `notional × bps`
/// product; the `i128 → i64` narrowing here is **checked**, so an adversarial
/// fee that does not fit `SignedCents` is a typed [`MoneyError::Overflow`] rather
/// than a silent saturation. Unreachable for realistic option fees.
fn per_leg_fee(
    schedule: Option<&FeeSchedule>,
    price_ticks: u128,
    quantity: u64,
    is_maker: bool,
) -> Result<SignedCents, MoneyError> {
    match schedule {
        None => Ok(SignedCents::new(0)),
        Some(schedule) => {
            let notional = price_ticks
                .checked_mul(u128::from(quantity))
                .ok_or(MoneyError::Overflow)?;
            let fee = schedule.calculate_fee(notional, is_maker);
            let fee = i64::try_from(fee).map_err(|_| MoneyError::Overflow)?;
            Ok(SignedCents::new(fee))
        }
    }
}

// ============================================================================
// Ergonomic spawn: the real executor wired into the actor
// ============================================================================

/// Spawns a per-underlying single-writer actor whose executor is the real
/// [`MatchingExecutor`] — the default order-path wiring (the #006
/// [`crate::exchange::PlaceholderExecutor`] stays for tests). Returns the bounded
/// [`ActorHandle`] plus the task's [`JoinHandle`] for graceful shutdown.
#[must_use]
pub fn spawn_matching_actor<J, F, C>(
    config: ActorConfig,
    journal: J,
    fan_out: F,
    clock: C,
) -> (ActorHandle, JoinHandle<()>)
where
    J: VenueJournal + Send + 'static,
    F: FanOut + Send + 'static,
    C: VenueClock + Send + 'static,
{
    let executor = MatchingExecutor::new(config.underlying.as_ref());
    spawn_underlying_actor(config, journal, executor, fan_out, clock)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::identity::LineageId;
    use crate::models::ClientOrderId;

    const UNDERLYING: &str = "BTC";
    const TS: crate::exchange::event::EventTimestamp =
        crate::exchange::event::EventTimestamp::new(1_700_000_000_000);

    fn lineage() -> LineageId {
        LineageId::new("run-1")
    }

    fn sym() -> Symbol {
        match Symbol::parse("BTC-20240329-50000-C") {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
        }
    }

    fn owner(byte: u8) -> Hash32 {
        Hash32([byte; 32])
    }

    /// Runs one command through the executor at `sequence`, minting the venue
    /// order id from the id grammar as the actor would.
    fn run(
        executor: &mut MatchingExecutor,
        lineage: &LineageId,
        sequence: u64,
        command: &VenueCommand,
    ) -> VenueOutcome {
        let seq = SequenceNumber::new(sequence);
        executor.execute(ExecutionContext {
            underlying: UNDERLYING,
            lineage_id: lineage,
            sequence: seq,
            venue_ts: TS,
            command,
        })
    }

    /// A limit add whose venue order id is the grammar id for `sequence`.
    #[allow(clippy::too_many_arguments)]
    fn add(
        lineage: &LineageId,
        sequence: u64,
        account: &str,
        owner_byte: u8,
        side: Side,
        price: u64,
        quantity: u64,
        tif: TimeInForce,
    ) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym(),
            order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(sequence), 0),
            account: AccountId::new(account),
            owner: owner(owner_byte),
            client_order_id: Some(ClientOrderId::new(format!("c-{sequence}"))),
            side,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity,
            time_in_force: tif,
            stp_mode: STPMode::None,
        }
    }

    fn market(
        lineage: &LineageId,
        sequence: u64,
        account: &str,
        side: Side,
        quantity: u64,
    ) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym(),
            order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(sequence), 0),
            account: AccountId::new(account),
            owner: owner(0xAA),
            client_order_id: None,
            side,
            order_type: OrderType::Market,
            limit_price: None,
            quantity,
            time_in_force: TimeInForce::Ioc,
            stp_mode: STPMode::None,
        }
    }

    // ---- add happy paths -------------------------------------------------

    #[test]
    fn test_limit_add_rests_when_it_does_not_cross() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        let outcome = run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "maker",
                0x11,
                Side::Sell,
                50_000,
                3,
                TimeInForce::Gtc,
            ),
        );
        match outcome {
            VenueOutcome::Added {
                fills,
                resting_quantity,
                stp_cancelled,
            } => {
                assert!(fills.is_empty());
                assert_eq!(resting_quantity, 3);
                assert!(stp_cancelled.is_empty());
            }
            other => panic!("expected Added, got {other:?}"),
        }
        // Top-of-book reflects the resting ask.
        let top = ex.top_of_book(&sym());
        assert_eq!(top.best_ask, Some(Cents::new(50_000)));
        assert_eq!(top.ask_depth, 3);
        assert_eq!(top.best_bid, None);
    }

    #[test]
    fn test_crossing_add_captures_two_linked_legs() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // Resting sell (maker) at seq 0.
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "maker",
                0x11,
                Side::Sell,
                50_000,
                2,
                TimeInForce::Gtc,
            ),
        );
        // Crossing buy (taker) at seq 1.
        let outcome = run(
            &mut ex,
            &lin,
            1,
            &add(
                &lin,
                1,
                "taker",
                0x22,
                Side::Buy,
                50_000,
                2,
                TimeInForce::Gtc,
            ),
        );
        match outcome {
            VenueOutcome::Added {
                fills,
                resting_quantity,
                stp_cancelled,
            } => {
                assert_eq!(resting_quantity, 0);
                assert!(stp_cancelled.is_empty());
                assert_eq!(fills.len(), 2);
                let maker = &fills[0];
                let taker = &fills[1];
                // Two legs of one match share the execution id...
                assert_eq!(maker.execution_id, taker.execution_id);
                assert_eq!(
                    maker.execution_id,
                    lin.execution_id(UNDERLYING, SequenceNumber::new(1), 0)
                );
                // ...but keep their own account / side / liquidity, and the maker
                // identity is recovered from the journaled add (seq 0).
                assert_eq!(maker.liquidity, LiquidityFlag::Maker);
                assert_eq!(taker.liquidity, LiquidityFlag::Taker);
                assert_eq!(maker.side, Side::Sell);
                assert_eq!(taker.side, Side::Buy);
                assert_eq!(maker.account, AccountId::new("maker"));
                assert_eq!(taker.account, AccountId::new("taker"));
                assert_eq!(maker.owner, owner(0x11));
                assert_eq!(taker.owner, owner(0x22));
                assert_eq!(
                    maker.order_id,
                    lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0)
                );
                assert_eq!(maker.price, Cents::new(50_000));
                assert_eq!(maker.quantity, 2);
                // No fee schedule configured → zero fees.
                assert_eq!(maker.fee, SignedCents::new(0));
                assert_eq!(taker.fee, SignedCents::new(0));
            }
            other => panic!("expected Added, got {other:?}"),
        }
        // The fully-consumed maker was pruned; the book is empty.
        assert_eq!(ex.top_of_book(&sym()), TopOfBook::default());
    }

    #[test]
    fn test_cross_underlying_symbol_is_rejected() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new("ETH");
        let outcome = run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "acct",
                0x11,
                Side::Buy,
                50_000,
                1,
                TimeInForce::Gtc,
            ),
        );
        match outcome {
            VenueOutcome::Rejected { reason } => assert!(reason.contains("cross-underlying")),
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    // ---- cancel ----------------------------------------------------------

    #[test]
    fn test_cancel_removes_a_resting_order() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "acct",
                0x11,
                Side::Sell,
                50_000,
                1,
                TimeInForce::Gtc,
            ),
        );
        let order_id = lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0);
        let outcome = run(
            &mut ex,
            &lin,
            1,
            &VenueCommand::CancelOrder {
                symbol: sym(),
                order_id: order_id.clone(),
                account: AccountId::new("acct"),
            },
        );
        match outcome {
            VenueOutcome::Cancelled {
                order_id: cancelled,
            } => assert_eq!(cancelled, order_id),
            other => panic!("expected Cancelled, got {other:?}"),
        }
        assert_eq!(ex.top_of_book(&sym()), TopOfBook::default());
    }

    #[test]
    fn test_cancel_unknown_order_is_rejected() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // Vivify the book so the leaf resolves, but the id was never added.
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "acct",
                0x11,
                Side::Sell,
                50_000,
                1,
                TimeInForce::Gtc,
            ),
        );
        let outcome = run(
            &mut ex,
            &lin,
            1,
            &VenueCommand::CancelOrder {
                symbol: sym(),
                order_id: VenueOrderId::new("nope"),
                account: AccountId::new("acct"),
            },
        );
        assert!(matches!(outcome, VenueOutcome::Rejected { .. }));
    }

    // ---- market ----------------------------------------------------------

    #[test]
    fn test_market_against_empty_book_is_zero_fill() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        let outcome = run(&mut ex, &lin, 0, &market(&lin, 0, "taker", Side::Buy, 5));
        match outcome {
            VenueOutcome::Market {
                fills,
                unfilled_quantity,
                stp_cancelled,
            } => {
                assert!(fills.is_empty());
                assert_eq!(unfilled_quantity, 5);
                assert!(stp_cancelled.is_empty());
            }
            other => panic!("expected Market, got {other:?}"),
        }
    }

    #[test]
    fn test_market_against_thin_book_partially_fills() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // Only 2 contracts of ask depth.
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "maker",
                0x11,
                Side::Sell,
                50_000,
                2,
                TimeInForce::Gtc,
            ),
        );
        // Buy 5 at market → fills 2, 3 unfilled (never rests).
        let outcome = run(&mut ex, &lin, 1, &market(&lin, 1, "taker", Side::Buy, 5));
        match outcome {
            VenueOutcome::Market {
                fills,
                unfilled_quantity,
                stp_cancelled,
            } => {
                assert_eq!(fills.len(), 2);
                assert_eq!(fills[0].liquidity, LiquidityFlag::Maker);
                assert_eq!(fills[1].liquidity, LiquidityFlag::Taker);
                assert_eq!(fills[1].quantity, 2);
                assert_eq!(unfilled_quantity, 3);
                assert!(stp_cancelled.is_empty());
            }
            other => panic!("expected Market, got {other:?}"),
        }
        // Market never rested a remainder.
        assert_eq!(ex.top_of_book(&sym()), TopOfBook::default());
    }

    #[test]
    fn test_market_full_fill_leaves_nothing_unfilled() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "maker",
                0x11,
                Side::Sell,
                50_000,
                5,
                TimeInForce::Gtc,
            ),
        );
        let outcome = run(&mut ex, &lin, 1, &market(&lin, 1, "taker", Side::Buy, 5));
        match outcome {
            VenueOutcome::Market {
                unfilled_quantity, ..
            } => assert_eq!(unfilled_quantity, 0),
            other => panic!("expected Market, got {other:?}"),
        }
    }

    // ---- error-after-fill diff capture (IOC remainder) -------------------

    #[test]
    fn test_ioc_partial_fill_is_captured_via_error_path_diff() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // Rest a sell of 1 at 50_000.
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "maker",
                0x11,
                Side::Sell,
                50_000,
                1,
                TimeInForce::Gtc,
            ),
        );
        // IOC buy of 3 crosses 1, then the 2-lot remainder is unfillable — the
        // `_full` leaf returns Err and the fill reaches only the armed slot.
        let outcome = run(
            &mut ex,
            &lin,
            1,
            &add(
                &lin,
                1,
                "taker",
                0x22,
                Side::Buy,
                50_000,
                3,
                TimeInForce::Ioc,
            ),
        );
        match outcome {
            VenueOutcome::Added {
                fills,
                resting_quantity,
                ..
            } => {
                // The fill is NOT lost to a bare Rejected — it is diff-captured.
                assert_eq!(fills.len(), 2);
                assert_eq!(fills[1].quantity, 1);
                // IOC never rests its remainder.
                assert_eq!(resting_quantity, 0);
            }
            other => panic!("expected diff-captured Added, got {other:?}"),
        }
    }

    // ---- replace ---------------------------------------------------------

    #[test]
    fn test_replace_cancels_then_adds_in_one_turn() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "acct",
                0x11,
                Side::Sell,
                50_000,
                2,
                TimeInForce::Gtc,
            ),
        );
        let outcome = run(
            &mut ex,
            &lin,
            1,
            &VenueCommand::Replace {
                symbol: sym(),
                order_id: lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0),
                new_order_id: lin.venue_order_id(UNDERLYING, SequenceNumber::new(1), 0),
                account: AccountId::new("acct"),
                side: Side::Sell,
                limit_price: Some(Cents::new(50_100)),
                quantity: 4,
                time_in_force: TimeInForce::Gtc,
                stp_mode: STPMode::None,
            },
        );
        match outcome {
            VenueOutcome::Replace { cancelled, add } => {
                assert!(cancelled);
                match add {
                    AddOutcome::Rested {
                        resting_quantity, ..
                    } => assert_eq!(resting_quantity, 4),
                    other => panic!("expected Rested add leg, got {other:?}"),
                }
            }
            other => panic!("expected Replace, got {other:?}"),
        }
        // The old order is gone; the replacement rests at the new price.
        let top = ex.top_of_book(&sym());
        assert_eq!(top.best_ask, Some(Cents::new(50_100)));
        assert_eq!(top.ask_depth, 4);
    }

    #[test]
    fn test_partial_replace_cancel_succeeds_add_rejected() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "acct",
                0x11,
                Side::Sell,
                50_000,
                2,
                TimeInForce::Gtc,
            ),
        );
        // Replace whose add leg is a FOK that cannot fill in full → killed → the
        // add leg is Rejected while the cancel already succeeded (not rolled back).
        let outcome = run(
            &mut ex,
            &lin,
            1,
            &VenueCommand::Replace {
                symbol: sym(),
                order_id: lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0),
                new_order_id: lin.venue_order_id(UNDERLYING, SequenceNumber::new(1), 0),
                account: AccountId::new("acct"),
                side: Side::Buy,
                limit_price: Some(Cents::new(40_000)),
                quantity: 2,
                time_in_force: TimeInForce::Fok,
                stp_mode: STPMode::None,
            },
        );
        match outcome {
            VenueOutcome::Replace { cancelled, add } => {
                assert!(cancelled, "the cancel leg succeeded");
                assert!(
                    matches!(add, AddOutcome::Rejected { .. }),
                    "the FOK add leg could not fill and was rejected, got {add:?}"
                );
            }
            other => panic!("expected Replace, got {other:?}"),
        }
        // The old order is gone and no new order rested — a defined state.
        assert_eq!(ex.top_of_book(&sym()), TopOfBook::default());
    }

    // ---- STP affected-id recording ---------------------------------------

    #[test]
    fn test_stp_cancel_maker_records_affected_resting_id() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // Configure the hierarchy so vivified leaves inherit CancelMaker STP.
        ex.underlying_book.set_stp_mode(STPMode::CancelMaker);

        // Owner 0x11 rests a sell.
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "self",
                0x11,
                Side::Sell,
                50_000,
                2,
                TimeInForce::Gtc,
            ),
        );
        // The SAME owner crosses with a buy → STP cancels the resting maker.
        let outcome = run(
            &mut ex,
            &lin,
            1,
            &add(
                &lin,
                1,
                "self",
                0x11,
                Side::Buy,
                50_000,
                2,
                TimeInForce::Gtc,
            ),
        );
        match outcome {
            VenueOutcome::Added {
                fills,
                stp_cancelled,
                ..
            } => {
                // No self-fill occurred; the resting same-owner maker was removed.
                assert!(fills.is_empty(), "STP prevented the self-trade");
                assert_eq!(stp_cancelled.len(), 1);
                assert_eq!(stp_cancelled[0].reason, CancelReason::SelfTradePrevention);
                assert_eq!(stp_cancelled[0].owner, owner(0x11));
                assert_eq!(
                    stp_cancelled[0].order_id,
                    lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0)
                );
            }
            other => panic!("expected Added with STP capture, got {other:?}"),
        }
    }

    // ---- fee capture (per-leg, from a configured schedule) ---------------

    #[test]
    fn test_per_leg_fee_from_schedule() {
        // -2 bps maker rebate, 5 bps taker, notional = 1_000 * 10 = 10_000.
        let schedule = FeeSchedule::new(-2, 5);
        let maker = match per_leg_fee(Some(&schedule), 1_000, 10, true) {
            Ok(fee) => fee,
            Err(e) => panic!("maker fee overflow: {e:?}"),
        };
        let taker = match per_leg_fee(Some(&schedule), 1_000, 10, false) {
            Ok(fee) => fee,
            Err(e) => panic!("taker fee overflow: {e:?}"),
        };
        assert_eq!(maker, SignedCents::new(-2));
        assert_eq!(taker, SignedCents::new(5));
        // No schedule → zero fee.
        assert_eq!(
            per_leg_fee(None, 1_000, 10, false).ok(),
            Some(SignedCents::new(0))
        );
    }

    // ---- determinism: same commands → same fills + top-of-book -----------

    #[test]
    fn test_same_command_stream_reconstructs_identical_fills_and_top_of_book() {
        let lin = lineage();
        let commands = [
            add(&lin, 0, "m1", 0x11, Side::Sell, 50_000, 3, TimeInForce::Gtc),
            add(&lin, 1, "m2", 0x12, Side::Sell, 50_100, 2, TimeInForce::Gtc),
            add(&lin, 2, "t1", 0x22, Side::Buy, 50_000, 2, TimeInForce::Gtc),
            add(&lin, 3, "b1", 0x33, Side::Buy, 49_900, 4, TimeInForce::Gtc),
            market(&lin, 4, "t2", Side::Buy, 3),
        ];

        let replay = |lin: &LineageId| -> (Vec<VenueOutcome>, TopOfBook) {
            let mut ex = MatchingExecutor::new(UNDERLYING);
            let outcomes = commands
                .iter()
                .enumerate()
                .map(|(i, command)| run(&mut ex, lin, i as u64, command))
                .collect();
            (outcomes, ex.top_of_book(&sym()))
        };

        let (outcomes_a, top_a) = replay(&lin);
        let (outcomes_b, top_b) = replay(&lin);
        assert_eq!(
            outcomes_a, outcomes_b,
            "same command stream must capture identical outcomes (fills)"
        );
        assert_eq!(
            top_a, top_b,
            "same command stream must reconstruct identical top-of-book"
        );
    }
}
