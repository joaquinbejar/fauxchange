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

use option_chain_orderbook::utils::format_expiration_yyyymmdd;
use option_chain_orderbook::{
    FeeSchedule, InstrumentRegistry, OptionOrderBook, SymbolIndex, SymbolParser, TradeResult,
    UnderlyingOrderBook,
};
use tokio::task::JoinHandle;

use crate::error::REDACTED_INTERNAL_MESSAGE;
use crate::exchange::actor::{
    ActorConfig, ActorHandle, CommandExecutor, ExecutionContext, FanOut, VenueClock,
    spawn_underlying_actor,
};
use crate::exchange::boundary::{
    Hash32, InstrumentStatus, OptionStyle, OrderId, STPMode, Side, TimeInForce, TimestampMs,
};
use crate::exchange::envelope::{
    AddOutcome, CancelReason, CancelledLeg, Fill, MassCancelScope, MassCancelType, RejectKind,
    VenueCommand, VenueOutcome,
};
use crate::exchange::event::{EventTimestamp, SequenceNumber};
use crate::exchange::instrument_status::InstrumentStatusRegistry;
use crate::exchange::journal::VenueJournal;
use crate::exchange::mm_identity::MARKET_MAKER_OWNER;
use crate::exchange::money::{Cents, MoneyError, SignedCents};
use crate::exchange::snapshot::{
    ExecutorState, IdempotencyEntry, IdempotencyFingerprint, IdempotencyKey, IdempotencyMap,
    IdempotencyRecord, InstrumentStatusCapture, RestingOrderCapture, SnapshotError,
};
use crate::exchange::symbol::Symbol;
use crate::microstructure::{MicrostructureConfig, MicrostructureConfigError, apply_to_underlying};
use crate::models::{AccountId, ClientOrderId, LiquidityFlag, OrderType, VenueOrderId};

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
// Market-maker control seam (filled by the persona layer — #47 phase 2)
// ============================================================================

/// The clamped market-maker control knobs a [`VenueCommand::MarketMakerControl`]
/// carries — the payload the sequenced apply seam hands the persona layer. Each
/// field is `None` when the command leaves that knob unchanged; the values are
/// **dimensionless** `f64` multipliers, not money ([01 §3](../../../docs/01-domain-model.md)).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketMakerControlKnobs {
    /// New global spread multiplier, when changing it.
    pub spread_multiplier: Option<f64>,
    /// New global size scalar, when changing it.
    pub size_scalar: Option<f64>,
    /// New global directional skew, when changing it.
    pub directional_skew: Option<f64>,
    /// New master-enabled (kill / enable) state, when changing it.
    pub enabled: Option<bool>,
}

/// The **sequenced apply seam** for a [`VenueCommand::MarketMakerControl`] — the
/// hook the single-writer executor invokes, inside the actor turn, to push a
/// control change onto the market maker so the knob takes effect on the sequenced
/// path ([02 §4.1](../../../docs/02-matching-architecture.md#41-venue-wide-commands-marketmakercontrol--clock--simstep),
/// [03 §10](../../../docs/03-protocol-surfaces.md)).
///
/// **#47 phase 1 defines the seam; the persona layer (#47 phase 2, `simulation-expert`)
/// implements it** over the `MarketMakerEngine` — mapping the knobs onto its
/// `set_spread_multiplier` / `set_size_scalar` / `set_directional_skew` /
/// `set_enabled` setters. The engine's setters are `&self` (interior mutability),
/// so an `Arc<MarketMakerEngine>` can implement this trait and be shared, by
/// handle, into every underlying's executor.
///
/// ## What the seam MUST NOT do (the determinism contract)
///
/// `apply_control` may update the engine's persona knobs (a plain state write) but
/// **must not** re-enter the sequencer or synchronously emit orders: the requotes a
/// control change induces enter the sequencer as their **own** journaled `AddOrder`
/// commands on the next price step, so a replay reproduces them from the journal.
/// The apply is a **live-only side effect** excluded from the captured
/// [`VenueOutcome::ControlApplied`], and the recovery/replay path installs **no**
/// sink (the fresh reconstruction executors carry `None`), so re-execution derives
/// the identical `ControlApplied` event without ever driving a live engine
/// ([02 §5](../../../docs/02-matching-architecture.md#5-determinism)).
pub trait MarketMakerControlSink: Send + Sync {
    /// Applies a market-maker control change on the sequenced path. Invoked once
    /// per committed [`VenueCommand::MarketMakerControl`], inside the actor turn.
    fn apply_control(&self, knobs: MarketMakerControlKnobs);
}

// ============================================================================
// Resting-order registry entry
// ============================================================================

/// A resting order's venue identity, keyed by its engine [`OrderId`] so a match's
/// maker leg is attributed from the **journaled** add command, not live book
/// state ([ADR-0009 §2](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
///
/// The `symbol` resolves the leaf the order rests on for the snapshot cut (#009):
/// a capture reads each order's *current* resting quantity back from that leaf,
/// and a restore clears the leaf before re-adding. The `side` lets a
/// [`MassCancelType::BySide`] sweep filter without a book read.
#[derive(Debug, Clone)]
struct RestingRecord {
    symbol: Symbol,
    venue_order_id: VenueOrderId,
    account: AccountId,
    owner: Hash32,
    side: Side,
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
    /// The per-account client-order-id idempotency map (#009): a matching retry
    /// returns the stored terminal result rather than opening a second order,
    /// and it is captured/restored as the fourth store of a snapshot cut
    /// ([01 §6.1](../../../docs/01-domain-model.md)).
    idempotency: IdempotencyMap,
    /// The venue-owned, **sequenced** per-instrument status registry (#47): a
    /// `SetInstrumentStatus` command transitions it and an `AddOrder` reads it to
    /// gate a non-`Active` instrument, both on this single-writer path, so it is a
    /// deterministic function of the journal.
    instrument_status: InstrumentStatusRegistry,
    /// The **optional** market-maker control apply seam (#47 phase 2): installed
    /// on the live order path so a `MarketMakerControl` command pushes its knobs
    /// onto the market maker, and left `None` on the replay/recovery reconstruction
    /// path (the requotes it induces are journaled as their own `AddOrder`
    /// commands, so re-execution needs no live engine).
    mm_control: Option<Arc<dyn MarketMakerControlSink>>,
}

impl MatchingExecutor {
    /// Builds an executor for one underlying, with an empty hierarchy that
    /// vivifies leaf books lazily on first use. No microstructure config is applied
    /// (a bare book: no fee schedule, no STP, no contract-spec validation) — the
    /// config-aware siblings [`new_with_registry_and_index`](Self::new_with_registry_and_index)
    /// (live) and [`new_with_microstructure`](Self::new_with_microstructure) (replay)
    /// carry the fee/STP/specs the determinism oracle scopes.
    #[must_use]
    pub fn new(underlying: impl Into<String>) -> Self {
        Self::from_book(UnderlyingOrderBook::new(underlying))
    }

    /// Builds an executor around an already-constructed (and, where applicable,
    /// microstructure-configured) upstream hierarchy, with empty registries and no
    /// market-maker control seam installed. The shared init for every constructor,
    /// so a new sequenced store is added in exactly one place.
    #[must_use]
    fn from_book(underlying_book: UnderlyingOrderBook) -> Self {
        Self {
            underlying_book,
            resting: HashMap::new(),
            venue_to_engine: HashMap::new(),
            idempotency: IdempotencyMap::new(),
            instrument_status: InstrumentStatusRegistry::new(),
            mm_control: None,
        }
    }

    /// Installs the market-maker control apply seam (#47 phase 2), returning `self`
    /// so it composes with a constructor. The live [`crate::state::AppState`] wiring
    /// injects the engine-backed sink here; the replay/recovery constructors leave
    /// it `None`.
    #[must_use]
    pub fn with_mm_control_sink(mut self, sink: Arc<dyn MarketMakerControlSink>) -> Self {
        self.mm_control = Some(sink);
        self
    }

    /// Builds an executor for one underlying with a **fresh** registry (its own
    /// `UnderlyingOrderBook`) and the venue [`MicrostructureConfig`] applied at book
    /// creation — the constructor journal recovery / replay uses so a book vivified
    /// during replay inherits the identical fee schedule, STP mode, and contract
    /// specs the live venue applied, and a fee/STP-sensitive scenario replays exactly
    /// ([02 §5](../../../docs/02-matching-architecture.md#5-determinism),
    /// [05 §4](../../../docs/05-microstructure-config.md#4-fee-schedules)).
    ///
    /// The config is applied **before any leaf is vivified** (the empty hierarchy is
    /// created here, and the upstream setters propagate to every leaf created
    /// afterwards), so recovery re-executes onto the identical leaf configuration.
    ///
    /// # Errors
    ///
    /// [`MicrostructureConfigError`] if the resolved contract specs are rejected by
    /// the upstream `ContractSpecsBuilder` — unreachable for a resolver-validated
    /// config, surfaced rather than unwrapped.
    pub fn new_with_microstructure(
        underlying: impl Into<String>,
        microstructure: &MicrostructureConfig,
    ) -> Result<Self, MicrostructureConfigError> {
        let underlying = underlying.into();
        let underlying_book = UnderlyingOrderBook::new(&underlying);
        apply_to_underlying(&underlying_book, microstructure, &underlying)?;
        Ok(Self::from_book(underlying_book))
    }

    /// Builds an executor for one underlying whose hierarchy shares a **venue-wide**
    /// [`InstrumentRegistry`] and [`SymbolIndex`] with every other underlying's
    /// book, so cross-underlying instrument-id allocation and symbol lookups stay
    /// O(1) across the whole venue without coupling the single writers — each
    /// underlying is still driven by its own actor
    /// ([010](../../../milestones/v0.1-backend-core/010-appstate-wiring.md)).
    ///
    /// The shared handles are threaded **straight into** the upstream
    /// `UnderlyingOrderBook::new_with_registry_and_index` (verified public at the
    /// locked 0.7.0 registry); matching is unchanged. This is the constructor
    /// [`crate::state::AppState`] wires per underlying.
    ///
    /// The venue [`MicrostructureConfig`] is applied at book creation — **before any
    /// leaf is vivified** — so every leaf inherits the identical fee schedule, STP
    /// mode, and contract specs. This is the **same apply** the replay/recovery
    /// constructor ([`new_with_microstructure`](Self::new_with_microstructure))
    /// performs, so a fee/STP-sensitive scenario reconstructs exactly
    /// ([02 §5](../../../docs/02-matching-architecture.md#5-determinism)).
    ///
    /// # Errors
    ///
    /// [`MicrostructureConfigError`] if the resolved contract specs are rejected by
    /// the upstream `ContractSpecsBuilder` — unreachable for a resolver-validated
    /// config, surfaced rather than unwrapped.
    pub fn new_with_registry_and_index(
        underlying: impl Into<String>,
        registry: Arc<InstrumentRegistry>,
        symbol_index: Arc<SymbolIndex>,
        microstructure: &MicrostructureConfig,
    ) -> Result<Self, MicrostructureConfigError> {
        let underlying = underlying.into();
        let underlying_book =
            UnderlyingOrderBook::new_with_registry_and_index(&underlying, registry, symbol_index);
        apply_to_underlying(&underlying_book, microstructure, &underlying)?;
        Ok(Self::from_book(underlying_book))
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
    /// identity, a cancel/replace can find its engine id, and a snapshot cut can
    /// resolve the leaf it rests on (#009).
    #[allow(clippy::too_many_arguments)]
    fn register_resting(
        &mut self,
        engine_id: OrderId,
        symbol: Symbol,
        venue_order_id: VenueOrderId,
        account: AccountId,
        owner: Hash32,
        side: Side,
    ) {
        self.venue_to_engine
            .insert(venue_order_id.clone(), engine_id);
        self.resting.insert(
            engine_id,
            RestingRecord {
                symbol,
                venue_order_id,
                account,
                owner,
                side,
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

    /// Whether `account` owns the resting order with engine id `engine_id` — the
    /// ownership gate every cancel and replace passes **before** mutating the book
    /// (Copilot PR #62 SECURITY). Grounded in the journaled resting-owner registry
    /// ([`RestingRecord::account`], folded from the add commands), so the check is
    /// deterministic and replay-stable — no wall-clock, no RNG, a point lookup.
    ///
    /// Fails **closed**: a missing registry entry (the two maps are kept in
    /// lockstep, so this is an invariant violation) returns `false` rather than
    /// grant a mutation the caller cannot be proven to own.
    #[must_use]
    fn account_owns(&self, engine_id: OrderId, account: &AccountId) -> bool {
        self.resting
            .get(&engine_id)
            .is_some_and(|record| &record.account == account)
    }

    // ---- command handlers -----------------------------------------------

    /// Applies **client-order-id idempotency** (#009) around an `AddOrder`
    /// before it touches a leaf ([01 §6.1](../../../docs/01-domain-model.md)).
    ///
    /// A placement carrying a `client_order_id` is deduplicated on the
    /// account-scoped key: a retry with a **matching** payload fingerprint
    /// returns the **stored terminal result** (no second order is created, the
    /// book is untouched), and a **different** payload at the same key is a
    /// conflicting reuse and is rejected. A first, non-duplicate placement runs
    /// normally and its terminal result is recorded under the key. The lookup is
    /// a `HashMap` point read — no wall-clock, RNG, or iteration order — so the
    /// map is a deterministic function of the journal and dedup fires identically
    /// on a live run and a replay. This is the **minimum** dedup #009 needs so a
    /// retry after a restore returns the stored result; the full pre-journal
    /// dedup and cancel/replace correlation land with the later idempotency work.
    #[allow(clippy::too_many_arguments)]
    fn add_with_idempotency(
        &mut self,
        ctx: &ExecutionContext<'_>,
        symbol: &Symbol,
        order_id: &VenueOrderId,
        account: &AccountId,
        owner: Hash32,
        client_order_id: Option<&ClientOrderId>,
        side: Side,
        order_type: OrderType,
        limit_price: Option<Cents>,
        quantity: u64,
        time_in_force: TimeInForce,
    ) -> VenueOutcome {
        let Some(client_order_id) = client_order_id else {
            return self.execute_add_order(
                ctx,
                symbol,
                order_id,
                account,
                owner,
                side,
                order_type,
                limit_price,
                quantity,
                time_in_force,
            );
        };

        let key = IdempotencyKey::new(account.clone(), client_order_id.clone());
        let fingerprint = IdempotencyFingerprint {
            symbol: symbol.clone(),
            side,
            order_type,
            limit_price,
            quantity,
            time_in_force,
        };
        if let Some(entry) = self.idempotency.lookup(&key) {
            if entry.fingerprint == fingerprint {
                // A matching retry: no second order enters the book and NO economic
                // effect re-executes. Surface a distinct `Duplicate` carrying the
                // ORIGINAL identity + terminal sequence + stored terminal outcome, so
                // the gateway renders the true original terminal report while every
                // fan-out projection treats it as a no-op — replaying the stored fills
                // through a fresh event would double-fold positions and re-print
                // phantom fills/depth (#099).
                return VenueOutcome::Duplicate {
                    original_order_id: entry.order_id.clone(),
                    original_sequence: entry.sequence,
                    terminal: Box::new(entry.terminal.clone()),
                };
            }
            // Conflicting reuse of the same key: refuse rather than rebind it.
            return VenueOutcome::rejected(
                RejectKind::InvalidOrder,
                "client_order_id was reused with a different order",
            );
        }

        let outcome = self.execute_add_order(
            ctx,
            symbol,
            order_id,
            account,
            owner,
            side,
            order_type,
            limit_price,
            quantity,
            time_in_force,
        );
        // Record ONLY a fresh placement's genuine terminal outcome (never a
        // `Duplicate`) under the key, tagged with this committed turn's sequence so a
        // later retry can echo the canonical identity.
        self.idempotency.record(
            key,
            IdempotencyEntry {
                fingerprint,
                order_id: order_id.clone(),
                sequence: ctx.sequence,
                terminal: outcome.clone(),
            },
        );
        outcome
    }

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
            Err(reason) => return VenueOutcome::rejected(RejectKind::InvalidOrder, reason),
        };

        // Instrument-status gate (#47): an order into a non-`Active` instrument is
        // rejected here, on the sequenced path, so a halted/settling/expired book
        // refuses new orders identically live and on replay. The status comes from
        // the venue-owned sequenced registry (a point lookup keyed on the symbol),
        // so the rejection is a deterministic function of the journaled
        // `SetInstrumentStatus` stream — no wall clock, RNG, or iteration order.
        if !self.instrument_status.is_accepting_orders(symbol) {
            return VenueOutcome::rejected(
                RejectKind::InstrumentNotActive,
                instrument_not_active_reason(symbol, self.instrument_status.status_of(symbol)),
            );
        }

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
                        return VenueOutcome::rejected(
                            RejectKind::InvalidOrder,
                            "limit order requires a limit price",
                        );
                    }
                };
                let add = self.run_add(
                    ctx,
                    leaf.as_ref(),
                    symbol,
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
                    VenueOutcome::rejected(RejectKind::NotFillable, reject_reason(add.reason))
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
        symbol: &Symbol,
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
            self.register_resting(
                engine_id,
                symbol.clone(),
                venue_order_id.clone(),
                account.clone(),
                owner,
                side,
            );
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
        // The instrument-status gate is enforced once, up front, in
        // `execute_add_order` (both the limit and market paths flow through it), so
        // by here the instrument is `Active` and accepting orders.
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

    /// Cancels a resting order by its venue order id, **only** when the requesting
    /// `account` owns it.
    ///
    /// Account ownership is enforced on the shared sequenced path — **before** any
    /// book mutation — so REST/WS/FIX all inherit it: a cancel from an account that
    /// is not the resting order's owner is rejected without touching the book, so
    /// no authenticated account can cancel another account's order (Copilot PR #62
    /// SECURITY). The rejection is a captured [`VenueOutcome::Rejected`], which the
    /// single-writer actor journals as this sequence's event.
    fn execute_cancel(
        &mut self,
        symbol: &Symbol,
        venue_order_id: &VenueOrderId,
        account: &AccountId,
    ) -> VenueOutcome {
        let leaf = match self.resolve_leaf_read(symbol) {
            Some(leaf) => leaf,
            None => {
                return VenueOutcome::rejected(RejectKind::NotFound, "order not found");
            }
        };
        let engine_id = match self.venue_to_engine.get(venue_order_id).copied() {
            Some(engine_id) => engine_id,
            None => {
                return VenueOutcome::rejected(RejectKind::NotFound, "order not found");
            }
        };
        // Ownership gate, BEFORE any mutation: the requesting account must own the
        // resting order, or the cancel is refused with the book untouched. The typed
        // `NotOwner` kind is masked as `NotFound` at the CLIENT boundary (#132) but
        // journaled + traced verbatim here as a detective control.
        if !self.account_owns(engine_id, account) {
            return VenueOutcome::rejected(RejectKind::NotOwner, NOT_ORDER_OWNER_REASON);
        }
        match leaf.cancel_order(engine_id) {
            Ok(true) => {
                self.remove_resting(engine_id);
                VenueOutcome::Cancelled {
                    order_id: venue_order_id.clone(),
                }
            }
            Ok(false) => VenueOutcome::rejected(RejectKind::NotResting, "order is not resting"),
            Err(error) => VenueOutcome::rejected(RejectKind::Internal, error.to_string()),
        }
    }

    /// Executes a **non-atomic** replace as cancel-then-add in one turn, recorded
    /// as one [`VenueOutcome`] at one sequence.
    ///
    /// The cancel leg is a **precondition** the add leg is gated on, enforced on
    /// the shared sequenced path so REST/WS/FIX all inherit it:
    ///
    /// - **Ownership** — the requesting `account` must own the target resting
    ///   order, checked **before** any mutation. A mismatch rejects the whole
    ///   command ([`VenueOutcome::Rejected`]) without cancelling or adding, so no
    ///   authenticated account can replace another account's order (Copilot PR #62
    ///   SECURITY).
    /// - **Atomic cancel leg** — if the target is unknown or the cancel does not
    ///   remove it, the whole command is rejected and the add leg does **not**
    ///   run, so a replace never creates a naked new order when nothing was
    ///   replaced (Copilot PR #62).
    ///
    /// Once the cancel leg succeeds, the add leg runs and is **not** rolled back if
    /// it is itself rejected (e.g. a killed `Fok`, or a missing limit price): that
    /// defined partial state is recorded losslessly as
    /// [`VenueOutcome::Replace`] `{ cancelled: true, add: Rejected }`. Every
    /// rejection is a captured outcome the single-writer actor journals.
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
            Err(reason) => return VenueOutcome::rejected(RejectKind::InvalidOrder, reason),
        };

        // Resolve the target resting order. An unknown target rejects the WHOLE
        // command — a replace must never add a naked new order when nothing was
        // replaced.
        let engine_id = match self.venue_to_engine.get(order_id).copied() {
            Some(engine_id) => engine_id,
            None => {
                return VenueOutcome::rejected(RejectKind::NotFound, "order not found");
            }
        };

        // Ownership gate, BEFORE any mutation: the requesting account must own the
        // target order, exactly as on the cancel path. The typed `NotOwner` kind is
        // masked as `NotFound` at the CLIENT boundary (#132) but journaled + traced
        // verbatim here as a detective control.
        if !self.account_owns(engine_id, account) {
            return VenueOutcome::rejected(RejectKind::NotOwner, NOT_ORDER_OWNER_REASON);
        }

        // The replacement inherits the cancelled order's STP owner (the `Replace`
        // command carries no owner of its own). Ownership just proved the record is
        // present, so the fallback is unreachable.
        let owner = self
            .resting
            .get(&engine_id)
            .map(|record| record.owner)
            .unwrap_or(Hash32([0u8; 32]));

        // Cancel leg. If it does not remove the target order, reject the WHOLE
        // replace and do NOT run the add leg.
        if !matches!(leaf.cancel_order(engine_id), Ok(true)) {
            return VenueOutcome::rejected(RejectKind::NotResting, "order is not resting");
        }
        self.remove_resting(engine_id);

        // Add leg — reached ONLY because the cancel leg succeeded; not rolled back
        // if it is itself rejected.
        let add = match limit_price {
            Some(price) => {
                let result = self.run_add(
                    ctx,
                    leaf.as_ref(),
                    symbol,
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
            None => AddOutcome::rejected(
                RejectKind::InvalidOrder,
                "replacement add requires a limit price",
            ),
        };

        VenueOutcome::Replace {
            cancelled: true,
            add,
        }
    }

    // ---- lifecycle + sweep commands (#47) --------------------------------

    /// Applies a [`VenueCommand::SetInstrumentStatus`] transition to the
    /// venue-owned sequenced registry, validated against the **upstream**
    /// lifecycle state machine ([`InstrumentStatus::can_transition`], never a venue
    /// reimplementation).
    ///
    /// The target leaf is vivified through the same idempotent `get_or_create_*`
    /// path an `AddOrder` uses (mirroring the upstream `submit_set_instrument_status`
    /// resolution), so replay rebuilds identical structural state; a malformed or
    /// cross-underlying symbol, or an illegal transition, is captured as a
    /// [`VenueOutcome::Rejected`] with a deterministic reason.
    fn execute_set_instrument_status(
        &mut self,
        symbol: &Symbol,
        status: InstrumentStatus,
    ) -> VenueOutcome {
        // Validate + vivify the target leaf (cross-underlying / parse guard).
        if let Err(reason) = self.resolve_leaf_vivify(symbol) {
            return VenueOutcome::rejected(RejectKind::InvalidOrder, reason);
        }
        match self.instrument_status.try_transition(symbol, status) {
            Ok(applied) => VenueOutcome::InstrumentStatusChanged {
                symbol: symbol.clone(),
                status: applied,
            },
            Err(error) => VenueOutcome::rejected(RejectKind::InvalidOrder, error.to_string()),
        }
    }

    /// Executes a [`VenueCommand::MassCancel`] within this underlying: selects the
    /// resting orders matching `scope` + `cancel_type` from the venue registry,
    /// cancels each through the **upstream** single-order cancel primitive
    /// ([`OptionOrderBook::cancel_order`], never a reimplemented sweep), and records
    /// each removal as an ordered [`CancelledLeg`]
    /// ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
    ///
    /// The venue registry — not the upstream count-only aggregate — is the source
    /// of the per-order `(venue_order_id, owner, symbol, side)` identity the FIX
    /// `OrderMassCancelReport` + per-order `ExecutionReport`s need. The affected
    /// list is **sorted by venue order id**, a deterministic sweep order
    /// independent of map-iteration order, so a replay reproduces it exactly.
    ///
    /// ## Cross-account isolation (SECURITY)
    ///
    /// A CLIENT mass cancel is scoped to its own `account`: the STP `owner` a
    /// `ByUser` filter names is **not** an authorization identity (multiple
    /// accounts may share one STP owner), so a `ByUser` / `BySide` sweep also
    /// requires `record.account == command.account` — an account can only ever
    /// hit its OWN orders. `MassCancelType::All` is the venue-internal
    /// expiry/lifecycle sweep (never a client path) and stays account-agnostic so
    /// an expiry roll cancels every account's orders on the instrument.
    fn execute_mass_cancel(
        &mut self,
        scope: &MassCancelScope,
        cancel_type: &MassCancelType,
        account: &AccountId,
    ) -> VenueOutcome {
        // Resolve the scope's expiration to its canonical `YYYYMMDD` once (an
        // `Expiration` / `Strike` scope); a malformed scope expiry is a rejected
        // command (deterministic — a pure function of the command). This
        // rejection path is unique to `MassCancel` (the coupled kill sweep always
        // uses the `Underlying` scope), so it stays here rather than in the shared
        // sweep helper.
        if let MassCancelScope::Expiration(expiry)
        | MassCancelScope::Strike {
            expiration: expiry, ..
        } = scope
            && let Err(error) = format_expiration_yyyymmdd(expiry)
        {
            return VenueOutcome::rejected(
                RejectKind::InvalidOrder,
                format!("mass-cancel scope expiry is unresolvable: {error}"),
            );
        }

        // A `ByUser` / `BySide` client sweep is pinned to the requesting account;
        // only the account-agnostic `All` lifecycle sweep crosses accounts.
        let account_scope = match cancel_type {
            MassCancelType::All => None,
            MassCancelType::ByUser(_) | MassCancelType::BySide(_) => Some(account),
        };

        VenueOutcome::MassCancelled {
            affected: self.sweep_matching(scope, cancel_type, account_scope),
        }
    }

    /// The reusable sweep body: cancels every resting order matching `scope` +
    /// `cancel_type` through the **upstream** single-order cancel primitive
    /// ([`OptionOrderBook::cancel_order`], never a reimplemented sweep) and returns
    /// each removal as an ordered [`CancelledLeg`]
    /// ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
    ///
    /// Shared by [`execute_mass_cancel`](Self::execute_mass_cancel) and by the
    /// coupled owner-scoped market-maker sweep a **kill** control runs inside its
    /// own turn (`MarketMakerControl { enabled: Some(false), .. }`), so both record
    /// the identical `(venue_order_id, owner, symbol, side, MassCancel)` shape. The
    /// affected list is **sorted by venue order id**, a deterministic sweep order
    /// independent of map-iteration order, so a replay reproduces it exactly — no
    /// wall-clock, no RNG. The caller resolves any scope-expiry rejection before
    /// invoking this.
    ///
    /// `account_scope` is `Some(account)` for a CLIENT `ByUser` / `BySide` sweep —
    /// an extra `record.account == account` gate so a caller can only hit its OWN
    /// orders even when several accounts share one STP owner (the owner is not an
    /// authorization identity, SECURITY). It is `None` for the venue-internal
    /// `All` expiry sweep and the market-maker owner sweep, both of which are
    /// account-agnostic by design.
    fn sweep_matching(
        &mut self,
        scope: &MassCancelScope,
        cancel_type: &MassCancelType,
        account_scope: Option<&AccountId>,
    ) -> Vec<CancelledLeg> {
        // Resolve the scope's expiration to its canonical `YYYYMMDD` for the
        // `mass_cancel_matches` filter; an unresolvable expiry simply matches
        // nothing here (the reject path lives in `execute_mass_cancel`).
        let scope_expiry = match scope {
            MassCancelScope::Expiration(expiry)
            | MassCancelScope::Strike {
                expiration: expiry, ..
            } => format_expiration_yyyymmdd(expiry).ok(),
            MassCancelScope::Underlying | MassCancelScope::Book(_) => None,
        };

        // Collect the matching resting orders (id + identity) from the venue
        // registry. Collected into a Vec and sorted below, so the HashMap iteration
        // order never reaches the sweep / event order.
        let mut targets: Vec<(OrderId, Symbol, VenueOrderId, Hash32, Side)> = self
            .resting
            .iter()
            .filter(|(_, record)| {
                mass_cancel_matches(
                    record,
                    scope,
                    scope_expiry.as_deref(),
                    cancel_type,
                    account_scope,
                )
            })
            .map(|(engine_id, record)| {
                (
                    *engine_id,
                    record.symbol.clone(),
                    record.venue_order_id.clone(),
                    record.owner,
                    record.side,
                )
            })
            .collect();
        targets.sort_by(|a, b| a.2.as_str().cmp(b.2.as_str()));

        let mut affected: Vec<CancelledLeg> = Vec::with_capacity(targets.len());
        for (engine_id, symbol, venue_order_id, owner, side) in targets {
            let Some(leaf) = self.resolve_leaf_read(&symbol) else {
                continue;
            };
            if let Ok(true) = leaf.cancel_order(engine_id) {
                self.remove_resting(engine_id);
                affected.push(CancelledLeg {
                    order_id: venue_order_id,
                    owner,
                    symbol,
                    side,
                    reason: CancelReason::MassCancel,
                });
            }
        }
        affected
    }

    /// Executes a [`VenueCommand::EvictExpiredOrders`] across this underlying:
    /// drives the **upstream** expiry sweep ([`OptionOrderBook::evict_expired_orders`],
    /// which compares the caller-supplied `now_ms` against each order's `Day`/`Gtd`
    /// deadline and never reads a clock of its own) on every leaf that holds a
    /// resting order, then maps the evicted engine ids back to their venue ids.
    ///
    /// Because the sweep is driven by the **journaled** `now_ms` and the resting set
    /// is rebuilt by re-executing the prior `AddOrder`s (with byte-identical explicit
    /// `Gtd` deadlines), a replay evicts the identical set. The evicted list is
    /// sorted by venue order id for a deterministic sweep order.
    ///
    /// **Named upstream limitation.** `Day` orders (and the *admission* of a
    /// `Gtd`/`Day` order) still resolve their deadline against the leaf's default
    /// wall clock — `option-chain-orderbook` 0.7.0 does not thread the venue clock
    /// into lazy leaf construction — so `Day`-TIF eviction is not replay-stable
    /// across arbitrary wall times. Explicit-deadline `Gtd` eviction (the value in
    /// the command) is deterministic; the residual admission gap is pinned by
    /// `tests/determinism.rs::test_day_gtd_admission_determinism_blocked_by_leaf_clock_gap`
    /// ([02 §5.5b](../../../docs/02-matching-architecture.md#5-determinism)).
    fn execute_evict_expired(&mut self, now_ms: EventTimestamp) -> VenueOutcome {
        // The distinct leaves that currently hold a resting order. Collected via a
        // set: per-leaf eviction is independent, so visitation order does not change
        // *which* orders are evicted, and the emitted list is sorted below.
        let mut leaves: Vec<Symbol> = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();
        for record in self.resting.values() {
            if seen.insert(record.symbol.as_str()) {
                leaves.push(record.symbol.clone());
            }
        }

        let cutoff = TimestampMs::new(now_ms.get());
        let mut evicted_ids: Vec<OrderId> = Vec::new();
        for symbol in &leaves {
            if let Some(leaf) = self.resolve_leaf_read(symbol) {
                evicted_ids.extend(leaf.evict_expired_orders(cutoff));
            }
        }

        let mut evicted: Vec<VenueOrderId> = Vec::with_capacity(evicted_ids.len());
        for engine_id in evicted_ids {
            let Some(venue_order_id) = self
                .resting
                .get(&engine_id)
                .map(|record| record.venue_order_id.clone())
            else {
                continue;
            };
            evicted.push(venue_order_id);
            self.remove_resting(engine_id);
        }
        evicted.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        VenueOutcome::Evicted { evicted }
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
                    symbol: record.symbol.clone(),
                    side: record.side,
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

    // ---- snapshot capture / restore (#009) -------------------------------

    /// Captures the executor's contribution to a consistent cut: the resting
    /// orders (read back from the **upstream book** so a partially-filled maker
    /// carries its *current* quantity, not the stale registered one) and the
    /// idempotency map ([02 §9](../../../docs/02-matching-architecture.md)).
    ///
    /// A pure read: it uses non-creating leaf lookups and never mutates
    /// hierarchy structure. The resting orders are sorted by `engine_seq` so the
    /// cut is deterministic and a restore reproduces price-time priority.
    #[must_use]
    pub fn capture_state(&self) -> ExecutorState {
        let mut resting_orders: Vec<RestingOrderCapture> = Vec::with_capacity(self.resting.len());
        for (engine_id, record) in &self.resting {
            let Some(leaf) = self.resolve_leaf_read(&record.symbol) else {
                tracing::error!(
                    symbol = record.symbol.as_str(),
                    "resting order's leaf missing during snapshot capture; skipping"
                );
                continue;
            };
            let Some(order) = leaf.inner().get_order(*engine_id) else {
                tracing::error!(
                    order_id = record.venue_order_id.as_str(),
                    "resting order not present in its leaf during snapshot capture; skipping"
                );
                continue;
            };
            let Some(engine_seq) = engine_id.as_u64() else {
                tracing::error!("resting engine id is not sequential; skipping capture");
                continue;
            };
            let Ok(price_cents) = u64::try_from(order.price().as_u128()) else {
                tracing::error!("resting price out of range during capture; skipping");
                continue;
            };
            resting_orders.push(RestingOrderCapture {
                symbol: record.symbol.clone(),
                order_id: record.venue_order_id.clone(),
                account: record.account.clone(),
                owner: record.owner,
                engine_seq,
                side: order.side(),
                price: Cents::new(price_cents),
                quantity: order.visible_quantity().as_u64(),
                time_in_force: order.time_in_force(),
            });
        }
        resting_orders.sort_by_key(|order| order.engine_seq);
        ExecutorState {
            resting_orders,
            idempotency: self.idempotency.capture(),
            instrument_statuses: self.capture_instrument_statuses(),
        }
    }

    /// Captures every leaf's **non-`Active`** lifecycle status so a restore keeps
    /// a `Halted` / `Settling` / `Expired` instrument non-accepting instead of
    /// silently reactivating it (a fresh rebuilt leaf comes up `Active`).
    ///
    /// A pure read: it walks the hierarchy through non-creating iterators and
    /// never mutates structure. `Active` leaves are skipped — the vivify default
    /// already reproduces `Active` on restore. The result is sorted by symbol so
    /// the cut is deterministic (no wall-clock, no RNG, no map-iteration order).
    #[must_use]
    fn capture_instrument_statuses(&self) -> Vec<InstrumentStatusCapture> {
        let mut statuses: Vec<InstrumentStatusCapture> = Vec::new();
        for (_expiration, expiration_book) in self.underlying_book.expirations().iter() {
            let mut strikes = expiration_book.strike_prices();
            strikes.sort_unstable();
            for strike in strikes {
                let Ok(strike_book) = expiration_book.get_strike(strike) else {
                    continue;
                };
                for leaf in [strike_book.call_arc(), strike_book.put_arc()] {
                    let status = leaf.status();
                    if status == InstrumentStatus::Active {
                        continue;
                    }
                    let Ok(symbol) = Symbol::parse(leaf.symbol()) else {
                        tracing::error!(
                            symbol = leaf.symbol(),
                            "leaf symbol failed to parse during status capture; skipping"
                        );
                        continue;
                    };
                    statuses.push(InstrumentStatusCapture { symbol, status });
                }
            }
        }
        statuses.sort_by(|a, b| a.symbol.as_str().cmp(b.symbol.as_str()));
        statuses
    }

    /// **Prepares** a restore of the leaf books + idempotency map by building a
    /// **complete detached executor** off to the side — the fallible,
    /// **live-non-mutating** phase where every re-add actually happens.
    ///
    /// This is what makes a restore all-or-nothing. It validates every captured
    /// order's symbol resolves within this underlying, orders the re-adds by
    /// `engine_seq` (reproducing price-time priority; a consistent cut never
    /// crosses, so no re-add matches), then re-adds **all** of them into a fresh
    /// detached [`MatchingExecutor`], installs the idempotency map, and re-applies
    /// each captured non-`Active` leaf lifecycle status (after the re-adds, so an
    /// order lands while its leaf is still `Active`). A single failing re-add or
    /// status transition aborts here with a typed error, and because nothing on
    /// this path touches `self`, the running executor's book and the other
    /// three stores are left **exactly** as they were
    /// ([02 §9](../../../docs/02-matching-architecture.md)). Only after every add
    /// has succeeded does [`commit_restore`](Self::commit_restore) swap the fully
    /// built executor into live state.
    ///
    /// Reusing each order's original `engine_seq` for `OrderId::sequential` keeps
    /// the venue↔engine id mapping stable and cannot collide, because the
    /// continued `underlying_sequence` is already past every captured `engine_seq`.
    /// Rebuilding into a fresh underlying book (rather than clearing the live one)
    /// also means every re-add lands on a freshly vivified, order-accepting leaf.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::RebuildFailed`] if a captured order's symbol does
    /// not parse, belongs to a different underlying, cannot be re-added to the
    /// detached book (a malformed cut — e.g. a duplicated `engine_seq`), or a
    /// captured leaf status transition is refused by the upstream lifecycle state
    /// machine. In every case the live executor is untouched.
    pub fn prepare_restore(
        &self,
        resting: &[RestingOrderCapture],
        idempotency: &[IdempotencyRecord],
        instrument_statuses: &[InstrumentStatusCapture],
    ) -> Result<PreparedRestore, SnapshotError> {
        let expected = self.underlying_book.underlying();
        let mut orders: Vec<PreparedRestingOrder> = Vec::with_capacity(resting.len());
        for capture in resting {
            let parsed = SymbolParser::parse(capture.symbol.as_str()).map_err(|e| {
                SnapshotError::RebuildFailed(format!(
                    "unparseable symbol '{}': {e}",
                    capture.symbol
                ))
            })?;
            if parsed.underlying() != expected {
                return Err(SnapshotError::RebuildFailed(format!(
                    "cross-underlying symbol '{}' does not match underlying '{expected}'",
                    capture.symbol
                )));
            }
            orders.push(PreparedRestingOrder {
                symbol: capture.symbol.clone(),
                order_id: capture.order_id.clone(),
                account: capture.account.clone(),
                owner: capture.owner,
                engine_seq: capture.engine_seq,
                side: capture.side,
                price: capture.price,
                quantity: capture.quantity,
                time_in_force: capture.time_in_force,
            });
        }
        orders.sort_by_key(|order| order.engine_seq);

        // Build the whole restored book on a detached executor. A re-add failure
        // here propagates before any live state is touched — `self` is `&self`.
        let mut detached = MatchingExecutor::new(expected);
        detached.rebuild_from(orders)?;
        detached.idempotency = IdempotencyMap::from_records(idempotency);
        // Re-apply lifecycle statuses AFTER every re-add: the upstream add path
        // rejects a non-`Active` leaf, so orders must land while the fresh leaf is
        // still `Active`, then the captured non-`Active` status is set.
        detached.apply_instrument_statuses(instrument_statuses)?;
        Ok(PreparedRestore { detached })
    }

    /// Re-applies each captured leaf lifecycle status onto this (detached)
    /// executor's rebuilt hierarchy — the status step of a restore.
    ///
    /// Each freshly-vivified leaf comes up [`InstrumentStatus::Active`], so this
    /// drives it to the captured non-`Active` target through the upstream
    /// `OptionOrderBook::set_status`, whose lifecycle state machine admits every
    /// reachable target (`Halted` / `Settling` / `Expired`) from `Active`. The
    /// captures are already ordered by symbol, so application is deterministic.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::RebuildFailed`] if a captured leaf's symbol does
    /// not resolve within this underlying or the upstream state machine refuses
    /// the transition (a malformed cut — e.g. an unreachable `Pending`). Because
    /// this runs on the detached executor, no live state is affected.
    fn apply_instrument_statuses(
        &self,
        instrument_statuses: &[InstrumentStatusCapture],
    ) -> Result<(), SnapshotError> {
        for capture in instrument_statuses {
            let leaf = self
                .resolve_leaf_vivify(&capture.symbol)
                .map_err(|reason| {
                    SnapshotError::RebuildFailed(format!(
                        "could not vivify leaf for status restore '{}': {reason}",
                        capture.symbol.as_str()
                    ))
                })?;
            leaf.set_status(capture.status).map_err(|error| {
                SnapshotError::RebuildFailed(format!(
                    "restoring instrument status {} for '{}' failed: {error}",
                    capture.status,
                    capture.symbol.as_str()
                ))
            })?;
        }
        Ok(())
    }

    /// Re-adds every prepared order (already ordered by `engine_seq`) into this
    /// executor's fresh hierarchy — the detached-build step of a restore.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::RebuildFailed`] on the first order whose leaf
    /// cannot be vivified or whose re-add is rejected by the engine. The caller
    /// discards the partially built detached executor, so no live state is
    /// affected.
    fn rebuild_from(&mut self, orders: Vec<PreparedRestingOrder>) -> Result<(), SnapshotError> {
        for order in orders {
            let leaf = self.resolve_leaf_vivify(&order.symbol).map_err(|reason| {
                SnapshotError::RebuildFailed(format!(
                    "could not vivify leaf for '{}': {reason}",
                    order.symbol
                ))
            })?;
            let engine_id = OrderId::sequential(order.engine_seq);
            leaf.add_limit_order_with_tif_and_user_full(
                engine_id,
                order.side,
                order.price.as_u128(),
                order.quantity,
                order.time_in_force,
                order.owner,
            )
            .map_err(|error| {
                SnapshotError::RebuildFailed(format!(
                    "re-adding restored order '{}' failed: {error}",
                    order.order_id.as_str()
                ))
            })?;
            self.register_resting(
                engine_id,
                order.symbol,
                order.order_id,
                order.account,
                order.owner,
                order.side,
            );
        }
        Ok(())
    }

    /// **Commits** a prepared restore — the truly infallible mutation phase: an
    /// atomic swap of the fully built detached executor into live state.
    ///
    /// All fallible work (validation and every re-add) already ran in
    /// [`prepare_restore`](Self::prepare_restore) against a detached executor, so
    /// installing it here cannot fail and cannot leave a half-restored book: the
    /// old book, registry, and idempotency map are replaced wholesale by the
    /// detached ones in a single move.
    pub fn commit_restore(&mut self, prepared: PreparedRestore) {
        *self = prepared.detached;
    }
}

/// A fully built, detached executor image ready to be swapped into live state —
/// the output of [`MatchingExecutor::prepare_restore`] applied by
/// [`MatchingExecutor::commit_restore`] (#009). The prepare/commit split is what
/// makes a restore **all-or-nothing**: every fallible re-add already happened on
/// this detached executor, so the commit is an infallible swap that never leaves
/// a half-restored live book.
pub struct PreparedRestore {
    detached: MatchingExecutor,
}

impl std::fmt::Debug for PreparedRestore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedRestore")
            .field("underlying", &self.detached.underlying())
            .field("resting_orders", &self.detached.resting.len())
            .field("idempotency", &self.detached.idempotency.len())
            .finish()
    }
}

/// One validated resting order in a [`PreparedRestore`].
#[derive(Debug)]
struct PreparedRestingOrder {
    symbol: Symbol,
    order_id: VenueOrderId,
    account: AccountId,
    owner: Hash32,
    engine_seq: u64,
    side: Side,
    price: Cents,
    quantity: u64,
    time_in_force: TimeInForce,
}

impl CommandExecutor for MatchingExecutor {
    fn execute(&mut self, context: ExecutionContext<'_>) -> VenueOutcome {
        match context.command {
            VenueCommand::AddOrder {
                symbol,
                order_id,
                account,
                owner,
                client_order_id,
                side,
                order_type,
                limit_price,
                quantity,
                time_in_force,
                ..
            } => self.add_with_idempotency(
                &context,
                symbol,
                order_id,
                account,
                *owner,
                client_order_id.as_ref(),
                *side,
                *order_type,
                *limit_price,
                *quantity,
                *time_in_force,
            ),
            VenueCommand::CancelOrder {
                symbol,
                order_id,
                account,
            } => self.execute_cancel(symbol, order_id, account),
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
            // A market-maker control change (#47): apply the knobs onto the market
            // maker through the sequenced apply seam (a live-only side effect — the
            // requotes it induces are journaled as their own `AddOrder` commands).
            // The replay/recovery path installs no sink, so re-execution derives the
            // identical event.
            //
            // A **kill** (`enabled: Some(false)`) additionally cancels the market
            // maker's standing quotes — COUPLED into this control's own turn, per
            // underlying, so the one journaled event is both "control applied" AND
            // "these MM-owner orders cancelled" (#117). This is crash-consistent
            // (no separate follow-on command a crash could skip between the two
            // appends) and honours the determinism contract: cancelling existing
            // resting orders is exactly what `sweep_matching` (shared with
            // `execute_mass_cancel`) already does within one turn — it is NOT
            // re-entering the sequencer or emitting new orders. Re-executing the
            // kill on replay re-runs the owner-scoped sweep against the rebuilt
            // resting set → an identical `swept`. Every non-kill control sweeps
            // nothing (`swept` empty).
            VenueCommand::MarketMakerControl {
                spread_multiplier,
                size_scalar,
                directional_skew,
                enabled,
            } => {
                if let Some(sink) = &self.mm_control {
                    sink.apply_control(MarketMakerControlKnobs {
                        spread_multiplier: *spread_multiplier,
                        size_scalar: *size_scalar,
                        directional_skew: *directional_skew,
                        enabled: *enabled,
                    });
                }
                let swept = if *enabled == Some(false) {
                    // The market-maker owner sweep is a venue-internal owner sweep
                    // (the MM owner IS the authorization boundary here), so it
                    // passes `None` — its behaviour is unchanged by the client
                    // account gate.
                    self.sweep_matching(
                        &MassCancelScope::Underlying,
                        &MassCancelType::ByUser(MARKET_MAKER_OWNER),
                        None,
                    )
                } else {
                    Vec::new()
                };
                VenueOutcome::ControlApplied { swept }
            }
            // Clock / sim-step advances have no leaf effect on this path; their
            // derived effects are journaled as their own sequenced commands.
            VenueCommand::Clock { .. } | VenueCommand::SimStep { .. } => {
                VenueOutcome::ControlApplied { swept: Vec::new() }
            }
            // Lifecycle + hierarchy-sweep commands (#47): all sequenced + journaled.
            VenueCommand::SetInstrumentStatus { symbol, status } => {
                self.execute_set_instrument_status(symbol, *status)
            }
            VenueCommand::MassCancel {
                scope,
                cancel_type,
                account,
            } => self.execute_mass_cancel(scope, cancel_type, account),
            VenueCommand::EvictExpiredOrders { now_ms } => self.execute_evict_expired(*now_ms),
        }
    }
}

// ============================================================================
// Free helpers
// ============================================================================

/// The client-safe reject reason for a cancel/replace whose requesting account is
/// not the resting order's owner — an authorization failure on the shared
/// sequenced path (Copilot PR #62 SECURITY).
///
/// Carried on the captured [`VenueOutcome::Rejected`]; the gateway boundary maps
/// it to an authorization rejection (HTTP `403` on REST/WS, an `OrderCancelReject`
/// with an authorization reason on FIX). It echoes no order or account detail.
const NOT_ORDER_OWNER_REASON: &str = "requesting account does not own the order";

/// The deterministic engine order id for a command: a sequential id keyed on the
/// per-underlying sequence, so the engine never RNG-mints a `Uuid` and the id is
/// unique within the underlying.
#[inline]
fn engine_order_id(sequence: SequenceNumber) -> OrderId {
    OrderId::sequential(sequence.get())
}

/// The **deterministic** reject reason for an order into a non-`Active`
/// instrument — a pure function of the symbol and its upstream status
/// [`Display`](std::fmt::Display), so a replay reproduces the exact reject text.
#[inline]
fn instrument_not_active_reason(symbol: &Symbol, status: InstrumentStatus) -> String {
    format!("instrument {symbol} is {status} and is not accepting orders")
}

/// Whether a resting order matches a mass-cancel `scope` + `cancel_type`, within
/// an optional owning-`account` gate.
///
/// `scope_expiry` is the scope expiration pre-formatted to canonical `YYYYMMDD`
/// (present for an `Expiration` / `Strike` scope), so the per-order predicate does
/// no fallible formatting. A resting order whose own symbol fails to parse is
/// excluded (it can never match a hierarchy scope) — defensive under the
/// single-writer invariant, where every registered symbol was already parsed.
///
/// `account_scope` is `Some(account)` for a CLIENT sweep: the STP `owner` a
/// `ByUser` filter names is NOT an authorization identity (multiple accounts may
/// share one STP owner), so the record must ALSO belong to the requesting account
/// — a caller can never reach another account's order (SECURITY). It is `None`
/// for the account-agnostic `All` lifecycle/expiry sweep and the market-maker
/// owner sweep.
fn mass_cancel_matches(
    record: &RestingRecord,
    scope: &MassCancelScope,
    scope_expiry: Option<&str>,
    cancel_type: &MassCancelType,
    account_scope: Option<&AccountId>,
) -> bool {
    // Account gate first: even a `ByUser(owner)` match must not cross accounts
    // that happen to share the STP owner.
    if let Some(account) = account_scope
        && record.account != *account
    {
        return false;
    }
    let type_matches = match cancel_type {
        MassCancelType::All => true,
        MassCancelType::BySide(side) => record.side == *side,
        MassCancelType::ByUser(owner) => record.owner == *owner,
    };
    if !type_matches {
        return false;
    }
    match scope {
        MassCancelScope::Underlying => true,
        MassCancelScope::Book(symbol) => record.symbol == *symbol,
        MassCancelScope::Expiration(_) => {
            let Ok(parsed) = SymbolParser::parse(record.symbol.as_str()) else {
                return false;
            };
            Some(parsed.expiration_str()) == scope_expiry
        }
        MassCancelScope::Strike { strike, .. } => {
            let Ok(parsed) = SymbolParser::parse(record.symbol.as_str()) else {
                return false;
            };
            Some(parsed.expiration_str()) == scope_expiry && parsed.strike() == *strike
        }
    }
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
        return AddOutcome::rejected(RejectKind::NotFillable, reject_reason(add.reason));
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
/// Uses the upstream **checked** `try_calculate_fee`: the venue's checked-fee proof
/// (`docs/05 §4.1`, the [`MicrostructureConfig`] resolver) makes its `Err(FeeOverflow)`
/// branch provably unreachable for every admissible notional, so the map to
/// [`MoneyError::Overflow`] is a seal-class fail-safe — never a clamp, panic, or
/// unwrap. The `i128 → i64` narrowing that follows is likewise checked, so a fee
/// that does not fit the persisted `SignedCents` is the same typed overflow rather
/// than a silent truncation.
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
            let fee = schedule
                .try_calculate_fee(notional, is_maker)
                .map_err(|_| MoneyError::Overflow)?;
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

/// Spawns a per-underlying single-writer actor whose [`MatchingExecutor`] shares a
/// **venue-wide** [`InstrumentRegistry`] + [`SymbolIndex`] with every other
/// underlying's book — the O(1) cross-underlying lookup wiring
/// [`crate::state::AppState`] uses so `BTC` and `ETH` sequence concurrently
/// without a shared writer lock
/// ([010](../../../milestones/v0.1-backend-core/010-appstate-wiring.md)). The venue
/// [`MicrostructureConfig`] is applied at book creation before any leaf is vivified.
/// Returns the bounded [`ActorHandle`] plus the task's [`JoinHandle`] for graceful
/// shutdown.
///
/// `mm_control` is the optional market-maker control apply seam (#47): `Some` on the
/// live path (the persona layer's engine-backed [`MarketMakerControlSink`], phase 2)
/// so a `MarketMakerControl` command takes effect on the sequenced path, and `None`
/// where no live engine is driven.
///
/// # Errors
///
/// [`MicrostructureConfigError`] if the resolved contract specs are rejected by the
/// upstream `ContractSpecsBuilder` (unreachable for a resolver-validated config).
#[allow(clippy::too_many_arguments)]
pub fn spawn_matching_actor_with_registry_and_index<J, F, C>(
    config: ActorConfig,
    journal: J,
    fan_out: F,
    clock: C,
    registry: Arc<InstrumentRegistry>,
    symbol_index: Arc<SymbolIndex>,
    microstructure: &MicrostructureConfig,
    mm_control: Option<Arc<dyn MarketMakerControlSink>>,
) -> Result<(ActorHandle, JoinHandle<()>), MicrostructureConfigError>
where
    J: VenueJournal + Send + 'static,
    F: FanOut + Send + 'static,
    C: VenueClock + Send + 'static,
{
    let executor = MatchingExecutor::new_with_registry_and_index(
        config.underlying.as_ref(),
        registry,
        symbol_index,
        microstructure,
    )?;
    let executor = match mm_control {
        Some(sink) => executor.with_mm_control_sink(sink),
        None => executor,
    };
    Ok(spawn_underlying_actor(
        config, journal, executor, fan_out, clock,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::actor::{FixedClock, NoopFanOut, UnderlyingActor};
    use crate::exchange::identity::{JournalHeader, LineageId};
    use crate::exchange::journal::{InMemoryVenueJournal, JournalRecord};
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

    /// The captured outcome journaled at `sequence` (its paired event) — a
    /// fully-committed turn always journals one, so a missing event is a bug.
    fn outcome_at(records: &[JournalRecord], sequence: u64) -> VenueOutcome {
        let target = SequenceNumber::new(sequence);
        for record in records {
            if let JournalRecord::Event(event) = record
                && event.underlying_sequence == target
            {
                return event.outcome.clone();
            }
        }
        panic!("no event journaled at sequence {sequence}");
    }

    /// A cancel command for `venue_order_id` submitted by `account`.
    fn cancel_by(venue_order_id: &VenueOrderId, account: &str) -> VenueCommand {
        VenueCommand::CancelOrder {
            symbol: sym(),
            order_id: venue_order_id.clone(),
            account: AccountId::new(account),
        }
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

    /// A client mass-cancel over the whole underlying, filtered by `cancel_type`
    /// and requested by `account`.
    fn mass_cancel(cancel_type: MassCancelType, account: &str) -> VenueCommand {
        VenueCommand::MassCancel {
            scope: MassCancelScope::Underlying,
            cancel_type,
            account: AccountId::new(account),
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
            VenueOutcome::Rejected { reason, .. } => assert!(reason.contains("cross-underlying")),
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

    #[test]
    fn test_cross_account_cancel_is_rejected_and_journaled() {
        // The security property on the shared sequenced path: an authenticated
        // account may NOT cancel another account's order. Driven end-to-end through
        // the single-writer actor so the rejection is proven to reach the journal,
        // and the victim's order is proven to survive (Copilot PR #62 SECURITY).
        let lin = lineage();
        let mut actor = UnderlyingActor::new(
            ActorConfig::new(UNDERLYING, lin.clone(), 16),
            InMemoryVenueJournal::new(JournalHeader::new(lin.clone())),
            MatchingExecutor::new(UNDERLYING),
            NoopFanOut,
            FixedClock::new(TS),
        );
        let order_id = lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0);

        // seq 0: "owner" rests a sell.
        if let Err(e) = actor.handle(add(
            &lin,
            0,
            "owner",
            0x11,
            Side::Sell,
            50_000,
            2,
            TimeInForce::Gtc,
        )) {
            panic!("the owner's add must commit: {e}");
        }
        // seq 1: a DIFFERENT account tries to cancel the owner's order.
        if let Err(e) = actor.handle(cancel_by(&order_id, "attacker")) {
            panic!("the cancel turn must commit its (rejected) outcome: {e}");
        }
        // seq 2: the TRUE owner cancels — this must succeed, proving the attacker's
        // attempt never removed the order.
        if let Err(e) = actor.handle(cancel_by(&order_id, "owner")) {
            panic!("the owner's cancel must commit: {e}");
        }

        let records = match actor.journal().read_from(SequenceNumber::START) {
            Ok(records) => records,
            Err(e) => panic!("in-memory read is infallible: {e}"),
        };
        // The attacker's cancel (seq 1) journaled a Rejected outcome carrying the
        // TYPED `NotOwner` kind AND the verbatim internal reason — the true cause is
        // preserved in the journal + tracing as a detective control, even though the
        // gateway masks it as not-found on the wire (#132).
        match outcome_at(&records, 1) {
            VenueOutcome::Rejected { kind, reason } => {
                assert_eq!(
                    kind,
                    RejectKind::NotOwner,
                    "the journal records the TRUE kind"
                );
                assert_eq!(reason, NOT_ORDER_OWNER_REASON.to_string());
            }
            other => panic!("expected the attacker's cancel to be Rejected, got {other:?}"),
        }
        // ...and the owner's later cancel (seq 2) succeeded — the order was still
        // resting after the cross-account attempt, so nothing was cancelled by it.
        assert!(
            matches!(outcome_at(&records, 2), VenueOutcome::Cancelled { .. }),
            "the owner's cancel must succeed, proving the order survived the attacker"
        );
    }

    #[test]
    fn test_client_by_user_mass_cancel_is_account_scoped_even_with_shared_stp_owner() {
        // SECURITY (#97 finding 1): two accounts share ONE STP owner. A client
        // `ByUser` cancel-all must sweep ONLY the requesting account's orders — the
        // STP owner is not an authorization identity. And the venue-internal `All`
        // expiry sweep, being account-agnostic, still cancels BOTH accounts' orders.
        const SHARED_OWNER: u8 = 0x55;
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);

        // seq 0: account-a rests a sell at 50_000; seq 1: account-b rests a sell at
        // 50_100 — SAME STP owner, DIFFERENT account. Different prices so neither
        // crosses the other (both are asks).
        let a = run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "account-a",
                SHARED_OWNER,
                Side::Sell,
                50_000,
                1,
                TimeInForce::Gtc,
            ),
        );
        assert!(matches!(a, VenueOutcome::Added { .. }), "A's add rests");
        let b = run(
            &mut ex,
            &lin,
            1,
            &add(
                &lin,
                1,
                "account-b",
                SHARED_OWNER,
                Side::Sell,
                50_100,
                1,
                TimeInForce::Gtc,
            ),
        );
        assert!(matches!(b, VenueOutcome::Added { .. }), "B's add rests");
        let a_order = lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0);
        let b_order = lin.venue_order_id(UNDERLYING, SequenceNumber::new(1), 0);

        // seq 2: account-a's ByUser cancel-all sweeps ONLY A's order, even though B
        // shares the STP owner the filter names.
        let outcome = run(
            &mut ex,
            &lin,
            2,
            &mass_cancel(MassCancelType::ByUser(owner(SHARED_OWNER)), "account-a"),
        );
        match outcome {
            VenueOutcome::MassCancelled { affected } => {
                assert_eq!(affected.len(), 1, "only account-a's order is swept");
                assert_eq!(affected[0].order_id, a_order);
                // The leg carries the resting order's own symbol/side (#97 finding 3).
                assert_eq!(affected[0].symbol, sym());
                assert_eq!(affected[0].side, Side::Sell);
            }
            other => panic!("expected MassCancelled, got {other:?}"),
        }
        // B's order survives: its ask level is still on the book.
        assert_eq!(
            ex.top_of_book(&sym()).best_ask,
            Some(Cents::new(50_100)),
            "account-b's order was never touched by account-a's cancel-all"
        );

        // seq 3: the account-agnostic `All` lifecycle/expiry sweep cancels B's
        // surviving order regardless of the account it names.
        let outcome = run(
            &mut ex,
            &lin,
            3,
            &mass_cancel(MassCancelType::All, "lifecycle"),
        );
        match outcome {
            VenueOutcome::MassCancelled { affected } => {
                assert_eq!(affected.len(), 1, "All sweeps the surviving B order");
                assert_eq!(affected[0].order_id, b_order);
            }
            other => panic!("expected MassCancelled, got {other:?}"),
        }
        assert_eq!(
            ex.top_of_book(&sym()).best_ask,
            None,
            "the All sweep emptied the book across accounts"
        );
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

    #[test]
    fn test_replace_with_missing_cancel_leg_does_not_add() {
        // A replace whose cancel leg finds nothing must reject the WHOLE command
        // and NOT run the add leg — otherwise it would create a naked new order
        // that replaced nothing (Copilot PR #62).
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // Seed an unrelated resting sell so the leaf exists, but the replace
        // targets an order id that was never added.
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
            &VenueCommand::Replace {
                symbol: sym(),
                order_id: VenueOrderId::new("never-existed"),
                new_order_id: lin.venue_order_id(UNDERLYING, SequenceNumber::new(1), 0),
                account: AccountId::new("acct"),
                side: Side::Buy,
                limit_price: Some(Cents::new(49_000)),
                quantity: 4,
                time_in_force: TimeInForce::Gtc,
                stp_mode: STPMode::None,
            },
        );
        // The whole replace is rejected (not a `Replace` outcome) — nothing added.
        assert!(
            matches!(outcome, VenueOutcome::Rejected { .. }),
            "a missing cancel leg must reject the whole replace, got {outcome:?}"
        );
        // The book is exactly the seed: the replacement buy at 49_000 never rested,
        // and the unrelated sell is untouched.
        let top = ex.top_of_book(&sym());
        assert_eq!(top.best_ask, Some(Cents::new(50_000)));
        assert_eq!(top.ask_depth, 1);
        assert_eq!(
            top.best_bid, None,
            "the replacement add leg must not have rested a bid"
        );
    }

    #[test]
    fn test_cross_account_replace_is_rejected_and_does_not_mutate() {
        // Ownership is enforced on the replace path exactly as on cancel: a
        // different account may not replace another account's resting order, and
        // the reject happens BEFORE any mutation (Copilot PR #62 SECURITY).
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        run(
            &mut ex,
            &lin,
            0,
            &add(
                &lin,
                0,
                "owner",
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
                account: AccountId::new("attacker"),
                side: Side::Sell,
                limit_price: Some(Cents::new(50_100)),
                quantity: 4,
                time_in_force: TimeInForce::Gtc,
                stp_mode: STPMode::None,
            },
        );
        match outcome {
            VenueOutcome::Rejected { kind, reason } => {
                assert_eq!(
                    kind,
                    RejectKind::NotOwner,
                    "the journal records the TRUE kind"
                );
                assert_eq!(reason, NOT_ORDER_OWNER_REASON.to_string());
            }
            other => panic!("expected a not-owner Rejected, got {other:?}"),
        }
        // The owner's original order still rests, untouched at its old price/depth —
        // neither cancelled nor replaced.
        let top = ex.top_of_book(&sym());
        assert_eq!(top.best_ask, Some(Cents::new(50_000)));
        assert_eq!(top.ask_depth, 2);
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

    // ---- snapshot capture / restore (#009) -------------------------------

    /// A limit add with an explicit account-scoped `client_order_id` (the
    /// idempotency key), rather than the per-sequence default of [`add`].
    #[allow(clippy::too_many_arguments)]
    fn add_with_cloid(
        lineage: &LineageId,
        sequence: u64,
        account: &str,
        owner_byte: u8,
        side: Side,
        price: u64,
        quantity: u64,
        cloid: &str,
    ) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym(),
            order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(sequence), 0),
            account: AccountId::new(account),
            owner: owner(owner_byte),
            client_order_id: Some(ClientOrderId::new(cloid.to_string())),
            side,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        }
    }

    #[test]
    fn test_capture_and_restore_returns_books_to_the_cut() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // Two resting orders on opposite sides that do not cross.
        run(
            &mut ex,
            &lin,
            0,
            &add(&lin, 0, "m1", 0x11, Side::Sell, 50_100, 3, TimeInForce::Gtc),
        );
        run(
            &mut ex,
            &lin,
            1,
            &add(&lin, 1, "b1", 0x22, Side::Buy, 49_900, 2, TimeInForce::Gtc),
        );
        let top_before = ex.top_of_book(&sym());
        let state = ex.capture_state();
        assert_eq!(state.resting_orders.len(), 2);

        // Mutate away from the cut: cancel one, add another.
        run(
            &mut ex,
            &lin,
            2,
            &VenueCommand::CancelOrder {
                symbol: sym(),
                order_id: lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0),
                account: AccountId::new("m1"),
            },
        );
        run(
            &mut ex,
            &lin,
            3,
            &add(&lin, 3, "m2", 0x33, Side::Sell, 50_050, 4, TimeInForce::Gtc),
        );
        assert_ne!(
            ex.top_of_book(&sym()),
            top_before,
            "mutation moved the book"
        );

        // Restore the cut: prepare then commit returns the book exactly.
        let prepared = match ex.prepare_restore(
            &state.resting_orders,
            &state.idempotency,
            &state.instrument_statuses,
        ) {
            Ok(p) => p,
            Err(e) => panic!("prepare failed: {e}"),
        };
        ex.commit_restore(prepared);
        assert_eq!(
            ex.top_of_book(&sym()),
            top_before,
            "restore returns the books to the snapshot state"
        );
    }

    #[test]
    fn test_restore_round_trips_instrument_lifecycle_status() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // Rest an order, then halt the instrument (a legal `Active -> Halted`
        // edge). A halted leaf stops accepting NEW orders but keeps its resting
        // book — exactly the "silently tradable again" regression this guards.
        run(
            &mut ex,
            &lin,
            0,
            &add(&lin, 0, "m1", 0x11, Side::Sell, 50_100, 3, TimeInForce::Gtc),
        );
        let leaf = match ex.resolve_leaf_vivify(&sym()) {
            Ok(l) => l,
            Err(e) => panic!("resolve failed: {e}"),
        };
        if let Err(e) = leaf.set_status(InstrumentStatus::Halted) {
            panic!("set_status failed: {e}");
        }
        assert!(!leaf.status().is_accepting_orders());

        // The cut captures the non-`Active` status (keyed by symbol).
        let state = ex.capture_state();
        assert!(
            state
                .instrument_statuses
                .iter()
                .any(|c| c.symbol == sym() && c.status == InstrumentStatus::Halted),
            "capture records the halted instrument status"
        );

        // Restore into a FRESH executor whose leaves would otherwise vivify
        // `Active` (the upstream default) — the bug being fixed.
        let mut restored = MatchingExecutor::new(UNDERLYING);
        let prepared = match restored.prepare_restore(
            &state.resting_orders,
            &state.idempotency,
            &state.instrument_statuses,
        ) {
            Ok(p) => p,
            Err(e) => panic!("prepare failed: {e}"),
        };
        restored.commit_restore(prepared);

        // The restored instrument keeps its status — NOT silently `Active` again.
        let restored_leaf = match restored.resolve_leaf_read(&sym()) {
            Some(l) => l,
            None => panic!("restored leaf missing"),
        };
        assert_eq!(
            restored_leaf.status(),
            InstrumentStatus::Halted,
            "a restored halted instrument must not silently become tradable"
        );
        assert!(!restored_leaf.status().is_accepting_orders());
        // The whole executor cut round-trips (resting book + status together).
        assert_eq!(restored.capture_state(), state);
    }

    #[test]
    fn test_capture_reads_current_resting_quantity_after_partial_maker_fill() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // Rest a sell of 5, then a buy of 2 partially consumes it → 3 rests.
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
        run(
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
        let state = ex.capture_state();
        assert_eq!(state.resting_orders.len(), 1);
        // The cut reads the reduced resting quantity from the book, not the stale
        // registered 5.
        assert_eq!(state.resting_orders[0].quantity, 3);
    }

    #[test]
    fn test_prepare_restore_rejects_a_cross_underlying_capture() {
        let lin = lineage();
        let ex = MatchingExecutor::new(UNDERLYING);
        // A capture whose symbol belongs to a different underlying is a fault the
        // preparation phase refuses before any mutation.
        let bad = RestingOrderCapture {
            symbol: match Symbol::parse("ETH-20240329-50000-C") {
                Ok(s) => s,
                Err(e) => panic!("fixture parse failed: {e:?}"),
            },
            order_id: lin.venue_order_id("ETH", SequenceNumber::new(0), 0),
            account: AccountId::new("x"),
            owner: owner(0x11),
            engine_seq: 0,
            side: Side::Sell,
            price: Cents::new(50_000),
            quantity: 1,
            time_in_force: TimeInForce::Gtc,
        };
        match ex.prepare_restore(&[bad], &[], &[]) {
            Err(SnapshotError::RebuildFailed(_)) => {}
            other => panic!("expected RebuildFailed, got {other:?}"),
        }
    }

    #[test]
    fn test_failed_restore_leaves_live_books_untouched_and_is_all_or_nothing() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        // A defined, non-empty live book: two non-crossing resting orders.
        run(
            &mut ex,
            &lin,
            0,
            &add(&lin, 0, "m1", 0x11, Side::Sell, 50_100, 3, TimeInForce::Gtc),
        );
        run(
            &mut ex,
            &lin,
            1,
            &add(&lin, 1, "b1", 0x22, Side::Buy, 49_900, 2, TimeInForce::Gtc),
        );
        let live_top_before = ex.top_of_book(&sym());
        let live_state_before = ex.capture_state();

        // A malformed cut that PASSES symbol validation (both are BTC contracts)
        // but whose second order re-uses the first's `engine_seq`. The detached
        // rebuild rests the first, then collides on `OrderId::sequential(seq)`
        // when it re-adds the second — the engine returns `DuplicateOrderId`, so
        // the restore fails mid-way. This is exactly the case the old commit path
        // would have half-applied after already clearing the live book.
        let dup_seq = 7;
        let malformed = vec![
            RestingOrderCapture {
                symbol: sym(),
                order_id: lin.venue_order_id(UNDERLYING, SequenceNumber::new(dup_seq), 0),
                account: AccountId::new("x"),
                owner: owner(0x33),
                engine_seq: dup_seq,
                side: Side::Sell,
                price: Cents::new(50_500),
                quantity: 1,
                time_in_force: TimeInForce::Gtc,
            },
            RestingOrderCapture {
                symbol: sym(),
                order_id: lin.venue_order_id(UNDERLYING, SequenceNumber::new(dup_seq), 1),
                account: AccountId::new("y"),
                owner: owner(0x44),
                // Duplicated `engine_seq` → the second re-add collides.
                engine_seq: dup_seq,
                side: Side::Sell,
                price: Cents::new(50_600),
                quantity: 1,
                time_in_force: TimeInForce::Gtc,
            },
        ];

        // Determinism-stable: the malformed restore fails identically on repeat,
        // and each failed prepare leaves EVERY live store byte-for-byte the cut.
        for _ in 0..2 {
            match ex.prepare_restore(&malformed, &[], &[]) {
                Err(SnapshotError::RebuildFailed(_)) => {}
                other => panic!("expected RebuildFailed, got {other:?}"),
            }
            assert_eq!(
                ex.top_of_book(&sym()),
                live_top_before,
                "a failed restore must not mutate the live book (all-or-nothing)"
            );
            assert_eq!(
                ex.capture_state(),
                live_state_before,
                "a failed restore leaves every live store untouched"
            );
        }

        // A well-formed restore of the live book's own cut still round-trips
        // exactly through the detached-then-swap path.
        let prepared = match ex.prepare_restore(
            &live_state_before.resting_orders,
            &live_state_before.idempotency,
            &live_state_before.instrument_statuses,
        ) {
            Ok(p) => p,
            Err(e) => panic!("prepare of a well-formed cut failed: {e}"),
        };
        ex.commit_restore(prepared);
        assert_eq!(
            ex.top_of_book(&sym()),
            live_top_before,
            "the successful restore round-trips to the cut"
        );
        assert_eq!(ex.capture_state(), live_state_before);
    }

    #[test]
    fn test_idempotency_retry_returns_stored_result_without_a_second_order() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        let first = run(
            &mut ex,
            &lin,
            0,
            &add_with_cloid(&lin, 0, "acct", 0x11, Side::Sell, 50_000, 2, "dup-1"),
        );
        let top_after_first = ex.top_of_book(&sym());
        assert_eq!(top_after_first.ask_depth, 2);

        // Retry the SAME (account, client_order_id) with a matching payload at a
        // later sequence: a distinct `Duplicate` surfaces the ORIGINAL identity +
        // terminal sequence + stored terminal outcome, and the book is untouched (no
        // second order) (#099).
        let retry = run(
            &mut ex,
            &lin,
            1,
            &add_with_cloid(&lin, 1, "acct", 0x11, Side::Sell, 50_000, 2, "dup-1"),
        );
        match &retry {
            VenueOutcome::Duplicate {
                original_order_id,
                original_sequence,
                terminal,
            } => {
                assert_eq!(
                    original_order_id,
                    &lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0),
                    "the retry echoes the ORIGINAL order id, not the retry turn's fresh id"
                );
                assert_eq!(
                    *original_sequence,
                    SequenceNumber::new(0),
                    "the retry echoes the ORIGINAL terminal sequence, not this turn's"
                );
                assert_eq!(
                    terminal.as_ref(),
                    &first,
                    "the Duplicate carries the stored terminal result for rendering"
                );
            }
            other => panic!("expected an idempotent Duplicate, got {other:?}"),
        }
        assert_eq!(
            ex.top_of_book(&sym()),
            top_after_first,
            "the retry created no second order"
        );
    }

    #[test]
    fn test_idempotency_conflicting_reuse_is_rejected() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        run(
            &mut ex,
            &lin,
            0,
            &add_with_cloid(&lin, 0, "acct", 0x11, Side::Sell, 50_000, 2, "key-1"),
        );
        // Same key, DIFFERENT payload (quantity) → conflicting reuse, rejected.
        let outcome = run(
            &mut ex,
            &lin,
            1,
            &add_with_cloid(&lin, 1, "acct", 0x11, Side::Sell, 50_000, 9, "key-1"),
        );
        match outcome {
            VenueOutcome::Rejected { reason, .. } => assert!(reason.contains("client_order_id")),
            other => panic!("expected a conflicting-reuse rejection, got {other:?}"),
        }
    }

    #[test]
    fn test_idempotency_map_round_trips_through_capture_and_restore() {
        let lin = lineage();
        let mut ex = MatchingExecutor::new(UNDERLYING);
        run(
            &mut ex,
            &lin,
            0,
            &add_with_cloid(&lin, 0, "acct", 0x11, Side::Sell, 50_000, 2, "dup-1"),
        );
        let state = ex.capture_state();
        assert_eq!(state.idempotency.len(), 1);

        // A fresh executor rehydrated from the cut serves the same retry.
        let mut restored = MatchingExecutor::new(UNDERLYING);
        let prepared = match restored.prepare_restore(
            &state.resting_orders,
            &state.idempotency,
            &state.instrument_statuses,
        ) {
            Ok(p) => p,
            Err(e) => panic!("prepare failed: {e}"),
        };
        restored.commit_restore(prepared);
        let top_after_restore = restored.top_of_book(&sym());
        let retry = run(
            &mut restored,
            &lin,
            1,
            &add_with_cloid(&lin, 1, "acct", 0x11, Side::Sell, 50_000, 2, "dup-1"),
        );
        // The rehydrated map serves the stored result as an idempotent `Duplicate`
        // carrying the original identity + the stored `Added` terminal; no second
        // order rests.
        match &retry {
            VenueOutcome::Duplicate {
                original_order_id,
                original_sequence,
                terminal,
            } => {
                assert_eq!(
                    original_order_id,
                    &lin.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0)
                );
                assert_eq!(*original_sequence, SequenceNumber::new(0));
                assert!(matches!(terminal.as_ref(), VenueOutcome::Added { .. }));
            }
            other => panic!("expected an idempotent Duplicate after restore, got {other:?}"),
        }
        assert_eq!(restored.top_of_book(&sym()), top_after_restore);
    }
}
