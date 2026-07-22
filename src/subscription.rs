//! Services layer: the WebSocket **market-data service** — the per-instrument
//! subscription manager (the monotonic `instrument_sequence` + event-sourced L2
//! aggregate), the bounded `tokio::broadcast` fan-out, the venue-wide connection
//! cap, and the [`FanOut`] wiring that feeds it committed
//! venue events **post-journal** ([03 §4, §4.1](../docs/03-protocol-surfaces.md),
//! [01 §9.1](../docs/01-domain-model.md)).
//!
//! ## A service, not a gateway — the layering
//!
//! This is a **service module** (a sibling of [`crate::auth`] / [`crate::ohlc`]),
//! **not** part of a gateway. [`OrderbookSubscriptionManager`] and [`WsFanOut`]
//! depend **only** on [`crate::models`] (the DTOs) and [`crate::exchange`] (the
//! committed [`VenueEvent`] + the
//! [`FanOut`] seam) and `tokio` — they never import
//! [`crate::state`] or [`crate::gateway`], so the layered dependency flow
//! (transport → application → domain / services) holds. [`crate::state::AppState`]
//! owns the manager (replacing the #010 subscriptions placeholder) and wires a
//! [`WsFanOut`] alongside the #008 [`StoreFanOut`](crate::exchange::StoreFanOut)
//! into every underlying's single-writer actor via the exchange-owned
//! [`TeeFanOut`](crate::exchange::TeeFanOut); the WS transport
//! ([`crate::gateway::ws`]) reaches this service through `AppState`.
//!
//! ## The fan-out is post-journal and never blocks the order path
//!
//! The actor calls [`FanOut::emit`] at step 5 —
//! **after** the paired [`VenueEvent`] is journaled
//! ([02 §6](../docs/02-matching-architecture.md)). [`WsFanOut::emit`] folds the
//! event into the affected instrument's L2 aggregate and enqueues the resulting
//! [`WsMessage`]s onto a **bounded** `tokio::broadcast` — a non-blocking `send`
//! that never waits on a slow consumer (a laggard drops and must re-snapshot), so
//! the order path is never stalled ([07 §4](../docs/07-performance-budgets.md),
//! [08 §5](../docs/08-threat-model.md)). The market-data `instrument_sequence` is
//! a **separate namespace** from the journaled `underlying_sequence`: a gap is
//! repaired only by a fresh snapshot (re-subscribe), never a resend
//! ([01 §9.1](../docs/01-domain-model.md)).
//!
//! ## Channel producers ([03 §4.1](../docs/03-protocol-surfaces.md))
//!
//! - `orderbook` — the committed book mutation, event-sourced into an L2
//!   aggregate; a subscribe yields one snapshot at a baseline
//!   `instrument_sequence`, then strictly-increasing resulting-quantity deltas.
//!   Only **user-driven** mutations emit deltas; a control-plane event
//!   (`MarketMakerControl`) never does.
//! - `trades` — one public print per match (`maker`/`taker` order ids).
//! - `fills` — one **anonymised** print per committed fill leg: the four join
//!   keys only (`execution_id`, `underlying_sequence`, `venue_ts`, `liquidity`),
//!   **never** `account` or `fee` (account-scoped detail is REST/FIX only).
//! - `prices` — the committed `SimStep` price override (the manual price path,
//!   #013); the continuous `PriceSimulator` walk (#016) extends this producer.
//! - `quotes` — the `Quoter` (#015): **not landed**, so this channel accepts a
//!   subscribe but flows no data yet (honest pending — no fabricated quotes).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast};

use crate::exchange::{
    AddOutcome, Cents, EventTimestamp, FanOut, Fill as SeamFill, MassCancelScope, Side as SeamSide,
    SignedCents, Symbol, SymbolParser, VenueCommand, VenueEvent, VenueOutcome,
};
use crate::models::{
    BookSide, ExecutionId, LiquidityFlag, PriceLevelChange, PriceLevelData, Side as DtoSide,
    SubscriptionChannel, VenueOrderId, WsMessage,
};

/// The bounded capacity of the venue-wide market-data broadcast — a **DoS
/// control** ([08 §5](../docs/08-threat-model.md)): a slow consumer never grows an
/// unbounded queue, it lags and must re-snapshot. The live value is venue config
/// (#022); this fixes a bounded default.
pub const WS_BROADCAST_CAPACITY: usize = 1_024;

/// The venue-wide cap on **concurrent** `/ws` connections — a **DoS control**
/// ([08 §5](../docs/08-threat-model.md)). A handshake acquires one permit at
/// upgrade and releases it when the socket closes; at the ceiling the venue
/// refuses the upgrade (`503`) rather than admit an unbounded number of sockets.
/// The live value is venue config (#022); this fixes a bounded default.
pub const MAX_WS_CONNECTIONS: usize = 1_024;

// ============================================================================
// Per-instrument L2 aggregate + market-data sequence
// ============================================================================

/// One resting order tracked in an instrument's L2 aggregate, so a later cancel
/// / fill / mass-cancel resolves the level it affects (the [`VenueOutcome`]
/// carries the affected `order_id`, not its price/quantity).
#[derive(Debug, Clone)]
struct RestingOrder {
    side: BookSide,
    /// Resting price in **cents**.
    price: u64,
    /// Remaining resting quantity in **contracts**.
    remaining: u64,
}

/// The event-sourced L2 aggregate for one instrument plus its monotonic
/// `instrument_sequence` — reconstructed from the committed [`VenueEvent`] stream
/// so a subscribe can serve a real snapshot and each user-driven mutation emits a
/// resulting-quantity delta.
#[derive(Debug)]
struct InstrumentState {
    /// The market-data `instrument_sequence` — the sequence of the **last**
    /// emitted delta (the aggregate reflects every delta up to and including it).
    sequence: u64,
    /// Bid levels: price cents → total resting quantity.
    bids: BTreeMap<u64, u64>,
    /// Ask levels: price cents → total resting quantity.
    asks: BTreeMap<u64, u64>,
    /// Resting orders by id, for cancel / fill / mass-cancel level resolution.
    orders: HashMap<VenueOrderId, RestingOrder>,
}

impl InstrumentState {
    fn new() -> Self {
        Self {
            sequence: 0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
        }
    }

    fn level(&self, side: BookSide) -> &BTreeMap<u64, u64> {
        match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        }
    }

    fn level_mut(&mut self, side: BookSide) -> &mut BTreeMap<u64, u64> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    fn qty_at(&self, side: BookSide, price: u64) -> u64 {
        self.level(side).get(&price).copied().unwrap_or(0)
    }

    /// Adds `qty` at `(side, price)`, tracking the resting order and the touched
    /// level. `checked_add(..).unwrap_or(MAX)` handles the unreachable overflow
    /// explicitly (admission-bounded contracts); the repo rules forbid
    /// `saturating_*`.
    #[allow(clippy::manual_saturating_arithmetic)]
    fn rest_order(
        &mut self,
        id: VenueOrderId,
        side: BookSide,
        price: u64,
        qty: u64,
        touched: &mut BTreeSet<(BookSide, u64)>,
    ) {
        if qty == 0 {
            return;
        }
        self.orders.insert(
            id,
            RestingOrder {
                side,
                price,
                remaining: qty,
            },
        );
        let level = self.level_mut(side);
        let entry = level.entry(price).or_insert(0);
        *entry = entry.checked_add(qty).unwrap_or(u64::MAX);
        touched.insert((side, price));
    }

    /// Reduces the level at `(side, price)` by `amount`, removing the level when
    /// it reaches zero. Clamps at zero defensively (an unknown / mid-stream order
    /// never drives a level negative). `checked_sub(..).unwrap_or(0)` is the
    /// explicit clamp — never `saturating_*`.
    #[allow(clippy::manual_saturating_arithmetic)]
    fn reduce_level(
        &mut self,
        side: BookSide,
        price: u64,
        amount: u64,
        touched: &mut BTreeSet<(BookSide, u64)>,
    ) {
        let level = self.level_mut(side);
        if let Some(qty) = level.get_mut(&price) {
            *qty = qty.checked_sub(amount).unwrap_or(0);
            if *qty == 0 {
                level.remove(&price);
            }
        }
        touched.insert((side, price));
    }

    /// Consumes `qty` from a resting maker order (a fill leg), reducing its level
    /// and dropping it when fully consumed.
    #[allow(clippy::manual_saturating_arithmetic)]
    fn reduce_order(
        &mut self,
        id: &VenueOrderId,
        qty: u64,
        touched: &mut BTreeSet<(BookSide, u64)>,
    ) {
        let Some(order) = self.orders.get_mut(id) else {
            return;
        };
        let amount = qty.min(order.remaining);
        order.remaining = order.remaining.checked_sub(amount).unwrap_or(0);
        let (side, price, gone) = (order.side, order.price, order.remaining == 0);
        self.reduce_level(side, price, amount, touched);
        if gone {
            self.orders.remove(id);
        }
    }

    /// Removes a resting order entirely (a cancel / mass-cancel / evict / STP
    /// removal), reducing its level by its whole remainder.
    fn remove_order(&mut self, id: &VenueOrderId, touched: &mut BTreeSet<(BookSide, u64)>) {
        let Some(order) = self.orders.remove(id) else {
            return;
        };
        self.reduce_level(order.side, order.price, order.remaining, touched);
    }

    /// Folds a committed maker-leg fill set (only `Maker` legs touch a resting
    /// order; the taker leg is the incoming aggressor, handled by the resting
    /// remainder).
    fn consume_maker_fills(&mut self, fills: &[SeamFill], touched: &mut BTreeSet<(BookSide, u64)>) {
        for fill in fills {
            if fill.liquidity == LiquidityFlag::Maker {
                self.reduce_order(&fill.order_id, fill.quantity, touched);
            }
        }
    }

    /// Applies a committed outcome's book effect and returns the resulting-quantity
    /// changes at every touched level (a level's resulting quantity is `0` when it
    /// was removed). Non-book outcomes (control, status, reject) return no changes.
    fn apply(&mut self, command: &VenueCommand, outcome: &VenueOutcome) -> Vec<PriceLevelChange> {
        let mut touched: BTreeSet<(BookSide, u64)> = BTreeSet::new();
        match outcome {
            VenueOutcome::Added {
                fills,
                resting_quantity,
                stp_cancelled,
            } => {
                self.consume_maker_fills(fills, &mut touched);
                for leg in stp_cancelled {
                    self.remove_order(&leg.order_id, &mut touched);
                }
                if *resting_quantity > 0
                    && let VenueCommand::AddOrder {
                        order_id,
                        side,
                        limit_price: Some(price),
                        ..
                    } = command
                {
                    self.rest_order(
                        order_id.clone(),
                        book_side(*side),
                        price.get(),
                        *resting_quantity,
                        &mut touched,
                    );
                }
            }
            VenueOutcome::Market {
                fills,
                stp_cancelled,
                ..
            } => {
                self.consume_maker_fills(fills, &mut touched);
                for leg in stp_cancelled {
                    self.remove_order(&leg.order_id, &mut touched);
                }
            }
            VenueOutcome::Cancelled { order_id } => {
                self.remove_order(order_id, &mut touched);
            }
            VenueOutcome::Replace { cancelled, add } => {
                if *cancelled && let VenueCommand::Replace { order_id, .. } = command {
                    self.remove_order(order_id, &mut touched);
                }
                match add {
                    AddOutcome::Filled {
                        fills,
                        stp_cancelled,
                    } => {
                        self.consume_maker_fills(fills, &mut touched);
                        for leg in stp_cancelled {
                            self.remove_order(&leg.order_id, &mut touched);
                        }
                    }
                    AddOutcome::Rested {
                        fills,
                        resting_quantity,
                        stp_cancelled,
                    } => {
                        self.consume_maker_fills(fills, &mut touched);
                        for leg in stp_cancelled {
                            self.remove_order(&leg.order_id, &mut touched);
                        }
                        if *resting_quantity > 0
                            && let VenueCommand::Replace {
                                new_order_id,
                                side,
                                limit_price: Some(price),
                                ..
                            } = command
                        {
                            self.rest_order(
                                new_order_id.clone(),
                                book_side(*side),
                                price.get(),
                                *resting_quantity,
                                &mut touched,
                            );
                        }
                    }
                    AddOutcome::Rejected { .. } => {}
                }
            }
            VenueOutcome::MassCancelled { affected } => {
                for leg in affected {
                    self.remove_order(&leg.order_id, &mut touched);
                }
            }
            // A kill (`MarketMakerControl { enabled: Some(false), .. }`) couples the
            // owner-scoped market-maker sweep into the control's own turn, so its
            // cancelled MM quotes are carried on `ControlApplied.swept` (not a
            // separate `MassCancel` event). Remove each swept leg from the aggregate
            // exactly as `MassCancelled` does, so a kill emits orderbook-removal
            // deltas. Every non-kill control (and `Clock` / `SimStep`) carries an
            // empty `swept`, so this is a no-op for them (#117).
            VenueOutcome::ControlApplied { swept } => {
                for leg in swept {
                    self.remove_order(&leg.order_id, &mut touched);
                }
            }
            VenueOutcome::Evicted { evicted } => {
                for id in evicted {
                    self.remove_order(id, &mut touched);
                }
            }
            // A `Duplicate` (#099 idempotent retry) executed no book effect — the
            // original placement's depth/prints were published at first placement, so
            // it MUST NOT re-apply any delta here (no phantom depth).
            VenueOutcome::InstrumentStatusChanged { .. }
            | VenueOutcome::Rejected { .. }
            | VenueOutcome::Duplicate { .. } => {}
        }
        touched
            .into_iter()
            .map(|(side, price)| PriceLevelChange {
                side,
                price: Cents::new(price),
                quantity: self.qty_at(side, price),
            })
            .collect()
    }

    /// Builds an [`WsMessage::OrderbookSnapshot`] of the current aggregate at the
    /// current `instrument_sequence`. Bids are best-first (descending price), asks
    /// best-first (ascending); `depth` truncates each side to its best levels.
    fn snapshot(&self, symbol: &Symbol, depth: Option<usize>) -> WsMessage {
        let mut bids: Vec<PriceLevelData> = self
            .bids
            .iter()
            .rev()
            .map(|(&price, &quantity)| PriceLevelData {
                price: Cents::new(price),
                quantity,
            })
            .collect();
        let mut asks: Vec<PriceLevelData> = self
            .asks
            .iter()
            .map(|(&price, &quantity)| PriceLevelData {
                price: Cents::new(price),
                quantity,
            })
            .collect();
        if let Some(depth) = depth {
            bids.truncate(depth);
            asks.truncate(depth);
        }
        WsMessage::OrderbookSnapshot {
            channel: SubscriptionChannel::Orderbook,
            symbol: symbol.clone(),
            sequence: self.sequence,
            bids,
            asks,
        }
    }
}

// ============================================================================
// The subscription manager (the shared market-data service)
// ============================================================================

/// The WebSocket market-data service: per-instrument `instrument_sequence` + L2
/// aggregate, one bounded venue-wide `tokio::broadcast` fan-out every connection
/// subscribes to, and the venue-wide connection-slot semaphore.
///
/// Owned by [`crate::state::AppState`] behind an `Arc`; fed committed events by
/// [`WsFanOut`] (post-journal) and read by the per-connection socket loop
/// (snapshot on subscribe, filtered forwarding of the broadcast).
#[derive(Debug)]
pub struct OrderbookSubscriptionManager {
    /// Per-instrument market-data state, keyed by the canonical [`Symbol`].
    instruments: DashMap<Symbol, InstrumentState>,
    /// The venue-wide bounded market-data broadcast (orderbook deltas, trade and
    /// fill prints, and price updates). A slow consumer lags and re-snapshots.
    global_tx: broadcast::Sender<WsMessage>,
    /// The venue-wide cap on concurrent `/ws` connections — one permit per open
    /// socket, released on close (a DoS control, [`MAX_WS_CONNECTIONS`]).
    connection_slots: Arc<Semaphore>,
}

impl Default for OrderbookSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderbookSubscriptionManager {
    /// Builds a manager with the default bounded broadcast capacity
    /// ([`WS_BROADCAST_CAPACITY`]) and connection cap ([`MAX_WS_CONNECTIONS`]).
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(WS_BROADCAST_CAPACITY)
    }

    /// Builds a manager with an explicit bounded broadcast capacity (clamped to at
    /// least `1`) and the default connection cap.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_limits(capacity, MAX_WS_CONNECTIONS)
    }

    /// Builds a manager with an explicit broadcast capacity **and** connection cap
    /// (both clamped to at least `1`) — used by the DoS-bound tests.
    #[must_use]
    pub fn with_limits(broadcast_capacity: usize, max_connections: usize) -> Self {
        let (global_tx, _rx) = broadcast::channel(broadcast_capacity.max(1));
        Self {
            instruments: DashMap::new(),
            global_tx,
            connection_slots: Arc::new(Semaphore::new(max_connections.max(1))),
        }
    }

    /// Reserves a connection slot for a new `/ws` socket, returning an owned permit
    /// held for the socket's lifetime (released when the permit drops on close), or
    /// `None` when the venue-wide [`MAX_WS_CONNECTIONS`] cap is reached (the
    /// handshake is refused). A non-blocking `try_acquire` — the order path and the
    /// upgrade never wait on it.
    #[must_use]
    pub fn try_acquire_connection(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.connection_slots).try_acquire_owned().ok()
    }

    /// The number of connection slots still available (for observability / tests).
    #[must_use]
    pub fn available_connection_slots(&self) -> usize {
        self.connection_slots.available_permits()
    }

    /// Returns a fresh receiver on the venue-wide market-data broadcast — one per
    /// connection. Dropping it tears the subscription down.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<WsMessage> {
        self.global_tx.subscribe()
    }

    /// The current [`WsMessage::OrderbookSnapshot`] for `symbol` (empty book at
    /// sequence `0` when the instrument has seen no committed mutation yet — an
    /// honest empty projection, never fabricated depth).
    #[must_use]
    pub fn orderbook_snapshot(&self, symbol: &Symbol, depth: Option<usize>) -> WsMessage {
        match self.instruments.get(symbol) {
            Some(state) => state.snapshot(symbol, depth),
            None => WsMessage::OrderbookSnapshot {
                channel: SubscriptionChannel::Orderbook,
                symbol: symbol.clone(),
                sequence: 0,
                bids: Vec::new(),
                asks: Vec::new(),
            },
        }
    }

    /// Folds one committed [`VenueEvent`] into the affected instrument's aggregate
    /// and enqueues the resulting market-data messages onto the bounded broadcast.
    /// Returns the assigned orderbook-delta `instrument_sequence`, if a delta was
    /// emitted (used by the sequence-monotonicity property test).
    ///
    /// Called by [`WsFanOut::emit`] at fan-out step 5 (post-journal). Every
    /// `broadcast::send` is O(1) and non-blocking — a full ring buffer drops the
    /// slowest consumers (they re-snapshot), never the producer.
    #[allow(clippy::manual_saturating_arithmetic)]
    pub fn on_committed_event(&self, event: &VenueEvent) -> Option<u64> {
        let Some(symbol) = command_symbol(&event.command) else {
            // A command with no single contract symbol: the manual price override
            // (`SimStep`) feeds the `prices` channel; every other such command
            // (clock, market-maker control) has no market-data effect.
            self.emit_price(event);
            return None;
        };

        // Market-maker requotes do NOT emit an `orderbook_delta` — they land in
        // the next periodic snapshot (Backend semantics kept,
        // [02 §6](../docs/02-matching-architecture.md)). The rule keys on the
        // venue-reserved market-maker identity the #015 engine tags its
        // `AddOrder`/`CancelOrder` with ([`crate::exchange::is_market_maker_command`]).
        // The marker is a venue-wide contract that lives in the domain core, so
        // this service reads it DOWNWARD from `crate::exchange`, never sideways
        // from the market-maker domain peer. The book effect is still folded into
        // the aggregate so a snapshot reflects the maker's resting liquidity, and
        // any fill/trade prints below still fire (a fill is a real print, not a
        // book delta).
        let is_market_maker = crate::exchange::is_market_maker_command(&event.command);

        // Fold the book effect under the instrument's shard guard, assigning the
        // next `instrument_sequence` only when the book actually changed AND the
        // mutation was user-driven. The guard is released before any
        // `broadcast::send` (send is non-blocking, so no lock is held across a
        // stall).
        let (sequence, changes) = {
            let mut state = self
                .instruments
                .entry(symbol.clone())
                .or_insert_with(InstrumentState::new);
            let changes = state.apply(&event.command, &event.outcome);
            if changes.is_empty() || is_market_maker {
                // No delta and no sequence bump for a requote (or a book no-op);
                // the aggregate was still updated above so snapshots stay correct.
                (state.sequence, Vec::new())
            } else {
                // Checked, never wrapping: the market-data sequence is a protocol
                // counter. `u64::MAX` is unreachable (2^64 mutations per
                // instrument); it clamps rather than wraps, and the market-data
                // namespace is best-effort (a client re-snapshots on any gap).
                let next = state.sequence.checked_add(1).unwrap_or(u64::MAX);
                state.sequence = next;
                (next, changes)
            }
        };

        let delta_sequence = if changes.is_empty() {
            None
        } else {
            let _ = self.global_tx.send(WsMessage::OrderbookDelta {
                symbol: symbol.clone(),
                sequence,
                changes,
            });
            Some(sequence)
        };

        self.emit_fills_and_trades(event, &symbol);
        delta_sequence
    }

    /// Enqueues the anonymised `fill` prints and the public `trade` prints for a
    /// committed event's captured fills.
    fn emit_fills_and_trades(&self, event: &VenueEvent, symbol: &Symbol) {
        let fills = outcome_fills(&event.outcome);
        if fills.is_empty() {
            return;
        }
        let Some(underlying) = underlying_of(symbol) else {
            // The symbol was validated on the way in, so this is unreachable; fail
            // safe (drop the prints) rather than emit an unattributable print.
            tracing::error!(
                symbol = symbol.as_str(),
                "could not resolve the underlying from a validated symbol; skipping ws prints"
            );
            return;
        };

        // `fills` channel: one ANONYMISED print per committed leg — the four join
        // keys only, never `account` or `fee`.
        for fill in fills {
            let _ = self.global_tx.send(WsMessage::Fill {
                execution_id: fill.execution_id.clone(),
                underlying_sequence: event.underlying_sequence,
                venue_ts: event.venue_ts,
                liquidity: fill.liquidity,
                symbol: underlying.clone(),
                instrument: symbol.clone(),
                side: dto_side(fill.side),
                quantity: fill.quantity,
                price: fill.price,
                // `edge` needs a quote-time theoretical value (a pricer, #015); it
                // defaults to `0` here exactly as the #008 `ExecutionRecord` does,
                // so the wire shape is stable and never fabricated.
                edge: SignedCents::new(0),
            });
        }

        // `trades` channel: one public print per match, pairing the two legs.
        for trade in pair_trades(fills, symbol, event.venue_ts) {
            let _ = self.global_tx.send(trade);
        }
    }

    /// Enqueues a `price` update for a committed `SimStep` override (the manual
    /// price path); the continuous `PriceSimulator` (#016) extends this producer.
    fn emit_price(&self, event: &VenueEvent) {
        if let VenueCommand::SimStep {
            underlying, price, ..
        } = &event.command
            && !matches!(event.outcome, VenueOutcome::Rejected { .. })
        {
            let _ = self.global_tx.send(WsMessage::Price {
                symbol: underlying.clone(),
                price_cents: *price,
            });
        }
    }
}

// ============================================================================
// FanOut wiring: WsFanOut (the exchange-owned TeeFanOut composes it with #008's
// StoreFanOut in AppState)
// ============================================================================

/// The [`FanOut`] that feeds the
/// [`OrderbookSubscriptionManager`] — the WS side of the actor's post-journal
/// fan-out (step 5).
///
/// Composed alongside the #008 [`StoreFanOut`](crate::exchange::StoreFanOut) via
/// the exchange-owned [`TeeFanOut`](crate::exchange::TeeFanOut) so the **same**
/// committed event lands in the shared stores and the WS broadcast without either
/// blocking the other or the order path.
#[derive(Debug, Clone)]
pub struct WsFanOut {
    manager: Arc<OrderbookSubscriptionManager>,
}

impl WsFanOut {
    /// Wires a fan-out over the shared subscription manager.
    #[must_use]
    #[inline]
    pub fn new(manager: Arc<OrderbookSubscriptionManager>) -> Self {
        Self { manager }
    }
}

impl FanOut for WsFanOut {
    #[inline]
    fn emit(&mut self, event: &VenueEvent) {
        // O(1) enqueue onto a bounded broadcast (after an O(log n) L2 fold); never
        // blocks the order path — a slow consumer lags and re-snapshots.
        let _ = self.manager.on_committed_event(event);
    }
}

// ============================================================================
// Projection helpers
// ============================================================================

/// The DTO [`Side`](DtoSide) for an upstream matching-seam [`Side`](SeamSide).
#[inline]
const fn dto_side(side: SeamSide) -> DtoSide {
    match side {
        SeamSide::Buy => DtoSide::Buy,
        SeamSide::Sell => DtoSide::Sell,
    }
}

/// The book side an order [`Side`](SeamSide) rests on: a buy is a bid, a sell an
/// ask.
#[inline]
const fn book_side(side: SeamSide) -> BookSide {
    match side {
        SeamSide::Buy => BookSide::Bid,
        SeamSide::Sell => BookSide::Ask,
    }
}

/// The single contract symbol a command targets, when it has one and can affect
/// that instrument's book. A hierarchy-wide mass cancel (non-`Book` scope) and
/// venue-global commands have none — and are not routable onto the per-underlying
/// path anyway ([`AppState::submit`](crate::state::AppState::submit)).
fn command_symbol(command: &VenueCommand) -> Option<Symbol> {
    match command {
        VenueCommand::AddOrder { symbol, .. }
        | VenueCommand::CancelOrder { symbol, .. }
        | VenueCommand::Replace { symbol, .. }
        | VenueCommand::SetInstrumentStatus { symbol, .. } => Some(symbol.clone()),
        VenueCommand::MassCancel {
            scope: MassCancelScope::Book(symbol),
            ..
        } => Some(symbol.clone()),
        _ => None,
    }
}

/// The captured fill legs of an outcome (empty for a non-filling outcome).
fn outcome_fills(outcome: &VenueOutcome) -> &[SeamFill] {
    match outcome {
        VenueOutcome::Added { fills, .. } | VenueOutcome::Market { fills, .. } => fills,
        VenueOutcome::Replace { add, .. } => match add {
            AddOutcome::Filled { fills, .. } | AddOutcome::Rested { fills, .. } => fills,
            AddOutcome::Rejected { .. } => &[],
        },
        _ => &[],
    }
}

/// The underlying ticker of a validated symbol via the upstream [`SymbolParser`]
/// (never hand-parsed).
fn underlying_of(symbol: &Symbol) -> Option<String> {
    SymbolParser::parse(symbol.as_str())
        .ok()
        .map(|parsed| parsed.underlying().to_string())
}

/// The maker/taker accumulator for one match while pairing fill legs into a
/// public trade print.
#[derive(Default)]
struct TradeAccum {
    maker: Option<VenueOrderId>,
    taker: Option<VenueOrderId>,
    price: Option<Cents>,
    quantity: Option<u64>,
}

/// Pairs committed fill legs into public [`WsMessage::Trade`] prints — one per
/// `execution_id`, carrying the maker and taker order ids. A leg without its
/// counterparty (never produced by the two-leg matching engine) is skipped.
fn pair_trades(fills: &[SeamFill], symbol: &Symbol, ts: EventTimestamp) -> Vec<WsMessage> {
    let mut order: Vec<ExecutionId> = Vec::new();
    let mut by_exec: HashMap<ExecutionId, TradeAccum> = HashMap::new();
    for fill in fills {
        if !by_exec.contains_key(&fill.execution_id) {
            order.push(fill.execution_id.clone());
            by_exec.insert(fill.execution_id.clone(), TradeAccum::default());
        }
        if let Some(acc) = by_exec.get_mut(&fill.execution_id) {
            acc.price = Some(fill.price);
            acc.quantity = Some(fill.quantity);
            match fill.liquidity {
                LiquidityFlag::Maker => acc.maker = Some(fill.order_id.clone()),
                LiquidityFlag::Taker => acc.taker = Some(fill.order_id.clone()),
            }
        }
    }
    order
        .into_iter()
        .filter_map(|execution_id| {
            let acc = by_exec.get(&execution_id)?;
            Some(WsMessage::Trade {
                trade_id: execution_id.as_str().to_string(),
                symbol: symbol.clone(),
                price: acc.price?,
                quantity: acc.quantity?,
                timestamp: ts,
                maker_order_id: acc.maker.clone()?,
                taker_order_id: acc.taker.clone()?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{
        CancelReason, CancelledLeg, Hash32, LineageId, STPMode, SequenceNumber, TimeInForce,
    };
    use crate::models::{AccountId, OrderType};

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

    /// A resting limit add (no fills) at `(side, price, qty)`.
    fn resting_add(seq: u64, order_id: &str, side: SeamSide, price: u64, qty: u64) -> VenueEvent {
        let command = VenueCommand::AddOrder {
            symbol: sym(),
            order_id: VenueOrderId::new(order_id),
            account: AccountId::new("acct"),
            owner: Hash32([1; 32]),
            client_order_id: None,
            side,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity: qty,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        };
        VenueEvent::new(
            SequenceNumber::new(seq),
            EventTimestamp::new(1_700_000_000_000),
            command,
            VenueOutcome::Added {
                fills: vec![],
                resting_quantity: qty,
                stp_cancelled: vec![],
            },
        )
    }

    /// The two linked legs of one crossing match, sharing one execution id.
    fn match_legs(seq: u64, price: u64, qty: u64) -> (SeamFill, SeamFill) {
        let lineage = lineage();
        let sequence = SequenceNumber::new(seq);
        let execution_id = lineage.execution_id(UNDERLYING, sequence, 0);
        let maker = SeamFill {
            execution_id: execution_id.clone(),
            order_id: VenueOrderId::new("maker-1"),
            account: AccountId::new("maker"),
            owner: Hash32([0x11; 32]),
            side: SeamSide::Sell,
            liquidity: LiquidityFlag::Maker,
            price: Cents::new(price),
            quantity: qty,
            fee: SignedCents::new(-10),
        };
        let taker = SeamFill {
            execution_id,
            order_id: VenueOrderId::new("taker-1"),
            account: AccountId::new("taker"),
            owner: Hash32([0x22; 32]),
            side: SeamSide::Buy,
            liquidity: LiquidityFlag::Taker,
            price: Cents::new(price),
            quantity: qty,
            fee: SignedCents::new(15),
        };
        (maker, taker)
    }

    /// A crossing taker buy that fully consumes the resting maker `maker-1`.
    fn crossing_buy(seq: u64, price: u64, qty: u64) -> VenueEvent {
        let (maker, taker) = match_legs(seq, price, qty);
        let command = VenueCommand::AddOrder {
            symbol: sym(),
            order_id: VenueOrderId::new("taker-1"),
            account: AccountId::new("taker"),
            owner: Hash32([0x22; 32]),
            client_order_id: None,
            side: SeamSide::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity: qty,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        };
        VenueEvent::new(
            SequenceNumber::new(seq),
            EventTimestamp::new(1_700_000_000_000),
            command,
            VenueOutcome::Added {
                fills: vec![maker, taker],
                resting_quantity: 0,
                stp_cancelled: vec![],
            },
        )
    }

    fn control_event(seq: u64) -> VenueEvent {
        VenueEvent::new(
            SequenceNumber::new(seq),
            EventTimestamp::new(1),
            VenueCommand::MarketMakerControl {
                spread_multiplier: Some(1.5),
                size_scalar: None,
                directional_skew: None,
                enabled: None,
            },
            VenueOutcome::ControlApplied { swept: vec![] },
        )
    }

    fn drain(rx: &mut broadcast::Receiver<WsMessage>) -> Vec<WsMessage> {
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            out.push(msg);
        }
        out
    }

    #[test]
    fn test_subscribe_snapshot_then_delta_ordering() {
        let manager = OrderbookSubscriptionManager::new();
        let mut rx = manager.subscribe();
        // A resting sell at 50_100 qty 8 → the aggregate reflects it at seq 1.
        assert_eq!(
            manager.on_committed_event(&resting_add(0, "m1", SeamSide::Sell, 50_100, 8)),
            Some(1)
        );
        // The snapshot reflects the resting ask at the baseline sequence.
        let snapshot = manager.orderbook_snapshot(&sym(), None);
        match snapshot {
            WsMessage::OrderbookSnapshot {
                sequence,
                asks,
                bids,
                ..
            } => {
                assert_eq!(sequence, 1);
                assert_eq!(
                    asks,
                    vec![PriceLevelData {
                        price: Cents::new(50_100),
                        quantity: 8
                    }]
                );
                assert!(bids.is_empty());
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }
        // A second resting sell emits a strictly-increasing delta.
        assert_eq!(
            manager.on_committed_event(&resting_add(1, "m2", SeamSide::Sell, 50_200, 3)),
            Some(2)
        );
        let msgs = drain(&mut rx);
        let deltas: Vec<u64> = msgs
            .iter()
            .filter_map(|m| match m {
                WsMessage::OrderbookDelta { sequence, .. } => Some(*sequence),
                _ => None,
            })
            .collect();
        assert_eq!(deltas, vec![1, 2], "deltas are strictly increasing from 1");
    }

    #[test]
    fn test_fill_print_is_anonymised() {
        let manager = OrderbookSubscriptionManager::new();
        let mut rx = manager.subscribe();
        // Rest a maker, then cross it — the crossing emits fill + trade prints.
        manager.on_committed_event(&resting_add(0, "maker-1", SeamSide::Sell, 50_000, 2));
        manager.on_committed_event(&crossing_buy(1, 50_000, 2));
        let msgs = drain(&mut rx);
        let fills: Vec<&WsMessage> = msgs
            .iter()
            .filter(|m| matches!(m, WsMessage::Fill { .. }))
            .collect();
        assert_eq!(fills.len(), 2, "one match, two anonymised fill legs");
        for fill in fills {
            let value = match serde_json::to_value(fill) {
                Ok(v) => v,
                Err(e) => panic!("serialise failed: {e}"),
            };
            // The public print omits account-scoped detail…
            assert!(value["data"].get("account").is_none());
            assert!(value["data"].get("fee").is_none());
            // …and carries the four join keys.
            assert!(value["data"]["execution_id"].is_string());
            assert!(value["data"]["underlying_sequence"].is_u64());
            assert!(value["data"]["venue_ts"].is_u64());
            assert!(value["data"]["liquidity"].is_string());
        }
        // A public trade print pairs the two legs.
        let trades = msgs
            .iter()
            .filter(|m| matches!(m, WsMessage::Trade { .. }))
            .count();
        assert_eq!(trades, 1, "one public trade print per match");
    }

    #[test]
    fn test_idempotent_duplicate_republishes_nothing() {
        // A #099 idempotent retry surfaces `VenueOutcome::Duplicate`; the WS fan-out
        // MUST republish NOTHING — no orderbook delta (no phantom depth), no fill
        // print, no trade print. The original placement's prints fired at first
        // placement; replaying them here would double-print.
        let manager = OrderbookSubscriptionManager::new();
        let mut rx = manager.subscribe();

        // A first crossing add prints a fill + trade and moves the book.
        manager.on_committed_event(&resting_add(0, "maker-1", SeamSide::Sell, 50_000, 2));
        let _ = drain(&mut rx);

        // The retry event carries the same AddOrder command with a `Duplicate` outcome.
        let original = crossing_buy(1, 50_000, 2);
        let duplicate = VenueEvent::new(
            SequenceNumber::new(2),
            EventTimestamp::new(1_700_000_000_000),
            original.command.clone(),
            VenueOutcome::Duplicate {
                original_order_id: VenueOrderId::new("taker-1"),
                original_sequence: SequenceNumber::new(1),
                terminal: Box::new(original.outcome.clone()),
            },
        );
        assert_eq!(
            manager.on_committed_event(&duplicate),
            None,
            "a Duplicate emits no orderbook delta sequence"
        );
        let republished = drain(&mut rx)
            .into_iter()
            .filter(|m| {
                matches!(
                    m,
                    WsMessage::OrderbookDelta { .. }
                        | WsMessage::Fill { .. }
                        | WsMessage::Trade { .. }
                )
            })
            .count();
        assert_eq!(
            republished, 0,
            "a Duplicate republishes no delta, fill, or trade"
        );
    }

    #[test]
    fn test_control_event_emits_no_orderbook_delta() {
        // A control-plane change (market-maker requote knobs) is never a book
        // delta — only user-driven mutations are. (When #015 emits requote adds,
        // they are filtered by the MM account; documented seam.)
        let manager = OrderbookSubscriptionManager::new();
        let mut rx = manager.subscribe();
        assert_eq!(manager.on_committed_event(&control_event(0)), None);
        let deltas = drain(&mut rx)
            .into_iter()
            .filter(|m| matches!(m, WsMessage::OrderbookDelta { .. }))
            .count();
        assert_eq!(deltas, 0, "a control event emits no orderbook delta");
    }

    #[test]
    fn test_kill_control_swept_removes_the_resting_level_from_the_aggregate() {
        // A kill couples the owner-scoped MM sweep into the control's own turn:
        // `ControlApplied.swept` carries the cancelled MM quote, which the WS
        // per-instrument aggregate must remove exactly like a `MassCancelled` leg,
        // emitting an orderbook-removal delta (#117). Asserted at the
        // `InstrumentState::apply` seam the change lives on (a symbol-less control is
        // routed to `apply` only where a swept leg names an instrument).
        let mut state = InstrumentState::new();
        // Rest an MM ask at 50_100 qty 8 → the aggregate reflects it.
        let add = VenueCommand::AddOrder {
            symbol: sym(),
            order_id: VenueOrderId::new("mm-1"),
            account: AccountId::new("mm"),
            owner: Hash32([0xEE; 32]),
            client_order_id: None,
            side: SeamSide::Sell,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(50_100)),
            quantity: 8,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        };
        state.apply(
            &add,
            &VenueOutcome::Added {
                fills: vec![],
                resting_quantity: 8,
                stp_cancelled: vec![],
            },
        );
        assert_eq!(state.qty_at(BookSide::Ask, 50_100), 8, "the MM ask rests");

        // A kill: the coupled owner-scoped sweep on `ControlApplied.swept` removes it.
        let kill = VenueCommand::MarketMakerControl {
            spread_multiplier: None,
            size_scalar: None,
            directional_skew: None,
            enabled: Some(false),
        };
        let changes = state.apply(
            &kill,
            &VenueOutcome::ControlApplied {
                swept: vec![CancelledLeg {
                    order_id: VenueOrderId::new("mm-1"),
                    owner: Hash32([0xEE; 32]),
                    symbol: sym(),
                    side: SeamSide::Sell,
                    reason: CancelReason::MassCancel,
                }],
            },
        );
        assert_eq!(
            state.qty_at(BookSide::Ask, 50_100),
            0,
            "the swept MM level is removed"
        );
        assert!(
            changes.iter().any(|change| change.side == BookSide::Ask
                && change.price == Cents::new(50_100)
                && change.quantity == 0),
            "a removal delta is emitted for the swept level"
        );
    }

    #[test]
    fn test_non_kill_control_swept_is_empty_and_emits_no_delta() {
        // A non-kill control (a spread knob) carries an empty `swept`, so `apply`
        // returns no change — the aggregate is untouched.
        let mut state = InstrumentState::new();
        let control = VenueCommand::MarketMakerControl {
            spread_multiplier: Some(1.5),
            size_scalar: None,
            directional_skew: None,
            enabled: None,
        };
        let changes = state.apply(&control, &VenueOutcome::ControlApplied { swept: vec![] });
        assert!(changes.is_empty(), "an empty-swept control emits no delta");
    }

    #[test]
    fn test_cancel_removes_the_level() {
        let manager = OrderbookSubscriptionManager::new();
        // Rest, then cancel — the level returns to zero (removed).
        manager.on_committed_event(&resting_add(0, "m1", SeamSide::Buy, 49_900, 12));
        let cancel = VenueEvent::new(
            SequenceNumber::new(1),
            EventTimestamp::new(1),
            VenueCommand::CancelOrder {
                symbol: sym(),
                order_id: VenueOrderId::new("m1"),
                account: AccountId::new("acct"),
            },
            VenueOutcome::Cancelled {
                order_id: VenueOrderId::new("m1"),
            },
        );
        manager.on_committed_event(&cancel);
        match manager.orderbook_snapshot(&sym(), None) {
            WsMessage::OrderbookSnapshot { bids, sequence, .. } => {
                assert!(bids.is_empty(), "the cancelled level is gone");
                assert_eq!(sequence, 2);
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_price_channel_from_sim_step() {
        let manager = OrderbookSubscriptionManager::new();
        let mut rx = manager.subscribe();
        let event = VenueEvent::new(
            SequenceNumber::new(0),
            EventTimestamp::new(1),
            VenueCommand::SimStep {
                now_ms: EventTimestamp::new(1),
                underlying: "BTC".to_string(),
                price: Cents::new(4_200_000),
                bid: None,
                ask: None,
            },
            VenueOutcome::ControlApplied { swept: vec![] },
        );
        // A `SimStep` has no contract symbol → no orderbook delta, but it feeds
        // the `prices` channel with the committed override.
        assert_eq!(manager.on_committed_event(&event), None);
        let msgs = drain(&mut rx);
        assert!(msgs.iter().any(|m| matches!(
            m,
            WsMessage::Price { symbol, price_cents }
                if symbol == "BTC" && *price_cents == Cents::new(4_200_000)
        )));
    }

    #[test]
    fn test_laggard_receiver_reports_lagged_then_re_snapshots() {
        // A bounded broadcast: a consumer that does not drain lags rather than
        // stalling the producer; the manager's snapshot is the recovery.
        let manager = OrderbookSubscriptionManager::with_capacity(2);
        let mut rx = manager.subscribe();
        for i in 0..6u64 {
            manager.on_committed_event(&resting_add(
                i,
                &format!("m{i}"),
                SeamSide::Sell,
                50_000 + i,
                1,
            ));
        }
        // The receiver is behind the bounded ring → the next recv reports Lagged.
        let mut saw_lagged = false;
        loop {
            match rx.try_recv() {
                Ok(_) => {}
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    saw_lagged = true;
                    break;
                }
                Err(_) => break,
            }
        }
        assert!(saw_lagged, "a slow consumer lags on a bounded broadcast");
        // The recovery is a fresh snapshot reflecting every folded mutation.
        match manager.orderbook_snapshot(&sym(), None) {
            WsMessage::OrderbookSnapshot { asks, sequence, .. } => {
                assert_eq!(asks.len(), 6, "all six resting levels are in the snapshot");
                assert_eq!(sequence, 6);
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_connection_cap_refuses_at_the_ceiling() {
        // Two connection slots: the third acquire is refused (the handshake would
        // return 503). A released permit frees its slot again.
        let manager = OrderbookSubscriptionManager::with_limits(WS_BROADCAST_CAPACITY, 2);
        assert_eq!(manager.available_connection_slots(), 2);
        let a = manager.try_acquire_connection().expect("slot 1");
        let b = manager.try_acquire_connection().expect("slot 2");
        assert_eq!(manager.available_connection_slots(), 0);
        assert!(
            manager.try_acquire_connection().is_none(),
            "at the ceiling the next connection is refused"
        );
        // Releasing one permit reclaims its slot.
        drop(a);
        assert_eq!(manager.available_connection_slots(), 1);
        assert!(manager.try_acquire_connection().is_some());
        drop(b);
    }
}
