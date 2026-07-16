//! The [`MarketMakerEngine`] — the price → requote pipeline that routes every
//! generated quote onto the **sequenced order path** as a journaled
//! [`VenueCommand`], the kill switch, the range-validated persona knobs, the
//! edge calc, and the [`MarketMakerEvent`] broadcast
//! ([015](../../milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md)).
//!
//! ## Requotes are journaled orders (rule 3, [02 §4](../../docs/02-matching-architecture.md))
//!
//! A price update triggers a requote; a requote cancels the stale two-sided quote
//! and adds a fresh one **through the [`CommandSink`]** — never by touching a
//! book. Each command carries the venue-reserved market-maker identity
//! ([`market_maker_account`] / [`MARKET_MAKER_OWNER`]) so fills attribute to the
//! maker and the WS manager can suppress the requote's `orderbook_delta`. Because
//! the commands enter the same actor + journal as client orders, generated
//! liquidity is part of the determinism oracle.
//!
//! ## Off the client path (rule 8)
//!
//! [`MarketMakerEngine::update_price`] is synchronous and **non-blocking**: it
//! stores the price, broadcasts, and hands the requote's commands to the sink,
//! which enqueues them without awaiting a receipt. No lock guard is ever held
//! across a sink enqueue or a broadcast send.
//!
//! ## Determinism + the replay-mute hook (rule 3/5)
//!
//! Time-to-expiry is derived from the **venue clock** ([`set_venue_now_ms`]),
//! never the wall clock, so `generate_quote` stays deterministic. On replay the
//! journaled requotes are replayed by the driver, so the live engine is **muted**
//! ([`set_muted`]) to avoid cascading duplicate orders — the hook is implemented
//! now even though the replay driver lands in v0.3.
//!
//! [`set_venue_now_ms`]: MarketMakerEngine::set_venue_now_ms
//! [`set_muted`]: MarketMakerEngine::set_muted

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use tokio::sync::broadcast;

use crate::exchange::{
    Cents, ExpirationDate, LineageId, MARKET_MAKER_OWNER, OptionStyle, STPMode, Side, Symbol,
    SymbolParser, TimeInForce, VenueCommand, market_maker_account,
};
use crate::market_maker::config::{
    DIRECTIONAL_SKEW_MAX, DIRECTIONAL_SKEW_MIN, MarketMakerConfig, MarketMakerEvent,
    SIZE_SCALAR_MAX, SIZE_SCALAR_MIN, SPREAD_MULTIPLIER_MAX, SPREAD_MULTIPLIER_MIN,
    validate_control_value,
};
use crate::market_maker::quoter::{QuoteInput, Quoter};
use crate::market_maker::sink::CommandSink;
use crate::models::{ExecutionId, OrderType, VenueOrderId};

/// The bounded capacity of the market-maker event broadcast — a DoS control,
/// never unbounded (rule 7). A slow subscriber lags and re-subscribes; the
/// producer sends and continues.
pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 1_024;

/// Milliseconds in a day, for the venue-clock time-to-expiry conversion.
const MILLIS_PER_DAY: f64 = 86_400_000.0;

/// A contract the market maker quotes, pre-parsed from its canonical [`Symbol`]
/// so the requote loop borrows rather than re-parses (rule 8).
#[derive(Debug, Clone)]
struct QuotableInstrument {
    /// The canonical contract symbol (the book key and command target).
    symbol: Symbol,
    /// The absolute contract expiry (a canonical `DateTime`), used with the
    /// venue clock to derive a deterministic relative time-to-expiry.
    expiration: ExpirationDate,
    /// The strike in **cents** (whole-unit strike × 100).
    strike_cents: u64,
    /// Call or put.
    style: OptionStyle,
}

/// A resting market-maker quote leg, tracked for cancel-on-requote and for the
/// edge calc when it fills.
#[derive(Debug, Clone)]
struct RestingQuote {
    /// The underlying ticker (for `cancel_symbol_orders`).
    underlying: String,
    /// The canonical contract symbol (the cancel target).
    symbol: Symbol,
    /// True for the bid (buy) leg, false for the ask (sell) leg.
    is_buy: bool,
    /// The quote-time theoretical value in **cents**, for the edge calc.
    theo_cents: u64,
    /// The remaining resting quantity in **contracts**.
    quantity: u64,
}

/// The two resting leg ids of one instrument: slot 0 is the bid, slot 1 the ask.
type LegSlots = [Option<VenueOrderId>; 2];

/// Maps a leg to its slot index (bid = 0, ask = 1).
#[inline]
const fn leg_slot(is_buy: bool) -> usize {
    if is_buy { 0 } else { 1 }
}

/// The market-maker engine: drives quoting across registered instruments and
/// routes every quote onto the sequenced order path.
pub struct MarketMakerEngine {
    /// The sequenced-path sink every requote command flows through.
    sink: Arc<dyn CommandSink>,
    /// The run lineage that namespaces the market-maker's minted order ids.
    lineage_id: LineageId,
    /// The quoter (holds the `optionstratlib` pricer).
    quoter: Quoter,
    // ---- shared state: deliberate coarse `std::sync::RwLock`, not `DashMap` ----
    //
    // The rest of the venue uses `DashMap` for sharded, lock-free per-key access
    // on the hot path. The market maker deliberately does NOT, for two reasons:
    //
    // (a) **Atomic multi-map consistency.** `cancel_symbol_orders` mutates the
    //     `resting` AND `legs` maps together under BOTH guards held at once, so a
    //     concurrent requote never observes a half-updated pair (a leg tracked in
    //     one map but not the other). `DashMap`'s per-key sharded locks cannot
    //     give a cross-map atomic section without holding several shard guards
    //     across a loop — the classic `DashMap` deadlock foot-gun. A single
    //     `RwLock` per map, acquired in a fixed order and never held across a sink
    //     enqueue or a broadcast, is the safe, simple choice.
    // (b) **Off the client hot path.** Requotes run on the market-maker task, not
    //     a client order path, so the coarser lock is never on a latency-critical
    //     section (rule 8). No `parking_lot` is pulled in (an unapproved new dep);
    //     `std::sync` poison is recovered via `PoisonError::into_inner`, never a
    //     panic.
    //
    /// The persona-substrate configuration (kill switch + range-validated knobs).
    config: RwLock<MarketMakerConfig>,
    /// Latest underlying prices (underlying ticker → **cents**).
    prices: RwLock<HashMap<String, u64>>,
    /// Registered quotable instruments (underlying ticker → contracts).
    instruments: RwLock<HashMap<String, Vec<QuotableInstrument>>>,
    /// Resting leg ids per instrument, for replace-not-accumulate on requote.
    /// Mutated together with `resting` under both guards in `cancel_symbol_orders`.
    legs: RwLock<HashMap<Symbol, LegSlots>>,
    /// Resting-quote metadata by order id, for the fill edge calc.
    resting: RwLock<HashMap<VenueOrderId, RestingQuote>>,
    /// Monotonic counter minting unique market-maker order ids.
    next_order_seq: AtomicU64,
    /// The venue-clock instant (ms) used to derive time-to-expiry.
    venue_now_ms: AtomicU64,
    /// The replay-mute flag: when set, price updates never cascade a requote.
    muted: AtomicBool,
    /// The bounded event broadcast.
    event_tx: broadcast::Sender<MarketMakerEvent>,
}

impl MarketMakerEngine {
    /// Builds an engine over `sink`, the run `lineage_id`, and `quoter`.
    #[must_use]
    pub fn new(sink: Arc<dyn CommandSink>, lineage_id: LineageId, quoter: Quoter) -> Self {
        let (event_tx, _) = broadcast::channel(DEFAULT_EVENT_CHANNEL_CAPACITY);
        Self {
            sink,
            lineage_id,
            quoter,
            config: RwLock::new(MarketMakerConfig::default()),
            prices: RwLock::new(HashMap::new()),
            instruments: RwLock::new(HashMap::new()),
            legs: RwLock::new(HashMap::new()),
            resting: RwLock::new(HashMap::new()),
            next_order_seq: AtomicU64::new(0),
            venue_now_ms: AtomicU64::new(0),
            muted: AtomicBool::new(false),
            event_tx,
        }
    }

    /// Subscribes to the engine's bounded event broadcast.
    #[must_use]
    #[inline]
    pub fn subscribe(&self) -> broadcast::Receiver<MarketMakerEvent> {
        self.event_tx.subscribe()
    }

    /// Sets the venue-clock instant (**ms**) used to derive time-to-expiry — a
    /// venue service, never the wall clock (rule 3). The simulation clock (#016)
    /// drives this.
    #[inline]
    pub fn set_venue_now_ms(&self, now_ms: u64) {
        self.venue_now_ms.store(now_ms, Ordering::Relaxed);
    }

    /// The replay-mute hook: when muted, a price update never cascades a live
    /// requote, so the replay driver's journaled requotes are not duplicated
    /// (rule 3). Idempotent.
    #[inline]
    pub fn set_muted(&self, muted: bool) {
        self.muted.store(muted, Ordering::Relaxed);
    }

    /// Whether the engine is muted for replay.
    #[must_use]
    #[inline]
    pub fn is_muted(&self) -> bool {
        self.muted.load(Ordering::Relaxed)
    }

    /// Registers a contract the maker will quote. Idempotent per symbol.
    ///
    /// The symbol is pre-parsed for its underlying, absolute expiry, strike, and
    /// style; a strike is stored in **cents** (whole-unit strike × 100).
    pub fn register_instrument(&self, symbol: &Symbol) {
        let Ok(parsed) = SymbolParser::parse(symbol.as_str()) else {
            // A `Symbol` is already validated, so this is unreachable; fail safe.
            tracing::error!(
                symbol = symbol.as_str(),
                "could not parse a validated symbol"
            );
            return;
        };
        let underlying = parsed.underlying().to_string();
        let strike_cents = parsed.strike().saturating_mul(100);
        let instrument = QuotableInstrument {
            symbol: symbol.clone(),
            expiration: *parsed.expiration(),
            strike_cents,
            style: parsed.option_style(),
        };
        let mut instruments = self
            .instruments
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let list = instruments.entry(underlying).or_default();
        if list.iter().any(|i| i.symbol == instrument.symbol) {
            return;
        }
        list.push(instrument);
    }

    /// The number of registered quotable contracts for `underlying`.
    #[must_use]
    pub fn registered_count(&self, underlying: &str) -> usize {
        self.instruments
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(underlying)
            .map_or(0, Vec::len)
    }

    // ---- prices + requote ------------------------------------------------

    /// Updates an underlying's price and, unless muted or disabled, requotes it.
    ///
    /// Synchronous and non-blocking (rule 8): stores the price, broadcasts, and
    /// hands the requote's commands to the sink. On the muted replay path the
    /// price is recorded and broadcast but no live requote is cascaded (rule 3).
    pub fn update_price(&self, underlying: &str, price_cents: u64) {
        self.prices
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(underlying.to_string(), price_cents);

        let _ = self.event_tx.send(MarketMakerEvent::PriceUpdated {
            symbol: underlying.to_string(),
            price_cents,
        });

        if self.is_muted() {
            return;
        }
        if self.is_enabled() && self.is_symbol_enabled(underlying) {
            self.requote_symbol(underlying);
        }
    }

    /// The latest price for `underlying`, in **cents**.
    #[must_use]
    pub fn get_price(&self, underlying: &str) -> Option<u64> {
        self.prices
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(underlying)
            .copied()
    }

    /// Requotes every registered instrument of `underlying` at its latest price.
    fn requote_symbol(&self, underlying: &str) {
        let Some(price_cents) = self.get_price(underlying) else {
            tracing::warn!(underlying, "no price available; skipping requote");
            return;
        };
        let config = self.get_config();
        let instruments = self
            .instruments
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(underlying)
            .cloned()
            .unwrap_or_default();
        for instrument in &instruments {
            self.update_quote(underlying, instrument, price_cents, &config);
        }
    }

    /// Requotes every priced, enabled underlying (after a knob change).
    fn requote_all(&self) {
        if !self.is_enabled() {
            return;
        }
        let underlyings: Vec<String> = self
            .prices
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect();
        for underlying in underlyings {
            if self.is_symbol_enabled(&underlying) {
                self.requote_symbol(&underlying);
            }
        }
    }

    /// Generates and routes a fresh two-sided quote for one instrument, cancelling
    /// the prior legs (replace-not-accumulate). No lock is held across a sink
    /// enqueue or a broadcast send.
    fn update_quote(
        &self,
        underlying: &str,
        instrument: &QuotableInstrument,
        spot_cents: u64,
        config: &MarketMakerConfig,
    ) {
        let Some(days_to_expiry) = self.days_to_expiry(&instrument.expiration) else {
            return;
        };
        let input = QuoteInput {
            spot_cents,
            strike_cents: instrument.strike_cents,
            days_to_expiry,
            style: instrument.style,
            spread_multiplier: config.spread_multiplier,
            size_scalar: config.size_scalar,
            directional_skew: config.directional_skew,
            iv: None,
        };
        let Some(params) = self.quoter.generate_quote(&input) else {
            tracing::warn!(
                symbol = instrument.symbol.as_str(),
                "skipping quote: non-finite theoretical value"
            );
            return;
        };

        // Take the prior legs and drop them from tracking (locks released before
        // any enqueue/broadcast).
        let prior = self
            .legs
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&instrument.symbol)
            .unwrap_or([None, None]);
        {
            let mut resting = self.resting.write().unwrap_or_else(PoisonError::into_inner);
            for id in prior.iter().flatten() {
                resting.remove(id);
            }
        }

        // Mint fresh leg ids and register them before enqueuing, so a later fill
        // notification resolves against the tracked quote.
        let bid_id = self.mint_order_id();
        let ask_id = self.mint_order_id();
        {
            let mut resting = self.resting.write().unwrap_or_else(PoisonError::into_inner);
            resting.insert(
                bid_id.clone(),
                RestingQuote {
                    underlying: underlying.to_string(),
                    symbol: instrument.symbol.clone(),
                    is_buy: true,
                    theo_cents: params.theo_price.get(),
                    quantity: params.bid_size,
                },
            );
            resting.insert(
                ask_id.clone(),
                RestingQuote {
                    underlying: underlying.to_string(),
                    symbol: instrument.symbol.clone(),
                    is_buy: false,
                    theo_cents: params.theo_price.get(),
                    quantity: params.ask_size,
                },
            );
        }
        self.legs
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                instrument.symbol.clone(),
                [Some(bid_id.clone()), Some(ask_id.clone())],
            );

        // Route: cancel the prior legs, then add the fresh bid/ask — in that order.
        for id in prior.into_iter().flatten() {
            self.sink.enqueue(VenueCommand::CancelOrder {
                symbol: instrument.symbol.clone(),
                order_id: id,
                account: market_maker_account(),
            });
        }
        self.sink.enqueue(self.add_command(
            &instrument.symbol,
            bid_id,
            Side::Buy,
            params.bid_price,
            params.bid_size,
        ));
        self.sink.enqueue(self.add_command(
            &instrument.symbol,
            ask_id,
            Side::Sell,
            params.ask_price,
            params.ask_size,
        ));

        let _ = self.event_tx.send(MarketMakerEvent::QuoteUpdated {
            symbol: instrument.symbol.as_str().to_string(),
            strike_cents: instrument.strike_cents,
            style: style_label(instrument.style),
            bid_price: params.bid_price,
            ask_price: params.ask_price,
            bid_size: params.bid_size,
            ask_size: params.ask_size,
        });
    }

    /// Builds a market-maker limit `AddOrder`, tagged with the reserved MM
    /// identity so fills attribute to the maker and the WS requote-no-delta rule
    /// keys on it.
    #[must_use]
    fn add_command(
        &self,
        symbol: &Symbol,
        order_id: VenueOrderId,
        side: Side,
        price: Cents,
        quantity: u64,
    ) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: symbol.clone(),
            order_id,
            account: market_maker_account(),
            owner: MARKET_MAKER_OWNER,
            client_order_id: None,
            side,
            order_type: OrderType::Limit,
            limit_price: Some(price),
            quantity,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        }
    }

    /// Mints a unique market-maker order id in the run lineage namespace.
    #[must_use]
    fn mint_order_id(&self) -> VenueOrderId {
        let seq = self.next_order_seq.fetch_add(1, Ordering::Relaxed);
        VenueOrderId::new(format!("{}:MM:{seq}", self.lineage_id.as_str()))
    }

    /// The relative time-to-expiry in **days**, derived from the venue clock —
    /// never the wall clock (rule 3). `None` for an expired or degenerate expiry.
    #[must_use]
    fn days_to_expiry(&self, expiration: &ExpirationDate) -> Option<f64> {
        let now_ms = self.venue_now_ms.load(Ordering::Relaxed);
        match expiration {
            ExpirationDate::DateTime(dt) => {
                let expiry_ms = dt.timestamp_millis();
                let remaining_ms = expiry_ms.checked_sub(i64::try_from(now_ms).ok()?)?;
                if remaining_ms <= 0 {
                    None
                } else {
                    Some(remaining_ms as f64 / MILLIS_PER_DAY)
                }
            }
            // Defensive: the venue only ever registers absolute `DateTime` expiries.
            ExpirationDate::Days(days) => {
                let value = days.to_f64();
                if value > 0.0 { Some(value) } else { None }
            }
        }
    }

    // ---- kill switch + range-validated knobs -----------------------------

    /// Whether quoting is globally enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .enabled
    }

    /// Whether quoting is enabled for `underlying` (default: enabled).
    #[must_use]
    pub fn is_symbol_enabled(&self, underlying: &str) -> bool {
        self.config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .symbol_enabled
            .get(underlying)
            .copied()
            .unwrap_or(true)
    }

    /// A snapshot of the current configuration.
    #[must_use]
    pub fn get_config(&self) -> MarketMakerConfig {
        self.config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Enables or disables quoting globally (the kill switch). Disabling cancels
    /// every resting quote; either way a `ConfigChanged` event is broadcast.
    pub fn set_enabled(&self, enabled: bool) {
        self.config
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .enabled = enabled;
        if !enabled {
            self.cancel_all_orders();
        }
        self.broadcast_config_change();
    }

    /// Enables or disables quoting for one `underlying`. Disabling cancels its
    /// resting quotes.
    pub fn set_symbol_enabled(&self, underlying: &str, enabled: bool) {
        self.config
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .symbol_enabled
            .insert(underlying.to_string(), enabled);
        if !enabled {
            self.cancel_symbol_orders(underlying);
        }
    }

    /// Sets the spread multiplier, rejecting `NaN`/`±Inf` and out-of-range values
    /// (rule 4). On success the config changes, a `ConfigChanged` is broadcast,
    /// and every priced underlying is requoted.
    ///
    /// # Errors
    ///
    /// Returns a client-safe message if `multiplier` is non-finite or outside
    /// `[0.1, 10.0]` (the boundary maps it to a `400`).
    pub fn set_spread_multiplier(&self, multiplier: f64) -> Result<(), String> {
        let value = validate_control_value(
            "spread_multiplier",
            multiplier,
            SPREAD_MULTIPLIER_MIN,
            SPREAD_MULTIPLIER_MAX,
        )?;
        self.config
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .spread_multiplier = value;
        self.broadcast_config_change();
        self.requote_all();
        Ok(())
    }

    /// Sets the size scalar, rejecting `NaN`/`±Inf` and out-of-range values
    /// (rule 4). Ends in a `ConfigChanged` broadcast and a requote.
    ///
    /// # Errors
    ///
    /// Returns a client-safe message if `scalar` is non-finite or outside
    /// `[0.0, 1.0]`.
    pub fn set_size_scalar(&self, scalar: f64) -> Result<(), String> {
        let value =
            validate_control_value("size_scalar", scalar, SIZE_SCALAR_MIN, SIZE_SCALAR_MAX)?;
        self.config
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .size_scalar = value;
        self.broadcast_config_change();
        self.requote_all();
        Ok(())
    }

    /// Sets the directional skew, rejecting `NaN`/`±Inf` and out-of-range values
    /// (rule 4). Ends in a `ConfigChanged` broadcast and a requote.
    ///
    /// # Errors
    ///
    /// Returns a client-safe message if `skew` is non-finite or outside
    /// `[-1.0, 1.0]`.
    pub fn set_directional_skew(&self, skew: f64) -> Result<(), String> {
        let value = validate_control_value(
            "directional_skew",
            skew,
            DIRECTIONAL_SKEW_MIN,
            DIRECTIONAL_SKEW_MAX,
        )?;
        self.config
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .directional_skew = value;
        self.broadcast_config_change();
        self.requote_all();
        Ok(())
    }

    /// Cancels every resting market-maker quote (routed as `CancelOrder`
    /// commands, in a deterministic order).
    pub fn cancel_all_orders(&self) {
        let mut orders: Vec<(Symbol, VenueOrderId)> = self
            .resting
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(id, quote)| (quote.symbol.clone(), id.clone()))
            .collect();
        // Deterministic sweep order regardless of map iteration order.
        orders.sort_by(|a, b| a.1.as_str().cmp(b.1.as_str()));

        self.resting
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.legs
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();

        for (symbol, order_id) in orders {
            self.sink.enqueue(VenueCommand::CancelOrder {
                symbol,
                order_id,
                account: market_maker_account(),
            });
        }
    }

    /// Cancels every resting quote for `underlying`.
    pub fn cancel_symbol_orders(&self, underlying: &str) {
        let mut orders: Vec<(Symbol, VenueOrderId)> = self
            .resting
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(_, quote)| quote.underlying == underlying)
            .map(|(id, quote)| (quote.symbol.clone(), id.clone()))
            .collect();
        orders.sort_by(|a, b| a.1.as_str().cmp(b.1.as_str()));

        {
            let mut resting = self.resting.write().unwrap_or_else(PoisonError::into_inner);
            let mut legs = self.legs.write().unwrap_or_else(PoisonError::into_inner);
            for (symbol, order_id) in &orders {
                resting.remove(order_id);
                legs.remove(symbol);
            }
        }

        for (symbol, order_id) in orders {
            self.sink.enqueue(VenueCommand::CancelOrder {
                symbol,
                order_id,
                account: market_maker_account(),
            });
        }
    }

    /// Notifies the engine that one of its resting quotes was (partially) filled.
    ///
    /// If `order_id` is a tracked market-maker leg, the captured edge is computed
    /// against the quote-time theoretical value ([`Quoter::calculate_edge`]), the
    /// tracked quantity is reduced (removed once fully filled), and an
    /// [`MarketMakerEvent::OrderFilled`] is broadcast. Ids the maker does not own
    /// are ignored. No lock is held across the broadcast.
    pub fn on_order_filled(
        &self,
        order_id: &VenueOrderId,
        execution_id: Option<ExecutionId>,
        fill_price_cents: u64,
        quantity: u64,
    ) {
        if quantity == 0 {
            return;
        }

        let (quote, reported, fully_filled) = {
            let mut resting = self.resting.write().unwrap_or_else(PoisonError::into_inner);
            let Some(remaining) = resting.get(order_id).map(|q| q.quantity) else {
                return;
            };
            let reported = quantity.min(remaining);
            if quantity >= remaining {
                let Some(quote) = resting.remove(order_id) else {
                    return;
                };
                (quote, reported, true)
            } else {
                let Some(quote) = resting.get_mut(order_id) else {
                    return;
                };
                // Guarded: quantity < remaining here.
                quote.quantity -= quantity;
                (quote.clone(), reported, false)
            }
        };

        if fully_filled {
            let mut legs = self.legs.write().unwrap_or_else(PoisonError::into_inner);
            if let Some(slots) = legs.get_mut(&quote.symbol) {
                let slot = leg_slot(quote.is_buy);
                if slots[slot].as_ref() == Some(order_id) {
                    slots[slot] = None;
                }
                if slots[0].is_none() && slots[1].is_none() {
                    legs.remove(&quote.symbol);
                }
            }
        }

        let edge = Quoter::calculate_edge(fill_price_cents, quote.theo_cents, quote.is_buy);
        let _ = self.event_tx.send(MarketMakerEvent::OrderFilled {
            order_id: order_id.clone(),
            execution_id,
            symbol: quote.symbol.as_str().to_string(),
            side: if quote.is_buy { "buy" } else { "sell" }.to_string(),
            quantity: reported,
            price: Cents::new(fill_price_cents),
            edge,
        });
    }

    /// Broadcasts the current configuration.
    fn broadcast_config_change(&self) {
        let config = self.get_config();
        let _ = self.event_tx.send(MarketMakerEvent::ConfigChanged {
            enabled: config.enabled,
            spread_multiplier: config.spread_multiplier,
            size_scalar: config.size_scalar,
            directional_skew: config.directional_skew,
        });
    }
}

/// The lowercase style label for a `QuoteUpdated` event.
#[must_use]
#[inline]
fn style_label(style: OptionStyle) -> String {
    match style {
        OptionStyle::Call => "call".to_string(),
        OptionStyle::Put => "put".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A [`CommandSink`] that records the commands enqueued to it, in order, for
    /// assertions — synchronous, non-blocking, deterministic.
    #[derive(Default)]
    struct CollectingSink {
        commands: Mutex<Vec<VenueCommand>>,
    }

    impl CollectingSink {
        fn drain(&self) -> Vec<VenueCommand> {
            std::mem::take(&mut self.commands.lock().unwrap_or_else(PoisonError::into_inner))
        }
    }

    impl CommandSink for CollectingSink {
        fn enqueue(&self, command: VenueCommand) {
            self.commands
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(command);
        }
    }

    const BTC_CALL: &str = "BTC-20351231-50000-C";

    fn sym(raw: &str) -> Symbol {
        Symbol::parse(raw).expect("valid fixture symbol")
    }

    /// An engine whose venue clock is set well before a far-future expiry, so
    /// time-to-expiry is a healthy positive number.
    fn engine() -> (MarketMakerEngine, Arc<CollectingSink>) {
        let sink = Arc::new(CollectingSink::default());
        let engine =
            MarketMakerEngine::new(sink.clone(), LineageId::new("run-1"), Quoter::default());
        // 2025-01-01T00:00:00Z in ms, well before the 2035 expiry.
        engine.set_venue_now_ms(1_735_689_600_000);
        (engine, sink)
    }

    #[test]
    fn test_price_update_requotes_as_add_orders_on_the_sequenced_path() {
        let (engine, sink) = engine();
        engine.register_instrument(&sym(BTC_CALL));
        engine.update_price("BTC", 5_000_000);

        let commands = sink.drain();
        // First requote: no prior legs, so exactly two AddOrders (bid + ask).
        assert_eq!(
            commands.len(),
            2,
            "first requote adds bid + ask, no cancels"
        );
        let mut sides = Vec::new();
        for command in &commands {
            match command {
                VenueCommand::AddOrder {
                    account,
                    owner,
                    side,
                    order_type,
                    limit_price,
                    ..
                } => {
                    assert!(
                        crate::exchange::is_market_maker_account(account),
                        "requote must carry the reserved MM account"
                    );
                    assert_eq!(*owner, MARKET_MAKER_OWNER);
                    assert_eq!(*order_type, OrderType::Limit);
                    assert!(limit_price.is_some(), "a limit add carries a price");
                    sides.push(*side);
                }
                other => panic!("expected an AddOrder, got {other:?}"),
            }
        }
        assert!(sides.contains(&Side::Buy) && sides.contains(&Side::Sell));
    }

    #[test]
    fn test_second_requote_cancels_the_prior_legs_then_re_adds() {
        let (engine, sink) = engine();
        engine.register_instrument(&sym(BTC_CALL));
        engine.update_price("BTC", 5_000_000);
        let _ = sink.drain();

        engine.update_price("BTC", 5_050_000);
        let commands = sink.drain();
        // Replace, not accumulate: two cancels then two adds.
        let cancels = commands
            .iter()
            .filter(|c| matches!(c, VenueCommand::CancelOrder { .. }))
            .count();
        let adds = commands
            .iter()
            .filter(|c| matches!(c, VenueCommand::AddOrder { .. }))
            .count();
        assert_eq!(cancels, 2, "the prior bid + ask are cancelled");
        assert_eq!(adds, 2, "a fresh bid + ask are added");
        // Cancels precede adds in the enqueued order.
        let first_add = commands
            .iter()
            .position(|c| matches!(c, VenueCommand::AddOrder { .. }))
            .expect("an add exists");
        let last_cancel = commands
            .iter()
            .rposition(|c| matches!(c, VenueCommand::CancelOrder { .. }))
            .expect("a cancel exists");
        assert!(last_cancel < first_add, "cancels are enqueued before adds");
    }

    #[test]
    fn test_update_price_enqueues_not_blocks() {
        // The requote enqueues onto the sink and returns; it never blocks on a
        // receipt (the CollectingSink records synchronously and returns).
        let (engine, sink) = engine();
        engine.register_instrument(&sym(BTC_CALL));
        engine.register_instrument(&sym("BTC-20351231-60000-P"));
        engine.update_price("BTC", 5_000_000);
        // Both instruments were requoted without the call blocking: 2 legs each.
        assert_eq!(sink.drain().len(), 4);
    }

    #[test]
    fn test_muted_engine_does_not_requote() {
        let (engine, sink) = engine();
        engine.register_instrument(&sym(BTC_CALL));
        engine.set_muted(true);
        engine.update_price("BTC", 5_000_000);
        assert!(
            sink.drain().is_empty(),
            "a muted engine records the price but cascades no requote"
        );
        // Unmuting restores requoting.
        engine.set_muted(false);
        engine.update_price("BTC", 5_010_000);
        assert_eq!(sink.drain().len(), 2);
    }

    #[test]
    fn test_disabled_engine_does_not_requote() {
        let (engine, sink) = engine();
        engine.register_instrument(&sym(BTC_CALL));
        engine.set_enabled(false);
        let _ = sink.drain(); // discard the (empty) cancel sweep
        engine.update_price("BTC", 5_000_000);
        assert!(sink.drain().is_empty(), "a disabled engine does not quote");
    }

    #[test]
    fn test_kill_switch_cancels_resting_quotes() {
        let (engine, sink) = engine();
        engine.register_instrument(&sym(BTC_CALL));
        engine.update_price("BTC", 5_000_000);
        let _ = sink.drain();

        engine.set_enabled(false);
        let commands = sink.drain();
        let cancels = commands
            .iter()
            .filter(|c| matches!(c, VenueCommand::CancelOrder { .. }))
            .count();
        assert_eq!(cancels, 2, "the kill switch cancels the resting bid + ask");
    }

    #[test]
    fn test_clamp_rejects_nan_and_out_of_range() {
        let (engine, _sink) = engine();
        assert!(engine.set_spread_multiplier(f64::NAN).is_err());
        assert!(engine.set_spread_multiplier(f64::INFINITY).is_err());
        assert!(engine.set_spread_multiplier(0.05).is_err(), "below 0.1");
        assert!(engine.set_spread_multiplier(10.5).is_err(), "above 10.0");
        assert!(engine.set_size_scalar(-0.1).is_err());
        assert!(engine.set_size_scalar(1.1).is_err());
        assert!(engine.set_directional_skew(f64::NAN).is_err());
        assert!(engine.set_directional_skew(-1.5).is_err());
        assert!(engine.set_directional_skew(1.5).is_err());
        // A rejected control leaves the config untouched.
        assert_eq!(engine.get_config().spread_multiplier, 1.0);
    }

    #[test]
    fn test_clamp_change_broadcasts_and_requotes() {
        let (engine, sink) = engine();
        engine.register_instrument(&sym(BTC_CALL));
        engine.update_price("BTC", 5_000_000);
        let _ = sink.drain();
        let mut events = engine.subscribe();

        assert!(engine.set_spread_multiplier(2.0).is_ok());
        assert_eq!(engine.get_config().spread_multiplier, 2.0);
        // The clamp change ended in a requote (cancel + re-add) and a broadcast.
        assert!(!sink.drain().is_empty(), "a clamp change requotes");
        assert!(
            matches!(
                events.try_recv(),
                Ok(MarketMakerEvent::ConfigChanged { .. })
            ),
            "a clamp change broadcasts ConfigChanged"
        );
    }

    #[test]
    fn test_on_order_filled_edge_and_untrack() {
        let (engine, sink) = engine();
        engine.register_instrument(&sym(BTC_CALL));
        engine.update_price("BTC", 5_000_000);
        let commands = sink.drain();

        // Find the bid leg (buy) and its id + theo.
        let (bid_id, theo) = commands
            .iter()
            .find_map(|c| match c {
                VenueCommand::AddOrder {
                    order_id,
                    side: Side::Buy,
                    ..
                } => Some((order_id.clone(), engine.quote_theo(order_id))),
                _ => None,
            })
            .expect("a bid leg exists");
        let mut events = engine.subscribe();

        // Fill the bid one cent below theo → +1 edge, and it untracks.
        let fill = theo.saturating_sub(1);
        engine.on_order_filled(&bid_id, None, fill, 1_000_000);
        match events.try_recv() {
            Ok(MarketMakerEvent::OrderFilled {
                side, edge, price, ..
            }) => {
                assert_eq!(side, "buy");
                assert_eq!(edge, 1, "buy edge = theo - fill");
                assert_eq!(price, Cents::new(fill));
            }
            other => panic!("expected OrderFilled, got {other:?}"),
        }
    }

    #[test]
    fn test_on_order_filled_ignores_unknown_and_zero_quantity() {
        let (engine, _sink) = engine();
        let mut events = engine.subscribe();
        engine.on_order_filled(&VenueOrderId::new("nope"), None, 100, 1);
        engine.on_order_filled(&VenueOrderId::new("nope"), None, 100, 0);
        assert!(
            events.try_recv().is_err(),
            "no event for an unknown or zero-quantity fill"
        );
    }

    #[test]
    fn test_register_instrument_is_idempotent() {
        let (engine, _sink) = engine();
        engine.register_instrument(&sym(BTC_CALL));
        engine.register_instrument(&sym(BTC_CALL));
        assert_eq!(engine.registered_count("BTC"), 1);
    }

    // A tiny test-only accessor for the tracked theo of a resting leg.
    impl MarketMakerEngine {
        fn quote_theo(&self, order_id: &VenueOrderId) -> u64 {
            self.resting
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .get(order_id)
                .map_or(0, |q| q.theo_cents)
        }
    }
}
