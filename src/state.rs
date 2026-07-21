//! Application layer: [`AppState`], the shared `Arc` wiring of the domain
//! ([`crate::exchange`]) and services ([`crate::auth`]) layers that every
//! gateway handler receives. Persistence ([`crate::db`]) is not wired yet —
//! its optional backend swaps in behind the store contract in v0.2/v0.3
//! ([010](../milestones/v0.1-backend-core/010-appstate-wiring.md),
//! [02 §6, §8](../docs/02-matching-architecture.md)).
//!
//! ## The seam between transport and domain
//!
//! `AppState` is the **application** layer: it assembles the per-underlying
//! single-writer actors ([`crate::exchange::ActorHandle`]), the venue-wide
//! instrument services, the shared derived stores, and the (placeholder) auth /
//! subscription / market-maker / simulator services behind **one** `Arc` a
//! gateway clones. It respects the one-way dependency flow — transport →
//! application → domain / persistence / services — so `AppState` imports the
//! domain but **never** `crate::gateway::*`, and nothing here imports back from
//! [`crate::lib`](crate) (CLAUDE.md *Module Boundaries*).
//!
//! ## Exactly one path onto the order path
//!
//! A gateway reaches a book **only** through [`AppState::submit`]: it routes a
//! [`VenueCommand`] to the right underlying's actor by the command's underlying
//! and awaits the [`Receipt`]. No gateway can reach a leaf or the sequencer
//! directly — the actor handle's bounded mailbox is the sole entry point
//! ([02 §8](../docs/02-matching-architecture.md)). Two underlyings run as two
//! independent actors sharing the registry/index **by handle**, so `BTC` and
//! `ETH` sequence concurrently without a shared writer lock.
//!
//! ## One set of shared stores, written post-journal and read here
//!
//! Every underlying's actor fans a committed [`VenueEvent`](crate::exchange::VenueEvent)
//! out (step 5, **after** it is journaled) into the **same** shared
//! [`InMemoryExecutionsStore`] / [`InMemoryPositionsStore`] / [`MarkPriceBook`]
//! `Arc`s that [`AppState`] exposes for reads — so a future REST read observes
//! exactly what the fan-out wrote. The `Arc` instances are cloned into each
//! actor's [`StoreFanOut`] at spawn and retained here; both sides point at the
//! one store ([02 §6](../docs/02-matching-architecture.md)).
//!
//! ## Placeholders (stable shape for #011–#016)
//!
//! The auth, subscription-manager, market-maker, and simulator services are
//! **placeholders** here so the wiring compiles and its shape is fixed; each real
//! implementation slots into its field without reshaping `AppState`:
//!
//! | Field           | Placeholder                 | Filled by |
//! |-----------------|-----------------------------|-----------|
//! | `auth`          | [`AuthPlaceholder`]         | #011/#012 |
//! | `subscriptions` | [`SubscriptionsPlaceholder`]| #014      |
//! | `market_maker`  | [`MarketMakerPlaceholder`]  | #015      |
//! | `simulator`     | [`SimulatorPlaceholder`]    | #016      |

use std::collections::HashMap;
use std::sync::Arc;

use option_chain_orderbook::{InstrumentRegistry, SymbolIndex, SymbolParser};

use crate::error::VenueError;
use crate::exchange::{
    ActorConfig, ActorHandle, EventTimestamp, ExecutionsStore, FixedClock, InMemoryExecutionsStore,
    InMemoryPositionsStore, InMemoryVenueJournal, JournalHeader, JournalSnapshot, LineageId,
    MarkPriceBook, MassCancelScope, Receipt, StoreFanOut, VenueCommand,
    spawn_matching_actor_with_registry_and_index,
};

/// The default bounded mailbox capacity for each per-underlying actor — a DoS
/// control, never unbounded ([08 §5](../docs/08-threat-model.md)). The real
/// per-instrument value is venue config (#022); this fixes a bounded default.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1_024;

/// The default fixed venue-clock instant, in **milliseconds**. `venue_ts` is not
/// the journaled total order (the `underlying_sequence` is), so a fixed instant
/// is deterministic and sufficient until the stepped / seeded clock lands. The
/// seeded clock service is wired with the simulation clock (#016).
pub const DEFAULT_VENUE_CLOCK_MS: u64 = 0;

/// The default run lineage token when none is supplied. Namespaces every
/// venue-minted id ([01 §6.1](../docs/01-domain-model.md)); the per-run unique
/// lineage is minted at bootstrap (#022).
pub const DEFAULT_LINEAGE_TOKEN: &str = "fauxchange";

// ============================================================================
// Service placeholders — stable field types for #011–#016
// ============================================================================

/// Placeholder for the JWT auth service (`JwtAuth`, `Permission`, `RateLimiter`)
/// — filled by **#011/#012**. A zero-sized stub so [`AppState`]'s shape is fixed;
/// the real service replaces this field's type without reshaping `AppState`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AuthPlaceholder;

/// Placeholder for the WebSocket subscription manager (per-symbol monotonic
/// sequence, broadcast fan-out) — filled by **#014**.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionsPlaceholder;

/// Placeholder for the market-maker engine handle — filled by **#015**.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MarketMakerPlaceholder;

/// Placeholder for the price simulator handle — filled by **#016**.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SimulatorPlaceholder;

// ============================================================================
// Construction parameters
// ============================================================================

/// The construction parameters for an [`AppState`]. Since the venue config
/// surface (#022) has not landed, the constructor takes an explicit list of
/// underlyings plus the run lineage, mailbox capacity, and venue-clock instant —
/// each with a bounded default.
#[derive(Debug, Clone)]
pub struct AppStateConfig {
    /// The underlyings to host — one single-writer actor is spawned per entry.
    /// Duplicates are ignored (a second actor is never spawned for the same
    /// underlying — that would be a second concurrent writer).
    pub underlyings: Vec<String>,
    /// The run lineage that namespaces every venue-minted id.
    pub lineage_id: LineageId,
    /// The bounded mailbox capacity for each actor.
    pub mailbox_capacity: usize,
    /// The fixed venue-clock instant, in **milliseconds**.
    pub venue_clock_ms: EventTimestamp,
}

impl AppStateConfig {
    /// Builds a config for `underlyings` with the bounded defaults
    /// ([`DEFAULT_LINEAGE_TOKEN`] / [`DEFAULT_MAILBOX_CAPACITY`] /
    /// [`DEFAULT_VENUE_CLOCK_MS`]).
    #[must_use]
    pub fn new(underlyings: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            underlyings: underlyings.into_iter().map(Into::into).collect(),
            lineage_id: LineageId::new(DEFAULT_LINEAGE_TOKEN),
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            venue_clock_ms: EventTimestamp::new(DEFAULT_VENUE_CLOCK_MS),
        }
    }

    /// Overrides the run lineage.
    #[must_use]
    pub fn with_lineage(mut self, lineage_id: LineageId) -> Self {
        self.lineage_id = lineage_id;
        self
    }

    /// Overrides the per-actor mailbox capacity.
    #[must_use]
    pub fn with_mailbox_capacity(mut self, mailbox_capacity: usize) -> Self {
        self.mailbox_capacity = mailbox_capacity;
        self
    }
}

// ============================================================================
// AppState
// ============================================================================

/// The shared `Arc` wiring every gateway handler receives — the application seam
/// between the transport gateways and the domain
/// ([010](../milestones/v0.1-backend-core/010-appstate-wiring.md),
/// [02 §8](../docs/02-matching-architecture.md)).
///
/// Cloned as `Arc<AppState>`; the struct itself is not `Clone`. The **shutdown
/// path** is dropping the last `Arc<AppState>`: that drops every per-underlying
/// [`ActorHandle`], closing each bounded mailbox, which drains any queued
/// commands and ends the actor's receive loop (the actor's documented shutdown
/// path, [`crate::exchange::actor`]). The spawned tasks are detached at
/// construction, so their lifetime is exactly the `AppState`'s.
///
/// # Examples
///
/// ```rust,no_run
/// use fauxchange::state::{AppState, AppStateConfig};
///
/// // Must be called within a `tokio` runtime — it spawns one actor per underlying.
/// let state = AppState::new(AppStateConfig::new(["BTC", "ETH"]));
/// assert_eq!(state.underlying_count(), 2);
/// assert!(state.hosts_underlying("BTC"));
/// ```
pub struct AppState {
    /// The venue-wide instrument registry — shared by every underlying's book so
    /// instrument-id allocation stays O(1) across the whole venue.
    registry: Arc<InstrumentRegistry>,
    /// The venue-wide symbol index — shared so cross-underlying symbol lookups
    /// stay O(1) without coupling the single writers.
    symbol_index: Arc<SymbolIndex>,
    /// The per-underlying single-writer actor handles, keyed by underlying
    /// ticker. Immutable after construction; every routed submit / snapshot is a
    /// point lookup, never an iteration on the sequenced path.
    underlyings: HashMap<Arc<str>, ActorHandle>,
    /// The single shared authoritative executions log — the **same** `Arc` every
    /// actor's [`StoreFanOut`] records into, so a read here observes the fan-out.
    executions: Arc<InMemoryExecutionsStore>,
    /// The single shared positions fold — the **same** `Arc` every actor's
    /// [`StoreFanOut`] folds into.
    positions: Arc<InMemoryPositionsStore>,
    /// The single shared live-only mark-price book (never journaled).
    marks: Arc<MarkPriceBook>,
    /// The run lineage namespacing every venue-minted id.
    lineage_id: LineageId,
    /// The JWT auth service (placeholder until #011/#012).
    auth: AuthPlaceholder,
    /// The WebSocket subscription manager (placeholder until #014).
    subscriptions: SubscriptionsPlaceholder,
    /// The market-maker engine handle (placeholder until #015).
    market_maker: MarketMakerPlaceholder,
    /// The price simulator handle (placeholder until #016).
    simulator: SimulatorPlaceholder,
}

impl AppState {
    /// Assembles an [`AppState`] behind an `Arc`, spawning **one single-writer
    /// actor per configured underlying** and wiring the real order path
    /// ([`crate::exchange::MatchingExecutor`]) and post-journal fan-out
    /// ([`StoreFanOut`]) into each.
    ///
    /// The venue-wide [`InstrumentRegistry`] + [`SymbolIndex`] are created once
    /// and passed to every actor **by handle** (via
    /// [`spawn_matching_actor_with_registry_and_index`]), and the shared
    /// executions / positions / mark stores are created once and cloned into each
    /// actor's fan-out — so every underlying writes to, and every read here
    /// observes, the one set of stores.
    ///
    /// A duplicate underlying in the config is skipped (with a `WARN`) rather than
    /// spawning a second concurrent writer for the same book.
    ///
    /// # Panics
    ///
    /// Must be called within a `tokio` runtime — it spawns the actor tasks; the
    /// spawn panics outside a runtime, matching
    /// [`spawn_matching_actor_with_registry_and_index`].
    #[must_use]
    pub fn new(config: AppStateConfig) -> Arc<Self> {
        let AppStateConfig {
            underlyings,
            lineage_id,
            mailbox_capacity,
            venue_clock_ms,
        } = config;

        // Venue-wide instrument services (O(1) cross-underlying lookups).
        let registry = Arc::new(InstrumentRegistry::new());
        let symbol_index = Arc::new(SymbolIndex::new());

        // The single shared derived stores: the same `Arc`s the fan-out writes to
        // and every gateway read observes.
        let executions = Arc::new(InMemoryExecutionsStore::new());
        let positions = Arc::new(InMemoryPositionsStore::new());
        let marks = Arc::new(MarkPriceBook::new());

        // A deterministic fixed clock (`venue_ts` is not the journaled order).
        let clock = FixedClock::new(venue_clock_ms);

        let mut handles: HashMap<Arc<str>, ActorHandle> = HashMap::with_capacity(underlyings.len());
        for underlying in underlyings {
            let ticker: Arc<str> = Arc::from(underlying);
            if handles.contains_key(&ticker) {
                tracing::warn!(
                    underlying = %ticker,
                    "duplicate underlying in AppStateConfig; skipping (no second writer)"
                );
                continue;
            }

            // Each actor owns its own append-only journal, keyed on the shared
            // lineage so re-derived ids stay in one namespace.
            let journal = InMemoryVenueJournal::new(JournalHeader::new(lineage_id.clone()));
            // The fan-out clones the shared store `Arc`s: the actor writes into
            // the very instances `AppState` exposes for reads.
            let fan_out = StoreFanOut::new(
                Arc::clone(&executions),
                Arc::clone(&positions),
                Arc::clone(&marks),
            );
            let actor_config =
                ActorConfig::new(Arc::clone(&ticker), lineage_id.clone(), mailbox_capacity);

            let (handle, join) = spawn_matching_actor_with_registry_and_index(
                actor_config,
                journal,
                fan_out,
                clock,
                Arc::clone(&registry),
                Arc::clone(&symbol_index),
            );
            // Detach: the actor's shutdown is its mailbox closing when this handle
            // drops with `AppState`; the mailbox drains its backlog first.
            drop(join);
            handles.insert(ticker, handle);
        }

        tracing::info!(
            underlyings = handles.len(),
            "AppState assembled; one single-writer actor spawned per underlying"
        );

        Arc::new(Self {
            registry,
            symbol_index,
            underlyings: handles,
            executions,
            positions,
            marks,
            lineage_id,
            auth: AuthPlaceholder,
            subscriptions: SubscriptionsPlaceholder,
            market_maker: MarketMakerPlaceholder,
            simulator: SimulatorPlaceholder,
        })
    }

    /// Submits a [`VenueCommand`] onto the sequenced order path — the **only** way
    /// a gateway reaches a book. The command is routed to the actor for its
    /// underlying and its [`Receipt`] is awaited
    /// ([02 §8](../docs/02-matching-architecture.md)).
    ///
    /// Routing extracts the underlying from the command (the target symbol, via
    /// the upstream [`SymbolParser`], for order-path and instrument commands; the
    /// `underlying` ticker for a `SimStep`; the `Book` symbol for a scoped mass
    /// cancel). Venue-global commands that carry no single underlying (`Clock`,
    /// `MarketMakerControl`, `EvictExpiredOrders`, and hierarchy-wide mass
    /// cancels) are not routable on this per-underlying submit path — their
    /// broadcast/lifecycle routing lands with the control-plane issues.
    ///
    /// # Errors
    ///
    /// - [`VenueError::InvalidOrder`] if the command's symbol does not parse, or
    ///   the command carries no routable underlying;
    /// - [`VenueError::NotFound`] if the underlying is not hosted by this venue;
    /// - the actor's own typed rejection ([`VenueError::RateLimited`] on a full
    ///   mailbox, [`VenueError::JournalUnavailable`] if the actor has stopped, or
    ///   a sequencing seal) otherwise.
    pub async fn submit(&self, command: VenueCommand) -> Result<Receipt, VenueError> {
        let handle = self.route(&command)?;
        handle.submit(command).await
    }

    /// Requests a read-only snapshot of `underlying`'s journal, routed to its
    /// actor — the read side of the per-underlying journal handle.
    ///
    /// # Errors
    ///
    /// - [`VenueError::NotFound`] if the underlying is not hosted;
    /// - [`VenueError::RateLimited`] / [`VenueError::JournalUnavailable`] from the
    ///   actor per [`ActorHandle::snapshot`].
    pub async fn journal_snapshot(&self, underlying: &str) -> Result<JournalSnapshot, VenueError> {
        let handle = self.handle_for(underlying)?;
        handle.snapshot().await
    }

    /// Resolves the actor handle a command routes to, cloning it so no borrow of
    /// `self` is held across the subsequent `.await`.
    fn route(&self, command: &VenueCommand) -> Result<ActorHandle, VenueError> {
        match command {
            VenueCommand::AddOrder { symbol, .. }
            | VenueCommand::CancelOrder { symbol, .. }
            | VenueCommand::Replace { symbol, .. }
            | VenueCommand::SetInstrumentStatus { symbol, .. } => {
                let parsed = SymbolParser::parse(symbol.as_str())
                    .map_err(|error| VenueError::InvalidOrder(error.to_string()))?;
                self.handle_for(parsed.underlying())
            }
            VenueCommand::MassCancel {
                scope: MassCancelScope::Book(symbol),
                ..
            } => {
                let parsed = SymbolParser::parse(symbol.as_str())
                    .map_err(|error| VenueError::InvalidOrder(error.to_string()))?;
                self.handle_for(parsed.underlying())
            }
            VenueCommand::SimStep { underlying, .. } => self.handle_for(underlying),
            VenueCommand::MassCancel { .. }
            | VenueCommand::EvictExpiredOrders { .. }
            | VenueCommand::MarketMakerControl { .. }
            | VenueCommand::Clock { .. } => Err(VenueError::InvalidOrder(
                "command carries no single routable underlying for the per-underlying \
                 submit path"
                    .to_string(),
            )),
        }
    }

    /// The actor handle for `underlying`, cloned, or [`VenueError::NotFound`] when
    /// the venue does not host it.
    fn handle_for(&self, underlying: &str) -> Result<ActorHandle, VenueError> {
        self.underlyings.get(underlying).cloned().ok_or_else(|| {
            VenueError::NotFound(format!(
                "underlying '{underlying}' is not hosted by this venue"
            ))
        })
    }

    /// The shared authoritative executions log — the **same** `Arc` the fan-out
    /// records into.
    #[must_use]
    #[inline]
    pub fn executions(&self) -> &Arc<InMemoryExecutionsStore> {
        &self.executions
    }

    /// The shared positions fold — the **same** `Arc` the fan-out folds into.
    #[must_use]
    #[inline]
    pub fn positions(&self) -> &Arc<InMemoryPositionsStore> {
        &self.positions
    }

    /// The shared live-only mark-price book.
    #[must_use]
    #[inline]
    pub fn marks(&self) -> &Arc<MarkPriceBook> {
        &self.marks
    }

    /// The venue-wide instrument registry.
    #[must_use]
    #[inline]
    pub fn registry(&self) -> &Arc<InstrumentRegistry> {
        &self.registry
    }

    /// The venue-wide symbol index.
    #[must_use]
    #[inline]
    pub fn symbol_index(&self) -> &Arc<SymbolIndex> {
        &self.symbol_index
    }

    /// The run lineage namespacing every venue-minted id.
    #[must_use]
    #[inline]
    pub fn lineage_id(&self) -> &LineageId {
        &self.lineage_id
    }

    /// The number of hosted underlyings (one actor each).
    #[must_use]
    #[inline]
    pub fn underlying_count(&self) -> usize {
        self.underlyings.len()
    }

    /// Whether this venue hosts `underlying`.
    #[must_use]
    #[inline]
    pub fn hosts_underlying(&self, underlying: &str) -> bool {
        self.underlyings.contains_key(underlying)
    }

    /// The hosted underlyings, **sorted** for a deterministic order regardless of
    /// map iteration order.
    #[must_use]
    pub fn underlyings(&self) -> Vec<&str> {
        let mut tickers: Vec<&str> = self.underlyings.keys().map(AsRef::as_ref).collect();
        tickers.sort_unstable();
        tickers
    }

    /// The JWT auth service (placeholder until #011/#012).
    #[must_use]
    #[inline]
    pub fn auth(&self) -> &AuthPlaceholder {
        &self.auth
    }

    /// The WebSocket subscription manager (placeholder until #014).
    #[must_use]
    #[inline]
    pub fn subscriptions(&self) -> &SubscriptionsPlaceholder {
        &self.subscriptions
    }

    /// The market-maker engine handle (placeholder until #015).
    #[must_use]
    #[inline]
    pub fn market_maker(&self) -> &MarketMakerPlaceholder {
        &self.market_maker
    }

    /// The price simulator handle (placeholder until #016).
    #[must_use]
    #[inline]
    pub fn simulator(&self) -> &SimulatorPlaceholder {
        &self.simulator
    }
}

impl std::fmt::Debug for AppState {
    /// A lightweight summary — deliberately not a `#[derive]` over the
    /// `DashMap`-backed registry/index/stores, whose derived `Debug` dumps entries
    /// in nondeterministic shard order.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("underlyings", &self.underlyings.len())
            .field("lineage_id", &self.lineage_id)
            .field("executions", &self.executions.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{Cents, Hash32, STPMode, Side, Symbol, TimeInForce};
    use crate::models::{AccountId, OrderType, VenueOrderId};

    fn config(underlyings: &[&str]) -> AppStateConfig {
        AppStateConfig::new(underlyings.iter().copied()).with_lineage(LineageId::new("run-1"))
    }

    fn sym(raw: &str) -> Symbol {
        match Symbol::parse(raw) {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
        }
    }

    fn cancel(symbol: &str) -> VenueCommand {
        VenueCommand::CancelOrder {
            symbol: sym(symbol),
            order_id: VenueOrderId::new("order-1"),
            account: AccountId::new("acct-1"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add(
        symbol: &str,
        order_id: &str,
        account: &str,
        owner: u8,
        side: Side,
        price: u64,
        quantity: u64,
    ) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym(symbol),
            order_id: VenueOrderId::new(order_id),
            account: AccountId::new(account),
            owner: Hash32([owner; 32]),
            client_order_id: None,
            side,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        }
    }

    // ---- construction spawns one actor per underlying --------------------

    #[tokio::test]
    async fn test_new_spawns_one_actor_per_configured_underlying() {
        let state = AppState::new(config(&["BTC", "ETH", "SOL"]));
        assert_eq!(state.underlying_count(), 3);
        assert!(state.hosts_underlying("BTC"));
        assert!(state.hosts_underlying("ETH"));
        assert!(state.hosts_underlying("SOL"));
        assert!(!state.hosts_underlying("DOGE"));
        // Deterministic, sorted view regardless of map order.
        assert_eq!(state.underlyings(), vec!["BTC", "ETH", "SOL"]);
        // The shared stores start empty and are exposed for reads.
        assert!(state.executions().is_empty());
    }

    #[tokio::test]
    async fn test_new_skips_a_duplicate_underlying() {
        // A repeated underlying must not spawn a second concurrent writer.
        let state = AppState::new(config(&["BTC", "BTC", "ETH"]));
        assert_eq!(state.underlying_count(), 2);
        assert_eq!(state.underlyings(), vec!["BTC", "ETH"]);
    }

    // ---- submit routes to the correct underlying's actor -----------------

    #[tokio::test]
    async fn test_submit_routes_to_the_correct_underlying_and_returns_a_receipt() {
        let state = AppState::new(config(&["BTC", "ETH"]));
        // A BTC cancel routes to the BTC actor and returns its receipt at seq 0.
        let receipt = match state.submit(cancel("BTC-20240329-50000-C")).await {
            Ok(r) => r,
            Err(e) => panic!("BTC submit failed: {e}"),
        };
        assert_eq!(receipt.underlying_sequence.get(), 0);
        // An ETH cancel routes to the *independent* ETH actor — also at its own
        // seq 0, proving the two underlyings sequence independently.
        let eth = match state.submit(cancel("ETH-20240329-3000-C")).await {
            Ok(r) => r,
            Err(e) => panic!("ETH submit failed: {e}"),
        };
        assert_eq!(eth.underlying_sequence.get(), 0);
        // A second BTC command advances only the BTC sequence.
        let btc2 = match state.submit(cancel("BTC-20240329-50000-C")).await {
            Ok(r) => r,
            Err(e) => panic!("second BTC submit failed: {e}"),
        };
        assert_eq!(btc2.underlying_sequence.get(), 1);
    }

    // ---- unknown underlying → typed error --------------------------------

    #[tokio::test]
    async fn test_submit_unknown_underlying_is_not_found() {
        let state = AppState::new(config(&["BTC"]));
        match state.submit(cancel("ETH-20240329-3000-C")).await {
            Err(VenueError::NotFound(detail)) => assert!(detail.contains("ETH")),
            other => panic!("expected NotFound for an unhosted underlying, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_submit_venue_global_command_is_not_routable() {
        let state = AppState::new(config(&["BTC"]));
        match state
            .submit(VenueCommand::Clock {
                now_ms: EventTimestamp::new(1),
            })
            .await
        {
            Err(VenueError::InvalidOrder(detail)) => assert!(detail.contains("routable")),
            other => panic!("expected an unroutable InvalidOrder, got {other:?}"),
        }
    }

    // ---- fan-out writes the store AppState exposes -----------------------

    #[tokio::test]
    async fn test_crossing_trade_fill_lands_in_the_shared_executions_store() {
        let state = AppState::new(config(&["BTC"]));
        let symbol = "BTC-20240329-50000-C";
        // Resting maker sell, then a crossing taker buy — both via the ONLY path.
        match state
            .submit(add(symbol, "maker-1", "maker", 0x11, Side::Sell, 50_000, 2))
            .await
        {
            Ok(_) => {}
            Err(e) => panic!("maker submit failed: {e}"),
        }
        match state
            .submit(add(symbol, "taker-1", "taker", 0x22, Side::Buy, 50_000, 2))
            .await
        {
            Ok(_) => {}
            Err(e) => panic!("taker submit failed: {e}"),
        }

        // The post-journal fan-out recorded both legs into the SAME store
        // AppState exposes for reads.
        assert_eq!(
            state.executions().len(),
            2,
            "one crossing match records two legs in the shared store"
        );
        let taker = match state.executions().list(
            &AccountId::new("taker"),
            &crate::exchange::ExecutionFilter::default(),
        ) {
            Ok(list) => list,
            Err(e) => panic!("executions list failed: {e}"),
        };
        assert_eq!(taker.len(), 1);
        assert_eq!(taker[0].price_cents, Cents::new(50_000));
        assert_eq!(taker[0].quantity, 2);
    }
}
