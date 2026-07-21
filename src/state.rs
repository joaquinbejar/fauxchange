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
//! ## Auth + subscriptions (real) and the remaining placeholders (#015–#016)
//!
//! The auth service is **real** as of #012: [`AppState`] owns the
//! [`AccountRegistry`] and an [`AuthService`] pinned to the concrete venue
//! [`FixedClock`] (built from [`JwtAuth`] + [`RateLimiter`] + the registry as the
//! [`RevocationOracle`]). The WebSocket subscription manager is **real** as of
//! #014: `AppState` owns the [`crate::subscription::OrderbookSubscriptionManager`]
//! service (a sibling of [`crate::auth`] / [`crate::ohlc`], **not** a gateway) and
//! tees a [`WsFanOut`] alongside each actor's
//! [`StoreFanOut`] via the exchange-owned [`TeeFanOut`], so the **same** committed
//! [`VenueEvent`](crate::exchange::VenueEvent) feeds the stores and the WS
//! broadcast post-journal. The market-maker and simulator services are still
//! **placeholders** so the wiring compiles and its shape is fixed; each real
//! implementation slots into its field without reshaping `AppState`:
//!
//! | Field           | Type / placeholder                | Filled by |
//! |-----------------|-----------------------------------|-----------|
//! | `auth`          | [`AuthService`] (real)             | #011/#012 |
//! | `accounts`      | [`AccountRegistry`] (real)         | #012      |
//! | `subscriptions` | [`crate::subscription::OrderbookSubscriptionManager`] (real) | #014 |
//! | `market_maker`  | [`MarketMakerPlaceholder`]        | #015      |
//! | `simulator`     | [`SimulatorPlaceholder`]          | #016      |

use std::collections::HashMap;
use std::sync::Arc;

use option_chain_orderbook::{InstrumentRegistry, SymbolIndex, SymbolParser};

use crate::auth::{
    AccountProvision, AccountRegistry, AccountStore, Argon2Hasher, AuthError, AuthService,
    BootstrapGate, DEFAULT_RATE_LIMIT_PER_WINDOW, JwtAuth, RateLimiter, RevocationOracle,
};
use crate::error::VenueError;
use crate::exchange::{
    ActorConfig, ActorHandle, EventTimestamp, ExecutionsStore, FixedClock, InMemoryExecutionsStore,
    InMemoryPositionsStore, InMemoryVenueJournal, JournalHeader, JournalSnapshot, LineageId,
    MarkPriceBook, MassCancelScope, Receipt, StoreFanOut, TeeFanOut, VenueCommand,
    spawn_matching_actor_with_registry_and_index,
};
use crate::models::AccountId;
// The WebSocket market-data SERVICE (#014) — a `crate::subscription` service (a
// sibling of `crate::auth` / `crate::ohlc`), NOT a gateway. `AppState` owns the
// manager and tees a `WsFanOut` alongside `StoreFanOut` (via the exchange-owned
// `TeeFanOut`) into every actor's fan-out. The service imports only the DTOs +
// the exchange core, never `crate::state` or `crate::gateway`, so the layered
// flow (transport → application → domain / services) holds — this is the same
// kind of wiring reference `AppState` already makes to `StoreFanOut`.
use crate::subscription::{OrderbookSubscriptionManager, WsFanOut};

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
// Service placeholders — stable field types for #014–#016
// ============================================================================

/// Placeholder for the market-maker engine handle — filled by **#015**.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MarketMakerPlaceholder;

/// Placeholder for the price simulator handle — filled by **#016**.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SimulatorPlaceholder;

// ============================================================================
// Construction parameters
// ============================================================================

/// The auth inputs for an [`AppState`]: the RS256 JWT key pair, the
/// `AUTH_BOOTSTRAP_SECRET` gate, the optional Argon2 pepper, the accounts to
/// provision, and the rate-limit budget
/// ([ADR-0007](../docs/adr/0007-fix-credentials-and-account-model.md),
/// [06 §8](../docs/06-deployment.md#8-auth-bootstrap)).
///
/// Built by the caller (`main.rs` in a real deployment, tests locally) so
/// [`AppState`] can pin a concrete rate-limit clock without a `config.rs`
/// dependency. `JwtAuth` is not `Clone`, so neither is this; the bootstrap secret
/// and pepper are secrets — the [`std::fmt::Debug`] impl **redacts** both.
pub struct AuthConfig {
    /// The RS256 JWT service ([`JwtAuth::dev`] locally, [`JwtAuth::from_paths`] in
    /// a real deployment).
    pub jwt: JwtAuth,
    /// The `AUTH_BOOTSTRAP_SECRET`; `None` disables token issuance entirely.
    pub bootstrap_secret: Option<String>,
    /// The optional Argon2 pepper (`AUTH_PASSWORD_PEPPER`), never persisted with a
    /// hash.
    pub pepper: Option<Vec<u8>>,
    /// The accounts to provision into the registry at construction.
    pub accounts: Vec<AccountProvision>,
    /// The per-window rate-limit budget.
    pub rate_limit_per_window: u32,
}

impl std::fmt::Debug for AuthConfig {
    /// Redacts the bootstrap secret and pepper; [`JwtAuth`]'s own `Debug` redacts
    /// the key material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("jwt", &self.jwt)
            .field(
                "bootstrap_secret",
                &self.bootstrap_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("pepper", &self.pepper.as_ref().map(|_| "<redacted>"))
            .field("accounts", &self.accounts.len())
            .field("rate_limit_per_window", &self.rate_limit_per_window)
            .finish()
    }
}

impl AuthConfig {
    /// Auth over an explicit RS256 key pair, with issuance disabled, no pepper, no
    /// accounts, and the default rate-limit budget — the base to build on.
    #[must_use]
    pub fn with_jwt(jwt: JwtAuth) -> Self {
        Self {
            jwt,
            bootstrap_secret: None,
            pepper: None,
            accounts: Vec::new(),
            rate_limit_per_window: DEFAULT_RATE_LIMIT_PER_WINDOW,
        }
    }

    /// Local dev auth: the embedded, non-secret dev key pair, issuance disabled,
    /// and no accounts — the default when an [`AppStateConfig`] carries no auth.
    ///
    /// # Errors
    ///
    /// [`AuthError::KeyLoad`] only if the embedded dev fixtures fail to parse (a
    /// build invariant; never in practice).
    pub fn dev() -> Result<Self, AuthError> {
        Ok(Self::with_jwt(JwtAuth::dev()?))
    }

    /// Sets the bootstrap secret that gates token issuance.
    #[must_use]
    pub fn with_bootstrap_secret(mut self, secret: impl Into<String>) -> Self {
        self.bootstrap_secret = Some(secret.into());
        self
    }

    /// Sets the optional Argon2 pepper.
    #[must_use]
    pub fn with_pepper(mut self, pepper: Vec<u8>) -> Self {
        self.pepper = Some(pepper);
        self
    }

    /// Sets the accounts to provision.
    #[must_use]
    pub fn with_accounts(mut self, accounts: Vec<AccountProvision>) -> Self {
        self.accounts = accounts;
        self
    }

    /// Overrides the per-window rate-limit budget.
    #[must_use]
    pub fn with_rate_limit(mut self, per_window: u32) -> Self {
        self.rate_limit_per_window = per_window;
        self
    }
}

/// The construction parameters for an [`AppState`]. Since the venue config
/// surface (#022) has not landed, the constructor takes an explicit list of
/// underlyings plus the run lineage, mailbox capacity, and venue-clock instant —
/// each with a bounded default — and the optional [`AuthConfig`] (a `None` auth
/// defaults to local dev auth in [`AppState::new`]).
#[derive(Debug)]
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
    /// The auth inputs; `None` defaults to [`AuthConfig::dev`] in
    /// [`AppState::new`].
    pub auth: Option<AuthConfig>,
}

impl AppStateConfig {
    /// Builds a config for `underlyings` with the bounded defaults
    /// ([`DEFAULT_LINEAGE_TOKEN`] / [`DEFAULT_MAILBOX_CAPACITY`] /
    /// [`DEFAULT_VENUE_CLOCK_MS`]) and **no** explicit auth (local dev auth is
    /// applied by [`AppState::new`]).
    #[must_use]
    pub fn new(underlyings: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            underlyings: underlyings.into_iter().map(Into::into).collect(),
            lineage_id: LineageId::new(DEFAULT_LINEAGE_TOKEN),
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            venue_clock_ms: EventTimestamp::new(DEFAULT_VENUE_CLOCK_MS),
            auth: None,
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

    /// Sets the explicit auth inputs (JWT key pair, bootstrap secret, pepper,
    /// provisioned accounts, rate-limit budget).
    #[must_use]
    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Some(auth);
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
/// # fn main() -> Result<(), fauxchange::auth::AuthError> {
/// // Must be called within a `tokio` runtime — it spawns one actor per underlying.
/// // Auth defaults to the embedded dev key pair when the config carries none.
/// let state = AppState::new(AppStateConfig::new(["BTC", "ETH"]))?;
/// assert_eq!(state.underlying_count(), 2);
/// assert!(state.hosts_underlying("BTC"));
/// # Ok(())
/// # }
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
    /// The venue account registry (the #012 [`AccountStore`](crate::auth::AccountStore)
    /// backend). The same `Arc`, cast to [`RevocationOracle`], is the auth
    /// service's revocation oracle — so a [`AccountRegistry::revoke`] is visible
    /// to the middleware on the next request.
    accounts: Arc<AccountRegistry>,
    /// The JWT auth service (real as of #012): JWT verification, the rate limiter
    /// on the venue [`FixedClock`], and the account revocation oracle.
    auth: AuthService<FixedClock>,
    /// The operator gate on token issuance (`AUTH_BOOTSTRAP_SECRET`), consulted by
    /// the registry-resolved mint ([`AppState::mint_token`]).
    bootstrap_gate: BootstrapGate,
    /// The WebSocket market-data subscription manager (#014) — the shared
    /// broadcast + per-instrument `instrument_sequence` service every `/ws`
    /// connection reads, fed post-journal by each actor's [`WsFanOut`].
    subscriptions: Arc<OrderbookSubscriptionManager>,
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
    /// Auth is built **before** any actor is spawned: a `None` [`AppStateConfig::auth`]
    /// defaults to [`AuthConfig::dev`], the registry is provisioned (hashing each
    /// account's password with Argon2id — a one-off bootstrap cost, never a
    /// request-path cost), and the [`AuthService`] is pinned to the venue
    /// [`FixedClock`], with the registry (as [`RevocationOracle`]) as its oracle.
    ///
    /// # Panics
    ///
    /// Must be called within a `tokio` runtime — it spawns the actor tasks; the
    /// spawn panics outside a runtime, matching
    /// [`spawn_matching_actor_with_registry_and_index`].
    ///
    /// # Errors
    ///
    /// [`AuthError`] if auth cannot be built: the embedded dev fixtures fail to
    /// parse ([`AuthError::KeyLoad`]), a provisioned password cannot be hashed
    /// ([`AuthError::PasswordHash`]), or two accounts collide on an id or FIX
    /// username ([`AuthError::Provisioning`]).
    pub fn new(config: AppStateConfig) -> Result<Arc<Self>, AuthError> {
        let AppStateConfig {
            underlyings,
            lineage_id,
            mailbox_capacity,
            venue_clock_ms,
            auth,
        } = config;

        // A deterministic fixed clock (`venue_ts` is not the journaled order); the
        // rate limiter and the sequenced path read this SAME venue clock.
        let clock = FixedClock::new(venue_clock_ms);

        // Build auth FIRST (the only fallible step): a `None` auth defaults to the
        // embedded dev key pair, then provision the registry and assemble the
        // service pinned to the venue clock. No actor is spawned until this holds.
        let AuthConfig {
            jwt,
            bootstrap_secret,
            pepper,
            accounts,
            rate_limit_per_window,
        } = match auth {
            Some(auth) => auth,
            None => AuthConfig::dev()?,
        };
        let hasher = Argon2Hasher::new(pepper);
        let account_registry = Arc::new(AccountRegistry::provision(hasher, accounts)?);
        // Clone the concrete `Arc`, then coerce to the oracle trait object (the
        // unsizing coercion applies to the cloned value, not through `Arc::clone`).
        let revocation: Arc<dyn RevocationOracle> = account_registry.clone();
        let auth_service = AuthService::new(
            jwt,
            RateLimiter::new(clock, rate_limit_per_window),
            revocation,
        );
        let bootstrap_gate = BootstrapGate::new(bootstrap_secret);

        // Venue-wide instrument services (O(1) cross-underlying lookups).
        let registry = Arc::new(InstrumentRegistry::new());
        let symbol_index = Arc::new(SymbolIndex::new());

        // The single shared derived stores: the same `Arc`s the fan-out writes to
        // and every gateway read observes.
        let executions = Arc::new(InMemoryExecutionsStore::new());
        let positions = Arc::new(InMemoryPositionsStore::new());
        let marks = Arc::new(MarkPriceBook::new());

        // The WebSocket market-data service: one shared bounded broadcast + the
        // per-instrument sequence/aggregate, fed by a `WsFanOut` teed alongside
        // each actor's `StoreFanOut` (both consume the SAME post-journal event).
        let subscriptions = Arc::new(OrderbookSubscriptionManager::new());

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
            // The fan-out tees the committed event into BOTH the shared stores
            // (`StoreFanOut`, #008) and the WS market-data broadcast (`WsFanOut`,
            // #014) — one post-journal event, two consumers, neither on the
            // order-path critical section. The store fan-out clones the shared
            // store `Arc`s (the actor writes into the very instances `AppState`
            // exposes for reads); the WS fan-out clones the shared manager `Arc`.
            let fan_out = TeeFanOut::new(
                StoreFanOut::new(
                    Arc::clone(&executions),
                    Arc::clone(&positions),
                    Arc::clone(&marks),
                ),
                WsFanOut::new(Arc::clone(&subscriptions)),
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
            accounts = account_registry.account_count(),
            "AppState assembled; one single-writer actor spawned per underlying"
        );

        Ok(Arc::new(Self {
            registry,
            symbol_index,
            underlyings: handles,
            executions,
            positions,
            marks,
            lineage_id,
            accounts: account_registry,
            auth: auth_service,
            bootstrap_gate,
            subscriptions,
            market_maker: MarketMakerPlaceholder,
            simulator: SimulatorPlaceholder,
        }))
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

    /// The JWT auth service (real as of #012) — JWT verification, the venue-clock
    /// rate limiter, and the account revocation oracle behind one handle every
    /// gateway consults.
    #[must_use]
    #[inline]
    pub fn auth(&self) -> &AuthService<FixedClock> {
        &self.auth
    }

    /// The venue account registry (the [`AccountStore`] backend) — resolution by
    /// JWT `sub` / FIX username, Argon2id verification, and revocation.
    #[must_use]
    #[inline]
    pub fn accounts(&self) -> &AccountRegistry {
        &self.accounts
    }

    /// The operator gate on token issuance (`AUTH_BOOTSTRAP_SECRET`).
    #[must_use]
    #[inline]
    pub fn bootstrap_gate(&self) -> &BootstrapGate {
        &self.bootstrap_gate
    }

    /// The registry-resolved bootstrap mint the token-issuance route (#013) calls:
    /// authorises `presented_secret`, resolves `account` to its **registered**
    /// permissions and current revocation epoch, and mints a JWT via #011's
    /// [`JwtAuth`]. Never fabricates a subject or permissions
    /// ([ADR-0007](../docs/adr/0007-fix-credentials-and-account-model.md)).
    ///
    /// `issued_at_secs` / `ttl_secs` are wall-clock **seconds** (token expiry is a
    /// credential-plane concern, not the venue clock); the route supplies
    /// `issued_at_secs` from the wall clock and typically
    /// [`DEFAULT_TOKEN_TTL_SECS`](crate::auth::DEFAULT_TOKEN_TTL_SECS) for the TTL.
    ///
    /// # Errors
    ///
    /// The registry-resolved mint errors: [`AuthError::BootstrapDisabled`] /
    /// [`AuthError::BootstrapMismatch`] (gate, checked before any account lookup),
    /// [`AuthError::UnknownAccount`], [`AuthError::TokenLifetime`], or
    /// [`AuthError::Signing`].
    pub fn mint_token(
        &self,
        account: &AccountId,
        presented_secret: &str,
        issued_at_secs: u64,
        ttl_secs: u64,
    ) -> Result<String, AuthError> {
        self.accounts.mint_for_account(
            self.auth.jwt(),
            &self.bootstrap_gate,
            account,
            presented_secret,
            issued_at_secs,
            ttl_secs,
        )
    }

    /// The WebSocket market-data subscription manager (#014) — the shared
    /// broadcast + per-instrument `instrument_sequence` service the `/ws`
    /// connections read (snapshot on subscribe, filtered forwarding of the
    /// bounded broadcast).
    #[must_use]
    #[inline]
    pub fn subscriptions(&self) -> &Arc<OrderbookSubscriptionManager> {
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

    /// Builds an [`AppState`] from `config`, defaulting to local dev auth (no
    /// accounts), panicking only if the infallible-in-practice auth build fails.
    fn new_state(config: AppStateConfig) -> Arc<AppState> {
        match AppState::new(config) {
            Ok(state) => state,
            Err(error) => panic!("AppState::new must succeed with dev auth: {error}"),
        }
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
        let state = new_state(config(&["BTC", "ETH", "SOL"]));
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
        let state = new_state(config(&["BTC", "BTC", "ETH"]));
        assert_eq!(state.underlying_count(), 2);
        assert_eq!(state.underlyings(), vec!["BTC", "ETH"]);
    }

    // ---- submit routes to the correct underlying's actor -----------------

    #[tokio::test]
    async fn test_submit_routes_to_the_correct_underlying_and_returns_a_receipt() {
        let state = new_state(config(&["BTC", "ETH"]));
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
        let state = new_state(config(&["BTC"]));
        match state.submit(cancel("ETH-20240329-3000-C")).await {
            Err(VenueError::NotFound(detail)) => assert!(detail.contains("ETH")),
            other => panic!("expected NotFound for an unhosted underlying, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_submit_venue_global_command_is_not_routable() {
        let state = new_state(config(&["BTC"]));
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
        let state = new_state(config(&["BTC"]));
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
