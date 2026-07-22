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
//! [`AccountRegistry`] and an [`AuthService`] pinned to the shared venue
//! [`SimClock`] (built from [`JwtAuth`] + [`RateLimiter`] + the registry as the
//! [`RevocationOracle`]) — the same advancing clock the actors stamp `venue_ts`
//! from, so rate-limit decisions replay deterministically (#028). The WebSocket subscription manager is **real** as of
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
//! | `market_maker`  | [`crate::market_maker::MarketMakerEngine`] (real) | #015 |
//! | `simulator`     | [`crate::simulation::PriceSimulator`] (real) | #016 |

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use option_chain_orderbook::{InstrumentRegistry, SymbolIndex, SymbolParser};
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::auth::{
    AccountProvision, AccountRegistry, AccountStore, Argon2Hasher, AuthError, AuthService,
    BootstrapGate, DEFAULT_RATE_LIMIT_PER_WINDOW, JwtAuth, RateLimitBudgets, RateLimiter,
    RevocationOracle,
};
use crate::db::{DatabasePool, DbError, PgVenueJournal};
use crate::error::VenueError;
use crate::exchange::{
    ActorConfig, ActorHandle, CancelledLeg, ClOrdIdIndex, ClOrdIdRecord, EventTimestamp,
    ExecutionsStore, ExpirationDate, FanOut, FanOutSealed, FanoutSummary, InMemoryExecutionsStore,
    InMemoryPositionsStore, InMemoryVenueJournal, InstrumentStatus, JournalError, JournalHeader,
    JournalSnapshot, LineageId, MarkPriceBook, MarketMakerControlSink, MassCancelScope,
    MatchingExecutor, PositionsStore, Receipt, Recovered, SequenceNumber, StoreFanOut, Symbol,
    TeeFanOut, VenueCommand, VenueEvent, VenueJournal, VenueOutcome, check_price_band,
    recover_into, spawn_matching_actor_with_registry_and_index,
    spawn_underlying_actor_with_clordid_index,
};
use crate::market_maker::{ActorCommandSink, MarketMakerControlHub, MarketMakerEngine, Quoter};
use crate::microstructure::{
    DEFAULT_INGRESS_BUFFER_CAPACITY, IngressReorderBuffer, IngressStamp, MicrostructureConfig,
    MicrostructureConfigError, ReleaseKey, release_deadline_us,
};
use crate::models::{AccountId, ClientOrderId};
use crate::simulation::{
    AssetConfig, ClockMode, CorrelationId, ExpiryPhase, ExpirySchedule, JournalStream,
    PriceSimulator, RecordingController, ReplayError, ReplayReport, RunManifest, ScenarioBundle,
    SimClock, SimError, SimulationConfig, SynthesizedChain, VenueClockConfig, VenueStepSink,
};
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

/// The default run-level seed recorded in the [`RunManifest`] when a caller
/// supplies none ([04 §6](../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
pub const DEFAULT_SEED: u64 = 0;

/// The default run lineage token when none is supplied. Namespaces every
/// venue-minted id ([01 §6.1](../docs/01-domain-model.md)); the per-run unique
/// lineage is minted at bootstrap (#022).
pub const DEFAULT_LINEAGE_TOKEN: &str = "fauxchange";

/// The default wall-clock cadence at which [`spawn_clock_cadence_driver`] advances
/// the shared venue clock in realtime / accelerated mode. Fine enough that
/// `venue_ts` stays fresh and the sliding rate-limit window (60 s) rolls smoothly,
/// while cheap: each tick is a single atomic store **off** the sequenced path — no
/// journal append, no book mutation. The live value becomes venue config (#046).
pub const DEFAULT_CLOCK_CADENCE: Duration = Duration::from_millis(250);

/// The bounded number of settle polls a session materialisation (#031) waits for
/// the async requote forwarder to vivify the synthesised chain — a DoS-free
/// ceiling, never an unbounded spin (mirrors the seed-phase settle).
const SESSION_SETTLE_MAX_POLLS: usize = 400;

/// The delay between session-materialisation settle polls, in **milliseconds**.
const SESSION_SETTLE_POLL_MS: u64 = 5;

/// A failure assembling an [`AppState`] — the two fallible boot steps: building
/// auth ([`AuthError`]) and opening the durable journal store when `DATABASE_URL` is
/// set ([`DbError`]). Both carry only non-secret detail (the `DbError` `Display` is
/// a redacted operation label; the `DATABASE_URL` is never logged or surfaced).
#[derive(Debug, thiserror::Error)]
pub enum AppStateError {
    /// Auth could not be built (dev fixtures failed to parse, a provisioned password
    /// could not be hashed, or two accounts collided on an id / FIX username).
    #[error(transparent)]
    Auth(#[from] AuthError),
    /// The durable journal store could not be opened for an underlying (a header
    /// row could not be ensured or read back, the **persisted header disagreed**
    /// with this run's lineage/schema, or the venue was assembled outside a tokio
    /// runtime). Only reachable when `DATABASE_URL` is set.
    #[error(transparent)]
    Db(#[from] DbError),
    /// The venue microstructure (#044) could not be applied to a book at creation —
    /// the resolved contract specs were rejected by the upstream builder. Unreachable
    /// for a resolver-validated config (the config seam proves it at load); surfaced
    /// rather than unwrapped.
    #[error(transparent)]
    Microstructure(#[from] MicrostructureConfigError),
    /// Boot-time journal recovery (#85) failed for an underlying whose durable
    /// journal is **non-empty** — a **fail-stop** refusal to serve rather than a
    /// silent fresh start over durable history (ADR-0004 restart recovery, ADR-0006
    /// §recovery). The carried [`JournalError`] names the exact cause — corruption at
    /// a precise `(underlying, sequence)`, a newer-than-binary envelope schema, a
    /// tampered record, or a durable read failure — and this variant adds the
    /// `underlying` whose stream could not be resumed. Only reachable when
    /// `DATABASE_URL` is set and the stream has rows.
    #[error("boot journal recovery failed for underlying '{underlying}': {source}")]
    Recovery {
        /// The underlying whose durable stream could not be recovered.
        underlying: String,
        /// The typed recovery failure (naming the exact sequence for a corruption).
        #[source]
        source: JournalError,
    },
    /// Two recovered underlyings carry **disagreeing** persisted run lineages — a
    /// contradictory durable manifest that cannot resolve to one venue run identity.
    /// The venue refuses to resume with a split id namespace (every venue-minted id
    /// is namespaced by `lineage_id`, #85). Carries only the **non-secret** run
    /// lineage tokens (derived from the run seed) and the underlyings — never a
    /// credential or the `DATABASE_URL`.
    #[error(
        "contradictory durable manifest: underlying '{underlying}' resumes lineage \
         '{lineage}' but '{first_underlying}' already resumed lineage '{first_lineage}'"
    )]
    RecoveryLineageConflict {
        /// The first recovered underlying, whose lineage fixed the venue run.
        first_underlying: String,
        /// The lineage token the first recovered underlying resumed.
        first_lineage: String,
        /// The recovered underlying whose lineage disagrees.
        underlying: String,
        /// The conflicting lineage token.
        lineage: String,
    },
    /// A recovered underlying's durable stream already occupies the **entire**
    /// `underlying_sequence` space (its last sequence is `u64::MAX`), so no continued
    /// sequence can be assigned. Fail-stop rather than reuse a sequence or wrap it —
    /// a wrapped sequence corrupts gap detection and replay (#85). Astronomically
    /// unreachable (2^64 sequences on one underlying); surfaced typed, not
    /// `unwrap`ped.
    #[error(
        "boot journal recovery cannot continue underlying '{underlying}': \
         underlying_sequence space exhausted at {sequence}"
    )]
    RecoverySequenceExhausted {
        /// The underlying whose sequence space is exhausted.
        underlying: String,
        /// The last (maximal) sequence present in the durable stream.
        sequence: u64,
    },
    /// Rebuilding the authoritative executions/positions stores from a recovered
    /// underlying's events hit a projection failure (the #131 `StoreFanOut` seal).
    /// The venue **refuses to serve** rather than start with stores that do not
    /// match the recovered journal — a journal-backed rebuild is the recovery (#85).
    #[error(
        "boot store rebuild failed for underlying '{underlying}': the {projection} \
         projection failed ({detail}) — refusing to serve a partially-rebuilt store"
    )]
    RecoveryProjectionFailed {
        /// The underlying whose store rebuild failed.
        underlying: String,
        /// Which authoritative projection failed (`executions` / `positions`).
        projection: &'static str,
        /// The store-error detail.
        detail: String,
    },
}

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
    /// The per-tier rate-limit budgets and window (#046). Defaults to a uniform
    /// [`DEFAULT_RATE_LIMIT_PER_WINDOW`] budget across every tier; the venue config
    /// (`[rate_limits]`) overrides it via [`AuthConfig::with_rate_limit_budgets`].
    pub rate_limit: RateLimitBudgets,
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
            .field("rate_limit", &self.rate_limit)
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
            rate_limit: RateLimitBudgets::uniform(DEFAULT_RATE_LIMIT_PER_WINDOW),
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

    /// Overrides the rate-limit budget with a **uniform** per-window limit across
    /// every tier (the pre-#046 single-limit shape).
    #[must_use]
    pub fn with_rate_limit(mut self, per_window: u32) -> Self {
        self.rate_limit = RateLimitBudgets::uniform(per_window);
        self
    }

    /// Overrides the rate-limit budget with explicit **per-tier**
    /// [`RateLimitBudgets`] — the #046 venue-config path (`[rate_limits]`).
    #[must_use]
    pub fn with_rate_limit_budgets(mut self, budgets: RateLimitBudgets) -> Self {
        self.rate_limit = budgets;
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
    /// The venue clock construction parameters (mode + virtual epoch) — the one
    /// clock the actors, the simulator, and the rate limiter share (#028).
    pub clock: VenueClockConfig,
    /// The one run-level seed recorded in the [`RunManifest`].
    pub seed: u64,
    /// The auth inputs; `None` defaults to [`AuthConfig::dev`] in
    /// [`AppState::new`].
    pub auth: Option<AuthConfig>,
    /// The price-simulator asset walks (empty ⇒ the simulator hosts no walked
    /// underlyings; the venue is fully usable and a `SimStep` still routes).
    pub assets: Vec<AssetConfig>,
    /// The simulation-wide parameters (cadence, horizon, virtual clock).
    pub simulation: SimulationConfig,
    /// The **optional** durable persistence pool (#023) — `None` (the default) is
    /// the fully in-memory venue; `Some` is the durable path
    /// (`DATABASE_URL` set), opened + migrated at boot by `main.rs` and passed in.
    pub db: Option<DatabasePool>,
    /// Whether the venue starts in the **serving** phase (#024). `true` (the
    /// default) is backward-compatible — the venue is immediately serving, and the
    /// runtime hierarchy-create routes refuse (manifest input). The seed flow sets
    /// this `false` to enter the bounded **seeding** phase, runs the seed manifest,
    /// then flips to serving with [`AppState::begin_serving`].
    pub start_serving: bool,
    /// The resolved venue microstructure (#044) — the fee schedule, STP mode, and
    /// per-underlying contract specs applied at each book's creation, and the
    /// venue-owned price band admitted at order entry. Defaults to the neutral
    /// [`MicrostructureConfig::default`] (zero fee, STP off, baseline specs).
    pub microstructure: MicrostructureConfig,
    /// The bounded depth of each per-underlying **ingress reorder buffer** (#111) —
    /// a DoS control on the deadline-ordered arrival buffer, never unbounded
    /// ([08 §5](../docs/08-threat-model.md#5-resource-exhaustion)). Defaults to
    /// [`DEFAULT_INGRESS_BUFFER_CAPACITY`]; only consulted when latency injection is
    /// configured (an empty buffer costs nothing on the FIFO fast path).
    pub ingress_buffer_capacity: usize,
}

impl AppStateConfig {
    /// Builds a config for `underlyings` with the bounded defaults
    /// ([`DEFAULT_LINEAGE_TOKEN`] / [`DEFAULT_MAILBOX_CAPACITY`] / a realtime
    /// [`VenueClockConfig`] / [`DEFAULT_SEED`]) and **no** explicit auth (local dev
    /// auth is applied by [`AppState::new`]).
    #[must_use]
    pub fn new(underlyings: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            underlyings: underlyings.into_iter().map(Into::into).collect(),
            lineage_id: LineageId::new(DEFAULT_LINEAGE_TOKEN),
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            clock: VenueClockConfig::default(),
            seed: DEFAULT_SEED,
            auth: None,
            assets: Vec::new(),
            simulation: SimulationConfig::default(),
            db: None,
            start_serving: true,
            microstructure: MicrostructureConfig::default(),
            ingress_buffer_capacity: DEFAULT_INGRESS_BUFFER_CAPACITY,
        }
    }

    /// Sets the venue clock construction parameters (mode + virtual epoch).
    #[must_use]
    pub fn with_clock(mut self, clock: VenueClockConfig) -> Self {
        self.clock = clock;
        self
    }

    /// Sets the run-level seed recorded in the [`RunManifest`].
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Sets whether the venue starts already **serving** (#024). Pass `false` to
    /// enter the bounded seeding phase; the seed flow flips to serving after the
    /// manifest is applied ([`AppState::begin_serving`]).
    #[must_use]
    pub fn with_serving(mut self, start_serving: bool) -> Self {
        self.start_serving = start_serving;
        self
    }

    /// Sets the optional durable persistence pool (#023). `None` (the default)
    /// keeps the fully in-memory venue; `Some` is the durable path, opened +
    /// migrated at boot before this call.
    #[must_use]
    pub fn with_db(mut self, db: Option<DatabasePool>) -> Self {
        self.db = db;
        self
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

    /// Sets the price-simulator asset walks.
    #[must_use]
    pub fn with_assets(mut self, assets: Vec<AssetConfig>) -> Self {
        self.assets = assets;
        self
    }

    /// Overrides the simulation-wide parameters.
    #[must_use]
    pub fn with_simulation(mut self, simulation: SimulationConfig) -> Self {
        self.simulation = simulation;
        self
    }

    /// Sets the resolved venue microstructure (#044) — the fee schedule, STP mode,
    /// and per-underlying contract specs applied at book creation, and the
    /// venue-owned price band admitted at order entry.
    #[must_use]
    pub fn with_microstructure(mut self, microstructure: MicrostructureConfig) -> Self {
        self.microstructure = microstructure;
        self
    }

    /// Overrides the bounded per-underlying ingress-reorder-buffer depth (#111) —
    /// the DoS cap on the deadline-ordered arrival buffer.
    #[must_use]
    pub fn with_ingress_buffer_capacity(mut self, capacity: usize) -> Self {
        self.ingress_buffer_capacity = capacity;
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
/// # fn main() -> Result<(), fauxchange::state::AppStateError> {
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
    /// on the shared venue [`SimClock`], and the account revocation oracle. The
    /// rate limiter reads the **same** advancing clock the sequenced path stamps
    /// events from, so its decisions replay deterministically (#028).
    auth: AuthService<SimClock>,
    /// The operator gate on token issuance (`AUTH_BOOTSTRAP_SECRET`), consulted by
    /// the registry-resolved mint ([`AppState::mint_token`]).
    bootstrap_gate: BootstrapGate,
    /// The WebSocket market-data subscription manager (#014) — the shared
    /// broadcast + per-instrument `instrument_sequence` service every `/ws`
    /// connection reads, fed post-journal by each actor's [`WsFanOut`].
    subscriptions: Arc<OrderbookSubscriptionManager>,
    /// The market-maker engine (real as of #015): the price → requote pipeline
    /// that routes every generated quote onto the sequenced order path through
    /// an [`ActorCommandSink`] over the same per-underlying actors.
    market_maker: Arc<MarketMakerEngine>,
    /// The price simulator (real as of #016): pre-generated `optionstratlib`
    /// walks whose every step routes onto the sequenced order path as a journaled
    /// [`VenueCommand::SimStep`] and drives the market maker. The interval loop is
    /// **not** auto-started here (a stepped-clock / bootstrap concern); drive
    /// [`PriceSimulator::step_once`](crate::simulation::PriceSimulator::step_once)
    /// or [`PriceSimulator::spawn`](crate::simulation::PriceSimulator::spawn).
    simulator: Arc<PriceSimulator>,
    /// The **optional** durable persistence pool (#023). `None` when
    /// `DATABASE_URL` is unset (the venue is fully in-memory); `Some` when the
    /// durable path was opened + migrated at boot. Held so the durable
    /// executions/config/account repositories reach it; the in-memory
    /// executions/positions fold above stays the live actor fan-out backend
    /// (promoting the durable store onto the fan-out is coupled to the v0.3
    /// journal + recovery, #029).
    db: Option<DatabasePool>,
    /// The venue lifecycle phase gate (#024): `false` in the bounded **seeding**
    /// phase, flipped to `true` (**serving**) once the seed manifest has been
    /// applied. Read by the runtime hierarchy-create routes so they refuse a
    /// mid-run mutation only once the venue is serving (the instrument set is a
    /// seed-time manifest input, [03 §10](../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
    /// A monotonic one-way flip — never flipped back.
    serving: AtomicBool,
    /// The one shared venue clock (#028) — the source the per-underlying actors
    /// stamp `venue_ts` from, the simulator stamps `SimStep.now_ms` from, and the
    /// auth rate limiter reads. Advancing it (stepped `Clock` command, or the
    /// realtime/accelerated cadence driver) happens off the sequenced read.
    clock: SimClock,
    /// The run manifest (#028) — the recorded `seed` + `clock_mode` that fix this
    /// run's determinism, so a replay can assert it reproduces the same run
    /// ([04 §6](../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
    manifest: RunManifest,
    /// The monotonic counter minting a shared [`CorrelationId`] per venue-control
    /// fan-out (a stepped `Clock` advance), so an operator can correlate the
    /// per-underlying commands one advance produced and detect a partial fan-out
    /// ([02 §4.1](../docs/02-matching-architecture.md#41-venue-wide-commands-marketmakercontrol--clock--simstep)).
    correlation_counter: AtomicU64,
    /// The venue recording flag (#030) — the record/replay control plane's
    /// scenario-capture window. The durable journal is always on; this marks
    /// whether a capture window is active for bundle export. Both the REST and WS
    /// record controls flip this **same** flag (control parity).
    recording: RecordingController,
    /// The resolved venue microstructure (#044) — the **same** config applied to
    /// every underlying's book at creation (fee schedule, STP mode, contract specs)
    /// and consulted at the order-admission seam for the venue-owned price band.
    /// Held behind an `Arc` so it can be carried into an exported [`ScenarioBundle`]
    /// as the config half of the determinism tuple: a replay applies the identical
    /// config, so a fee/STP-sensitive scenario reconstructs exactly.
    microstructure: Arc<MicrostructureConfig>,
    /// The last operational lifecycle phase the scheduled-expiry driver (#047) drove
    /// each `(underlying, expiration-day-ms)` to, so a repeated
    /// [`run_expiry_roll`](Self::run_expiry_roll) only issues **forward** transitions
    /// and never re-issues a settled expiration (avoiding an illegal regressive
    /// `SetInstrumentStatus`). Live-only driver state; the sequenced commands it
    /// issues are journaled and replay without it.
    expiry_phases: std::sync::Mutex<std::collections::HashMap<(String, i64), ExpiryPhase>>,
    /// The underlyings this venue **resumed** from a non-empty durable journal at
    /// boot (#85): their book / executions / positions state was reconstructed by
    /// re-execution and their actor continues the journaled `underlying_sequence`.
    /// **Recover wins over seed** — the bounded seeding phase must **not** re-seed a
    /// recovered underlying (a re-seed would journal a duplicate opening `SimStep`
    /// onto the resumed stream), so it consults [`is_recovered`](Self::is_recovered).
    /// Read off the sequenced path only (a point-lookup `contains`, never iterated on
    /// a hot path). Empty on a fresh boot or the fully in-memory path.
    recovered: std::collections::HashSet<Arc<str>>,
    /// The venue-wide, account-scoped `(account, ClOrdID) → order_id` correlation
    /// index (#098) — the **cross-session** bridge every underlying's actor publishes
    /// into POST-journal on the sequenced path, and the FIX/REST surfaces resolve
    /// from so a client can cancel/replace/status an order it placed in a **prior**
    /// session. It is a derived, journal-scoped artifact (never journaled itself):
    /// #085 boot recovery rebuilds it from the same `AddOrder` / `Replace` stream, so
    /// it survives a restart without a separate durable copy. The key is
    /// account-scoped, so a resolution can only ever return an order the
    /// authenticated account placed.
    clordid_index: Arc<ClOrdIdIndex>,
    /// The per-underlying **ingress reorder buffers** (#111), keyed by underlying —
    /// the deadline-ordered arrival buffers that apply the seeded
    /// [`LatencyOffset`](crate::microstructure::LatencyOffset) **before** the
    /// sequencer. Empty and untouched on the FIFO fast path (latency injection off);
    /// the actor still assigns `underlying_sequence` in receipt order, so the journal
    /// records the post-reorder order and replay reproduces it without re-running the
    /// reorder. Immutable after construction (a point lookup per submit, never an
    /// iteration on the sequenced path).
    ingress: HashMap<Arc<str>, Arc<IngressChannel>>,
    /// The venue-wide **monotonic per-arrival counter** stamped at ingress admission
    /// (#111) — the `arrival_sequence` half of the deterministic
    /// `(session_id, arrival_sequence)` tie-break. Advanced by a **checked** CAS
    /// ([`AppState::next_arrival_sequence`]); never wrapped (a wrapped counter would
    /// corrupt the tie-break total order).
    arrival_counter: AtomicU64,
    /// A venue-wide monotonic counter minting the `msg_seq` of a **REST** ingress
    /// stamp (#111) — REST carries no native per-message sequence, so the gateway
    /// edge stamps `(account, rest-seq)`. Advanced by a checked CAS; a fixed request
    /// order mints a fixed sequence, so a REST run's latency draws are reproducible.
    rest_ingress_counter: AtomicU64,
}

/// The result of a venue-control clock advance fanned across the underlyings —
/// the coordinator's in-memory ack ([02 §4.1](../docs/02-matching-architecture.md#41-venue-wide-commands-marketmakercontrol--clock--simstep)).
///
/// It reports the venue instant the clock advanced to, the shared
/// [`CorrelationId`] tagging this fan-out, and the per-underlying accept/commit
/// (a [`Receipt`] on success, a typed [`VenueError`] otherwise) — so a **partial**
/// fan-out (committed on some underlyings, not others) is surfaced, never hidden.
/// Journaling the correlation id durably for post-hoc partial-detection queries
/// lands with the durable journal (#029); #028's tag is this in-memory ack.
#[derive(Debug)]
pub struct ClockAdvance {
    /// The venue instant the shared clock advanced to (or held at), in **ms**.
    pub now_ms: EventTimestamp,
    /// The shared correlation id tagging every per-underlying command this advance
    /// fanned out.
    pub correlation_id: CorrelationId,
    /// Per-underlying accept/commit, keyed by ticker, in the deterministic sorted
    /// order.
    pub per_underlying: Vec<(String, Result<Receipt, VenueError>)>,
}

impl ClockAdvance {
    /// The number of underlyings that committed the advance (a successful
    /// [`Receipt`]).
    #[must_use]
    pub fn committed_count(&self) -> usize {
        self.per_underlying
            .iter()
            .filter(|(_, result)| result.is_ok())
            .count()
    }

    /// Whether the fan-out was **partial** — at least one underlying committed and
    /// at least one did not. An all-committed or (degenerate) all-failed fan-out is
    /// not partial.
    #[must_use]
    pub fn is_partial(&self) -> bool {
        let committed = self.committed_count();
        committed != 0 && committed != self.per_underlying.len()
    }
}

/// The summary of one [`AppState::run_expiry_roll`] pass — how many expirations were
/// advanced to each phase and how many sequenced commands were issued.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExpiryRollReport {
    /// Expirations advanced to `Settling` on this roll.
    pub settling: usize,
    /// Expirations advanced to `Expired` on this roll.
    pub expired: usize,
    /// Sequenced commands (`MassCancel` / `SetInstrumentStatus`) **committed** this
    /// roll (a rejected command is not counted and does not advance a phase).
    pub commands_issued: usize,
}

/// One expiration the scheduled roll could **not** advance because a required
/// sequenced command — the scoped [`MassCancel`](crate::exchange::VenueCommand::MassCancel)
/// (incl. `GTC`) or a
/// [`SetInstrumentStatus`](crate::exchange::VenueCommand::SetInstrumentStatus) — was
/// rejected on the sequenced order path (#47). The expiration is **left at its prior
/// operational phase** so a later roll retries it; it is never recorded `Settling` /
/// `Expired` while resting orders may remain live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiryRollFailure {
    /// The underlying whose expiration could not advance.
    pub underlying: String,
    /// The expiration's UTC-day identity instant in **ms** (the group key), a pure
    /// function of the expiration's `ExpirationDate::DateTime` — never a wall clock.
    pub expiration_ms: i64,
    /// The operational phase the roll was attempting to reach and did **not**.
    pub attempted_phase: ExpiryPhase,
    /// The **redacted**, client-safe rejection message from the sequenced submit —
    /// the reason the required command did not commit. Never a secret or a cause chain.
    pub reason: String,
}

/// A **partial** scheduled-expiry roll (#47): at least one expiration's required
/// sequenced command was rejected, so that expiration was **not** advanced.
///
/// The operational phase (`Settling` / `Expired`) advances **only after every
/// required sequenced command for that expiration commits**; a rejected `MassCancel`
/// or `SetInstrumentStatus` leaves the expiration at its prior phase, never marking
/// it `Expired` while resting orders remain live
/// ([05 §10](../docs/05-microstructure-config.md#10-halt-scenarios)). The error carries
/// the summary of the expirations that *did* fully commit ([`ExpiryRollReport`])
/// alongside the typed list of the expirations that did **not**, so the caller retries
/// the roll rather than treating a falsely-advanced instrument as settled.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExpiryRollError {
    /// One or more expirations could not advance because a required sequenced command
    /// was rejected. Carries the committed summary and the per-expiration failures, in
    /// the roll's deterministic sorted order.
    #[error(
        "scheduled expiry roll partially applied: {} expiration(s) not advanced",
        .failures.len()
    )]
    Partial {
        /// The expirations that fully committed and advanced on this roll.
        report: ExpiryRollReport,
        /// The expirations left un-advanced (a required command was rejected), in the
        /// roll's deterministic sorted order.
        failures: Vec<ExpiryRollFailure>,
    },
}

/// Whether `command` is **venue-global** — it names no single routable underlying
/// and so fans out to every hosted underlying's actor (#47): a `MarketMakerControl`,
/// an `EvictExpiredOrders`, or a hierarchy-wide (non-`Book`) `MassCancel`. A `Book`
/// mass cancel names one instrument and routes per-underlying; a `Clock` advance
/// enters through the clock coordinator, not this raw submit path, so neither is
/// venue-global here.
#[must_use]
fn is_venue_global(command: &VenueCommand) -> bool {
    match command {
        VenueCommand::MarketMakerControl { .. } | VenueCommand::EvictExpiredOrders { .. } => true,
        VenueCommand::MassCancel { scope, .. } => !matches!(scope, MassCancelScope::Book(_)),
        _ => false,
    }
}

/// One order swept by a client [`VenueCommand::MassCancel`], paired with the
/// `underlying_sequence` of the sweeping command **on that order's underlying** —
/// the join key a per-order render needs (the FIX `ExecutionReport (8)`
/// `SecondaryExecID (527)`, and the composite `ExecID` grammar) that a bare
/// [`CancelledLeg`] does not carry (#97).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweptLeg {
    /// The swept order's leg — its venue order id, STP owner, and cancel reason.
    pub leg: CancelledLeg,
    /// The sweeping `MassCancel` command's `underlying_sequence` on this order's
    /// underlying — distinct per underlying in a venue-global fan.
    pub sequence: SequenceNumber,
}

/// The result of a client [`VenueCommand::MassCancel`]: the **complete** set of
/// swept legs aggregated across every underlying the sweep reached, paired with
/// the fan-out delivery summary (#97).
///
/// The `fanout` is the honest delivery signal: a venue-global sweep fans to every
/// hosted underlying's actor, and the venue makes **no** promise of atomic
/// venue-wide fan-out (there is no venue-wide total order), so `ok_count < total`
/// is a real, reportable state. A gateway MUST surface a non-`fully_applied`
/// delivery rather than present a partial sweep as an unqualified success — some
/// underlyings' live orders may remain. A `Book`-scoped sweep names one
/// instrument → one actor, so its delivery is `{ ok_count: 1, total: 1 }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MassCancelDelivery {
    /// The swept legs, in the deterministic sorted-underlying then venue-id sweep
    /// order. Its length is the caller's true cancelled count.
    pub swept: Vec<SweptLeg>,
    /// How the venue-global fan-out delivered across underlyings.
    pub fanout: FanoutSummary,
}

/// The swept legs captured on a committed `MassCancel` receipt (each tagged with
/// the receipt's `underlying_sequence`), or empty for any other outcome (a
/// rejected sweep carries no legs). Cloned rather than moved so the fan can
/// aggregate across receipts without consuming them.
#[must_use]
fn swept_legs_of(receipt: &Receipt) -> Vec<SweptLeg> {
    match &receipt.outcome {
        Some(VenueOutcome::MassCancelled { affected }) => affected
            .iter()
            .map(|leg| SweptLeg {
                leg: leg.clone(),
                sequence: receipt.underlying_sequence,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Rebuilds the shared executions / positions / mark state for a **recovered**
/// underlying (#85) by replaying its re-derived [`VenueEvent`] stream through the
/// **same** [`StoreFanOut`] projection the live post-journal fan-out uses.
///
/// [`recover_into`] re-executes onto the reconstruction executor **without** a
/// fan-out (it rebuilds only the book), so the shared executions log and positions
/// fold start empty; this replays each recovered event once — exactly as the live
/// path emits each committed event once — so the folds match a never-restarted run.
/// Only the store fan-out is driven (executions / positions / marks); the WS tee is
/// **not**, because boot has no subscribers and historical events are not live
/// market data. `marks` warms to the last recovered trade (a live-only, recomputed
/// value, never journaled — outside the determinism oracle).
///
/// **Fallible (#85 review).** A projection failure during this rebuild is surfaced
/// (`Err(FanOutSealed)`), not swallowed, so [`AppState::new`] fails startup on a
/// partially-rebuilt store rather than begin serving authoritative stores that do
/// not match the recovered journal (#131 made `StoreFanOut::emit` fallible).
fn rebuild_stores_from_events<E: ExecutionsStore, P: PositionsStore>(
    executions: &Arc<E>,
    positions: &Arc<P>,
    marks: &Arc<MarkPriceBook>,
    events: &[VenueEvent],
) -> Result<(), FanOutSealed> {
    let mut store_fan_out = StoreFanOut::new(
        Arc::clone(executions),
        Arc::clone(positions),
        Arc::clone(marks),
    );
    for event in events {
        store_fan_out.emit(event)?;
    }
    Ok(())
}

/// Whether `command` is **bufferable ingress** — a client order-entry command
/// (`AddOrder` / `CancelOrder` / `Replace`) that carries a `(session_id, msg_seq)`
/// identity and is subject to the deterministic ingress-reorder buffer (#111). Every
/// other command (venue-global controls, `Clock`, `SimStep`, instrument status, a
/// scoped mass cancel) enters the sequencer directly on the FIFO path — it carries
/// no client latency and must not be held behind an arrival deadline.
#[must_use]
fn is_bufferable_ingress(command: &VenueCommand) -> bool {
    matches!(
        command,
        VenueCommand::AddOrder { .. }
            | VenueCommand::CancelOrder { .. }
            | VenueCommand::Replace { .. }
    )
}

/// Resolves the target underlying ticker of a **bufferable** ingress command by
/// parsing its symbol through the upstream [`SymbolParser`] grammar (#111). A
/// non-parsing symbol is an [`VenueError::InvalidOrder`]; a non-bufferable command
/// never reaches here (its caller guards with [`is_bufferable_ingress`]).
fn bufferable_underlying(command: &VenueCommand) -> Result<String, VenueError> {
    let symbol = match command {
        VenueCommand::AddOrder { symbol, .. }
        | VenueCommand::CancelOrder { symbol, .. }
        | VenueCommand::Replace { symbol, .. } => symbol,
        _ => {
            return Err(VenueError::InvalidOrder(
                "command is not bufferable ingress".to_string(),
            ));
        }
    };
    let parsed = SymbolParser::parse(symbol.as_str())
        .map_err(|error| VenueError::InvalidOrder(error.to_string()))?;
    Ok(parsed.underlying().to_string())
}

// ============================================================================
// Ingress reorder buffer wiring (#111)
// ============================================================================

/// One buffered client order awaiting release into its underlying's actor: the
/// journaled [`VenueCommand`] and the [`oneshot`] the caller of
/// [`AppState::submit_with_ingress`] is awaiting. The reply carries the actor's
/// [`Receipt`] once the command is released (the clock strictly passed its deadline)
/// and sequenced.
struct PendingIngress {
    /// The command to release onto the sequenced order path in deadline order.
    command: VenueCommand,
    /// The reply the buffered caller awaits — filled when the command is released
    /// and the actor returns its [`Receipt`].
    reply: oneshot::Sender<Result<Receipt, VenueError>>,
}

/// The per-underlying ingress-reorder channel (#111): the bounded deadline-ordered
/// buffer, a release lock serializing releases into the actor (so it **receives**
/// commands in deadline order), and the actor handle releases are forwarded onto.
///
/// The buffer is guarded by a **std** mutex held only for the O(log n) insert/drain —
/// never across an `.await`. The `release_lock` is a **tokio** mutex held across the
/// forward `.await` on purpose: it is off the sequenced path and guards ingress
/// **ordering** (not a book or the matching hot path), so at most one release runs
/// per underlying and the actor's FIFO mailbox preserves deadline order.
struct IngressChannel {
    /// The bounded, deadline-ordered arrival buffer for this underlying.
    buffer: std::sync::Mutex<IngressReorderBuffer<PendingIngress>>,
    /// Serializes releases so the actor receives released commands in deadline order.
    release_lock: AsyncMutex<()>,
    /// The single-writer actor released commands are forwarded onto.
    handle: ActorHandle,
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
    /// request-path cost), and the [`AuthService`] is pinned to the shared venue
    /// [`SimClock`] (the same advancing clock the actors stamp `venue_ts` from,
    /// #028), with the registry (as [`RevocationOracle`]) as its oracle.
    ///
    /// # Panics
    ///
    /// Must be called within a `tokio` runtime — it spawns the actor tasks; the
    /// spawn panics outside a runtime, matching
    /// [`spawn_matching_actor_with_registry_and_index`].
    ///
    /// # Errors
    ///
    /// [`AppStateError::Auth`] if auth cannot be built: the embedded dev fixtures
    /// fail to parse ([`AuthError::KeyLoad`]), a provisioned password cannot be
    /// hashed ([`AuthError::PasswordHash`]), or two accounts collide on an id or FIX
    /// username ([`AuthError::Provisioning`]). [`AppStateError::Db`] if
    /// `DATABASE_URL` is set but a per-underlying durable journal store cannot be
    /// opened (its header row cannot be ensured).
    pub fn new(config: AppStateConfig) -> Result<Arc<Self>, AppStateError> {
        let AppStateConfig {
            underlyings,
            lineage_id,
            mailbox_capacity,
            clock: clock_config,
            seed,
            auth,
            assets,
            simulation,
            db,
            start_serving,
            microstructure,
            ingress_buffer_capacity,
        } = config;

        // The one shared venue microstructure (#044): the SAME resolved config is
        // applied to every underlying's book at creation, and its fingerprint is
        // recorded in the run manifest so a replay scopes fee/STP/specs-sensitive
        // reproduction. Held behind an `Arc` shared into the actors' book-creation
        // and the admission seam.
        let microstructure = Arc::new(microstructure);

        // The one shared venue clock (#028): the rate limiter, every actor's
        // `venue_ts`, and the simulator's `SimStep.now_ms` all read this SAME
        // advancing clock, so a single seeded clock decides every timestamp and
        // replay reuses the recorded value. Cloned as a cheap `Arc` handle into
        // each consumer; advancing happens off the sequenced read.
        let clock = SimClock::new(clock_config);
        // Record the microstructure fingerprint alongside the seed + clock mode: the
        // config manifest is part of the determinism tuple, so an exported bundle's
        // config is gated against this recorded fingerprint before replay.
        let manifest = RunManifest::new(seed, clock.mode())
            .with_microstructure_fingerprint(microstructure.fingerprint());

        // Build auth FIRST (the only fallible step): a `None` auth defaults to the
        // embedded dev key pair, then provision the registry and assemble the
        // service pinned to the venue clock. No actor is spawned until this holds.
        let AuthConfig {
            jwt,
            bootstrap_secret,
            pepper,
            accounts,
            rate_limit,
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
            // The per-tier venue rate-limit budgets (#046) on the shared venue
            // clock, so throttling replays deterministically.
            RateLimiter::with_budgets(clock.clone(), rate_limit),
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

        // The one shared cross-session `(account, ClOrdID) → order_id` index (#098):
        // every underlying's ACTOR publishes successful placements into this SAME
        // instance POST-journal on the sequenced path, and the FIX/REST surfaces
        // resolve from it, so a client cancels/replaces an order it placed in a prior
        // session. It is a derived artifact rebuilt from the journal on #085 recovery
        // (both the boot re-execution below and the live actor use the identical
        // deterministic derivation), never a second durable source of truth.
        let clordid_index = Arc::new(ClOrdIdIndex::with_default_ceiling());

        // The WebSocket market-data service: one shared bounded broadcast + the
        // per-instrument sequence/aggregate, fed by a `WsFanOut` teed alongside
        // each actor's `StoreFanOut` (both consume the SAME post-journal event).
        let subscriptions = Arc::new(OrderbookSubscriptionManager::new());

        // The late-bound market-maker control hub (#047): created BEFORE the actors
        // (which take it as their sequenced control sink) and bound to the engine
        // once it is constructed with their handles. Installed only on the live path;
        // the replay/recovery executors carry no sink, so a `MarketMakerControl`
        // replays as an identical `ControlApplied` without a live engine.
        let mm_control_hub = MarketMakerControlHub::new();
        let mm_control_sink: Arc<dyn MarketMakerControlSink> = Arc::clone(&mm_control_hub) as _;

        // ==================================================================
        // Boot-time journal recovery (#85)
        // ==================================================================
        //
        // Pass 1 — PROBE + RECONSTRUCT. For each configured underlying (deduped),
        // read its durable journal header; a **non-empty** stream is reconstructed
        // HERE by re-execution ([`recover_into`], the SAME single reducer the replay
        // driver uses — stored event = integrity oracle) into a shared-registry
        // executor, so recovered leaves vivify onto the venue-wide symbol index
        // exactly as a live underlying's do. An absent/empty stream is a fresh boot
        // (today's path, unchanged). Recovery runs BEFORE any actor is spawned so the
        // venue can (a) resolve ONE rehydrated run lineage from the persisted
        // stream(s) and (b) **fail-stop** on a corrupt / newer-schema / unreadable
        // journal rather than silently start fresh over durable history
        // (ADR-0004 restart recovery, ADR-0006 §recovery, docs/02 §9).
        struct RecoveredUnderlying {
            recovered: Recovered,
            persisted_lineage: LineageId,
        }
        let mut ordered: Vec<Arc<str>> = Vec::with_capacity(underlyings.len());
        let mut seen: std::collections::HashSet<Arc<str>> =
            std::collections::HashSet::with_capacity(underlyings.len());
        let mut recovered_map: HashMap<Arc<str>, RecoveredUnderlying> = HashMap::new();
        for underlying in underlyings {
            let ticker: Arc<str> = Arc::from(underlying);
            if !seen.insert(Arc::clone(&ticker)) {
                tracing::warn!(
                    underlying = %ticker,
                    "duplicate underlying in AppStateConfig; skipping (no second writer)"
                );
                continue;
            }
            ordered.push(Arc::clone(&ticker));

            // Recovery is only possible on the durable path — the in-memory journal
            // never outlives the process, so a fresh in-memory venue is always a
            // fresh boot.
            let Some(pool) = db.as_ref() else {
                continue;
            };
            match PgVenueJournal::open_for_recovery(pool, ticker.as_ref()) {
                Ok(recovery_journal) => {
                    // Build the reconstruction book with the SHARED venue registry +
                    // index and the venue microstructure applied at creation — the
                    // SAME wiring the live/fresh path uses — so recovered instruments
                    // are venue-visible and re-execution reproduces the recorded
                    // fees/fills/events (no market-maker control sink: recovery runs
                    // WITHOUT it, so a `MarketMakerControl` re-derives an identical
                    // `ControlApplied` with no live engine).
                    let executor = MatchingExecutor::new_with_registry_and_index(
                        ticker.as_ref(),
                        Arc::clone(&registry),
                        Arc::clone(&symbol_index),
                        &microstructure,
                    )?;
                    // Rebuild the shared cross-session `(account, ClOrdID) → order_id`
                    // index (#098) as recovery re-executes: `recover_into` applies the
                    // SAME deterministic `apply_committed_correlation` the live actor
                    // publishes post-journal, so the resumed venue resolves the same
                    // client ids it did before the restart (#085 boot recovery).
                    let recovered = recover_into(
                        &recovery_journal,
                        executor,
                        Some(&*microstructure),
                        Some(&clordid_index),
                    )
                    .map_err(|source| AppStateError::Recovery {
                        underlying: ticker.to_string(),
                        source,
                    })?;
                    // A header row with NO records is an earlier fresh open (or a
                    // crash before the first command) — treated as a fresh boot (seed
                    // applies, actor at `SequenceNumber::START`), preserving today's
                    // empty-journal behavior.
                    if recovered.last_sequence.is_some() {
                        let persisted_lineage = recovery_journal.header().lineage_id.clone();
                        tracing::info!(
                            underlying = %ticker,
                            last_sequence = recovered.last_sequence.map(SequenceNumber::get),
                            events = recovered.events.len(),
                            "resumed underlying from a non-empty durable journal"
                        );
                        recovered_map.insert(
                            Arc::clone(&ticker),
                            RecoveredUnderlying {
                                recovered,
                                persisted_lineage,
                            },
                        );
                    }
                }
                // No header row → nothing durable to recover → fresh boot.
                Err(DbError::ValueRange {
                    field: "journal header",
                }) => {}
                // Any other durable read failure at boot is fatal — refuse to serve
                // rather than start fresh over a stream that could not be read.
                Err(other) => return Err(AppStateError::Db(other)),
            }
        }

        // Resolve ONE venue run lineage: the persisted lineage every recovered
        // underlying agrees on (rehydrated so continued + market-maker + simulator
        // ids all stay in the recovered run's namespace), or the config lineage on a
        // fresh boot. Disagreeing persisted lineages are a contradictory durable
        // manifest — fail-stop, never a split id namespace. Iterates the ordered
        // ticker list (deterministic), point-looking-up the map (never iterating it).
        let mut resolved_lineage: Option<(Arc<str>, LineageId)> = None;
        for ticker in &ordered {
            if let Some(entry) = recovered_map.get(ticker) {
                match &resolved_lineage {
                    None => {
                        resolved_lineage =
                            Some((Arc::clone(ticker), entry.persisted_lineage.clone()));
                    }
                    Some((first_ticker, first_lineage))
                        if *first_lineage != entry.persisted_lineage =>
                    {
                        return Err(AppStateError::RecoveryLineageConflict {
                            first_underlying: first_ticker.to_string(),
                            first_lineage: first_lineage.as_str().to_string(),
                            underlying: ticker.to_string(),
                            lineage: entry.persisted_lineage.as_str().to_string(),
                        });
                    }
                    Some(_) => {}
                }
            }
        }
        let lineage_id = match resolved_lineage {
            Some((_, persisted)) => {
                tracing::info!(
                    lineage = %persisted.as_str(),
                    "rehydrated run lineage from the durable journal"
                );
                persisted
            }
            None => lineage_id,
        };

        // Pass 2 — SPAWN. One single-writer actor per underlying: RECOVERED
        // (continuing the journaled `underlying_sequence` over the reconstructed
        // book) or FRESH (at `SequenceNumber::START`).
        let mut handles: HashMap<Arc<str>, ActorHandle> = HashMap::with_capacity(ordered.len());
        let mut recovered_set: std::collections::HashSet<Arc<str>> =
            std::collections::HashSet::new();
        for ticker in ordered {
            // The fan-out tees the committed event into BOTH the shared stores
            // (`StoreFanOut`, #008) and the WS market-data broadcast (`WsFanOut`,
            // #014) — one post-journal event, two consumers, neither on the
            // order-path critical section.
            let fan_out = TeeFanOut::new(
                StoreFanOut::new(
                    Arc::clone(&executions),
                    Arc::clone(&positions),
                    Arc::clone(&marks),
                ),
                WsFanOut::new(Arc::clone(&subscriptions)),
            );
            let header = JournalHeader::new(lineage_id.clone());

            let (handle, join) = match recovered_map.remove(&ticker) {
                Some(RecoveredUnderlying { recovered, .. }) => {
                    let Recovered {
                        events,
                        executor,
                        last_sequence,
                    } = recovered;
                    // Only non-empty streams were inserted, so `last_sequence` is
                    // `Some`; a defensive `ok_or` keeps the prod path free of
                    // `unwrap`/`expect`.
                    let last = last_sequence.ok_or_else(|| AppStateError::Recovery {
                        underlying: ticker.to_string(),
                        source: JournalError::Backend {
                            operation: "recovery last_sequence missing on a non-empty stream",
                        },
                    })?;
                    // Continue PAST the last journaled sequence — never reset, never
                    // wrap (a wrapped sequence corrupts gap detection and replay).
                    let start = last.checked_next().ok_or_else(|| {
                        AppStateError::RecoverySequenceExhausted {
                            underlying: ticker.to_string(),
                            sequence: last.get(),
                        }
                    })?;
                    // Rebuild the shared executions / positions / mark state from the
                    // recovered events: recovery re-executes onto the executor WITHOUT
                    // fan-out, so the stores start empty until we replay the events
                    // through the SAME projection the live post-journal fan-out uses.
                    // Only the store fan-out is driven — NOT the WS tee: boot has no
                    // subscribers and historical events are not live market data.
                    // A projection failure here means the rebuilt stores would not
                    // match the recovered journal — fail startup rather than begin
                    // serving a partially-rebuilt authoritative store (#85 review /
                    // #131).
                    rebuild_stores_from_events(&executions, &positions, &marks, &events).map_err(
                        |sealed| AppStateError::RecoveryProjectionFailed {
                            underlying: ticker.to_string(),
                            projection: sealed.projection,
                            detail: sealed.detail,
                        },
                    )?;
                    // Install the live market-maker control seam now that recovery
                    // (which runs WITHOUT it) is done, so a post-resume
                    // `MarketMakerControl` takes effect on the sequenced path.
                    let executor = executor.with_mm_control_sink(Arc::clone(&mm_control_sink));
                    // Open the durable WRITE journal with the REHYDRATED header — the
                    // persisted lineage matches the stored one, so `open` verifies the
                    // header rather than conflicting on it (#112). The recovered arm is
                    // only reachable on the durable path, but resolve `db` typed rather
                    // than `unwrap`.
                    let pool = db.as_ref().ok_or(AppStateError::Db(DbError::Unavailable))?;
                    let journal = PgVenueJournal::open(pool, ticker.as_ref(), header)?;
                    let actor_config =
                        ActorConfig::new(Arc::clone(&ticker), lineage_id.clone(), mailbox_capacity)
                            .with_start_sequence(start);
                    recovered_set.insert(Arc::clone(&ticker));
                    // Spawn with the reconstructed executor directly (its registry /
                    // index / microstructure were wired at recovery-build time), and
                    // give the ACTOR the shared cross-session ClOrdID index (#098) so
                    // its FUTURE live commands publish correlations post-journal — the
                    // recovered stream's correlations were already rebuilt during
                    // `recover_into` above (the identical deterministic derivation).
                    spawn_underlying_actor_with_clordid_index(
                        actor_config,
                        journal,
                        executor,
                        fan_out,
                        clock.clone(),
                        Some(Arc::clone(&clordid_index)),
                    )
                }
                None => {
                    // Fresh boot (today's path, unchanged): swap the STORE, not the
                    // contract — durable `PgVenueJournal` (#029) when `DATABASE_URL` is
                    // set, in-memory otherwise; both are the SAME `VenueJournal` trait,
                    // so the write-ahead turn discipline is identical. The venue
                    // microstructure (#044) is applied at book creation, before any leaf
                    // is vivified.
                    let actor_config =
                        ActorConfig::new(Arc::clone(&ticker), lineage_id.clone(), mailbox_capacity);
                    match db.as_ref() {
                        Some(pool) => {
                            let journal = PgVenueJournal::open(pool, ticker.as_ref(), header)?;
                            spawn_matching_actor_with_registry_and_index(
                                actor_config,
                                journal,
                                fan_out,
                                clock.clone(),
                                Arc::clone(&registry),
                                Arc::clone(&symbol_index),
                                &microstructure,
                                // The market-maker control apply seam (#047): a
                                // committed `MarketMakerControl` pushes its knobs onto
                                // the engine through the late-bound hub, inside the
                                // actor turn.
                                Some(Arc::clone(&mm_control_sink)),
                                // The shared cross-session ClOrdID index (#098): the
                                // SAME instance for every underlying, so a placement on
                                // any book is cross-session correlatable from any
                                // FIX/REST surface. Held by the actor, published
                                // post-journal.
                                Some(Arc::clone(&clordid_index)),
                            )?
                        }
                        None => {
                            let journal = InMemoryVenueJournal::new(header);
                            spawn_matching_actor_with_registry_and_index(
                                actor_config,
                                journal,
                                fan_out,
                                clock.clone(),
                                Arc::clone(&registry),
                                Arc::clone(&symbol_index),
                                &microstructure,
                                Some(Arc::clone(&mm_control_sink)),
                                Some(Arc::clone(&clordid_index)),
                            )?
                        }
                    }
                }
            };
            // Detach: the actor's shutdown is its mailbox closing when this handle
            // drops with `AppState`; the mailbox drains its backlog first.
            drop(join);
            handles.insert(ticker, handle);
        }

        // The per-underlying ingress reorder buffers (#111): one bounded,
        // deadline-ordered arrival buffer per actor, each holding a clone of that
        // underlying's handle so a released command is forwarded onto the SAME
        // sequenced order path client orders take. Built from the completed `handles`
        // map (one channel per hosted underlying) BEFORE the map is cloned into the
        // market-maker / simulator sinks. On the FIFO fast path (latency injection
        // off) these buffers stay empty and untouched.
        let ingress: HashMap<Arc<str>, Arc<IngressChannel>> = handles
            .iter()
            .map(|(ticker, handle)| {
                (
                    Arc::clone(ticker),
                    Arc::new(IngressChannel {
                        buffer: std::sync::Mutex::new(IngressReorderBuffer::new(
                            ingress_buffer_capacity,
                        )),
                        release_lock: AsyncMutex::new(()),
                        handle: handle.clone(),
                    }),
                )
            })
            .collect();

        // The market-maker engine (#015): its requotes enter the SAME per-underlying
        // actors as client orders, through an `ActorCommandSink` over cloned actor
        // handles — so generated liquidity is journaled and replayable, never a
        // direct book mutation. The sink's forwarder task is detached (its lifetime
        // is the `AppState`'s, like the actors').
        let market_maker = Arc::new(
            MarketMakerEngine::new(
                // The requote sink admits each generated quote against the venue
                // price band (#109) with the SAME resolved config the submit seam
                // uses, so a band-violating requote never rests on a leaf.
                ActorCommandSink::new(handles.clone(), Arc::clone(&microstructure)),
                lineage_id.clone(),
                Quoter::default(),
            )
            // The run-level seed the persona-jitter sub-stream derives from (#047), so
            // persona jitter is reproducible for a fixed seed and replays identically.
            .with_run_seed(seed),
        );

        // Bind the control hub to the engine now that it exists: from here a
        // sequenced `MarketMakerControl` applies its knobs live. The bind is
        // synchronous and precedes serving, so no control reaches an unbound hub.
        mm_control_hub.bind(Arc::clone(&market_maker));

        // The price simulator (#016): each walked (or overridden) price step routes
        // through a `VenueStepSink` onto the SAME per-underlying actors as client
        // orders (a journaled `SimStep`) and drives the market maker, so synthetic
        // prices and the requotes they induce are journaled and replayable. The
        // interval loop is not auto-started (a stepped-clock / bootstrap concern);
        // the wiring is in place and every served step is journaled through the sink.
        let simulator = PriceSimulator::new(
            assets,
            simulation,
            // The step sink admits each simulated reference price against the venue
            // band (#109) with the SAME resolved config the submit seam uses, so an
            // out-of-band sim price is rejected before it is sequenced or requotes.
            VenueStepSink::new(
                handles.clone(),
                Arc::clone(&market_maker),
                Arc::clone(&microstructure),
            ),
            clock.clone(),
        );

        tracing::info!(
            underlyings = handles.len(),
            recovered = recovered_set.len(),
            accounts = account_registry.account_count(),
            durable = db.is_some(),
            manifest = %manifest.summary(),
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
            market_maker,
            simulator,
            db,
            serving: AtomicBool::new(start_serving),
            clock,
            manifest,
            correlation_counter: AtomicU64::new(0),
            recording: RecordingController::default(),
            microstructure,
            expiry_phases: std::sync::Mutex::new(std::collections::HashMap::new()),
            recovered: recovered_set,
            clordid_index,
            ingress,
            arrival_counter: AtomicU64::new(0),
            rest_ingress_counter: AtomicU64::new(0),
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
    /// cancel). **Venue-global** commands that carry no single underlying — a
    /// `MarketMakerControl`, an `EvictExpiredOrders`, and a hierarchy-wide (non-`Book`)
    /// `MassCancel` — **fan out** to every hosted underlying's actor, each journaled
    /// in its own stream (mirroring the [`advance_clock_step`](Self::advance_clock_step)
    /// coordinator); a `Clock` still enters through that clock coordinator, not this
    /// raw submit path.
    ///
    /// A fan-out returns a **representative** [`Receipt`] (the last committed
    /// underlying's) carrying a [`FanoutSummary`](crate::exchange::FanoutSummary)
    /// in [`Receipt::fanout`]; a **partial** fan-out (committed on some underlyings,
    /// not others) is both logged (`WARN`) and surfaced through that summary
    /// (`ok_count` / `total` / `fully_applied`) so a control-plane response reports
    /// it rather than hiding it — the venue does not promise atomic venue-wide
    /// fan-out (#118).
    ///
    /// A **sequenced market-maker kill** (`MarketMakerControl { enabled: Some(false),
    /// .. }`) is just a normal venue-global control here: it needs **no** separate
    /// follow-on command. The executor **couples** the owner-scoped market-maker
    /// sweep into the kill control's own sequenced turn, per underlying, so each
    /// underlying journals **one** event that both applies the control and cancels
    /// the maker's standing quotes ([`VenueOutcome::ControlApplied`](crate::exchange::VenueOutcome::ControlApplied)
    /// carrying the swept legs). That is crash-consistent (there is no cross-command
    /// gap a crash could open between "control applied" and "quotes cancelled") and
    /// reports one fan-out receipt (nothing discarded) — see the executor's
    /// `MarketMakerControl` arm (#117).
    ///
    /// # Errors
    ///
    /// - [`VenueError::InvalidOrder`] if the command's symbol does not parse, the
    ///   command carries no routable underlying, or the order price falls outside the
    ///   venue-owned price band (#044);
    /// - [`VenueError::NotFound`] if the underlying is not hosted by this venue (or a
    ///   venue-global command is submitted to a venue hosting no underlyings);
    /// - the actor's own typed rejection ([`VenueError::RateLimited`] on a full
    ///   mailbox, [`VenueError::JournalUnavailable`] if the actor has stopped, or
    ///   a sequencing seal) otherwise.
    pub async fn submit(&self, command: VenueCommand) -> Result<Receipt, VenueError> {
        // The venue-owned price-band admission cap (#044) is checked BEFORE the
        // command reaches the sequencer, so an over-band order is rejected at the
        // gateway and never journaled — replay never re-executes a price the live
        // venue refused.
        self.admit_command_price(&command)?;
        if is_venue_global(&command) {
            return self.submit_venue_global(command).await;
        }
        let handle = self.route(&command)?;
        handle.submit(command).await
    }

    /// Submits a client order-entry command through the **deterministic
    /// ingress-reorder buffer** (#111) — the gateway-edge entry point that applies
    /// the seeded [`LatencyOffset`](crate::microstructure::LatencyOffset) *before*
    /// the sequencer, so a slow client (a large drawn offset) can lose the queue
    /// race to a later-arriving fast one
    /// ([03 §6.1](../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering)).
    ///
    /// `stamp` is the message's `(session_id, msg_seq)` identity (FIX
    /// `(SenderCompID, MsgSeqNum)`, REST `(account, request-seq)`), which keys the
    /// #45 seeded draw — the same identity always draws the same offset, so the
    /// reorder is reproducible for a fixed seed + config + input stream.
    ///
    /// **Fast path.** When latency injection is off (the default) or `command` is not
    /// a bufferable client order, this is byte-identical to [`submit`](Self::submit):
    /// plain FIFO onto the actor, no buffering, no reorder, no regression. The
    /// reorder buffer is engaged **only** when latency is configured **and** the
    /// command is an `AddOrder` / `CancelOrder` / `Replace`.
    ///
    /// # Errors
    ///
    /// The same typed rejections as [`submit`](Self::submit), plus
    /// [`VenueError::RateLimited`] when the bounded ingress buffer is at capacity (a
    /// flood / hostile-offset **drop**, never unbounded growth), and
    /// [`VenueError::JournalUnavailable`] if the venue shuts down while the command
    /// is still buffered awaiting release.
    pub async fn submit_with_ingress(
        &self,
        command: VenueCommand,
        stamp: IngressStamp,
    ) -> Result<Receipt, VenueError> {
        if self.microstructure.latency().is_enabled() && is_bufferable_ingress(&command) {
            self.submit_reordered(command, stamp).await
        } else {
            // FIFO fast path: no latency injection or a non-order-entry command. The
            // ingress metadata is unused here (it only keys the seeded draw + the
            // deadline tie-break, both of which are the reorder path's concern).
            let _ = stamp;
            self.submit(command).await
        }
    }

    /// The buffered ingress path: admit the price band, draw the seeded offset,
    /// compute the release deadline, hold the command in its underlying's
    /// deadline-ordered buffer, and await the actor's [`Receipt`] once the venue
    /// clock strictly passes that deadline and the command is released in order.
    ///
    /// The ordering **rule** never calls `SystemTime`: the deadline is
    /// `venue_now_at_arrival + clamped LatencyOffset` (the venue clock, which is
    /// wall-fed under a realtime clock) and the tie-break is
    /// `(session_id, arrival_sequence)` on a checked monotonic counter. Live
    /// run-to-run reproducibility of the reorder holds under a controlled clock;
    /// **replay is deterministic regardless** (see [`crate::microstructure::ingress`]).
    /// A buffered order's completion is gated on the venue clock **advancing past its
    /// deadline**, not just on admission — under a stepped clock that never advances,
    /// a buffered caller awaits until the clock steps.
    async fn submit_reordered(
        &self,
        command: VenueCommand,
        stamp: IngressStamp,
    ) -> Result<Receipt, VenueError> {
        // Admit the venue price band at the gateway edge, BEFORE buffering — an
        // over-band order is rejected here and never buffered, never journaled
        // (mirrors `submit`'s admission; a released command bypasses that seam).
        self.admit_command_price(&command)?;
        let underlying = bufferable_underlying(&command)?;
        let channel = self
            .ingress
            .get(underlying.as_str())
            .cloned()
            .ok_or_else(|| {
                VenueError::NotFound(format!(
                    "underlying '{underlying}' is not hosted by this venue"
                ))
            })?;

        // The seeded per-message draw (#45) — a pure function of
        // `(run_seed, session_id, msg_seq)`; NOT a fresh RNG here.
        let offset = self.microstructure.latency().draw(
            self.manifest.seed,
            &stamp.session_id,
            stamp.msg_seq,
        );
        // The release deadline on the VIRTUAL clock — the venue instant read at
        // admission plus the clamped offset (checked; a hostile offset is bounded to
        // the horizon so the buffer cannot hold a command forever).
        let now_ms = self.clock.now_ms().get();
        // A range overflow (an astronomically-unreachable venue instant) is a typed
        // rejection, never a manufactured `u64::MAX` deadline that could never be
        // released (#111 review).
        let deadline_us = release_deadline_us(now_ms, offset).map_err(|_| VenueError::Overflow)?;
        // The checked monotonic arrival counter — the tie-break's total-order key.
        let arrival_sequence = self.next_arrival_sequence()?;
        let key = ReleaseKey::new(deadline_us, Arc::clone(&stamp.session_id), arrival_sequence);

        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut buffer = channel
                .buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            buffer
                .insert(
                    key,
                    PendingIngress {
                        command,
                        reply: reply_tx,
                    },
                )
                // Bounded DoS drop: a flood / hostile offset that fills the buffer is
                // a typed throttle, never unbounded growth ([08 §5]).
                .map_err(|_full| {
                    tracing::warn!(
                        underlying = %underlying,
                        session = %stamp.session_id,
                        "ingress reorder buffer at capacity; dropping order (throttled)"
                    );
                    VenueError::RateLimited
                })?;
        }
        tracing::debug!(
            underlying = %underlying,
            session = %stamp.session_id,
            msg_seq = stamp.msg_seq,
            offset_us = offset.micros(),
            deadline_us,
            arrival_sequence,
            "order buffered at the ingress edge; awaiting deadline release"
        );

        // Release anything the clock has already made due (e.g. an earlier advance
        // passed a deadline while this caller was mid-admission). This never releases
        // the just-buffered entry (its deadline is >= now, and release is strict).
        self.release_ingress(&channel).await;

        // Await the committed receipt — filled by whichever release pump (this kick,
        // a clock advance, or the realtime cadence driver) forwards the command once
        // the clock strictly passes its deadline. A dropped sender means the venue is
        // shutting down.
        match reply_rx.await {
            Ok(result) => result,
            Err(_) => Err(VenueError::JournalUnavailable),
        }
    }

    /// Releases every buffered command in one underlying's ingress channel whose
    /// deadline the venue clock has **strictly passed**, forwarding them onto the
    /// actor in deadline order (#111).
    ///
    /// The `release_lock` serializes releases per underlying so the actor's FIFO
    /// mailbox receives the commands in exactly the drained order — the tokio mutex
    /// is held across the forward `.await` on purpose (it is off the sequenced path
    /// and guards ingress **ordering**, not a book). The **std** buffer mutex is
    /// dropped before any `.await` (never held across it). No book is mutated here;
    /// the actor assigns `underlying_sequence` in receipt order and journals it.
    async fn release_ingress(&self, channel: &IngressChannel) {
        let _release = channel.release_lock.lock().await;
        // Snapshot the virtual instant ONCE: the drained batch is exactly the set of
        // entries due at this instant, in strict key order (the release horizon).
        let now_us = self.ingress_now_us();
        let due = {
            let mut buffer = channel
                .buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            buffer.drain_below(now_us)
        };
        for (_key, pending) in due {
            // Forward onto the SAME per-underlying actor client orders take, in
            // deadline order. A closed reply just means the caller went away.
            let receipt = channel.handle.submit(pending.command).await;
            let _ = pending.reply.send(receipt);
        }
    }

    /// Releases due ingress across **every** underlying, in the deterministic sorted
    /// order (#111) — the pump a clock advance (stepped) or the realtime/accelerated
    /// cadence driver runs after the venue clock moves. Each underlying's journal is
    /// independent, so cross-underlying order is immaterial; sorted iteration keeps
    /// it tidy and free of hash-map order. A no-op when every buffer is empty (the
    /// FIFO fast path).
    async fn release_all_ingress(&self) {
        // Clone the channel handles out first so no borrow of `self.ingress` is held
        // across the `.await` in `release_ingress`.
        let channels: Vec<Arc<IngressChannel>> = self
            .underlyings()
            .iter()
            .filter_map(|ticker| self.ingress.get(*ticker).cloned())
            .collect();
        for channel in channels {
            self.release_ingress(&channel).await;
        }
    }

    /// The current venue instant in **microseconds** — the release-horizon clock read
    /// (a pure atomic load promoted `ms → µs`, checked, never a wall-clock read on the
    /// ordering decision). The explicit `checked_mul(..).unwrap_or(u64::MAX)` is the
    /// form the repo rules require over a banned `saturating_*` (mirrors
    /// [`crate::simulation::SimClock::step`]); the clamp is an unreachable fail-safe on
    /// an absurd instant, never a silent wrap.
    #[allow(clippy::manual_saturating_arithmetic)]
    #[inline]
    fn ingress_now_us(&self) -> u64 {
        self.clock
            .now_ms()
            .get()
            .checked_mul(1_000)
            .unwrap_or(u64::MAX)
    }

    /// Mints the next **checked** monotonic arrival sequence (#111) — the
    /// `arrival_sequence` tie-break key. A CAS loop over the venue-wide counter using
    /// `checked_add`, so the counter is never wrapped (a wrapped sequence would
    /// corrupt the tie-break's total order).
    ///
    /// # Errors
    ///
    /// [`VenueError::SequenceExhausted`] at `u64::MAX` arrivals — astronomically
    /// unreachable, but surfaced rather than wrapped, per the checked-arithmetic rule.
    fn next_arrival_sequence(&self) -> Result<u64, VenueError> {
        let mut current = self.arrival_counter.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(1) else {
                return Err(VenueError::SequenceExhausted);
            };
            match self.arrival_counter.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(current),
                Err(observed) => current = observed,
            }
        }
    }

    /// Mints a **REST** ingress stamp (#111) for `account`: the session id is the
    /// account, the `msg_seq` a checked monotonic venue-wide REST counter (REST
    /// carries no native per-message sequence). A fixed request order mints a fixed
    /// stamp sequence, so a REST run's seeded latency draws are reproducible.
    ///
    /// # Errors
    ///
    /// [`VenueError::SequenceExhausted`] at `u64::MAX` REST requests (unreachable;
    /// surfaced rather than wrapped).
    pub fn next_rest_ingress_stamp(&self, account: &AccountId) -> Result<IngressStamp, VenueError> {
        let mut current = self.rest_ingress_counter.load(Ordering::Relaxed);
        let msg_seq = loop {
            let Some(next) = current.checked_add(1) else {
                return Err(VenueError::SequenceExhausted);
            };
            match self.rest_ingress_counter.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break current,
                Err(observed) => current = observed,
            }
        };
        Ok(IngressStamp::new(account.as_str(), msg_seq))
    }

    /// Submits a [`VenueCommand::SetInstrumentStatus`] transition onto the
    /// sequenced order path (#47) — the typed entry point the admin instrument-status
    /// route builds on. It routes by the target **symbol** to that instrument's
    /// underlying actor (a status change targets one instrument, not the whole venue),
    /// so it returns that actor's [`Receipt`].
    ///
    /// # Errors
    ///
    /// The same typed rejections as [`submit`](Self::submit): an unparseable /
    /// cross-underlying symbol ([`VenueError::InvalidOrder`]), an unhosted underlying
    /// ([`VenueError::NotFound`]), or the actor's own sequencing rejection.
    pub async fn submit_set_instrument_status(
        &self,
        symbol: Symbol,
        status: InstrumentStatus,
    ) -> Result<Receipt, VenueError> {
        self.submit(VenueCommand::SetInstrumentStatus { symbol, status })
            .await
    }

    /// Drives **scheduled expiry / roll** at venue-clock `now_ms` (#047): enumerates
    /// every vivified contract, groups by `(underlying, expiration)`, and issues the
    /// sequenced lifecycle transitions each expiration is **due** for through
    /// [`submit`](Self::submit) — a scoped `MassCancel` (incl. `GTC`) then
    /// `SetInstrumentStatus(Settling)` at the operational expiry time, and
    /// `SetInstrumentStatus(Expired)` at settlement, per `schedule`.
    ///
    /// The upstream `ExpiryScheduler` is a **schedule source only**; this driver
    /// issues every transition as a journaled command so a roll replays identically
    /// ([05 §10](../docs/05-microstructure-config.md#10-halt-scenarios)). It tracks the
    /// last phase driven per expiration, so a repeated call only advances **forward**
    /// (idempotent, never a regressive illegal transition). Groups and symbols are
    /// iterated in sorted order, so the emitted command stream is deterministic. No
    /// lock is held across the `submit` `.await`.
    ///
    /// The operational phase for an expiration advances (its `expiry_phases` entry is
    /// written) **only after every required sequenced command for that expiration has
    /// committed** — the scoped `MassCancel` (incl. `GTC`) then the per-symbol
    /// `SetInstrumentStatus`. On the **first** rejected command the driver stops issuing
    /// that expiration's remaining commands and leaves its phase unchanged, so a later
    /// roll retries it; an instrument is **never** recorded `Settling` / `Expired` while
    /// a `MassCancel` or status transition did not commit and resting orders may remain
    /// live. Each expiration is independent — one expiration's rejection never blocks
    /// another's advance.
    ///
    /// `now_ms` is the **venue clock** instant (a venue service, never `SystemTime`).
    ///
    /// # Errors
    ///
    /// [`ExpiryRollError::Partial`] when at least one expiration's required sequenced
    /// command was rejected: the error carries the [`ExpiryRollReport`] of the
    /// expirations that *did* fully commit and the typed [`ExpiryRollFailure`] list
    /// naming each expiration left un-advanced (with the phase it failed to reach), so
    /// the caller retries rather than treating a falsely-advanced instrument as settled.
    pub async fn run_expiry_roll(
        &self,
        schedule: &ExpirySchedule,
        now_ms: i64,
    ) -> Result<ExpiryRollReport, ExpiryRollError> {
        // Group vivified contracts by (underlying, expiration-identity-ms) in sorted
        // order for a deterministic issue sequence.
        let mut groups: BTreeMap<(String, i64), (ExpirationDate, Vec<Symbol>)> = BTreeMap::new();
        for raw in self.symbol_index.symbols() {
            let Ok(parsed) = SymbolParser::parse(&raw) else {
                continue;
            };
            let expiration = *parsed.expiration();
            let key_ms = match &expiration {
                ExpirationDate::DateTime(dt) => dt.timestamp_millis(),
                // A relative `Days` expiry drives no calendar roll (it breaks replay)
                // and is skipped here, never constructed or propagated.
                ExpirationDate::Days(_) => continue, // days-expiry-allow: defensive read-arm
            };
            let Ok(symbol) = Symbol::parse(&raw) else {
                continue;
            };
            let entry = groups
                .entry((parsed.underlying().to_string(), key_ms))
                .or_insert_with(|| (expiration, Vec::new()));
            entry.1.push(symbol);
        }

        let mut report = ExpiryRollReport::default();
        let mut failures: Vec<ExpiryRollFailure> = Vec::new();
        for ((underlying, key_ms), (expiration, mut symbols)) in groups {
            let Some(target) = schedule.phase_at(&expiration, now_ms) else {
                continue;
            };
            symbols.sort_by(|left, right| left.as_str().cmp(right.as_str()));
            let last = {
                let map = self
                    .expiry_phases
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                map.get(&(underlying.clone(), key_ms))
                    .copied()
                    .unwrap_or(ExpiryPhase::PreExpiry)
            };
            if target <= last {
                continue;
            }
            let commands = schedule.transition_commands(&expiration, &symbols, last, target);
            // Issue every required command; the operational phase advances ONLY once
            // all of them commit. On the FIRST rejection we stop issuing this
            // expiration's remaining commands and leave its phase at `last`, so a later
            // roll retries it — never marking it `Settling` / `Expired` while a
            // `MassCancel` or a `SetInstrumentStatus` did not commit (rule 2 / #47).
            let mut committed = 0usize;
            let mut rejection: Option<VenueError> = None;
            for command in commands {
                match self.submit(command).await {
                    Ok(_) => committed = committed.checked_add(1).unwrap_or(committed),
                    Err(error) => {
                        tracing::warn!(
                            underlying = %underlying,
                            attempted_phase = ?target,
                            error = %error,
                            "expiry-roll command rejected — phase not advanced, will retry"
                        );
                        rejection = Some(error);
                        break;
                    }
                }
            }
            report.commands_issued = report
                .commands_issued
                .checked_add(committed)
                .unwrap_or(report.commands_issued);
            if let Some(error) = rejection {
                // A required command did not commit: DO NOT advance the phase. The
                // expiration stays at `last` and this roll is reported partial.
                failures.push(ExpiryRollFailure {
                    underlying,
                    expiration_ms: key_ms,
                    attempted_phase: target,
                    reason: error.redacted_message(),
                });
                continue;
            }
            // Every required command committed — it is now safe to advance the phase.
            self.expiry_phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert((underlying, key_ms), target);
            match target {
                ExpiryPhase::Settling => {
                    report.settling = report.settling.checked_add(1).unwrap_or(report.settling);
                }
                ExpiryPhase::Expired => {
                    report.expired = report.expired.checked_add(1).unwrap_or(report.expired);
                }
                ExpiryPhase::PreExpiry => {}
            }
        }
        if failures.is_empty() {
            Ok(report)
        } else {
            Err(ExpiryRollError::Partial { report, failures })
        }
    }

    /// Evicts every resting order whose intraday `Day` / `Gtd` time-in-force has
    /// expired at venue-clock `now_ms` (#047) — a single journaled, venue-global
    /// [`EvictExpiredOrders`](VenueCommand::EvictExpiredOrders) fanned to every hosted
    /// underlying, so the sweep replays from its journaled `now_ms`.
    ///
    /// # Errors
    ///
    /// The same typed rejections as [`submit`](Self::submit) for a venue-global fan.
    pub async fn evict_expired_orders(&self, now_ms: u64) -> Result<Receipt, VenueError> {
        self.submit(VenueCommand::EvictExpiredOrders {
            now_ms: EventTimestamp::new(now_ms),
        })
        .await
    }

    /// Fans a **venue-global** command (a `MarketMakerControl`, an
    /// `EvictExpiredOrders`, or a hierarchy-wide non-`Book` `MassCancel`) to every
    /// hosted underlying's actor, in the deterministic **sorted** order, each
    /// journaled in its own stream. Returns the last committed [`Receipt`] with a
    /// [`FanoutSummary`](crate::exchange::FanoutSummary) attached in
    /// [`Receipt::fanout`]; a partial fan-out is logged under a shared
    /// [`CorrelationId`] **and** reported through that summary, never hidden. No
    /// borrow of `self` is held across an `.await` — each handle is cloned out first.
    async fn submit_venue_global(&self, command: VenueCommand) -> Result<Receipt, VenueError> {
        let correlation_id = self.next_correlation_id();
        let tickers: Vec<String> = self.underlyings().into_iter().map(str::to_string).collect();
        if tickers.is_empty() {
            return Err(VenueError::NotFound(
                "no hosted underlyings for a venue-global command".to_string(),
            ));
        }
        let total = tickers.len();
        let mut committed: Option<Receipt> = None;
        let mut first_error: Option<VenueError> = None;
        let mut ok_count = 0usize;
        for ticker in &tickers {
            let result = match self.handle_for(ticker) {
                Ok(handle) => handle.submit(command.clone()).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(receipt) => {
                    // Checked (rule 9); the count is bounded by the hosted-underlying
                    // set, so the floor is unreachable but keeps the crate `+=`-free.
                    ok_count = ok_count.checked_add(1).unwrap_or(ok_count);
                    committed = Some(receipt);
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if ok_count != 0 && ok_count != total {
            tracing::warn!(
                correlation_id = %correlation_id,
                committed = ok_count,
                total,
                "venue-global command fan-out was partial across underlyings"
            );
        }
        // Surface the fan-out delivery on the representative receipt (#118): a
        // control-plane response reads `ok_count`/`total`/`fully_applied` from it and
        // reports a partial fan-out rather than an unqualified success. The counts are
        // simple loop totals — no wall-clock, no RNG.
        let summary = FanoutSummary { ok_count, total };
        match (committed, first_error) {
            (Some(receipt), _) => Ok(receipt.with_fanout(summary)),
            (None, Some(error)) => Err(error),
            // Non-empty `tickers` with no Ok and no Err is unreachable; return a
            // typed rejection rather than fabricate a receipt.
            (None, None) => Err(VenueError::JournalUnavailable),
        }
    }

    /// Submits a client-requested [`VenueCommand::MassCancel`] and returns the
    /// **complete** set of swept orders aggregated across every underlying the
    /// sweep reached — the gateway-facing entry point REST `cancel-all` and FIX
    /// `q` share (#97).
    ///
    /// A non-`Book` mass cancel is venue-global ([`is_venue_global`]): it fans to
    /// every hosted underlying's actor, each of which sweeps its own resting
    /// registry filtered by the command's [`MassCancelType`] and journals its own
    /// [`VenueOutcome::MassCancelled`]. The representative-receipt `submit` fan
    /// (#118) surfaces only ONE underlying's outcome — inadequate for a client
    /// cancel-all on a multi-underlying venue — so this method concatenates every
    /// actor's affected legs (each already in its deterministic venue-id sweep
    /// order, iterated over the sorted underlying set) into the account's full
    /// cancelled set. A `Book` scope names one instrument, routes to that
    /// underlying's actor, and returns its swept legs directly.
    ///
    /// Owner scoping ([`MassCancelType::ByUser`]) is enforced **inside each
    /// actor's executor**, so a caller only ever sweeps its OWN resting orders
    /// regardless of scope; cross-account isolation is a property of the sequenced
    /// sweep, not of this fan. Each per-underlying `MassCancel` is journaled with
    /// its own `underlying_sequence`, so replay reproduces the identical sweeps.
    ///
    /// # Errors
    ///
    /// - [`VenueError::InvalidOrder`] if `command` is not a
    ///   [`VenueCommand::MassCancel`];
    /// - [`VenueError::NotFound`] if a `Book` scope names an unhosted underlying,
    ///   or a venue-global sweep reaches a venue hosting no underlyings;
    /// - the first actor's typed rejection if **every** targeted underlying
    ///   rejected the sweep. A **partial** fan-out (some underlyings committed,
    ///   some rejected) is NOT collapsed into a clean success: the committed legs
    ///   are returned TOGETHER with the [`FanoutSummary`], so a caller can tell a
    ///   partial delivery (some underlyings still hold live orders) from a full
    ///   one — the venue does not promise atomic venue-wide fan-out.
    pub async fn submit_mass_cancel(
        &self,
        command: VenueCommand,
    ) -> Result<MassCancelDelivery, VenueError> {
        if !matches!(command, VenueCommand::MassCancel { .. }) {
            return Err(VenueError::InvalidOrder(
                "submit_mass_cancel requires a MassCancel command".to_string(),
            ));
        }
        // A `Book`-scoped sweep names one instrument → one actor, one receipt that
        // carries the full swept set for that book; its fan-out is trivially full
        // (one underlying, one delivery).
        if !is_venue_global(&command) {
            let handle = self.route(&command)?;
            let receipt = handle.submit(command).await?;
            return Ok(MassCancelDelivery {
                swept: swept_legs_of(&receipt),
                fanout: FanoutSummary {
                    ok_count: 1,
                    total: 1,
                },
            });
        }
        // Venue-global fan: aggregate every underlying's swept legs, in the
        // deterministic sorted-underlying then venue-id sweep order.
        let correlation_id = self.next_correlation_id();
        let tickers: Vec<String> = self.underlyings().into_iter().map(str::to_string).collect();
        if tickers.is_empty() {
            return Err(VenueError::NotFound(
                "no hosted underlyings for a venue-global mass cancel".to_string(),
            ));
        }
        let total = tickers.len();
        let mut aggregated: Vec<SweptLeg> = Vec::new();
        let mut first_error: Option<VenueError> = None;
        let mut ok_count = 0usize;
        for ticker in &tickers {
            let result = match self.handle_for(ticker) {
                Ok(handle) => handle.submit(command.clone()).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(receipt) => {
                    // Checked (rule 9); bounded by the hosted-underlying set.
                    ok_count = ok_count.checked_add(1).unwrap_or(ok_count);
                    aggregated.extend(swept_legs_of(&receipt));
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if ok_count == 0 {
            // Every underlying rejected the sweep — surface the first rejection
            // rather than an empty "success".
            return Err(first_error.unwrap_or(VenueError::JournalUnavailable));
        }
        if ok_count != total {
            tracing::warn!(
                correlation_id = %correlation_id,
                committed = ok_count,
                total,
                "venue-global mass cancel fan-out was partial across underlyings"
            );
        }
        // Return the swept legs TOGETHER with the fan-out summary: a partial fan-out
        // (`ok_count < total`) is a reportable state, never hidden behind a clean
        // aggregated success (#97 finding 2).
        Ok(MassCancelDelivery {
            swept: aggregated,
            fanout: FanoutSummary { ok_count, total },
        })
    }

    /// Admits a price-bearing order command against the venue-owned price band
    /// (`[min_price_cents, max_price_cents]`, #044) resolved for the command's
    /// underlying — the admission seam that runs **before matching** so an over-band
    /// price never reaches a leaf and is never journaled
    /// ([05 §4.1](../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable)).
    ///
    /// Only `AddOrder` / `Replace` carrying a `limit_price` are checked; a market
    /// order (no limit price) and every non-order command carry no price to admit.
    /// Delegates to the **shared** [`check_price_band`] so the live submit seam and
    /// the replay/recovery re-execution seam enforce the venue-owned band identically
    /// (a non-parsing symbol is skipped here and rejected by [`route`](Self::route)).
    fn admit_command_price(&self, command: &VenueCommand) -> Result<(), VenueError> {
        check_price_band(&self.microstructure, command)
            .map_err(|error| VenueError::InvalidOrder(error.to_string()))
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

    /// Advances the shared venue clock by one **stepped** interval and fans the
    /// resulting `Clock` command to every underlying actor — the venue-control
    /// coordinator for a stepped clock tick
    /// ([02 §4.1](../docs/02-matching-architecture.md#41-venue-wide-commands-marketmakercontrol--clock--simstep),
    /// [04 §5](../docs/04-market-data-and-replay.md#5-clock-control)).
    ///
    /// The clock is advanced **first** (so each actor stamps the new instant), then
    /// a `Clock { now_ms }` is submitted to every actor as a normal per-underlying
    /// sequenced command carrying that value — so the advance is part of the
    /// recorded input stream and replay reproduces it from the journaled command,
    /// never by re-reading the replay clock. The returned [`ClockAdvance`] reports
    /// per-underlying accept/commit keyed by a shared [`CorrelationId`], surfacing a
    /// **partial** fan-out rather than hiding it (the venue does not promise atomic
    /// all-or-nothing fan-out — there is no venue-wide total order).
    ///
    /// In realtime / accelerated modes [`SimClock::step`](crate::simulation::SimClock::step)
    /// is a no-op read, so this fans the current instant without advancing (those
    /// modes advance via the cadence driver); it is the stepped-mode control path.
    ///
    /// **Concurrency caveat.** This is **not** internally serialized against
    /// concurrent callers: it advances the shared clock and then awaits each
    /// actor's fan-out. Today only sequential drivers (tests) call it, so the clock
    /// is stable across a single advance. A future **live** REST/WS clock-control
    /// surface that drives this concurrently MUST serialize advances (or enforce
    /// at-most-one-in-flight) so a racing advance cannot bump the shared clock
    /// between an actor journaling a `Clock { now_ms }` and stamping its
    /// `venue_ts` — that serialization is that surface's responsibility, not
    /// enforced here. (#030 shipped the record/replay controls, which replay
    /// **offline** and never drive a live clock advance, so no such surface is
    /// wired today.)
    pub async fn advance_clock_step(&self) -> ClockAdvance {
        let now_ms = self.clock.step();
        let advance = self.fan_clock(now_ms).await;
        // #111: the clock just advanced — release every ingress command the advance
        // made due, in deadline order, after the journaled `Clock` marker. A no-op
        // when no latency is injected (empty buffers).
        self.release_all_ingress().await;
        advance
    }

    /// Advances the shared venue clock **monotonically** to `target_ms` (a no-op if
    /// at or below the current instant) and fans the resulting `Clock` command to
    /// every underlying actor — the explicit-instant sibling of
    /// [`advance_clock_step`](Self::advance_clock_step), for driving the venue clock
    /// to a chosen instant. The same **concurrency caveat** applies: concurrent
    /// advances are not serialized here; a future live clock-control surface must
    /// enforce that.
    pub async fn advance_clock_to(&self, target_ms: u64) -> ClockAdvance {
        let now_ms = self.clock.advance_to(target_ms);
        let advance = self.fan_clock(now_ms).await;
        // #111: release ingress the advance made due, in deadline order (no-op when
        // no latency is injected).
        self.release_all_ingress().await;
        advance
    }

    /// Fans a `Clock { now_ms }` to every hosted underlying (in the deterministic
    /// **sorted** order), collecting per-underlying accept/commit under one shared
    /// correlation id. No borrow of `self` is held across an `.await` — each handle
    /// is cloned out first.
    async fn fan_clock(&self, now_ms: EventTimestamp) -> ClockAdvance {
        let correlation_id = self.next_correlation_id();
        let command = VenueCommand::Clock { now_ms };
        let tickers: Vec<String> = self.underlyings().into_iter().map(str::to_string).collect();
        let mut per_underlying = Vec::with_capacity(tickers.len());
        for ticker in tickers {
            let result = match self.handle_for(&ticker) {
                Ok(handle) => handle.submit(command.clone()).await,
                Err(error) => Err(error),
            };
            per_underlying.push((ticker, result));
        }
        let advance = ClockAdvance {
            now_ms,
            correlation_id,
            per_underlying,
        };
        if advance.is_partial() {
            tracing::warn!(
                correlation_id = %correlation_id,
                now_ms = now_ms.get(),
                committed = advance.committed_count(),
                total = advance.per_underlying.len(),
                "clock advance fan-out was partial across underlyings"
            );
        }
        advance
    }

    /// Mints the next shared [`CorrelationId`] for a venue-control fan-out.
    fn next_correlation_id(&self) -> CorrelationId {
        CorrelationId::new(self.correlation_counter.fetch_add(1, Ordering::Relaxed))
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

    /// The **optional** durable persistence pool (#023) — `Some` on the durable
    /// path (`DATABASE_URL` set, opened + migrated at boot), `None` for the fully
    /// in-memory venue. Never `.unwrap()`ed; a durable consumer degrades
    /// explicitly when it is `None`.
    #[must_use]
    #[inline]
    pub fn db(&self) -> Option<&DatabasePool> {
        self.db.as_ref()
    }

    /// Whether the venue is running the durable persistence path.
    #[must_use]
    #[inline]
    pub fn is_persistent(&self) -> bool {
        self.db.is_some()
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

    /// Whether `underlying` was **resumed** from a non-empty durable journal at boot
    /// (#85) — its book / executions / positions state was reconstructed by
    /// re-execution and its actor continues the journaled `underlying_sequence`.
    ///
    /// **Seed-vs-recover precedence:** the bounded seeding phase consults this so a
    /// recovered underlying is **not** re-seeded — recover wins for underlyings with
    /// journal history; seed applies only to genuinely fresh ones. Re-seeding a
    /// recovered underlying would journal a duplicate opening `SimStep` onto the
    /// resumed stream. A point-lookup, never iterated on the sequenced path.
    #[must_use]
    #[inline]
    pub fn is_recovered(&self, underlying: &str) -> bool {
        self.recovered.contains(underlying)
    }

    /// The underlyings resumed from a durable journal at boot (#85), **sorted** for a
    /// deterministic order regardless of set iteration order — the recovered-set
    /// companion to [`underlyings`](Self::underlyings), for the boot log and tests.
    #[must_use]
    pub fn recovered_underlyings(&self) -> Vec<&str> {
        let mut tickers: Vec<&str> = self.recovered.iter().map(AsRef::as_ref).collect();
        tickers.sort_unstable();
        tickers
    }

    /// The number of client orders currently **held** across every per-underlying
    /// ingress reorder buffer (#111) — awaiting the venue clock to pass their release
    /// deadline. Zero on the FIFO fast path (no latency injected). An observability
    /// read of the bounded DoS control, not a sequenced-path read.
    #[must_use]
    pub fn ingress_pending(&self) -> usize {
        self.ingress
            .values()
            .map(|channel| {
                channel
                    .buffer
                    .lock()
                    .map(|buffer| buffer.len())
                    .unwrap_or_else(|poisoned| poisoned.into_inner().len())
            })
            .sum()
    }

    /// The JWT auth service (real as of #012) — JWT verification, the venue-clock
    /// rate limiter, and the account revocation oracle behind one handle every
    /// gateway consults.
    #[must_use]
    #[inline]
    pub fn auth(&self) -> &AuthService<SimClock> {
        &self.auth
    }

    /// The one shared venue clock (#028) — the source every `venue_ts`, the
    /// simulator's `SimStep.now_ms`, and the rate limiter read. Advance it with
    /// [`advance_clock_step`](Self::advance_clock_step) /
    /// [`advance_clock_to`](Self::advance_clock_to).
    #[must_use]
    #[inline]
    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    /// The run manifest (#028/#030) — the recorded `seed` + `clock_mode` +
    /// `instrument_seed` + microstructure fingerprint + pinned crate/dependency
    /// versions fixing this run's determinism.
    #[must_use]
    #[inline]
    pub fn manifest(&self) -> &RunManifest {
        &self.manifest
    }

    /// The resolved venue microstructure (#044) — the fee schedule, STP mode, and
    /// per-underlying contract specs applied to every book at creation, and the
    /// venue-owned price band admitted at order entry. The **same** `Arc` an
    /// exported [`ScenarioBundle`] carries as the config half of the determinism
    /// tuple.
    #[must_use]
    #[inline]
    pub fn microstructure(&self) -> &Arc<MicrostructureConfig> {
        &self.microstructure
    }

    // ---- record / replay control plane (#030) ----------------------------

    /// Whether the venue's scenario-capture window is active (#030). The durable
    /// journal is always on; this is the operator-facing record on/off flag.
    #[must_use]
    #[inline]
    pub fn is_recording(&self) -> bool {
        self.recording.is_recording()
    }

    /// Flips the venue's scenario-capture window, returning the **previous** state.
    /// Both the REST record route and the WS `record` action call this **same**
    /// method (control parity). Admin gating is enforced at each gateway, not here.
    pub fn set_recording(&self, on: bool) -> bool {
        let previous = self.recording.set_recording(on);
        if previous != on {
            tracing::info!(recording = on, "venue scenario-capture window toggled");
        }
        previous
    }

    /// Exports the current venue's journal as a portable [`ScenarioBundle`] — the
    /// per-underlying journal streams (from each actor's read-only journal
    /// snapshot, in deterministic **sorted** order) plus the run [`RunManifest`], so
    /// a recorded scenario is self-describing and replayable on any machine (#030).
    ///
    /// The stream headers are rebuilt from the run lineage + current envelope schema
    /// (the one header every actor shares), so no extra per-actor read is needed.
    ///
    /// # Errors
    ///
    /// Propagates the per-underlying [`ActorHandle::snapshot`] rejection
    /// ([`VenueError::RateLimited`] / [`VenueError::JournalUnavailable`]).
    pub async fn export_bundle(&self) -> Result<ScenarioBundle, VenueError> {
        let header = JournalHeader::new(self.lineage_id.clone());
        let mut streams = Vec::with_capacity(self.underlyings.len());
        // Deterministic sorted underlying order — the portable bundle is stable.
        for ticker in self.underlyings() {
            let snapshot = self.journal_snapshot(ticker).await?;
            streams.push(JournalStream::new(ticker, header.clone(), snapshot.records));
        }
        // Carry the resolved venue microstructure (#044) — the config half of the
        // determinism tuple — so a replay applies the identical fee/STP/specs and a
        // fee-sensitive scenario reconstructs exactly. The manifest already pins the
        // matching fingerprint (set at construction), so the replay equality gate
        // holds.
        Ok(ScenarioBundle::new(self.manifest.clone(), streams)
            .with_microstructure((*self.microstructure).clone()))
    }

    /// Replays a recorded scenario [`ScenarioBundle`] **offline** into a fresh
    /// registry per underlying, reconstructing identical events, fills, and
    /// top-of-book plus the executions store and positions fold (#030). It does
    /// **not** mutate this live venue — replay is a fresh re-execution, and the
    /// live requote engine is never invoked (the driver is structurally mute).
    ///
    /// The CPU-bound re-execution runs on a blocking thread so a large bundle never
    /// stalls an async worker (replay is not a client-latency hot path).
    ///
    /// # Errors
    ///
    /// The driver's typed [`ReplayError`] — a version mismatch, a corrupted /
    /// schema-refused / malformed journal, or a durable-read backend failure.
    pub async fn replay_bundle(
        &self,
        bundle: &ScenarioBundle,
    ) -> Result<ReplayReport, ReplayError> {
        let bundle = bundle.clone();
        match tokio::task::spawn_blocking(move || crate::simulation::replay_bundle(&bundle)).await {
            Ok(result) => result,
            Err(_join) => Err(ReplayError::Backend {
                operation: "replay task join",
            }),
        }
    }

    // ---- stepped synthetic sessions (#031) -------------------------------

    /// Materialises a synthesised session chain onto the **live** venue: registers
    /// each leaf with the market maker at its `smile_curve`-shaped IV, then sets the
    /// opening price — a journaled [`SimStep`](VenueCommand::SimStep) plus the
    /// maker's requote, whose `AddOrder`s **vivify** the leaf books through the
    /// sequenced order path (never a direct book mutation). After this the venue is
    /// **live** and client orders match against the synthetic liquidity.
    ///
    /// The chain's underlying must be a **hosted price-seam asset**
    /// ([`AssetConfig`], typically `SessionConfig::to_asset_config`) so the opening
    /// price can be journaled through the simulator. Returns the number of contracts
    /// registered. A bounded settle waits for the off-thread requote forwarder to
    /// vivify the chain into the shared symbol index before returning.
    ///
    /// # Errors
    ///
    /// [`SimError::UnknownUnderlying`] if the chain's underlying is not a hosted
    /// price-seam asset.
    pub async fn materialize_session(&self, chain: &SynthesizedChain) -> Result<usize, SimError> {
        // Register every leaf (call + put per strike) with its smile IV BEFORE the
        // first price, so the opening requote quotes the whole chain in one pass.
        let instruments = chain.instruments();
        for (symbol, iv) in &instruments {
            self.market_maker
                .register_instrument_with_iv(symbol, Some(*iv));
        }

        // Opening price → a journaled SimStep + the maker's requote vivifies leaves.
        self.simulator.set_price(&chain.underlying, chain.spot)?;

        // Bounded settle: wait for the ordered requote forwarder to vivify the
        // synthesised contracts into the shared symbol index (a completeness wait,
        // never an unbounded spin).
        let expected: std::collections::HashSet<String> = chain
            .strikes
            .iter()
            .flat_map(|strike| {
                [
                    strike.call.as_str().to_string(),
                    strike.put.as_str().to_string(),
                ]
            })
            .collect();
        let mut settled = false;
        for _ in 0..SESSION_SETTLE_MAX_POLLS {
            let present: std::collections::HashSet<String> =
                self.symbol_index.symbols().into_iter().collect();
            if expected.iter().all(|symbol| present.contains(symbol)) {
                settled = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(SESSION_SETTLE_POLL_MS)).await;
        }
        if !settled {
            // Mirrors the seed-phase settle precedent (`seed.rs`): surface a
            // stalled requote forwarder to the operator instead of proceeding
            // silently — the session still materialises, but visibility matters.
            let present: std::collections::HashSet<String> =
                self.symbol_index.symbols().into_iter().collect();
            let vivified = expected
                .iter()
                .filter(|symbol| present.contains(*symbol))
                .count();
            tracing::warn!(
                expected = expected.len(),
                vivified,
                "session settle did not complete within the settle window; proceeding"
            );
        }

        Ok(instruments.len())
    }

    /// Advances a **stepped synthetic session** by one step: advances the venue
    /// clock by its fixed virtual interval (fanning a journaled `Clock` command to
    /// every underlying actor under one [`CorrelationId`]) and walks the underlying
    /// one price step (a journaled `SimStep` per hosted asset, driving the maker's
    /// journaled requotes). Returns the [`ClockAdvance`] so a caller can inspect the
    /// correlation id / detect a partial fan-out.
    ///
    /// Each step enters the sequencer as journaled commands, so replay reproduces
    /// the session from the journal with the live requote engine muted (#030). In
    /// realtime / accelerated modes the clock advance is a no-op read (those advance
    /// via the cadence driver); this is the **stepped-mode** session path.
    pub async fn step_session(&self) -> ClockAdvance {
        // Advance the clock (and journal the Clock command) first, so the SimStep it
        // triggers is stamped with the advanced instant.
        let advance = self.advance_clock_step().await;
        self.simulator.step_once();
        advance
    }

    /// The venue account registry (the [`AccountStore`] backend) — resolution by
    /// JWT `sub` / FIX username, Argon2id verification, and revocation.
    #[must_use]
    #[inline]
    pub fn accounts(&self) -> &AccountRegistry {
        &self.accounts
    }

    /// The shared, account-scoped `(account, ClOrdID) → order_id` correlation index
    /// (#098) — the cross-session bridge the FIX and REST surfaces resolve through.
    #[must_use]
    #[inline]
    pub fn clordid_index(&self) -> &Arc<ClOrdIdIndex> {
        &self.clordid_index
    }

    /// Resolves the order the **authenticated** `account` placed under `client_order_id`
    /// (#098) — the **single** cross-session correlation seam **both** the FIX
    /// (`OrigClOrdID` on `F`/`G`/status) and REST (cancel/replace/status by
    /// client-order-id) surfaces resolve through, so the two are parity by
    /// construction. A [`None`] means the account never placed that id **or** the
    /// id is unknown — a cross-account id is a different key and resolves to
    /// [`None`], so the caller cannot tell a foreign-owned id from an absent one
    /// (the #132 masking). The lookup is a synchronous point read — no lock is held
    /// across an `.await`.
    #[must_use]
    #[inline]
    pub fn resolve_client_order_id(
        &self,
        account: &AccountId,
        client_order_id: &ClientOrderId,
    ) -> Option<ClOrdIdRecord> {
        self.clordid_index.resolve(account, client_order_id)
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
    /// [`AuthError::UnknownAccount`], [`AuthError::AccountRevoked`] (a revoked
    /// account is permanently refused a fresh token, mirroring the FIX-logon rule),
    /// [`AuthError::TokenLifetime`], or [`AuthError::Signing`].
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

    /// The market-maker engine (real as of #015) — the price → requote pipeline
    /// whose generated quotes route onto the sequenced order path, the kill
    /// switch, the clamped persona knobs, and the [`MarketMakerEvent`] broadcast.
    ///
    /// The venue-global market-maker control plane (`MarketMakerControl` routing:
    /// kill / enable / clamp changes applied to this engine and journaled) is a
    /// later control-plane issue; [`AppState::submit`] still declines a
    /// `MarketMakerControl` as not per-underlying-routable. Operators reach the
    /// engine's setters directly through this handle.
    ///
    /// [`MarketMakerEvent`]: crate::market_maker::MarketMakerEvent
    #[must_use]
    #[inline]
    pub fn market_maker(&self) -> &Arc<MarketMakerEngine> {
        &self.market_maker
    }

    /// The price simulator (real as of #016) — pre-generated `optionstratlib`
    /// walks whose every step routes onto the sequenced order path as a journaled
    /// `SimStep` and drives the market maker. Operators / a bootstrap start its
    /// loop ([`PriceSimulator::spawn`](crate::simulation::PriceSimulator::spawn)) or
    /// step it deterministically
    /// ([`PriceSimulator::step_once`](crate::simulation::PriceSimulator::step_once)).
    #[must_use]
    #[inline]
    pub fn simulator(&self) -> &Arc<PriceSimulator> {
        &self.simulator
    }

    /// Whether the venue has flipped to the **serving** phase (#024).
    ///
    /// `false` during the bounded seeding phase, `true` once
    /// [`begin_serving`](Self::begin_serving) has been called. The runtime
    /// hierarchy-create routes gate on this: population happens in the seeding
    /// window (from the seed manifest, not a runtime REST create), and once
    /// serving a hierarchy create/delete is refused — the instrument set is a
    /// seed-time manifest input ([06 §7](../docs/06-deployment.md#7-seed-data-and-scenarios),
    /// [03 §10](../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
    #[must_use]
    #[inline]
    pub fn is_serving(&self) -> bool {
        self.serving.load(Ordering::Acquire)
    }

    /// Flips the venue from **seeding** to **serving** — a monotonic, one-way
    /// transition the seed flow calls once the manifest has been applied, before
    /// binding the gateways. Idempotent (a second call is a harmless no-op); the
    /// venue never flips back to seeding within a run.
    pub fn begin_serving(&self) {
        // `Release` pairs with the `Acquire` in `is_serving`, so a route observing
        // `true` also observes every seeded write that happened-before the flip.
        if !self.serving.swap(true, Ordering::Release) {
            tracing::info!("venue flipped to serving; runtime hierarchy mutation now refused");
        }
    }
}

// ============================================================================
// The clock-cadence driver (#028; self-review fix #112)
// ============================================================================

/// Spawns the venue **clock-cadence driver** — the owned background task that
/// advances the shared venue [`SimClock`] on a wall cadence in realtime /
/// accelerated mode, so `venue_ts` progresses and the sliding rate-limit window
/// rolls for the whole life of the running service.
///
/// Without it the venue clock never advances off the sequenced path: nothing calls
/// [`SimClock::tick`](crate::simulation::SimClock::tick), so `now_ms` is frozen at
/// the epoch, `venue_ts` is constant, and the rate-limit windows never roll for the
/// entire process lifetime (the self-review gap tracked in #112). Stepped mode was
/// never affected — it advances via explicit `Clock` commands — so only the default
/// live modes (realtime / accelerated) were broken.
///
/// Returns `None` in [`ClockMode::Stepped`]: a stepped clock advances **only** on an
/// explicit `Clock` [`VenueCommand`] (the #028 control coordinator
/// [`AppState::advance_clock_step`] / [`AppState::advance_clock_to`], or a replay
/// driver), never on a wall cadence, so no auto-advancing driver is spawned.
///
/// # Determinism
///
/// The driver drives **only** the venue clock: each tick is a single
/// [`SimClock::tick`](crate::simulation::SimClock::tick), a wall read taken **off**
/// the sequenced path that advances the clock's atomic instant. It mutates no book
/// and appends no journal record — the journaled `Clock` advances that fan to the
/// actors stay the #028 control-coordinator path, not a new sequenced path invented
/// here. Reading the real wall clock in this off-path driver is the intended
/// realtime time source, not a sequenced-path violation: the sequenced read
/// [`SimClock::now_ms`](crate::simulation::SimClock::now_ms) stays a pure atomic
/// load, and a replay reproduces `venue_ts` from the journaled values rather than
/// re-reading the wall clock.
///
/// # Shutdown
///
/// The task holds only a [`Weak`] handle to [`AppState`] (mirroring
/// [`spawn_rate_limit_sweeper`](crate::gateway::rest::spawn_rate_limit_sweeper)), so
/// when the last strong `Arc<AppState>` drops (server shutdown) the next tick fails
/// to upgrade and the loop exits cleanly — it never keeps the venue alive. `main.rs`
/// additionally `abort`s the returned handle once the REST server drains, for prompt
/// shutdown.
#[must_use]
pub fn spawn_clock_cadence_driver(state: &Arc<AppState>) -> Option<JoinHandle<()>> {
    spawn_clock_cadence_driver_with_cadence(state, DEFAULT_CLOCK_CADENCE)
}

/// [`spawn_clock_cadence_driver`] with an explicit `cadence` — the seam tests drive
/// on a short interval so the advance is observable without racing the default
/// cadence.
#[must_use]
pub(crate) fn spawn_clock_cadence_driver_with_cadence(
    state: &Arc<AppState>,
    cadence: Duration,
) -> Option<JoinHandle<()>> {
    if matches!(state.clock().mode(), ClockMode::Stepped { .. }) {
        tracing::info!(
            "clock cadence driver: stepped mode advances only via explicit Clock \
             commands; not spawning a wall-cadence driver"
        );
        return None;
    }

    let mode = state.clock().mode().as_token();
    let weak: Weak<AppState> = Arc::downgrade(state);
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(cadence);
        // Never burst-catch-up after a stall: a delayed tick just resumes the
        // cadence (the wall-tracking advance is absolute, so a skipped tick is not
        // lost time — the next tick jumps straight to the true wall instant).
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match weak.upgrade() {
                // Advance ONLY the venue clock — a wall read off the sequenced path.
                // `tick` is mode-aware (realtime / accelerated here); the returned
                // instant is intentionally discarded. No book mutation, no journal.
                Some(state) => {
                    let _ = state.clock().tick();
                    // #111: the clock advanced — release any ingress command it made
                    // due, in deadline order. Off the sequenced path; a no-op when no
                    // latency is injected (empty buffers). The RELEASE INSTANT tracks
                    // the wall clock here (realtime), but the release ORDER is the
                    // deterministic deadline order, never wall order.
                    state.release_all_ingress().await;
                }
                // The last strong `Arc<AppState>` dropped: shut the driver down.
                None => break,
            }
        }
        tracing::debug!("clock cadence driver stopped");
    });
    tracing::info!(
        cadence_ms = cadence.as_millis(),
        mode,
        "clock cadence driver spawned; venue clock advances off the sequenced path"
    );
    Some(handle)
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
            .field("durable", &self.db.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{
        Cents, Hash32, JournalRecord, MARKET_MAKER_ACCOUNT, MARKET_MAKER_OWNER, MassCancelType,
        STPMode, Side, Symbol, TimeInForce, VenueOutcome,
    };
    use crate::models::{AccountId, ClientOrderId, OrderType, VenueOrderId};

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

    /// An `AddOrder` carrying a `client_order_id` — the account-scoped
    /// idempotency key an idempotent resend reuses.
    #[allow(clippy::too_many_arguments)]
    fn add_keyed(
        symbol: &str,
        order_id: &str,
        account: &str,
        owner: u8,
        side: Side,
        price: u64,
        quantity: u64,
        client_order_id: &str,
    ) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym(symbol),
            order_id: VenueOrderId::new(order_id),
            account: AccountId::new(account),
            owner: Hash32([owner; 32]),
            client_order_id: Some(ClientOrderId::new(client_order_id)),
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

    // ---- serving-phase flag (#024) ---------------------------------------

    #[tokio::test]
    async fn test_default_state_starts_serving() {
        // Backward-compatible: a plain construction is immediately serving.
        let state = new_state(config(&["BTC"]));
        assert!(state.is_serving());
    }

    #[tokio::test]
    async fn test_seeding_phase_flag_flips_once() {
        let state = new_state(config(&["BTC"]).with_serving(false));
        assert!(!state.is_serving(), "starts in the bounded seeding phase");
        state.begin_serving();
        assert!(state.is_serving(), "flips to serving");
        // Idempotent: a second flip is a harmless no-op.
        state.begin_serving();
        assert!(state.is_serving());
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
    async fn test_submit_clock_command_is_not_routable_by_raw_submit() {
        // A raw `Clock` submit is still refused (a stepped advance enters through the
        // clock coordinator, not this path) — the venue-global fan-out (#47) does not
        // capture `Clock`.
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

    // ---- venue-global fan-out routing (#47) ------------------------------

    #[tokio::test]
    async fn test_market_maker_control_fans_out_and_is_journaled_by_every_underlying() {
        let state = new_state(config(&["BTC", "ETH"]));
        // A venue-global **non-kill** MarketMakerControl (a spread knob, `enabled:
        // None`) fans to every actor and journals in each stream as a SINGLE command
        // whose `ControlApplied` sweeps nothing. A kill (`enabled: Some(false)`)
        // couples an owner-scoped sweep INTO the same control turn (still one command
        // per underlying) — covered by the dedicated kill tests below (#117).
        let receipt = match state
            .submit(VenueCommand::MarketMakerControl {
                spread_multiplier: Some(1.5),
                size_scalar: None,
                directional_skew: None,
                enabled: None,
            })
            .await
        {
            Ok(r) => r,
            Err(e) => panic!("MarketMakerControl fan-out failed: {e}"),
        };
        assert_eq!(receipt.underlying_sequence.get(), 0);
        for ticker in ["BTC", "ETH"] {
            let snap = match state.journal_snapshot(ticker).await {
                Ok(s) => s,
                Err(e) => panic!("{ticker} snapshot failed: {e}"),
            };
            assert_eq!(
                snap.last_sequence.map(|s| s.get()),
                Some(0),
                "{ticker} journaled the control command"
            );
        }
    }

    #[tokio::test]
    async fn test_sequenced_kill_couples_owner_scoped_sweep_into_the_control_turn() {
        // A **sequenced kill** (`MarketMakerControl { enabled: Some(false) }`) journals
        // exactly ONE command per underlying — the control — whose `ControlApplied`
        // carries the owner-scoped market-maker sweep in the SAME turn. There is no
        // separate follow-on `MassCancel` a crash could skip: control + cancel are
        // atomic per underlying (#117).
        let state = new_state(config(&["BTC"]));
        // A resting market-maker quote + a client order that must survive.
        state
            .submit(add(
                "BTC-20240329-50000-C",
                "mm-ask",
                MARKET_MAKER_ACCOUNT,
                0xEE,
                Side::Sell,
                50_000,
                2,
            ))
            .await
            .expect("mm quote rests");
        state
            .submit(add(
                "BTC-20240329-50000-C",
                "cli-bid",
                "alice",
                0x11,
                Side::Buy,
                40_000,
                3,
            ))
            .await
            .expect("client order rests");

        let receipt = state
            .submit(VenueCommand::MarketMakerControl {
                spread_multiplier: None,
                size_scalar: None,
                directional_skew: None,
                enabled: Some(false),
            })
            .await
            .expect("kill fans out");
        // The primary receipt is the CONTROL's (sequence 2, after the two adds); its
        // outcome carries the coupled owner-scoped sweep.
        assert_eq!(receipt.underlying_sequence.get(), 2);
        match &receipt.outcome {
            Some(VenueOutcome::ControlApplied { swept }) => {
                assert_eq!(swept.len(), 1, "only the maker's own quote is swept");
                assert!(
                    swept.iter().all(|leg| leg.owner == MARKET_MAKER_OWNER),
                    "the client order is untouched — only MARKET_MAKER_OWNER legs are swept"
                );
            }
            other => panic!("expected a coupled ControlApplied receipt, got {other:?}"),
        }

        // The journal carries the control as the LAST command (sequence 2) — no
        // separate follow-on `MassCancel`. The only cancellation is the coupled sweep
        // on the control's own `ControlApplied` outcome.
        let snap = state.journal_snapshot("BTC").await.expect("BTC snapshot");
        assert_eq!(
            snap.last_sequence.map(|s| s.get()),
            Some(2),
            "the control is the last journaled command — no follow-on MassCancel"
        );
        assert!(
            !snap.records.iter().any(|record| matches!(
                record,
                JournalRecord::Event(event)
                    if matches!(event.command, VenueCommand::MassCancel { .. })
            )),
            "a kill journals NO separate MassCancel command"
        );
        // The kill's own event carries the owner-scoped sweep in the same turn.
        let swept = snap
            .records
            .iter()
            .find_map(|record| match record {
                JournalRecord::Event(event) => match (&event.command, &event.outcome) {
                    (
                        VenueCommand::MarketMakerControl {
                            enabled: Some(false),
                            ..
                        },
                        VenueOutcome::ControlApplied { swept },
                    ) => Some(swept.clone()),
                    _ => None,
                },
                _ => None,
            })
            .expect("the kill control event carries the coupled sweep");
        assert_eq!(swept.len(), 1, "only the maker's own quote is swept");
        assert!(
            swept.iter().all(|leg| leg.owner == MARKET_MAKER_OWNER),
            "the client order is untouched — only MARKET_MAKER_OWNER legs are cancelled"
        );
    }

    #[tokio::test]
    async fn test_second_sequenced_kill_on_a_disabled_engine_does_not_double_cancel() {
        // A second kill on an already-disabled engine still journals its OWN control
        // whose coupled sweep is a pure function of the command, but there is nothing
        // left to sweep — it is idempotent and safe, never a double-cancel or a panic
        // (#117).
        let state = new_state(config(&["BTC"]));
        state
            .submit(add(
                "BTC-20240329-50000-C",
                "mm-ask",
                MARKET_MAKER_ACCOUNT,
                0xEE,
                Side::Sell,
                50_000,
                2,
            ))
            .await
            .expect("mm quote rests");
        state
            .submit(VenueCommand::MarketMakerControl {
                spread_multiplier: None,
                size_scalar: None,
                directional_skew: None,
                enabled: Some(false),
            })
            .await
            .expect("first kill");
        // Second kill: the control is a no-op (already disabled) and its coupled sweep
        // finds nothing left to cancel.
        state
            .submit(VenueCommand::MarketMakerControl {
                spread_multiplier: None,
                size_scalar: None,
                directional_skew: None,
                enabled: Some(false),
            })
            .await
            .expect("second kill is safe");

        // Sequences: add(0), control(1), control(2) — no separate sweep commands.
        let snap = state.journal_snapshot("BTC").await.expect("BTC snapshot");
        assert_eq!(snap.last_sequence.map(|s| s.get()), Some(2));
        assert!(
            !snap.records.iter().any(|record| matches!(
                record,
                JournalRecord::Event(event)
                    if matches!(event.command, VenueCommand::MassCancel { .. })
            )),
            "a kill journals NO separate MassCancel command"
        );
        // The two coupled ControlApplied outcomes: the first swept the one quote, the
        // second swept nothing (already gone).
        let swept: Vec<usize> = snap
            .records
            .iter()
            .filter_map(|record| match record {
                JournalRecord::Event(event) => match &event.outcome {
                    VenueOutcome::ControlApplied { swept } => Some(swept.len()),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        assert_eq!(
            swept,
            vec![1, 0],
            "the first kill sweeps the one quote; the second finds nothing to cancel"
        );
    }

    #[tokio::test]
    async fn test_hierarchy_mass_cancel_fans_out_to_every_underlying() {
        let state = new_state(config(&["BTC", "ETH"]));
        match state
            .submit(VenueCommand::MassCancel {
                scope: MassCancelScope::Underlying,
                cancel_type: MassCancelType::All,
                account: AccountId::new("admin"),
            })
            .await
        {
            Ok(_) => {}
            Err(e) => panic!("hierarchy MassCancel fan-out failed: {e}"),
        }
        for ticker in ["BTC", "ETH"] {
            let snap = match state.journal_snapshot(ticker).await {
                Ok(s) => s,
                Err(e) => panic!("{ticker} snapshot failed: {e}"),
            };
            assert_eq!(snap.last_sequence.map(|s| s.get()), Some(0));
        }
    }

    #[tokio::test]
    async fn test_book_scoped_mass_cancel_routes_to_one_underlying() {
        // A `Book`-scoped mass cancel names one instrument and routes per-underlying:
        // only the owning actor journals it.
        let state = new_state(config(&["BTC", "ETH"]));
        match state
            .submit(VenueCommand::MassCancel {
                scope: MassCancelScope::Book(sym("BTC-20240329-50000-C")),
                cancel_type: MassCancelType::All,
                account: AccountId::new("admin"),
            })
            .await
        {
            Ok(_) => {}
            Err(e) => panic!("book-scoped MassCancel failed: {e}"),
        }
        let btc = state.journal_snapshot("BTC").await.expect("BTC snapshot");
        let eth = state.journal_snapshot("ETH").await.expect("ETH snapshot");
        assert_eq!(
            btc.last_sequence.map(|s| s.get()),
            Some(0),
            "BTC journaled it"
        );
        assert_eq!(eth.last_sequence, None, "ETH was not touched");
    }

    #[tokio::test]
    async fn test_submit_set_instrument_status_routes_by_symbol() {
        let state = new_state(config(&["BTC"]));
        let receipt = match state
            .submit_set_instrument_status(sym("BTC-20240329-50000-C"), InstrumentStatus::Halted)
            .await
        {
            Ok(r) => r,
            Err(e) => panic!("submit_set_instrument_status failed: {e}"),
        };
        assert_eq!(receipt.underlying_sequence.get(), 0);
    }

    #[tokio::test]
    async fn test_venue_global_command_on_empty_venue_is_not_found() {
        let state = new_state(config(&[]));
        match state
            .submit(VenueCommand::MarketMakerControl {
                spread_multiplier: Some(1.0),
                size_scalar: None,
                directional_skew: None,
                enabled: None,
            })
            .await
        {
            Err(VenueError::NotFound(detail)) => assert!(detail.contains("no hosted underlyings")),
            other => panic!("expected NotFound on an empty venue, got {other:?}"),
        }
    }

    // ---- scheduled expiry roll gates the phase advance on every commit (#47) ----

    #[tokio::test]
    async fn test_expiry_roll_does_not_advance_phase_when_a_required_command_is_rejected() {
        use option_chain_orderbook::SymbolRef;

        // The venue hosts BTC, but the shared symbol index is seeded with a leaf under
        // an UNHOSTED underlying ("ZZZ"). Its per-symbol `SetInstrumentStatus` routes to
        // a non-existent actor and is rejected (`NotFound`), so the required sequenced
        // command set for that expiration never fully commits.
        let state = new_state(config(&["BTC"]));
        let raw = "ZZZ-20250102-50000-C";
        let parsed = SymbolParser::parse(raw).expect("valid unhosted-underlying symbol");
        let sym_ref = SymbolRef::new(
            parsed.underlying(),
            *parsed.expiration(),
            parsed.strike(),
            parsed.option_style(),
        );
        // `register` returns `true` only on a duplicate overwrite; this is a fresh
        // insert, so it returns `false`.
        assert!(
            !state.symbol_index().register(raw, sym_ref),
            "seed the unhosted leaf into the shared index as a new entry"
        );
        assert!(
            state.symbol_index().contains(raw),
            "the leaf is now indexed"
        );

        let schedule = ExpirySchedule::default();
        let expiration = *parsed.expiration();
        let (_expiry, settle) = schedule
            .operational_instants(&expiration)
            .expect("DateTime expiry");
        let key_ms = match &expiration {
            ExpirationDate::DateTime(dt) => dt.timestamp_millis(),
            // days-expiry-allow: defensive read-arm; the fixture symbol is a DateTime expiry.
            ExpirationDate::Days(_) => panic!("fixture is a DateTime expiry"),
        };

        // Drive the roll past settlement: the target phase is `Expired`, but the
        // per-symbol status transition is rejected, so the phase MUST NOT advance —
        // the driver returns a typed partial result instead.
        let failures = match state.run_expiry_roll(&schedule, settle).await {
            Err(ExpiryRollError::Partial { failures, .. }) => failures,
            Ok(report) => panic!(
                "a rejected required command must yield a typed partial result, got Ok({report:?})"
            ),
        };
        let failure = failures
            .iter()
            .find(|f| f.underlying == "ZZZ")
            .expect("the partial result names the un-advanced ZZZ expiration");
        assert_eq!(failure.expiration_ms, key_ms);
        assert_eq!(
            failure.attempted_phase,
            ExpiryPhase::Expired,
            "it names the phase it failed to reach"
        );

        // The phase was NOT recorded — the instrument is not falsely marked Expired
        // while its resting orders may remain live; it stays at the implicit PreExpiry.
        {
            let map = state
                .expiry_phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(
                map.get(&("ZZZ".to_string(), key_ms)).is_none(),
                "a rejected roll must leave the expiry phase un-advanced so it is retried"
            );
        }

        // Re-running the roll at the same instant RETRIES it (still partial), rather
        // than silently treating the expiration as settled.
        assert!(
            matches!(
                state.run_expiry_roll(&schedule, settle).await,
                Err(ExpiryRollError::Partial { .. })
            ),
            "the un-advanced expiration is retried on the next roll, not skipped as done"
        );
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

    // ---- #099: an idempotent resend surfaces the STORED terminal outcome --

    #[tokio::test]
    async fn test_idempotent_resend_receipt_carries_the_stored_terminal_outcome() {
        // #118 threaded the FRESH command's captured outcome onto the `Receipt`;
        // #099 asserts the RESEND path reuses that same seam: on a dedup hit the
        // executor returns the STORED terminal outcome (the original fills, the
        // canonical order/exec ids), and it is surfaced byte-identically on the
        // resend's `Receipt.outcome` — never a recomputed / read-back approximation.
        let state = new_state(config(&["BTC"]));
        let symbol = "BTC-20240329-50000-C";

        // Resting maker sell 2, then a crossing taker buy 3 KEYED with a ClOrdID —
        // it fills 2 and rests 1.
        match state
            .submit(add(symbol, "maker-1", "maker", 0x11, Side::Sell, 50_000, 2))
            .await
        {
            Ok(_) => {}
            Err(e) => panic!("maker submit failed: {e}"),
        }
        let original = match state
            .submit(add_keyed(
                symbol,
                "taker-1",
                "taker",
                0x22,
                Side::Buy,
                50_000,
                3,
                "dup",
            ))
            .await
        {
            Ok(r) => r,
            Err(e) => panic!("original taker submit failed: {e}"),
        };
        let original_outcome = original
            .outcome
            .clone()
            .expect("an order submit always carries a captured outcome");
        assert!(
            matches!(
                &original_outcome,
                VenueOutcome::Added { fills, resting_quantity: 1, .. } if fills.len() == 2
            ),
            "the original crossing add fills 2 (two legs) and rests 1, got {original_outcome:?}"
        );
        let legs_after_original = state.executions().len();
        assert_eq!(
            legs_after_original, 2,
            "one crossing match records exactly two execution legs"
        );

        // Resend the byte-identical taker: SAME account + ClOrdID, a FRESH order id
        // (the standard retry after a dropped ack). The executor dedups.
        let resend = match state
            .submit(add_keyed(
                symbol,
                "taker-RESEND",
                "taker",
                0x22,
                Side::Buy,
                50_000,
                3,
                "dup",
            ))
            .await
        {
            Ok(r) => r,
            Err(e) => panic!("resend submit failed: {e}"),
        };

        // (a) The resend's receipt carries an idempotent `Duplicate` echoing the
        // ORIGINAL identity + terminal sequence and boxing the STORED terminal
        // outcome — the canonical order/exec ids and the ORIGINAL fills — not a
        // recomputed fresh outcome or a phantom retry id (#099).
        match resend.outcome.as_ref() {
            Some(VenueOutcome::Duplicate {
                original_sequence,
                terminal,
                ..
            }) => {
                assert_eq!(
                    *original_sequence, original.underlying_sequence,
                    "the resend echoes the ORIGINAL terminal sequence, not the resend turn's"
                );
                assert_eq!(
                    terminal.as_ref(),
                    &original_outcome,
                    "the Duplicate boxes the stored terminal outcome"
                );
            }
            other => panic!("the resend receipt must be an idempotent Duplicate, got {other:?}"),
        }

        // (b) The dedup opened NO second order: the store still holds exactly the
        // two original legs — no phantom fill.
        assert_eq!(
            state.executions().len(),
            legs_after_original,
            "the resend opened no second order (no phantom fill in the store)"
        );

        // (c) The projected taker legs are the ORIGINAL fill, never an empty fresh
        // read-back keyed on the resend's fresh order id / sequence.
        assert_eq!(
            resend
                .outcome
                .as_ref()
                .map(VenueOutcome::taker_fill_legs)
                .unwrap_or_default(),
            vec![(Cents::new(50_000), 2)],
            "the resend renders the ORIGINAL taker fill from the stored outcome"
        );
    }

    // ---- clock coordinator (#028) ----------------------------------------

    /// A stepped-clock config over `underlyings`, starting at `start_ms` and
    /// advancing by `step_ms` per step.
    fn stepped_config(underlyings: &[&str], start_ms: u64, step_ms: u64) -> AppStateConfig {
        config(underlyings).with_clock(VenueClockConfig::stepped(start_ms, step_ms))
    }

    #[tokio::test]
    async fn test_advance_clock_step_fans_to_all_underlyings_and_advances_venue_ts() {
        let state = new_state(stepped_config(&["BTC", "ETH"], 1_000, 500));
        // The stepped advance moves the shared clock by exactly the interval and
        // fans a Clock command to EVERY underlying, each committing on its own
        // sequence 0.
        let advance = state.advance_clock_step().await;
        assert_eq!(advance.now_ms, EventTimestamp::new(1_500));
        assert_eq!(state.clock().now_ms(), EventTimestamp::new(1_500));
        assert_eq!(advance.committed_count(), 2, "both underlyings committed");
        assert!(!advance.is_partial(), "a full fan-out is not partial");
        for (ticker, result) in &advance.per_underlying {
            match result {
                Ok(receipt) => assert_eq!(
                    receipt.underlying_sequence.get(),
                    0,
                    "{ticker} sequenced the Clock command at its own seq 0"
                ),
                Err(e) => panic!("{ticker} clock fan-out failed: {e}"),
            }
        }
        // A subsequent order is now stamped with the advanced venue instant.
        let receipt = match state.submit(cancel("BTC-20240329-50000-C")).await {
            Ok(r) => r,
            Err(e) => panic!("post-advance submit failed: {e}"),
        };
        assert_eq!(
            receipt.underlying_sequence.get(),
            1,
            "after the Clock at seq 0"
        );
        assert_eq!(
            receipt.venue_ts,
            EventTimestamp::new(1_500),
            "venue_ts is stamped from the advanced shared clock"
        );
    }

    #[tokio::test]
    async fn test_advance_clock_step_is_a_no_op_advance_in_realtime() {
        // In realtime mode `step()` is a no-op read: the fan-out still reaches every
        // underlying (at the current instant), the clock does not jump on a step.
        let state = new_state(config(&["BTC"]).with_clock(VenueClockConfig::realtime(7_000)));
        let advance = state.advance_clock_step().await;
        assert_eq!(advance.now_ms, EventTimestamp::new(7_000));
        assert_eq!(advance.committed_count(), 1);
    }

    #[tokio::test]
    async fn test_manifest_records_seed_and_clock_mode() {
        let state = new_state(stepped_config(&["BTC"], 1_000, 500).with_seed(99));
        assert_eq!(state.manifest().seed, 99);
        assert_eq!(state.manifest().clock_mode, "stepped");
    }

    // ---- clock cadence driver (#028; self-review fix #112) ---------------

    /// Polls the shared clock until it advances past `start`, or `budget` elapses,
    /// returning the observed instant — so the assertion waits on the driver rather
    /// than racing a fixed sleep.
    async fn wait_for_advance(state: &Arc<AppState>, start: u64, budget: Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            let now = state.clock().now_ms().get();
            if now > start || tokio::time::Instant::now() >= deadline {
                return now;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    #[tokio::test]
    async fn test_clock_cadence_driver_advances_the_venue_clock_in_accelerated() {
        // The bug fixed by #112: in the running service nothing advanced the venue
        // clock, so `venue_ts` was constant for the process lifetime. The cadence
        // driver advances it off the sequenced path. A large multiplier makes even a
        // couple of short cadence ticks of real wall time advance the virtual clock
        // far past the epoch, so the assertion is robust to timing jitter.
        let start = 1_000_000_000_000;
        let state =
            new_state(config(&["BTC"]).with_clock(VenueClockConfig::accelerated(start, 100_000)));
        assert_eq!(
            state.clock().now_ms().get(),
            start,
            "starts parked at the epoch"
        );

        let driver = match spawn_clock_cadence_driver_with_cadence(&state, Duration::from_millis(2))
        {
            Some(handle) => handle,
            None => panic!("realtime/accelerated must spawn a cadence driver"),
        };

        let advanced = wait_for_advance(&state, start, Duration::from_secs(2)).await;
        assert!(
            advanced > start,
            "the cadence driver advanced the venue clock off the sequenced path \
             ({advanced} > {start})"
        );

        // Clean shutdown: aborting the driver stops the loop (it also exits on the
        // dropped `Weak` when `state` drops at end of scope).
        driver.abort();
    }

    #[tokio::test]
    async fn test_clock_cadence_driver_is_not_spawned_in_stepped_mode() {
        // Stepped clocks advance ONLY on an explicit Clock command, never on a wall
        // cadence: no driver is spawned, and the clock does not auto-advance.
        let start = 5_000;
        let state = new_state(stepped_config(&["BTC"], start, 500));
        assert!(
            spawn_clock_cadence_driver_with_cadence(&state, Duration::from_millis(2)).is_none(),
            "stepped mode must spawn no wall-cadence driver"
        );

        // Give any (erroneously spawned) loop ample time to fire, then confirm the
        // clock is still parked at the epoch — a stepped clock never auto-advances.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            state.clock().now_ms().get(),
            start,
            "a stepped clock does not auto-advance without an explicit step"
        );

        // The explicit #028 control path still advances it by exactly the interval.
        state.advance_clock_step().await;
        assert_eq!(
            state.clock().now_ms().get(),
            start + 500,
            "an explicit Clock advance still moves a stepped clock"
        );
    }

    // ---- boot-recovery store rebuild is fallible (#85 review) -------------

    /// An executions store whose `record` always fails — the projection fault boot
    /// recovery must fail-stop on rather than serve a partially-rebuilt store.
    struct FaultyExec;

    impl ExecutionsStore for FaultyExec {
        fn record(
            &self,
            _record: crate::models::ExecutionRecord,
        ) -> Result<(), crate::exchange::StoreError> {
            Err(crate::exchange::StoreError::Backend("injected".to_string()))
        }
        fn get(
            &self,
            _execution_id: &crate::models::ExecutionId,
            _account: &AccountId,
        ) -> Result<Option<crate::models::ExecutionRecord>, crate::exchange::StoreError> {
            Ok(None)
        }
        fn list(
            &self,
            _account: &AccountId,
            _filter: &crate::exchange::ExecutionFilter,
        ) -> Result<Vec<crate::models::ExecutionRecord>, crate::exchange::StoreError> {
            Ok(Vec::new())
        }
        fn len(&self) -> usize {
            0
        }
    }

    #[tokio::test]
    async fn test_rebuild_stores_from_events_surfaces_a_projection_failure() {
        // #85 review: boot recovery must FAIL STARTUP (never serve a partial store)
        // when a projection fails during the store rebuild. `rebuild_stores_from_events`
        // surfaces the #131 `StoreFanOut` seal as `Err`, which `AppState::new` maps to
        // a fatal `AppStateError::RecoveryProjectionFailed`.
        let state = new_state(config(&["BTC"]));
        // A crossing match → a fill-bearing event in the journal.
        state
            .submit(add(
                "BTC-20240329-50000-C",
                "mk",
                "maker",
                0x11,
                Side::Sell,
                50_000,
                2,
            ))
            .await
            .expect("maker rests");
        state
            .submit(add(
                "BTC-20240329-50000-C",
                "tk",
                "taker",
                0x22,
                Side::Buy,
                50_000,
                2,
            ))
            .await
            .expect("taker crosses");
        let snap = state.journal_snapshot("BTC").await.expect("BTC snapshot");
        let events: Vec<VenueEvent> = snap
            .records
            .into_iter()
            .filter_map(|r| match r {
                JournalRecord::Event(e) => Some(e),
                _ => None,
            })
            .collect();
        assert!(
            events.iter().any(|e| matches!(
                &e.outcome,
                VenueOutcome::Added { fills, .. } if !fills.is_empty()
            )),
            "the crossing produced a fill-bearing event to rebuild"
        );

        // Happy path: in-memory stores rebuild cleanly.
        let exec_ok = Arc::new(InMemoryExecutionsStore::new());
        let pos_ok = Arc::new(InMemoryPositionsStore::new());
        rebuild_stores_from_events(&exec_ok, &pos_ok, &Arc::new(MarkPriceBook::new()), &events)
            .expect("a clean rebuild returns Ok");
        assert!(exec_ok.len() >= 2, "the crossing's two legs were rebuilt");

        // Faulty executions store: the rebuild surfaces the seal, projection named.
        let exec_bad = Arc::new(FaultyExec);
        let pos = Arc::new(InMemoryPositionsStore::new());
        let sealed =
            rebuild_stores_from_events(&exec_bad, &pos, &Arc::new(MarkPriceBook::new()), &events);
        assert_eq!(
            sealed.err().map(|s| s.projection),
            Some("executions"),
            "a projection failure fails the rebuild rather than serving a partial store"
        );
    }
}
