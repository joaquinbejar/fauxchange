//! The [`PriceSimulator`] — the async price-walk loop ported from the Backend and
//! re-pointed at the sequencer: each generated step becomes a journaled
//! [`VenueCommand::SimStep`](crate::exchange::VenueCommand::SimStep) that drives
//! the market maker, so synthetic prices and the liquidity they induce are both
//! replayable ([016](../../milestones/v0.1-backend-core/016-price-simulator-walks.md),
//! [04 §2](../../docs/04-market-data-and-replay.md#2-synthetic-price-generation),
//! [specs §5](../../docs/specs/option-chain-orderbook-backend.md#5-simulation-and-ohlc)).
//!
//! ## The venue clock, not `SystemTime` (rule 3)
//!
//! Each emitted step is stamped `now_ms` from the injected venue
//! [`SimClock`](crate::simulation::SimClock) — the **one** clock the whole venue
//! reads (#028) — and carries that value into the `SimStep`, so replay reuses the
//! exact recorded value. `step_once` **reads** the current venue instant (a pure
//! atomic load, never `SystemTime`); the clock is **advanced** by the cadence
//! driver / control coordinator (realtime & accelerated track wall time off the
//! sequenced path; stepped advances only on an explicit `Clock` command), so the
//! sim's price cadence runs off the injected clock rather than a private counter.
//!
//! ## Journal-driven replay, not seed-regenerated (rule 3/5)
//!
//! `optionstratlib`'s walk sampler builds its own RNG per draw and cannot consume
//! the run seed, so the walk is **excluded** from same-seed regeneration. The
//! guaranteed reproduction is the **journal**: the `SimStep`s and the requotes
//! they cause are recorded, and replay re-executes them directly (with the live
//! market maker muted so it never re-derives a cascading requote,
//! [`MarketMakerEngine::set_muted`](crate::market_maker::MarketMakerEngine::set_muted)).
//!
//! ## Pre-generated paths, regenerated off-lock (rule 8)
//!
//! Each asset's path is pre-generated over a horizon and served step by step; when
//! exhausted it is regenerated **off the state lock** (the `optionstratlib`
//! generation runs with no guard held). A walk failure backs the asset off
//! **dormant** rather than busy-looping. The price fan-out is a **bounded**
//! `tokio::broadcast`; a laggard drops and re-reads (rule 7).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use tokio::sync::broadcast;

use crate::exchange::{Cents, EventTimestamp};
use crate::simulation::clock::{ClockMode, SimClock};
use crate::simulation::sink::StepSink;
use crate::simulation::walk::{SimError, WalkTypeConfig, generate_path};

/// The default bounded capacity of the price-update broadcast — a DoS control,
/// never unbounded (rule 7). A slow subscriber lags and re-reads.
pub const DEFAULT_PRICE_CHANNEL_CAPACITY: usize = 1_024;

/// The default pre-generation horizon in steps (~30 days of one-minute steps).
pub const DEFAULT_HORIZON_STEPS: usize = 43_200;

/// The default virtual-clock advance per step, in **milliseconds** (one minute).
pub const DEFAULT_STEP_MS: u64 = 60_000;

/// The default virtual-clock epoch, in **milliseconds** (2025-01-01T00:00:00Z) —
/// a fixed, deterministic start (never the wall clock) that sits well before the
/// venue's far-dated option expiries, so time-to-expiry stays positive.
pub const DEFAULT_START_MS: u64 = 1_735_689_600_000;

/// The default wall-clock cadence of the interval loop.
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(1);

// ============================================================================
// Config + fan-out payload
// ============================================================================

/// The per-asset walk configuration: which underlying, its starting price, and
/// the walk shape.
#[derive(Debug, Clone)]
pub struct AssetConfig {
    /// The underlying ticker whose price this walk drives.
    pub underlying: String,
    /// The starting price in **cents**.
    pub initial_price: Cents,
    /// Annualized drift (used by GBM / jump-diffusion; ignored by OU).
    pub drift: f64,
    /// Annualized volatility (strictly positive).
    pub volatility: f64,
    /// The surfaced walk type.
    pub walk_type: WalkTypeConfig,
}

impl AssetConfig {
    /// Builds an asset config with zero drift and the given walk type.
    #[must_use]
    pub fn new(
        underlying: impl Into<String>,
        initial_price: Cents,
        volatility: f64,
        walk_type: WalkTypeConfig,
    ) -> Self {
        Self {
            underlying: underlying.into(),
            initial_price,
            drift: 0.0,
            volatility,
            walk_type,
        }
    }

    /// Sets the annualized drift.
    #[must_use]
    pub fn with_drift(mut self, drift: f64) -> Self {
        self.drift = drift;
        self
    }
}

/// The simulation-wide parameters: the loop cadence, the pre-generation horizon,
/// the virtual-clock step and epoch, and the bounded broadcast capacity.
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    /// The wall-clock cadence of the interval loop.
    pub tick_interval: Duration,
    /// The pre-generation horizon, in steps.
    pub horizon_steps: usize,
    /// The virtual-clock advance per step, in **milliseconds**.
    pub step_ms: u64,
    /// The virtual-clock epoch, in **milliseconds**.
    pub start_ms: u64,
    /// The bounded price-broadcast capacity.
    pub price_channel_capacity: usize,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            tick_interval: DEFAULT_TICK_INTERVAL,
            horizon_steps: DEFAULT_HORIZON_STEPS,
            step_ms: DEFAULT_STEP_MS,
            start_ms: DEFAULT_START_MS,
            price_channel_capacity: DEFAULT_PRICE_CHANNEL_CAPACITY,
        }
    }
}

/// A published price step — the `tokio::broadcast` payload every `subscribe`r
/// receives. Money is integer [`Cents`]; `now_ms` is the venue-clock instant the
/// step was stamped with (the same value carried into the journaled `SimStep`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceUpdate {
    /// The underlying ticker.
    pub underlying: String,
    /// The new price in **cents**.
    pub price: Cents,
    /// The venue-clock instant of the step, in **milliseconds**.
    pub now_ms: EventTimestamp,
}

// ============================================================================
// Per-asset state
// ============================================================================

/// One asset's live walk state: its config, its pre-generated path and cursor,
/// its last price, and whether a walk failure has backed it off dormant.
struct AssetState {
    config: AssetConfig,
    path: Vec<Cents>,
    cursor: usize,
    current: Cents,
    dormant: bool,
}

// ============================================================================
// PriceSimulator
// ============================================================================

/// The synthetic price simulator: pre-generates a walk per configured asset,
/// serves one step per tick, publishes a [`PriceUpdate`], and routes each step
/// through the sequencer via the [`StepSink`] (which also drives the market
/// maker).
///
/// Held as `Arc<PriceSimulator>`. The interval loop is **not** started by
/// construction — call [`PriceSimulator::spawn`] to run it, or drive
/// [`PriceSimulator::step_once`] directly for deterministic stepping (tests / a
/// stepped clock). Either way, every served step is journaled through the sink.
pub struct PriceSimulator {
    /// The seam onto the sequencer + market maker (one journaled step).
    sink: Arc<dyn StepSink>,
    /// The simulation-wide parameters.
    config: SimulationConfig,
    /// Per-asset walk state, keyed by underlying ticker.
    assets: RwLock<HashMap<String, AssetState>>,
    /// The bounded price-update broadcast.
    price_tx: broadcast::Sender<PriceUpdate>,
    /// The injected venue clock — the **one** source of the `now_ms` every step is
    /// stamped with, shared with the per-underlying actors' `venue_ts` (#028).
    clock: SimClock,
    /// The loop's stop flag (set by [`PriceSimulator::stop`]).
    stopped: AtomicBool,
}

impl PriceSimulator {
    /// Builds a simulator over `assets` and `config`, stamping every step from the
    /// injected venue `clock` and routing it through `sink`. Each asset's initial
    /// path is pre-generated eagerly; an asset whose walk fails to generate starts
    /// **dormant** (it serves no price until a [`set_price`](Self::set_price)
    /// override revives it) rather than aborting construction.
    #[must_use]
    pub fn new(
        assets: Vec<AssetConfig>,
        config: SimulationConfig,
        sink: Arc<dyn StepSink>,
        clock: SimClock,
    ) -> Arc<Self> {
        let (price_tx, _) = broadcast::channel(config.price_channel_capacity.max(1));
        let mut map: HashMap<String, AssetState> = HashMap::with_capacity(assets.len());
        for asset in assets {
            let (path, dormant) = match generate_path(
                asset.walk_type,
                asset.initial_price,
                asset.drift,
                asset.volatility,
                config.step_ms,
                config.horizon_steps,
            ) {
                Ok(path) => (path, false),
                Err(error) => {
                    tracing::warn!(
                        underlying = %asset.underlying,
                        %error,
                        "initial price walk failed; asset starts dormant"
                    );
                    (Vec::new(), true)
                }
            };
            let underlying = asset.underlying.clone();
            let current = asset.initial_price;
            map.insert(
                underlying,
                AssetState {
                    config: asset,
                    path,
                    cursor: 0,
                    current,
                    dormant,
                },
            );
        }

        Arc::new(Self {
            sink,
            config,
            assets: RwLock::new(map),
            price_tx,
            clock,
            stopped: AtomicBool::new(false),
        })
    }

    /// The injected venue clock this simulator stamps its steps from — the shared
    /// handle the actors also read for `venue_ts`.
    #[must_use]
    #[inline]
    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    /// Subscribes to the bounded price-update broadcast — one receiver per
    /// consumer. A laggard drops (`RecvError::Lagged`) and re-reads.
    #[must_use]
    #[inline]
    pub fn subscribe(&self) -> broadcast::Receiver<PriceUpdate> {
        self.price_tx.subscribe()
    }

    /// The latest price for `underlying`, in **cents**.
    #[must_use]
    pub fn get_price(&self, underlying: &str) -> Option<Cents> {
        self.read_assets()
            .get(underlying)
            .map(|state| state.current)
    }

    /// A snapshot of every asset's latest price, in **cents**.
    #[must_use]
    pub fn get_all_prices(&self) -> HashMap<String, Cents> {
        self.read_assets()
            .iter()
            .map(|(underlying, state)| (underlying.clone(), state.current))
            .collect()
    }

    /// The configured underlyings this simulator hosts, **sorted** for a
    /// deterministic order regardless of map iteration.
    #[must_use]
    pub fn underlyings(&self) -> Vec<String> {
        let mut underlyings: Vec<String> = self.read_assets().keys().cloned().collect();
        underlyings.sort_unstable();
        underlyings
    }

    /// Manually overrides `underlying`'s price — the programmatic price insert.
    ///
    /// The override is journaled the **same way** as a walked step: it publishes a
    /// [`PriceUpdate`] and routes a [`VenueCommand::SimStep`](crate::exchange::VenueCommand::SimStep)
    /// through the sink (never a bare write), stamped with the current virtual
    /// venue-clock instant. It also revives a dormant asset.
    ///
    /// # Errors
    ///
    /// - [`SimError::UnknownUnderlying`] if `underlying` is not a configured asset
    ///   (the transport-level `POST /api/v1/prices` override goes through the actor
    ///   on its own path and is not scoped to configured assets).
    pub fn set_price(&self, underlying: &str, price: Cents) -> Result<(), SimError> {
        {
            let mut assets = self.write_assets();
            let state = assets
                .get_mut(underlying)
                .ok_or_else(|| SimError::UnknownUnderlying(underlying.to_string()))?;
            state.current = price;
            state.dormant = false;
        }
        // The venue Clock service (#028) is the instant source now — a pure atomic
        // read, never `SystemTime`; `emit` journals the step before it publishes.
        let now_ms = self.clock.now_ms();
        self.emit(now_ms, underlying, price);
        Ok(())
    }

    /// Emits one price step for every asset **at the current venue-clock instant**
    /// — the interval loop's body, exposed for deterministic stepping.
    ///
    /// It **reads** `now_ms` from the injected clock (a pure atomic load, never
    /// `SystemTime`); advancing the clock is the cadence driver's / control
    /// coordinator's job, so a caller composes an advance with an emit
    /// (`clock.step()` / `clock.tick()` then `step_once()`). Each asset serves its
    /// next pre-generated price (regenerating off-lock when exhausted, backing off
    /// dormant on a walk failure), publishes a [`PriceUpdate`], and routes a
    /// journaled `SimStep` through the sink. A stopped simulator is a no-op. Assets
    /// are stepped in a **sorted** order so the per-tick sequence does not depend on
    /// map iteration (rule 5).
    pub fn step_once(&self) {
        if self.stopped.load(Ordering::Relaxed) {
            return;
        }
        let now_ms = self.clock.now_ms();
        for underlying in self.underlyings() {
            if let Some(price) = self.next_price(&underlying) {
                self.emit(now_ms, &underlying, price);
            }
        }
    }

    /// Advances the venue clock by one cadence tick and emits a step at the new
    /// instant — the composed advance-then-emit the wall-cadence loop runs.
    ///
    /// The clock advance ([`SimClock::tick`](crate::simulation::SimClock::tick))
    /// happens **off** the sequenced read: realtime / accelerated track wall time,
    /// and stepped is a no-op read (its clock is advanced by the control
    /// coordinator, so a wall loop does not drive its cadence).
    pub fn tick_once(&self) {
        if self.stopped.load(Ordering::Relaxed) {
            return;
        }
        self.clock.tick();
        self.step_once();
    }

    /// Spawns the interval loop, which calls [`tick_once`](Self::tick_once) on each
    /// wall tick (advancing the venue clock and emitting a step) until
    /// [`stop`](Self::stop) is called (or the simulator is dropped). Must be called
    /// within a `tokio` runtime.
    ///
    /// In [`ClockMode::Stepped`](crate::simulation::ClockMode::Stepped) the venue
    /// clock is advanced by the control coordinator, not a wall cadence, so the
    /// spawned loop does not auto-step there — it is a no-op that logs and returns.
    pub fn spawn(self: &Arc<Self>) {
        if matches!(self.clock.mode(), ClockMode::Stepped { .. }) {
            tracing::info!(
                "price simulator: stepped clock mode — cadence is driven by the control \
                 coordinator, not the wall loop; not spawning"
            );
            return;
        }
        let this = Arc::clone(self);
        let interval = self.config.tick_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if this.stopped.load(Ordering::Relaxed) {
                    break;
                }
                this.tick_once();
            }
            tracing::debug!("price simulator loop stopped");
        });
    }

    /// Stops the interval loop at its next tick (idempotent).
    #[inline]
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    /// Whether the interval loop has been stopped.
    #[must_use]
    #[inline]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    // ---- internals -------------------------------------------------------

    /// Journals the `SimStep` through the sink **first**, then publishes the
    /// [`PriceUpdate`] only if the step was admitted (rule 3). A dropped/rejected
    /// step has no journal record, so a subscriber must never observe its price —
    /// replay could not reproduce it. No lock is held across the sink call or the
    /// broadcast send (rule 8).
    fn emit(&self, now_ms: EventTimestamp, underlying: &str, price: Cents) {
        // Journal-before-publish: route the step onto the sequenced path and bail
        // if the bounded sink dropped it — no price is published for a step that
        // was never journaled.
        if !self.sink.apply_step(now_ms, underlying, price, None, None) {
            return;
        }
        // Admitted: send-and-continue on the bounded broadcast (a laggard drops,
        // rule 7).
        let _ = self.price_tx.send(PriceUpdate {
            underlying: underlying.to_string(),
            price,
            now_ms,
        });
    }

    /// Serves `underlying`'s next price, regenerating its path **off-lock** when
    /// exhausted. Returns `None` for a dormant asset, an unknown underlying, or a
    /// walk that fails to regenerate (which backs the asset off dormant).
    fn next_price(&self, underlying: &str) -> Option<Cents> {
        loop {
            let served = {
                let mut assets = self.write_assets();
                let state = assets.get_mut(underlying)?;
                if state.dormant {
                    return None;
                }
                match state.path.get(state.cursor).copied() {
                    // Advance the cursor with CHECKED arithmetic; a cursor that
                    // cannot advance is treated as exhausted (regenerate off-lock)
                    // rather than wrapping to a repeated index. Unreachable in
                    // practice: the path length is far below `usize::MAX`, so `get`
                    // returns `None` long before the cursor could overflow.
                    Some(price) => match state.cursor.checked_add(1) {
                        Some(next) => {
                            state.cursor = next;
                            state.current = price;
                            Some(price)
                        }
                        None => None,
                    },
                    None => None,
                }
            };
            match served {
                Some(price) => return Some(price),
                // Exhausted: regenerate off-lock, then loop to serve the fresh path.
                None => {
                    if !self.regenerate(underlying) {
                        return None;
                    }
                }
            }
        }
    }

    /// Regenerates `underlying`'s path from its current price, **off the state
    /// lock** (the `optionstratlib` generation runs with no guard held). On success
    /// the fresh path replaces the old and the cursor resets; on failure the asset
    /// is backed off dormant. Returns whether a fresh path is now available.
    fn regenerate(&self, underlying: &str) -> bool {
        // Read the seed (config + current price) under a brief lock, then drop it.
        let (config, seed) = {
            let assets = self.read_assets();
            let Some(state) = assets.get(underlying) else {
                return false;
            };
            (state.config.clone(), state.current)
        };

        match generate_path(
            config.walk_type,
            seed,
            config.drift,
            config.volatility,
            self.config.step_ms,
            self.config.horizon_steps,
        ) {
            Ok(fresh) => {
                let mut assets = self.write_assets();
                if let Some(state) = assets.get_mut(underlying) {
                    state.path = fresh;
                    state.cursor = 0;
                    state.dormant = false;
                }
                true
            }
            Err(error) => {
                tracing::warn!(
                    underlying,
                    %error,
                    "price walk regeneration failed; backing off dormant"
                );
                let mut assets = self.write_assets();
                if let Some(state) = assets.get_mut(underlying) {
                    state.dormant = true;
                }
                false
            }
        }
    }

    /// The read guard over the asset map, recovering a poisoned lock rather than
    /// panicking (the map holds no invariant a panic could have corrupted).
    #[inline]
    fn read_assets(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, AssetState>> {
        self.assets.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// The write guard over the asset map, recovering a poisoned lock.
    #[inline]
    fn write_assets(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, AssetState>> {
        self.assets.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl std::fmt::Debug for PriceSimulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PriceSimulator")
            .field("assets", &self.read_assets().len())
            .field("clock_now_ms", &self.clock.now_ms().get())
            .field("stopped", &self.is_stopped())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::exchange::VenueCommand;

    /// A [`StepSink`] that records every applied step, in order, for assertions —
    /// synchronous, non-blocking, deterministic, and free of any actor / market
    /// maker (the sim→sequencer→MM wiring is covered by the integration tests).
    #[derive(Default)]
    struct CollectingStepSink {
        steps: Mutex<Vec<VenueCommand>>,
    }

    impl CollectingStepSink {
        fn drain(&self) -> Vec<VenueCommand> {
            std::mem::take(&mut self.steps.lock().unwrap_or_else(PoisonError::into_inner))
        }
    }

    impl StepSink for CollectingStepSink {
        fn apply_step(
            &self,
            now_ms: EventTimestamp,
            underlying: &str,
            price: Cents,
            bid: Option<Cents>,
            ask: Option<Cents>,
        ) -> bool {
            self.steps
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(VenueCommand::SimStep {
                    now_ms,
                    underlying: underlying.to_string(),
                    price,
                    bid,
                    ask,
                });
            // A synchronous collector always admits the step.
            true
        }
    }

    /// A [`StepSink`] that reports every step **dropped** (never admitted) — models
    /// a full or closed bounded forwarder, so a journal-before-publish caller must
    /// publish no price for the step.
    struct DroppingStepSink;

    impl StepSink for DroppingStepSink {
        fn apply_step(
            &self,
            _now_ms: EventTimestamp,
            _underlying: &str,
            _price: Cents,
            _bid: Option<Cents>,
            _ask: Option<Cents>,
        ) -> bool {
            false
        }
    }

    fn config() -> SimulationConfig {
        SimulationConfig {
            horizon_steps: 8,
            ..SimulationConfig::default()
        }
    }

    fn gbm_asset(underlying: &str, price: u64) -> AssetConfig {
        AssetConfig::new(
            underlying,
            Cents::new(price),
            0.20,
            WalkTypeConfig::GeometricBrownian,
        )
    }

    fn simulator(assets: Vec<AssetConfig>) -> (Arc<PriceSimulator>, Arc<CollectingStepSink>) {
        let sink = Arc::new(CollectingStepSink::default());
        // A stepped venue clock at the deterministic epoch, advancing by exactly the
        // walk's step interval — the sim stamps each emission from this shared clock.
        let clock = SimClock::stepped(DEFAULT_START_MS, DEFAULT_STEP_MS);
        let sim = PriceSimulator::new(assets, config(), sink.clone(), clock);
        (sim, sink)
    }

    #[test]
    fn test_new_pregenerates_and_exposes_initial_price() {
        let (sim, _sink) = simulator(vec![gbm_asset("BTC", 5_000_000)]);
        assert_eq!(sim.get_price("BTC"), Some(Cents::new(5_000_000)));
        assert_eq!(sim.get_price("ETH"), None);
        assert_eq!(
            sim.get_all_prices().get("BTC").copied(),
            Some(Cents::new(5_000_000))
        );
        assert_eq!(sim.underlyings(), vec!["BTC".to_string()]);
    }

    #[test]
    fn test_step_once_broadcasts_and_routes_a_sim_step() {
        let (sim, sink) = simulator(vec![gbm_asset("BTC", 5_000_000)]);
        let mut rx = sim.subscribe();
        sim.step_once();

        // A PriceUpdate is broadcast in cents, carrying the venue-clock instant.
        let update = rx.try_recv().expect("a price update was broadcast");
        assert_eq!(update.underlying, "BTC");
        assert_eq!(update.now_ms, EventTimestamp::new(DEFAULT_START_MS));

        // …and the SAME step routed through the sink as a journaled SimStep.
        let steps = sink.drain();
        assert_eq!(steps.len(), 1, "one asset, one SimStep per step");
        match &steps[0] {
            VenueCommand::SimStep {
                now_ms,
                underlying,
                price,
                ..
            } => {
                assert_eq!(underlying, "BTC");
                assert_eq!(*price, update.price, "sink price matches the broadcast");
                assert_eq!(
                    *now_ms,
                    EventTimestamp::new(DEFAULT_START_MS),
                    "the venue-clock now_ms is carried in the command"
                );
            }
            other => panic!("expected a SimStep, got {other:?}"),
        }
    }

    #[test]
    fn test_now_ms_advances_deterministically_per_step() {
        let (sim, sink) = simulator(vec![gbm_asset("BTC", 5_000_000)]);
        // Emit at the current instant, then advance the stepped venue clock by
        // exactly its interval — so each emission is stamped `start_ms + n ×
        // step_ms`, deterministic and monotonic, from the injected clock (never
        // `SystemTime`).
        for _ in 0..4 {
            sim.step_once();
            sim.clock().step();
        }
        let stamps: Vec<u64> = sink
            .drain()
            .into_iter()
            .filter_map(|command| match command {
                VenueCommand::SimStep { now_ms, .. } => Some(now_ms.get()),
                _ => None,
            })
            .collect();
        assert_eq!(
            stamps,
            vec![
                DEFAULT_START_MS,
                DEFAULT_START_MS + DEFAULT_STEP_MS,
                DEFAULT_START_MS + 2 * DEFAULT_STEP_MS,
                DEFAULT_START_MS + 3 * DEFAULT_STEP_MS,
            ]
        );
    }

    #[test]
    fn test_set_price_routes_through_the_sink_as_a_sim_step() {
        let (sim, sink) = simulator(vec![gbm_asset("BTC", 5_000_000)]);
        sim.set_price("BTC", Cents::new(4_200_000))
            .expect("BTC is a configured asset");
        assert_eq!(sim.get_price("BTC"), Some(Cents::new(4_200_000)));
        let steps = sink.drain();
        assert_eq!(steps.len(), 1);
        match &steps[0] {
            VenueCommand::SimStep {
                underlying, price, ..
            } => {
                assert_eq!(underlying, "BTC");
                assert_eq!(*price, Cents::new(4_200_000), "the override is journaled");
            }
            other => panic!("expected a SimStep, got {other:?}"),
        }
    }

    #[test]
    fn test_set_price_unknown_underlying_errors() {
        let (sim, sink) = simulator(vec![gbm_asset("BTC", 5_000_000)]);
        assert!(matches!(
            sim.set_price("ETH", Cents::new(1)),
            Err(SimError::UnknownUnderlying(_))
        ));
        assert!(
            sink.drain().is_empty(),
            "an unknown override routes nothing"
        );
    }

    #[test]
    fn test_paths_regenerate_off_lock_when_exhausted() {
        // Horizon is 8; stepping well past it must keep serving prices (the path
        // regenerated off-lock) rather than stalling or panicking.
        let (sim, sink) = simulator(vec![gbm_asset("BTC", 5_000_000)]);
        let ticks = 8 * 3 + 1;
        for _ in 0..ticks {
            sim.step_once();
        }
        let count = sink
            .drain()
            .into_iter()
            .filter(|c| matches!(c, VenueCommand::SimStep { .. }))
            .count();
        assert_eq!(
            count, ticks,
            "every tick served a price across regenerations"
        );
    }

    #[test]
    fn test_walk_failure_backs_off_dormant() {
        // A zero volatility fails the walk at the f64 boundary → the asset starts
        // dormant and serves no price, but the simulator does not panic and other
        // assets are unaffected.
        let bad = AssetConfig::new(
            "BAD",
            Cents::new(1_000),
            0.0,
            WalkTypeConfig::GeometricBrownian,
        );
        let (sim, sink) = simulator(vec![bad, gbm_asset("BTC", 5_000_000)]);
        sim.step_once();
        let underlyings: Vec<String> = sink
            .drain()
            .into_iter()
            .filter_map(|c| match c {
                VenueCommand::SimStep { underlying, .. } => Some(underlying),
                _ => None,
            })
            .collect();
        assert_eq!(
            underlyings,
            vec!["BTC".to_string()],
            "the dormant asset serves nothing; the healthy one still steps"
        );
    }

    #[test]
    fn test_stopped_simulator_is_a_no_op() {
        let (sim, sink) = simulator(vec![gbm_asset("BTC", 5_000_000)]);
        sim.stop();
        assert!(sim.is_stopped());
        sim.step_once();
        assert!(
            sink.drain().is_empty(),
            "a stopped simulator serves nothing"
        );
    }

    #[test]
    fn test_price_broadcast_is_bounded_and_laggard_drops() {
        let sink = Arc::new(CollectingStepSink::default());
        let sim = PriceSimulator::new(
            vec![gbm_asset("BTC", 5_000_000)],
            SimulationConfig {
                horizon_steps: 64,
                price_channel_capacity: 2,
                ..SimulationConfig::default()
            },
            sink,
            SimClock::stepped(DEFAULT_START_MS, DEFAULT_STEP_MS),
        );
        let mut rx = sim.subscribe();
        for _ in 0..6 {
            sim.step_once();
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
        assert!(saw_lagged, "a slow consumer lags on the bounded broadcast");
    }

    #[test]
    fn test_emit_does_not_publish_when_the_step_is_dropped() {
        // The sink reports every step DROPPED (no journal record). `emit` must
        // journal-before-publish, so a dropped step publishes NO `PriceUpdate` —
        // a subscriber never observes a price replay cannot reproduce (rule 3).
        let sim = PriceSimulator::new(
            vec![gbm_asset("BTC", 5_000_000)],
            config(),
            Arc::new(DroppingStepSink),
            SimClock::stepped(DEFAULT_START_MS, DEFAULT_STEP_MS),
        );
        let mut rx = sim.subscribe();
        sim.step_once();
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "a dropped step must not publish a PriceUpdate"
        );

        // A programmatic override is guarded the same way: the drop suppresses its
        // broadcast too.
        sim.set_price("BTC", Cents::new(4_200_000))
            .expect("BTC is a configured asset");
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "a dropped override must not publish a PriceUpdate"
        );
    }

}
