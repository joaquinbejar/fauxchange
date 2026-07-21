//! Domain: synthetic price walks, stepped deterministic sessions, the
//! replay driver, and clock control.
//!
//! Governed by `docs/04-market-data-and-replay.md`.
//!
//! ## What is here today (#016)
//!
//! The [`PriceSimulator`] over `optionstratlib` walks: an async interval loop that
//! walks each configured underlying and publishes [`PriceUpdate`]s over a bounded
//! `tokio::broadcast`, with `get_price` / `get_all_prices` / `set_price`. Each
//! generated step is **not** a bare price write — it enters the venue through a
//! [`StepSink`], which routes it onto the per-underlying sequenced order path as a
//! journaled [`VenueCommand::SimStep`](crate::exchange::VenueCommand::SimStep) and
//! drives the market maker ([`MarketMakerEngine`](crate::market_maker::MarketMakerEngine)),
//! whose requotes enter the **same** actor path as their own journaled orders —
//! so synthetic prices and the liquidity they induce are both replayable exactly
//! like real order flow ([04 §2](../../docs/04-market-data-and-replay.md#2-synthetic-price-generation)).
//!
//! - [`WalkTypeConfig`] — the v1 surfaced walk set (`GeometricBrownian` /
//!   `MeanReverting` / `JumpDiffusion`), each mapped 1:1 onto an
//!   [`optionstratlib::simulation::WalkType`]; the walk runs **entirely through
//!   `optionstratlib`** (no hand-rolled stochastic process), and the `f64`
//!   boundary is guarded on the way back to integer [`Cents`](crate::exchange::Cents).
//! - [`PriceSimulator`] — the ported loop, re-pointed at the sequencer and the
//!   venue clock. `now_ms` comes from a **deterministic virtual venue clock**
//!   (`start_ms + step × step_ms`), never `SystemTime`, and is carried in the
//!   `SimStep` so replay reuses the exact value.
//! - [`StepSink`] / [`VenueStepSink`] — the seam onto the sequencer + market
//!   maker (one journaled step: the `SimStep` plus the requotes it induces).
//!
//! ## Determinism: journal-driven, not seed-regenerated
//!
//! `optionstratlib`'s walk sampler constructs its own RNG per draw and cannot
//! consume the run seed, so the walk is **excluded** from same-seed regeneration.
//! The guaranteed reproduction is the **journal**: the `SimStep`s and the requotes
//! they cause are recorded, and replay re-executes them directly (the replay
//! driver mutes the live market maker via
//! [`MarketMakerEngine::set_muted`](crate::market_maker::MarketMakerEngine::set_muted)
//! so it never re-derives a cascading requote,
//! [04 §2, §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
//!
//! ## The clock as a venue service (#028)
//!
//! [`SimClock`] is the one venue time source — realtime / accelerated / stepped,
//! seeded from the run-level seed and recorded in the [`RunManifest`]. It
//! implements the [`VenueClock`](crate::exchange::VenueClock) seam the
//! per-underlying actors stamp `venue_ts` from and the price walk stamps its
//! `SimStep.now_ms` from, so a single seeded clock decides every timestamp and
//! `now_ms` on the sequenced path is a pure atomic load (never `SystemTime`). A
//! stepped advance is a `Clock` [`VenueCommand`](crate::exchange::VenueCommand)
//! fanned to every underlying actor by the venue-control coordinator (owned by
//! [`AppState`](crate::state::AppState)), so the advance joins the recorded input
//! stream and replay reproduces it from the journaled value.
//!
//! ## Not here yet
//!
//! The stepped deterministic sessions + smile curve and the replay driver (v0.3,
//! #030/#031) land later; this module hosts the price-walk generation, its
//! sequenced-path routing, and the clock service.

pub mod clock;
pub mod manifest;
pub mod simulator;
pub mod sink;
pub mod walk;

pub use self::clock::{
    ClockMode, CorrelationId, DEFAULT_ACCEL_MULTIPLIER, DEFAULT_CLOCK_START_MS,
    DEFAULT_STEP_INTERVAL_MS, SimClock, VenueClockConfig,
};
pub use self::manifest::RunManifest;
pub use self::simulator::{
    AssetConfig, DEFAULT_HORIZON_STEPS, DEFAULT_PRICE_CHANNEL_CAPACITY, DEFAULT_START_MS,
    DEFAULT_STEP_MS, DEFAULT_TICK_INTERVAL, PriceSimulator, PriceUpdate, SimulationConfig,
};
pub use self::sink::{DEFAULT_STEP_SINK_CAPACITY, StepSink, VenueStepSink};
pub use self::walk::{SimError, WalkTypeConfig};
