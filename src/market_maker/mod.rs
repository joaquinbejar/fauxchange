//! Domain: the market-maker engine, option pricer, and quoter — persona-driven
//! quoting routed onto the **same sequenced order path** as client orders
//! ([`crate::exchange`]).
//!
//! The subsystem is the persona substrate ported from the
//! `option-chain-orderbook-backend` market maker
//! ([specs §3](../../docs/specs/option-chain-orderbook-backend.md#3-market-maker)),
//! with the `fauxchange` seam wired in: a requote is a **journaled
//! [`VenueCommand`](crate::exchange::VenueCommand)**, not a side channel, so
//! generated liquidity is part of the determinism oracle
//! ([015](../../milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md),
//! [02 §4](../../docs/02-matching-architecture.md)).
//!
//! - [`OptionPricer`] — the Black-Scholes theoretical value and the first-order
//!   Greeks, computed **entirely through `optionstratlib`** (no hand-rolled
//!   Black-Scholes or Greeks), guarding the `f64` boundary (`None`, never a
//!   poisoned value).
//! - [`Quoter`] — the persona-shaped two-sided quote generator; a pure,
//!   deterministic function of its [`QuoteInput`], plus the [`calculate_edge`]
//!   capture calc ([`Quoter::calculate_edge`]).
//! - [`MarketMakerConfig`] / [`validate_control_value`] — the range-validated
//!   persona knobs (`spread_multiplier ∈ [0.1, 10.0]`, `size_scalar ∈ [0.0, 1.0]`,
//!   `directional_skew ∈ [-1.0, 1.0]`; `NaN` and out-of-range values are
//!   **rejected**, never coerced).
//! - [`MarketMakerEngine`] — the `update_price → requote → update_quote`
//!   pipeline, the kill switch, the edge calc, the [`MarketMakerEvent`]
//!   broadcast, and the replay-mute hook — routing every quote through the
//!   [`CommandSink`].
//! - [`CommandSink`] / [`ActorCommandSink`] — the seam onto the sequenced path.
//!
//! The venue-reserved market-maker identity marker
//! ([`market_maker_account`](crate::exchange::market_maker_account) /
//! [`MARKET_MAKER_OWNER`](crate::exchange::MARKET_MAKER_OWNER) /
//! [`is_market_maker_command`](crate::exchange::is_market_maker_command)) is a
//! venue-wide contract consumed by both this domain and the WS service, so it
//! lives in [`crate::exchange`] beside the `VenueCommand` it tags — not here.
//!
//! Governed by `docs/05-microstructure-config.md`.
//!
//! [`calculate_edge`]: Quoter::calculate_edge

pub mod config;
pub mod control_hub;
pub mod engine;
pub mod persona;
pub mod pricer;
pub mod quoter;
pub mod sink;

pub use self::config::{
    DIRECTIONAL_SKEW_MAX, DIRECTIONAL_SKEW_MIN, MarketMakerConfig, MarketMakerEvent,
    SIZE_SCALAR_MAX, SIZE_SCALAR_MIN, SPREAD_MULTIPLIER_MAX, SPREAD_MULTIPLIER_MIN,
    validate_control_knobs, validate_control_value,
};
pub use self::control_hub::MarketMakerControlHub;
pub use self::engine::{MarketMakerEngine, RecoveredMmLeg};
pub use self::persona::{PersonaConfig, PersonaError, PersonaJitter, PersonaJitterDraw};
pub use self::pricer::{DEFAULT_IV, DEFAULT_RISK_FREE_RATE, OptionPricer};
pub use self::quoter::{
    DEFAULT_BASE_SIZE, DEFAULT_BASE_SPREAD_BPS, QuoteInput, QuoteParams, Quoter,
};
pub use self::sink::{ActorCommandSink, CommandSink, DEFAULT_SINK_CAPACITY};
