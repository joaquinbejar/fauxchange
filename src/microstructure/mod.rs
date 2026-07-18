//! Domain: latency injection, fee schedules, self-trade prevention, and
//! per-instrument contract specs — the venue's personality expressed as
//! configuration, not code.
//!
//! Most knobs **surface an upstream type** (`FeeSchedule`, `STPMode`,
//! `ContractSpecs` / `ValidationConfig`) applied at the leaf by `orderbook-rs`;
//! `fauxchange` exposes them as declarative venue config rather than inventing new
//! mechanisms ([05 §1](../../../docs/05-microstructure-config.md#1-scope)). The one
//! knob with **no upstream equivalent** is the venue-owned `max_price_cents` /
//! `min_price_cents` admission band (the upstream `ValidationConfig` carries no
//! price bound), which also anchors the checked-fee proof
//! ([05 §4.1](../../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable)).
//!
//! Governed by `docs/05-microstructure-config.md`.

mod apply;
mod config;
mod error;
mod fees;
mod latency;
mod specs;
mod stp;

pub use apply::apply_to_underlying;
pub use config::{FileMicrostructure, MicrostructureConfig, MicrostructureProfile};
pub use error::{LatencyConfigError, MicrostructureConfigError, PriceBoundError};
pub use fees::FeeConfig;
pub use latency::{FileLatency, LatencyConfig, LatencyModel, LatencyOffset};
pub use specs::{ContractSpecsConfig, PriceBounds, ResolvedContractSpecs};
pub use stp::{StpConfig, StpMode};
