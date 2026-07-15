//! Bootstrap entry point for the `fauxchange` binary.
//!
//! This stub compiles and does nothing yet; it exists so the crate has a
//! single binary target from the start (`docs/00-design-bootstrap.md`
//! §6, module map). The bootstrap sequence below is the target shape —
//! each step lands with the module it wires, not here:
//!
//! 1. Load venue configuration (`fauxchange::config`) — instruments,
//!    microstructure profiles, gateway ports.
//! 2. Initialize the `tracing` subscriber (`EnvFilter`, JSON output in
//!    production images).
//! 3. Assemble `AppState` (`fauxchange::state`) — wires the exchange,
//!    market maker, simulation, microstructure, db, and auth layers.
//! 4. Spawn the gateways (`fauxchange::gateway::rest`,
//!    `fauxchange::gateway::ws`, `fauxchange::gateway::fix`) and the
//!    background tasks (simulator, market-maker requote loop, OHLC
//!    aggregator).
//!
//! `anyhow` is permitted in this file only, per Override O-3
//! (`docs/governance-precedence.md` §2) — every other layer keeps typed
//! `thiserror` errors. It is not yet added: there is nothing fallible to
//! bootstrap.

fn main() {
    // 1. config::load()            — see fauxchange::config
    // 2. tracing subscriber init   — see docs/06-deployment.md §9 (Observability)
    // 3. state::AppState::new(...) — see fauxchange::state
    // 4. tokio::spawn gateway::{rest, ws, fix}::serve(...) + background tasks
}
