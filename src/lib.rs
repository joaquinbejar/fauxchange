#![forbid(unsafe_code)]

//! # fauxchange
//!
//! `fauxchange` (*faux* + *exchange*) is an exchange-in-a-box: a local
//! options exchange simulator for testing trading systems — "LocalStack
//! for trading". It wraps the upstream matching engine and option-chain
//! hierarchy from [`orderbook-rs`] / [`option-chain-orderbook`] behind
//! three protocol front-ends — REST, WebSocket, and a FIX 4.4 gateway
//! built on [`IronFix`] primitives — with deterministic record/replay,
//! configurable microstructure, JWT auth, and optional PostgreSQL
//! persistence.
//!
//! [`orderbook-rs`]: https://github.com/joaquinbejar/OrderBook-rs
//! [`option-chain-orderbook`]: https://github.com/joaquinbejar/Option-Chain-OrderBook
//! [`IronFix`]: https://github.com/joaquinbejar/IronFix
//!
//! ## Status
//!
//! Under active design and early implementation — see the numbered
//! design docs under `docs/` (source of truth during the design phase)
//! and `docs/ROADMAP.md` for the delivery plan starting at v0.1.0. This
//! crate is a **venue**, not a matching engine: matching, fills, fees,
//! self-trade prevention, and the option-chain hierarchy live upstream
//! and are never reimplemented here.
//!
//! ## Architecture
//!
//! `fauxchange` ships as a single crate with one binary; sub-domains are
//! modules, not workspace members. Dependencies flow one way only —
//! transport → application → domain / persistence / services:
//!
//! - **Transport** — [`gateway`], the three protocol front-ends (REST,
//!   WebSocket, FIX 4.4) that translate wire formats into venue commands.
//!   A gateway translates; it never decides.
//! - **Application** — [`state`], the shared wiring that assembles the
//!   domain, persistence, and service layers into the state every
//!   gateway handler is given.
//! - **Domain** — [`exchange`] (the sequenced order path onto the
//!   upstream matching stack), [`market_maker`] (persona-driven
//!   quoting), [`simulation`] (synthetic price generation and replay),
//!   [`microstructure`] (latency, fees, STP, contract specs), and
//!   [`ohlc`] (OHLC bar aggregation).
//! - **Persistence** — [`db`], optional `sqlx`/PostgreSQL storage for the
//!   journal, executions, and venue configuration; the venue runs fully
//!   in-memory when `DATABASE_URL` is unset.
//! - **Services** — [`auth`], JWT authentication and the permission
//!   model shared by every gateway.
//!
//! [`error`] and [`models`] are the shared boundary — typed errors mapped
//! to HTTP status codes and FIX rejects, and the DTOs that carry data
//! across every protocol surface — and are re-exported at the crate
//! root. [`config`] is cross-cutting: venue configuration.
//!
//! No module outside `gateway/` reaches into another gateway's
//! internals, and nothing in `src/` imports back from this crate root —
//! see `CLAUDE.md` "Module Boundaries" for the enforced rules.

pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod exchange;
pub mod gateway;
pub mod market_maker;
pub mod microstructure;
pub mod models;
pub mod ohlc;
pub mod simulation;
pub mod state;

// Re-exported at the crate root: `error` and `models` are the shared
// boundary types every gateway and downstream crate depends on directly.
// `error` now exports the `VenueError` boundary and its HTTP / FIX / WS
// renderings (#003), so its glob is live.
pub use error::*;
// `models` currently exposes `Permission` ahead of the full DTO surface (#004).
pub use models::*;

#[cfg(test)]
mod tests {
    /// Smoke test: the crate links and its module tree is reachable.
    #[test]
    fn test_crate_links_and_module_tree_is_reachable() {
        assert_eq!(2 + 2, 4);
    }
}
