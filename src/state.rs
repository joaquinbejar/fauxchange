//! Application layer: `AppState`, the shared `Arc` wiring of the domain
//! (`exchange`, `market_maker`, `simulation`, `microstructure`),
//! persistence (`db`), and services (`auth`) layers that every gateway
//! handler receives.
//!
//! Governed by `docs/02-matching-architecture.md`.
