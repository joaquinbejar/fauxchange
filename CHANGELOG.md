# Changelog

All notable changes to `fauxchange` are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The full versioning and release-process policy lives in the design docs
(local until v0.1.0).

## [Unreleased]

### Added

- Domain boundary newtypes, integer-cents money, and the symbol grammar (#2)
  in `src/exchange/`: the `Cents` / `SignedCents` / `Notional` money newtypes
  (private fields, validated constructors, checked arithmetic returning a typed
  `MoneyError`, bare-integer wire via `#[serde(transparent)]`); re-exports of
  the upstream boundary newtypes (`OrderId`, `Side`, `Price`, `Quantity`,
  `TimeInForce`, `OptionStyle`, `ExpirationDate`, `TimestampMs`, `Hash32`,
  `InstrumentStatus`) so the venue names them without redefinition; the
  venue-owned `EventTimestamp` and `SequenceNumber`; a `Symbol` newtype routed
  through the upstream `SymbolParser` with the `validate_venue_expiry`
  invariant (`ExpirationDate::Days` refused, non-canonical `23:59:59 UTC`
  instant rejected as an aliasing error); and the `Instrument` value object.
  Adds the `option-chain-orderbook`, `optionstratlib`, `serde`, and `thiserror`
  dependencies (plus `proptest` / `serde_json` dev-deps) and property tests
  (`cents_never_lossy`, `symbol_roundtrip`) in `tests/property.rs`.

- Crate skeleton (#1): the canonical module tree from
  `docs/00-design-bootstrap.md` §6 as empty, `//!`-documented stubs —
  `config`, `error`, `models`, `state`, `gateway/{rest,ws,fix}`,
  `exchange`, `market_maker`, `simulation`, `microstructure`, `ohlc`,
  `db`, `auth` — plus `#![forbid(unsafe_code)]`, crate-level docs in
  `src/lib.rs` (`error`/`models` re-exported at the crate root), a
  commented bootstrap outline in `src/main.rs`, and the empty
  `tests/`, `benches/`, `migrations/`, `docker/` directories. No venue
  behavior yet — every module is an empty stub so later issues add code
  into a tree that already compiles.

## [0.0.1] - 2026-07-12

### Added

- Reserved the `fauxchange` crate name on crates.io.

[Unreleased]: https://github.com/joaquinbejar/fauxchange/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/joaquinbejar/fauxchange/releases/tag/v0.0.1
