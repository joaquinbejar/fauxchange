# Changelog

All notable changes to `fauxchange` are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The full versioning and release-process policy lives in the design docs
(local until v0.1.0).

## [Unreleased]

### Added

- REST/WS DTO layer (#4) in `src/models.rs`: the venue value objects and their
  `serde` + `utoipa::ToSchema` projection onto the wire, prices in integer cents
  and timestamps in venue-clock milliseconds. Covers the whole inherited Backend
  route surface — order entry (`PlaceLimitOrderRequest` / `PlaceMarketOrderRequest`
  + responses), bulk/cancel-all, price get/set, hierarchy CRUD views
  (`InstrumentView`, quotes, underlying/expiration/strike summaries), the
  account-scoped `ExecutionRecord` and the distinct public-anonymised WS `fill`
  print (no `account`/`fee`; the four join keys `execution_id` /
  `underlying_sequence` / `venue_ts` / `liquidity`), positions, controls, chain /
  volatility-surface, greeks / metrics, OHLC, auth token, and admin
  snapshot req/resp. Adds the value objects `Order` / `Fill` / `Position` /
  `Account`, the wire enums with pinned casing (`Permission` / `Side` /
  `OptionStyle` / `OrderStatus` lowercase, `TimeInForce` `UPPERCASE`,
  `OrderType` / `LiquidityFlag` `snake_case`), the opaque identity newtypes
  (`AccountId` / `ClientOrderId` / `VenueOrderId` / `ExecutionId`), and the
  `WsMessage` protocol (`#[serde(tag = "type", content = "data")]`, all
  server→client variants), whose `error` variant reuses the #003 `WsError`
  envelope verbatim. Money fields are only `Cents` / `SignedCents` newtypes (the
  sole floats are documented analytics — Greeks/IV/VWAP/impact); every request
  DTO carries `#[serde(deny_unknown_fields)]`; and `validate_order_shape`
  enforces the boundary order rules (Limit⇒price, Market⇒none, quantity>0,
  price>0) as a typed `VenueError`. Adds `ToSchema` to the #003 `ErrorEnvelope`
  / `WsError` / `WsErrorCode` / `WsErrorCategory` (architect finding B) and the
  `utoipa` 5 dependency (already resolved transitively — no new tree version).
  New tests: co-located validation + casing + `deny_unknown_fields` units,
  the `order_dto_serde_identity` / `ws_message_serde_identity` property tests in
  `tests/property.rs`, and per-DTO / per-`WsMessage`-variant wire goldens under
  `tests/golden/{rest,ws}/` (asserting integer cents and the `type` discriminant,
  with an `UPDATE_GOLDEN` regeneration mode) in `tests/golden.rs`.

- Typed error boundary (#3) in `src/error.rs`: the closed-set `VenueError`
  (`NotFound` / `InvalidOrder` / `Unauthorized` / `Forbidden(Permission)` /
  `RateLimited` / `Overflow` / `Upstream(#[from] option_chain_orderbook::Error)`)
  with three renderings of one failure. `IntoResponse` maps each variant to
  exactly one HTTP status (404/400/401/403/429/500) via an exhaustive match,
  emits a typed `ErrorEnvelope` JSON body (never `serde_json::Value`), and
  attaches `Retry-After` + `X-RateLimit-Remaining` context on 429. The FIX
  reject **seam** (`FixRejectContext` → `FixReject` with `FixRejectKind` /
  `FixRejectReason`) selects `ExecutionReport (8) Rejected` / `OrderCancelReject
  (9)` / `MarketDataRequestReject (Y)` / `BusinessMessageReject (j)` / `Reject
  (3)` **by inbound message context** and the reason category **by the error**,
  per the authoritative `docs/03 §8` matrix — types and a pure mapping only, no
  wire encoding (that lands with the acceptor, #039). The versioned WebSocket
  envelope (`WsError`, schema `ws-error.v1`) maps every variant to a stable
  `(code, category)` with `terminal` / `retryable` / `retry_after_ms`
  (`Unauthorized` terminal, command errors non-terminal). Internal / `Overflow`
  / `Upstream` details are redacted on the HTTP body, the FIX `Text (58)`, and
  the WS message; the `#002` `MoneyError` / `SymbolError` fold into `VenueError`
  via `From`. Adds `Permission { Read, Trade, Admin }` (lowercase wire) in
  `src/models.rs` — the canonical home per `docs/01 §8` — and the `axum` 0.8
  dependency (lean, `json`-only feature set) for `IntoResponse`. Error-envelope
  goldens under `tests/golden/{rest,ws}/` with shape tests in `tests/golden.rs`.

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
