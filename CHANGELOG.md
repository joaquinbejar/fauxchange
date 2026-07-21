# Changelog

All notable changes to `fauxchange` are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The full versioning and release-process policy lives in the design docs
(local until v0.1.0).

## [Unreleased]

### Added

- Versioned `VenueCommand` / `VenueEvent` v1 envelope + lossless outcomes (#5)
  in `src/exchange/` (`envelope.rs`, `identity.rs`) — the venue's own internal
  instruction set, carrying the account/owner/TIF/order-type/STP identity the
  upstream `OptionChainCommand` drops **in** and the captured fills **out**,
  while invoking upstream matching unchanged
  ([ADR-0006](docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
  [ADR-0009](docs/adr/0009-lossless-venue-envelope-outcomes.md)). Adds
  `VenueCommand` (`AddOrder` / `CancelOrder` / `Replace` / `MassCancel` /
  `SetInstrumentStatus` / `EvictExpiredOrders` and the control-plane
  `MarketMakerControl` / `Clock` / `SimStep`), the `VenueEvent`
  (`{ schema, underlying_sequence, venue_ts, command, outcome }`, mandatory
  `schema = "venue.v1"` tag), and the lossless `VenueOutcome` branches —
  `Added { fills, resting_quantity, stp_cancelled }`,
  `Market { fills, unfilled_quantity, stp_cancelled }` (the empty-book zero-fill
  case representable), `Replace { cancelled, add: AddOutcome
  (Filled { fills, stp_cancelled } / Rested { fills, resting_quantity,
  stp_cancelled } / Rejected) }` (explicitly non-atomic),
  `MassCancelled { affected: ordered Vec<CancelledLeg> }` (count derived),
  `Cancelled` / `InstrumentStatusChanged` / `Evicted` / `ControlApplied` /
  `Rejected { reason }`. Because a self-trade-prevention removal
  (`cancel_maker` / `cancel_both`) is a side-effect of a single add turn (one
  sequence, one event, no separate cancel command), the add-side outcomes carry
  a `stp_cancelled: Vec<CancelledLeg>` (`CancelReason::SelfTradePrevention`,
  empty when no STP fired) so the affected resting legs are recorded losslessly
  ([ADR-0009 §4](docs/adr/0009-lossless-venue-envelope-outcomes.md)); `Rejected`
  carries none because an STP removal is itself a book mutation. Models the
  **two linked legs per match** with the
  lossless internal `Fill` (adds the STP `owner: Hash32` and the seam `Side` to
  the #004 DTO `Fill`, sharing one `execution_id` across the maker + taker leg,
  each with its own account/side/liquidity/fee) and the venue-owned
  `CancelReason`. Adds the run `LineageId` with the deterministic composite-id
  grammar `"{lineage_id}:{underlying}:{underlying_sequence}:{index}"` for venue
  order ids and `execution_id`s (collision-free across runs and underlyings —
  `BTC:1 ≠ ETH:1`) and the `JournalHeader { schema_version, lineage_id }`.
  Re-exports the upstream `STPMode` at the boundary (available without the
  `sequencer` feature). Envelope serde pins `PascalCase` variant tags,
  `snake_case` fields, and `deny_unknown_fields`, and reuses the upstream seam
  newtypes (`Side` → `BUY`/`SELL`, `TimeInForce` → `GTC`, `Hash32` hex) with
  cents as integers. `MassCancelScope` / `MassCancelType` are owned venue-side
  mirrors of the upstream enums (which sit behind the `sequencer` feature that
  pulls the on-disk journal store #005 excludes), mapped 1:1 by the #006 actor.
  New tests: per-variant construction / serde units, id-grammar determinism +
  cross-underlying uniqueness + two-leg `execution_id` sharing, the
  `venue_envelope_serde_identity` and `venue_id_grammar_collision_free` property
  tests in `tests/property.rs`, and the `venue.v1` golden
  (`tests/golden/venue/add_order_event.json`) in `tests/golden.rs`. No new
  dependencies.

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
