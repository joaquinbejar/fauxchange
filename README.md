# fauxchange

## fauxchange

`fauxchange` (*faux* + *exchange*) is an exchange-in-a-box: a local
options exchange simulator for testing trading systems — "LocalStack
for trading". It wraps the upstream matching engine and option-chain
hierarchy from [`orderbook-rs`] / [`option-chain-orderbook`] behind
three protocol front-ends — REST, WebSocket, and a FIX 4.4 gateway
built on [`IronFix`] primitives — with deterministic record/replay,
configurable microstructure, JWT auth, and optional PostgreSQL
persistence.

[`orderbook-rs`]: https://github.com/joaquinbejar/OrderBook-rs
[`option-chain-orderbook`]: https://github.com/joaquinbejar/Option-Chain-OrderBook
[`IronFix`]: https://github.com/joaquinbejar/IronFix

### Status

Under active design and early implementation — see the numbered
design docs under `docs/` (source of truth during the design phase)
and `docs/ROADMAP.md` for the delivery plan starting at v0.1.0. This
crate is a **venue**, not a matching engine: matching, fills, fees,
self-trade prevention, and the option-chain hierarchy live upstream
and are never reimplemented here.

### Architecture

`fauxchange` ships as a single crate with one binary; sub-domains are
modules, not workspace members. Dependencies flow one way only —
transport → application → domain / persistence / services:

- **Transport** — [`gateway`], the three protocol front-ends (REST,
  WebSocket, FIX 4.4) that translate wire formats into venue commands.
  A gateway translates; it never decides.
- **Application** — [`state`], the shared wiring that assembles the
  domain, persistence, and service layers into the state every
  gateway handler is given; and [`seed`], the bounded seeding phase
  that applies a scenario manifest before the venue flips to serving.
- **Domain** — [`exchange`] (the sequenced order path onto the
  upstream matching stack), [`market_maker`] (persona-driven
  quoting), [`simulation`] (synthetic price generation and replay),
  [`microstructure`] (latency, fees, STP, contract specs), and
  [`ohlc`] (OHLC bar aggregation).
- **Persistence** — [`db`], optional `sqlx`/PostgreSQL storage for the
  journal, executions, and venue configuration; the venue runs fully
  in-memory when `DATABASE_URL` is unset.
- **Services** — [`auth`], JWT authentication and the permission
  model shared by every gateway; and [`subscription`], the WebSocket
  market-data service (the per-instrument subscription manager + broadcast
  fan-out) the `/ws` gateway reads through `AppState`.

[`error`] and [`models`] are the shared boundary — typed errors mapped
to HTTP status codes and FIX rejects, and the DTOs that carry data
across every protocol surface — and are re-exported at the crate
root. [`config`] is cross-cutting: venue configuration.

No module outside `gateway/` reaches into another gateway's
internals, and nothing in `src/` imports back from this crate root —
see `CLAUDE.md` "Module Boundaries" for the enforced rules.

### Determinism — the bounded, testable guarantee

Determinism is `fauxchange`'s product, stated as a **bounded contract**, not a
byte-for-byte promise the dependencies cannot keep. Given the **same journal**
(the `venue.v1` `VenueEvent` stream, including the `MarketMakerControl` /
`Clock` / `SimStep` commands), the **same config manifest** (seed, clock mode,
microstructure config, instrument seed), and the **same pinned crate/dependency
versions**, a replay reproduces **identical fills, events, and resting book
state per underlying**, judged by *ordered `VenueEvent`-stream equality per
underlying* — top-of-book after each event is a cheap witness. Replay and
recovery share **one algorithm**: re-execution with the stored event as the
integrity oracle, always into a **fresh** registry.

Excluded from the oracle, recomputed live and **never asserted equal**: mark
price, unrealised P&L, Greeks, and any derived analytic float; process-global
numeric registry ids (the canonical symbol string is the identity); the engine
clock and its `Uuid::new_v4()` trade-id namespace; cross-underlying interleaving
(there is no venue-wide total order); out-of-sequencer state (an admin snapshot
restore starts a new journal lineage — it is not a replay input); and OHLC bars
(an exclusion **by derivation** — the same fills reproduce the same bars). The
synthetic price **walk** is reproduced from the journal, **not** by seed
regeneration (the `optionstratlib` sampler owns its own RNG); every stored
expiry is an absolute `ExpirationDate::DateTime`. The guarantee and its full
exclusion index are enforced by the `tests/determinism.rs` oracle.

License: MIT
