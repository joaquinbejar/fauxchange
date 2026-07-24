[![MIT License](https://img.shields.io/badge/license-MIT-blue)](./LICENSE)
[![Crates.io](https://img.shields.io/crates/v/fauxchange.svg)](https://crates.io/crates/fauxchange)
[![Downloads](https://img.shields.io/crates/d/fauxchange.svg)](https://crates.io/crates/fauxchange)
[![Stars](https://img.shields.io/github/stars/joaquinbejar/fauxchange.svg)](https://github.com/joaquinbejar/fauxchange/stargazers)
[![Issues](https://img.shields.io/github/issues/joaquinbejar/fauxchange.svg)](https://github.com/joaquinbejar/fauxchange/issues)
[![PRs](https://img.shields.io/github/issues-pr/joaquinbejar/fauxchange.svg)](https://github.com/joaquinbejar/fauxchange/pulls)

[![Build Status](https://img.shields.io/github/actions/workflow/status/joaquinbejar/fauxchange/ci.yml)](https://github.com/joaquinbejar/fauxchange/actions)
[![Coverage](https://img.shields.io/codecov/c/github/joaquinbejar/fauxchange)](https://codecov.io/gh/joaquinbejar/fauxchange)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://docs.rs/fauxchange)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](https://www.rust-lang.org)



## fauxchange

`fauxchange` (*faux* + *exchange*) is an **exchange-in-a-box**: a local
options exchange simulator for testing trading systems — think
*"LocalStack for trading"*. Point your order-management system, market-maker,
or risk engine at it over **REST, WebSocket, or FIX 4.4**, and it behaves
like a real options venue — a live order book, real matching and fills,
fees, self-trade prevention, market-data fan-out, and a deterministic
record/replay tape you can rewind — all on your laptop or in CI, with
**one `docker compose up`**.

It wraps the upstream matching engine and option-chain hierarchy from
[`orderbook-rs`] / [`option-chain-orderbook`] behind three protocol
front-ends, adds a FIX 4.4 gateway built on [`IronFix`] primitives, prices
options through [`optionstratlib`], and packages the whole thing as a single
runnable binary. This crate is a **venue**, not a matching engine: matching,
fills, fees, self-trade prevention, and the option-chain hierarchy live
upstream and are never reimplemented here.

[`orderbook-rs`]: https://github.com/joaquinbejar/OrderBook-rs
[`option-chain-orderbook`]: https://github.com/joaquinbejar/Option-Chain-OrderBook
[`IronFix`]: https://github.com/joaquinbejar/IronFix
[`optionstratlib`]: https://github.com/joaquinbejar/OptionStratLib

### Why fauxchange

Integration-testing a trading system against a real exchange is slow,
costly, non-deterministic, and often impossible outside market hours.
`fauxchange` gives you a venue you fully control:

- **Deterministic** — the same journal + config replays to identical fills,
  events, and resting book state, so a failing integration test reproduces
  byte-for-byte (within a bounded, documented oracle — see below).
- **Multi-protocol** — the exact same order over REST and over FIX produces
  the exact same book state and fills; a fill renders identically on REST,
  WS, and FIX. Test each surface against one source of truth.
- **Configurable microstructure** — tick/lot size, fee schedules, STP mode,
  latency injection, and market-maker personas are declarative venue config,
  not code forks.
- **Batteries included** — JWT auth, a market-making engine that quotes a
  live two-sided book, synthetic price walks, optional Postgres persistence,
  and a packaged conformance harness.

### What's inside

- **REST gateway** (Axum) — ~45 routes covering the option hierarchy, order
  entry (place / cancel / replace / mass-cancel / status), positions,
  executions, quotes, Greeks, prices, venue controls (kill-switch / halt),
  admin snapshots, and replay export. Every endpoint is `#[utoipa::path]`-
  annotated and served with a live **Swagger UI** / OpenAPI document.
- **WebSocket gateway** — market-data fan-out over the `orderbook`,
  `trades`, `quotes`, `prices`, and `fills` channels, plus market-maker
  control. WS is subscription + control only — it carries **no** order-entry
  message by design.
- **FIX 4.4 gateway** (on [`IronFix`]) — a real TCP acceptor with a session
  FSM: `Logon`/`Logout`/`Heartbeat`/`TestRequest`, `ResendRequest` /
  `SequenceReset` gap-fill, `NewOrderSingle (D)` / `OrderCancelRequest (F)` /
  `OrderCancelReplaceRequest (G)` / `OrderMassCancelRequest (q)` /
  `OrderStatusRequest (H)`, `ExecutionReport (8)` and reject flows, and
  `MarketDataRequest (V)` → snapshot `(W)` / incremental `(X)`. Logon
  authenticates against the **same** permission model as REST/WS. Shipped
  **disabled by default** — enable it with `[fix] enabled = true`.
- **Sequenced exchange core** — a single-writer per-underlying actor that
  journals a write-ahead `VenueCommand`/`VenueEvent` envelope, then invokes
  the upstream matching **unchanged**, with snapshot/restore, crash
  recovery, and a `ClOrdID` idempotency index.
- **Market maker** — a persona-driven engine (`OptionPricer` + `Quoter`)
  that keeps a live two-sided book, priced through [`optionstratlib`].
- **Simulation & replay** — synthetic price walks ([`optionstratlib`]
  `WalkType`), stepped deterministic sessions, a controllable venue clock,
  and a replay driver that re-runs a recorded tape.
- **Persistence (optional)** — `sqlx`/PostgreSQL for the journal,
  executions, and venue config when `DATABASE_URL` is set; fully in-memory
  otherwise. `docker compose up` provides both.
- **Auth** — JWT (RS256) with an Argon2id account registry, a
  `Permission { Read, Trade, Admin }` model, and a sliding-window rate
  limiter enforced on every mutating op across all three protocols.
- **Conformance harness** — `fauxchange conformance` spins ephemeral
  in-process venues, drives the frozen parity + conformance suites across
  REST/WS/FIX, and emits a machine-readable report a downstream CI gates on.

> **Not yet implemented:** OHLC bar aggregation. The `GET .../ohlc` endpoint
> is wired but returns an empty bar list until the aggregator lands.

### Money & time

**Money crosses every boundary as integer cents** (`u64`/`u128`/`i64`) —
never `f64` — across every DTO, FIX field, WS message, and DB column.
Timestamps are milliseconds (`u64`) or ISO-8601 strings. Derived analytic
floats (Greeks, IV, mark price) are the only documented exception, and they
are excluded from the determinism oracle.

### Built on the ecosystem

`fauxchange` is the venue that ties together four upstream crates by the
same author — it wires them into a runnable exchange rather than
reimplementing any of them:

- **[`IronFix`]** — FIX 4.4 framing and typed primitives. `fauxchange`
  consumes four of its crates: `ironfix-core` (the `MsgType` / `CompId` /
  `SeqNum` vocabulary and the `RawMessage` zero-copy decode view),
  `ironfix-tagvalue` (the `Decoder` / `Encoder` + checksum framing),
  `ironfix-dictionary` (the pinned `FIX.4.4` begin-string), and
  `ironfix-transport` (the `FixCodec` wire framer). The **TCP acceptor, the
  session FSM, the typed message structs, and the resend/gap-fill logic are
  `fauxchange`'s own work built on top of these primitives** — IronFix
  provides the framing; the venue provides the session.
- **[`option-chain-orderbook`]** — the hierarchical options books
  (`Underlying → Expiration → Strike → OptionOrderBook`) and, via its
  `sequencer` feature, the command/event/journal/replay machinery the
  deterministic order path is built on.
- **[`orderbook-rs`]** — the lock-free matching engine underneath: fills,
  fees, and self-trade prevention live here.
- **[`optionstratlib`]** — options pricing, Greeks, `ExpirationDate`, and
  the `WalkType` price-walk models used by the simulator and market maker.

### Quick start

One command brings the venue up (REST + WS; FIX and Postgres are opt-in):

```bash
docker compose -f docker/docker-compose.yml up
# REST + Swagger UI on http://localhost:8080/swagger-ui
# add Postgres persistence:  docker compose ... --profile persistent up
```

Or run it from source:

```bash
cargo run --release            # in-memory venue, self-seeded from seeds/default.toml
cargo run --release -- conformance   # run the cross-surface conformance harness
```

As a library, the DTOs, the typed boundary error, and the venue core are
public — see [`models`], [`error`], and [`exchange`].

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

[`conformance`] is the packaged `fauxchange conformance` harness (a
transport/bootstrap-layer artifact over [`state`]): it spins ephemeral
in-process venues, drives the frozen parity + conformance suites across
REST/WS/FIX, and emits a machine-readable report a downstream CI gates on.

No module outside `gateway/` reaches into another gateway's
internals, and nothing in `src/` imports back from this crate root —
see `CLAUDE.md` "Module Boundaries" for the enforced rules.

### Three gateways, one order path

Protocol parity is a contract, with a scoped shape:

- **Order-entry parity is REST ≡ FIX.** An order placed over FIX and the
  same order over REST produce identical book state, fills, and events. WS
  is not an order-entry protocol.
- **Observation parity is REST / WS / FIX.** A fill and its market data
  render identically on all three surfaces.
- **Control parity is REST / WS.** The control plane (kill-switch, halt,
  snapshots) has no FIX message.

Order mutations enter [`exchange`] through the sequenced order path
regardless of protocol. A gateway with private matching semantics, or a book
mutated off the sequenced path, is a bug — it silently breaks replay.

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

### Project status

`fauxchange` is a working, extensively-tested implementation: the REST, WS,
and FIX gateways, the sequenced exchange core, the market maker, the
simulator/replay driver, auth, optional persistence, and the conformance
harness are all in place, exercised by a large unit + integration + golden +
adversarial + property test suite. It targets adoption in production CI and
integration infrastructure, so performance and security are acceptance
criteria, not afterthoughts. Edition 2024, stable toolchain, `#![forbid(unsafe_code)]`,
MIT-licensed.



## 🧩 The ecosystem

`fauxchange` is the venue that wires together four upstream crates by the same
author — it never reimplements matching, the option-chain hierarchy, FIX
framing, or options math:

| Crate | Role in fauxchange |
|-------|--------------------|
| [IronFix](https://github.com/joaquinbejar/IronFix) | FIX 4.4 framing + typed primitives (`FixCodec`, `MsgType`/`CompId`/`SeqNum`, tag-value `Decoder`/`Encoder`). The acceptor, session FSM, typed messages, and resend/gap-fill are the venue's own work on top. |
| [option-chain-orderbook](https://github.com/joaquinbejar/Option-Chain-OrderBook) | Hierarchical options books (`Underlying → Expiration → Strike`) and the `sequencer` feature (command/event/journal/replay). |
| [orderbook-rs](https://github.com/joaquinbejar/OrderBook-rs) | The lock-free matching engine underneath — fills, fees, and self-trade prevention. |
| [OptionStratLib](https://github.com/joaquinbejar/OptionStratLib) | Options pricing, Greeks, `ExpirationDate`, and `WalkType` price walks. |

## 🛠 Makefile commands

This project ships a `Makefile` with the common developer tasks. A few of the
most useful:

### 🔧 Build & run

```sh
make build         # cargo build (debug, all features)
make release       # cargo build --release (zero warnings)
make run           # boot the venue locally (REST + WS on 127.0.0.1:8080)
make run-seeded    # boot with seeded instruments (BTC, ETH) for manual testing
```

### 🧪 Test & quality

```sh
make test              # unit + integration + doctest suite (mirrors CI `test`)
make test-conformance  # the tests/ conformance + e2e suite (golden/determinism/parity/rest/ws)
make fmt               # apply rustfmt
make lint              # clippy, all targets/features, warnings denied
make fix               # auto-apply compiler-suggested fixes
make doc               # missing-docs gate on every pub item
make pre-push          # the canonical ready-to-push gate (fmt + lint + test + readme + doc)
```

### 📦 Packaging & docs

```sh
make docker-smoke  # docker e2e smoke: compose up -> health -> order -> WS fill -> shutdown
make soak          # stability soak (flat memory, no sequence gaps, restart-from-journal)
make readme        # regenerate README.md from src/lib.rs docs via cargo-readme
make publish       # publish fauxchange to crates.io (confirm first)
```

### 📈 Coverage, benches & security

```sh
make coverage         # cargo tarpaulin — text + lcov summary
make bench-regression # reduced-sample hot-path benches + the bench-regression gate
make audit         # cargo audit — RustSec advisory scan
make deny          # cargo deny — license/ban/duplicate/source policy
```

## Contribution and Contact

Contributions are welcome. If you would like to contribute, please:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and ensure the project still builds and all tests pass (`make pre-push`).
4. Commit your changes and push your branch to your fork.
5. Open a pull request against `main`.

If you have any questions, issues, or feedback, please contact the maintainer:

### **Contact Information**
- **Author**: Joaquín Béjar García
- **Email**: jb@taunais.com
- **Telegram**: [@joaquin_bejar](https://t.me/joaquin_bejar)
- **Repository**: <https://github.com/joaquinbejar/fauxchange>
- **Documentation**: <https://docs.rs/fauxchange>

We appreciate your interest and look forward to your contributions!

**License**: MIT
