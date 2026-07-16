# Changelog

All notable changes to `fauxchange` are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The full versioning and release-process policy lives in the design docs
(local until v0.1.0).

## [Unreleased]

### Added

- **`bench-hdr` harness + first `BENCH.md` baseline** (#20) in `benches/`,
  `Cargo.toml`, `tests/bench_harness.rs`, and `BENCH.md` at the repo root
  ([020](milestones/v0.1-backend-core/020-bench-hdr-harness-baseline.md),
  [07 §5](docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention)).
  Four registered `[[bench]]` targets (`harness = false`, so each controls its
  own measurement loop rather than criterion's default statistical-convergence
  runner): `hp1_order_path` (the sequenced order path — full-turn closed-loop,
  the upstream match cost paired per turn and reported as its own out-of-budget
  series, the resulting venue-added delta, the write-ahead command/event
  append's own cost, and a coordinated-omission-corrected open-loop sojourn-time
  series via a genuine intended-send-time load generator), `hp2_ws_fanout` (a
  committed `VenueEvent` fanned out to `N ∈ {1, 10, 100, 1 000}` subscriber
  broadcast slots over the real `TeeFanOut(StoreFanOut, WsFanOut)` /
  `OrderbookSubscriptionManager` from #008/#014, checking HP-1 p99 stays flat
  in N), `alloc_profile` (a `#[global_allocator]` counting-allocator profile of
  the steady-state actor turn, both direct (`UnderlyingActor::handle`) and via
  the async `ActorHandle::submit` mailbox round-trip), and a supplementary
  `criterion_match_cost` (a real, working example of the convention's
  "criterion for orchestration" half, explicitly never cited as `BENCH.md`
  evidence — mean is not an accepted quantile report). Every reported
  distribution goes through `benches/support/hdr.rs`'s `hdrhistogram`-backed
  p50/p99/p99.9/p99.99 report — never criterion's default mean — and the
  quantile/histogram plumbing itself is unit-tested against known
  distributions (uniform, constant, bimodal, empty) in
  `tests/bench_harness.rs` (5/5 passing), pulling in the exact same
  `benches/support/hdr.rs` file `cargo bench` uses via `#[path]` rather than a
  duplicated copy. Two new dev-only dependencies, each with a Cargo.toml audit
  note: `hdrhistogram` (`7`, `default-features = false` — only the base
  `Histogram` type is used, not `.hgrm` serialisation or `SyncHistogram`) and
  `criterion` (`0.8`, `default-features = false` — only `Bencher::iter` is
  used, not `rayon`/`plotters`/HTML reports). `BENCH.md` records the first
  real baseline: every figure was actually measured by running `cargo bench`
  on the reference host (Apple M4 Max, macOS, `rustc 1.97.0`) — none
  estimated or invented — with full run conditions, and every number is
  labelled a DESIGN TARGET comparison, never an achieved SLO. The baseline
  surfaces a real, reproducible finding worth a follow-up:
  `InMemoryVenueJournal::append`'s `(sequence, kind)` uniqueness check is a
  linear scan over the whole in-memory record stream, so the write-ahead
  append's cost — and therefore HP-1's full-turn p99/p99.9/p99.99 — grows
  with journal depth within a single run (p99 932 µs at ~105k journaled
  records vs 33 µs at ~2.2k, on the identical code path); HP-1's own "< 1 ms
  p99" DESIGN TARGET is only marginally met at that depth and is exceeded at
  p99.9/p99.99. The allocation profile likewise shows the steady-state turn is
  measurably far from the "zero heap allocation" DESIGN TARGET (~78 / ~63
  allocs per submitted command, direct vs. async-mailbox path) — honestly
  reported as a process-wide allocation-pressure count, not a call-stack
  attribution (no such profiler is available in this environment), and named
  as the regression-signal baseline going forward. HP-2's N-sweep confirms its
  DESIGN TARGET holds: p99 does not grow across a 1 000× increase in
  subscriber count. Deliberately out of scope, per the milestone: HP-3 (FIX
  parse, v0.4 #043), HP-4 (market-maker requote, v0.5 #050), HP-5 durable/
  PostgreSQL journal append (v0.3 #035), and the CI `bench-regression` gate
  (armed before v1.0, #053) — nothing in CI fails a PR on these numbers today,
  only confirms the benches compile (`cargo clippy --all-targets
  --all-features -- -D warnings`).

- **CI: `cargo audit` + `cargo deny` + fmt/clippy/test/build gates** (#19) in
  `.github/workflows/ci.yml`, `deny.toml`, `.cargo/audit.toml`,
  `rust-toolchain.toml`, and the `Makefile`
  ([019](milestones/v0.1-backend-core/019-ci-audit-deny-lint.md),
  [08 §8](docs/08-threat-model.md#8-supply-chain-controls),
  [TESTING.md §11–§12](docs/TESTING.md#11-ci-matrix)). Wires the CI-matrix
  jobs — `fmt` (`cargo fmt --all --check`), `clippy`
  (`cargo clippy --all-targets --all-features -- -D warnings`), `test`
  (`cargo test --all-features`), `build-release` (`cargo build --release`,
  `RUSTFLAGS=-D warnings`), `doctests`, `msrv`, and the `golden` /
  `determinism` / `parity` suites (#4/#17/#18) — as the v0.1 **supply-chain
  gate from the first milestone**: `cargo-audit` and `cargo-deny` run on
  every push and on PRs into `main`/`release/**`, on a pinned runner
  (`ubuntu-24.04`) with every action pinned (no `latest`, no floating branch
  refs), and cancellation of superseded runs on this ref. `deny.toml`
  encodes the license allow-list actually present in the tree (MIT, MIT-0,
  Apache-2.0, BSD-2-Clause, BSD-3-Clause, BSL-1.0, CC0-1.0, ISC, Unicode-3.0,
  Unlicense, Zlib, `bzip2-1.0.6` — enumerated with `cargo deny list`, no
  blanket wildcard), a crates.io-only source policy, a `wildcards = "deny"`
  ban on unpinned dependency ranges, and one documented advisory ignore
  (RUSTSEC-2024-0436, `paste` unmaintained — a compile-time proc-macro dep
  transitive via `optionstratlib → statrs → nalgebra → simba`, **not** a
  vulnerability, no safe upgrade available upstream); `.cargo/audit.toml`
  mirrors the same ignore for the `cargo audit` CLI a developer runs
  locally, so the two tools agree. The `[graph] targets` restriction to the
  platforms `fauxchange` actually builds for (Linux gnu/musl, macOS
  aarch64/x86_64) prunes UEFI/wasm-only transitive deps (`r-efi`,
  `LGPL-2.1-or-later`) that never compile here, rather than papering over
  them with a license exception. No actual vulnerability was found on the
  current dependency tree — a real advisory is never added to either ignore
  list; it fails the build. `rust-toolchain.toml` pins the stable toolchain
  (1.97.0); the `msrv` job pins 1.96.0, the verified floor
  (`expiration_date`, transitive via `optionstratlib`, requires rustc
  1.96 — confirmed locally that `cargo +1.95.0 check --all-features` fails
  and `cargo +1.96.0 check --all-features` passes). The `msrv` job's
  `cargo check` uses an explicit `cargo +${{ env.RUST_MSRV }}` override
  rather than relying on `dtolnay/rust-toolchain`'s `rustup default`: rustup
  toolchain-selection precedence is `+toolchain` override >
  `RUSTUP_TOOLCHAIN` env > `rust-toolchain(.toml)` file > `rustup default`,
  and the repo-root `rust-toolchain.toml` (pinning 1.97.0) OUTRANKS a bare
  `rustup default 1.96.0` — so an unqualified `cargo check` in that job
  would silently compile under 1.97.0 and never exercise the MSRV floor it
  claims to gate (confirmed locally: plain `cargo -V` resolves 1.97.0 with
  `rust-toolchain.toml` present; `cargo +1.96.0 -V` correctly resolves
  1.96.0 despite it). The `Makefile` adds `pre-push`
  (`fix fmt lint-fix test check-spanish release readme doc` — every binding
  Pre-Submission Checklist item in one canonical local gate, so local and CI
  never diverge), `lint-fix`, `check-spanish` (a diacritics heuristic over
  `src/` + `tests/` per `rules/global_rules.md`), `audit`, `deny`,
  `test-conformance`, `coverage`/`coverage-html`, `publish`, `run`/
  `run-seeded`, and `workflow-<job-id>` targets that run any CI job locally
  via `act` against the stable job ids above. Explicitly out of scope (per
  the milestone doc's Scope): the `fuzz` job (FIX gateway, v0.4), the
  `bench-regression` gate (armed before v1.0), the `migrations` job (lands
  with the optional PostgreSQL persistence layer, v0.2 #023), and the
  `docker-smoke` / `image-build` stages (land with the Dockerfile/compose
  topology and container hardening, v0.2 #025/#026, and the cold-bring-up
  e2e smoke, v0.2 #027). No `src/` change; no new crate dependency
  (`cargo-audit` / `cargo-deny` are CI-installed tools, not crate deps).
- **The v0.1 protocol-parity suite** (#18) in `tests/parity.rs` +
  `tests/conformance/`
  ([018](milestones/v0.1-backend-core/018-parity-fixtures-rest-ws.md),
  [03 §7](docs/03-protocol-surfaces.md),
  [TESTING.md §6–§7](docs/TESTING.md)). The milestone's primary acceptance test —
  the contract that the surface an order arrives on does not change what the
  venue does — scoped to the surfaces present at v0.1 (REST + WS; FIX joins at
  v0.4). **Reachability:** every documented Backend REST route is served with its
  OpenAPI shape (a `(path, methods)` inventory checked against the live
  `/api-docs/openapi.json`, plus a representative live-router sweep), and every
  documented WS message round-trips to its #004 golden. **Observation parity
  (REST ≡ WS):** one committed fill renders identically as a REST
  `ExecutionRecord` and a WS `fill` on the four join keys
  (`execution_id`/`liquidity`/`underlying_sequence`/`venue_ts`) plus
  price/quantity/side — both projections of the same committed event — while the
  WS `fill` omits `account`/`fee` (the public anonymised print) and the REST
  record carries them (the authoritative account-scoped log). **Market-data
  parity:** `orderbook_delta` carries a strictly-increasing per-instrument
  `instrument_sequence` and resulting-quantity semantics (the change quantity is
  the level's new total), and a laggard gap recovers by a fresh snapshot, never a
  resend. **Control parity (REST ≡ WS):** the REST kill-switch/enable and the WS
  `kill`/`enable` actions build the identical `MarketMakerControl` command and
  surface the identical honest not-routable outcome (`InvalidOrder` on both, not
  a fabricated success — the command is not yet routable, #015), and the Admin
  permission gate is identical across surfaces. **REST order-entry base:** place /
  partial-fill / cancel-replace driven over the live REST surface against
  identically-seeded fresh venues, compared under the documented **normalization
  rule** — protocol-only fields (transport `venue_ts`, and the per-surface
  order-id / `ClOrdID` mapping placeholders `order_id`/`new_order_id`/
  `client_order_id`) are normalized away, while `underlying_sequence`,
  `execution_id`, fills, and resting-book state are compared **verbatim**;
  unit-tested for which fields are stripped vs kept (including the STP
  `stp_cancelled` outcome shape). The normalization helper, the per-surface
  fresh-venue topology, and the cross-surface join-key projection live in a
  reusable `tests/conformance/` module so the v0.4 FIX order-entry arm (#41) and
  the v1.0 packaged conformance harness (#51) **extend** the suite rather than
  rewrite it.
- **The flagship determinism harness** (#17) in `tests/determinism.rs`
  ([017](milestones/v0.1-backend-core/017-determinism-test-harness.md),
  [02 §5–§6](docs/02-matching-architecture.md),
  [ADR-0006](docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
  [TESTING.md §5](docs/TESTING.md)). The one canonical record/replay harness
  `fauxchange`'s bounded determinism contract ships against from v0.1: `record`
  drives a `VenueCommand` stream through a fresh `MatchingExecutor`, journaling
  every write-ahead `(command, event)` pair and capturing the per-event
  top-of-book witness (proven to mirror a real `UnderlyingActor` journal
  record-for-record — `VenueEvent` value equality); `replay` reconstructs the
  events + witnesses by re-executing
  every journaled `VenueCommand` in `N` order into a **fresh** registry; and the
  **oracle** asserts ordered-`VenueEvent`-stream equality per underlying plus the
  top-of-book cheap witness. `recover` is the recovery-as-re-execution reducer —
  the same re-execution with the stored `VenueEvent` as the **integrity oracle**:
  a clean journal re-executes to events equal to the stored ones, a **corrupted
  stored event** halts with the typed `JournalCorruption { underlying, sequence }`
  naming the exact `(underlying, N)`, a **tail command with no paired event**
  re-executes to derive the event, and a **newer-than-binary envelope schema**
  refuses to start. **Exclusions are asserted AS exclusions**: mark price,
  unrealised P&L, Greeks, `instrument_sequence`, and the engine `Uuid`
  trade-id / clock namespace are **structurally absent** from a journaled
  `VenueEvent` (a `Fill` carries only its nine journaled join keys), two replays
  with **different live marks** still agree on the event stream while the
  live-recomputed unrealised P&L differs, and two records under **different venue
  clocks** capture identical outcomes (only `venue_ts` differs). **Fault injection
  at both append stages** (a consolidated `FaultJournal` over the real actor): a
  pre-execution append failure reuses `N` with no gap and replay reconstructs
  identically; a post-mutation event-append failure seals the underlying, and a
  restart re-executes the tail command to the identical event (the crossing fill
  survives the seal). **Lossless capture**: an `IOC` order that fills and returns
  `Err` from the upstream `_full` leaf is journaled with its fill (never a bare
  `Rejected`) and replays identically, and a **partial `Replace`** (cancel
  succeeded, `FOK` add rejected) replays as one identical
  `Replace { cancelled: true, add: Rejected }`. **Replay-stable expiries**: a
  canonical `ExpirationDate::DateTime` fixture replays identically, and a
  `Days`-relative expiry is rejected at load. Consolidates the determinism rows
  previously scattered across `tests/{order_path,actor,stores,snapshot,
  market_maker,simulation}.rs` into the one suite the `determinism` CI job (#19)
  targets; the randomised `journal_replay_reconstructs_book` property stays in
  `tests/property.rs`. Tests-only — no `src/` change.

- **The `PriceSimulator` over `optionstratlib` walks routed through the
  sequencer** (#16) in `src/simulation/`
  ([016](milestones/v0.1-backend-core/016-price-simulator-walks.md),
  [04 §2](docs/04-market-data-and-replay.md),
  [specs §5](docs/specs/option-chain-orderbook-backend.md#5-simulation-and-ohlc)).
  The Backend `PriceSimulator` is ported: an async interval loop that walks each
  configured underlying and publishes `PriceUpdate`s over a **bounded**
  `tokio::broadcast`, with `get_price` / `get_all_prices` / `set_price`; paths are
  pre-generated over a horizon and regenerated **off-lock** when exhausted, and a
  walk failure backs the asset off dormant rather than busy-looping. `WalkTypeConfig`
  surfaces the v1 set — `GeometricBrownian` / `MeanReverting` (OU) / `JumpDiffusion` —
  each mapped 1:1 onto an `optionstratlib::simulation::WalkType`; the walk runs
  **entirely through `optionstratlib`** (no hand-rolled stochastic process), and
  the **`f64` boundary is guarded** on the way back to integer `Cents` (a
  non-finite / negative / out-of-range price is rejected, never cast). Each step
  is **not** a bare price write: it enters the venue through a `VenueStepSink`,
  which routes it onto the per-underlying sequenced order path as a journaled
  `VenueCommand::SimStep` **and** drives the market maker (#15
  `update_price`), whose requotes enter the **same** actor path as their own
  journaled `AddOrder`s — so synthetic prices and the liquidity they induce are
  both replayable exactly like real order flow. A manual `set_price` override is
  journaled the same way. **The `now_ms` comes from a deterministic virtual venue
  clock** (`start_ms + step × step_ms`), never `SystemTime`, and is carried in the
  `SimStep` so replay reuses the exact value. **Determinism is journal-driven, not
  seed-regenerated**: `optionstratlib`'s walk sampler builds its own RNG per draw
  and cannot consume the run seed, so the walk is excluded from same-seed
  regeneration; the guaranteed reproduction is the journal — the integration test
  runs a simulated session (journaled `SimStep`s + requotes → a crossing fill) and
  the determinism test replays the journal into a **fresh** venue (with the live
  market maker muted, #15 `set_muted`) and reproduces byte-identical events,
  price path, and fills without cascading duplicate orders. Wired into `AppState`
  (replacing the `SimulatorPlaceholder`); the interval loop is not auto-started
  (a stepped-clock / bootstrap concern). **No new dependencies** — the
  `optionstratlib::simulation` API is in the existing (no-feature) build.

- **The market maker on the sequenced order path — `MarketMakerEngine` /
  `OptionPricer` / `Quoter`** (#15) in `src/market_maker/`
  ([015](milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md),
  [02 §4](docs/02-matching-architecture.md),
  [specs §3](docs/specs/option-chain-orderbook-backend.md#3-market-maker)). The
  Backend market maker is ported as the persona substrate with the `fauxchange`
  seam wired in: a requote is a **journaled `VenueCommand`, not a side channel**.
  A price update triggers `update_price → requote_symbol → update_quote`, which
  cancels the stale two-sided quote and adds a fresh one **through a
  `CommandSink`** onto the same per-underlying single-writer actor + journal as
  client orders — never a direct book call — so generated liquidity is part of
  the determinism oracle and replays exactly. Every requote order carries the
  venue-reserved market-maker identity (`market_maker_account()` /
  `MARKET_MAKER_OWNER`) so fills attribute to the maker and the WS subscription
  manager **suppresses the `orderbook_delta` for a requote** (MM liquidity lands
  in the next periodic snapshot; the rule keys on `is_market_maker_command`).
  **Options math goes entirely through `optionstratlib`** — `OptionPricer` builds
  an `optionstratlib::Options` and calls `optionstratlib::pricing::black_scholes`
  for the theoretical value and the `optionstratlib::greeks::Greeks` trait for
  `delta`/`gamma`/`vega`/`theta` (no hand-rolled Black-Scholes or Greeks; the
  Backend's `erf`/`norm_cdf` pricer is dropped). The **`f64` boundary is guarded**
  end-to-end: a non-finite/degenerate input yields `None`, never a poisoned
  value into a `QuoteParams`, an `AddOrder`, or a broadcast; money stays integer
  cents on the surface. **Determinism**: time-to-expiry is derived from the
  **venue clock** (`set_venue_now_ms`), never the wall clock, so
  `generate_quote` is a pure function of its `QuoteInput`. The persona knobs
  (`MarketMakerConfig { enabled, spread_multiplier, size_scalar,
  directional_skew }`) are clamped to `[0.1,10.0]` / `[0.0,1.0]` / `[-1.0,1.0]`
  with NaN-rejecting `validate_control_value`; every clamp change ends in a
  requote + a `ConfigChanged` broadcast. Also: the kill switch
  (`set_enabled`/`set_symbol_enabled`, `cancel_all_orders`/`cancel_symbol_orders`),
  the `on_order_filled` edge calc (`calculate_edge`, integer & overflow-safe),
  the bounded `MarketMakerEvent` broadcast (`subscribe()`), and the **replay-mute
  hook** (`set_muted` — a muted engine records prices but cascades no live
  requote, so the v0.3 replay driver's journaled requotes are never duplicated).
  Wired into `AppState` (replacing the `MarketMakerPlaceholder`) via an
  `ActorCommandSink` over the per-underlying actor handles; the requote loop runs
  off the client order path (a slow requote never inflates a client `AddOrder`'s
  latency). Venue-global `MarketMakerControl` routing through `AppState::submit`
  stays a documented control-plane seam — the engine and its setters are ready
  for it, but `submit` still declines a `MarketMakerControl` as not
  per-underlying-routable.
- **The WebSocket surface — the `WsMessage` protocol, channel producers, and the
  subscription manager** (#14) in `src/gateway/ws/` + the new `src/subscription.rs`
  service module ([03 §4, §4.1, §4.2](docs/03-protocol-surfaces.md),
  [01 §9.1](docs/01-domain-model.md)). `GET /ws` upgrades to the tagged
  `WsMessage` protocol behind an authenticated handshake: the bearer JWT is read
  from the `Authorization` header **or** a `?token=` / `?access_token=` query
  parameter (a browser WebSocket cannot set headers) and admitted through the
  venue's one `AuthService::admit` (baseline `Read`) — a missing/invalid token or
  an exhausted rate-limit budget **refuses the upgrade** (`401`/`429`), the socket
  never opens. The `OrderbookSubscriptionManager` (replacing the #010
  subscriptions placeholder in `AppState`) keeps a per-instrument monotonic
  `instrument_sequence` — the market-data namespace, **separate** from the
  journaled `underlying_sequence`, gap-repaired only by a fresh snapshot, never a
  resend — plus an event-sourced L2 aggregate over a **bounded**
  `tokio::broadcast` fan-out. **Every advertised channel has a real
  producer/filter/sequence policy**: `orderbook` (committed book mutation →
  snapshot then strictly-increasing resulting-quantity deltas), `trades` (one
  public print per match), `fills` (one **anonymised** print per committed fill
  leg — the four join keys only, **never** `account`/`fee`; account-scoped detail
  stays REST/FIX), and `prices` (the committed `SimStep` override); `quotes` is
  honest-pending on the `Quoter` (#015). **Only user-driven book mutations emit
  `orderbook_delta`** — a control-plane event never does. **Layering**: the
  manager + `WsFanOut` are a `crate::subscription` **service** (a sibling of
  `crate::auth`/`crate::ohlc`, **not** a gateway), so `AppState` owns them without
  importing `crate::gateway`; the generic `TeeFanOut` fan-out combinator lives in
  `crate::exchange` beside the `FanOut` trait. **Critical fan-out wiring**: a
  `WsFanOut` (a #006 `FanOut` impl) is composed with #008's `StoreFanOut` via the
  exchange-owned `TeeFanOut`, so `AppState` feeds the **same** post-journal
  `VenueEvent` to both the stores and the WS broadcast; the broadcast enqueue is
  O(1) and non-blocking, off the actor's critical path (a laggard drops and
  re-snapshots, never stalling the order path). **Client → server actions**
  (`subscribe`/`unsubscribe`/`batch_*`/`list_subscriptions`) manage per-connection
  subscription state capped at `MAX_SUBSCRIPTIONS_PER_CONNECTION` (256, a DoS
  control); the market-maker control actions (`set_spread`/`set_size`/`set_skew`/
  `kill`/`enable`) are rate-limited **then** `Admin`-gated (admission-first) and
  routed as sequenced `MarketMakerControl` commands (control parity, REST ≡ WS).
  **WS carries no order entry** — any place/cancel/replace-shaped frame is
  rejected with the typed (non-terminal) `WsError` envelope; an auth/terminal
  error closes the socket, a command error keeps it open. **DoS bounds**
  (docs/08 §5): a venue-wide concurrent-connection cap (`MAX_WS_CONNECTIONS` = 1024,
  a semaphore permit per socket → `503` at the ceiling, released on close), an
  idle/liveness reaper (a heartbeat protocol ping each 30 s; a connection with no
  inbound traffic for `MAX_IDLE_TICKS` = 4 ticks is closed), an up-front
  `MAX_BATCH_SIZE` (64) cap on `batch_*` before the array is iterated, and a
  64 KiB inbound frame/message cap (replacing axum's 16 MiB/64 MiB defaults).
  **Live-session re-validation**: each heartbeat tick re-checks the socket's
  session via `AuthService::revalidate_session` — a token revoked or expired since
  the handshake closes the socket with the terminal `Unauthorized` error (the
  handshake admits only once). Enables the axum `ws` feature (pulls
  `tokio-tungstenite` + `tungstenite` + `data-encoding` + `futures-sink` NEW;
  `sha1`/`base64` were already in the tree — only a new dep edge) and promotes
  `serde_json` to a direct dependency for client-frame parsing (already in the
  tree — no new crate); `BookSide` gains a derived `Ord` for the manager's
  touched-level set (additive, no wire change). **`MarketMakerControl` routing
  seam**: `AppState::submit` does not yet route the venue-global
  `MarketMakerControl` (a #010 deviation), so the control actions surface the
  honest not-routable error rather than fabricate a success — the same seam #013's
  REST controls hit. Tests: unit (subscribe→snapshot→delta ordering, anonymised
  fill shape, requote-no-delta, sub-cap enforcement, order-entry-frame rejection,
  control forbidden/rate-limit-first/not-routable non-terminal, connection-cap
  ceiling, batch-cap rejection, session revalidation revoke/expire/unknown);
  property (`ws_instrument_sequence_monotonic_per_symbol`); integration
  (`tests/ws.rs`) — the real `GET /ws` handshake over a bound server
  (`401`/`101`/query-param `101`/`429`), subscribe→sequenced-deltas-never-backward,
  laggard re-snapshot, live-session revalidation closes a revoked socket, and the
  typed error envelope close-vs-continue semantics.
- **The REST gateway — the ~50-route Backend surface on Axum 0.8 with utoipa
  OpenAPI + Swagger UI** (#13) in `src/gateway/rest/`
  ([03 §3, §10](docs/03-protocol-surfaces.md),
  [specs/option-chain-orderbook-backend.md §1](docs/specs/option-chain-orderbook-backend.md)).
  `create_router(Arc<AppState>) -> Router` assembles every Backend route group —
  health/meta, auth-token, controls, prices, underlyings/expirations/strikes
  hierarchy CRUD, volatility-surface, chain matrix, per-contract
  book/orders/quote/greeks/snapshot/last-trade/ohlc/metrics, orders (bulk +
  cancel-all), positions, executions, admin snapshots — as handlers extracting
  `State(Arc<AppState>)` and returning `Result<Json<T>, VenueError>`, each with
  `#[utoipa::path]` and its #004 DTOs registered in the served
  `/api-docs/openapi.json`; the Swagger UI is merged at `/swagger-ui`.
  **Order-entry is re-pointed onto the sequenced path**: `POST .../orders`,
  `.../orders/market`, `DELETE .../orders/{id}`, and bulk-place each translate to
  an `AddOrder`/`CancelOrder` `VenueCommand` submitted through `AppState::submit`
  (never a direct book call) and return the resulting event's
  `underlying_sequence` for cross-surface correlation. **Operation-class routing**
  ([03 §10](docs/03-protocol-surfaces.md#10-state-changing-operation-classification)):
  `POST /api/v1/prices` is journaled as a **SimStep**-class command (not a bare
  price write), runtime hierarchy create/delete is **refused as a manifest
  input**, and auth-token issuance + admin snapshots are replay exclusions.
  **Auth on every mutating op**: a shared JWT + sliding-window rate-limit layer
  (`AppState::auth().admit`) gates a baseline `Read` for all non-exempt routes
  and each handler gates its own `Trade`/`Admin`; `GET /health` is exempt from
  both, `POST /api/v1/auth/token` is JWT-exempt but peer-rate-limited. The
  **`ConnectInfo<SocketAddr>` → `PeerAddr`** injection layer feeds the real socket
  peer (never an `X-Forwarded-For` header) to the rate-limit key, and a bounded
  periodic task runs `RateLimiter::sweep_expired` off the request path (both
  DoS controls, [08 §5](docs/08-threat-model.md)). Adds `utoipa-swagger-ui` 9
  (axum 0.8, `vendored` assets → offline-safe build) and enables the axum
  server + tokio `net`/`time` features; `src/main.rs` now serves the router with
  the dev-key release gate. Book-state reads (quote/depth/chain/greeks/metrics),
  venue-global controls, and live snapshot capture/restore are honest empty
  projections or typed errors pending the actor book-read path and control-plane
  routing (flagged as `matching-expert` seam dependencies — no fabricated data).
  Review-hardening: the bulk endpoints are bounded (`MAX_BULK_ORDER_ITEMS` /
  `MAX_BULK_CANCEL_ITEMS` = 500, a DoS control so one account cannot monopolize a
  single-writer mailbox); `TokenRequest`'s `Debug` is hand-rolled to **redact the
  bootstrap secret**; `CancelOrderResponse`, `BulkOrderResultItem`, and
  `InstrumentToggleResponse` carry a **typed `sequence`** (not prose) so #018
  parity can read it; the limit-order status is TIF-aware so a killed `IOC`/`FOK`
  reports `Rejected` (never a false `Accepted`), and the instrument toggle reports
  *accepted and sequenced* rather than a confirmed effect (the applied/rejected
  outcome waits on the `Receipt`→`VenueOutcome` seam). `src/main.rs` installs a
  `tracing-subscriber` (fmt + `RUST_LOG` env filter) at boot so startup logs are
  not dropped.
- **The venue account registry — credentials, revocation epoch, and
  account-resolved bootstrap minting** (#12) in `src/auth.rs`, wired into
  `src/state.rs` ([ADR-0007](docs/adr/0007-fix-credentials-and-account-model.md),
  [01 §8](docs/01-domain-model.md), [06 §8](docs/06-deployment.md)). The
  registry-internal `Account { id (IS the JWT sub), owner: Hash32, permissions,
  credentials, revocation_epoch }` and `Credentials { fix_username, password_hash
  (Argon2id PHC, #[serde(skip_serializing)]), fix_comp_ids }` model an account
  once behind both credential paths; the `CompIdBinding` (SenderCompID,
  TargetCompID) is **declared now, enforced from v0.4** (ADR-0010). The in-memory
  `AccountRegistry` (DashMap) provisions from an explicit `Vec<AccountProvision>`
  (the seed-manifest **format** is #024), indexes accounts by `AccountId` (a
  direct JWT-`sub` lookup) **and** by FIX `Username (553)`, both resolving one
  account row + permission set, and implements the #011 `RevocationOracle`
  (`current_revocation_epoch`) so a `revoke` (which bumps the epoch) refuses the
  account's outstanding tokens on the next request via the existing
  `auth_middleware`. **Account-resolved minting** (`AccountRegistry::mint_for_account`,
  exposed as `AppState::mint_token`) replaces the Backend's ephemeral-subject
  minting: it authorises `AUTH_BOOTSTRAP_SECRET` **first** (no account
  enumeration pre-auth), resolves an **existing** `AccountId` to its **registered**
  permissions + current revocation epoch, and mints via #011's
  `JwtAuth::mint_token` — never a fresh `Uuid`, never arbitrary requested
  permissions. Passwords are hashed with **Argon2id** (`Argon2Hasher`) at the
  pinned OWASP baseline (`m = 19456 KiB`, `t = 2`, `p = 1`) with an optional
  `AUTH_PASSWORD_PEPPER` (an Argon2 secret, never written into the PHC string),
  constant-time verification, and **rehash-on-verify** when a stored hash used
  weaker parameters (`PasswordVerification`); the FIX login path
  (`verify_fix_password` → `FixLoginOutcome`) is schema-ready for the v0.4
  acceptor and equalises unknown-username timing. The plaintext, the hash, and the
  pepper never appear on the wire (the `password_hash` is `skip_serializing`) or
  in a log/error (redacting `Debug` on `Credentials` / `Account` / `AccountProvision`
  / `PasswordVerification` / `Argon2Hasher`; issuance errors carry only static
  labels). The `AccountStore` trait (a `RevocationOracle` supertrait exposing
  lookup-by-id / lookup-by-fix-username / verify / revoke / count) is the drop-in
  seam for the v0.2 PostgreSQL `accounts` backend (#023/#024); `AppState`
  currently pins the concrete `Arc<AccountRegistry>` (so `mint_for_account` stays
  an inherent method), and the v0.2 swap to `Arc<dyn AccountStore>` promotes
  `mint_for_account` to a trait default method — a localized change confined to
  `src/auth.rs`/`src/state.rs`, invisible to the gateways. `AppState` now owns
  the `AccountRegistry` and a
  **real** `AuthService<FixedClock>` (replacing `AuthPlaceholder`), pinned to the
  venue clock, with the registry (as `RevocationOracle`) as its oracle;
  `AppStateConfig` gains an optional `AuthConfig` (JWT key pair / `dev()`,
  bootstrap secret, pepper, provisioned accounts, rate-limit budget) and
  `AppState::new` is now fallible (`Result<Arc<Self>, AuthError>`) on the auth
  build. New dependencies: `argon2` 0.5 (pure-Rust RustCrypto Argon2id) and
  `rand_core` 0.6 with `getrandom` (already in the tree; only the salt CSPRNG
  feature is added).

- **JWT (RS256/x509) auth, the `Permission` implication, and the sliding-60 s
  `RateLimiter`** (#11) in `src/auth.rs` — the **one** authorization model across
  every surface ([ADR-0005](docs/adr/0005-jwt-auth-for-rest-ws.md),
  [03 §6, §6.1](docs/03-protocol-surfaces.md),
  [01 §8](docs/01-domain-model.md)); the legacy Backend `ApiKeyStore` /
  `sk_live_` path is **not** carried over (JWT is the only credential mechanism).
  `JwtAuth` signs RS256 tokens with an x509 key pair: `from_paths(cert, key)` /
  `from_pem` load the PEM pair with the **public key extracted from the
  certificate** (jsonwebtoken's `DecodingKey::from_rsa_pem` reads a `CERTIFICATE`
  PEM directly, so no separate x509 parser is pulled), `mint_token` /
  `verify_token`, and a clearly-labelled `dev()` fixture built from an **embedded,
  non-secret** dev keypair. `verify_token` pins **RS256** (rejecting `alg:none`
  and HS256 algorithm-confusion), enforces `exp`, and collapses every failure to a
  redacted `VenueError::Unauthorized` — the token and the cause are never logged
  or returned. `Claims` carries `sub` (the `AccountId`), the permission set, `iat`
  / `exp`, and the account `revocation_epoch`; `Claims::has_permission` applies the
  `Admin ⇒ Read + Trade` implication via the new `Permission::grants` (enforced in
  the auth layer, matched exhaustively — `Read ⊂ Trade ⊂ Admin`). The
  `auth_middleware` Axum layer resolves identity, enforces the **admission** rate
  limit, checks the revocation epoch, and gates the required `Permission`, rendering
  `401` / `403` / `429` through the #003 `VenueError` boundary; `GET /health` is
  fully exempt from **both** auth and rate limiting. The `RateLimiter` is a
  sliding-60 s window on the **injected venue clock** (`RateLimitClock`, bridged
  from the venue `FixedClock` — never `SystemTime`), keyed on `(account,
  revocation_epoch)` for an authenticated request (so a revoked-but-signed token
  cannot drain a fresh session's budget) with a peer-IP fallback, emitting
  `X-RateLimit-Limit` / `-Remaining` / `-Reset` (and `Retry-After` on `429`);
  decisions are replay-deterministic on the venue clock, with the `(session_id,
  arrival_sequence)` tie-break documented as the ingress-ordering seam. The
  limiter's key-space is bounded **by construction** by a `max_keys` ceiling
  (default `100_000`, a DoS control per [08 §5](docs/08-threat-model.md)): a
  would-be new key at capacity triggers an opportunistic inline sweep and, if
  still full, **fails closed** rather than grow — an attacker cycling source IPs
  cannot exhaust memory. Token
  issuance is gated by `BootstrapGate` (`AUTH_BOOTSTRAP_SECRET`, constant-time
  compare), and `JwtAuth::release_gated(DevMode)` refuses the embedded dev keys in
  a published image unless `--dev` / `FAUXCHANGE_DEV` is set (the image-scan test
  is #26). Secrets never leak: `JwtAuth` and `BootstrapGate` have redacting
  `Debug` impls. New dependency: `jsonwebtoken` 9 (`default-features = false,
  features = ["use_pem"]`), whose crypto backend `ring` 0.17 is already in the
  tree (no new crypto impl, no new major version); `tower` 0.5 (`util`, dev-only,
  already in-tree via axum) drives the middleware integration test. New tests:
  unit mint/verify (happy + tamper + expiry + `alg:none` + HS256 confusion),
  `has_permission` implication matrix, `/health` exemption, bootstrap-secret gate,
  dev-key release-gate refusal, revoked/unknown-account refusal, the rate-limiter
  sliding window + key independence + sweep + header rendering + venue-clock
  determinism, and secret-redaction assertions (`auth.rs`); the integration
  request flow through `auth_middleware` (missing token `401`, insufficient
  permission `403`, over-limit `429` with `X-RateLimit-*`, `/health` `200`), the
  `rate_limiter_window_bound` property (≤ N per 60 s window), and the
  replay-stable venue-clock decision test (`tests/auth.rs`). `src/auth.rs` only —
  the `AppState` wiring (registry account resolution) is #12.
- **`AppState`: the shared `Arc` wiring of the venue core and services** (#10) in
  `src/state.rs` — the application seam between the transport gateways and the
  domain ([02 §6, §8](docs/02-matching-architecture.md)). `AppState::new` takes an
  `AppStateConfig` (an explicit list of underlyings, since the config surface #22
  has not landed) and spawns **one single-writer actor per underlying**, wiring
  the real order path (`MatchingExecutor`) and post-journal fan-out (`StoreFanOut`,
  #8) into each — the order path and fan-out are live, not placeholder. A
  venue-wide `InstrumentRegistry` + `SymbolIndex` are created once and passed to
  every actor **by handle** (via the new
  `MatchingExecutor::new_with_registry_and_index` /
  `spawn_matching_actor_with_registry_and_index` in `src/exchange/executor.rs`,
  routing straight into the upstream `UnderlyingOrderBook::new_with_registry_and_index`
  verified public at the locked `option-chain-orderbook` 0.7.0), so cross-underlying
  lookups stay O(1) without coupling the writers (`BTC` and `ETH` sequence
  concurrently). The single shared executions / positions / mark stores are the
  **same `Arc` instances** each actor's `StoreFanOut` writes to and `AppState`
  exposes for reads. `AppState::submit` is the **only** path onto the sequenced
  order path: it routes a `VenueCommand` to the right underlying's actor (extracting
  the underlying via the upstream `SymbolParser` / the command's ticker) and awaits
  the `Receipt`, returning `VenueError::NotFound` for an unhosted underlying and
  `VenueError::InvalidOrder` for a venue-global command that carries no single
  routable underlying (broadcast/lifecycle routing lands with the control-plane
  issues). The auth / subscription-manager / market-maker / simulator services are
  stable-typed placeholders (`AuthPlaceholder` #11/#12, `SubscriptionsPlaceholder`
  #14, `MarketMakerPlaceholder` #15, `SimulatorPlaceholder` #16) that slot in
  without reshaping `AppState`. The shutdown path is dropping the last
  `Arc<AppState>`, which closes each bounded mailbox (draining its backlog) and ends
  the actor loop. Layering holds: `AppState` imports the domain, never
  `src/gateway/*`. Unit + integration (`tests/state.rs`) tests cover the spawned
  actor set, per-underlying routing, an unknown-underlying typed error, and an
  end-to-end submit whose crossing fill lands in the shared executions store
  `AppState` exposes.
- Book **snapshot + restore** over a consistent cut with a fresh journal epoch
  (#9) in `src/exchange/` (`snapshot.rs`, plus executor / stores / actor /
  journal wiring) — the operator escape hatch that is an **explicit replay
  exclusion**: it captures *state*, not the *sequence of decisions*, so a restore
  starts a new journal epoch rather than inject a book the journal never produced
  ([02 §9](docs/02-matching-architecture.md),
  [03 §10](docs/03-protocol-surfaces.md),
  [01 §6.1](docs/01-domain-model.md)). A `VenueSnapshot` is an atomic cut, as of
  one instant, of the **four** derived stores together — the leaf **books**
  (resting orders read back from the upstream book so a partially-filled maker
  carries its *current* quantity), the **executions** log, the **positions**
  fold, and the per-account **client-order-id idempotency map** — plus
  config/version `SnapshotMetadata`. Non-journaled analytics (mark price,
  unrealised P&L, Greeks, registry ids) are **excluded** and recompute live.
  `UnderlyingActor::capture` / `restore` are the entry points for the admin
  snapshot routes (#13) and the replay base-state hook (#30). A restore is
  **all-or-nothing**: metadata is validated first (a schema / lineage / config
  mismatch is refused with no mutation), the book rebuild is *prepared* (fallible,
  non-mutating) and the `SnapshotRestored` epoch marker journaled **before** any
  store is swapped, so a mid-restore fault rolls back all four; the commit is an
  infallible swap under actor quiescence (the "one PostgreSQL transaction" mode is
  the v0.3 durable seam). The marker opens a fresh epoch carrying the run
  `lineage_id` forward so id derivation continues in the same namespace, and the
  `underlying_sequence` **continues** from the last journaled value (it does
  **not** reset). Reproducibility is asserted **within** an epoch; the restore
  boundary is explicitly **out of scope** of the determinism oracle, not silently
  divergent.
- A `SnapshotRestored { snapshot_id, epoch, lineage_id }` epoch marker as a new
  `venue.v1` journal record (`JournalRecord::Epoch` / `RecordKind::Epoch`, #9),
  carrying the mandatory `schema` tag with the same `deny_unknown_fields`
  discipline and a committed golden; unlike a command/event pair it is **not**
  re-executable — recovery treats it as an epoch boundary.
- A per-account **client-order-id idempotency map** (`IdempotencyMap`, #9) owned
  by the single-writer executor and captured/restored as the fourth store of the
  cut ([01 §6.1](docs/01-domain-model.md)): a retry with a matching payload
  fingerprint returns the **stored terminal result** (no second order), and a
  conflicting reuse of the same key is rejected. It is a deterministic function of
  the journal, so a duplicate `ClOrdID`/client-id retried **after** a restore
  returns the stored result. (The full pre-journal dedup, cancel/replace
  `OrigClOrdID` correlation, and retention-window eviction are completed by the
  later FIX/idempotency issue.)
- In-memory executions + positions stores + the backend-agnostic store contract
  (#8) in `src/exchange/` (`stores.rs`) — the authoritative fill log and the
  per-`(account, symbol)` position fold, both derived from committed `VenueEvent`
  fills through the actor's post-journal `FanOut` seam #6 left open
  ([01 §7](docs/01-domain-model.md),
  [02 §6](docs/02-matching-architecture.md)). Adds `StoreFanOut`, the #8
  replacement for `NoopFanOut`: it runs **only after** a `VenueEvent` is
  journaled (step 5), projecting each committed fill **leg** into an
  authoritative `ExecutionRecord` and folding it into a `Position`, so the
  executions log stays a **deterministic function of the journal** (same journal
  → same executions). Both legs of one match are recorded (shared `execution_id`,
  distinct account / side / liquidity / fee — a maker rebate is negative), keyed
  `(execution_id, liquidity)` so the key stays unique even for a same-account
  self-trade. Positions fold with **exact integer-cents** accounting: `i128`
  checked accumulators (`checked_*`, never `saturating_*` / `wrapping_*`) give a
  signed `net_quantity`, a volume-weighted `avg_price`, and a `realized_pnl`, with
  the realized/unrealized split computed from one exact cost basis so
  `realized + unrealized == net_cash − fees + net_quantity × mark` holds
  **exactly** as an arithmetic identity — distinct from the ADR-0006 bounded
  replay oracle, and it even folds in the live mark (the truncated `avg_price`
  is never used in the P&L).
  `unrealized_pnl` is marked at **read time** against a `MarkSource` — the
  production `MarkPriceBook` wraps the upstream
  `option_chain_orderbook::MarkPriceCalculator` (verified present in the locked
  0.7.0) — and is a **live-only** projection: not journaled, not asserted across
  replays ([02 §5.5](docs/02-matching-architecture.md)); the read API takes the
  mark as an explicit argument to keep that boundary visible, and `delta_exposure`
  is `0.0` (Greeks not wired yet). The key deliverable is the backend-agnostic
  `ExecutionsStore` / `PositionsStore` **contract**: the in-memory
  `InMemoryExecutionsStore` / `InMemoryPositionsStore` here and the durable
  PostgreSQL stores (#23) implement the **same** traits, so the REST reads (#13)
  don't change when the backend swaps (the in-memory insertion order is a
  surrogate for the durable `SERIAL` id an SQL store would `ORDER BY`). The
  projected `ExecutionRecord` is the #4 wire DTO unchanged; without a pricer /
  latency injector wired in #8, `theo_value_cents` defaults to the fill price
  (so `edge_cents` is `0`) and `latency_us` is `0` — both documented live-only
  analytics per [01 §7](docs/01-domain-model.md) that later issues supply without
  a wire-shape change. New dependency: `dashmap` 6 (over `Arc<RwLock<HashMap<>>>`
  per `rules/global_rules.md` Concurrency), already resolved transitively via the
  upstream matching stack — no new tree version. New tests: unit executions
  both-leg insertion + account-scoped ordered listing, positions fold (signed
  net, volume-weighted avg, partial close, flip, both counterparties) and the
  upstream mark-book wiring (`stores.rs`); the
  `position_pnl_stays_consistent_across_fills` property (`tests/property.rs`); the
  `rest/execution_report.json` + `rest/positions.json` per-leg goldens
  (`tests/golden.rs`); and the orders → matching → stores integration, the
  store-projection-vs-golden assertions, and the executions-log determinism test
  through the public actor surface (`tests/stores.rs`).
- Real order path onto upstream matching (#7) in `src/exchange/`
  (`executor.rs`) — the `CommandExecutor` seam #6 left open, now driving the
  upstream `option-chain-orderbook` matching **unchanged** and capturing the
  lossless `VenueOutcome`
  ([02 §4–§5](docs/02-matching-architecture.md),
  [ADR-0009](docs/adr/0009-lossless-venue-envelope-outcomes.md)). Adds
  `MatchingExecutor`, which owns one per-underlying `UnderlyingOrderBook` and,
  per command, **vivifies the target leaf** through the hierarchy's idempotent
  `get_or_create_*` path (the same pure-function-of-the-symbol resolution the
  upstream `SequencedUnderlyingOrderBook` uses, so replay rebuilds identical
  structural state), drives the **account-preserving** `_full` leaf
  (`add_limit_order_with_tif_and_user_full` → `TradeResult`) for a limit
  `AddOrder`, and the **true non-resting market primitive**
  (`orderbook_rs::OrderBook::submit_market_order_with_user` via
  `OptionOrderBook::inner()`) for a market order — never a marketable-limit
  substitute, with an empty-book fast path that returns zero-fill / fully
  unfilled rather than an invented price. Captures every match as **two linked
  legs** (maker + taker sharing one `execution_id`, per-leg account / owner /
  fee) with the resting maker's identity recovered from the **journaled add
  command** via a deterministic registry, not live book state
  ([ADR-0009 §2](docs/adr/0009-lossless-venue-envelope-outcomes.md)). Captures
  fills on **both** paths: on `Ok` from the returned `TradeResult`, and on the
  **error-after-fills** `Err` path (an unfillable `Ioc` remainder, or an STP
  cancel after earlier fills) via a single-writer-safe **before/after diff** of
  the leaf's armed `last_trade_result()` capture slot (keyed on the strictly
  monotonic `engine_seq`; upstream Option-Chain-OrderBook#148: last-write-wins,
  no `take`/`clear`) — so a command that executed fills is **never** a bare
  `Rejected` ([ADR-0009 §1](docs/adr/0009-lossless-venue-envelope-outcomes.md)).
  Implements `CancelOrder` and the **non-atomic** `Replace` (cancel-then-add in
  one turn, one `VenueEvent` at one sequence, explicit `Replace { cancelled,
  add }` — no rollback if the add is rejected), and records STP-cancelled
  same-owner resting makers (`stp_cancelled`, sorted for a deterministic sweep
  order) recovered by an owner-scoped resting diff. Execution consults **no**
  wall-clock, RNG, or map-iteration order: the engine order id is assigned
  deterministically as `OrderId::sequential(underlying_sequence)` (the engine
  never RNG-mints a `Uuid` on the path), and the engine's process-local trade
  ids / wall-clock trade timestamps are excluded from the oracle
  ([02 §5](docs/02-matching-architecture.md)). Adds the `TopOfBook` read
  projection (the determinism oracle's read surface) and the ergonomic
  `spawn_matching_actor` wiring the real executor into the #6 actor (the
  `PlaceholderExecutor` stays for tests). Upstream methods verified against the
  **locked** `option-chain-orderbook` 0.7.0 (with transitive `orderbook-rs`
  0.10.5 / `pricelevel` 0.8.4); no new dependencies. New tests: unit
  add/cancel/replace/market happy paths + rejections, error-after-fill diff
  capture, empty-book + thin-book market, partial-replace (`cancelled: true`,
  add `Rejected`), STP affected-id recording, and per-leg fee capture
  (`executor.rs`); the `journal_replay_reconstructs_book` property
  (`tests/property.rs`); `market` / partial-`replace` outcome goldens extending
  the `venue.v1` set (`tests/golden.rs`); the seed → orders → matching →
  captured-fills integration round-trip and the **binding** determinism test
  (same journal → same fills + top-of-book) driven through the public actor /
  executor surface (`tests/order_path.rs`).
- Per-underlying single-writer actor + in-memory write-ahead envelope journal
  (#6) in `src/exchange/` (`actor.rs`, `journal.rs`) — the determinism
  foundation every book mutation flows through
  ([ADR-0006 §2–§3](docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
  [02 §4–§6, §8](docs/02-matching-architecture.md)). Adds the
  `UnderlyingActor` (one `tokio` task per underlying, the sole caller of the
  order path) with a **bounded** `mpsc` mailbox + `oneshot` receipts
  (`ActorHandle::submit` → `Receipt`; a full mailbox returns a typed
  `RateLimited` busy, never an unbounded queue), and its venue-owned
  `underlying_sequence` as a **`u64` checked counter** (advanced with
  `checked_add` per committed command — the upstream `OptionChainSequencer` is
  `pub(crate)`, so the venue owns numbering). Implements the write-ahead
  durability protocol per turn: append the `VenueCommand` envelope **before**
  executing (`N` advances only on a confirmed append; a confirmed pre-execution
  failure **reuses `N`** with the book untouched and no gap; an ambiguous result
  is resolved by an idempotent durable **tail read-back**), then execute +
  capture (the `CommandExecutor` seam, filled by #7), append the paired
  `VenueEvent`, and fan out (the `FanOut` seam, filled by #8) **only after** the
  event is journaled. A **post-mutation** event-append failure **seals** the
  underlying (no fan-out); a sequence **exhaustion** at `u64::MAX` seals with
  `SequenceExhausted`, never wraps. Adds `InMemoryVenueJournal` behind the
  `VenueJournal` trait (named to match the upstream `OptionChainJournal` shape —
  `append` / `read_from` / `last_sequence` — so the durable store swaps in at
  #29), the paired `JournalRecord` (`Command` / `Event`, keyed
  `(underlying, N, kind)` with idempotent re-append), and the deterministic
  `FixedClock` / `PlaceholderExecutor` / `NoopFanOut` #006 seam stubs. Extends
  the boundary `VenueError` with `SequenceExhausted` and `JournalUnavailable`
  (both redacted `500`/`internal`, non-retryable, non-terminal) and adds
  `JournalError` (`AppendFailed` / `Ambiguous` / `Conflict` / `Corruption`, the
  fixed durable/recovery contract). Enables the upstream
  `option-chain-orderbook` `sequencer` feature (activates upstream `tokio` +
  `orderbook-rs/journal`, pulling `memmap2` 0.9 into the tree — `crc32fast` was
  already present) to make the sequencer / mass-cancel types reachable for #7.
  The `memmap2` machinery is unused by #6 (which ships its own in-memory
  journal) and is flagged for the `cargo audit` / `cargo deny` gate (#19). New
  dependencies: `tokio` (`rt` + `sync` + `macros`; `rt-multi-thread` dev-only)
  for the actor runtime, and `tracing` for the actor's lifecycle / degraded-path
  logging — both already resolved transitively, adding no new tree version. New
  tests: unit single-writer ordering under concurrent submits, `checked_add`
  monotonicity, `SequenceExhausted` at `u64::MAX`, reuse-`N` + tail-read-back
  idempotency, and mailbox backpressure → busy (`actor.rs`); journal
  append/read/dedup/conflict units (`journal.rs`); `SequenceExhausted` /
  `JournalUnavailable` redaction units (`error.rs`); the
  `sequence_monotonic_per_symbol` property (`tests/property.rs`); and the
  integration actor round-trip + determinism fault-injection rows
  (pre-execution append fail → book untouched, reuse `N`; post-mutation append
  fail → seal, no fan-out) through the public seam surface (`tests/actor.rs`).
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
