# Roadmap — `fauxchange`

| Field      | Value                                       |
|------------|---------------------------------------------|
| Status     | Living                                      |
| Last edit  | 2026-07-12                                  |

This is a living document. Each phase has clear acceptance criteria and a
tight scope. New work that does not fit a phase goes to the wishlist or
the anti-roadmap; it never sneaks into an in-flight version. The version
numbering here matches the scope sections in [PRD.md §6](PRD.md#6-scope).

## Where we are

`fauxchange` is at **v0.0.1** — a crates.io name-reservation placeholder
published 2026-07-12. No implementation code exists yet; `src/` is a
stub. The numbered design docs under `docs/` are the source of truth for
this phase, the per-issue specs live under `milestones/` (one directory
per version), and this roadmap orders the work that turns them into code,
starting at v0.1.0.

> **Status 2026-07-12:** Design phase — **issues are filed and live.** All
> **55** issues for this repo are open on GitHub
> (`joaquinbejar/fauxchange`), numbered `#1`–`#55` with the GitHub number
> equal to the local 3-digit milestone id (verified zero drift), grouped
> under milestones `v0.1`–`v1.0` — part of the **167** issues filed across
> the three repos (`IronCondor` / `ChainView` / `fauxchange`). What does
> **not** yet exist is code: `src/` is still a stub and no phase below has
> started. The `docs/` set (00 bootstrap, 01–06 sub-domains,
> ADR-0001…0009, specs) plus the `milestones/` per-issue specs are the
> source of truth until code lands.

Workflow rules for the build-out: one PR at a time (sequential), each
issue closed via `Closes #N` in the PR description, full pre-submission
checklist per PR ([TESTING.md §12](TESTING.md#12-pre-submission-checklist-binding)).
Every change on the sequenced order path ships with a determinism test;
every gateway change ships with the parity test its surface can express —
**order-entry parity across the order-entry surfaces present at that
milestone** (REST only until FIX lands in v0.4, then REST + FIX; never WS,
which has no order-entry message) and **observation/control parity across
all present surfaces** ([03-protocol-surfaces.md §7](03-protocol-surfaces.md#7-protocol-parity-guarantees)).

## v0.1 — Consolidate the Backend core

**Goal.** A single `fauxchange` binary that serves the documented
Option-Chain-OrderBook-Backend REST + WebSocket surface, matches orders
through the upstream engine behind the sequencer, and authenticates with
one permission model. Parity with the Backend's documented surface is the
acceptance test — no new protocol behavior yet.

The in-memory envelope journal, receipts, and single-writer ordering exist
from the first commit — durable persistence swaps only the store in v0.3
(#29), so determinism is never bolted on later. Auth is consolidated onto
one JWT / `Permission` model: the legacy `ApiKeyStore` is dropped and a
new venue **account registry** (credentials, `owner`, revocation epoch) is
introduced beyond the Backend carry-over, and bootstrap minting selects a
named account rather than a fresh subject.

### Issues

- [ ] #1 — Scaffold crate skeleton, module tree, and lib.rs stubs (S; no deps)
- [ ] #2 — Domain boundary newtypes, integer-cents money, and symbol grammar (M; depends on #1)
- [ ] #3 — Typed error boundary with HTTP-status and FIX-reject mapping seam (M; depends on #1)
- [ ] #4 — REST/WS DTO layer in models.rs (serde + ToSchema, prices in cents) (M; depends on #2, #3)
- [ ] #5 — Versioned VenueCommand/VenueEvent envelope types and outcomes (M; depends on #2)
- [ ] #6 — Per-underlying single-writer actor with in-memory envelope journal and checked sequence (L; depends on #5)
- [ ] #7 — Route add/cancel/replace onto option-chain-orderbook matching and capture MatchResult (L; depends on #6)
- [ ] #8 — In-memory executions and positions stores (M; depends on #7)
- [ ] #9 — Book snapshot and restore for the sequenced state (M; depends on #7)
- [ ] #10 — AppState: shared Arc wiring of core and services (M; depends on #7, #8)
- [ ] #11 — JWT RS256 auth, Permission model, and sliding-60s rate limiter (L; depends on #3)
- [ ] #12 — Venue account registry with credentials, revocation epoch, and bootstrap minting (M; depends on #11)
- [ ] #13 — Carry over the ~50 REST routes with utoipa OpenAPI and Swagger UI (L; depends on #4, #10, #11)
- [ ] #14 — WebSocket surface: WsMessage protocol, channels, and subscription manager (L; depends on #4, #10, #11)
- [ ] #15 — Market-maker engine, OptionPricer, and Quoter on the sequenced path (L; depends on #7, #10)
- [ ] #16 — PriceSimulator walks feeding the sequencer (M; depends on #7, #10, #15)
- [ ] #17 — Determinism test harness — same journal produces identical fills and top-of-book (M; depends on #7, #8)
- [ ] #18 — Parity fixtures — REST/WS observation parity and REST order-entry parity (M; depends on #13, #14)
- [ ] #19 — CI: cargo audit + cargo deny + fmt/clippy/test gates (M; depends on #1)
- [ ] #20 — bench-hdr harness for the order path and WS fan-out with first BENCH.md baseline (M; depends on #7, #14)
- [ ] #21 — Threat-model coverage: untrusted-input hardening and captured-log credential test (M; depends on #11, #13, #14)

Full per-issue specs: `milestones/v0.1-backend-core/` (local).

**Acceptance.**
- Every documented Backend REST route and WS message is reachable with
  the same shape (parity fixtures green).
- A recorded order sequence, replayed, produces identical fills and
  top-of-book (determinism test — the seed of the v0.3 work).
- No `f64` money on any boundary; `#![forbid(unsafe_code)]` holds.
- **Security/perf gate:** `cargo audit` + `cargo deny` are green in CI
  (wired from this first milestone); the threat model
  ([08-threat-model.md](08-threat-model.md)) covers every surface shipped so
  far, and every untrusted input has its validation + resource ceiling +
  typed error ([Security & performance gates](#security--performance-gates-per-milestone)).

## v0.2 — One-command packaging

**Goal.** `docker compose up` yields a serving, seeded venue. The venue
becomes a dependency you can stand up next to the system under test.

The compose topology brings up `fauxchange` plus an optional
`postgres:18-alpine`, and one image serves both the DB-less (in-memory)
and persistent paths; `deny_unknown_fields` on the config surface makes a
config typo a startup error, not silence.

### Issues

- [ ] #22 — Layered config surface (file + env + CLI) with deny_unknown_fields (M; depends on #10)
- [ ] #23 — Optional sqlx PgPool layer with migrations and executions persistence (M; depends on #8, #22)
- [ ] #24 — Scenario seed format populating underlyings, expirations, strikes, and opening prices (M; depends on #22, #12)
- [ ] #25 — Multi-stage Dockerfile and docker-compose topology (M; depends on #19, #22, #23)
- [ ] #26 — Container hardening — non-root, distroless option, no secrets, loopback metrics (M; depends on #25)
- [ ] #27 — Cold-bring-up budget tracking and Docker e2e smoke test (S; depends on #25, #24)

Full per-issue specs: `milestones/v0.2-packaging/` (local).

**Acceptance.**
- `docker compose up` reaches a serving venue with a seeded chain in
  < 30 s cold.
- DB-less mode (in-memory, `DATABASE_URL` unset) and the persistent path
  both work from the same image.
- An unknown config key aborts startup with a clear error.

## v0.3 — Historical replay + OptionChain-Simulator integration

**Goal.** A run can be recorded and replayed deterministically, and
synthetic chains can advance step-by-step. This is the phase that makes
the whole venue reproducible — and it lands before FIX on purpose, so FIX
conformance can be tested without flaky sleeps.

The durable store (#29) backs the *same* `VenueEvent` envelope journal the
actor already writes in-memory in v0.1 (#6) — the receipt, recovery, and
durability contract is unchanged, only the store is swapped (the Backend
persisted executions only). The clock becomes a venue service rather than
`SystemTime`, and every wall-clock-relative `ExpirationDate::Days` is
removed because relative expiries break replay stability.

### Issues

- [ ] #28 — Clock as a venue service (realtime, accelerated, stepped; seeded) (M; depends on #6)
- [ ] #29 — Persist the venue envelope journal durably (swap store, same contract) (L; depends on #6, #23, #28)
- [ ] #30 — Replay driver reproducing identical order events, fills, and top-of-book (L; depends on #28, #29)
- [ ] #31 — Stepped synthetic sessions absorbing the OptionChain-Simulator model (L; depends on #16, #28)
- [ ] #32 — Replace ExpirationDate::Days with ExpirationDate::DateTime everywhere (M; depends on #7)
- [ ] #33 — Document the determinism guarantee and exclusions with a test-oracle (M; depends on #30, #32)
- [ ] #34 — Adversarial fixtures for the journal/replay deserialiser (M; depends on #29)
- [ ] #35 — Persistent-mode order-path budget and WS fan-out p99 flatness (M; depends on #20, #29)

Full per-issue specs: `milestones/v0.3-replay/` (local).

**Acceptance.**
- Record a session, replay it → identical order events, fills, and
  top-of-book across runs.
- The determinism guarantee and its documented exclusions (mark prices
  not journaled; out-of-sequencer state not reproduced) are written down
  and covered by a test-oracle.
- A stepped session advances identically for the same seed.

## v0.4 — FIX 4.4 gateway

**Goal.** A FIX 4.4 acceptor that an order router can conformance-test
against, producing executions identical to the REST/WS path. IronFix
supplies framing, the type vocabulary, and a session FSM skeleton; the
acceptor, typed messages, resend logic, and validation are new work
([ADR-0002](adr/0002-fix-4-4-gateway-on-ironfix.md),
[specs/ironfix.md](specs/ironfix.md)).

IronFix ships no acceptor and only a `MemoryStore`, so the accept loop and
a durable resend / gap-fill sequence store are new work here. Rejects are
**context-sensitive** per surface (`ExecutionReport (8) Rejected` for a bad
new order, `OrderCancelReject (9)` for a bad cancel/replace,
`MarketDataRequestReject (Y)` for MD, `BusinessMessageReject (j)`
otherwise; bare `Reject (3)` is session-level only), and the **control
plane is deliberately not exposed over FIX** — controls stay REST/WS
([03 §7](03-protocol-surfaces.md#7-protocol-parity-guarantees),
[03 §8](03-protocol-surfaces.md#8-error-mapping-across-surfaces)).

### Issues

- [ ] #36 — FIX 4.4 typed messages and pinned dialect (types, requiredness, decimal-Price seam) (M; depends on #4)
- [ ] #37 — FIX TCP acceptor and FixCodec accept loop (L; depends on #11, #36)
- [ ] #38 — FIX session layer: logon auth, heartbeat, resend/gap-fill, durable sequence store (L; depends on #37, #12, #29)
- [ ] #39 — FIX typed order flow onto the sequenced path with context-sensitive rejects (L; depends on #38, #7, #3)
- [ ] #40 — FIX market data: MarketDataRequest to Snapshot/Incremental over WS book semantics (M; depends on #39, #14)
- [ ] #41 — FIX-REST order-entry parity and REST/WS/FIX observation parity suite (M; depends on #39, #40, #18)
- [ ] #42 — FIX tag-value parser fuzz target, auth-mapping audit, no credential echo (M; depends on #37, #38)
- [ ] #43 — FIX parse/encode hot-path budget in BENCH.md (M; depends on #39, #20)

Full per-issue specs: `milestones/v0.4-fix-gateway/` (local).

**Acceptance.**
- An order over FIX yields identical book state and fills as the same
  order over REST (parity suite).
- Logon maps to `Read` / `Trade` / `Admin` exactly as REST/WS bearer
  tokens do — no second auth system.
- A captured conformance script (session admin + order + MD) passes.

## v0.5 — Microstructure config

**Goal.** The venue's personality is configuration, not code. A scenario
file can provoke the failure modes real venues only exhibit under stress.

Fees, STP, contract specs, latency, rate limits, and market-maker personas
become declarative config surfaced from the upstream types, not forks of
matching. The only partial-fill knob is the market maker's **resting size**
(`size_scalar` / `base_size`): partial fills then fall out of real matching
against that finite liquidity, not a synthetic fill-probability policy —
the latter would require upstream matching support and is out of scope
([05-microstructure-config.md §9](05-microstructure-config.md#9-partial-fill-and-liquidity-shaping)).

### Issues

- [ ] #44 — Fee schedule, STP mode, and contract specs as declarative config (M; depends on #22, #8)
- [ ] #45 — Seeded latency injection (fixed/uniform/normal/lognormal) (M; depends on #22, #28)
- [ ] #46 — Rate-limit config and per-instrument microstructure profiles (M; depends on #22, #11)
- [ ] #47 — Market-maker personas, resting-liquidity shaping, and halt scenarios (L; depends on #15, #22)
- [ ] #48 — DoS controls tested as security controls (rate-limit, bounded mailbox, connection cap) (M; depends on #46, #6)
- [ ] #49 — Scenario reproduces a documented failure mode deterministically; out-of-range knobs rejected (M; depends on #47, #44, #45, #46)
- [ ] #50 — Market-maker requote budget and requote isolation assertion (M; depends on #47, #20)

Full per-issue specs: `milestones/v0.5-microstructure/` (local).

**Acceptance.**
- A scenario config reproduces a documented failure mode (throttling,
  halt, wide-spread starvation) deterministically for a fixed seed.
- Out-of-range knob values are rejected at startup with a typed error.

## v1.0 — Stability + conformance suite

**Goal.** Freeze the surfaces and prove them. Promote to 1.0 once each
surface has shipped without a breaking change for one quarter.

Freeze and prove the surfaces — a packaged conformance harness plus
cross-surface parity, the final fuzz corpus and `SECURITY.md`, an armed CI
performance-regression gate, and a stability soak — then the one-off v1.0
acceptance checklist and release cut.

### Issues

- [ ] #51 — Packaged `fauxchange conformance` harness across REST, WS, and FIX (L; depends on #41, #18)
- [ ] #52 — REST/WS JSON decoder fuzz targets, full adversarial corpus, and final SECURITY.md (M; depends on #42, #34)
- [ ] #53 — Arm the CI performance regression gate (M; depends on #35, #43, #50)
- [ ] #54 — Stability soak: flat memory, no sequence gaps, clean shutdown, restart-from-journal (L; depends on #51, #30)
- [ ] #55 — v1.0 acceptance checklist and release cut (M; depends on #51, #52, #53, #54)

Full per-issue specs: `milestones/v1.0-stability/` (local).

**Acceptance.**
- Conformance + parity suites green across all three surfaces.
- Soak: flat memory over the soak window; determinism holds after a
  restart-from-journal.
- v1.0 criteria (below) all green.

## v1.0 — Stability commitment

Promote to 1.0 when each surface has shipped without breaking changes for
one quarter:

- REST: endpoints + DTOs + OpenAPI SemVer-stable across 1 quarter.
- WebSocket: `WsMessage` shapes + channel + sequence semantics stable.
- FIX 4.4: the supported message set + session semantics stable.
- Determinism: journal format + replay contract stable; a journal
  recorded at v1.0 replays on any v1.x.
- Config: file schema + env vars stable (additive only).
- Packaging: `docker compose` service names, ports, and volume contract
  stable.

## Security & performance gates (per milestone)

Production-readiness is gated, not assumed ([ADR-0008](adr/0008-production-grade-performance-and-security.md)).
Each gate lands *with* its milestone, so no version claims a property it has
not wired a check for. Every performance number stays a **DESIGN TARGET**
until `BENCH.md` measures it — the no-fabricated-benchmarks rule is absolute.

| Milestone | Security gate | Performance gate |
|-----------|---------------|------------------|
| **v0.1** | `cargo audit` + `cargo deny` green in CI; threat model covers REST/WS + auth; every untrusted input has validation + ceiling + typed error; captured-log credential test green ([08](08-threat-model.md)) | `bench-hdr` harness stood up for the order path + WS fan-out; first `BENCH.md` baseline (in-memory journal) — targets labelled as targets ([07](07-performance-budgets.md)) |
| **v0.2** | Container hardening: non-root, minimal/distroless base option, no secrets in the image, loopback-only metrics; supply-chain gates run against the image build ([08 §9](08-threat-model.md#9-container-hardening-deployment)) | Cold-bring-up budget (`docker compose up` < 30 s) tracked; durable-journal append budgeted **separately** from the in-memory order path ([07 §3](07-performance-budgets.md#3-latency-budgets-design-targets)) |
| **v0.3** | Adversarial fixtures for the journal/replay deserialiser (hostile bundle → typed reject, no panic) | Persistent-mode order-path budget measured (`in-memory HP-1 + one durable append`); WS fan-out p99 asserted flat in subscriber count N |
| **v0.4** | **FIX tag-value parser fuzz target** (`cargo fuzz`); FIX-logon auth mapping and sequence handling audited by `api-security-auditor`; no credential echo in `Text (58)` | FIX parse/encode hot-path budget added to `BENCH.md` once the wire dialect is pinned |
| **v0.5** | Rate-limit / bounded-mailbox / connection-cap DoS controls tested as security controls, not just fairness ([08 §5](08-threat-model.md#5-denial-of-service-posture)) | Market-maker requote budget added; requote isolation asserted (a slow requote never inflates a client order's latency) |
| **v1.0** | REST/WS JSON decoders added to the fuzz set; full adversarial corpus green; supported-versions + disclosure policy final in [`SECURITY.md`](../SECURITY.md) | **CI regression gate armed:** a budget-breaching p99/p99.9 regression fails the build; soak asserts flat memory + steady-state allocation discipline |

**Standing rules across all milestones:** no performance *claim* ships
without `bench-hdr` numbers in `BENCH.md`; a dependency addition ships with an
audit note ([CLAUDE.md](../CLAUDE.md) Key Decisions); `#![forbid(unsafe_code)]`
holds; the `api-security-auditor` agent reviews every gateway/auth/db/migration
change before merge.

## Dependency notes

- **#6 (the single-writer actor + envelope journal) gates everything.**
  Determinism and journaling are foundational; retrofitting them later
  would rewrite the order path.
- **FIX (v0.4) depends on replay (v0.3).** Replay makes FIX conformance
  deterministic — the only way to test a session layer without flaky
  sleeps. FIX is scheduled after replay for exactly this reason.
- **#32 (`ExpirationDate::DateTime`) blocks the determinism guarantee.**
  Wall-clock-relative expiries make a replay non-reproducible; land it
  inside the v0.3 window, not after.
- **Microstructure (v0.5) builds on the config surface (#22).** The
  scenario file format is introduced in v0.2 and extended, not replaced.
- Benchmark obligations (latency-injection fidelity, matching throughput)
  follow the `bench-hdr` policy — no perf claim without p50/p99/p99.9
  numbers in `BENCH.md`.

## Anti-roadmap

What `fauxchange` explicitly will **not** become:

- A real venue — no real money, settlement, custody, or external
  liquidity.
- A trading system, OMS, or EMS. The consumer is the trading system.
- A backtester — that is
  [IronCondor](https://github.com/joaquinbejar/IronCondor).
- A trader-facing UI — that is
  [ChainView](https://github.com/joaquinbejar/ChainView).
- A market-data vendor or redistributable feed.
- A multi-tenant hosted SaaS.
- A reimplementation of matching — the engine stays upstream in
  `orderbook-rs` / `option-chain-orderbook`.
- A separate heavy data stack — the OptionChain-Simulator's
  ClickHouse / Redis / Mongo topology is absorbed as *ideas*, not
  carried in as dependencies.

## Wishlist

Ideas worth tracking but not scheduled:

- Adversarial / reinforcement-learning scenario generation (deferred, as
  in IronCondor's later phase).
- A gRPC gateway alongside REST / WS / FIX.
- Wider FIX coverage: mass quote (`i`), RFQ (`AH` / `AI`), security
  list / definition / status.
- Fault injection beyond latency: dropped messages, partial network
  partitions, session flaps.
- Multi-node topology — several `fauxchange` instances as distinct
  venues for cross-venue routing tests.
- Bundled Prometheus + Grafana dashboards in the compose file.
- mTLS as an opt-in transport layer over the JWT auth model.
- Non-options asset classes behind a feature flag (see [PRD.md](PRD.md)
  Q-8).

## Changelog

`implement-roadmap` Step 7 appends one row here after each merge (real
environment date, the closed issue, the merged PR, a one-line summary).
Ticking the issue's checkbox in its `### Issues` list above is the paired
edit. The table starts empty — no issue has merged yet (code does not exist).

| Date | Issue | PR | Summary |
|------|-------|----|---------|
| —    | —     | —  | _(empty — the first merged PR replaces this seed row)_ |
