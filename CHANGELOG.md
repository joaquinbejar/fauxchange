# Changelog

All notable changes to `fauxchange` are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The full versioning and release-process policy lives in the design docs
(local until v0.1.0).

## [Unreleased]

### Added

- **REST pre-submit idempotency dedup — aligns the `underlying_sequence`
  progression with FIX on retries** (#103). The #41 parity suite surfaced a
  cross-surface asymmetry: a REST idempotent retry **consumed** an
  `underlying_sequence` (the executor deduped AFTER the write-ahead journal, so the
  retry was journaled as an idempotent `VenueOutcome::Duplicate` — one sequence
  spent), while a FIX retry did not (the gateway deduped BEFORE submit ⇒ no
  sequence consumed). Economic parity held, but a duplicate interleaved with new
  same-underlying orders shifted the subsequent `underlying_sequence` differently
  per surface. REST `place_limit_order` / `place_market_order` now consult a
  **pre-submit** dedup on `(account, client_order_id)` **before** minting an id or
  submitting — resolving the venue-shared `(account, ClOrdID) → order` index (#98)
  via `AppState::resolve_client_order_id`. A byte-identical retry (matching
  `symbol`/`side`/`quantity`) returns the STORED terminal report — the FULL canonical
  identity (the original `order_id` **and** its committed `underlying_sequence`, both
  carried on the index `ClOrdIdRecord`) plus its committed fills re-read from the
  executions store — with **no** submit and **no** sequence consumed, exactly as the
  FIX gateway does (#39). Because the sequence is stored on the record (populated by
  `apply_committed_correlation` from the committed event, so live and recovery derive
  it identically), even a purely-**resting** (zero-fill) retry echoes the original
  sequence — never a fabricated `0` — matching the post-submit
  `VenueOutcome::Duplicate` (#142) `rendered_identity`. A placement with no
  `client_order_id` is never dedupable.
  - **Race safety net (#142).** The correlation is published POST-journal, so a
    *sequential* retry always observes it; two *concurrent* retries can both miss it
    and submit, where the actor's own idempotency map dedups the loser to
    `VenueOutcome::Duplicate` and the post-submit path renders the canonical identity
    (never a never-added id) with no re-fan-out. The race is correct — it just
    consumes a sequence in that rare window. This is a fast path in front of the
    journaled guard, not a replacement.
  - **Conflicting reuse.** The index holds only `order_id`/`symbol`/`side`/
    `quantity` (no price/TIF), so a same-key reuse with a differing
    `symbol`/`side`/`quantity` is NOT a fast-path hit — it submits and the actor's
    full-fingerprint idempotency map rejects it (`client_order_id was reused`),
    rendered as the observed `rejected` outcome; a price/TIF-only reuse under a
    matching `symbol`/`side`/`quantity` is treated as an idempotent replay (a
    documented, deliberately-minimal-index limitation).
  - **Scope.** Pre-submit dedup covers `Added` / `Market` placements (the ones
    published into the index). A sequenced-**`Rejected`** first placement's retry is
    not indexed, so it falls through and the actor's `Duplicate` backstop reports the
    stored terminal (consuming a backstop-guarded sequence); economic parity still
    holds. Determinism holds: the index is a deterministic function of the journal
    (rebuilt from the journaled `AddOrder` stream on recovery, a deduped retry never
    journaled) and account-scoped (a cross-account `client_order_id` collision is
    never a hit). Parity test: A/B/retry-A/C over REST and FIX both journal `[0,1,2]`.
- **Surfaced the sequenced `VenueOutcome` to callers across REST/WS/FIX** (#118).
  `Receipt` now carries the observed outcome + a `FanoutSummary`, so a gateway
  renders the OBSERVED result — an order into a halted/`Settling`/`Expired`
  instrument reports `Rejected` (not a false `Accepted`/`New`), an illegal
  instrument-status toggle returns `409`, and a partial venue-global control
  fan-out reports `ok_count`/`total`/`fully_applied` — never the requested state.
- **Rendered the stored terminal report on an idempotent resend** (#99) — a
  resend projects the original fills from the receipt's captured `VenueOutcome`
  (`taker_fill_legs`) instead of a store read-back keyed on the freshly-minted
  order id, so both REST and FIX return the true stored terminal report.
- **A sequenced market-maker kill now cancels standing quotes** (#117) — the
  application-layer control path emits a separate journaled owner-scoped
  `MassCancel{ByUser(MARKET_MAKER_OWNER)}` (replay-safe, emitted once live and
  reproduced on replay) so a kill matches the live `set_enabled(false)` semantics.
- **Wired the explicit venue-wide mass-cancel** (#97) — REST cancel-all + FIX
  `OrderMassCancelRequest(q)` route an owner-scoped `MassCancel` through the
  sequenced path and render the real result (`r` accepted + one
  `ExecutionReport(8) Canceled` per swept order); an account can cancel only its
  own orders (owner-scoped).
- **Boot-time journal recovery — a durable venue resumes on restart** (#85).
  `AppState::new` now, when `DATABASE_URL` is set, reads each underlying's
  durable journal header and — for a non-empty stream — runs the production
  recovery reducer (`exchange::recover_into`, re-executing the journaled
  records with the stored event as the integrity oracle, the SAME single
  algorithm as offline replay, NEVER through `AppState::submit`), rebuilds the
  book + executions + positions + mark state, rehydrates the venue run
  `lineage_id`, and spawns each actor CONTINUING its stream at
  `last_sequence + 1` (via the new `ActorConfig::with_start_sequence`) instead
  of colliding at `0`. Before this, restarting against a non-empty journal hit
  the unique key with a `Conflict` on the first post-restart command.
  - **Fail-stop on a corrupt or too-new journal.** A `JournalCorruption` /
    `SchemaTooNew` (or a re-execution that diverges from the stored event —
    e.g. config drift) at boot is a typed fatal `AppStateError::Recovery`
    naming the exact `(underlying, sequence)` — the venue refuses to serve
    rather than silently start fresh over durable history. A contradictory
    durable manifest (disagreeing persisted lineages) fails stop with
    `RecoveryLineageConflict`; the `u64::MAX` edge is `RecoverySequenceExhausted`.
  - **Seed-vs-recover precedence.** A recovered underlying is NOT re-seeded
    (instrument registration + opening-price seeding skip it) — recover wins
    for underlyings with journal history; seeding applies only to genuinely
    fresh underlyings. The empty-journal fresh-boot path is unchanged.
  - Determinism oracle (`tests/determinism.rs`): record → restart → continue,
    and the continued run's events are identical to a never-restarted run
    given the same post-restart commands. A `testcontainers` restart test
    (`tests/recovery.rs`, `#[ignore]`-gated) drives boot → trade → kill →
    re-boot against the same Postgres and resumes at the continued sequence.
- **PostgreSQL-durable FIX session store — process-restart persistence** (#95).
  A `PgFixSessionStore` implements the existing `FixSessionStore` trait
  (unchanged); `main.rs` now selects it via `select_fix_session_store(db)` when
  `DATABASE_URL` is set and the in-memory backend otherwise (parity with #29's
  `PgVenueJournal`). So each `(account_id, sender_comp_id, target_comp_id)`
  session's sequence counters, outbound-frame resend log, and `SequenceReset`
  audit now survive a **process restart**, not just a same-process reconnect.
  - Migration `20260716120600_fix_sessions.sql`: three tables keyed on
    `(account_id, sender_comp_id, target_comp_id)` — `fix_session_counters`
    (`>= 1` CHECKed sender/target seqs, doubling as the key registry for the
    `MAX_SESSION_KEYS` bound), `fix_session_outbound` (append-not-dedup resend
    log, byte-exact `frame BYTEA`, oldest-first eviction), `fix_session_resets`
    (bounded reset-audit ring, injected venue-clock `at_ms`, never wall-clock).
  - Parameterized `sqlx::query!` / `query_scalar!` only; the `.sqlx/` offline
    artifacts are committed so a `SQLX_OFFLINE=true` build is green. Typed
    `SessionStoreError::Backend` static labels — `DATABASE_URL` / credentials /
    frame payloads are never logged. `checked` (`try_from`) on every
    seq/ms `u64`↔`i64` conversion. The durable backend lives in the gateway
    (`src/gateway/fix/pg_store.rs`) depending inward on `crate::db`, keeping the
    transport→persistence direction (a transport trait's backend, not a domain
    store).
  - `testcontainers` tests (`tests/db.rs`, `#[ignore]`-gated): counters +
    resend log + reset audit survive a simulated process exit (drop store +
    pool, reopen a fresh pool against the same DB, resume numbering); plus an
    in-memory/PG behavioral-parity test over one fixed trait-call scenario.
- **Durable `(account, ClOrdID) → order_id` index for cross-session
  cancel/replace** (#98). #39 correlated a FIX `OrigClOrdID` to the venue
  `order_id` via a **per-session** `HashMap`, so a cancel/replace on a new
  connection (or after reconnect) answered `9 Unknown order`. This adds an
  **account-scoped** `ClOrdIdIndex` in `AppState` that both FIX and REST resolve
  through (`AppState::resolve_client_order_id`), so a client can
  cancel/replace/status an order it placed in a **prior session**.
  - **Journal-derived, published post-journal.** `(account, ClOrdID) →
    order_id` is a deterministic function of the journaled `AddOrder` / `Replace`
    stream, so the **single-writer actor** publishes each correlation as a pure
    side effect **after** the paired event durably lands (never before — a
    placement whose event append fails seals the underlying and leaves the index
    untouched, so it can never expose an uncommitted mapping) and **#85 boot
    recovery rebuilds the identical mapping** by re-executing the same events
    through the same derivation. No PG table, no migration to keep in sync; the
    recovery integrity oracle is unaffected (the index never changes an event).
  - **Cross-session replace now journaled** (fix for the earlier per-session-only
    limitation): `VenueCommand::Replace` carries the replacement `ClOrdID` and the
    retired `OrigClOrdID` (both `#[serde(default)]`, so pre-#098 records still
    decode). On a committed successful replace the actor publishes
    `(account, new_ClOrdID) → new_order_id` and **retires** the stale
    `(account, OrigClOrdID)` entry — reproduced identically on recovery — so a
    replaced order is cancel/replace-correlatable on a later connection and the old
    id no longer resolves to a live order.
  - **Account isolation.** The key is `(AccountId, ClientOrderId)`; a colliding
    `ClOrdID` under another account is a different key ⇒ resolves to `None` ⇒ a
    clean `9` / not-found, **indistinguishable** from a genuinely unknown id (no
    cross-account ownership leak, mirroring #132's masking). The index is bounded
    both venue-wide (`DEFAULT_MAX_CLORDID_INDEX_ENTRIES`, typed `Full`) **and per
    account** (`DEFAULT_MAX_CLORDID_PER_ACCOUNT`, typed `AccountFull`) — the
    per-account fairness sub-quota stops one account exhausting the shared index
    for everyone (a degraded-path drop, never a failed order).
  - Tests: cross-session cancel + replace succeed on a new session; account-B
    cannot resolve account-A's colliding `ClOrdID`; an idempotent add retry never
    overwrites the canonical mapping; a fault-injected post-mutation event-append
    failure leaves the index without the uncommitted mapping and seals the actor; a
    stale cancel renders a masked `9`, not a false `Canceled`; the rebuilt index
    after restart resolves the same `ClOrdID`s deterministically (including a
    replace); REST/FIX parity at the shared seam.
- **`TransactTime(60)` on FIX `ExecutionReport(8)` — 4-of-4 observation join
  keys** (#104). #41 found the FIX report rendered 3 of the 4 fill-observation
  join keys (`ExecID 17`, `SecondaryExecID 527`, `LastLiquidityInd 851`) but not
  `venue_ts`. The report now carries `TransactTime(60)` as the venue event-time
  carrier, populated from `venue_ts` (the deterministic injected venue clock,
  never wall-clock) via the same `UtcTimestamp` formatter the inbound
  `NewOrderSingle(D)`'s tag 60 uses — so REST≡WS≡FIX now agree on all four join
  keys directly on the wire. A `Trade` leg carries the fill's own `executed_at`;
  accept/terminal/cancel carry the command commit `venue_ts`. Round-trips
  (`encode∘decode`), goldens regenerated, `docs/specs/fix-dialect.md` §2.2
  updated. (Tag 60 is now a required field on the report's decode path — the
  `fix_decode` fuzz target was rebuilt.)
- **FIX market-data hardening — re-subscribe semantics + trades/quotes
  boundary** (#101). Closes the three gaps #40's orderbook-only FIX MD left:
  - **Re-subscribe no longer silently orphans an `MDReqID` (security P3).** A
    second `MarketDataRequest(V)` (new `MDReqID`) naming a `Symbol(55)` already
    subscribed on the session is now rejected whole with a
    `MarketDataRequestReject(Y)` `MDReqRejReason=1` (redacted `Text(58)`
    disambiguates duplicate-symbol from duplicate-`MDReqID`), leaving the prior
    subscription and its `MDReqID` **live and untouched** — instead of silently
    overwriting it and orphaning the old id with no signal.
  - **A mixed `V` carrying a `Trade` entry (`269=2`) no longer silently drops
    it.** A `V` requesting a trade tape — alone or mixed with a book side — is
    rejected whole with `Y` `MDReqRejReason=8` (unsupported `MDEntryType`), via
    a `requests_trade_tape` gate.
  - **Trades/quotes decision documented as a coherent boundary.** Top-of-book
    "quotes" are already served as the depth-bounded (`MarketDepth=1`) orderbook
    projection (`269=0/1` `W`/`X`) — no separate channel is needed or minted. A
    **trade tape** (`269=2`) is **permanently out** of FIX MD: a trade print has
    no book snapshot and rides its **own** separate `instrument_sequence`
    namespace, distinct from the orderbook's. FIX `W`/`X` carries exactly one
    `RptSeq(83)` (the orderbook's), so serving the tape would multiplex a second
    on-the-wire sequence namespace under one `MDReqID`, breaking the "orderbook
    is the only on-the-wire `RptSeq` on a FIX MD subscription" invariant;
    per-fill detail already reaches a FIX client via `ExecutionReport(8)`.
    Documented in `docs/specs/fix-dialect.md` §2.3 + `docs/03` §5.4.
- **Venue price band enforced on the market-maker + simulation producer seams**
  (#109). The venue-owned `min_price_cents` / `max_price_cents` admission band
  (`MicrostructureConfig::admit_price`) was enforced at the user order seam
  (`AppState::submit`) + the replay re-execution seam, but the **internal
  producers** bypassed it — a band-unaware MM requote or a simulation price step
  could rest **outside** the band. Both seams now enforce the SAME band (the
  existing `admit_price` / `check_price_band`, not a forked check):
  - **MM requote (`ActorCommandSink`) drops** a band-violating quote before it
    is submitted or journaled (logged `debug`); each requote leg is its own
    `VenueCommand`, so the out-of-band side drops while the in-band side still
    posts, and a `CancelOrder` (no price) is never band-blocked.
  - **Simulation step (`VenueStepSink`) rejects** an out-of-band reference price
    at `apply_step` (returns `false`), so it is never sequenced, drives no
    requote, and — honoring the journal-before-publish contract — publishes no
    price.
  - Both drops happen **before** the write-ahead journal append, so an
    out-of-band item is simply never in the journal — replay reproduces the
    identical admitted stream (proved by `test_band_drop_is_identical_across_two_runs`).
  - `check_price_band` now also admits a `SimStep` **reference** price against
    the band, so a price entered via REST `insert_price` (submitted as a
    `SimStep` through `AppState::submit`) is band-checked identically to the
    simulation producer — no producer can inject an out-of-band price. The band
    is now an unconditional venue-wide admission invariant (gateway
    order-admission + REST `insert_price` + replay + both internal producers),
    which makes the checked-fee proof unconditional.
- **Added the v1.0 stability soak** (#54, `tests/load.rs`,
  [BENCH.md §14](BENCH.md#14-stability-soak--flat-memory-no-sequence-gaps-clean-shutdown-restart-from-journal-054-v10)).
  `#[ignore]` + `SOAK=1`-gated (never on the fast CI gate;
  `SOAK=1 cargo test --test load -- --ignored`, or `make soak`), it drives a
  bounded, sustained order-flow window through the real REST router
  (`tests/conformance/`) and asserts the four v1.0 acceptance properties:
  flat RSS over the window (`ps -o rss=`, a disclosed POSIX-only mechanism —
  never `getrusage`'s monotonic-peak `ru_maxrss`), no sequence gaps in either
  `underlying_sequence` (the live journal) or `instrument_sequence` (the live
  WS broadcast), a clean shutdown that GENUINELY drains a concurrent burst
  against a deliberately small bounded mailbox with zero orders lost — a
  dedicated actor built directly on `spawn_matching_actor` (the one place an
  awaitable completion `JoinHandle` exists; `AppState` itself detaches and
  discards it) drops every handle, then actually AWAITS the actor's own task
  to completion before reading a surviving `Arc<Mutex<...>>`-backed journal
  clone back, rather than inferring the drain from the submitters' own
  results — and restart-from-journal determinism — reusing the real
  `fauxchange::simulation::replay_bundle` recovery-as-re-execution oracle
  (never a reimplementation), including the negative case: a corrupted
  stored event halts recovery with the typed `JournalCorruption` naming the
  exact `(underlying, sequence)`. Also records real REST round-trip
  throughput/latency and a seeded-latency-draw fidelity check via the
  `bench-hdr` `hdrhistogram` harness, disclosing that the live gateway-edge
  application of injected latency is itself deferred to #111. New `Makefile`
  `soak` target. A real `SOAK=1 cargo test --test load -- --ignored` run
  (the documented default invocation) passed clean in 61.10 s with all four
  properties holding — see `BENCH.md` §14 for the measured numbers.

- **Armed the CI performance regression gate — the v1.0 performance gate**
  (#53, [07 §6](docs/07-performance-budgets.md#6-ci-regression-gate),
  [BENCH.md §13](BENCH.md#13-ci-regression-gate-ceilings-re-verification-and-the-dry-run-053)).
  New `.github/workflows/bench-regression.yml`: a `bench-regression` job
  (armed on every push and PR to `main`/`release/**`, reduced sample counts)
  plus a `bench-regression-nightly` job (`schedule` + `workflow_dispatch`,
  full default sample counts matching `BENCH.md`'s own methodology). Both run
  the new `scripts/bench_regression_gate.py` (stdlib-only, no new
  dependency) against a documented, generous absolute ceiling per hot path —
  never a same-machine p99 delta against the M4-Max-laptop-recorded
  `BENCH.md` baselines, since this repo has no self-hosted CI runner (the two
  rejected approaches — a pinned self-hosted runner class, a first
  CI-runner-established baseline — are named and their infeasibility
  documented in `BENCH.md` §13). The gate covers all five hot paths
  (HP-1..HP-5), the allocation-counting profile (a no-regression-over-
  baseline gate, not a literal-zero gate — the measured common actor turn is
  real and non-zero, `BENCH.md` §6/§13), and the HP-2 fan-out flatness sweep
  (recomputed independently from the parsed quantiles, not merely trusting
  the bench's own printed verdict). A real, injected 20 ms slowdown
  (temporarily added to `benches/hp1_order_path.rs`, reverted before commit —
  `src/` was never touched) proved the gate fails on a genuine regression
  before it was reverted; a synthetic log proved the alloc/flatness/missing-
  series branches too; the real, un-injected re-verification run proves a
  clean baseline passes. `BENCH.md` §13 also discloses an unresolved,
  reproducible ~2.3-2.6x divergence between this re-verification's freshly
  measured allocation counts and the previously committed §6 figures (same
  machine, same code, same `Cargo.lock`) — named as a follow-up for
  `matching-expert`/`architect` with a real call-stack profiler, not silently
  reconciled. `Makefile` gains `bench-regression` / `bench-regression-full`
  local targets mirroring the two CI jobs, and `workflow-<job-id>` now points
  `act` at the whole `.github/workflows/` directory (not just `ci.yml`) so it
  keeps mapping to every job id across both workflow files.
- **Packaged `fauxchange conformance` harness across REST, WS, and FIX** (#51) —
  a runnable `conformance` subcommand (`src/conformance/`) that spins ephemeral
  in-process parity venues behind the real REST server and FIX 4.4 acceptor, and
  drives the frozen cross-surface parity + conformance suites: order-entry parity
  REST ≡ FIX (place / partial / cancel-replace / STP / per-leg fees / reject /
  same-payload retry, under the docs/03 §7 normalization rule — protocol-only
  fields normalized, venue ids compared verbatim), observation parity REST/WS/FIX
  on the documented join keys, control parity REST/WS (no FIX control message),
  the FIX session/order/market-data conformance script plus every docs/03 §8
  reject row with a redacted `Text (58)`, and REST/WS conformance (OpenAPI shape,
  tokenless `/health`, permission gating, subscribe → snapshot → sequenced
  deltas with a monotonic `instrument_sequence`). Emits a machine-readable
  `conformance.v1` JSON report (per surface, per case) — every failure `detail`
  is length-bounded and **redacted** so the report never carries a secret / JWT /
  `DATABASE_URL` / credential echo — and exits **non-zero on any surface
  failure** so a downstream CI can gate on it
  ([03 §7](docs/03-protocol-surfaces.md), [TESTING.md §6–§7](docs/TESTING.md)).
- **HP-4 market-maker requote budget and the requote-isolation assertion — the
  v0.5 performance gate** (#50,
  [07 §3-4](docs/07-performance-budgets.md#3-latency-budgets-design-targets)).
  Tests/benches-only, no public-surface change. `benches/mm_requote_hdr.rs`
  measures the real, persona-driven requote pipeline
  (`MarketMakerEngine::update_price` → `requote_symbol` → the edge calc →
  `update_quote` → the generated `VenueCommand`s handed to a `CommandSink`,
  #47) over a 10-contract chain, reporting p50/p99/p99.9/p99.99 via
  `hdrhistogram` (never criterion's mean) in two sections — engine-only (pure
  compute, `CountingSink`) and mailbox-wired (the real `ActorCommandSink` onto
  a real spawned actor) — closed-loop and open-loop (coordinated-omission
  corrected). `BENCH.md` §12 carries the measured DESIGN-TARGET baseline, the
  run conditions, and a disclosed 2-vs-4-worker scheduler-contention tuning
  finding. `tests/requote_isolation.rs` asserts the milestone's central
  acceptance criterion: a continuous, concurrent, realistic-cadence requote
  sharing a client's own underlying actor mailbox does not inflate the
  client's HP-1-style p99 beyond a documented, honestly-derived tolerance
  factor (5/5 real runs at ~1.0x — no measurable inflation at this
  configuration). `benches/alloc_profile.rs` gains a third section proving the
  steady-state requote's allocation count (343 allocs/op over a 10-contract
  chain — non-zero, honestly reported as the regression-signal baseline docs/07
  §4 asks for). Shared fixtures land in `benches/support/mm_workload.rs`
  (the persona-bound chain + `CountingSink`) and `benches/support/workload.rs`
  (`jitter_stream`), reused by all three. The CI bench-regression *gate* stays
  unarmed (#053, v1.0) — this bench/test pair runs non-gating, same as every
  other hot path in `BENCH.md`.
- **Scenario failure-mode determinism + the out-of-range knob rejection matrix —
  the v0.5 capstone** (#49,
  [05 §11](docs/05-microstructure-config.md#11-determinism-of-microstructure)).
  Tests-only, no public-surface change. The new `tests/scenario_failure_modes.rs`
  composes the already-built knobs (#44–#47) into three documented failure modes
  and proves each reproduces for a fixed descriptor, asserting equality only over
  the **journaled** artifacts (fills / events / book state per underlying) with
  mark prices asserted **out of scope as exclusions**: **throttling** (the real
  `RateLimiter` over a `RateLimitConfig`-derived budget on a fixed venue clock
  yields the byte-identical allow/deny stream and the `429`/`Retry-After`,
  reproducibly and seed-independently — it is a gateway boundary control, not a
  journaled artifact); **halt** (a journaled `SetInstrumentStatus(Halted)` control
  plus an order into the halted strike is the journaled `Rejected` that replays,
  reproduced across two independent runs via `export_bundle` → `replay_bundle`);
  and **wide-spread starvation** (a seeded `wide_skewed` persona rests a finite,
  wide ladder that starves a taker which a `tight` persona would have filled — the
  same seed regenerates the identical journaled command stream and starved
  outcome). Plus a **rejection matrix**: co-located `test_config_rejects_out_of_range_*`
  unit tests for every microstructure knob (fee bps + the checked-fee-proof bound;
  latency NaN/negative/`min > max`; the three persona clamps + NaN; tick/lot/
  `max_price_cents`; non-positive rate-limit window/budget), each a typed
  `ConfigError` at load, and an extension of the `config_validate_rejects_out_of_range`
  property test over the full v0.5 knob space (accept **iff** genuinely in range).
- **DoS-control security-gate suite — the five DoS bounds proven as security
  controls, not merely fairness knobs — the v0.5 security gate** (#48,
  [08 §5](docs/08-threat-model.md#5-denial-of-service-posture)). Tests-only, no
  public-surface change: the new `tests/dos_controls.rs` floods each of the
  five already-wired bounds far past its configured ceiling and asserts the
  control's own bookkeeping stays bounded (not merely that the last request
  was rejected) — the sliding-window `RateLimiter`'s tracked key-space under a
  flood of thousands of distinct callers, and one `AccountId`'s **one budget
  shared across REST and FIX**; the per-underlying actor's bounded `mpsc`
  mailbox under a concurrent flood (never queues past capacity); the WS
  broadcast fan-out's laggard-drop-and-resnapshot (a non-draining consumer
  pays for one `Lagged` catch-up, never a message-by-message replay); the
  venue connection cap and the per-connection (256) WS subscription cap under
  flood; and the checked `u64` sequence counter's permanent seal at
  exhaustion (seeded via the actor's own test seam, never counted to `2^64`).
- **Market-maker personas, resting-liquidity shaping, and halt scenarios** (#47) —
  a typed `PersonaConfig` (`base_spread_bps`, `base_size`, `spread_multiplier`,
  `size_scalar`, `directional_skew`) surfaced from `[market_maker.personas.<name>]`,
  reusing the Backend `validate_control_value` clamps (NaN-rejecting;
  `spread_multiplier ∈ [0.1, 10]`, `size_scalar ∈ [0, 1]`, `directional_skew ∈
  [-1, 1]`) at **load and runtime control**
  ([05 §8](docs/05-microstructure-config.md#8-market-maker-personas)). Personas are
  resolved **per instrument / per underlying** (the seeding phase binds each
  contract to its `PersonaConfig`; the engine's global config is a neutral runtime
  overlay applied once on top), so a `tight` maker on one strike and a
  `wide_skewed` maker on another differ in their **journaled** quotes — lifting the
  former one-global-persona limit. Persona jitter (quote-size / skew noise) is
  seeded per `(persona, symbol)` from the one run-level seed via a new shared
  `SplitMix64`+FNV-1a primitive (`src/rng.rs`) under an **independent** `FauxPers`
  domain tag (latency `src/microstructure/latency.rs` refactored onto the same
  primitive), so it is reproducible for a fixed seed and replays identically.
  Resting-liquidity shaping bounds the size at each quoted level via
  `base_size` / `size_scalar` (+ jitter trim); **partial fills arise only from real
  matching against finite resting liquidity — there is no synthetic
  fill-probability / slice-sizing draw anywhere on the path**
  ([05 §9](docs/05-microstructure-config.md#9-partial-fill-and-liquidity-shaping)).
  Runtime knobs over WS control actions (`set_spread` / `set_size` / `set_skew` /
  `kill` / `enable`) and REST `POST /api/v1/controls/{parameters,kill-switch}` now
  validate at the boundary (`validate_control_knobs`) and **apply** the knob:
  the sequenced `MarketMakerControl` fans out to every underlying's actor and, via
  a late-bound `MarketMakerControlHub` (bound in `AppState::new`, installed only on
  the live path — replay installs no sink), pushes the change onto the engine
  inside the actor turn as an idempotent, requote-free state write; the requotes it
  induces enter as their own journaled `AddOrder`s, so a control change replays
  from the journal without a live engine. Halt scenarios run on the sequenced
  execution plane (phase 1): the `InstrumentStatusRegistry` gates an order into a
  non-`Active` instrument to a journaled `Rejected` that replays identically, and
  `SetInstrumentStatus` / `MassCancel` (incl. `GTC`) / `EvictExpiredOrders` execute
  and journal. A new `ExpirySchedule` (`src/simulation/expiry.rs`) uses the upstream
  `ExpiryScheduler` operational times (default `08:00` / `08:30 UTC`) as a
  **schedule source only** — never the book-mutating upstream lifecycle manager —
  and `AppState::run_expiry_roll` issues the sequenced `MassCancel → Settling →
  Expired` transitions forward-only; `AppState::evict_expired_orders` sweeps
  intraday `Day` / `Gtd` TIF. Expiries use `ExpirationDate::DateTime` (no `Settled`
  status — the lifecycle is `Settling → Expired`)
  ([05 §10](docs/05-microstructure-config.md#10-halt-scenarios)). **Honest
  limitations:** the halt/expiry reject is decided inside the actor turn and is
  **journaled + replays**, but is not yet surfaced live through the
  `Receipt`→`VenueOutcome` seam; `Day` / `Gtd` eviction is journaled and
  replay-stable but its removal effect depends on the per-leaf clock the pinned
  `option-chain-orderbook` 0.7.0 does not inject.
- **Per-tier rate limits and per-instrument microstructure profiles** (#46) — the
  `[rate_limits]` config section surfaces a validated `RateLimitConfig`
  (`window_secs`, `read_per_window` / `trade_per_window` / `admin_per_window`,
  `#[serde(deny_unknown_fields)]`, non-positive values rejected at load with a
  typed `ConfigError::BadRateLimitValue`) wired into the existing sliding-window
  `RateLimiter` from #11. A `RateLimitTier` (`Read` / `Trade` / `Admin`, highest
  held permission) rides the authenticated `RateLimitKey::Account` so one
  `AccountId` keeps **one** budget across REST / WS / FIX, and the window stays
  keyed on the injected venue clock so throttling replays deterministically. A
  `MicrostructureProfile` resolves the active knobs per instrument / per
  underlying on the order path with venue-default inheritance (specs + price band
  vary per instrument today; fees / STP / latency are venue-wide and personas
  join in #47), applied identically on the live and replay seams so a
  profiled instrument reconstructs exactly from a bundle
  ([05 §5](docs/05-microstructure-config.md#5-rate-limits),
  [05 §7](docs/05-microstructure-config.md#7-per-instrument-profiles)). REST / WS
  overflow returns `429` with `X-RateLimit-*` headers.
- **Seeded latency injection on the virtual clock** (#45) — a new
  `src/microstructure/latency.rs` surfacing `[microstructure.latency]` as a
  validated `LatencyConfig` with a `model` selector
  (`fixed` / `uniform` / `normal` / `lognormal`) and per-model parameters
  (`us`; `min_us` / `max_us`; `mean_us` / `sigma`; `median_us` / `sigma`),
  `#[serde(deny_unknown_fields)]` on the file surface
  ([05 §3](docs/05-microstructure-config.md#3-latency-injection)). Every inbound
  message draws a per-message delay against the configured distribution, seeded
  per `(session_id, msg_seq)` from the one run-level seed via an **independent**
  venue-owned sub-stream (a self-contained SplitMix64 keyed by an FNV-1a fold of
  the session id under a fixed latency domain tag — no new RNG dependency, no
  unseeded RNG). The draw is a pure function of its inputs (no clock, no shared
  counter, no map-iteration order), so two runs with the same seed draw
  identically and different seeds diverge. The delay is a **virtual-clock
  offset** (`LatencyOffset`, #28's clock), not a real `tokio::time::sleep`. This
  change lands the config, the seeded draw, and the offset; the **live
  gateway-edge application** — the deterministic ingress-reorder buffer
  ([03 §6.1](docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering))
  that consumes the offset to actually reshape *arrival order* into the
  single-writer actor before the sequencer — is deferred to
  [#111](https://github.com/joaquinbejar/fauxchange/issues/111). The determinism
  tests already prove the load-bearing invariant (reordering arrivals only
  permutes order, never mutates a command, and a fixed permuted order replays to
  identical fills), so #111 wires the mechanism onto a proven contract. The
  offset rides the reproducible timeline so a latency-injected run replays
  identically. The
  `normal` draw is clamped `≥ 0` and every draw is guarded finite before it
  becomes an offset (NaN / Inf can never reach the surface). Startup validation
  rejects a missing/negative parameter, a non-finite or negative `sigma`, and
  `min_us > max_us` with a typed `LatencyConfigError` folded into
  `MicrostructureConfigError::Latency`. The resolved config rides in the scenario
  bundle and folds into the microstructure fingerprint, so a latency-injected
  scenario is self-describing and a fingerprint mismatch refuses replay. Unlike
  the price walk (journal-driven, `optionstratlib`'s sampler unseedable), the
  latency sub-stream *is* seed-reproducible — the two are kept distinct in the
  determinism tests (`test_injected_latency_changes_arrival_order_only`,
  `test_latency_config_rides_the_bundle_and_gates_replay`).

- **Fee schedule, self-trade prevention, and contract specs as declarative
  venue config — the v0.5 microstructure opener** (#44) — a new
  `src/microstructure/` config surface that exposes the **upstream**
  `FeeSchedule`, `STPMode`, and `ContractSpecs` / `ValidationConfig` as
  validated venue config, never forking matching
  ([05 §1](docs/05-microstructure-config.md#1-scope)). `[microstructure.fees]`
  → `FeeConfig` (`maker_bps` negative = rebate, `taker_bps` ≥ 0);
  `[microstructure.stp]` → `StpConfig` with venue snake_case tokens
  (`off` / `cancel_taker` / `cancel_maker` / `cancel_both`) mapping the upstream
  `STPMode` keyed on the account owner `Hash32`; `[instruments."<SYM>".specs]`
  and the venue-default `[microstructure.specs]` → `ContractSpecsConfig`
  (`tick_size_cents`, `lot_size`, `min_price_cents`, `max_price_cents`,
  `max_order_qty`), settable per instrument or per underlying with venue-default
  inheritance for unset knobs. The **venue-owned `max_price_cents` /
  `min_price_cents` admission band** has no upstream equivalent (verified: the
  pinned `orderbook-rs` 0.10.5 `ValidationConfig` carries no price bound) and is
  enforced before matching via `MicrostructureConfig::admit_price`. The
  **checked-fee startup proof** (`FeeConfig::validate_notional_bound`, run per
  resolved spec in `MicrostructureConfig::resolve`) rejects any config whose
  widest notional (`max_price_cents × max_order_qty`) could drive the upstream
  `FeeSchedule::calculate_fee` onto its `saturating_mul` branch — Part A bounds
  it by the upstream `FeeSchedule::max_guaranteed_exact_notional()` (the
  `try_calculate_fee` guarantee landed in 0.10.5), Part B keeps the worst-case
  fee within the persisted `i64` cents — so the saturating branch is provably
  unreachable at runtime (Override O-1: money is checked, never saturating).
  A property test asserts no accepted config drives `try_calculate_fee` to an
  error, and `apply_to_underlying` (the seam the venue calls at book creation)
  is unit-tested against real upstream `UnderlyingOrderBook`s proving STP `off`
  allows / `cancel_taker` prevents a same-owner self-trade, an off-tick price is
  leaf-rejected, and two fresh books with the same config replay identical fills.
  `#[serde(deny_unknown_fields)]` on every new struct; an out-of-range value is a
  startup `ConfigError::Microstructure`. Surfaced through `src/config.rs`
  (`Config.microstructure`, resolved from the file layer with the proof at load).
- **The #44 microstructure wired into the venue's sequenced order path** (#44) —
  the resolved `MicrostructureConfig` now flows from `AppStateConfig`/`main.rs`
  into every book at creation and is applied **identically on the live and replay
  paths**, so a fee/STP-sensitive scenario reconstructs exactly
  ([02 §5](docs/02-matching-architecture.md#5-determinism)). Four seams:
  (1) `MatchingExecutor::new_with_registry_and_index` (live) and
  `new_with_microstructure` (recovery) apply the fee schedule, STP mode, and
  contract specs before any leaf is vivified; (2) `AppState::submit` enforces the
  venue-owned price band **before** the sequencer, so an over-`max_price_cents`
  order is rejected (`InvalidOrder`) and never journaled; (3) journal recovery
  (`recover_with_microstructure`) and the replay driver
  (`replay_streams_with_microstructure`) thread the config so a replayed book
  inherits the identical schedule — and a recorded `ScenarioBundle` now **carries
  the resolved `MicrostructureConfig`** with `RunManifest.microstructure_fingerprint`
  as the equality gate (a mismatch refuses replay, like the schema/version guards);
  (4) the fill seam computes fees through the **checked** upstream
  `FeeSchedule::try_calculate_fee` (its provably-unreachable overflow maps to the
  typed `MoneyError::Overflow`, never a clamp). The flagship determinism test
  records a maker-rebate / taker-fee self-cross scenario and replays it from the
  bundle into identical fills (incl. `Fill.fee`), events, and top-of-book.
- **Hardened the replay/bundle path so a hostile scenario bundle cannot bypass the
  #44 controls** (#44) — closing two review findings on the Admin-only replay
  surface: (1) the venue-owned price band is now enforced on the **replay
  re-execution seam** (a shared `check_price_band` the live `AppState::submit` and
  the recovery reducer both call), so a bundle smuggling an out-of-band `AddOrder` /
  `Replace` price is refused (`ReplayError::PriceOutOfBand`) before the command
  re-executes — the live and replay admission seams now enforce the band
  identically; (2) the **checked-fee proof is re-run** on a bundle's carried
  `MicrostructureConfig` (which deserializes directly, bypassing
  `MicrostructureConfig::resolve` — the fingerprint gate is tamper-detection, not
  authenticity) via `MicrostructureConfig::validate` in `verify_bundle_microstructure`,
  so a self-consistent hostile bundle carrying an unprovable fee config is refused
  with the **specific** `ReplayError::ConfigRejected` before any command
  re-executes, never a downstream generic overflow. The `apply.rs` band-enforcement
  claim is scoped honestly: the band holds at the gateway order-admission and replay
  re-execution seams, but the internal market-maker requote producer
  (`ActorCommandSink`) does not yet run it — a market-maker quote (incl. a
  simulator-induced one) can rest outside the band, fail-safe today via the checked
  fee seam — with the enforcement + the over-band behavioural choice (drop / clamp /
  reject) tracked as follow-up #109. (The simulator sink forwards `SimStep` price
  updates, which carry no order price, so the band does not apply to it.)
- **The HP-3 FIX parse/encode hot-path budget in `BENCH.md` — the v0.4
  PERFORMANCE gate** (#43) — `benches/hp3_fix_parse.rs`, a `bench-hdr`
  benchmark (hdrhistogram p50/p99/p99.9/p99.99, never criterion's mean)
  measuring `fauxchange::gateway::fix::decode` on a framed `NewOrderSingle
  (D)` and `FixBody::encode` on a typed `ExecutionReport (8)` — the EXACT
  functions the live acceptor's dispatch seam calls, never a
  reimplementation, and pure wire-seam venue overhead (no matching, no order
  path, [07 §5](docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention)'s
  match/overhead separation). Fixtures
  (`benches/support/fix_fixtures.rs`) reuse the identical golden `D`/`8`
  shapes `tests/golden_fix.rs` already golden-tests (#036), with two new
  `tests/bench_harness.rs` unit tests proving both fixtures decode to
  themselves rather than silently measuring a reject path. Warmup precedes
  every measured loop; coordinated omission is disclosed and corrected via a
  new `support::openloop::run_open_loop_pure` (extending
  `benches/support/openloop.rs`'s existing `ActorHandle`-shaped
  `run_open_loop` to a generic, mailbox-free open-loop generator for a plain
  synchronous call) — each call dispatches on a fixed intended-send schedule
  independent of completion, recording sojourn time. `BENCH.md` §11 records
  three independent real runs on this machine: closed-loop decode `p50` is
  **sub-microsecond** (750–875 ns) with a low-single-digit-µs `p99`/`p99.9` tail
  (1.08–2.54 µs), and encode is **sub-microsecond through `p99.9`** (417–750 ns)
  — one to two orders of magnitude inside a sub-millisecond reading, stated as the
  grounding DESIGN TARGET data docs/07 §3-HP3 has withheld a number pending
  ("architect follow-up to state the actual budget, not set by this bench").
  The open-loop sojourn numbers are honestly disclosed as dominated by the
  harness's own Tokio task-spawn/scheduling overhead at this sub-microsecond
  operation scale (~10–25× the closed-loop service time) rather than a
  decode/encode regression — a real, different effect from HP-1's open-loop
  section, where that same overhead is negligible next to a
  hundreds-of-microseconds actor turn. No new dependency: reuses the existing
  `hdrhistogram`/`criterion` dev-dependencies and the already-present
  `ironfix-core` types. Does not arm the CI `bench-regression` gate
  (deliberately out of scope, lands before v1.0, #053).
- **The FIX parser fuzz target, adversarial corpus, and no-credential-echo
  proof — the v0.4 security gate** (#42) — `fuzz/fuzz_targets/fix_decode.rs`
  (a `cargo-fuzz`/`libfuzzer-sys` harness, its own deliberately separate Cargo
  workspace so the root `Cargo.lock` and `cargo audit`/`cargo deny` gates never
  see it) drives arbitrary bytes through the **exact real two-stage decode
  path** the acceptor runs on every inbound TCP read —
  `BoundedFrameDecoder::decode` (framing) then
  `fauxchange::gateway::fix::decode` (tag-value) — never a reimplementation.
  `fuzz/corpus/fix_decode/` commits 15 hand-crafted adversarial fixtures
  (oversized frame, truncated/incomplete frame, malformed checksum, tag
  injection reopening a duplicate `Symbol`, duplicate/missing required tags,
  an out-of-range `Price`, a malformed `Symbol`, a repeating-group
  count/delimiter violation, field- and group-entry ceilings, a `BeginString`
  mismatch, and both unsupported `MsgType` shapes), and the new
  `tests/fix_adversarial.rs` feeds each one to the real pipeline asserting the
  **specific** `FixDecodeError`/`FrameError` variant and its `FixRejectRoute`
  classification — never a blanket `is_err()`, never a panic, never a silent
  accept — plus a blanket no-panic sweep over the whole corpus directory.
  `tests/fix_session.rs` gains a companion captured-log test driving a full
  logon **+ order flow** (resting order, crossing fill, an unknown-order
  cancel reject, an unsupported-message business reject — every reply type
  that can carry a `Text (58)`) and asserting no password, Argon2id hash,
  bootstrap secret, or JWT signing-key fragment appears in either the captured
  `tracing` output or any outbound `Text (58)`, complementing the existing
  #38 logon-only test. The CI `fuzz` job (nightly toolchain pinned to that job
  only, `.github/workflows/ci.yml`) runs a short bounded pass
  (`-max_total_time=120 -max_len=262144`) over the committed corpus on every
  push/PR.
  **A real bug, found and closed in this issue:** a short local fuzzing run
  surfaced a genuine crash within ~90 seconds — a duplicate/mid-body injected
  `CheckSum (10)` field (e.g. `10=624`, before the real trailing checksum)
  reached `ironfix-tagvalue`'s `parse_checksum`, which folds
  `d0*100 + d1*10 + d2` in unchecked `u8` arithmetic and overflows for any
  three-digit value `> 255` (panicking in a debug build, silently wrapping to
  a wrong value in a release build). The existing `BoundedFrameDecoder`
  framing-layer precheck only inspects the positionally-**trailing** checksum,
  so it did not close this: `ironfix_tagvalue::Decoder::decode` folds the
  **first** `CheckSum (10)` it reaches, wherever tag 10 occurs. Closed by a new
  pre-codec `reject_malformed_checksum` guard in `src/gateway/fix/mod.rs`
  (scans **every** tag-10 occurrence, not only the trailing one) and a new
  `FixDecodeError::MalformedChecksum` variant routing to a session
  `Reject (3)` (`SessionRejectReason=6` IncorrectDataFormat, `RefTagID=10`,
  `src/gateway/fix/error.rs`), with an exhaustive `000..=999` mid-body sweep
  test and the actual minimized crashing input committed byte-for-byte as
  `fuzz/corpus/fix_decode/mid_body_checksum_overflow.fix` (asserted in both
  the co-located `src/gateway/fix/mod.rs` unit tests and
  `tests/fix_adversarial.rs`) — the fuzz gate did exactly what it exists to
  do. The corresponding upstream `ironfix-tagvalue` `parse_checksum`
  reachability is tracked against the existing IronFix#4-class report.
- **The FIX arm of the parity/conformance suite** (#41) — extends the REST/WS
  parity fixtures with FIX: **order-entry parity REST ≡ FIX** (one
  identically-seeded fresh venue per surface, comparing `VenueEvent` streams
  under the explicit normalization rule — protocol-only fields normalized,
  `underlying_sequence`/`execution_id`/fills/book state compared verbatim; cases
  place / partial / cancel-replace / STP-shape / fees / reject / retry),
  **observation parity REST/WS/FIX** on the fill join keys (a FIX
  `ExecutionReport (8)` renders three of the four keys — `venue_ts` is REST/WS
  only, recoverable via `execution_id`), and a **captured FIX conformance
  script** asserting the happy path plus every context-sensitive reject row
  (`3` / `8 Rejected` / `9` / `Y` / `j` / `Logout 5`) with reason/reference tags
  and `Text (58)` redaction. WS is asserted out of order-entry parity, present in
  observation. Tests only. Two documented cross-surface nuances tracked as
  follow-ups: REST-vs-FIX idempotency sequence consumption (#103) and an optional
  `TransactTime (60)` on the FIX report for four-of-four join keys (#104).
- **FIX market data as a thin projection of the WS subscription manager** (#40) —
  `MarketDataRequest (V)` maps onto the **same**
  `OrderbookSubscriptionManager` (#14) the WS surface reads, so
  `MarketDataSnapshotFullRefresh (W)` renders the identical book snapshot as
  `orderbook_snapshot` and `MarketDataIncrementalRefresh (X)` the identical
  resulting-quantity deltas as `orderbook_delta`, both carrying the same
  per-instrument `instrument_sequence` as `RptSeq (83)` — **observation parity by
  construction** (the WS market-data gap contract IS the FIX one,
  [03 §5.4 / §7](docs/03-protocol-surfaces.md#54-market-data)). The new
  `src/gateway/fix/md_projection.rs` is a pure `WsMessage → W`/`X` projection
  (bids → `MDEntryType 0`, asks → `1`; `MDEntrySize (271) = 0` = level removed via
  `MDUpdateAction (279) Delete`, else `Change`), and `VenueFixSession` gains the
  market-data router: a `V` subscribes its symbols, emits a `W` baseline per
  symbol immediately, and streams `X` deltas drained from the shared broadcast
  onto the same bounded outbound mailbox (on inbound activity and on the session
  cadence tick). A per-symbol baseline filter (mirroring the WS surface) drops any
  delta at or below the delivered `W`'s sequence, so `RptSeq (83)` strictly
  increases across the `W → X` boundary and the FIX and WS streams are identical
  by construction. Recovery keeps the two FIX sequence namespaces **distinct**: an
  `instrument_sequence` gap (a lagged broadcast) recovers by a fresh `W` per
  subscription — never `ResendRequest (2)`, which repairs only session
  `MsgSeqNum`. Market-maker requotes do not emit `X` (reflected in the next
  snapshot, inherited from the manager). An unsupported request is a
  `MarketDataRequestReject (Y)` with `MDReqRejReason (281)`, never a bare
  `Reject (3)`: a Trade-tape-only request → `8` (the trade-tape/quote projection
  over FIX MD is deferred; the book is the `instrument_sequence`-carrying
  surface), a duplicate `MDReqID` → `1`, the per-session subscription ceiling →
  `2`, and a session lacking `Read` → `3`. `V` requires `Read`, gated on the same
  permission model as REST/WS; `prices`/`fills` are not served over FIX MD.
  `MDEntryPx (270)` crosses the wire decimal through the #36 checked `Price`
  seam, cents internally. Adds `md_projection` unit tests, the same-book
  RptSeq≡`instrument_sequence` observation-parity property, the MM-requote and
  gap-recovery-by-fresh-`W` properties, and the end-to-end `V`→`W`→`X` / `Y`
  reject conformance suite
  ([040](milestones/v0.4-fix-gateway/040-fix-market-data.md),
  [specs/fix-dialect.md §2.3](docs/specs/fix-dialect.md#23-market-data-subscription-surfaces-03-54)).
- **FIX order entry mapped onto the sequenced order path with context-sensitive
  rejects** (#39) — the order-submitting messages `NewOrderSingle (D)` /
  `OrderCancelRequest (F)` / `OrderCancelReplaceRequest (G)` translate to the
  **same** `VenueCommand` the REST handler produces and submit through the same
  single-writer `AppState::submit` seam (**parity by construction** — a new shared
  `add_order_command` builder both gateways call, so an order over FIX and over
  REST derive the byte-identical command, fills, resting state, and
  `underlying_sequence`, [03 §7](docs/03-protocol-surfaces.md#7-protocol-parity-guarantees)).
  `OrderMassCancelRequest (q)` renders an honest `OrderMassCancelReport (r)`
  `Rejected` (venue-wide mass-cancel routing is not yet wired — the per-underlying
  submit path does not route a mass cancel, as the REST cancel-all handler returns
  `400`; tracked as [#97](https://github.com/joaquinbejar/fauxchange/issues/97)),
  and `OrderStatusRequest (H)` is a committed-fills status read (no submit). The new
  `src/gateway/fix/order_flow.rs` renders committed `VenueEvent`/`Fill` legs as
  `ExecutionReport (8)` (`New` on accept; `Trade` with `PartiallyFilled`/`Filled`
  per fill leg carrying `SecondaryExecID (527)` = `underlying_sequence`,
  per-leg `Commission (12)`+`CommType (13)=3`, and `LastLiquidityInd (851)`;
  `Canceled` for a killed `IOC`/`FOK`/market remainder), and the `SessionFsm`
  gains the async order router on `VenueFixSession`. **Rejects are
  context-sensitive** through the existing `src/error.rs` seam: a `D` failure is
  `ExecutionReport (8) Rejected` (`OrdRejReason (103)`), an `F`/`G` failure is
  `OrderCancelReject (9)` (`CxlRejReason (102)` + `CxlRejResponseTo (434)`), an
  unsupported application `MsgType` is the new typed `BusinessMessageReject (j)`,
  and only a session-protocol failure is a bare `Reject (3)` — never `3` for an
  application failure. Cross-protocol idempotency rides the `ClOrdID` (the
  account-scoped key the executor dedups on); `Trade` permission + the sliding
  rate limiter gate every mutating op in message context (a throttled `D` →
  `8 Rejected`, never `3`); **no control-plane message is exposed on FIX**. Adds
  `UtcTimestamp::to_epoch_ms` (the `ExpireTime (126)` → `Gtd(ms)` inverse), a
  `business_message_reject_j` golden, a REST≡FIX command-parity proptest, a
  FIX-arriving-order determinism test, and the end-to-end reject-matrix /
  per-leg-fee / idempotent-retry conformance suite
  ([039](milestones/v0.4-fix-gateway/039-fix-order-flow.md),
  [03 §5.3 / §8](docs/03-protocol-surfaces.md#53-order-entry-and-execution-reports),
  [specs/fix-dialect.md §2.2 / §4 / §5](docs/specs/fix-dialect.md#22-order-entry-and-execution),
  [ADR-0006](docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
  [ADR-0009](docs/adr/0009-lossless-venue-envelope-outcomes.md)).
- **The FIX 4.4 session layer — acceptor-side logon auth, the account ↔ CompID
  binding, checked non-wrapping sequence counters, heartbeat cadence, and an
  account-keyed session store with resend / reset** (in-memory backend; session
  state survives a *reconnect*, and a PostgreSQL backend for process-restart
  persistence is deferred to
  [#95](https://github.com/joaquinbejar/fauxchange/issues/95)) (#38) in the new
  `src/gateway/fix/fsm.rs` (the `SessionFsm` + `VenueFixSession` /
  `VenueFixSessionFactory`) and `src/gateway/fix/store.rs` (the `FixSessionStore`
  swap seam + `InMemoryFixSessionStore`), with the `main.rs` wiring swapping the
  #037 stub for the real session, two validated `[fix]` knobs
  (`logon_timeout_secs`, `max_heart_bt_int_secs`), and
  `tests/fix_session.rs` + new `tests/property.rs` cases
  ([038](milestones/v0.4-fix-gateway/038-fix-session-layer.md),
  [03 §5.2 / §6](docs/03-protocol-surfaces.md#52-session-management--the-acceptor-fsm-and-checked-counters),
  [ADR-0007](docs/adr/0007-fix-credentials-and-account-model.md),
  [ADR-0010](docs/adr/0010-fix-session-account-binding.md)). IronFix models the
  **initiator** only (`Session<S>` cannot receive-logon/send-ack) and wraps its
  `SequenceManager` with `fetch_add`, so the acceptor FSM
  (`Listen → AwaitingLogon → Authenticating → Active`, + resend / close), the
  **checked** `MsgSeqNum` counters (overflow → a typed `SequenceExhausted` that
  **seals** the session, never a silent wrap), and the durable store are new
  venue work. **One permission model, no second auth system**: a `Logon (A)`
  verifies plaintext `Username (553)` / `Password (554)` against the venue account
  registry's Argon2id hash (run under `tokio::task::spawn_blocking` — a CPU-bound
  verify never stalls the accept loop or drain) and maps to the **same**
  `AccountId` / `Permission` a JWT resolves; session admin needs no permission,
  trading (`D`/`F`/`G`/`q`) needs `Trade`, market data / status (`V`/`H`) needs
  `Read`, and there is **no FIX `Admin` row**. The immutable account ↔
  `(SenderCompID, TargetCompID)` binding is checked after credential verify: a
  tuple bound to a *different* account (or an unbound tuple) is rejected at logon
  (`Logout 5`, `SessionBindingViolation`) **before** `Active`; `Account (1)` on an
  order must equal the authenticated account (else a session `Reject 3` — no
  delegation); revocation drops the live session and refuses future logons; the
  venue's `Logon` ack is hand-built **without** `553` / `554` (no credential ever
  emitted, and none logged or echoed in a `Text 58`). The account-keyed store
  (keyed on `(account_id, comp_id_tuple)`, the #029 swap-store shape) persists the
  counters, a bounded resend log, and a `SequenceReset` session-event audit trail
  so a reconnect resumes numbering and a `ResetSeqNumFlag=Y` / `SequenceReset (4)`
  resets **only within the bound account**. The #037 dispatch-unboundedness
  obligation is closed: the acceptor now **races each dispatch against the
  shutdown signal and a hard max-dispatch timeout** and delivers a periodic
  `on_tick` for the negotiated heartbeat / logon-timeout / per-tick revocation
  drop.
- **The FIX 4.4 TCP acceptor — the raw-TCP accept loop, per-connection
  lifecycle, and DoS controls over IronFix's `FixCodec`** (#37) in the new
  `src/gateway/fix/acceptor.rs`, the `[fix]` config section
  (`src/config.rs`), the `main.rs` gateway wiring, and `tests/fix_acceptor.rs`
  ([037](milestones/v0.4-fix-gateway/037-fix-tcp-acceptor-codec.md),
  [03 §5 / §5.1](docs/03-protocol-surfaces.md#5-fix-44-gateway-new),
  [ADR-0002](docs/adr/0002-fix-4-4-gateway-on-ironfix.md),
  [08 §4 / §5](docs/08-threat-model.md#4-untrusted-input-hardening)). IronFix
  ships **no** acceptor — `ironfix-transport` has the `FixCodec` framing codec
  only, no TCP listener — so the accept loop, the per-connection tokio task, and
  the connection lifecycle are new venue work. **The pipe**: `TcpListener::accept`
  → one task per connection → the read half is framed by a venue
  `BoundedFrameDecoder` (around `FixCodec`) and each complete frame decodes into
  the #036 typed messages at a **dispatch seam** (`FixSession` / `FixSessionFactory`)
  where the #038 session FSM plugs in; the seam callbacks are **async** (RPITIT
  `impl Future + Send`, no `async-trait` box) so #038's Argon2id logon verify and
  registry lookup never block a tokio worker. #037 ships a logging stub reaching
  the venue through `Arc<AppState>`. Reply frames flow through a **bounded**
  outbound `mpsc` a dedicated writer task drains. **The DoS controls, wired as
  security controls from the first commit** ([08 §5](docs/08-threat-model.md#5-denial-of-service-posture)):
  a venue **connection cap** (the N+1th connection is refused, not queued); a
  **bounded per-session mailbox** (a full mailbox latches a typed busy and closes
  the session, never an unbounded queue); a **max-frame-length** cap enforced at
  the framing boundary; a **read-idle timeout** (`[fix] idle_timeout_secs`,
  default `30`s) closing a silent connection so a Slowloris of idle sockets cannot
  pin the connection cap (connection hygiene this issue owns; refined by #038's
  negotiated heartbeat); and a boot-validated ceiling on the
  `connection_cap × max_frame_bytes` **product** (`<= 1 GiB` aggregate). **Two
  `ironfix` unchecked-arithmetic panics are closed venue-side** at the framing
  boundary (both the framing-layer analogue of #036's `validate_body_length`, both
  no-panic in debug **and** release): the `BoundedFrameDecoder` pre-checks the
  declared `BodyLength (9)` against `max_frame_bytes` before `FixCodec`'s unchecked
  `... + body_length + 7` add, **and** pre-validates a complete frame's
  `CheckSum (10)` as a **magnitude `<= 255`** (the u8 domain of a real `sum % 256`
  checksum) before `ironfix-tagvalue`'s `parse_checksum` folds `d0*100 + d1*10 + d2`
  into a `u8` — that fold overflows for the **whole `256..=999` band**, not just a
  hundreds digit `>= 3`, so the guard parses the three digits with widening `u16`
  arithmetic (it cannot itself overflow) and rejects any value `> 255`, boundary-exact
  (`255` still flows to `FixCodec`'s real sum check, `256` is rejected). No panic on
  the entire `000..=999` checksum input space. Both guards surface a typed
  `FrameError` and close the session with no unbounded allocation. (A follow-up
  contract gap is documented at the `FixSession` seam for #038: the acceptor does not
  yet bound/cancel an in-flight async `on_message`/`on_decode_error` — harmless with
  the instant #037 stub, but #038's Argon2id-verifying dispatch must be bounded so a
  stall cannot hold a slot past the idle timeout.) **Graceful shutdown** over a
  `watch` signal drains
  the in-flight sessions with no task leak (a live-session gauge witnesses churn
  stays flat). `tracing` spans/events carry the peer address and message-**type** /
  error-**kind** only — **never** a frame payload or a decoded field (a `Logon`
  `Password (554)` is never logged; asserted by a captured-subscriber test). **The
  `[fix]` config section** (`enabled` / `connection_cap` / `mailbox_depth` /
  `max_frame_bytes` / `idle_timeout_secs`, on `deny_unknown_fields`) has its
  DoS-control ranges **and their aggregate product validated at boot** (a typed
  `ConfigError::BadFixValue` on an out-of-range or over-aggregate value); the
  gateway is **disabled by default** (opt-in until the session FSM #038, order
  routing #039, and market data #040 land) and `main.rs` spawns it only when
  enabled. **New dependencies** (all published, `cargo audit` + `cargo
  deny` green): `ironfix-transport` `0.3` (the `FixCodec` framing codec — the one
  new crate edge; its whole dependency tree was already resolved), and the
  promotion of the already-in-tree `tokio-util` `0.7` (`codec` feature, the
  `Decoder` trait) and `bytes` `1` (the framing buffer) from transitive to direct
  (zero new resolved versions), plus the tokio `io-util` feature for the manual
  socket read/write loop (no `futures-util` `Framed` dependency). The session FSM,
  logon auth, sequence store, order routing, and market data build on this seam
  (#038–#040).
- **The FIX 4.4 typed message vocabulary and the pinned `fauxchange.fix44.v1`
  dialect — the first v0.4 (FIX gateway) work** (#36) in the new
  `src/gateway/fix/` module tree, `tests/golden/fix/`, `tests/golden_fix.rs`,
  `tests/property.rs`, and `Cargo.toml`
  ([036](milestones/v0.4-fix-gateway/036-fix-typed-messages-dialect.md),
  [fix-dialect.md](docs/specs/fix-dialect.md),
  [ADR-0002](docs/adr/0002-fix-4-4-gateway-on-ironfix.md),
  [ADR-0003](docs/adr/0003-money-as-integer-cents.md),
  [ADR-0009](docs/adr/0009-lossless-venue-envelope-outcomes.md)). This is the
  **vocabulary layer only** — hand-written typed structs with encode/decode over
  IronFix primitives; it compiles standalone with tests and is not yet wired into
  a live gateway (the TCP acceptor is #37, the session FSM #38, order-path routing
  #39, market-data wiring #40). **The first new dependencies since v0.2**:
  `ironfix-core`, `ironfix-tagvalue`, and `ironfix-dictionary` `0.3` (verified
  published on crates.io; `cargo audit` + `cargo deny` stay green), consumed for
  the `MsgType`/`CompId`/`SeqNum`/`Timestamp` vocabulary, the zero-copy
  `Decoder`/`Encoder` + checksum framing, and the `Version::Fix44` begin-string
  pin — **not** the transport/session/engine crates (those land with #37/#38).
  **The typed messages**: session admin (`Logon A`, `Logout 5`, `Heartbeat 0`,
  `TestRequest 1`, `ResendRequest 2`, `SequenceReset 4`, `Reject 3`); order entry
  (`NewOrderSingle D`, `OrderCancelRequest F`, `OrderCancelReplaceRequest G`,
  `OrderMassCancelRequest q`, `OrderStatusRequest H`, `ExecutionReport 8`,
  `OrderCancelReject 9`, `OrderMassCancelReport r`); market data (`V`/`W`/`X`/`Y`)
  — each on the standard header/trailer with a golden fixture (tags `9`/`10`
  asserted). **The checked decimal-`Price` ↔ integer-`Cents` seam**
  (`src/gateway/fix/price.rs`): exact-or-reject on the `CENTS_SCALE`=2 currency
  scale **and** the instrument tick, parsed with checked **integer** arithmetic —
  no `f64` anywhere, the decimal exists only as a string at the FIX edge, and
  `44=500.05` ↔ `50005` cents round-trips losslessly (the economic-parity golden
  asserts REST/FIX agree on cents). Requiredness (`R`/`C`/`O`) and closed-set
  enums (`Side`, `OrdType`, `TimeInForce`, `ExecType`/`OrdStatus`,
  `MassCancelRequestType`, `MDEntryType`, `SubscriptionRequestType`,
  `MDUpdateAction`) are enforced exhaustively — a missing `R` tag, a
  mis-conditioned `C` tag, or an unknown enum value is a **typed reject, never a
  silent default**; repeating groups (`NoMDEntryTypes`, `NoRelatedSym`,
  `NoMDEntries`, and the **ordered** `NoAffectedOrders`, ADR-0009) enforce
  delimiter/count rules; and every inbound-byte path returns a typed
  `FixDecodeError` — **no `.unwrap()` on caller bytes**. Composite ids ride
  `OrderID (37)`/`ExecID (17)` and the `underlying_sequence` rides
  `SecondaryExecID (527)`; an unsupported application `MsgType` is classified
  toward `BusinessMessageReject (j)` (the routing is #39). The `Logon`
  `Password (554)` is held in a `Debug`-redacted `SecretField` so it never reaches
  a log. Property tests cover encode∘decode round-trip identity and the
  never-lossy `Price` ↔ `Cents` seam over the input space. **Decode-layer
  hardening** (`src/gateway/fix/limits.rs`): the venue's own `decode()` validates
  `BodyLength (9)` against the actual frame length **before** the ironfix codec —
  closing a reachable worker panic from ironfix-tagvalue 0.3.0's unchecked
  `body_start + body_length` on a hostile oversized `BodyLength` (a PoC regression
  test asserts a typed reject, no panic; an upstream IronFix fix is requested
  separately) — and adds a per-message field ceiling, a per-group entry ceiling, a
  duplicate-scalar-tag rejection (no silent first-wins, `SessionRejectReason=13`),
  and bounded truncation of every stored untrusted field value so no future
  `Text (58)` renderer can echo an unbounded payload.
- **The persistent-mode order-path budget and the WS fan-out N-sweep
  flatness re-verification — the v0.3 performance gate** (#35) in the new
  `benches/hp5_durable_append.rs`, `benches/hp2_ws_fanout.rs`, `Cargo.toml`,
  and `BENCH.md`
  ([035](milestones/v0.3-replay/035-persistent-order-path-budget.md),
  [07 §3](docs/07-performance-budgets.md#3-latency-budgets-design-targets),
  [07 §4](docs/07-performance-budgets.md#4-throughput-scaling-and-isolation-budgets)).
  Extends the #020 `bench-hdr` baseline to the durable journal and closes the
  v0.3 ROADMAP performance gate. **HP-5 — durable PostgreSQL journal append,
  measured for the first time** (`benches/hp5_durable_append.rs`, new
  `[[bench]]` target): the SAME real actor stack HP-1 measures, journal
  swapped from `InMemoryVenueJournal` to the durable `PgVenueJournal` (#029)
  against a REAL ephemeral `postgres:18-alpine` (`testcontainers`, never
  mocked), reusing HP-1's exact `TimingJournal`/`TimingExecutor` pairing
  methodology so the append-only and match-only series stay paired per turn.
  Two real runs (`HP5_WARMUP_OPS=200 HP5_MEASURED_OPS=2000`): durable
  `hp5_command_append`/`hp5_event_append` p50 ~263–293 µs — only ~1.7–1.9×
  the post-`#34` in-memory append at the median (a real Postgres round-trip
  over local Docker loopback is not dramatically slower than the now-`#34`-
  inflated in-memory store at p50); the tail is NOT stable enough across the
  two runs to claim a multiplier (run 1's durable p99.9 is ~2.4× its
  in-memory counterpart, run 2's is ~1/4 of it — disclosed as an open,
  unresolved tail-instability finding, not smoothed over). **The persistent-
  mode composition**: both "in-memory HP-1 + one durable append round-trip"
  (the literal arithmetic composition the acceptance criterion asks for) AND
  a directly MEASURED fused persistent-mode full turn
  (`hp5_persistent_full_turn_closed_loop`, a real actor wired end-to-end with
  the durable journal) are reported side by side — the measured-fused number
  (p50 ~560–602 µs) is the one `BENCH.md` names as the actual persistent-mode
  budget, with the arithmetic decomposition shown only to explain where it
  comes from (match ≈ unchanged, append ≈ dominant), never presented as a
  precise identity. An open-loop, coordinated-omission-corrected section (500
  ops, a SECOND fresh ephemeral container so no rows are shared with the
  closed-loop section) surfaces a disclosed, **unresolved anomaly**: sojourn
  p50 (~2.1–2.2 ms) runs ~3.5–4× the closed-loop full-turn p50 despite zero
  mailbox rejections and a conservative 10 ms intended interval; two
  plausible, unattributed causes are named (connection/pool cold-start on the
  un-warmed fresh container; `block_in_place`'s worker-handoff overhead under
  the open-loop section's concurrent task dispatch) but neither is confirmed
  by a call-stack profiler, and the finding is reported honestly rather than
  resolved by guessing. **The `#34` delta, the milestone's required tracked
  follow-up**: `#34` added `check_record_size` (a full
  `serde_json::to_string` pass, done only to measure byte length) to the
  START of `InMemoryVenueJournal::append` — a genuinely NEW cost the
  in-memory store never paid before (unlike the durable store, which already
  serializes to build its SQL parameter, so its own size check is ~free).
  Re-running `hp1_order_path` at the IDENTICAL `#020` baseline configuration
  (same machine, same seed, same sample sizes) TWICE shows p50 essentially
  unchanged (±1–3%, inside the baseline's own disclosed run-to-run noise) but
  p99/p99.9/p99.99 consistently and substantially worse in BOTH runs, on BOTH
  `hp1_command_append` and `hp1_event_append` (run 1: p99 +33.5%/+34.0%/
  +35.6%; run 2: p99 +60.8%/+61.7%/+62.9%) — a pattern this consistent across
  two quantiles × two call sites × two runs is reported as a real,
  disclosed regression (mechanism named but not individually measured: the
  new per-append allocation plausibly compounds with the pre-existing
  O(journal-depth) linear-scan cost §3.1 already diagnosed, rather than
  merely adding to it), not dismissed as noise. **HP-1's own "sub-millisecond
  at p99" DESIGN TARGET, already only "just inside the ceiling" at the `#020`
  baseline, is now measurably further from being met** — strengthening,
  not creating, the existing `#020` follow-up recommendation (an
  index-backed `(SequenceNumber, RecordKind)` uniqueness check) for
  `matching-expert`/`architect` to evaluate against both costs together.
  **HP-2 WS fan-out N-sweep, re-verified post-`#34`**: `benches/hp2_ws_fanout.rs`
  now prints an explicit flatness verdict against a stated
  `FLATNESS_TOLERANCE_PCT` (±15 percentage points — wide enough to absorb the
  `#020` baseline's own disclosed ~13% p99 run-to-run noise, narrow enough to
  catch a genuine O(N) regression) rather than leaving "flat" as an
  unstated eyeball judgment; the re-run (N ∈ {1, 10, 100, 1 000}) reports
  `PASS` — worst observed |p99 delta| 3.7%, well inside tolerance, confirming
  docs/07 §4's DESIGN TARGET still holds after `#34`'s added per-append cost
  (which raises HP-2's absolute p99 baseline but does not break the N-flatness
  the sweep exists to check, since the added cost is identical across all
  four N columns). No CI `bench-regression` gate is armed by this change
  (deliberately out of scope — that is #053, v1.0); `cargo clippy
  --all-targets --all-features -- -D warnings` continues to be the only CI
  check on the bench suite, now covering the new `hp5_durable_append`
  target too. No `src/` change — a pure measurement/benchmark-harness issue;
  `src/db/journal.rs` and `src/exchange/journal.rs` are read, not modified.
- **Adversarial fixture corpus + bounded deserialiser for the journal / replay /
  seed-bundle decode surface — the v0.3 security gate** (#34,
  [034](milestones/v0.3-replay/034-journal-replay-adversarial-fixtures.md),
  [08 §4](docs/08-threat-model.md#4-untrusted-input-hardening),
  [08 §6](docs/08-threat-model.md#6-fuzzing-and-adversarial-testing),
  [TESTING.md §14](docs/TESTING.md#14-security-testing)). A committed corpus under
  `tests/adversarial/` (`journal/` records, `bundle/` scenario bundles, `seed/`
  manifests) is fed to the **real** deserialisers by `tests/adversarial.rs`, each
  asserting the **specific** typed reject (`JournalError` / `ReplayError` /
  `ConfigError` — never a blanket `is_err()`) with **no panic**, **no unbounded
  allocation**, and **no partial apply** (a hostile stream discards the whole
  replay — no `ReplayReport`, hence no reconstructed stores). Classes: oversized,
  truncated, field/tag injection, duplicate/missing fields, out-of-range economic
  fields (negative cents, overflow quantity), malformed symbols, a hostile bundle
  manifest, a newer bundle/journal `schema_version` (refused), and a tampered stored
  event → `JournalCorruption { underlying, sequence }` (the integrity oracle) driven
  from tampered bytes on disk. The corpus is committed as files so it seeds the
  coverage-guided `cargo fuzz` targets in v1.0 (#52).
- **A bounded, no-unbounded-allocation deserialiser with a write ≤ read ceiling
  symmetry invariant** (#34) closing an unbounded-`fetch_all` OOM-on-restart vector
  in #29's shipped read path, found during #34's threat-surface mapping. New named
  ceilings with typed rejects (`JournalError::ResourceLimit` /
  `ReplayError::ResourceLimit`):
  - a **per-record byte** ceiling (`MAX_JOURNAL_RECORD_BYTES` = **2 MiB**, a
    fill-aware value: an event's size ≈ `fills × ~230 B`, so 2 MiB clears
    ~9,000 fill legs ≈ one order crossing ~4,500 resting orders — ~25× a heavy
    ~180-order sweep) enforced **symmetrically at write and read on both stores**.
    **The load-bearing property:** because write and read use the *same* constant,
    no record the venue durably writes can trip the read `record_bytes` refusal — so
    the read refusal fires only on **external tampering / a hostile bundle**, and an
    over-ceiling event **seals the underlying loudly at write time** (the existing
    post-mutation seal, ADR-0006 §3) instead of being written and then permanently
    bricking recovery/replay/export of that stream. (An earlier draft enforced the
    read ceiling at 64 KiB with **no** write check — a legitimate heavy sweep would
    write fine and then brick replay forever; the symmetry + fill-aware value fix
    that.)
  - a **per-read record-count** ceiling (`MAX_JOURNAL_RECORDS` = 1,000,000) **and** a
    **per-read total-byte** ceiling (`MAX_JOURNAL_STREAM_BYTES` = 1 GiB), enforced by
    a cheap `count(*)` + `sum(octet_length(payload))` **pre-fetch bounding query**
    that refuses an over-budget stream **before** `fetch_all` allocates (closing the
    compounded gap where the fetch buffered rows before any check ran); a `LIMIT` +
    per-row `octet_length` `CASE` (an over-ceiling payload is returned as NULL, so
    the blob never leaves Postgres) remain as defense-in-depth. An over-budget stream
    is **refused** (never truncated into a silent partial recovery); paging/streaming
    is the documented forward seam.
  - a **total on-disk-bundle byte** ceiling (`MAX_BUNDLE_BYTES` = 64 MiB) enforced
    before `serde_json` parses an on-disk bundle (the on-disk path has no 1 MiB
    REST-body transport cap).
- **The determinism guarantee, written down and made enforceable — the v0.3
  capstone** (#33) as documentation + a consolidated test-oracle, no new runtime
  behaviour ([033](milestones/v0.3-replay/033-determinism-guarantee-oracle.md),
  [02 §5](docs/02-matching-architecture.md#5-determinism),
  [04 §6](docs/04-market-data-and-replay.md#6-determinism-and-seeding),
  [ADR-0004](docs/adr/0004-deterministic-replay-with-seeded-clock.md)). The
  canonical guarantee is finalised against the **shipped** replay driver (#30) and
  the `ExpirationDate::DateTime` guard (#32): given the same journal (the `venue.v1`
  `VenueEvent` stream incl. `MarketMakerControl` / `Clock` / `SimStep`), the same
  config manifest (seed, clock mode, microstructure config, instrument seed), and
  the same crate/dependency versions, a replay reproduces identical fills, events,
  and resting book state **per underlying**, judged by ordered `VenueEvent`-stream
  equality per underlying (top-of-book a cheap witness). It now lives in the
  **tracked** crate docs (`src/lib.rs` → regenerated `README.md` via
  `make readme`) as the shipped claim, alongside the local canonical docs. The
  **documented exclusions** are enumerated consistently and each is asserted *as an
  exclusion*: mark price / unrealised P&L / Greeks / derived analytic floats
  (recomputed live); process-global numeric registry ids (canonical symbol string
  is the identity); the engine clock + its `Uuid::new_v4()` trade-id namespace;
  cross-underlying interleaving (no venue-wide total order); out-of-sequencer state
  (an admin snapshot restore starts a new journal lineage — not a replay input; a
  restore-boundary journal fail-stops under the single-epoch driver); and OHLC bars
  (by derivation — same fills ⇒ same bars). The already-established honesty threads
  are folded in so the list matches code, not aspiration: the price walk is
  journal-driven, **not** seed-regenerated (`optionstratlib`'s sampler is
  unseedable); `Day`/`GTD` TIF admission determinism is deferred to the upstream
  leaf-clock seam (the `#[ignore]`d ready-to-enable test); the two annotated `Days`
  carve-outs at the clock-free kernel seams (#32); and boot-time resume of a
  non-empty durable journal is the reducer (#29/#30) plus not-yet-built wiring
  (tracked in #85). The flagship `tests/determinism.rs` module doc is now the
  **oracle's index** (each guarantee clause and each exclusion names the test that
  enforces it); new cases assert the two remaining exclusions — out-of-sequencer
  state is not a replay input (a restore boundary fail-stops through the shipped
  #30 driver) and **seed isolation** for the venue-owned derivation (`seed →
  LineageId → id namespace`: same seed reproduces the id namespace, distinct seeds
  never collide) — while the price-walk reproduction is asserted journal-driven and
  the v0.5 RNG sub-streams (persona jitter, latency) are noted forward-scoped, not
  fabricated. A stepped session advancing identically for the same config is wired
  into the oracle.
- **`ExpirationDate::DateTime` everywhere + a permanent replay-stability guard**
  (#32) — every *stored / journaled* expiry is an absolute `ExpirationDate::DateTime`
  (an instant), never a wall-clock-relative `ExpirationDate::Days` (which re-resolves
  against "now" on replay and maps to a different calendar date), closing a binding
  determinism invariant ([04 §6](docs/04-market-data-and-replay.md#6-determinism-and-seeding),
  [02 §5.4](docs/02-matching-architecture.md#5-determinism),
  [ADR-0004](docs/adr/0004-deterministic-replay-with-seeded-clock.md)). The venue
  refuses a relative expiry as a **typed rejection** at every construction boundary
  — `ConfigError` at config / seed-manifest load (`parse_seed_expiry` →
  `validate_venue_expiry`), `VenueError::InvalidOrder` (HTTP 400) at the REST/WS DTO
  boundary (the `YYYYMMDD` symbol grammar cannot express a `Days`), and a typed
  `SimError::ChainSynthesisFailed` at stepped-session synthesis (a new
  `validate_venue_expiry` guard making the `DateTime`-only guarantee explicit rather
  than incidental to `parse_yyyymmdd`) — so a relative expiry is never silently
  re-resolved. A new recursive, non-vacuous grep guard
  (`test_no_days_relative_expiry_survives_anywhere_in_the_venue`) scans the **whole
  `src/`** and asserts **no `ExpirationDate::Days` construction survives**, exempting
  only comment prose and an **explicit, enumerated `days-expiry-allow` allow-list** of
  the four legitimate uses (each annotated in-place with its upstream evidence): the
  market-maker pricer's clock-free kernel argument, the price-walk's `Step` x-axis
  nominal, the engine's defensive read-arm, and the symbol match-to-reject arms.
  **The pattern (the wording #33's guarantee doc inherits):** the venue's stored /
  journaled / identity expiry form is *always* `ExpirationDate::DateTime`; at the two
  clock-free kernel seams the venue converts `DateTime − venue_now` → a `Days`-valued
  duration (off the #28 venue clock) and the kernel never reads wall-clock — because
  `Days(_).get_years()` is `days / DAYS_IN_A_YEAR` (pure) whereas
  `DateTime(_).get_years()` / `get_days()` read `Utc::now()` (verified pinned:
  `expiration_date` 0.2.1 `convert.rs:26,65,83`; there is no public
  `get_years_from(base)`, so `Days` is the *only* clock-free vehicle). **Honest note
  on the acceptance-criterion letter:** the AC says "no `Days` in simulation", but the
  price walk's nominal MUST stay `Days` — a `DateTime` there would call optionstratlib
  `Xstep::new` → `get_days()` → `Utc::now()` and *reintroduce* the wall-clock read this
  issue removes; the **spirit is preserved** (no wall-clock re-resolution anywhere;
  `Days` survives only as the clock-free duration vehicle at two annotated kernel
  seams, never as a stored expiry). A new **`[expiry_lifecycle]`** config section adds
  the **operational** expiry / settlement times-of-day (`OperationalTime`, `HH:MM:SS`
  UTC, defaults `08:00:00` / `08:30:00`; dependency-free, no `chrono` added), kept
  **distinct** from the `23:59:59 UTC` symbol-identity instant and validated at load
  (`settlement >= expiry`, both strictly before `23:59:59`) with typed `ConfigError`s
  naming the offending combination — an invalid time combination is a startup failure.
  The section is `deny_unknown_fields` and file-only; it is *validated now* but the
  lifecycle scheduler that **consumes** these times lands in v0.5 (the milestone's
  explicit out-of-scope). Options math (pricing, Greeks) continues through
  `optionstratlib` — never hand-rolled — over the venue-resolved time-to-expiry.
  Exercised by `tests/determinism.rs` (a `DateTime` fixture replays identically; a
  `Days`-relative expiry is rejected at load; the venue-wide grep guard),
  `tests/market_maker.rs`
  (`test_requote_prices_are_identical_under_a_fixed_venue_clock` — two requotes at the
  same `venue_now_ms` produce byte-identical limit prices despite wall-clock advance),
  the DTO-boundary rejection unit tests in `src/error.rs` / `src/gateway/rest/support.rs`,
  and the `[expiry_lifecycle]` validation tests in `src/config.rs` (defaults valid;
  settlement-before-expiry / at-identity / malformed / unknown-key rejected)
  ([032](milestones/v0.3-replay/032-expiration-datetime-everywhere.md)).
- **Stepped synthetic sessions — the OptionChain-Simulator model absorbed as code,
  not a dependency** (#31) in the new `src/simulation/session.rs` (`SessionConfig`,
  `synthesize_chain`, `SynthesizedChain`), a per-instrument implied-volatility
  override on the market maker (`register_instrument_with_iv`), and the
  `AppState::materialize_session` / `AppState::step_session` wiring
  ([031](milestones/v0.3-replay/031-stepped-synthetic-sessions.md),
  [04 §3](docs/04-market-data-and-replay.md#3-chain-synthesis-and-stepped-sessions),
  [ADR-0001](docs/adr/0001-consolidate-option-chain-orderbook-backend.md)). A
  session carries per-scenario generation parameters — initial price,
  days-to-expiration, volatility, risk-free rate, dividend yield, walk method,
  strike interval, chain size, `smile_curve` (+ `skew_slope`), and `spread`. Chain
  synthesis deterministically materialises the `expirations × strikes` instrument
  grid with the per-strike implied volatility shaped by the smile **through
  `optionstratlib`** (`chains::utils::adjust_volatility`,
  `σ(K) = σ_atm · clamp(1 + skew·m + smile·m²)`, `m = ln(K/S)` — a positive
  `smile_curve` raises the wing IVs), never a hand-rolled surface; each leaf is
  seeded with a smile-shaped market-maker quote (its IV threaded into the maker's
  journaled quotes), so from that point the venue is **live** and client orders
  match the synthetic liquidity. Every synthesised expiry is an absolute
  `ExpirationDate::DateTime` derived by pure civil-date arithmetic (never
  `Utc::now()`, never `Days`), so a session is replay-stable. A session step
  advances the stepped venue clock by its fixed virtual interval (a journaled
  `Clock` command fanned per underlying under one `correlation_id`) and walks the
  underlying one price step (a journaled `SimStep`, driving the maker's journaled
  `AddOrder` requotes) — so replay reproduces the session from the journal with the
  live requote engine muted (#030), and no cascading duplicate orders are
  generated. Same-seed determinism is scoped **honestly**: the chain *synthesis*
  (grid + smile IVs + expiry) is a pure deterministic function of the config, while
  the stochastic *price path* is reproduced from the **journal** (the
  `optionstratlib` walk sampler owns its own RNG and cannot consume the seed), not
  seed-regenerated. The retired service's `/api/v1/chain` session-lifecycle REST
  surface and its ClickHouse / Redis / MongoDB stack are **not** carried over (zero
  new dependencies). Exercised by unit tests (`test_chain_synthesis_materialises_grid`,
  `test_smile_curve_shapes_smile`, parameter validation), `tests/determinism.rs`
  (deterministic synthesis + the smile reshaping with the curve), and
  `tests/integration.rs` (synthesise → materialise → step → replay via the #030
  driver identically; a client order matches the synthetic liquidity and fills).
- **The replay driver — reproduce identical events, fills, and top-of-book from
  the durable journal** (#30) in the new `src/simulation/replay.rs`
  (`replay_streams` / `replay_bundle`, `ScenarioBundle` / `JournalStream`,
  `ReplayReport`, `ReplayError`, `RecordingController`) with the record/replay
  control plane in `src/gateway/rest/replay.rs` + the WS `record` / `replay_bundle`
  actions ([030](milestones/v0.3-replay/030-replay-driver.md),
  [04 §4](docs/04-market-data-and-replay.md#4-historical-replay),
  [ADR-0004](docs/adr/0004-deterministic-replay-with-seeded-clock.md),
  [ADR-0006](docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
  Replay and recovery share **one algorithm — re-execution**: the driver reuses
  the #29 `exchange::recover` core **verbatim** (never a second "apply the stored
  event" path), re-executing every `VenueCommand` in `underlying_sequence` order
  into a **fresh** `InstrumentRegistry` per underlying with the stored `VenueEvent`
  as the integrity oracle — a mismatch halts with `JournalCorruption { underlying,
  sequence }`, a newer-than-binary envelope schema is refused. Oracle equality is
  stated over **symbols + `underlying_sequence`** (never process-global registry
  ids); mark prices and unrealised P&L are recomputed **live** and excluded. Two
  input formats: the **native journal** and a portable **scenario bundle** (journal
  streams + the `RunManifest`, now extended with `instrument_seed`, a microstructure
  fingerprint, and the **pinned crate/dependency versions** — real compile-time
  values, all `#[serde(default)]` so an older manifest stays backward-readable);
  `replay_bundle` verifies the bundle schema + the manifest's pinned versions
  against the running binary first (a typed `VersionMismatch`), and a bundle
  without a manifest / a malformed bundle is a typed decode error, never a panic.
  Reproduction is **journal-driven, not seed-regenerated**: the live requote engine
  is muted (the offline driver never invokes it, so journaled market-maker
  `AddOrder`s replay with no cascade), and journaled non-order inputs
  (`EvictExpiredOrders { now_ms }`, `SetInstrumentStatus`, `Clock` / `SimStep`
  values) are applied **from the command**, never a replay clock. The **executions
  store** and **positions fold** are reconstructed from the same replayed events
  through the live post-journal `StoreFanOut`. The record/replay controls are
  `Admin`-gated with REST ≡ WS **control parity** (both surfaces flip the same
  `RecordingController` / run the same offline replay), and there is **no** FIX
  control surface. Scope is honestly **single-epoch**: a journal crossing a
  snapshot-restore boundary **fails stop** at the first post-restore command
  (the restore boundary is outside the determinism oracle), and reconstructing a
  restored cut plus **boot-time resume** of a non-empty durable journal into a live
  venue are tracked in #85. Exercised across unit, property
  (`journal_driver_replay_reconstructs_book`), the flagship `tests/determinism.rs`
  (same journal → same fills; exclusions tested as exclusions; multi-underlying
  partial control fan-out reproduced per underlying; replay-stable `DateTime`
  expiries; the restore-boundary fail-stop), `tests/parity.rs` (control + observation
  parity), and a `testcontainers` `postgres:18-alpine` integration test (record into
  a durable venue → export the bundle → replay offline → reconstructed executions +
  positions fold match the live goldens).
- **Durable persistence for the venue envelope journal — swap the store, keep
  the contract** (#29) in the new `src/db/journal.rs` (`PgVenueJournal`) and
  `src/exchange/recovery.rs` (`recover` / `Recovered`, the production recovery
  reducer), behind the unchanged `VenueJournal` trait
  ([029](milestones/v0.3-replay/029-persist-envelope-journal.md),
  [02 §6](docs/02-matching-architecture.md#6-the-journal),
  [ADR-0006](docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
  The same `venue.v1` envelope the per-underlying actor already journaled
  in-memory is now durably persisted when `DATABASE_URL` is set (in-memory
  otherwise); the receipt / recovery / durability contract is **identical** —
  only the store changed. A new migration (`migrations/20260716120500_journal.sql`)
  adds a per-underlying `journal_headers` row (`lineage_id` + `schema_version`)
  and an append-only `journal_records` stream keyed **unique** on
  `(underlying, underlying_sequence, kind)`, storing each record's exact
  `venue.v1` bytes as `TEXT` (so a JSONB key-reorder can never mutate the
  oracle); an idempotent re-append is `ON CONFLICT DO NOTHING` + an O(1)
  read-back (identical payload → no-op, differing → typed `Conflict`). The
  write-ahead protocol is preserved on the durable store (confirmed pre-execute
  failure reuses `N`; an ambiguous append is resolved by durable tail read-back;
  a post-mutation event-append failure seals the underlying with
  `JournalUnavailable`). Recovery is **re-execution with the stored event as the
  oracle** (no event-applier): it reads the header first, refuses a
  newer-than-binary journal with the typed `JournalError::SchemaTooNew`, and
  halts on a divergent stored event with `JournalError::Corruption { underlying,
  sequence }`. `sqlx::Error` is mapped to typed domain errors at the boundary
  (never on a `pub` surface, never leaking `DATABASE_URL`); all queries are
  parameterised with the offline `.sqlx` cache committed. The boot-time replay
  **driver** (reload a snapshot and re-execute into a running venue) remains #30;
  until then a fresh boot persists forward and the unique key makes an accidental
  resume of a non-empty journal fail loud. Exercised by `testcontainers`
  `postgres:18-alpine` integration tests (recovery-by-re-execution,
  sequence-continuity across a snapshot epoch, idempotent re-append,
  newer-schema refusal), now wired into the CI `migrations` job.
- **The clock as a seeded venue service (realtime / accelerated / stepped) — the
  first v0.3 replay seam** (#28) in the new `src/simulation/clock.rs` (`SimClock`,
  `VenueClockConfig`, `ClockMode`, `CorrelationId`) and `src/simulation/manifest.rs`
  (`RunManifest` recording `seed` + `clock_mode`)
  ([028](milestones/v0.3-replay/028-clock-venue-service.md),
  [04 §5](docs/04-market-data-and-replay.md#5-clock-control),
  [ADR-0004](docs/adr/0004-deterministic-replay-with-seeded-clock.md)). Time is a
  venue service, not `SystemTime`: `SimClock` is the **one** clock the venue reads
  — the per-underlying actors stamp `venue_ts` from it, the price-walk cadence
  stamps its `SimStep.now_ms` from it, and the auth rate limiter reads it (so
  rate-limit decisions replay deterministically); `now_ms()` on the sequenced path
  is a pure atomic load with no wall-clock read (guarded by
  `tests/determinism.rs::test_no_wall_clock_read_on_the_sequenced_path`). A
  stepped advance is a per-underlying **sequenced, journaled** `Clock { now_ms }`
  command fanned to every actor by the `AppState` venue-control coordinator
  (`advance_clock_step` / `advance_clock_to`, returning a `ClockAdvance` that
  surfaces a partial fan-out), so replay reproduces the advance from the journaled
  value — never a replay clock. The `[clock]` config section gains the file-only
  `multiplier` (accelerated) and `step_interval_ms` (stepped) knobs
  (`deny_unknown_fields` preserved). **Named upstream limitation (documented, not
  silent):** `orderbook-rs` 0.10.5 exposes `OrderBook::with_clock` /
  `Arc<dyn Clock>`, but the pinned `option-chain-orderbook` 0.7.0 does not thread
  it through lazy `get_or_create_*` leaf construction, so deterministic `Day`/`GTD`
  time-in-force **admission** at the leaf is deferred to that upstream work — the
  cross-run check exists today as the clearly-labeled `#[ignore]`d
  `test_day_gtd_admission_determinism_blocked_by_leaf_clock_gap`, and the intraday
  `EvictExpiredOrders` sweep stays a journaled no-op
  ([02 §5.5b](docs/02-matching-architecture.md#5-determinism)).
- **The Docker e2e smoke test and the cold-bring-up wall-clock budget — the
  v0.2 "one command" proof** (#27) in the new `tests/docker_smoke.rs`,
  `.github/workflows/ci.yml`, `Makefile`, and `BENCH.md`
  ([027](milestones/v0.2-packaging/027-cold-bringup-e2e-smoke.md),
  [TESTING.md §9](docs/TESTING.md#9-docker-e2e-smoke),
  [07 §7](docs/07-performance-budgets.md#7-what-is-explicitly-out-of-budget),
  [PRD NFR-3](docs/PRD.md#4-non-functional-requirements)). `DOCKER=1`-gated
  (a plain `cargo test` self-skips cleanly — verified, the default suite stays
  green with no Docker present): `docker compose -f docker/docker-compose.yml
  up -d` (image built once, untimed, ahead of the measured window — cold start
  is container-start + self-seed → serving, never `cargo build`) → the first
  successful `GET /health` `200` (the REST listener binds only AFTER the
  bounded seeding phase completes and `AppState::begin_serving()` flips,
  `src/main.rs`, so a live `/health` alone IS the serving-and-seeded signal) →
  mint a bootstrap token for the seeded `market-taker` account
  (`seeds/default.toml`) → place one market order over REST against the seeded
  at-the-money `BTC-20261231-50000-C` contract → observe the resulting fill
  over the SAME `fills` WS channel (subscribed BEFORE the order is placed, so
  the broadcast is never raced) → assert no panic in the container logs and a
  clean `docker compose down`. A `ComposeGuard` runs `docker compose down -v`
  in its `Drop` impl — proven to run on every exit path (`Ok`, `Err`, a panic
  unwind, or the outer `tokio::time::timeout` dropping the in-flight future) —
  so a failed run never leaks containers/volumes; the dedicated CI
  `docker-smoke` stage additionally runs an unconditional teardown step as a
  belt-and-braces safety net. **Verified against the real container, twice**
  (Docker 29.6.1, `fauxchange:local` 187 MB `runtime-slim`): cold bring-up
  measured **0.556 s** (image freshly built in the same invocation) and
  **0.483 s** (image already cached) — both real numbers, ~14× under the
  30 s DESIGN TARGET budget on a DB-less default local run (`BENCH.md` §8;
  never a fabricated figure, and explicitly not claimed to generalise to a
  `--profile persistent` cold start, a larger seed manifest, or a slower CI
  disk). **The persistent-mode durable journal append is now an explicit,
  binding separate budget line** (`BENCH.md` §7's HP-5 bullet), never folded
  into HP-1's in-memory sub-millisecond target — the durable quantiles
  themselves still land with the durable journal in v0.3 (#035); #027 only
  establishes and documents the separation. New CI stage `docker-smoke`
  (`needs: [image-build]`, `DOCKER=1`, NOT on the plain `test` job) builds the
  compose image, runs the test, and adds a belt-and-braces teardown step; new
  `make docker-smoke` target mirrors it locally. Two new dev-only
  dependencies, each a **zero-new-resolved-version** addition (verified
  against `Cargo.lock` before and after: no new `[[package]]` entry, only two
  new dependency edges from the `fauxchange` package itself) — `tokio-tungstenite`
  0.29 (the black-box WS client observing the fill over the real running
  container; already resolved transitively via `axum` 0.8.9's own `ws`
  feature) and `futures-util` 0.3 (the `StreamExt`/`SinkExt` traits
  `WebSocketStream` needs; already resolved transitively via `axum`/`tower`) —
  both dev-only, never reach the shipped image, already `cargo audit`/`cargo
  deny`-gated as existing transitive deps. The REST leg (health poll, token
  mint, order placement) deliberately adds **no** client dependency instead: a
  small hand-rolled HTTP/1.1 JSON client over the already-present
  `tokio::net::TcpStream` (`Connection: close`, read-to-EOF, no
  `Content-Length`/chunked parser needed) covers the three simple JSON calls,
  avoiding `hyper-util`'s client machinery (also already in the tree, but only
  for `axum`'s own server-side usage today). The stale `axum` `ws`-feature
  audit note ("no CLIENT WebSocket crate is added") is corrected to describe
  this new client honestly rather than left contradicting it.
- **Container hardening — non-root, a distroless variant, a no-baked-secrets
  scan, loopback metrics, read-only/dropped-caps run posture, and the
  supply-chain gate on the image build** (#26)
  ([026](milestones/v0.2-packaging/026-container-hardening.md),
  [08 §7](docs/08-threat-model.md#7-secrets-handling),
  [08 §8](docs/08-threat-model.md#8-supply-chain-controls),
  [08 §9](docs/08-threat-model.md#9-container-hardening-deployment),
  [06 §12](docs/06-deployment.md#12-container-hardening-v02-26)). Hardens the
  #25 working image without reshaping it. `docker/Dockerfile` now builds TWO
  runtime targets off the SAME `builder` stage: `runtime-slim` (unchanged
  default — last stage in the file, so a plain `docker build` / `docker
  compose build` still resolves here; `debian:bookworm-slim` + `curl`
  `HEALTHCHECK`) and the new `runtime-distroless`
  (`gcr.io/distroless/cc-debian12:nonroot`, **pinned by digest**, verified
  against the manifest-LIST digest so amd64/arm64 both resolve correctly; no
  shell, no package manager — `cc-debian12` was chosen because it ships
  exactly the glibc deps the release binary needs, verified via `ldd`:
  `libgcc_s.so.1`/`libm.so.6`/`libc.so.6`; no `HEALTHCHECK` on this target —
  there is no shell/curl to run one from inside the container, an honest
  tradeoff documented in the Dockerfile, not an oversight — `runtime-slim`
  stays the default so the one-command distribution keeps a working
  healthcheck). Both targets run as a fixed **uid/gid 65532** (the
  conventional distroless "nonroot" id, used on BOTH targets for one
  consistent PodSecurityContext / compose `user:` value regardless of base
  image) — verified with real `docker build --target
  runtime-slim`/`runtime-distroless` + `docker run --entrypoint id` /
  `docker inspect --format '{{.Config.User}}'`; both boot, self-seed, and
  serve `GET /health` (`200`), and an `exec sh` into `runtime-distroless`
  fails as expected (no shell). Measured local image sizes: `runtime-slim`
  187 MB, `runtime-distroless` 76.4 MB. New
  `docker/scan-image-secrets.sh` — the no-baked-secrets gate: scans ONLY the
  layer(s) carrying fauxchange's own `COPY` targets (the compiled binary, the
  baked `seeds/default.toml`) for an unrecognised PRIVATE KEY block (pinned
  by SHA-256 against the ONE known, reviewed `JwtAuth::dev()` fixture,
  src/auth.rs — a real leaked key still fails), a credentialed
  `postgres(ql)://user:pass@...` connection string, an
  `AUTH_BOOTSTRAP_SECRET=value` assignment, and any `fix_password` other than
  the documented dev fixture (`dev-taker-secret-change-me`); deliberately
  scoped away from the upstream `debian:bookworm-slim` base image after
  verifying locally that an unscoped scan trips on GnuPG's OWN internal
  test-key fixtures (`gpgv`/`libgcrypt`, compiled in for `apt` package-
  signature verification) — Debian's supply chain, not a fauxchange-baked
  secret. Verified against a deliberately "dirty" test image (a smuggled real
  private key + a substituted `fix_password` in a separate `COPY` layer) to
  confirm the scan actually fails on a real finding, not just passes on a
  clean one; run in CI (`image-build` job) against both runtime targets, and
  locally via `docker/scan-image-secrets.sh <image-ref>`. The dev-keys
  release gate (`JwtAuth::release_gated`, shipped in #011) already refused
  `JwtAuth::dev()` keys without `--dev`/`FAUXCHANGE_DEV`; its named
  acceptance test `test_auth_refuses_dev_keys_without_flag` did not yet exist
  under that exact name (a functionally-identical test existed as
  `test_dev_key_release_gate_refuses_without_dev_mode`) — renamed to the
  milestone-specified name with a doc comment cross-referencing
  `docker/scan-image-secrets.sh` as the content-layer backstop on the same
  control. `:9090`'s loopback-only compose binding (already true since #25)
  now has a CI assertion (`docker compose config --format json | jq`,
  `image-build` job) so a future metrics server inherits it by construction.
  `docker-compose.yml`'s `fauxchange` service gains `read_only: true`,
  `cap_drop: [ALL]`, `security_opt: [no-new-privileges:true]` (an explicit
  `target: runtime-slim` was also added to its `build:` block, defensive
  against a future Dockerfile stage reorder) — verified locally
  (`docker run --read-only --cap-drop=ALL --security-opt
  no-new-privileges:true`, both targets) that the venue still serves
  `/health` with **zero `tmpfs` mounts**: it needed no writable path at all
  (fully in-memory, `tracing` to stdout only) — an honest finding, not a
  gap papered over with a defensive mount nothing exercises. The `postgres`
  service itself is NOT hardened here (out of scope; its data-directory /
  Unix-socket writable paths make read-only-rootfs a separate, non-trivial
  change). The `image-build` CI job now `needs: [cargo-audit, cargo-deny]` —
  a new advisory or a policy violation fails BEFORE either runtime image is
  built, wiring the existing #19 supply-chain gate onto the image build
  itself, not just the crate. No new Rust dependency (the scan script is
  bash + the runner's preinstalled `jq`/`python3`, matching #25's "no extra
  action to pin for a plain `docker build`" precedent).
- **Multi-stage `docker/Dockerfile` and the `docker/docker-compose.yml`
  one-command topology** (#25)
  ([025](milestones/v0.2-packaging/025-dockerfile-compose.md),
  [06 §2](docs/06-deployment.md#2-distribution-model),
  [06 §3](docs/06-deployment.md#3-docker-compose-topology),
  [06 §5](docs/06-deployment.md#5-ports-and-endpoints)). ONE crate, ONE
  binary, ONE image: a pinned `rust:1.97.0-bookworm` builder (matching
  `rust-toolchain.toml`, `SQLX_OFFLINE=true` against the committed `.sqlx/`,
  zero-warning `cargo build --release`) into a `debian:bookworm-slim` runtime
  (chosen over alpine/musl — the crate depends on `ring` via `jsonwebtoken`
  and `sqlx`'s `tls-rustls-ring`); non-root/distroless/read-only-rootfs
  hardening is #26 on top of this working image. The container `HEALTHCHECK`
  and the compose service healthcheck both poll `GET /health` (auth- and
  rate-limit-exempt, `src/gateway/rest/meta.rs`) via `curl`. Both persistence
  modes run from the **same image**: `DATABASE_URL` unset is fully in-memory
  (compose default); the `--profile persistent` overlay adds a pinned
  `postgres:18-alpine` (internal-only, no host port) and, once `DATABASE_URL`
  is exported pointing at it, `main.rs` opens the `PgPool` and runs
  `sqlx::migrate!` at boot — verified end-to-end locally in both modes
  (`docker compose up` and `--profile persistent up`), including a real
  `postgres:18-alpine` fix: its 18+ image layout requires a single volume
  mount at `/var/lib/postgresql`, not the pre-18 `.../data` convention.
  **Seed model reconciled against #24**: `docs/06 §3` (drafted pre-#24)
  describes a one-shot `seed` service driving the manifest over REST after a
  health check; #24 shipped a different, now-authoritative mechanism instead
  — the venue **self-seeds in-process** at boot (`src/seed.rs
  apply_seed_phase`, applied *before* `AppState::begin_serving()`, after which
  runtime hierarchy mutation is refused as a seed-time manifest input). A
  separate REST-driving seed service would duplicate that work or hit the
  post-flip refusal, so there is none: `docker-compose.yml` instead passes
  `--config /app/seeds/default.toml` (baked into the image) to the
  `fauxchange` service itself, in both profiles — `seeds/default.toml` and
  `seeds/README.md` are corrected to describe this (they previously also
  referenced a non-existent `FAUXCHANGE_CONFIG` env var; the seed sections
  load from the `--config <file>` layer only, `src/config.rs`). Ports match
  [06 §5](docs/06-deployment.md#5-ports-and-endpoints): `8080` REST/WS is
  live; `9878` FIX and `9090` metrics are **reserved** (`EXPOSE`d, and for
  metrics loopback-published in compose) but not yet backed by a listener —
  the FIX acceptor is v0.4 (`src/gateway/fix` is still a stub) and no
  Prometheus endpoint exists yet (verified: nothing answers on either port
  today). `FAUXCHANGE_DEV=1` is set in `docker-compose.yml` (not baked into
  the Dockerfile) because `main.rs` does not yet load a real RS256 key pair
  from mounted paths — only the embedded dev fixture, gated by
  `JwtAuth::release_gated`; without it the process **exits at startup**
  (`DevKeyRefused`) and the container never becomes healthy (a real deployment
  must not set this once real-key mounting lands, tracked with #26).
  `AUTH_BOOTSTRAP_SECRET` gets a documented,
  overridable dev default so the compose venue can mint a token immediately.
  `.dockerignore` (repo root) keeps `target/` (tens of GB locally) and every
  developer-only path (`docs/`, `rules/`, `milestones/`, `.claude/`,
  `CLAUDE.md`, `AGENTS.md`) out of the build context and every image layer.
  CI gets an additive `image-build` job (`.github/workflows/ci.yml`) that
  builds the image and validates `docker compose config` in both profiles —
  build only, no push (a release-pipeline concern, docs/06 §10); the
  `docker-smoke` compose-up + one-order-round-trip e2e is #27.
- **The scenario seed format + the bounded seeding phase** (#24) in
  `src/config.rs`, the new `src/seed.rs`, and `seeds/`
  ([024](milestones/v0.2-packaging/024-seed-data-format.md),
  [06 §7](docs/06-deployment.md#7-seed-data-and-scenarios),
  [06 §8](docs/06-deployment.md#8-auth-bootstrap),
  [03 §10](docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
  The `[accounts.*]` / `[instruments.*]` / `[market_maker.*]` config sections —
  previously `IgnoredAny` placeholders (#22) — are now **real, validated**
  `#[serde(deny_unknown_fields)]` structs resolving into a `SeedManifest` on
  `Config::seed`, so a typo *inside* a seeded account or instrument now aborts
  startup naming the key. Every seeded expiry is validated at **load** to an
  absolute canonical `ExpirationDate::DateTime` (a `YYYYMMDD` date or a
  `23:59:59 UTC` instant); a relative `Days` expiry is **refused**
  (`ConfigError::SeedDaysExpiry`) because it is wall-clock-relative and breaks
  replay. Strike ladders must be non-empty with distinct positive strikes, and
  persona knobs are range-checked. `main.rs` assembles the venue in a bounded
  **seeding phase** (`AppStateConfig::with_serving(false)`), then
  `seed::apply_seed_phase` applies the manifest in a **fixed order** — default
  persona → account provisioning (idempotent, Argon2id-hashed FIX passwords) →
  contract registration → opening prices — and flips to **serving**
  (`AppState::begin_serving`). Opening prices are set through the #016 price seam
  as journaled `SimStep`s whose market-maker quotes **vivify** the leaf books onto
  the shared symbol index (the honest population path — there is no REST
  hierarchy-create; the inherited `POST /api/v1/underlyings/…` routes are refusal
  stubs, now **phase-aware**: refused as a seed-time manifest input once serving).
  Re-running the seeder is **idempotent** (an account or instrument already present
  at the same specs is a no-op; a conflicting spec — a different opening price, or
  an account id at different permissions — is a typed error). A default
  `seeds/default.toml` scenario ships two underlyings, a strike ladder on
  `DateTime` expiries, opening prices, a default persona, and a Read + a Trade
  account with credentials. Money stays **integer cents** throughout; no new
  dependencies (`toml` from #22, `argon2` from #12). Prior `AppState` /
  `AppStateConfig` construction is unchanged (the venue defaults to serving).
- **The optional `sqlx`/PostgreSQL persistence layer — a durable executions
  backend behind the v0.1 store contract** (#23) in `src/db/`, `migrations/`,
  and `.sqlx/`, wired into `src/main.rs` and `AppState`
  ([023](milestones/v0.2-packaging/023-optional-pg-persistence.md),
  [06 §6](docs/06-deployment.md#6-persistence),
  [05 §4.1](docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable),
  [08 §7](docs/08-threat-model.md#7-secrets-handling)). Persistence is
  **optional** and selected at **runtime**, not by a cargo feature: with
  `DATABASE_URL` unset the venue is fully in-memory; with it set, `main.rs`
  opens a `PgPool` and runs `sqlx::migrate!("./migrations")` at boot — one
  binary, both modes. The `sqlx` dependency (`0.9.0`, matching `sqlx-cli`) uses
  the `runtime-tokio` + `tls-rustls-ring` (ring-backed rustls, no OpenSSL/C) +
  `postgres` + `macros` + `migrate` features; pool size and the slow-`acquire`
  warning threshold come from a new `[persistence]` config surface
  (`FAUXCHANGE_DB_MAX_CONNECTIONS` / `FAUXCHANGE_DB_SLOW_ACQUIRE_MS`), never
  hard-coded. `PgExecutionsStore` implements the **same** #8 `ExecutionsStore`
  trait as the in-memory backend, and `select_executions_store(db)` returns
  whichever backend behind that one contract. **Scope, stated honestly:** in
  #23 the durable store is migration-verified and parity-tested through the #8
  contract (`tests/db.rs`), but the **live single-writer actor fan-out still
  writes and reads the in-memory store** — `AppState` does not yet route live
  fills through the PG backend, so with `DATABASE_URL` set live executions do
  **not** persist to Postgres yet. Promoting the durable store onto the live
  fan-out is coupled to the sync→async single-writer rewire + the durable
  journal/recovery (v0.3, #29). Every query is a **compile-time-checked**
  `sqlx::query!` / `query_as!`
  with bound parameters (`$1, $2, …`); no value or identifier is ever
  interpolated. Cents persist as `BIGINT` (`i64`) — lossless because
  `MAX_PRICE_CENTS` bounds them (no `f64` money). Migrations are timestamp-
  prefixed and immutable once merged: `executions` (the authoritative fill log,
  the only table with read/write code here), plus the `underlying_prices` /
  `market_maker_configs` / `system_control` / `accounts` **schema skeletons**
  (grounded in the #12 `Account` / `Credentials` model — `id`, `owner`,
  `permissions`, the Argon2id `password_hash` **never plaintext**,
  `fix_username`, the comp-id binding, `revocation_epoch`; their read/write
  code lands with the surfaces that own them, #24). `sqlx::Error` is mapped to
  a typed `DbError` carrying only a non-secret label and **never leaked through
  a `pub` signature** (`DbError` → `StoreError::Backend` for the store contract
  and → a redacted internal `VenueError`); the `DATABASE_URL` is never logged.
  **The durable command journal is NOT built here** — it stays in-memory, and
  journal-backed recovery is v0.3 (#29): this layer supplies the durable
  executions backend + the config/account tables (behind the #8 contract, not
  yet on the live fan-out; see above), but book/fold state is not recovered on
  restart, so **a restart without an admin snapshot is a fresh venue**
  (documented, not silently implied). Positions are a derived fold —
  **not** persisted (no PG positions store). The committed `.sqlx/` offline
  data lets every non-DB CI job (and the release build) compile **offline**
  (`SQLX_OFFLINE=true`); a new **`migrations`** CI job runs the migrations + the
  DB integration test against a real ephemeral `postgres:18-alpine` via
  `testcontainers` (an `#[ignore]`-gated `tests/db.rs` test proving durable ≡
  in-memory backend parity behind one contract; the default `cargo test` suite
  stays green WITHOUT Docker). `deny.toml` gains `CDLA-Permissive-2.0` (the
  Mozilla CA-bundle license from `webpki-roots`, via the rustls TLS stack) to
  its allow-list, with a justification; `cargo audit` / `cargo deny` stay green.
- **The layered venue config surface — file + env + CLI with
  `deny_unknown_fields`** (#22) in `src/config.rs`, wired into `src/main.rs`,
  with a new `.env.example` and a `config_validate_rejects_out_of_range`
  property ([022](milestones/v0.2-packaging/022-config-surface.md),
  [06 §4](docs/06-deployment.md#4-configuration),
  [05 §2](docs/05-microstructure-config.md#2-config-model),
  [08 §7](docs/08-threat-model.md#7-secrets-handling)). The v0.2 config
  foundation the later milestones **extend, never replace**. A run is
  configured from four layers merged in a **fixed precedence** — defaults
  (in code) → TOML file (`--config <path>`) → environment → CLI flags, later
  winning — each layer a field-wise overlay over an untyped `RawConfig`, then
  **validated once** into the typed effective `Config`. The v0.2 concerns:
  `[server]` (`FAUXCHANGE_HTTP_ADDR` / `--http-addr`, default `0.0.0.0:8080`),
  `[fix]` (`FAUXCHANGE_FIX_ADDR` / `--fix-addr`, default `0.0.0.0:9878`),
  `[persistence]` (`DATABASE_URL` / `--database-url` — **unset ⇒ in-memory**,
  set ⇒ the `PersistenceBackend::Postgres` toggle #23 consumes; the config
  decides the backend, not the DB module), `[clock]` (`FAUXCHANGE_CLOCK` /
  `--clock`, the `realtime | accelerated | stepped` enum carried through for
  the clock services #28), `[determinism]` (`FAUXCHANGE_SEED` / `--seed`, one
  run-level `u64` feeding the run **lineage id** namespace), `[auth]`
  (`AUTH_BOOTSTRAP_SECRET`), and `[logging]` (`FAUXCHANGE_LOG_FORMAT` /
  `--log-format`, the `json | pretty` enum; structured-JSON emission is the
  observability milestone's #06). **`#[serde(deny_unknown_fields)]` on every
  file section + the top level**, so a typo aborts startup with a typed
  `ConfigError::UnknownKey` **naming the offending key** (extracted from
  serde's diagnostic) rather than silently defaulting — the ROADMAP v0.2
  acceptance item. **Boot-time validation before a single request**: bind
  addresses parse to `SocketAddr` (`BadAddress`), the clock/log-format enums
  check against their closed vocabularies (`InvalidClock` /
  `InvalidLogFormat`), and the seed parses as `u64` (`BadSeed`) — a
  `thiserror` `ConfigError` (no `anyhow`; distinct from the request-boundary
  `VenueError`) fails the process fast. **Secrets never reach a log**:
  `AUTH_BOOTSTRAP_SECRET` and `DATABASE_URL` are wrapped in a `Secret` newtype
  whose `Debug`/`Display` render `<redacted>` — redaction lives in one type,
  not at each call site — so the **effective config is logged once at boot**
  (`Config::render_effective`) with both secrets absent; the plaintext is
  reachable only through the explicitly-named `Secret::expose`, called at the
  DB pool / bootstrap gate. The `[accounts.*]` / `[instruments.*]` /
  `[microstructure.*]` / `[market_maker.*]` / `[rate_limits]` sections are
  **documented extension points** — accepted by the file loader today (typed
  `serde::de::IgnoredAny`, so a forward-looking config file is not rejected)
  but not validated here; the seed (#24) and v0.5 microstructure (#44–#47)
  swap each placeholder for a real `deny_unknown_fields` struct **without
  reshaping** the loader. `main.rs` now loads + validates the config first,
  logs the redacted effective config, and builds `AppStateConfig` from it
  (server bind address, seed → run lineage, bootstrap secret; the underlyings
  stay env-seeded until the `[instruments.*]` manifest #24). New dependency:
  `toml` 1 (`default-features = false, features = ["parse", "serde"]` — parse
  only, no serializer), which adds `toml` + `serde_spanned` to the tree (both
  `toml-rs`, MIT OR Apache-2.0, already on the `deny.toml` allow-list — **no
  new SPDX id, no `deny.toml` change**; `cargo audit` / `cargo deny` green);
  its parser deps (`toml_parser` / `toml_datetime` / `winnow` / `serde_core`)
  were already resolved, and **no CLI crate is added** — a small hand-rolled
  `--config`/scalar-override parser keeps `clap` (a dev-only transitive of
  `criterion`) out of the runtime binary. Injectable env lookup + explicit CLI
  args make the loader a pure, deterministic seam the unit and property tests
  drive without mutating the process environment (edition-2024 `set_var` is
  `unsafe`, forbidden here). Tests: unit (`src/config.rs`) — precedence
  (default/file/env/CLI each winning at its level), unknown-key rejection
  (naming the key, section + top-level), invalid clock / log-format / bind
  address / seed, the `DATABASE_URL` backend toggle, `--config` file selection
  + missing-file `FileRead`, empty-env-as-unset, seed → lineage, and the
  effective-config **secret redaction** (asserting both markers are absent
  from the render and the derived `Debug`); property
  (`config_validate_rejects_out_of_range`, `tests/property.rs`) — the
  validator accepts a clock/log-format/seed/address value **iff** it is
  genuinely valid, else fails with the matching typed `ConfigError`, the
  harness stood up for v0.5 to extend. `.env.example` declares every env var
  with its default and range per `rules/global_rules.md` *Configuration*.

- **Threat-model input hardening + captured-log credential test — the v0.1
  security capstone** (#21) across `src/models.rs`, `src/gateway/rest/mod.rs`,
  `src/auth.rs`, and a new `tests/security.rs`
  ([021](milestones/v0.1-backend-core/021-threat-model-input-hardening.md),
  [08 §4–§7](docs/08-threat-model.md#4-untrusted-input-hardening),
  [TESTING.md §14](docs/TESTING.md#14-security-testing)). Audits every v0.1
  untrusted input against the [08 §4](docs/08-threat-model.md#4-untrusted-input-hardening)
  table so each names its validation + resource ceiling + typed error, fills the
  gaps, and adds the defining security-test deliverables:
  - **The venue-owned max accepted/resting price ceiling** — the CODEX-tracked
    prerequisite the threat model names as the required economic-field bound.
    Two documented venue constants, `MAX_PRICE_CENTS` (`10^12` cents) and
    `MAX_ORDER_QUANTITY` (`10^6` contracts), are enforced in
    `validate_order_shape` (`src/models.rs`) — the boundary the REST/bulk
    handlers call **before** the sequenced order path. An order with
    `price > MAX_PRICE_CENTS` or `quantity > MAX_ORDER_QUANTITY` is a typed
    `400` (`InvalidOrder`), never accepted. A compile-time assertion pins the
    `MAX_PRICE_CENTS × MAX_ORDER_QUANTITY ≤ i64::MAX` invariant that keeps the
    per-leg fee narrowing (`SignedCents`/`i64`) and the upstream `notional × bps`
    product (`u128`) **off both saturation branches**.
  - **An explicit REST request-body ceiling** — `MAX_REQUEST_BODY_BYTES`
    (`1` MiB) applied via `DefaultBodyLimit`, replacing axum's undocumented
    framework default with a *named* DoS bound an oversized body hits (`413`)
    before it is buffered; it pairs with the per-batch `MAX_BULK_ORDER_ITEMS`
    item cap.
  - **The venue-reserved market-maker identity guard** (tracked #15 follow-up) —
    `AccountRegistry::insert_account` now rejects provisioning any account whose
    id is the reserved `@market-maker` account or whose STP owner is the reserved
    `MARKET_MAKER_OWNER` (`Hash32([0xEE; 32])`) with a typed `AuthError::Provisioning`,
    so a seed/admin cannot shadow (impersonate or mass-cancel) the venue's own
    quotes.
  - **The captured-log credential test** (`tests/security.rs`) — drives a full
    mint → order → error flow with a `tracing_subscriber` capture layer installed
    (a `MakeWriter` over a shared buffer; no new dependency — `tracing-subscriber`
    is already a runtime dependency) and asserts no captured log, error response
    body, or serialised state contains a password, an Argon2id hash
    (`$argon2id$`), the JWT signing key, the bootstrap secret, the Argon2 pepper,
    or a DB connection string; the effective-config-at-boot log is asserted
    redacted.
  - **The auth/authorization matrix, adversarial fixtures, and DoS-control
    suites** — every mutating REST op rejects missing/insufficient permission; a
    `Read` account is refused order entry on REST and (via the frame parser) on
    WS; a revocation refuses the account's tokens; oversized bodies, truncated
    JSON, out-of-range economic fields, malformed symbols, and unknown DTO fields
    each produce the correct typed `4xx`/typed WS reject (never a panic, never a
    silent accept); and the rate limiter (one budget), bounded actor mailbox
    (backpressure → typed `RateLimited`), bounded broadcast (laggard drop, no
    OOM), connection cap, and sequence-exhaustion sealing are each exercised as
    **security controls**; the captured-log test additionally proves a
    spawned-actor-task tracing event lands in the capture buffer, so its
    credential-absence assertions are not vacuously true. No new dependency; no
    `.unwrap()` on any inbound-data path; no `unsafe`. Known follow-ups (tracked,
    out of #21 scope): the `modify_order` handler carries no economic-field ceiling
    yet because it is an inert stub returning `InvalidOrder` unconditionally — the
    ceiling lands when modify is wired to the sequenced path; and the
    auth/authorization matrix is representative, not exhaustive (every mutating
    handler structurally calls `require()` — a per-handler exhaustive-matrix test is
    a nice-to-have follow-up).
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

### Fixed

- **Crash-consistent durable FIX session store** (#149, on #095). Closed three
  crash windows in the durable (`PgFixSessionStore`) / in-memory session-store
  contract. (1B) `emit` now stores an outbound frame AND advances
  `next_sender_seq` in ONE atomic operation via the new
  `FixSessionStore::store_outbound_and_advance` (one Postgres transaction / one
  locked in-memory critical section), so a crash can no longer store a frame with
  the counter un-advanced and REUSE its `MsgSeqNum` (a FIX-fatal duplicate).
  (1A) A routed order-entry mutation (`D`/`F`/`G`/`q`) now advances the inbound
  `next_target_seq` in memory (for gap detection) but DEFERS the durable persist
  until AFTER `AppState::submit` durably commits the exchange effect — so a crash
  before the journal append leaves the durable counter at the message's own seq,
  re-admitting the client's resend for idempotent reprocessing (the merged
  `ClOrdID` guard dedups it) instead of silently dropping the order.
  (2) New-key creation now serializes on a transaction-scoped Postgres advisory
  lock, so concurrent first-logons for different keys can no longer each observe
  `count < ceiling` under `READ COMMITTED` and overshoot `MAX_SESSION_KEYS`; the
  durable key-space bound now matches the in-memory hard bound.

### Security

- **Typed reject discriminant + not-owner≡not-found masking** (#132). A journaled
  typed `RejectKind` replaces the string-matched reject reason, and the existence
  kinds (`NotOwner`/`NotFound`/`NotResting`) render byte-identically at the client
  boundary across REST/WS/FIX for cancel AND replace — closing the BOLA/IDOR
  order-enumeration side channel (order ids are deterministically minted). The
  true kind + reason stay in the journal/`tracing` as a detective control. Adds
  `RejectKind` to the journaled `Rejected` outcome, back-compatible via
  `#[serde(default)]` so a pre-#132 durable journal still replays.
- **Validation / hardening backlog — boundary invariants across the exchange,
  gateway, and auth seams** (#131). Nine defensive fixes that close
  decode-time, TOCTOU, and observability gaps without changing any valid
  wire form:
  - `Instrument`/`Symbol` now deserialize **through** the validating
    constructor (`#[serde(try_from)]` → `Instrument::try_new` + a
    canonical-symbol cross-check), so a persisted/config record bearing
    `ExpirationDate::Days` (replay-breaking) or coordinates that disagree with
    its symbol is a typed decode error, not a silently-admitted book.
  - The journaled `MarketMakerControl` persona floats
    (`spread_multiplier` / `size_scalar` / `directional_skew`) reject
    `NaN` / `±Inf` / out-of-range at the envelope decode boundary via
    `deserialize_with` range guards, so a corrupt journal record can't poison
    replay; valid values decode unchanged (no golden churn).
  - An executions/positions projection failure **fail-stops the owning actor**,
    not just the fan-out: `FanOut::emit` surfaces a typed seal that the actor
    seals the underlying on (logged at `ERROR`), so it stops matching, journaling,
    and returning success receipts once a served store would diverge from the
    committed journal — a journal-backed rebuild is the recovery (the WS feed
    stays best-effort and never seals).
  - A journal read failure while building an actor **snapshot** now
    **propagates** as a typed error instead of collapsing into a false-empty
    `records` vec while `last_sequence` still reports records present — the
    internally-inconsistent snapshot that would have silently corrupted
    recovery/replay is refused.
  - The dampened mark advances **once per `execution_id`**, not once per
    account leg, so a single two-leg match no longer double-counts the mark.
  - REST enforces the **GTD ↔ `gtd_expires_at`** pairing at the DTO boundary
    (GTD requires a strictly-future expiry; any non-GTD TIF carrying an expiry
    is rejected), for both the single and bulk/multi-leg place shapes; the
    expiry lands as an absolute `ExpirationDate::DateTime`, never a `Days`
    offset.
  - A malformed JSON body now renders the typed `ErrorEnvelope` (400) via a
    drop-in `Json` extractor instead of axum's default plaintext 422, and the
    error body is attached to every `#[utoipa::path]` route's OpenAPI
    responses (the `413` oversized-body contract is preserved).
  - The `max_keys` issuance ceiling **and** the WS ticket-store cap are now
    reserved **atomically** (a single locked reserve-or-reject) so a concurrent
    flood can't exceed either cap.
  - WebSocket upgrades accept a **short-lived single-use ticket**
    (`POST /api/v1/auth/ws-ticket`, 30 s TTL, CSPRNG, DoS-capped) carrying the
    bearer's `Permission`, so the JWT no longer has to travel in the upgrade
    query string (the bearer path is retained for back-compat); the permission
    model is identical. Redemption re-checks the account revocation epoch **and**
    the bearer JWT's own `exp`, so a ticket cannot outlive a revoke or the
    token's expiry.
  - A `broadcast` lag now **re-snapshots or emits a typed `Resync` marker on
    every subscribed channel** (`trades` / `quotes` / `prices` / `fills`, not
    just `orderbook`), so no channel silently drops messages after an overflow.

- **Bumped IronFix to 0.3.1 and retired the now-redundant FIX pre-decode
  guards** (#140). `ironfix-core` / `ironfix-tagvalue` / `ironfix-dictionary` /
  `ironfix-transport` → `0.3.1` (all four; `Cargo.toml` pins + `Cargo.lock`,
  audit note added). The 0.3.1 decoder now rejects a malformed `BodyLength(9)`
  and a bad/absent `CheckSum(10=)` **internally** with checked arithmetic, so
  the venue's own pre-decode guards for those two cases became redundant:
  - Removed the `BoundedFrameDecoder` **framing precheck** (`FrameTooLarge` /
    `MalformedChecksum`) and the tag-value **`validate_body_length` +
    `10=`/CheckSum scan** — after empirically confirming, against the published
    0.3.1 source, that the decoder produces an **equivalent typed reject** (same
    reject class → `IncorrectDataFormat` / `SessionReject`, no panic) for each
    case. Removed the now-unconstructible `InvalidBodyLength` / `MalformedChecksum`
    error variants.
  - **`BoundedFrameDecoder` is kept as a by-policy byte cap** (a DoS/resource
    ceiling via `FixCodec::with_max_message_size`), explicitly re-documented as
    **policy, not a security-correctness check** (the hostile-arithmetic
    correctness now lives upstream) — an over-cap frame is still rejected
    (`MessageTooLarge`) **before** the full body is buffered.
  - Re-verified after the bump: `cargo audit` (0 vulns) + `cargo deny` clean;
    the `fix_decode` fuzz target rebuilt + ran 918,950 iterations with **no new
    crash**; FIX conformance/adversarial/golden suites green (the decoder's
    built-in rejections match the removed guards' behavior).
- **REST/WS JSON decoder fuzz targets, the journal/bundle deserialiser fuzz
  target, and the final `SECURITY.md` — the v1.0 security gate** (#52). Three
  `cargo fuzz` targets extend the FIX-primary fuzz set (#42) with the
  secondary REST/WS/journal decoders named in
  [08 §6](docs/08-threat-model.md#6-fuzzing-and-adversarial-testing): drives
  arbitrary bytes through the REAL production decode paths — no new
  validation logic, this only proves the existing ceilings/typed errors.
  - `fuzz/fuzz_targets/rest_json_decode.rs` — `axum::Json::<T>::from_bytes`
    (the exact function axum's own extractor calls) plus each DTO's real
    `.validate()`, under the router's `MAX_REQUEST_BODY_BYTES` ceiling, for
    `PlaceLimitOrderRequest` / `PlaceMarketOrderRequest` / `BulkOrderRequest`
    (the `Symbol` round-trip validator on every item) / `InsertPriceRequest`.
  - `fuzz/fuzz_targets/ws_frame_decode.rs` — `fauxchange::gateway::ws::parse_frame`
    (the exact function the socket loop calls on every inbound text frame),
    under `MAX_WS_FRAME_BYTES` and the real transport's UTF-8 guarantee.
  - `fuzz/fuzz_targets/journal_bundle_decode.rs` — folds the #34 adversarial
    corpus into the coverage-guided fuzz set: `decode_journal_record` and
    `ScenarioBundle::from_json` (both self-bounding on
    `MAX_JOURNAL_RECORD_BYTES` / `MAX_BUNDLE_BYTES` before the `serde_json`
    parse), so `cargo fuzz` now covers FIX + REST + WS + journal.
  - Each target's committed seed corpus (`fuzz/corpus/<target>/`) exercises
    oversized/truncated/malformed-JSON/unknown-field/duplicate-field/
    out-of-range-economic-field/malformed-symbol/deeply-nested-type-mismatch
    shapes (the journal/bundle corpus reuses #34's committed adversarial
    fixtures verbatim); every seed decodes without a panic or an unbounded
    allocation across a full local `cargo +nightly fuzz run` pass with no
    crash found.
  - The CI `fuzz` job (`.github/workflows/ci.yml`) now builds and runs all
    four targets — the existing FIX budget is unchanged; each new target gets
    its own short, bounded `-max_total_time=60 -max_len=<target-ceiling>`
    pass, seeded by its committed corpus.
  - `SECURITY.md`'s placeholder `0.0.x`/`0.x` supported-versions table is
    replaced with the `v1.0` policy (security fixes on the latest published
    `1.x` minor once `v1.0.0` ships; `0.x` carries no security-fix
    guarantee); the private disclosure channel (GitHub Security Advisories +
    email) and coordinated-disclosure expectations are confirmed unchanged.

## [0.0.1] - 2026-07-12

### Added

- Reserved the `fauxchange` crate name on crates.io.

[Unreleased]: https://github.com/joaquinbejar/fauxchange/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/joaquinbejar/fauxchange/releases/tag/v0.0.1
