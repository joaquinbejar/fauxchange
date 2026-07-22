//! # v0.5 capstone: three documented failure modes, each reproduced deterministically
//! ([#49](../milestones/v0.5-microstructure/049-scenario-failure-mode-determinism.md)).
//!
//! This suite **composes** the already-built microstructure knobs (#44 fees/STP/specs,
//! #45 latency, #46 rate-limits/profiles, #47 personas/halts) into three failure-mode
//! scenarios and proves each reproduces for a fixed descriptor. It does not
//! reimplement any knob; it drives the real config-load, submit, and export/replay
//! seams and asserts equality only over the **journaled** artifacts (fills / events /
//! book state per underlying), with mark prices and other derived analytics asserted
//! **out of scope as exclusions**
//! ([TESTING.md §5](../docs/TESTING.md#5-determinism--replay-tests),
//! [05 §11](../docs/05-microstructure-config.md#11-determinism-of-microstructure)).
//!
//! | Failure mode | How the scenario reproduces it |
//! |--------------|--------------------------------|
//! | **Throttling** — a rate-limit config drives a client into a `429` / throttle | the real `RateLimiter` over a `RateLimitConfig`-derived budget on a fixed venue clock yields the byte-identical allow/deny decision stream across two runs; the throttle is a **gateway boundary control** (not journaled), so its determinism is over `(config + venue clock + request order)` and is independent of the run seed (no RNG in the limiter) |
//! | **Halt** — a per-instrument halt starves order entry | a journaled `SetInstrumentStatus(Halted)` control command plus an order into the halted strike is the **journaled `Rejected`** that replays (the live-surfacing caveat is #118, so this asserts at the journal/replay level); a second independent run of the same descriptor reproduces the identical event stream + book state |
//! | **Wide-spread starvation** — a `wide_skewed` persona + finite resting liquidity starves a taker | the seeded persona quotes a wide, finite two-sided ladder; a taker that would fill against a `tight` persona cannot reach the `wide_skewed` ask and is starved; the same seed reproduces the identical journaled command stream and the same starved outcome on replay |
//!
//! Runtime control changes (the halt) are journaled commands and part of the
//! reproduced stream — reproduced, not treated as static config (#47, #49).

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::json;
use tower::ServiceExt;

use fauxchange::auth::{
    AccountProvision, RateLimitBudgets, RateLimitDecision, RateLimitKey, RateLimitTier, RateLimiter,
};
use fauxchange::config::RateLimitConfig;
use fauxchange::exchange::{
    ActorConfig, Cents, EventTimestamp, ExecutionFilter, ExecutionsStore, FixedClock, Hash32,
    InMemoryVenueJournal, InstrumentStatus, JournalHeader, JournalRecord, LineageId, MarkSource,
    MatchingExecutor, NoopFanOut, STPMode, SequenceNumber, Side, Symbol, TimeInForce, TopOfBook,
    UnderlyingActor, VenueCommand, VenueEvent, VenueJournal, VenueOutcome,
};
use fauxchange::gateway::rest::create_router;
use fauxchange::market_maker::{CommandSink, MarketMakerEngine, PersonaConfig, Quoter};
use fauxchange::microstructure::MicrostructureConfig;
use fauxchange::simulation::{
    ClockMode, JournalStream, ReplayReport, RunManifest, ScenarioBundle, VenueClockConfig,
    replay_bundle,
};
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};
use fauxchange::{AccountId, LiquidityFlag, OrderType, Permission, VenueOrderId};

/// The deterministic venue instant the sequenced path (and the fixed rate-limit
/// clock) stamp from — a single fixed instant is enough because the venue clock is
/// never advanced in these tests (no cadence driver is spawned).
const CLOCK_MS: u64 = 1_700_000_000_000;

fn sym(raw: &str) -> Symbol {
    match Symbol::parse(raw) {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
    }
}

// ============================================================================
// Failure mode 1 — throttling: a rate-limit config drives a client into a 429
// ============================================================================

/// Runs a fixed request stream for one client through a fresh `RateLimiter` built
/// from `budgets` on a **fixed** venue clock, returning the ordered decisions. No
/// RNG and no wall clock touch the limiter, so this is a pure function of
/// `(budgets, request order)`.
fn throttle_run(budgets: RateLimitBudgets, request_count: u32) -> Vec<RateLimitDecision> {
    let limiter =
        RateLimiter::with_budgets(FixedClock::new(EventTimestamp::new(CLOCK_MS)), budgets);
    let key = RateLimitKey::Account {
        account: AccountId::new("throttled-client"),
        revocation_epoch: 0,
        tier: RateLimitTier::Trade,
    };
    (0..request_count)
        .map(|_| limiter.check_and_record_status(&key))
        .collect()
}

#[test]
fn test_scenario_throttling_reproduces_the_same_throttle_reject_for_a_fixed_config() {
    // The scenario config: a 3-request Trade budget over a 60 s window, validated at
    // load exactly as the venue would (`RateLimitConfig::validate` + `to_budgets`).
    let config = RateLimitConfig {
        window_secs: 60,
        read_per_window: 100,
        trade_per_window: 3,
        admin_per_window: 100,
    };
    config
        .validate()
        .expect("the rate-limit scenario config is valid at load");
    let budgets = config.to_budgets();
    let budget = budgets.trade();
    let request_count = budget + 3;

    // Same descriptor (config + fixed clock + request order) => identical decisions.
    let first = throttle_run(budgets, request_count);
    let second = throttle_run(budgets, request_count);
    assert_eq!(
        first, second,
        "the throttle decision stream reproduces exactly for the same descriptor"
    );

    // The documented failure mode: exactly the budget is admitted, then the client
    // is throttled with a `Retry-After` (the `429` signal) for the rest of the window.
    let admitted = first.iter().filter(|d| d.allowed).count();
    assert_eq!(
        admitted, budget as usize,
        "exactly the configured Trade budget is admitted before the throttle"
    );
    let throttled = first
        .get(budget as usize)
        .expect("an over-budget request exists");
    assert!(
        !throttled.allowed,
        "the first over-budget request is throttled (the 429)"
    );
    assert_eq!(throttled.remaining, 0, "no budget remains at the throttle");
    assert!(
        throttled.retry_after_ms.is_some(),
        "the throttle carries a Retry-After hint (the 429 backoff signal)"
    );
    assert!(
        first[budget as usize..].iter().all(|d| !d.allowed),
        "every request past the budget stays throttled within the window"
    );
}

/// The bootstrap secret gating token minting on the throttle-scenario venues.
const RL_SECRET: &str = "throttle-scenario-secret";
/// The per-contract REST order path for the fixture contract (`BTC` call).
const RL_ORDER_PATH: &str =
    "/api/v1/underlyings/BTC/expirations/20240329/strikes/50000/options/call/orders";

fn rl_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A fresh single-underlying venue whose Trade tier carries `budgets`, plus a
/// `trader-1` Trade account and the bootstrap secret that gates token minting.
fn rl_venue(budgets: RateLimitBudgets) -> Arc<AppState> {
    let accounts = vec![AccountProvision::new(
        AccountId::new("trader-1"),
        Hash32([2; 32]),
        vec![Permission::Trade],
    )];
    let auth = match AuthConfig::dev() {
        Ok(auth) => auth
            .with_bootstrap_secret(RL_SECRET)
            .with_accounts(accounts)
            .with_rate_limit_budgets(budgets),
        Err(error) => panic!("dev auth must build: {error}"),
    };
    match AppState::new(AppStateConfig::new(["BTC"]).with_auth(auth)) {
        Ok(state) => state,
        Err(error) => panic!("AppState must build: {error}"),
    }
}

/// Posts one order through the REAL router, returning `(status, [X-RateLimit-Limit,
/// X-RateLimit-Remaining, X-RateLimit-Reset, Retry-After])`.
async fn rl_post_order(state: &Arc<AppState>, bearer: &str) -> (StatusCode, [Option<String>; 4]) {
    let body = match serde_json::to_vec(&json!({ "side": "buy", "price": 50_000, "quantity": 1 })) {
        Ok(bytes) => bytes,
        Err(e) => panic!("serialising the order body must succeed: {e}"),
    };
    let request = match Request::builder()
        .method("POST")
        .uri(RL_ORDER_PATH)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
    {
        Ok(request) => request,
        Err(e) => panic!("building the request must succeed: {e}"),
    };
    let router: Router = create_router(Arc::clone(state));
    let response = match router.oneshot(request).await {
        Ok(response) => response,
        Err(e) => panic!("router must be infallible: {e}"),
    };
    let status = response.status();
    let read = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    let headers = [
        read("x-ratelimit-limit"),
        read("x-ratelimit-remaining"),
        read("x-ratelimit-reset"),
        read("retry-after"),
    ];
    // Drain the body so the connection is not left mid-response (never inspected).
    let _ = to_bytes(response.into_body(), usize::MAX).await;
    (status, headers)
}

#[tokio::test]
async fn test_scenario_throttling_drives_a_real_rest_429_with_ratelimit_headers() {
    // The SAME 3-request Trade budget config, now driven into a REAL REST 429 over the
    // live router (not the RateLimiter directly). A 60 s window on a venue never
    // advanced mid-run means the window never expires, so the wire status stream is a
    // pure function of the config + request order — reproduced across two fresh venues.
    let config = RateLimitConfig {
        window_secs: 60,
        read_per_window: 100,
        trade_per_window: 3,
        admin_per_window: 100,
    };
    config
        .validate()
        .expect("the rate-limit scenario config is valid at load");
    let budgets = config.to_budgets();
    let budget = budgets.trade();
    let request_count = budget + 3;

    async fn drive(
        state: &Arc<AppState>,
        bearer: &str,
        count: u32,
    ) -> Vec<(StatusCode, [Option<String>; 4])> {
        let mut out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            out.push(rl_post_order(state, bearer).await);
        }
        out
    }

    let venue_a = rl_venue(budgets);
    let token_a = venue_a
        .mint_token(&AccountId::new("trader-1"), RL_SECRET, rl_now_secs(), 3_600)
        .expect("minting must succeed");
    let run_a = drive(&venue_a, &token_a, request_count).await;

    let venue_b = rl_venue(budgets);
    let token_b = venue_b
        .mint_token(&AccountId::new("trader-1"), RL_SECRET, rl_now_secs(), 3_600)
        .expect("minting must succeed");
    let run_b = drive(&venue_b, &token_b, request_count).await;

    // The wire status stream reproduces exactly across the two identically-configured
    // venues (the deterministic failure mode, observed on the real surface).
    let statuses = |run: &[(StatusCode, [Option<String>; 4])]| -> Vec<StatusCode> {
        run.iter().map(|(status, _)| *status).collect()
    };
    assert_eq!(
        statuses(&run_a),
        statuses(&run_b),
        "the REST status stream reproduces exactly for the same descriptor"
    );

    // Exactly the configured budget is admitted (200), then the client hits a 429.
    let admitted = statuses(&run_a)
        .iter()
        .filter(|status| **status == StatusCode::OK)
        .count();
    assert_eq!(
        admitted, budget as usize,
        "exactly the configured Trade budget is admitted before the 429"
    );

    // The first over-budget request is a real HTTP 429 with the X-RateLimit-* context
    // and a Retry-After (the 429 backoff signal).
    let (status, headers) = &run_a[budget as usize];
    assert_eq!(
        *status,
        StatusCode::TOO_MANY_REQUESTS,
        "the first over-budget request is a real 429"
    );
    assert_eq!(
        headers[0].as_deref(),
        Some(budget.to_string().as_str()),
        "X-RateLimit-Limit is the configured Trade budget"
    );
    assert_eq!(
        headers[1].as_deref(),
        Some("0"),
        "X-RateLimit-Remaining is 0 at the throttle"
    );
    assert!(headers[2].is_some(), "X-RateLimit-Reset present on the 429");
    assert!(
        headers[3].is_some(),
        "Retry-After present on the 429 (the backoff signal)"
    );

    // Every request past the budget stays a 429 within the window.
    assert!(
        run_a[budget as usize..]
            .iter()
            .all(|(status, _)| *status == StatusCode::TOO_MANY_REQUESTS),
        "every over-budget request stays throttled within the window"
    );
}

// ============================================================================
// Failure mode 2 — halt: a per-instrument halt starves order entry (journaled)
// ============================================================================

const HALT_UNDERLYING: &str = "BTC";
const HALT_CALL: &str = "BTC-20260626-50000-C";
const HALT_PUT: &str = "BTC-20260626-50000-P";

/// A single-underlying in-memory venue on a stepped (never auto-advancing) clock,
/// so the "clock mode" half of the descriptor is explicit and `venue_ts` is a fixed
/// constant across runs.
fn halt_state() -> Arc<AppState> {
    let config = AppStateConfig::new([HALT_UNDERLYING])
        .with_lineage(LineageId::new("run-halt-scenario"))
        .with_microstructure(MicrostructureConfig::default())
        .with_clock(VenueClockConfig::stepped(CLOCK_MS, 60_000));
    match AppState::new(config) {
        Ok(state) => state,
        Err(error) => panic!("AppState with dev auth must build: {error}"),
    }
}

/// A GTC limit `AddOrder` from a client account onto the venue's BTC chain.
#[allow(clippy::too_many_arguments)]
fn client_add(
    symbol: &str,
    order_id: &str,
    account: &str,
    owner: u8,
    side: Side,
    price: u64,
    quantity: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(symbol),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new(account),
        owner: Hash32([owner; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

/// Drives the halt scenario onto `state`: a journaled `SetInstrumentStatus(Halted)`
/// control on the CALL strike (part of the reproduced stream), then an order into
/// the halted strike (journaled `Rejected` — starved entry), then an order into the
/// still-`Active` PUT (rests — the gate is per-instrument). Every command enters the
/// sequenced path.
async fn run_halt_scenario(state: &Arc<AppState>) {
    // The runtime control change is a journaled command, not static config.
    state
        .submit(VenueCommand::SetInstrumentStatus {
            symbol: sym(HALT_CALL),
            status: InstrumentStatus::Halted,
        })
        .await
        .expect("the halt control sequences");
    // An order into the halted strike: the reject is a journaled OUTCOME (the actor
    // returns Ok — the command was sequenced and its `Rejected` event captured), not
    // a pre-sequencer submit error.
    state
        .submit(client_add(
            HALT_CALL,
            "into-halt",
            "alice",
            0x11,
            Side::Buy,
            50_000,
            3,
        ))
        .await
        .expect("the halted-strike order sequences to a journaled Rejected");
    // A control order into the still-Active PUT rests — proving the gate is
    // per-instrument, not a venue-wide freeze.
    state
        .submit(client_add(
            HALT_PUT,
            "on-active",
            "bob",
            0x22,
            Side::Sell,
            30_000,
            2,
        ))
        .await
        .expect("the active strike accepts the order");
}

#[tokio::test]
async fn test_scenario_halt_starves_order_entry_and_replays_the_journaled_rejection() {
    let state = halt_state();
    run_halt_scenario(&state).await;

    // Export the descriptor bundle (manifest: seed + clock mode + microstructure
    // fingerprint; plus the journaled command stream) and replay it offline through
    // the real driver.
    let bundle = state.export_bundle().await.expect("export bundle");
    let report = replay_bundle(&bundle).expect("the halt scenario replays exactly");
    let replay = report.underlying(HALT_UNDERLYING).expect("BTC replay");

    // The oracle over JOURNALED artifacts: the halted-strike order replays as the
    // captured `Rejected` naming the halt — that is the reproduced failure mode.
    let halted_event = replay
        .events
        .iter()
        .find(|event| {
            matches!(
                &event.command,
                VenueCommand::AddOrder { symbol, .. } if symbol.as_str() == HALT_CALL
            )
        })
        .expect("the halted-strike order is journaled");
    match &halted_event.outcome {
        VenueOutcome::Rejected { reason, .. } => assert!(
            reason.contains("Halted"),
            "the reject names the halt status: {reason}"
        ),
        other => panic!("an order into a halted strike must replay as Rejected, got {other:?}"),
    }

    // Book state per underlying: the halted strike rests nothing (starved), the
    // Active strike rests its order.
    assert_eq!(
        replay.top_of_book(&sym(HALT_CALL)),
        TopOfBook::default(),
        "the halted strike accepted no liquidity — order entry is starved"
    );
    let active_top = replay.top_of_book(&sym(HALT_PUT));
    assert_eq!(active_top.best_ask, Some(Cents::new(30_000)));
    assert_eq!(active_top.ask_depth, 2);

    // Same-descriptor reproduction: a SECOND independent live run of the same
    // descriptor exports a bundle whose replay reconstructs the IDENTICAL journaled
    // event stream (including the `Rejected`) and the identical book state. The
    // fixed stepped clock makes `venue_ts` a constant, so event equality is exact.
    let state_b = halt_state();
    run_halt_scenario(&state_b).await;
    let bundle_b = state_b.export_bundle().await.expect("export bundle b");
    let report_b = replay_bundle(&bundle_b).expect("the second run replays exactly");
    let replay_b = report_b.underlying(HALT_UNDERLYING).expect("BTC replay b");
    assert_eq!(
        replay.events, replay_b.events,
        "two runs of the same descriptor reproduce the identical journaled event stream"
    );
    assert_eq!(
        replay.top_of_book(&sym(HALT_CALL)),
        replay_b.top_of_book(&sym(HALT_CALL)),
        "the starved halted strike reproduces identically"
    );
    assert_eq!(
        replay.top_of_book(&sym(HALT_PUT)),
        replay_b.top_of_book(&sym(HALT_PUT)),
        "the active strike's book reproduces identically"
    );

    // Exclusion (asserted, not silently divergent): mark prices are a DERIVED
    // analytic recomputed from journaled trade prints, not a journaled oracle
    // artifact. This scenario produces no fill (the halted order is Rejected, the
    // Active order rests untouched), so the reconstructed mark book carries no mark
    // for either strike — the derived analytic is out of the oracle's scope.
    assert_eq!(
        report.marks.mark(&sym(HALT_CALL)),
        None,
        "no trade printed, so the halted strike has no derived mark (marks are excluded)"
    );
    assert_eq!(
        report.marks.mark(&sym(HALT_PUT)),
        None,
        "the resting Active order printed no trade, so its mark is likewise absent"
    );
}

// ============================================================================
// Failure mode 3 — wide-spread starvation: a wide_skewed persona + finite
// resting liquidity starves a taker (journaled, seed-reproducible)
// ============================================================================

const WIDE_UNDERLYING: &str = "BTC";
/// A far-future expiry so, at `WIDE_NOW_MS`, the persona quotes a live chain.
const WIDE_CALL: &str = "BTC-20351231-50000-C";
/// 2025-01-01T00:00:00Z in ms — well before the 2035 expiry.
const WIDE_NOW_MS: u64 = 1_735_689_600_000;
const WIDE_SPOT_CENTS: u64 = 5_000_000;
/// The one run-level seed the persona-jitter sub-stream derives from.
const WIDE_SEED: u64 = 0xFA11_5EED;
/// A taker larger than any single persona's finite resting slice, so a fill (if it
/// crosses) is bounded by real matching, never by the taker's own size.
const TAKER_QTY: u64 = 25;

/// A [`CommandSink`] that records the requote commands routed to it, in order.
#[derive(Default)]
struct CollectingSink {
    commands: Mutex<Vec<VenueCommand>>,
}

impl CollectingSink {
    fn take(&self) -> Vec<VenueCommand> {
        std::mem::take(&mut self.commands.lock().expect("sink lock"))
    }
}

impl CommandSink for CollectingSink {
    fn enqueue(&self, command: VenueCommand) {
        self.commands.lock().expect("sink lock").push(command);
    }
}

/// The journaled requote `AddOrder` stream a seeded engine produces for `persona` on
/// the CALL — the venue-owned, seed-reproducible generated liquidity.
fn persona_requote_stream(
    seed: u64,
    persona_name: &str,
    persona: PersonaConfig,
) -> Vec<VenueCommand> {
    let sink = Arc::new(CollectingSink::default());
    let engine = MarketMakerEngine::new(
        sink.clone(),
        LineageId::new("run-wide-scenario"),
        Quoter::default(),
    )
    .with_run_seed(seed);
    engine.set_venue_now_ms(WIDE_NOW_MS);
    engine.register_instrument_with_persona(&sym(WIDE_CALL), None, persona_name, persona);
    engine.update_price(WIDE_UNDERLYING, WIDE_SPOT_CENTS);
    sink.take()
}

/// The `(price, size)` of the sell (ask) leg in a requote command stream.
fn ask_leg(commands: &[VenueCommand]) -> Option<(u64, u64)> {
    commands.iter().find_map(|command| match command {
        VenueCommand::AddOrder {
            side: Side::Sell,
            limit_price: Some(price),
            quantity,
            ..
        } => Some((price.get(), *quantity)),
        _ => None,
    })
}

/// A client taker limit buy onto the CALL.
fn taker_buy(order_id: &str, price: u64, quantity: u64) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(WIDE_CALL),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new("starved-taker"),
        owner: Hash32([0x44; 32]),
        client_order_id: None,
        side: Side::Buy,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

/// Records `commands` into the production journal via the real single-writer
/// actor, then replays the recorded [`ScenarioBundle`] through the production
/// driver [`replay_bundle`] — the failure-mode replay path (never a direct
/// `MatchingExecutor::execute`). Returns the recorded event stream and the replay
/// report, so the oracle compares journaled events / reconstructed fills / book.
fn record_and_replay(commands: &[VenueCommand]) -> (Vec<VenueEvent>, ReplayReport) {
    let lineage = LineageId::new("run-wide-scenario");
    let header = JournalHeader::new(lineage.clone());
    let mut actor = UnderlyingActor::new(
        ActorConfig::new(WIDE_UNDERLYING, lineage.clone(), 64),
        InMemoryVenueJournal::new(header.clone()),
        MatchingExecutor::new(WIDE_UNDERLYING),
        NoopFanOut,
        FixedClock::new(EventTimestamp::new(CLOCK_MS)),
    );
    for command in commands {
        actor
            .handle(command.clone())
            .expect("the actor turn journals and commits");
    }
    let records = actor
        .journal()
        .read_from(SequenceNumber::START)
        .expect("read the recorded journal");
    let recorded_events: Vec<VenueEvent> = records
        .iter()
        .filter_map(|record| match record {
            JournalRecord::Event(event) => Some(event.clone()),
            _ => None,
        })
        .collect();
    let stream = JournalStream::new(WIDE_UNDERLYING, header, records);
    let manifest = RunManifest::new(WIDE_SEED, ClockMode::Realtime)
        .with_microstructure_fingerprint(MicrostructureConfig::default().fingerprint());
    let bundle = ScenarioBundle::new(manifest, vec![stream]);
    let report = replay_bundle(&bundle)
        .expect("the recorded scenario replays through the production driver");
    (recorded_events, report)
}

/// The taker's filled quantity (as taker) in a replay report's reconstructed
/// executions store — the failure signal: `0` means starved.
fn taker_filled_in(report: &ReplayReport, account: &str) -> u64 {
    let filter = ExecutionFilter::default();
    report
        .executions
        .list(&AccountId::new(account), &filter)
        .map(|records| {
            records
                .iter()
                .filter(|record| record.liquidity == LiquidityFlag::Taker)
                .map(|record| record.quantity)
                .sum()
        })
        .unwrap_or(0)
}

#[test]
fn test_scenario_wide_skewed_persona_starves_a_taker_and_reproduces_for_a_fixed_seed() {
    // Two personas differing (dominantly) in spread width. `wide_skewed` quotes a
    // spread ~25× wider than `tight` (200 bps × 2.5 vs 20 bps × 1.0), so even after
    // the -0.4 skew narrows its ask, the wide ask sits far above the tight ask.
    let wide = PersonaConfig::try_new(200, 6, 2.5, 0.8, -0.4).expect("wide_skewed persona");
    let tight = PersonaConfig::try_new(20, 6, 1.0, 0.8, 0.0).expect("tight persona");

    // The wide persona rests a FINITE, wide two-sided ladder.
    let wide_stream = persona_requote_stream(WIDE_SEED, "wide_skewed", wide);
    let (wide_ask, wide_ask_size) = ask_leg(&wide_stream).expect("the wide persona quoted an ask");
    assert!(
        wide_ask_size >= 1 && wide_ask_size <= wide.base_size,
        "the persona rests a finite, positive slice bounded by base_size: {wide_ask_size}"
    );
    let (tight_ask, _) = ask_leg(&persona_requote_stream(WIDE_SEED, "tight", tight))
        .expect("the tight persona quoted an ask");
    assert!(
        tight_ask < wide_ask,
        "the wide_skewed persona quotes a strictly wider ask ({wide_ask}) than tight ({tight_ask})"
    );

    // A taker priced just under the wide ask: it cannot reach the wide persona's
    // liquidity, but WOULD cross the far-tighter tight-persona ask.
    let taker_limit = wide_ask - 1;

    // A taker priced just under the wide ask.
    let mut wide_scenario = wide_stream.clone();
    wide_scenario.push(taker_buy("starved-taker-order", taker_limit, TAKER_QTY));
    let mut tight_scenario = persona_requote_stream(WIDE_SEED, "tight", tight);
    tight_scenario.push(taker_buy("control-taker-order", taker_limit, TAKER_QTY));

    // Failure mode, proven through the PRODUCTION replay driver: record the scenario
    // into the real journal, replay the `ScenarioBundle` via `replay_bundle`, and read
    // the starvation off the RECONSTRUCTED executions store — against the wide_skewed
    // persona's finite resting liquidity the taker takes nothing (starved) and no fill
    // prints anywhere.
    let (wide_events, wide_report) = record_and_replay(&wide_scenario);
    assert_eq!(
        taker_filled_in(&wide_report, "starved-taker"),
        0,
        "the wide spread starves the taker: zero taker fill in the replayed executions"
    );
    assert_eq!(
        wide_report.executions.len(),
        0,
        "an unreachable wide ask prints no fill anywhere on the reconstructed run"
    );

    // Control (proves the WIDE SPREAD is the cause, not the taker's own price): the
    // identical taker against the tight persona's liquidity DOES fill on replay —
    // bounded by that persona's finite resting slice (real matching, not synthetic).
    // The taker account is `starved-taker` in both scenarios (`taker_buy`); only the
    // resting persona differs, so a fill here isolates the wide spread as the cause.
    let (_tight_events, tight_report) = record_and_replay(&tight_scenario);
    assert!(
        taker_filled_in(&tight_report, "starved-taker") > 0,
        "a tighter spread would have filled the same taker — the starvation is caused by the wide spread"
    );

    // Same-seed reproduction of the venue-owned sub-stream: the seeded persona jitter
    // is a pure function of the seed, so the identical descriptor regenerates the
    // byte-identical journaled command stream.
    assert_eq!(
        persona_requote_stream(WIDE_SEED, "wide_skewed", wide),
        wide_stream,
        "the same seed reproduces the identical persona requote ladder"
    );

    // Oracle reproduction through the production driver: recording the same scenario
    // and replaying it again reconstructs the IDENTICAL journaled event stream and
    // book state per underlying (compared over journaled artifacts, not a direct
    // executor re-run), and matches the events recorded on the first pass.
    let (rerecorded_events, replay_again) = record_and_replay(&wide_scenario);
    assert_eq!(
        rerecorded_events, wide_events,
        "the recorded VenueEvent stream reproduces exactly for the same descriptor"
    );
    let first = wide_report
        .underlying(WIDE_UNDERLYING)
        .expect("the first replay carries the BTC underlying");
    let again = replay_again
        .underlying(WIDE_UNDERLYING)
        .expect("the second replay carries the BTC underlying");
    assert_eq!(
        first.events, again.events,
        "replay_bundle re-derives the identical ordered event stream"
    );
    assert_eq!(
        first.events, wide_events,
        "the driver-replayed events equal the events recorded live"
    );
    assert_eq!(
        first.top_of_book(&sym(WIDE_CALL)),
        again.top_of_book(&sym(WIDE_CALL)),
        "the reconstructed book (the starved taker resting unfilled) reproduces exactly"
    );
}
