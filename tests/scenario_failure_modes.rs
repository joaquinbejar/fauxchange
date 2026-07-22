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

use fauxchange::auth::{
    RateLimitBudgets, RateLimitDecision, RateLimitKey, RateLimitTier, RateLimiter,
};
use fauxchange::config::RateLimitConfig;
use fauxchange::exchange::{
    Cents, CommandExecutor, EventTimestamp, ExecutionContext, FixedClock, Hash32, InstrumentStatus,
    LineageId, MarkSource, MatchingExecutor, STPMode, SequenceNumber, Side, Symbol, TimeInForce,
    TopOfBook, VenueCommand, VenueOutcome,
};
use fauxchange::market_maker::{CommandSink, MarketMakerEngine, PersonaConfig, Quoter};
use fauxchange::microstructure::MicrostructureConfig;
use fauxchange::simulation::{VenueClockConfig, replay_bundle};
use fauxchange::state::{AppState, AppStateConfig};
use fauxchange::{AccountId, LiquidityFlag, OrderType, VenueOrderId};

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

/// Replays a command stream through a fresh executor (the determinism-oracle read
/// surface), returning every outcome and the reconstructed top-of-book.
fn replay_stream(commands: &[VenueCommand]) -> (Vec<VenueOutcome>, TopOfBook) {
    let lineage = LineageId::new("run-wide-scenario");
    let mut executor = MatchingExecutor::new(WIDE_UNDERLYING);
    let mut outcomes = Vec::with_capacity(commands.len());
    for (index, command) in commands.iter().enumerate() {
        outcomes.push(executor.execute(ExecutionContext {
            underlying: WIDE_UNDERLYING,
            lineage_id: &lineage,
            sequence: SequenceNumber::new(index as u64),
            venue_ts: EventTimestamp::new(CLOCK_MS),
            command,
        }));
    }
    (outcomes, executor.top_of_book(&sym(WIDE_CALL)))
}

/// The taker's filled quantity (as taker) after replaying `commands` — the failure
/// signal: `0` means starved.
fn taker_filled(commands: &[VenueCommand]) -> u64 {
    let (outcomes, _top) = replay_stream(commands);
    match outcomes.last().expect("a taker outcome") {
        VenueOutcome::Added { fills, .. } => fills
            .iter()
            .filter(|fill| fill.liquidity == LiquidityFlag::Taker)
            .map(|fill| fill.quantity)
            .sum(),
        other => panic!("expected the taker add outcome, got {other:?}"),
    }
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

    // Failure mode: against the wide_skewed persona's finite resting liquidity the
    // taker is starved — zero fill — and rests unfilled.
    let mut wide_scenario = wide_stream.clone();
    wide_scenario.push(taker_buy("starved-taker-order", taker_limit, TAKER_QTY));
    assert_eq!(
        taker_filled(&wide_scenario),
        0,
        "the wide spread starves the taker: zero fill against the unreachable wide ask"
    );

    // Control (proves the WIDE SPREAD is the cause, not the taker's own price): the
    // identical taker against the tight persona's liquidity DOES fill — bounded by
    // that persona's finite resting slice (real matching, not a synthetic partial).
    let mut tight_scenario = persona_requote_stream(WIDE_SEED, "tight", tight);
    tight_scenario.push(taker_buy("control-taker-order", taker_limit, TAKER_QTY));
    assert!(
        taker_filled(&tight_scenario) > 0,
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

    // Oracle reproduction: replaying the same journaled command stream twice
    // reconstructs identical outcomes (fills/events) and book state per underlying.
    let (outcomes_a, top_a) = replay_stream(&wide_scenario);
    let (outcomes_b, top_b) = replay_stream(&wide_scenario);
    assert_eq!(
        outcomes_a, outcomes_b,
        "the starved outcome reproduces exactly for the same descriptor"
    );
    assert_eq!(
        top_a, top_b,
        "the reconstructed book (the starved taker resting unfilled) reproduces exactly"
    );
}
