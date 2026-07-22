//! Integration + determinism tests for the market maker on the sequenced path
//! ([015](../milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md)).
//!
//! - A price update → requote produces MM-tagged `AddOrder` `VenueCommand`s that
//!   route onto the sequenced order path.
//! - Replaying the resulting command stream (the "journal") through the upstream
//!   matching executor reproduces **identical** fills and top-of-book — the
//!   determinism oracle covers generated liquidity, and the requote is journaled,
//!   not re-derived.
//! - A crossing client order fills the maker's resting quote, attributed to the
//!   venue-reserved market-maker account.
//! - The real `AppState` wiring routes a live requote onto the per-underlying
//!   actor and journals it.

use std::sync::{Arc, Mutex};

use fauxchange::exchange::{
    Cents, CommandExecutor, EventTimestamp, ExecutionContext, Hash32, InstrumentStatus,
    JournalRecord, LineageId, MARKET_MAKER_OWNER, MassCancelScope, MassCancelType,
    MatchingExecutor, STPMode, SequenceNumber, Side, Symbol, TimeInForce, TopOfBook, VenueCommand,
    VenueOutcome, is_market_maker_account, is_market_maker_command, market_maker_account,
};
use fauxchange::market_maker::{CommandSink, MarketMakerEngine, PersonaConfig, Quoter};
use fauxchange::models::{AccountId, OrderType, VenueOrderId};
use fauxchange::state::{AppState, AppStateConfig};

const UNDERLYING: &str = "BTC";
const CALL: &str = "BTC-20351231-50000-C";
const TS: EventTimestamp = EventTimestamp::new(1_700_000_000_000);
/// 2025-01-01T00:00:00Z in ms — well before the 2035 expiry.
const VENUE_NOW_MS: u64 = 1_735_689_600_000;

/// A [`CommandSink`] that records the commands routed to it, in order.
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

fn sym(raw: &str) -> Symbol {
    Symbol::parse(raw).expect("valid fixture symbol")
}

/// Builds an engine over a collecting sink, with the venue clock set.
fn engine() -> (MarketMakerEngine, Arc<CollectingSink>) {
    let sink = Arc::new(CollectingSink::default());
    let engine = MarketMakerEngine::new(sink.clone(), LineageId::new("run-1"), Quoter::default());
    engine.set_venue_now_ms(VENUE_NOW_MS);
    (engine, sink)
}

/// A client limit order (a non-market-maker account).
fn client_add(order_id: &str, side: Side, price: u64, quantity: u64) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(CALL),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new("alice"),
        owner: fauxchange::exchange::Hash32([0x11; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

/// Replays a command stream through a fresh executor, returning every outcome and
/// the final top-of-book — the determinism-oracle read surface.
fn replay(commands: &[VenueCommand]) -> (Vec<VenueOutcome>, TopOfBook) {
    let lineage = LineageId::new("run-1");
    let mut executor = MatchingExecutor::new(UNDERLYING);
    let mut outcomes = Vec::with_capacity(commands.len());
    for (index, command) in commands.iter().enumerate() {
        let outcome = executor.execute(ExecutionContext {
            underlying: UNDERLYING,
            lineage_id: &lineage,
            sequence: SequenceNumber::new(index as u64),
            venue_ts: TS,
            command,
        });
        outcomes.push(outcome);
    }
    (outcomes, executor.top_of_book(&sym(CALL)))
}

#[test]
fn test_requote_produces_market_maker_add_orders() {
    let (engine, sink) = engine();
    engine.register_instrument(&sym(CALL));
    engine.update_price(UNDERLYING, 5_000_000);

    let commands = sink.take();
    assert_eq!(commands.len(), 2, "first requote adds a bid and an ask");
    for command in &commands {
        match command {
            VenueCommand::AddOrder {
                symbol, account, ..
            } => {
                assert_eq!(symbol, &sym(CALL));
                assert!(
                    is_market_maker_account(account),
                    "every requote order carries the reserved MM account"
                );
            }
            other => panic!("expected an AddOrder, got {other:?}"),
        }
    }
}

/// #032: the pricer resolves time-to-expiry from the instrument's **absolute
/// `ExpirationDate::DateTime`** against the **venue clock**, never the wall clock.
/// Two requotes at the **same `venue_now_ms`** produce byte-identical limit prices
/// even though `Utc::now()` advances between the two calls — proving the clock-free
/// `DateTime − venue_now → days` seam is a pure function of the injected clock, the
/// precondition the replay oracle rests on.
#[test]
fn test_requote_prices_are_identical_under_a_fixed_venue_clock() {
    fn limit_prices(commands: &[VenueCommand]) -> Vec<(Side, Cents)> {
        commands
            .iter()
            .filter_map(|command| match command {
                VenueCommand::AddOrder {
                    side, limit_price, ..
                } => limit_price.map(|price| (*side, price)),
                _ => None,
            })
            .collect()
    }

    let (engine, sink) = engine();
    engine.register_instrument(&sym(CALL));

    engine.update_price(UNDERLYING, 5_000_000);
    let first = limit_prices(&sink.take());
    assert_eq!(first.len(), 2, "a requote quotes a bid and an ask");

    // A second requote at the identical venue clock — wall time has advanced since
    // the first call, the injected venue clock has not.
    engine.update_price(UNDERLYING, 5_000_000);
    let second = limit_prices(&sink.take());

    assert_eq!(
        first, second,
        "the pricer is a pure function of the venue clock — no wall-clock drift between requotes"
    );
}

#[test]
fn test_requote_commands_replay_to_a_crossing_fill_attributed_to_the_market_maker() {
    let (engine, sink) = engine();
    engine.register_instrument(&sym(CALL));
    engine.update_price(UNDERLYING, 5_000_000);
    let mut commands = sink.take();

    // The maker's ask price is the price a client buy will cross.
    let ask_price = commands
        .iter()
        .find_map(|c| match c {
            VenueCommand::AddOrder {
                side: Side::Sell,
                limit_price: Some(price),
                ..
            } => Some(*price),
            _ => None,
        })
        .expect("the requote placed an ask leg");

    // Append a client buy that crosses the resting maker ask.
    commands.push(client_add("client-1", Side::Buy, ask_price.get(), 1));

    let (outcomes, _top) = replay(&commands);
    // The last outcome (the client buy) captured a two-leg fill: maker = the MM.
    let last = outcomes.last().expect("an outcome for the client order");
    let fills = match last {
        VenueOutcome::Added { fills, .. } => fills,
        other => panic!("expected the client add to fill against the maker, got {other:?}"),
    };
    assert!(!fills.is_empty(), "the crossing client order filled");
    let maker = fills
        .iter()
        .find(|f| f.liquidity == fauxchange::LiquidityFlag::Maker)
        .expect("a maker leg");
    assert_eq!(
        maker.account,
        market_maker_account(),
        "the resting maker leg attributes to the venue-reserved market-maker account"
    );
}

#[test]
fn test_journaled_requotes_replay_to_identical_fills_and_top_of_book() {
    let (engine, sink) = engine();
    engine.register_instrument(&sym(CALL));
    engine.update_price(UNDERLYING, 5_000_000);
    let mut commands = sink.take();

    let ask_price = commands
        .iter()
        .find_map(|c| match c {
            VenueCommand::AddOrder {
                side: Side::Sell,
                limit_price: Some(price),
                ..
            } => Some(*price),
            _ => None,
        })
        .expect("an ask leg");
    commands.push(client_add("client-1", Side::Buy, ask_price.get(), 1));

    // The command stream (the journal content) actually contains the MM requote
    // adds — the determinism oracle covers generated liquidity, asserted from the
    // journal, not re-derived on replay.
    let mm_adds = commands
        .iter()
        .filter(|c| is_market_maker_command(c))
        .count();
    assert_eq!(mm_adds, 2, "the journal contains the two MM requote adds");

    // Replaying the SAME journal twice reproduces identical fills and top-of-book.
    let (outcomes_a, top_a) = replay(&commands);
    let (outcomes_b, top_b) = replay(&commands);
    assert_eq!(outcomes_a, outcomes_b, "identical fills on replay");
    assert_eq!(top_a, top_b, "identical top-of-book on replay");
}

#[tokio::test]
async fn test_appstate_wiring_routes_a_live_requote_onto_the_actor_and_journals_it() {
    let state = AppState::new(AppStateConfig::new([UNDERLYING])).expect("AppState with dev auth");
    let engine = state.market_maker();
    engine.set_venue_now_ms(VENUE_NOW_MS);
    engine.register_instrument(&sym(CALL));

    // A live price update requotes through the real ActorCommandSink → forwarder
    // → per-underlying actor → write-ahead journal.
    engine.update_price(UNDERLYING, 5_000_000);

    // The forwarder submits asynchronously; poll the journal (bounded) until the
    // MM requote adds are journaled.
    let mut mm_commands = 0;
    for _ in 0..200 {
        let snapshot = state
            .journal_snapshot(UNDERLYING)
            .await
            .expect("journal snapshot");
        mm_commands = snapshot
            .records
            .iter()
            .filter_map(|record| match record {
                JournalRecord::Command(journaled) => Some(&journaled.command),
                _ => None,
            })
            .filter(|command| is_market_maker_command(command))
            .count();
        if mm_commands >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        mm_commands >= 2,
        "the live requote's MM add orders reached the sequenced journal (found {mm_commands})"
    );
}

// ============================================================================
// #047 — halt scenarios on the sequenced path (executor-driven, replayable)
// ============================================================================

/// A resting client limit with an explicit time-in-force (for the TIF-eviction
/// lifecycle class).
fn client_add_tif(
    order_id: &str,
    side: Side,
    price: u64,
    quantity: u64,
    time_in_force: TimeInForce,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(CALL),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new("alice"),
        owner: Hash32([0x11; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force,
        stp_mode: STPMode::None,
    }
}

#[test]
fn test_order_into_halted_strike_is_rejected() {
    // Halt the strike, then submit an order into it: the sequenced instrument-status
    // registry gates the add to a journaled `Rejected`, and the same command stream
    // replays to the identical rejection (halt-reject replay).
    let commands = vec![
        VenueCommand::SetInstrumentStatus {
            symbol: sym(CALL),
            status: InstrumentStatus::Halted,
        },
        client_add("client-halted", Side::Buy, 100, 1),
    ];
    let (outcomes, _top) = replay(&commands);
    assert!(
        matches!(outcomes[0], VenueOutcome::InstrumentStatusChanged { .. }),
        "the halt transition is applied and journaled, got {:?}",
        outcomes[0]
    );
    match &outcomes[1] {
        VenueOutcome::Rejected { reason, .. } => {
            assert!(
                reason.contains("Halted") || reason.contains("not accepting"),
                "the reject names the halt: {reason}"
            );
        }
        other => panic!("an order into a halted strike must be rejected, got {other:?}"),
    }

    // Halt-reject replay: the identical stream reproduces the identical rejection.
    let (outcomes_b, _top_b) = replay(&commands);
    assert_eq!(outcomes, outcomes_b, "the halt reject replays identically");
}

#[test]
fn test_resume_after_halt_reaccepts_orders_on_replay() {
    // Halt then resume (Active): an order after resume is accepted, and the whole
    // stream replays identically.
    let commands = vec![
        VenueCommand::SetInstrumentStatus {
            symbol: sym(CALL),
            status: InstrumentStatus::Halted,
        },
        VenueCommand::SetInstrumentStatus {
            symbol: sym(CALL),
            status: InstrumentStatus::Active,
        },
        client_add("client-resumed", Side::Buy, 100, 1),
    ];
    let (outcomes, _top) = replay(&commands);
    assert!(
        matches!(outcomes[2], VenueOutcome::Added { .. }),
        "an order after resume is accepted, got {:?}",
        outcomes[2]
    );
    let (outcomes_b, _top_b) = replay(&commands);
    assert_eq!(outcomes, outcomes_b);
}

#[test]
fn test_journaled_market_maker_control_replays_identically() {
    // A `MarketMakerControl` is journaled; the replay/recovery executor installs NO
    // control sink, so it re-executes to an identical `ControlApplied` WITHOUT a live
    // engine — the requotes it induces are journaled as their own AddOrders.
    let commands = vec![
        VenueCommand::MarketMakerControl {
            spread_multiplier: Some(2.5),
            size_scalar: Some(0.5),
            directional_skew: Some(-0.25),
            enabled: Some(false),
        },
        client_add("client-1", Side::Buy, 100, 1),
    ];
    let (outcomes_a, top_a) = replay(&commands);
    assert!(
        matches!(&outcomes_a[0], VenueOutcome::ControlApplied { swept } if swept.is_empty()),
        "a sink-less replay derives ControlApplied (no MM orders rest, so nothing is swept), \
         got {:?}",
        outcomes_a[0]
    );
    let (outcomes_b, top_b) = replay(&commands);
    assert_eq!(
        outcomes_a, outcomes_b,
        "MarketMakerControl replays identically"
    );
    assert_eq!(top_a, top_b);
}

#[test]
fn test_lifecycle_transitions_replay_by_class() {
    // Two distinct lifecycle classes in one journal, both replay-stable:
    //   (A) contract expiry: MassCancel(incl GTC) -> Settling -> Expired, then an
    //       order into the expired strike is rejected;
    //   (B) intraday TIF eviction: a resting Gtd order evicted by EvictExpiredOrders.
    let gtd_deadline = 1_600_000_000_000u64;
    let now_past_deadline = EventTimestamp::new(gtd_deadline + 1);
    let commands = vec![
        // (B) a resting Gtd bid that will be TIF-evicted (added first, still Active).
        client_add_tif("gtd-bid", Side::Buy, 90, 1, TimeInForce::Gtd(gtd_deadline)),
        VenueCommand::EvictExpiredOrders {
            now_ms: now_past_deadline,
        },
        // (A) contract expiry sequence for the same strike.
        VenueCommand::MassCancel {
            scope: MassCancelScope::Book(sym(CALL)),
            cancel_type: MassCancelType::All,
            account: AccountId::new("venue-expiry-scheduler"),
        },
        VenueCommand::SetInstrumentStatus {
            symbol: sym(CALL),
            status: InstrumentStatus::Settling,
        },
        VenueCommand::SetInstrumentStatus {
            symbol: sym(CALL),
            status: InstrumentStatus::Expired,
        },
        // An order into the expired strike is rejected (terminal, not accepting).
        client_add("post-expiry", Side::Buy, 100, 1),
    ];

    let (outcomes_a, top_a) = replay(&commands);
    // (B) the intraday TIF-eviction sweep is journaled + replayable. NOTE: whether the
    // Gtd bid is actually removed depends on the per-leaf clock, which
    // option-chain-orderbook 0.7.0 does not inject (the known Day/GTD leaf-clock
    // upstream gap) — so we assert the sweep is a deterministic `Evicted` outcome, not
    // a non-empty eviction (which would fabricate an effect the pinned upstream cannot
    // yet deliver).
    assert!(
        matches!(&outcomes_a[1], VenueOutcome::Evicted { .. }),
        "the EvictExpiredOrders sweep is journaled as a deterministic Evicted outcome, got {:?}",
        outcomes_a[1]
    );
    // (A) the contract-expiry transitions applied and the post-expiry add is rejected.
    assert!(matches!(
        outcomes_a[3],
        VenueOutcome::InstrumentStatusChanged {
            status: InstrumentStatus::Settling,
            ..
        }
    ));
    assert!(matches!(
        outcomes_a[4],
        VenueOutcome::InstrumentStatusChanged {
            status: InstrumentStatus::Expired,
            ..
        }
    ));
    assert!(
        matches!(&outcomes_a[5], VenueOutcome::Rejected { .. }),
        "an order into an expired strike is rejected, got {:?}",
        outcomes_a[5]
    );

    // Both classes replay identically from the same journal.
    let (outcomes_b, top_b) = replay(&commands);
    assert_eq!(
        outcomes_a, outcomes_b,
        "lifecycle transitions replay by class"
    );
    assert_eq!(top_a, top_b);
}

// ============================================================================
// #047 — personas: per-instrument resolution, resting-liquidity shaping, jitter
// ============================================================================

/// A persona engine seeded deterministically for reproducible jitter.
fn persona_engine(seed: u64) -> (MarketMakerEngine, Arc<CollectingSink>) {
    let sink = Arc::new(CollectingSink::default());
    let engine = MarketMakerEngine::new(sink.clone(), LineageId::new("run-1"), Quoter::default())
        .with_run_seed(seed);
    engine.set_venue_now_ms(VENUE_NOW_MS);
    (engine, sink)
}

/// The (side, price, size) of every requote AddOrder in a command stream.
fn requote_legs(commands: &[VenueCommand]) -> Vec<(Side, u64, u64)> {
    commands
        .iter()
        .filter_map(|c| match c {
            VenueCommand::AddOrder {
                side,
                limit_price: Some(price),
                quantity,
                ..
            } => Some((*side, price.get(), *quantity)),
            _ => None,
        })
        .collect()
}

#[test]
fn test_persona_per_instrument_rests_finite_liquidity_and_a_taker_sweeps_partial_slices() {
    // A persona rests a FINITE quote size; a taker order larger than that finite size
    // fills only the partial slice real matching produces (no fill-probability draw).
    let (engine, sink) = persona_engine(7);
    let persona = PersonaConfig::try_new(200, 4, 1.0, 1.0, 0.0).expect("valid persona");
    engine.register_instrument_with_persona(&sym(CALL), None, "wide", persona);
    engine.update_price(UNDERLYING, 5_000_000);
    let mut commands = sink.take();

    let legs = requote_legs(&commands);
    let (ask_price, ask_size) = legs
        .iter()
        .find_map(|(side, price, size)| (*side == Side::Sell).then_some((*price, *size)))
        .expect("the persona quoted an ask leg");
    assert!(ask_size >= 1, "the persona rests a finite, positive size");
    assert!(
        ask_size <= persona.base_size,
        "resting size is bounded by base_size * size_scalar (+jitter trim): {ask_size} <= {}",
        persona.base_size
    );

    // A taker buy for MORE than the resting size sweeps it: real matching fills only
    // the finite resting slice, leaving the remainder to rest (Gtc) — NOT a synthetic
    // partial.
    let taker_qty = ask_size + 10;
    commands.push(client_add("taker", Side::Buy, ask_price, taker_qty));
    let (outcomes, _top) = replay(&commands);
    let filled: u64 = match outcomes.last().expect("taker outcome") {
        VenueOutcome::Added { fills, .. } => fills
            .iter()
            .filter(|f| f.liquidity == fauxchange::LiquidityFlag::Taker)
            .map(|f| f.quantity)
            .sum(),
        other => panic!("expected the taker add outcome, got {other:?}"),
    };
    assert_eq!(
        filled, ask_size,
        "the taker fills exactly the finite resting slice ({ask_size}), not more"
    );
}

#[test]
fn test_different_personas_quote_different_ladders_per_instrument() {
    // `tight` vs `wide_skewed` on the same underlying differ in their journaled
    // quotes — the per-instrument persona resolution lifts the one-global-persona
    // limit.
    let (engine, sink) = persona_engine(11);
    let tight = PersonaConfig::try_new(20, 10, 1.0, 0.8, 0.0).expect("tight");
    let wide = PersonaConfig::try_new(200, 10, 2.5, 0.8, -0.4).expect("wide");
    let put = "BTC-20351231-50000-P";
    engine.register_instrument_with_persona(&sym(CALL), None, "tight", tight);
    engine.register_instrument_with_persona(&sym(put), None, "wide_skewed", wide);
    engine.update_price(UNDERLYING, 5_000_000);
    let commands = sink.take();

    let spread_for = |symbol: &Symbol| -> u64 {
        let mut bid = None;
        let mut ask = None;
        for c in &commands {
            if let VenueCommand::AddOrder {
                symbol: s,
                side,
                limit_price: Some(price),
                ..
            } = c
                && s == symbol
            {
                match side {
                    Side::Buy => bid = Some(price.get()),
                    Side::Sell => ask = Some(price.get()),
                }
            }
        }
        ask.expect("ask") - bid.expect("bid")
    };
    let tight_spread = spread_for(&sym(CALL));
    let wide_spread = spread_for(&sym(put));
    assert!(
        wide_spread > tight_spread,
        "the wide persona quotes a wider spread than the tight one: {wide_spread} !> {tight_spread}"
    );
}

#[test]
fn test_persona_size_scalar_shapes_once_not_twice_under_neutral_global_overlay() {
    // #047 regression: with the engine's global config left at its NEUTRAL default
    // (1.0/1.0/0.0), a persona with `size_scalar = 0.5` rests HALF its base_size, not
    // a quarter. A quarter would mean the 0.5 was applied twice (persona AND global
    // overlay) — the seed-phase double-apply bug. Jitter only trims size by <= 20%, so
    // the effective size lands in `(base_size*0.5*0.8, base_size*0.5]` = `(40, 50]`,
    // strictly above the double-applied quarter (`base_size*0.25 = 25`).
    let (engine, sink) = persona_engine(3);
    // A `balanced`-style persona: base_size 100, size_scalar 0.5, otherwise neutral.
    let balanced = PersonaConfig::try_new(100, 100, 1.0, 0.5, 0.0).expect("valid persona");
    // The global config is untouched (neutral) — mirrors the seed phase after the fix.
    assert_eq!(
        engine.get_config().size_scalar,
        1.0,
        "global overlay is neutral"
    );

    engine.register_instrument_with_persona(&sym(CALL), None, "balanced", balanced);
    engine.update_price(UNDERLYING, 5_000_000);

    let legs = requote_legs(&sink.take());
    let bid_size = legs
        .iter()
        .find_map(|(side, _, size)| (*side == Side::Buy).then_some(*size))
        .expect("a bid leg");
    assert!(
        bid_size > 25 && bid_size <= 50,
        "size_scalar 0.5 shapes ONCE (~half of base_size 100, jitter-trimmed): \
         {bid_size} not in (25, 50] — a value <= 25 would be the 0.25 double-apply"
    );
}

#[test]
fn test_same_seed_persona_jitter_is_reproducible_end_to_end() {
    // Two engines with the SAME run seed produce byte-identical persona quotes
    // (jitter is a pure function of the seed + (persona, symbol)).
    fn quote_once(seed: u64) -> Vec<(Side, u64, u64)> {
        let (engine, sink) = persona_engine(seed);
        let persona = PersonaConfig::try_new(150, 8, 1.5, 0.9, 0.2).expect("persona");
        engine.register_instrument_with_persona(&sym(CALL), None, "jittered", persona);
        engine.update_price(UNDERLYING, 5_000_000);
        requote_legs(&sink.take())
    }
    assert_eq!(
        quote_once(42),
        quote_once(42),
        "the same seed reproduces the identical persona quote ladder"
    );
    assert_ne!(
        quote_once(1),
        quote_once(2),
        "different seeds diverge the persona jitter"
    );
}

#[tokio::test]
async fn test_sequenced_market_maker_control_applies_the_knob_via_appstate() {
    // The WS/REST control path journals a `MarketMakerControl` fanned to every actor;
    // the executor's apply seam pushes the knob onto the live engine (kill switch =
    // enabled:false). Sessions are NOT dropped — the engine stays registered.
    let state = AppState::new(AppStateConfig::new([UNDERLYING])).expect("AppState");
    let engine = state.market_maker();
    engine.set_venue_now_ms(VENUE_NOW_MS);
    engine.register_instrument(&sym(CALL));
    assert!(engine.is_enabled(), "quoting starts enabled");

    // A sequenced kill-switch control.
    state
        .submit(VenueCommand::MarketMakerControl {
            spread_multiplier: Some(2.0),
            size_scalar: None,
            directional_skew: None,
            enabled: Some(false),
        })
        .await
        .expect("venue-global MarketMakerControl fans out and is accepted");

    // The knob took effect on the live engine through the sequenced apply seam.
    let config = engine.get_config();
    assert!(!config.enabled, "the kill switch stopped quoting");
    assert_eq!(config.spread_multiplier, 2.0, "the spread knob was applied");
    // Sessions are intact: the instrument is still registered.
    assert_eq!(
        engine.registered_count(UNDERLYING),
        1,
        "sessions not dropped"
    );
}

/// An `AddOrder` submitted through the real `AppState` order path — used to rest
/// maker + client liquidity before a sequenced kill.
fn state_add(
    order_id: &str,
    account: AccountId,
    owner: Hash32,
    side: Side,
    price: u64,
    quantity: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(CALL),
        order_id: VenueOrderId::new(order_id),
        account,
        owner,
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

/// Rests a maker ask (owner = `MARKET_MAKER_OWNER`) and a client bid that must
/// survive an owner-scoped sweep, then submits a sequenced kill. Shared by the
/// coupled-sweep replay test and the crash-consistency journal test.
async fn rest_maker_and_client_then_kill(state: &AppState) {
    // A resting maker ask (owner = MARKET_MAKER_OWNER) and a resting client bid that
    // must survive the owner-scoped sweep. They do not cross (40_000 < 50_000).
    state
        .submit(state_add(
            "mm-ask",
            market_maker_account(),
            MARKET_MAKER_OWNER,
            Side::Sell,
            50_000,
            2,
        ))
        .await
        .expect("maker quote rests");
    state
        .submit(state_add(
            "cli-bid",
            AccountId::new("alice"),
            Hash32([0x11; 32]),
            Side::Buy,
            40_000,
            3,
        ))
        .await
        .expect("client order rests");

    // The sequenced kill: the executor COUPLES the owner-scoped sweep into this
    // control's own turn — no separate follow-on command.
    state
        .submit(VenueCommand::MarketMakerControl {
            spread_multiplier: None,
            size_scalar: None,
            directional_skew: None,
            enabled: Some(false),
        })
        .await
        .expect("kill fans out and is accepted");
}

#[tokio::test]
async fn test_sequenced_kill_couples_owner_scoped_sweep_and_replays_identically() {
    // #117 determinism: a sequenced kill journals ONE event per underlying whose
    // outcome is `ControlApplied { swept }` — the owner-scoped sweep is coupled into
    // the control's own turn, NOT a separate `MassCancel` command. Exporting the
    // journal and replaying it offline (the sink-less recovery driver) reproduces the
    // event AND the same cancellation of the maker's standing quote, proving the
    // coupled sweep is a replay-stable, crash-consistent re-derivation.
    let state = AppState::new(AppStateConfig::new([UNDERLYING])).expect("AppState");
    rest_maker_and_client_then_kill(&state).await;

    // Export the venue journal and replay it offline into a fresh registry — `Ok`
    // proves every re-derived event equalled the stored one (the integrity oracle).
    let bundle = state
        .export_bundle()
        .await
        .expect("export the journal bundle");
    let report = state
        .replay_bundle(&bundle)
        .await
        .expect("replay the journal");
    let replay = report.underlying(UNDERLYING).expect("BTC replay");

    // The kill control replays to `ControlApplied { swept }` WITHOUT a live engine,
    // and its swept legs re-cancel exactly the maker's own quote (owner-scoped).
    let swept = replay
        .events
        .iter()
        .find_map(|event| match (&event.command, &event.outcome) {
            (
                VenueCommand::MarketMakerControl {
                    enabled: Some(false),
                    ..
                },
                VenueOutcome::ControlApplied { swept },
            ) => Some(swept.clone()),
            _ => None,
        })
        .expect("the kill control replays with its coupled sweep");
    assert_eq!(
        swept.len(),
        1,
        "the replayed kill re-sweeps the one maker quote in the same turn"
    );
    assert_eq!(
        swept[0].owner, MARKET_MAKER_OWNER,
        "the coupled sweep is owner-scoped to the market maker"
    );
    assert!(
        swept.iter().all(|leg| leg.owner == MARKET_MAKER_OWNER),
        "only MARKET_MAKER_OWNER legs are swept — the client bid's owner is NOT swept"
    );

    // There is NO separate `MassCancel` event produced by the kill — the only
    // cancellation is the coupled `swept` on the control's own outcome.
    assert!(
        !replay
            .events
            .iter()
            .any(|event| matches!(event.command, VenueCommand::MassCancel { .. })),
        "a coupled kill journals NO separate MassCancel command"
    );

    // The reconstructed book proves the maker quote no longer rests; the client bid
    // survives — the cancellation is reproduced from the journal, not re-derived live.
    let top = replay.top_of_book(&sym(CALL));
    assert_eq!(
        top.best_ask, None,
        "the maker ask was swept — it no longer rests on the replayed book"
    );
    assert_eq!(
        top.best_bid,
        Some(Cents::new(40_000)),
        "the client bid is untouched by the owner-scoped sweep"
    );
}

#[tokio::test]
async fn test_sequenced_kill_journals_exactly_one_atomic_command_carrying_the_sweep() {
    // #117 crash-consistency at the journal level: a sequenced kill over a venue with
    // a resting MM quote appends exactly ONE command for the kill (the
    // `MarketMakerControl`) whose paired event carries the sweep. There is NO separate
    // `MassCancel` record — so no cross-command gap a crash could open between "control
    // applied" and "quotes cancelled". Control + cancel are atomic in one turn.
    let state = AppState::new(AppStateConfig::new([UNDERLYING])).expect("AppState");
    rest_maker_and_client_then_kill(&state).await;

    let snapshot = state
        .journal_snapshot(UNDERLYING)
        .await
        .expect("journal snapshot");

    // Exactly ONE write-ahead command record is the kill control.
    let kill_commands = snapshot
        .records
        .iter()
        .filter(|record| {
            matches!(
                record,
                JournalRecord::Command(journaled)
                    if matches!(
                        journaled.command,
                        VenueCommand::MarketMakerControl { enabled: Some(false), .. }
                    )
            )
        })
        .count();
    assert_eq!(
        kill_commands, 1,
        "the kill is exactly one atomic journaled command"
    );

    // NO record (command write-ahead OR paired event) is a `MassCancel` — the sweep is
    // not a separate command a crash could skip.
    assert!(
        !snapshot.records.iter().any(|record| match record {
            JournalRecord::Command(journaled) =>
                matches!(journaled.command, VenueCommand::MassCancel { .. }),
            JournalRecord::Event(event) => matches!(event.command, VenueCommand::MassCancel { .. }),
            JournalRecord::Epoch(_) => false,
        }),
        "a coupled kill journals NO separate MassCancel record"
    );

    // The kill's paired EVENT carries the coupled owner-scoped sweep.
    let swept = snapshot
        .records
        .iter()
        .find_map(|record| match record {
            JournalRecord::Event(event) => match (&event.command, &event.outcome) {
                (
                    VenueCommand::MarketMakerControl {
                        enabled: Some(false),
                        ..
                    },
                    VenueOutcome::ControlApplied { swept },
                ) => Some(swept.clone()),
                _ => None,
            },
            _ => None,
        })
        .expect("the kill control event carries the coupled sweep");
    assert_eq!(
        swept.len(),
        1,
        "the kill's own event carries the one swept quote"
    );
    assert!(
        swept.iter().all(|leg| leg.owner == MARKET_MAKER_OWNER),
        "only MARKET_MAKER_OWNER legs are swept"
    );
}

#[tokio::test]
async fn test_scheduled_expiry_roll_issues_sequenced_transitions() {
    // The scheduled-expiry driver issues the lifecycle transitions as sequenced
    // commands: an order into a settled/expired strike is then rejected.
    use fauxchange::simulation::{ExpiryPhase, ExpirySchedule};

    // A near-dated contract so the operational settlement instant is easy to cross.
    let near = "BTC-20250102-50000-C";
    let state = AppState::new(AppStateConfig::new([UNDERLYING])).expect("AppState");
    let engine = state.market_maker();
    engine.set_venue_now_ms(VENUE_NOW_MS);
    engine.register_instrument(&sym(near));
    engine.update_price(UNDERLYING, 5_000_000);
    // Wait for the requote to vivify the leaf into the shared symbol index.
    for _ in 0..200 {
        if state.symbol_index().symbols().iter().any(|s| s == near) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let schedule = ExpirySchedule::default();
    let exp = fauxchange::exchange::SymbolParser::parse(near)
        .expect("parse")
        .expiration()
        .to_owned();
    // Drive the clock past the operational settlement instant (08:30 UTC).
    let (_expiry, settle) = schedule
        .operational_instants(&exp)
        .expect("DateTime expiry");
    assert_eq!(
        schedule.phase_at(&exp, settle),
        Some(ExpiryPhase::Expired),
        "at settlement the expiration is Expired"
    );
    let report = state
        .run_expiry_roll(&schedule, settle)
        .await
        .expect("every required lifecycle command commits for the hosted underlying");
    assert!(
        report.commands_issued >= 1,
        "the roll issued sequenced lifecycle commands ({})",
        report.commands_issued
    );

    // An order into the now-expired strike: the reject is decided INSIDE the actor
    // turn (a journaled `Rejected` outcome that replays), but `submit` returns the
    // accepted+sequenced `Receipt` — the reject is NOT surfaced live through the
    // `Receipt`→`VenueOutcome` seam (a pre-existing limitation; we do not overclaim
    // live rejection). We assert the command was accepted and sequenced.
    let receipt = state
        .submit(client_add_into(near, Side::Buy, 100, 1))
        .await
        .expect("the order is accepted and sequenced (its Rejected outcome is journaled)");
    let _ = receipt;

    // A second roll at the same instant is idempotent (no forward transition).
    let again = state
        .run_expiry_roll(&schedule, settle)
        .await
        .expect("a repeat roll at the same instant advances nothing and cannot be partial");
    assert_eq!(again.commands_issued, 0, "a repeat roll issues nothing new");
}

/// A client add targeting an arbitrary symbol (for the expiry-roll integration).
fn client_add_into(symbol: &str, side: Side, price: u64, quantity: u64) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(symbol),
        order_id: VenueOrderId::new("expiry-probe"),
        account: AccountId::new("alice"),
        owner: Hash32([0x11; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}
