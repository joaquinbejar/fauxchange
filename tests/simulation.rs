//! Integration + determinism tests for the [`PriceSimulator`] on the sequenced
//! path ([016](../milestones/v0.1-backend-core/016-price-simulator-walks.md)).
//!
//! - A simulated session routes each walked step onto the per-underlying actor as
//!   a journaled `SimStep`, and the market maker it drives journals its requote
//!   `AddOrder`s on the same path — synthetic prices and the liquidity they induce
//!   are both in the journal.
//! - The maker's resting liquidity crosses a client order into a real fill.
//! - Replaying the journal into a **fresh** venue reproduces the exact price path
//!   and the requotes (journal-driven, **not** seed-regenerated): re-executing the
//!   recorded commands yields byte-identical events and executions.

use std::sync::Arc;
use std::time::Duration;

use fauxchange::exchange::{
    Cents, ExecutionsStore, Hash32, JournalRecord, LineageId, STPMode, Side, Symbol, TimeInForce,
    VenueCommand, VenueEvent, VenueOutcome, is_market_maker_command,
};
use fauxchange::models::{AccountId, OrderType, VenueOrderId};
use fauxchange::simulation::{AssetConfig, PriceSimulator, SimulationConfig, WalkTypeConfig};
use fauxchange::state::{AppState, AppStateConfig};

const UNDERLYING: &str = "BTC";
const CALL: &str = "BTC-20351231-50000-C";
const INITIAL: u64 = 5_000_000; // $50,000

fn sym(raw: &str) -> Symbol {
    Symbol::parse(raw).expect("valid fixture symbol")
}

/// A small, fast simulation config: a short horizon, one-minute virtual steps.
fn sim_config() -> SimulationConfig {
    SimulationConfig {
        horizon_steps: 16,
        ..SimulationConfig::default()
    }
}

/// Builds an in-process venue hosting `BTC`, with one GBM asset and the call
/// registered with the market maker so a walked step produces resting quotes.
fn venue_with_walk() -> Arc<AppState> {
    let config = AppStateConfig::new([UNDERLYING])
        .with_lineage(LineageId::new("run-1"))
        .with_assets(vec![AssetConfig::new(
            UNDERLYING,
            Cents::new(INITIAL),
            0.20,
            WalkTypeConfig::GeometricBrownian,
        )])
        .with_simulation(sim_config());
    let state = AppState::new(config).expect("AppState with dev auth");
    state.market_maker().register_instrument(&sym(CALL));
    state
}

/// The committed events of `underlying`'s journal, in sequence order.
async fn journal_events(state: &AppState, underlying: &str) -> Vec<VenueEvent> {
    state
        .journal_snapshot(underlying)
        .await
        .expect("journal snapshot")
        .records
        .into_iter()
        .filter_map(|record| match record {
            JournalRecord::Event(event) => Some(event),
            _ => None,
        })
        .collect()
}

/// Polls `underlying`'s journal (bounded) until it holds at least `sim_steps`
/// committed `SimStep`s and at least `mm_adds` market-maker requote commands —
/// the async forwarders commit off-thread, so the settle is a bounded poll.
async fn settle(
    state: &AppState,
    underlying: &str,
    sim_steps: usize,
    mm_adds: usize,
) -> Vec<VenueEvent> {
    for _ in 0..400 {
        let events = journal_events(state, underlying).await;
        let steps = events
            .iter()
            .filter(|event| matches!(event.command, VenueCommand::SimStep { .. }))
            .count();
        let adds = events
            .iter()
            .filter(|event| is_market_maker_command(&event.command))
            .count();
        if steps >= sim_steps && adds >= mm_adds {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    journal_events(state, underlying).await
}

// ============================================================================
// A simulated session journals SimSteps + requotes
// ============================================================================

#[tokio::test]
async fn test_simulated_session_journals_sim_steps_and_requotes() {
    let state = venue_with_walk();

    // Three deterministic steps: each walks BTC (a journaled SimStep) and drives
    // the market maker, whose requote AddOrders journal on the same actor.
    for _ in 0..3 {
        state.simulator().step_once();
    }
    let events = settle(&state, UNDERLYING, 3, 2).await;

    let sim_steps: Vec<&VenueEvent> = events
        .iter()
        .filter(|event| matches!(event.command, VenueCommand::SimStep { .. }))
        .collect();
    assert!(
        sim_steps.len() >= 3,
        "every walked step entered the sequenced path as a journaled SimStep (found {})",
        sim_steps.len()
    );
    // Each SimStep carries the venue-clock now_ms and a cents price.
    for event in &sim_steps {
        match &event.command {
            VenueCommand::SimStep {
                now_ms,
                price,
                underlying,
                ..
            } => {
                assert_eq!(underlying, UNDERLYING);
                assert!(
                    now_ms.get() >= 1_735_689_600_000,
                    "now_ms is the venue clock"
                );
                assert!(price.get() > 0, "the walked price is positive cents");
            }
            other => panic!("expected a SimStep, got {other:?}"),
        }
        assert!(matches!(event.outcome, VenueOutcome::ControlApplied { .. }));
    }

    let mm_adds = events
        .iter()
        .filter(|event| {
            is_market_maker_command(&event.command)
                && matches!(event.command, VenueCommand::AddOrder { .. })
        })
        .count();
    assert!(
        mm_adds >= 2,
        "the SimStep drove the market maker, whose requote AddOrders are journaled (found {mm_adds})"
    );
}

// ============================================================================
// The maker's resting liquidity crosses a client order into a fill
// ============================================================================

#[tokio::test]
async fn test_simulated_session_produces_a_crossing_fill() {
    let state = venue_with_walk();

    // Walk a few steps so the maker has resting bid/ask liquidity in the book.
    for _ in 0..3 {
        state.simulator().step_once();
    }
    settle(&state, UNDERLYING, 3, 2).await;

    // A client buy priced far above any plausible ask crosses the maker's resting
    // ask (filling at the maker's price), producing a real fill.
    let crossing = VenueCommand::AddOrder {
        symbol: sym(CALL),
        order_id: VenueOrderId::new("client-taker-1"),
        account: AccountId::new("client"),
        owner: Hash32([0x42; 32]),
        client_order_id: None,
        side: Side::Buy,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(100_000_000)),
        quantity: 1,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    };
    state.submit(crossing).await.expect("crossing buy submits");

    // The crossing match records its two legs (maker + taker) in the shared store.
    let mut filled = false;
    for _ in 0..200 {
        if !state.executions().is_empty() {
            filled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        filled,
        "the client order crossed the maker's synthetic resting liquidity into a fill"
    );
}

// ============================================================================
// Journal-driven replay reproduces the exact price path + requotes
// ============================================================================

#[tokio::test]
async fn test_journal_replay_reproduces_price_path_and_requotes() {
    // --- venue A: run a simulated session that also produces a fill -----------
    let state_a = venue_with_walk();
    for _ in 0..3 {
        state_a.simulator().step_once();
    }
    settle(&state_a, UNDERLYING, 3, 2).await;
    state_a
        .submit(VenueCommand::AddOrder {
            symbol: sym(CALL),
            order_id: VenueOrderId::new("client-taker-1"),
            account: AccountId::new("client"),
            owner: Hash32([0x42; 32]),
            client_order_id: None,
            side: Side::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(100_000_000)),
            quantity: 1,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        })
        .await
        .expect("crossing buy submits");

    // Let the fill and any trailing requotes settle, then snapshot the journal.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let events_a = journal_events(&state_a, UNDERLYING).await;
    assert!(!events_a.is_empty(), "venue A produced a journal");

    // --- venue B: a FRESH venue that replays A's journaled commands -----------
    // The replay driver mutes the live market maker so a replayed price never
    // re-derives a cascading requote — the requotes are already journaled and are
    // replayed directly (journal-driven, not seed-regenerated).
    let state_b =
        AppState::new(AppStateConfig::new([UNDERLYING]).with_lineage(LineageId::new("run-1")))
            .expect("AppState with dev auth");
    state_b.market_maker().set_muted(true);

    // Re-execute A's commands, in A's exact per-underlying order, onto B.
    for event in &events_a {
        state_b
            .submit(event.command.clone())
            .await
            .expect("replayed command submits");
    }
    let events_b = journal_events(&state_b, UNDERLYING).await;

    // The journal reproduces byte-for-byte: same commands, same outcomes, same
    // sequence and venue timestamp — the bounded oracle over the whole session.
    assert_eq!(
        events_b, events_a,
        "replaying the journal reproduces identical events (commands + outcomes)"
    );

    // The price path is reproduced exactly (an explicit projection of the above).
    let path = |events: &[VenueEvent]| -> Vec<u64> {
        events
            .iter()
            .filter_map(|event| match &event.command {
                VenueCommand::SimStep { price, .. } => Some(price.get()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(
        path(&events_b),
        path(&events_a),
        "the replayed SimStep price path is identical"
    );
    assert!(
        !path(&events_a).is_empty(),
        "the session walked at least one step"
    );

    // The requotes reproduce the same fills — the shared executions match.
    assert_eq!(
        state_b.executions().len(),
        state_a.executions().len(),
        "replay reproduces the same fills without cascading duplicate orders"
    );
    assert!(
        !state_a.executions().is_empty(),
        "the session produced a fill to reproduce"
    );
}

// ============================================================================
// Causality: a requote is never sequenced before its causing SimStep
// ============================================================================

#[tokio::test]
async fn test_requote_is_never_sequenced_before_its_causing_sim_step() {
    let state = venue_with_walk();

    // Walk a few steps: each is a journaled SimStep that, once sequenced, drives
    // the market maker's requote AddOrders onto the SAME per-underlying journal.
    for _ in 0..3 {
        state.simulator().step_once();
    }
    let events = settle(&state, UNDERLYING, 3, 2).await;

    // The journal is the single per-underlying total order (the determinism
    // oracle). Locate the first SimStep in that order.
    let first_sim = events
        .iter()
        .position(|event| matches!(event.command, VenueCommand::SimStep { .. }))
        .expect("the session journaled at least one SimStep");

    // The maker holds no price until a CONFIRMED SimStep drives it, so every
    // market-maker requote command must be sequenced strictly after the first
    // SimStep — a requote can never be journaled before its causing step. (Before
    // the fix the SimStep and its derived requotes raced through two independent
    // forwarders, so a requote could be journaled first.)
    let requote_indices: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| is_market_maker_command(&event.command))
        .map(|(idx, _)| idx)
        .collect();
    assert!(
        !requote_indices.is_empty(),
        "the SimStep drove the maker to journal at least one requote"
    );
    assert!(
        requote_indices.iter().all(|&idx| idx > first_sim),
        "every requote (indices {requote_indices:?}) is sequenced after the first SimStep (idx {first_sim})"
    );
    assert!(
        events[..first_sim]
            .iter()
            .all(|event| !is_market_maker_command(&event.command)),
        "no market-maker requote is sequenced before the first SimStep"
    );
}

// ============================================================================
// A programmatic set_price override is journaled the same way as a walk step
// ============================================================================

#[tokio::test]
async fn test_simulator_set_price_override_is_journaled_as_a_sim_step() {
    let state = venue_with_walk();
    let simulator: &Arc<PriceSimulator> = state.simulator();

    simulator
        .set_price(UNDERLYING, Cents::new(4_200_000))
        .expect("BTC is a configured asset");
    assert_eq!(simulator.get_price(UNDERLYING), Some(Cents::new(4_200_000)));

    // The override entered the sequenced path as a journaled SimStep (never a bare
    // write), exactly like a walked step.
    let events = settle(&state, UNDERLYING, 1, 0).await;
    let overridden = events.iter().any(|event| {
        matches!(
            &event.command,
            VenueCommand::SimStep { underlying, price, .. }
                if underlying == UNDERLYING && *price == Cents::new(4_200_000)
        )
    });
    assert!(
        overridden,
        "the manual set_price override is journaled as a SimStep, not a bare price write"
    );
}
