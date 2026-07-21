//! Integration + determinism tests for the executions / positions stores
//! ([008](../milestones/v0.1-backend-core/008-executions-positions-stores.md)),
//! exercised through the **public** surface from an external crate.
//!
//! - `test_orders_through_matching_populate_stores` — drive crossing orders
//!   through the real single-writer actor + `MatchingExecutor`, and confirm the
//!   committed fills fan out into both stores: two execution legs (shared
//!   `execution_id`, distinct accounts) and the two counterparties' positions.
//! - `test_store_projection_matches_execution_report_golden` /
//!   `test_store_projection_matches_positions_golden` — the store's projection of
//!   a committed match reproduces the committed `rest/*.json` goldens.
//! - `test_executions_log_is_deterministic_function_of_journal` — the same
//!   command stream yields byte-identical executions on a fresh store (the
//!   event-sourced determinism the #017 harness builds on).

use std::sync::Arc;

use fauxchange::exchange::{
    ActorConfig, Cents, CommandExecutor, EventTimestamp, ExecutionContext, ExecutionFilter,
    ExecutionsStore, FanOut, Fill as VenueFill, FixedClock, Hash32, InMemoryExecutionsStore,
    InMemoryPositionsStore, InMemoryVenueJournal, JournalHeader, LineageId, MarkPriceBook,
    MarkSource, MatchingExecutor, PositionsStore, STPMode, SequenceNumber, Side, SignedCents,
    StoreFanOut, Symbol, TimeInForce, VenueClock, VenueCommand, VenueEvent, VenueOutcome,
    spawn_matching_actor,
};
use fauxchange::{AccountId, ExecutionId, ExecutionRecord, LiquidityFlag, OrderType};

const UNDERLYING: &str = "BTC";
const CLOCK: FixedClock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));

fn sym() -> Symbol {
    match Symbol::parse("BTC-20240329-50000-C") {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
    }
}

fn journal(lineage: &LineageId) -> InMemoryVenueJournal {
    InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()))
}

#[allow(clippy::too_many_arguments)]
fn add(
    lineage: &LineageId,
    sequence: u64,
    account: &str,
    owner_byte: u8,
    side: Side,
    price: u64,
    quantity: u64,
    tif: TimeInForce,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(),
        order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(sequence), 0),
        account: AccountId::new(account),
        owner: Hash32([owner_byte; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: tif,
        stp_mode: STPMode::None,
    }
}

/// Loads and parses a committed golden fixture under `tests/golden/`.
fn load_golden(relative: &str) -> serde_json::Value {
    let path = format!("{}/tests/golden/{}", env!("CARGO_MANIFEST_DIR"), relative);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) => panic!("failed to read golden {path}: {e}"),
    };
    match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(e) => panic!("failed to parse golden {path}: {e}"),
    }
}

// ---- orders → matching → stores (through the real actor) ------------------

#[tokio::test]
async fn test_orders_through_matching_populate_stores() {
    let lineage = LineageId::new("run-1");
    let config = ActorConfig::new(UNDERLYING, lineage.clone(), 32);
    let executions = Arc::new(InMemoryExecutionsStore::new());
    let positions = Arc::new(InMemoryPositionsStore::new());
    let marks = Arc::new(MarkPriceBook::new());
    let fan = StoreFanOut::new(
        Arc::clone(&executions),
        Arc::clone(&positions),
        Arc::clone(&marks),
    );
    let (handle, join) = spawn_matching_actor(config, journal(&lineage), fan, CLOCK);

    // Seed a resting maker (sequence 0), then cross it with a taker (sequence 1).
    for command in [
        add(
            &lineage,
            0,
            "maker",
            0x11,
            Side::Sell,
            50_000,
            2,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            1,
            "taker",
            0x22,
            Side::Buy,
            50_000,
            2,
            TimeInForce::Gtc,
        ),
    ] {
        if let Err(e) = handle.submit(command).await {
            panic!("submit failed: {e}");
        }
    }

    // Both legs of the one match landed in the executions store, sharing the
    // aggressor's execution id, each attributed to its own account.
    assert_eq!(executions.len(), 2, "one match records two legs");
    let execution_id = lineage.execution_id(UNDERLYING, SequenceNumber::new(1), 0);
    let maker_leg = executions
        .get(&execution_id, &AccountId::new("maker"))
        .expect("get maker leg")
        .expect("a recorded maker leg");
    let taker_leg = executions
        .get(&execution_id, &AccountId::new("taker"))
        .expect("get taker leg")
        .expect("a recorded taker leg");
    assert_eq!(maker_leg.execution_id, taker_leg.execution_id);
    assert_eq!(maker_leg.liquidity, LiquidityFlag::Maker);
    assert_eq!(taker_leg.liquidity, LiquidityFlag::Taker);
    assert_eq!(maker_leg.account, AccountId::new("maker"));
    assert_eq!(taker_leg.account, AccountId::new("taker"));
    // No fee schedule is configured on this path, so both fees are zero.
    assert_eq!(maker_leg.fee_cents, SignedCents::new(0));
    assert_eq!(taker_leg.fee_cents, SignedCents::new(0));
    assert_eq!(maker_leg.underlying_sequence, SequenceNumber::new(1));

    // The positions fold gives each counterparty its own signed net position.
    let maker_pos = positions
        .get(&AccountId::new("maker"), &sym(), None)
        .expect("get maker position")
        .expect("a maker position");
    let taker_pos = positions
        .get(&AccountId::new("taker"), &sym(), None)
        .expect("get taker position")
        .expect("a taker position");
    assert_eq!(maker_pos.net_quantity, -2);
    assert_eq!(taker_pos.net_quantity, 2);
    assert_eq!(maker_pos.avg_price, Cents::new(50_000));
    assert_eq!(taker_pos.avg_price, Cents::new(50_000));
    assert_eq!(maker_pos.realized_pnl, SignedCents::new(0));

    // The mark book was fed the trade print (50_000) on fan-out; marking against
    // it is a live-only projection.
    let mark = marks.mark(&sym());
    assert_eq!(
        mark,
        Some(Cents::new(50_000)),
        "the first trade seeds the mark"
    );
    let marked = positions
        .get(&AccountId::new("taker"), &sym(), mark)
        .expect("get marked position")
        .expect("a marked position");
    assert_eq!(marked.current_price, Some(Cents::new(50_000)));
    // Marked at the entry price, unrealized is zero.
    assert_eq!(marked.unrealized_pnl, Some(SignedCents::new(0)));

    drop(handle);
    match join.await {
        Ok(()) => {}
        Err(e) => panic!("actor did not shut down cleanly: {e}"),
    }
}

// ---- the store projection reproduces the committed goldens ----------------

/// Builds one crossing match's committed event with two linked fill legs (a maker
/// rebate, a taker fee) — the golden fixtures' fan-out input.
fn golden_match_event() -> VenueEvent {
    let lineage = LineageId::new("run-1");
    let seq = SequenceNumber::new(7);
    let execution_id = lineage.execution_id(UNDERLYING, seq, 0);
    let command = add(
        &lineage,
        7,
        "taker-acct",
        0x22,
        Side::Buy,
        50_000,
        2,
        TimeInForce::Gtc,
    );
    let outcome = VenueOutcome::Added {
        fills: vec![
            VenueFill {
                execution_id: execution_id.clone(),
                order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(1), 0),
                account: AccountId::new("maker-acct"),
                owner: Hash32([0x11; 32]),
                side: Side::Sell,
                liquidity: LiquidityFlag::Maker,
                price: Cents::new(50_000),
                quantity: 2,
                fee: SignedCents::new(-10),
            },
            VenueFill {
                execution_id,
                order_id: lineage.venue_order_id(UNDERLYING, seq, 0),
                account: AccountId::new("taker-acct"),
                owner: Hash32([0x22; 32]),
                side: Side::Buy,
                liquidity: LiquidityFlag::Taker,
                price: Cents::new(50_000),
                quantity: 2,
                fee: SignedCents::new(15),
            },
        ],
        resting_quantity: 0,
        stp_cancelled: vec![],
    };
    VenueEvent::new(
        seq,
        EventTimestamp::new(1_700_000_000_000),
        command,
        outcome,
    )
}

#[tokio::test]
async fn test_store_projection_matches_execution_report_golden() {
    let executions = Arc::new(InMemoryExecutionsStore::new());
    let mut fan = StoreFanOut::new(
        Arc::clone(&executions),
        Arc::new(InMemoryPositionsStore::new()),
        Arc::new(MarkPriceBook::new()),
    );
    fan.emit(&golden_match_event());

    let execution_id = ExecutionId::new("run-1:BTC:7:0");
    let maker = executions
        .get(&execution_id, &AccountId::new("maker-acct"))
        .expect("get maker")
        .expect("maker leg");
    let taker = executions
        .get(&execution_id, &AccountId::new("taker-acct"))
        .expect("get taker")
        .expect("taker leg");
    let report = vec![maker, taker];

    let produced = serde_json::to_value(&report).expect("serialise execution report");
    assert_eq!(
        produced,
        load_golden("rest/execution_report.json"),
        "the store's executions projection must match the committed golden"
    );
}

#[tokio::test]
async fn test_store_projection_matches_positions_golden() {
    let positions = Arc::new(InMemoryPositionsStore::new());
    let mut fan = StoreFanOut::new(
        Arc::new(InMemoryExecutionsStore::new()),
        Arc::clone(&positions),
        Arc::new(MarkPriceBook::new()),
    );
    fan.emit(&golden_match_event());

    let symbol = sym();
    let mark = Some(Cents::new(50_500));
    let maker = positions
        .get(&AccountId::new("maker-acct"), &symbol, mark)
        .expect("get maker")
        .expect("maker position");
    let taker = positions
        .get(&AccountId::new("taker-acct"), &symbol, mark)
        .expect("get taker")
        .expect("taker position");
    let report = vec![maker, taker];

    let produced = serde_json::to_value(&report).expect("serialise positions");
    assert_eq!(
        produced,
        load_golden("rest/positions.json"),
        "the store's positions projection must match the committed golden"
    );
}

// ---- determinism: the executions log is a function of the journal ---------

/// Drives a command stream through a fresh executor + store fan-out and returns
/// every recorded execution leg for both accounts, in journal order.
fn executions_from(commands: &[VenueCommand], lineage: &LineageId) -> Vec<ExecutionRecord> {
    let executions = Arc::new(InMemoryExecutionsStore::new());
    let mut executor = MatchingExecutor::new(UNDERLYING);
    let mut fan = StoreFanOut::new(
        Arc::clone(&executions),
        Arc::new(InMemoryPositionsStore::new()),
        Arc::new(MarkPriceBook::new()),
    );
    for (index, command) in commands.iter().enumerate() {
        let sequence = SequenceNumber::new(index as u64);
        let outcome = executor.execute(ExecutionContext {
            underlying: UNDERLYING,
            lineage_id: lineage,
            sequence,
            venue_ts: CLOCK.now_ms(),
            command,
        });
        let event = VenueEvent::new(sequence, CLOCK.now_ms(), command.clone(), outcome);
        fan.emit(&event);
    }
    let mut all = Vec::new();
    for account in ["m1", "m2", "t1", "b1"] {
        let mut legs = executions
            .list(&AccountId::new(account), &ExecutionFilter::default())
            .expect("list executions");
        all.append(&mut legs);
    }
    all
}

#[test]
fn test_executions_log_is_deterministic_function_of_journal() {
    let lineage = LineageId::new("run-1");
    let commands = vec![
        add(
            &lineage,
            0,
            "m1",
            0x11,
            Side::Sell,
            50_000,
            3,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            1,
            "m2",
            0x12,
            Side::Sell,
            50_100,
            2,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            2,
            "t1",
            0x22,
            Side::Buy,
            50_050,
            4,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            3,
            "b1",
            0x33,
            Side::Buy,
            49_900,
            1,
            TimeInForce::Gtc,
        ),
    ];

    let run_a = executions_from(&commands, &lineage);
    let run_b = executions_from(&commands, &lineage);

    assert_eq!(
        run_a, run_b,
        "the same journal must yield an identical executions log"
    );
    // The fixture actually crossed (guards against a vacuous pass).
    assert!(
        !run_a.is_empty(),
        "the command stream must produce at least one fill leg"
    );
}
