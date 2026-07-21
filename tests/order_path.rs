//! Integration + binding-determinism tests for the sequenced order path
//! ([007](../milestones/v0.1-backend-core/007-order-path-onto-matching.md)),
//! exercised through the **public** surface from an external crate.
//!
//! - `test_seed_orders_matching_captured_fills_round_trip` — seed a resting
//!   maker, cross it with a taker through the in-process actor, and confirm the
//!   two linked fill legs (maker recovered from the journaled add, shared
//!   `execution_id`, per-leg account) are captured into the paired `VenueEvent`.
//! - `test_same_journal_reconstructs_identical_fills_and_top_of_book` — the
//!   **binding** determinism assertion this sequenced-path change ships with: the
//!   same command stream captures identical fills and reconstructs an identical
//!   top-of-book on a fresh instance (the full harness lands with #017).
//! - `test_journal_replay_reproduces_captured_outcomes` — replaying the journaled
//!   commands through a fresh executor reproduces the journaled outcomes.

use fauxchange::exchange::{
    ActorConfig, CommandExecutor, EventTimestamp, ExecutionContext, FixedClock,
    InMemoryVenueJournal, JournalHeader, JournalRecord, LineageId, MatchingExecutor, NoopFanOut,
    SequenceNumber, Symbol, TopOfBook, VenueClock, VenueCommand, VenueOutcome,
    spawn_matching_actor,
};
use fauxchange::exchange::{Hash32, STPMode, Side, TimeInForce};
use fauxchange::{AccountId, ClientOrderId, LiquidityFlag, OrderType};

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

/// A limit add whose venue order id is the deterministic grammar id for the
/// sequence the actor will assign (submissions are serial, so `sequence` matches).
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
        client_order_id: Some(ClientOrderId::new(format!("c-{sequence}"))),
        side,
        order_type: OrderType::Limit,
        limit_price: Some(fauxchange::exchange::Cents::new(price)),
        quantity,
        time_in_force: tif,
        stp_mode: STPMode::None,
    }
}

fn market(
    lineage: &LineageId,
    sequence: u64,
    account: &str,
    side: Side,
    quantity: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(),
        order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(sequence), 0),
        account: AccountId::new(account),
        owner: Hash32([0xAA; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Market,
        limit_price: None,
        quantity,
        time_in_force: TimeInForce::Ioc,
        stp_mode: STPMode::None,
    }
}

// ---- seed → orders → matching → captured fills (through the actor) --------

#[tokio::test]
async fn test_seed_orders_matching_captured_fills_round_trip() {
    let lineage = LineageId::new("run-1");
    let config = ActorConfig::new(UNDERLYING, lineage.clone(), 32);
    let (handle, join) = spawn_matching_actor(config, journal(&lineage), NoopFanOut, CLOCK);

    // Seed a resting maker (sequence 0), then cross it with a taker (sequence 1).
    match handle
        .submit(add(
            &lineage,
            0,
            "maker",
            0x11,
            Side::Sell,
            50_000,
            2,
            TimeInForce::Gtc,
        ))
        .await
    {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0)),
        Err(e) => panic!("seed submit failed: {e}"),
    }
    match handle
        .submit(add(
            &lineage,
            1,
            "taker",
            0x22,
            Side::Buy,
            50_000,
            2,
            TimeInForce::Gtc,
        ))
        .await
    {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence, SequenceNumber::new(1)),
        Err(e) => panic!("cross submit failed: {e}"),
    }

    // The paired event at sequence 1 carries the two-leg fill, captured losslessly.
    let snapshot = match handle.snapshot().await {
        Ok(s) => s,
        Err(e) => panic!("snapshot failed: {e}"),
    };
    let event = snapshot
        .records
        .iter()
        .find_map(|record| match record {
            JournalRecord::Event(event) if event.underlying_sequence == SequenceNumber::new(1) => {
                Some(event)
            }
            _ => None,
        })
        .expect("a paired event at sequence 1");

    match &event.outcome {
        VenueOutcome::Added {
            fills,
            resting_quantity,
            ..
        } => {
            assert_eq!(*resting_quantity, 0);
            assert_eq!(fills.len(), 2, "one match → two linked legs");
            let maker = &fills[0];
            let taker = &fills[1];
            assert_eq!(maker.execution_id, taker.execution_id);
            assert_eq!(maker.liquidity, LiquidityFlag::Maker);
            assert_eq!(taker.liquidity, LiquidityFlag::Taker);
            // The resting maker's identity is recovered from the journaled add
            // (sequence 0), not live book state.
            assert_eq!(maker.account, AccountId::new("maker"));
            assert_eq!(taker.account, AccountId::new("taker"));
            assert_eq!(
                maker.order_id,
                lineage.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0)
            );
        }
        other => panic!("expected a captured Added fill event, got {other:?}"),
    }

    drop(handle);
    match join.await {
        Ok(()) => {}
        Err(e) => panic!("actor did not shut down cleanly: {e}"),
    }
}

// ---- binding determinism: same journal → same fills + top-of-book --------

/// Drives a command stream through a fresh executor, returning the captured
/// outcomes and the final top-of-book — the determinism oracle's two artifacts.
fn replay_direct(commands: &[VenueCommand], lineage: &LineageId) -> (Vec<VenueOutcome>, TopOfBook) {
    let mut executor = MatchingExecutor::new(UNDERLYING);
    let outcomes = commands
        .iter()
        .enumerate()
        .map(|(index, command)| {
            executor.execute(ExecutionContext {
                underlying: UNDERLYING,
                lineage_id: lineage,
                sequence: SequenceNumber::new(index as u64),
                venue_ts: CLOCK.now_ms(),
                command,
            })
        })
        .collect();
    (outcomes, executor.top_of_book(&sym()))
}

#[test]
fn test_same_journal_reconstructs_identical_fills_and_top_of_book() {
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
            "b1",
            0x33,
            Side::Buy,
            49_900,
            5,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            3,
            "t1",
            0x22,
            Side::Buy,
            50_000,
            2,
            TimeInForce::Gtc,
        ),
        market(&lineage, 4, "t2", Side::Buy, 1),
    ];

    let (outcomes_a, top_a) = replay_direct(&commands, &lineage);
    let (outcomes_b, top_b) = replay_direct(&commands, &lineage);

    assert_eq!(
        outcomes_a, outcomes_b,
        "same journal must capture identical fills"
    );
    assert_eq!(
        top_a, top_b,
        "same journal must reconstruct identical top-of-book"
    );
    // The stream actually produced fills (guards against a vacuous pass).
    assert!(
        outcomes_a
            .iter()
            .any(|o| matches!(o, VenueOutcome::Added { fills, .. } if !fills.is_empty())),
        "the fixture must exercise a real match"
    );
}

// ---- journal replay reproduces the journaled outcomes --------------------

#[tokio::test]
async fn test_journal_replay_reproduces_captured_outcomes() {
    let lineage = LineageId::new("run-1");
    let config = ActorConfig::new(UNDERLYING, lineage.clone(), 32);
    let (handle, join) = spawn_matching_actor(config, journal(&lineage), NoopFanOut, CLOCK);

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
            "t1",
            0x22,
            Side::Buy,
            50_000,
            2,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            2,
            "b1",
            0x33,
            Side::Buy,
            49_900,
            4,
            TimeInForce::Gtc,
        ),
    ];
    for command in &commands {
        if let Err(e) = handle.submit(command.clone()).await {
            panic!("submit failed: {e}");
        }
    }

    let snapshot = match handle.snapshot().await {
        Ok(s) => s,
        Err(e) => panic!("snapshot failed: {e}"),
    };

    // Replay the journaled COMMAND records through a fresh executor and assert the
    // reconstructed outcome equals the one journaled on the EVENT record — the
    // "same journal → same fills" oracle, scoped to per-underlying state.
    let mut replay = MatchingExecutor::new(UNDERLYING);
    for record in &snapshot.records {
        if let JournalRecord::Command(journal_command) = record {
            let sequence = journal_command.sequence;
            let reconstructed = replay.execute(ExecutionContext {
                underlying: UNDERLYING,
                lineage_id: &lineage,
                sequence,
                venue_ts: journal_command.venue_ts,
                command: &journal_command.command,
            });
            let journaled = snapshot
                .records
                .iter()
                .find_map(|r| match r {
                    JournalRecord::Event(event) if event.underlying_sequence == sequence => {
                        Some(&event.outcome)
                    }
                    _ => None,
                })
                .expect("a paired event for each replayed command");
            assert_eq!(
                &reconstructed,
                journaled,
                "replay must reproduce the journaled outcome at sequence {}",
                sequence.get()
            );
        }
    }

    drop(handle);
    let _ = join.await;
}
