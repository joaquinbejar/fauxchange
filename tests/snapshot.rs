//! Integration tests for book **snapshot + restore** over a consistent cut with
//! a fresh journal epoch
//! ([009](../milestones/v0.1-backend-core/009-snapshot-restore.md),
//! [02 §9](../docs/02-matching-architecture.md#9-snapshots-and-restore)),
//! exercised through the **public** surface from an external crate.
//!
//! The `snapshot → mutate → restore` round-trip asserts all six acceptance
//! criteria: (a) the books return to the snapshot state, (b) a `SnapshotRestored`
//! marker opens a fresh epoch, (c) the `underlying_sequence` continues (does not
//! reset), (d) executions / positions / idempotency reconcile to the same
//! instant, (e) a metadata mismatch is rejected, and (f) a mid-restore fault
//! rolls back all four stores. A final determinism test scopes replay
//! reproducibility **within** an epoch and asserts the restore boundary is
//! **out of scope**, not silently divergent.

use std::sync::Arc;

use fauxchange::exchange::{
    ActorConfig, Cents, CommandExecutor, EventTimestamp, ExecutionFilter, ExecutionsStore,
    FixedClock, Hash32, InMemoryExecutionsStore, InMemoryPositionsStore, InMemoryVenueJournal,
    JournalHeader, JournalRecord, LineageId, MarkPriceBook, MatchingExecutor, PositionsStore,
    RecordKind, RestingOrderCapture, STPMode, SequenceNumber, Side, SnapshotError, StoreFanOut,
    Symbol, TimeInForce, UnderlyingActor, VenueCommand, VenueJournal, VenueSnapshot,
};
use fauxchange::{AccountId, ClientOrderId, OrderType};

const UNDERLYING: &str = "BTC";
const CONFIG_FP: &str = "cfg-1";
const CLOCK: FixedClock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));

/// The default order-path wiring: the real executor over the in-memory stores.
type Venue = UnderlyingActor<
    InMemoryVenueJournal,
    MatchingExecutor,
    StoreFanOut<InMemoryExecutionsStore, InMemoryPositionsStore>,
    FixedClock,
>;

fn sym() -> Symbol {
    match Symbol::parse("BTC-20240329-50000-C") {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
    }
}

fn lineage() -> LineageId {
    LineageId::new("run-1")
}

/// Builds a venue actor over shared in-memory stores, returning the actor and the
/// shared store handles (for post-restore reconciliation reads).
fn build_venue(
    lineage: &LineageId,
) -> (
    Venue,
    Arc<InMemoryExecutionsStore>,
    Arc<InMemoryPositionsStore>,
) {
    let executions = Arc::new(InMemoryExecutionsStore::new());
    let positions = Arc::new(InMemoryPositionsStore::new());
    let fan = StoreFanOut::new(
        Arc::clone(&executions),
        Arc::clone(&positions),
        Arc::new(MarkPriceBook::new()),
    );
    let journal = InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()));
    let executor = MatchingExecutor::new(UNDERLYING);
    let actor = UnderlyingActor::new(
        ActorConfig::new(UNDERLYING, lineage.clone(), 64),
        journal,
        executor,
        fan,
        CLOCK,
    );
    (actor, executions, positions)
}

#[allow(clippy::too_many_arguments)]
fn add(
    lineage: &LineageId,
    sequence: u64,
    account: &str,
    owner: u8,
    side: Side,
    price: u64,
    quantity: u64,
    cloid: &str,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(),
        order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(sequence), 0),
        account: AccountId::new(account),
        owner: Hash32([owner; 32]),
        client_order_id: Some(ClientOrderId::new(cloid.to_string())),
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

fn cancel(lineage: &LineageId, target_seq: u64, account: &str) -> VenueCommand {
    VenueCommand::CancelOrder {
        symbol: sym(),
        order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(target_seq), 0),
        account: AccountId::new(account),
    }
}

/// Submits a command to the directly-owned actor (single-writer, synchronous)
/// and returns the assigned sequence.
fn submit(actor: &mut Venue, command: VenueCommand) -> SequenceNumber {
    match actor.handle(command) {
        Ok(receipt) => receipt.underlying_sequence,
        Err(e) => panic!("submit failed: {e}"),
    }
}

/// Seeds the pre-snapshot state and returns the captured cut plus the actor.
///
/// Book at capture: a maker resting 3 (after a partial fill) at 50_000 and an
/// extra resting 4 at 50_200; two executions legs; two positions; three
/// idempotency keys.
fn seed_and_capture() -> (
    Venue,
    Arc<InMemoryExecutionsStore>,
    Arc<InMemoryPositionsStore>,
    VenueSnapshot,
) {
    let lin = lineage();
    let (mut actor, executions, positions) = build_venue(&lin);
    // seq 0: maker rests 5 @ 50_000.
    submit(
        &mut actor,
        add(&lin, 0, "maker", 0x11, Side::Sell, 50_000, 5, "c-maker"),
    );
    // seq 1: taker buys 2 @ 50_000 → fills 2, maker rests 3.
    submit(
        &mut actor,
        add(&lin, 1, "taker", 0x22, Side::Buy, 50_000, 2, "c-taker"),
    );
    // seq 2: an extra maker rests 4 @ 50_200.
    submit(
        &mut actor,
        add(&lin, 2, "extra", 0x33, Side::Sell, 50_200, 4, "c-extra"),
    );
    let snapshot = actor.capture("snap-1", CONFIG_FP);
    (actor, executions, positions, snapshot)
}

/// The `JournalRecord::Epoch` markers currently in the actor's journal.
fn epoch_markers(actor: &Venue) -> Vec<fauxchange::exchange::SnapshotRestored> {
    actor
        .journal()
        .read_from(SequenceNumber::START)
        .expect("read journal")
        .into_iter()
        .filter_map(|record| match record {
            JournalRecord::Epoch(marker) => Some(marker),
            _ => None,
        })
        .collect()
}

// ============================================================================
// (a)+(b)+(c)+(d): snapshot → mutate → restore reconciles all four stores
// ============================================================================

#[test]
fn test_snapshot_mutate_restore_reconciles_all_four_stores() {
    let lin = lineage();
    let (mut actor, _executions, _positions, snap1) = seed_and_capture();

    // The cut captured the expected four-store instant.
    assert_eq!(
        snap1.executor.resting_orders.len(),
        2,
        "maker(3) + extra(4)"
    );
    assert_eq!(snap1.executions.len(), 2, "one match = two legs");
    assert_eq!(snap1.positions.len(), 2, "maker + taker positions");
    assert_eq!(
        snap1.executor.idempotency.len(),
        3,
        "c-maker/c-taker/c-extra"
    );

    // ---- MUTATE away from the cut ----
    // seq 3: cancel the extra order; seq 4: a taker consumes the maker's 3.
    submit(&mut actor, cancel(&lin, 2, "extra"));
    submit(
        &mut actor,
        add(&lin, 4, "taker2", 0x44, Side::Buy, 50_000, 3, "c-taker2"),
    );
    let pre_restore_last = actor.journal().last_sequence();
    assert_eq!(pre_restore_last, Some(SequenceNumber::new(4)));
    assert_eq!(actor.epoch(), 0, "no restore yet");
    // The book has genuinely moved (a fresh capture differs from the cut).
    assert_ne!(actor.capture("probe", CONFIG_FP).executor, snap1.executor);

    // ---- RESTORE the cut ----
    let receipt = match actor.restore(&snap1, CONFIG_FP) {
        Ok(r) => r,
        Err(e) => panic!("restore failed: {e}"),
    };

    // (b) A fresh epoch is opened by exactly one SnapshotRestored marker.
    let markers = epoch_markers(&actor);
    assert_eq!(markers.len(), 1, "one epoch marker opens the fresh epoch");
    let marker = &markers[0];
    assert_eq!(marker.epoch, 1, "epoch incremented");
    assert_eq!(marker.lineage_id, lin, "lineage carried forward");
    assert!(marker.is_current_schema());
    assert_eq!(actor.epoch(), 1);

    // (c) The underlying_sequence CONTINUES from the last journaled value — the
    // marker opens at 5, not a reset to 0.
    assert_eq!(receipt.underlying_sequence, SequenceNumber::new(5));
    assert_eq!(marker.underlying_sequence, SequenceNumber::new(5));

    // (a)+(d) Books, executions, positions, and idempotency all reconcile to the
    // captured instant (a fresh capture equals the cut's four stores).
    let snap2 = actor.capture("snap-2", CONFIG_FP);
    assert_eq!(
        snap2.executor, snap1.executor,
        "books + idempotency restored"
    );
    assert_eq!(snap2.executions, snap1.executions, "executions reconciled");
    assert_eq!(snap2.positions, snap1.positions, "positions reconciled");

    // A post-restore order continues the sequence past the marker (6, not reset).
    let next = submit(
        &mut actor,
        add(&lin, 6, "post", 0x55, Side::Sell, 51_000, 1, "c-post"),
    );
    assert_eq!(next, SequenceNumber::new(6));
}

// ============================================================================
// (c) idempotency: a ClOrdID retried AFTER restore returns the stored result
// ============================================================================

#[test]
fn test_clordid_retried_after_restore_returns_stored_result_not_a_second_order() {
    let lin = lineage();
    let (mut actor, _executions, _positions, snap1) = seed_and_capture();

    // The stored terminal result for the seeded maker's client id.
    let stored = snap1
        .executor
        .idempotency
        .iter()
        .find(|record| record.key.client_order_id == ClientOrderId::new("c-maker"))
        .map(|record| record.entry.terminal.clone())
        .expect("c-maker in the captured idempotency map");

    // Mutate then restore the cut.
    submit(
        &mut actor,
        add(&lin, 3, "noise", 0x66, Side::Buy, 49_000, 1, "c-noise"),
    );
    if let Err(e) = actor.restore(&snap1, CONFIG_FP) {
        panic!("restore failed: {e}");
    }
    let restored_books = actor.capture("probe", CONFIG_FP).executor;

    // Retry the SAME (account, client id) with the SAME payload as the seed.
    let retry_seq = submit(
        &mut actor,
        add(&lin, 6, "maker", 0x11, Side::Sell, 50_000, 5, "c-maker"),
    );

    // The journaled event at the retry replays the STORED terminal result...
    let event = actor
        .journal()
        .read_from(retry_seq)
        .expect("read")
        .into_iter()
        .find_map(|record| match record {
            JournalRecord::Event(event) if event.underlying_sequence == retry_seq => Some(event),
            _ => None,
        })
        .expect("a paired event at the retry sequence");
    assert_eq!(event.outcome, stored, "retry replays the stored terminal");

    // ...and the book gained NO second order (still the restored cut).
    assert_eq!(
        actor.capture("probe2", CONFIG_FP).executor,
        restored_books,
        "no second order was created by the idempotent retry"
    );
}

// ============================================================================
// (e) a metadata-mismatched snapshot is rejected without mutation
// ============================================================================

#[test]
fn test_metadata_mismatch_is_rejected_without_mutation() {
    let (mut actor, _executions, _positions, snap1) = seed_and_capture();
    let before = actor.capture("before", CONFIG_FP);

    // A config fingerprint that does not match the running venue is refused.
    match actor.restore(&snap1, "cfg-DIFFERENT") {
        Err(SnapshotError::MetadataMismatch(_)) => {}
        other => panic!("expected a MetadataMismatch, got {other:?}"),
    }

    // Nothing changed: no epoch opened, no marker journaled, stores untouched.
    assert_eq!(actor.epoch(), 0);
    assert!(epoch_markers(&actor).is_empty());
    let after = actor.capture("after", CONFIG_FP);
    assert_eq!(after.executor, before.executor);
    assert_eq!(after.executions, before.executions);
    assert_eq!(after.positions, before.positions);
}

// ============================================================================
// (f) a mid-restore fault rolls back all four stores
// ============================================================================

#[test]
fn test_mid_restore_fault_rolls_back_all_four_stores() {
    let lin = lineage();
    let (mut actor, executions, positions, mut snap1) = seed_and_capture();
    let before = actor.capture("before", CONFIG_FP);
    let executions_before = executions
        .list(&AccountId::new("taker"), &ExecutionFilter::default())
        .expect("list");
    let positions_before = positions
        .get(&AccountId::new("maker"), &sym(), None)
        .expect("get");

    // Inject a fault the restore hits mid-way (metadata still valid, but a
    // captured order belongs to a different underlying) — the preparation phase
    // fails before any store is swapped.
    snap1.executor.resting_orders.push(RestingOrderCapture {
        symbol: match Symbol::parse("ETH-20240329-50000-C") {
            Ok(s) => s,
            Err(e) => panic!("fixture parse failed: {e:?}"),
        },
        order_id: lin.venue_order_id("ETH", SequenceNumber::new(0), 0),
        account: AccountId::new("bad"),
        owner: Hash32([0x99; 32]),
        engine_seq: 99,
        side: Side::Sell,
        price: Cents::new(50_000),
        quantity: 1,
        time_in_force: TimeInForce::Gtc,
    });

    match actor.restore(&snap1, CONFIG_FP) {
        Err(SnapshotError::RebuildFailed(_)) => {}
        other => panic!("expected a RebuildFailed rollback, got {other:?}"),
    }

    // All four stores are untouched, and no epoch was opened.
    assert_eq!(actor.epoch(), 0);
    assert!(epoch_markers(&actor).is_empty());
    let after = actor.capture("after", CONFIG_FP);
    assert_eq!(after.executor, before.executor, "books rolled back");
    assert_eq!(
        executions
            .list(&AccountId::new("taker"), &ExecutionFilter::default())
            .expect("list"),
        executions_before,
        "executions rolled back"
    );
    assert_eq!(
        positions
            .get(&AccountId::new("maker"), &sym(), None)
            .expect("get"),
        positions_before,
        "positions rolled back"
    );
}

// ============================================================================
// Determinism: reproducible WITHIN an epoch; the restore boundary is excluded
// ============================================================================

#[test]
fn test_replay_reproducibility_is_within_epoch_and_restore_boundary_is_excluded() {
    let lin = lineage();

    // WITHIN an epoch: replaying the journaled COMMAND records through a fresh
    // executor reproduces the journaled EVENT outcomes — the determinism oracle.
    let (mut actor, _e, _p, _snap) = seed_and_capture();
    let records = actor
        .journal()
        .read_from(SequenceNumber::START)
        .expect("read");
    let mut replay = MatchingExecutor::new(UNDERLYING);
    for record in &records {
        if let JournalRecord::Command(journal_command) = record {
            let reconstructed = replay.execute(fauxchange::exchange::ExecutionContext {
                underlying: UNDERLYING,
                lineage_id: &lin,
                sequence: journal_command.sequence,
                venue_ts: journal_command.venue_ts,
                command: &journal_command.command,
            });
            let journaled = records
                .iter()
                .find_map(|r| match r {
                    JournalRecord::Event(event)
                        if event.underlying_sequence == journal_command.sequence =>
                    {
                        Some(&event.outcome)
                    }
                    _ => None,
                })
                .expect("a paired event for each command");
            assert_eq!(
                &reconstructed, journaled,
                "within an epoch, replay reproduces the journaled outcome"
            );
        }
    }

    // ACROSS a restore boundary: a restore writes a SnapshotRestored Epoch marker
    // — NOT a re-executable command/event pair — so the command-replay oracle
    // above neither includes nor reproduces the state injection. The boundary is
    // an explicit replay EXCLUSION (asserted here as a distinct Epoch record with
    // no VenueCommand to replay), not a silent divergence.
    let snapshot = actor.capture("snap-x", CONFIG_FP);
    submit(&mut actor, cancel(&lin, 2, "extra"));
    if let Err(e) = actor.restore(&snapshot, CONFIG_FP) {
        panic!("restore failed: {e}");
    }
    let restore_seq = SequenceNumber::new(4);
    let epoch_records: Vec<JournalRecord> = actor
        .journal()
        .read_from(restore_seq)
        .expect("read")
        .into_iter()
        .filter(|record| record.sequence() == restore_seq)
        .collect();
    // The restore boundary is a single Epoch record — never a Command a replay
    // would re-execute.
    assert_eq!(epoch_records.len(), 1);
    assert_eq!(epoch_records[0].kind(), RecordKind::Epoch);
    assert!(
        !epoch_records
            .iter()
            .any(|record| record.kind() == RecordKind::Command),
        "the restore boundary carries no command to replay — out of oracle scope"
    );
}
