//! The **flagship determinism suite** — `fauxchange`'s product stated as a
//! bounded, testable contract
//! ([017](../milestones/v0.1-backend-core/017-determinism-test-harness.md),
//! [02 §5–§6](../docs/02-matching-architecture.md),
//! [ADR-0006](../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
//! [TESTING.md §5](../docs/TESTING.md#5-determinism--replay-tests)).
//!
//! ## The oracle
//!
//! The comparison oracle is **ordered `VenueEvent`-stream equality per
//! underlying**: a replay `≡` the recorded run iff, for each underlying, the
//! sequence of `VenueEvent`s (each command and its captured outcome — fills,
//! cancels, evictions, status changes) is equal in order and in value. Top-of-book
//! after each event follows from event equality and is asserted as a **cheap
//! witness**. Cross-underlying interleaving is outside a single underlying's claim,
//! and process-local instrument-registry ids are **excluded** — equality is stated
//! over the canonical symbol string and `underlying_sequence`, never registry ids
//! ([02 §5, §5.2](../docs/02-matching-architecture.md)).
//!
//! ## The harness API (the record/replay helpers, unit-covered here)
//!
//! - [`record`] / `record_with` — drive a `VenueCommand` stream through a fresh
//!   [`MatchingExecutor`], journaling every write-ahead `(command, event)` pair
//!   into an [`InMemoryVenueJournal`] and capturing the per-event top-of-book
//!   witness. This is *the same executor path the single-writer actor drives*
//!   (`test_actor_journal_and_harness_record_agree` proves the recording mirrors a
//!   real [`UnderlyingActor`] journal byte-for-byte).
//! - [`replay`] — reconstruct the events + witnesses by re-executing every
//!   journaled `VenueCommand` in `N` order into a **fresh** registry (replay
//!   reconstructs book state, not historical marks).
//! - [`recover`] — recovery-as-re-execution: the same re-execution as [`replay`],
//!   but using the stored `VenueEvent` as the **integrity oracle** — it halts with
//!   a typed `JournalCorruption { underlying, sequence }` on divergence, derives
//!   the event for a tail command with no paired event, and refuses a
//!   newer-than-binary envelope schema (the one recovery algorithm of
//!   [ADR-0006](../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//!
//! The randomised sibling `journal_replay_reconstructs_book` lives in
//! `tests/property.rs`; this file adds the deterministic fixtures and the
//! recovery / fault-injection / lossless-capture / exclusion / expiry cases the
//! property cannot express.

use std::collections::BTreeSet;
use std::sync::Arc;

use fauxchange::exchange::{
    ActorConfig, AddOutcome, Cents, CommandExecutor, EventTimestamp, ExecutionContext,
    ExecutionsStore, ExpirationDate, FanOut, FixedClock, Hash32, InMemoryExecutionsStore,
    InMemoryPositionsStore, InMemoryVenueJournal, JournalCommand, JournalError, JournalHeader,
    JournalRecord, LineageId, MarkPriceBook, MatchingExecutor, NoopFanOut, PositionsStore,
    RecordKind, STPMode, SequenceNumber, Side, StoreFanOut, Symbol, SymbolError, SymbolParser,
    TimeInForce, TopOfBook, UnderlyingActor, VenueClock, VenueCommand, VenueEvent, VenueJournal,
    VenueOutcome, recover, validate_venue_expiry,
};
use fauxchange::simulation::{
    ClockMode, JournalStream, RunManifest, ScenarioBundle, SessionConfig, WalkTypeConfig,
    replay_bundle, replay_streams, synthesize_chain,
};
use fauxchange::{AccountId, OrderType, VenueError};

const UNDERLYING: &str = "BTC";
const CALL: &str = "BTC-20240329-50000-C";
const PUT: &str = "BTC-20240329-50000-P";
/// The deterministic venue clock the sequenced path stamps events from — a fixed
/// instant is sufficient because the journaled total order is the
/// `underlying_sequence`, not `venue_ts`.
const CLOCK: FixedClock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));

// ============================================================================
// Fixtures
// ============================================================================

fn sym(raw: &str) -> Symbol {
    match Symbol::parse(raw) {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
    }
}

/// A limit add whose venue order id is the deterministic grammar id for the
/// sequence it will be assigned (submissions are serial, so `sequence` matches).
#[allow(clippy::too_many_arguments)]
fn add(
    lineage: &LineageId,
    sequence: u64,
    raw_symbol: &str,
    account: &str,
    owner_byte: u8,
    side: Side,
    price: u64,
    quantity: u64,
    tif: TimeInForce,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(raw_symbol),
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

fn market(
    lineage: &LineageId,
    sequence: u64,
    raw_symbol: &str,
    account: &str,
    side: Side,
    quantity: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(raw_symbol),
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

fn cancel(lineage: &LineageId, target_seq: u64, raw_symbol: &str, account: &str) -> VenueCommand {
    VenueCommand::CancelOrder {
        symbol: sym(raw_symbol),
        order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(target_seq), 0),
        account: AccountId::new(account),
    }
}

#[allow(clippy::too_many_arguments)]
fn replace(
    lineage: &LineageId,
    target_seq: u64,
    new_seq: u64,
    raw_symbol: &str,
    account: &str,
    side: Side,
    limit_price: Option<u64>,
    quantity: u64,
    tif: TimeInForce,
) -> VenueCommand {
    VenueCommand::Replace {
        symbol: sym(raw_symbol),
        order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(target_seq), 0),
        new_order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(new_seq), 0),
        account: AccountId::new(account),
        side,
        limit_price: limit_price.map(Cents::new),
        quantity,
        time_in_force: tif,
        stp_mode: STPMode::None,
    }
}

/// A rich single-underlying session that touches two contracts (call + put),
/// crosses into fills, takes liquidity with a market order, and cancels a resting
/// order — enough distinct outcomes to make the oracle non-vacuous.
///
/// Timeline: seq0 rests a call ask (3), seq1 a second call ask (2), seq2 a put bid
/// (4), seq3 a call bid (5, rests), seq4 a call buy (2, crosses seq0), seq5 a call
/// market buy (1, takes seq0's remainder), seq6 cancels the put bid.
fn rich_stream(lineage: &LineageId) -> Vec<VenueCommand> {
    vec![
        add(
            lineage,
            0,
            CALL,
            "m1",
            0x11,
            Side::Sell,
            50_000,
            3,
            TimeInForce::Gtc,
        ),
        add(
            lineage,
            1,
            CALL,
            "m2",
            0x12,
            Side::Sell,
            50_100,
            2,
            TimeInForce::Gtc,
        ),
        add(
            lineage,
            2,
            PUT,
            "p1",
            0x13,
            Side::Buy,
            30_000,
            4,
            TimeInForce::Gtc,
        ),
        add(
            lineage,
            3,
            CALL,
            "b1",
            0x33,
            Side::Buy,
            49_900,
            5,
            TimeInForce::Gtc,
        ),
        add(
            lineage,
            4,
            CALL,
            "t1",
            0x22,
            Side::Buy,
            50_000,
            2,
            TimeInForce::Gtc,
        ),
        market(lineage, 5, CALL, "t2", Side::Buy, 1),
        cancel(lineage, 2, PUT, "p1"),
    ]
}

fn witnesses() -> Vec<Symbol> {
    vec![sym(CALL), sym(PUT)]
}

// ============================================================================
// The record/replay harness
// ============================================================================

/// A recorded session: the write-ahead journal plus the per-event artifacts
/// captured at record time — the oracle's inputs.
struct Recording {
    underlying: String,
    witnesses: Vec<Symbol>,
    journal: InMemoryVenueJournal,
    /// The ordered `VenueEvent`s as recorded (the oracle's primary artifact).
    events: Vec<VenueEvent>,
    /// Top-of-book after each committed event, one row per witness symbol.
    tops: Vec<Vec<TopOfBook>>,
}

/// The reconstructed artifacts a [`replay`] produces from a recording's journal.
struct Replay {
    events: Vec<VenueEvent>,
    tops: Vec<Vec<TopOfBook>>,
}

/// Records a command stream under the default venue [`CLOCK`].
fn record(commands: &[VenueCommand], lineage: &LineageId, witnesses: &[Symbol]) -> Recording {
    record_with(commands, lineage, witnesses, CLOCK)
}

/// Records a command stream by driving a fresh [`MatchingExecutor`] and journaling
/// every write-ahead `(command, event)` pair (exactly as the single-writer actor
/// does), capturing the per-event top-of-book witness.
fn record_with(
    commands: &[VenueCommand],
    lineage: &LineageId,
    witnesses: &[Symbol],
    clock: FixedClock,
) -> Recording {
    let mut executor = MatchingExecutor::new(UNDERLYING);
    let mut journal = InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()));
    let mut events = Vec::with_capacity(commands.len());
    let mut tops = Vec::with_capacity(commands.len());

    for (index, command) in commands.iter().enumerate() {
        let sequence = SequenceNumber::new(index as u64);
        let venue_ts = clock.now_ms();
        // Step 1: write-ahead command append, before executing.
        append(
            &mut journal,
            JournalRecord::command(sequence, venue_ts, command.clone()),
        );
        // Steps 3–4: execute + capture the lossless outcome, then append the event.
        let outcome = executor.execute(ExecutionContext {
            underlying: UNDERLYING,
            lineage_id: lineage,
            sequence,
            venue_ts,
            command,
        });
        let event = VenueEvent::new(sequence, venue_ts, command.clone(), outcome);
        append(&mut journal, JournalRecord::event(event.clone()));
        // The cheap witness: top-of-book after this event, per witness symbol.
        tops.push(witnesses.iter().map(|w| executor.top_of_book(w)).collect());
        events.push(event);
    }

    Recording {
        underlying: UNDERLYING.to_string(),
        witnesses: witnesses.to_vec(),
        journal,
        events,
        tops,
    }
}

/// Replays a recording's journal by re-executing every journaled `VenueCommand` in
/// `N` order into a **fresh** registry — the replay algorithm. The lineage is read
/// back from the journal header, so re-derived ids land in the same namespace.
fn replay(recording: &Recording) -> Replay {
    let lineage = recording.journal.header().lineage_id.clone();
    let mut executor = MatchingExecutor::new(recording.underlying.as_str());
    let commands = command_records(&recording.journal);
    let mut events = Vec::with_capacity(commands.len());
    let mut tops = Vec::with_capacity(commands.len());

    for jc in commands {
        let outcome = executor.execute(ExecutionContext {
            underlying: recording.underlying.as_str(),
            lineage_id: &lineage,
            sequence: jc.sequence,
            venue_ts: jc.venue_ts,
            command: &jc.command,
        });
        tops.push(
            recording
                .witnesses
                .iter()
                .map(|w| executor.top_of_book(w))
                .collect(),
        );
        events.push(VenueEvent::new(
            jc.sequence,
            jc.venue_ts,
            jc.command.clone(),
            outcome,
        ));
    }

    Replay { events, tops }
}

/// The oracle: ordered `VenueEvent`-stream equality per underlying, plus the
/// top-of-book witness after each event.
fn assert_replay_equals(recording: &Recording, replay: &Replay) {
    assert_eq!(
        replay.events, recording.events,
        "the ordered VenueEvent stream must be equal per underlying"
    );
    assert_eq!(
        replay.tops, recording.tops,
        "top-of-book after each event must be equal (the cheap witness)"
    );
}

// ============================================================================
// Recovery-as-re-execution (the integrity oracle)
// ============================================================================
//
// The recovery reducer is now PRODUCTION code — `fauxchange::exchange::recover`
// ([`src/exchange/recovery.rs`], #029) — returning the typed `JournalError`
// (`Corruption` and the newer-than-binary `SchemaTooNew`), replacing the v0.1
// test-local `RecoveryHalt` halt with the real production error the ADR obligated
// #029 to introduce. The fixtures below build the journals it walks; the
// assertions live in section 3.

// ============================================================================
// Journal helpers
// ============================================================================

fn append(journal: &mut InMemoryVenueJournal, record: JournalRecord) {
    if let Err(e) = journal.append(record) {
        panic!("in-memory journal append must not fail: {e}");
    }
}

fn read_all<J: VenueJournal>(journal: &J) -> Vec<JournalRecord> {
    match journal.read_from(SequenceNumber::START) {
        Ok(records) => records,
        Err(e) => panic!("in-memory journal read must not fail: {e}"),
    }
}

fn command_records(journal: &InMemoryVenueJournal) -> Vec<JournalCommand> {
    command_records_from(&read_all(journal))
        .into_iter()
        .cloned()
        .collect()
}

fn command_records_from(records: &[JournalRecord]) -> Vec<&JournalCommand> {
    let mut commands: Vec<&JournalCommand> = records
        .iter()
        .filter_map(|record| match record {
            JournalRecord::Command(command) => Some(command),
            _ => None,
        })
        .collect();
    commands.sort_by_key(|command| command.sequence);
    commands
}

/// Rebuilds a recording's journal, replacing the stored EVENT at `target` with a
/// divergent outcome — a corrupted stored event for the integrity-oracle test.
fn corrupt_event_at(recording: &Recording, target: SequenceNumber) -> InMemoryVenueJournal {
    let mut journal = InMemoryVenueJournal::new(recording.journal.header().clone());
    for record in read_all(&recording.journal) {
        match record {
            JournalRecord::Event(event) if event.underlying_sequence == target => {
                let corrupted = VenueEvent::new(
                    event.underlying_sequence,
                    event.venue_ts,
                    event.command.clone(),
                    VenueOutcome::Rejected {
                        reason: "corrupted-by-test".to_string(),
                    },
                );
                append(&mut journal, JournalRecord::event(corrupted));
            }
            other => append(&mut journal, other),
        }
    }
    journal
}

/// Rebuilds a recording's journal without the paired EVENT for its highest
/// sequence — a tail command with no paired event (a crash between write-ahead and
/// event append).
fn drop_tail_event(recording: &Recording) -> InMemoryVenueJournal {
    let tail = recording
        .events
        .last()
        .map(|event| event.underlying_sequence);
    let mut journal = InMemoryVenueJournal::new(recording.journal.header().clone());
    for record in read_all(&recording.journal) {
        if let JournalRecord::Event(event) = &record
            && Some(event.underlying_sequence) == tail
        {
            continue; // drop the tail event; keep its command
        }
        append(&mut journal, record);
    }
    journal
}

/// Rebuilds a recording's journal under a header schema newer than the binary.
fn with_newer_schema(recording: &Recording) -> InMemoryVenueJournal {
    let header = JournalHeader {
        schema_version: "venue.v2".to_string(),
        lineage_id: recording.journal.header().lineage_id.clone(),
    };
    let mut journal = InMemoryVenueJournal::new(header);
    for record in read_all(&recording.journal) {
        append(&mut journal, record);
    }
    journal
}

// ============================================================================
// A fault-injecting journal (consolidated from the #006 fault-injection rows)
// ============================================================================

/// A [`VenueJournal`] wrapping the in-memory store that **confirms-fails** the
/// append at one chosen `(sequence, kind)`, once — the both-append-stages fault
/// substrate.
struct FaultJournal {
    inner: InMemoryVenueJournal,
    fail_at: Option<(SequenceNumber, RecordKind)>,
}

impl FaultJournal {
    fn new(inner: InMemoryVenueJournal, fail_at: (SequenceNumber, RecordKind)) -> Self {
        Self {
            inner,
            fail_at: Some(fail_at),
        }
    }
}

impl VenueJournal for FaultJournal {
    fn header(&self) -> &JournalHeader {
        self.inner.header()
    }

    fn append(&mut self, record: JournalRecord) -> Result<(), JournalError> {
        if self.fail_at == Some((record.sequence(), record.kind())) {
            self.fail_at = None; // fire once, then let the reuse/retry succeed
            return Err(JournalError::AppendFailed("injected".to_string()));
        }
        self.inner.append(record)
    }

    fn read_from(&self, from: SequenceNumber) -> Result<Vec<JournalRecord>, JournalError> {
        self.inner.read_from(from)
    }

    fn last_sequence(&self) -> Option<SequenceNumber> {
        self.inner.last_sequence()
    }
}

/// The default order-path actor over a fault-injecting journal (real executor,
/// no-op fan-out — the harness inspects the journal, not the stores).
fn fault_actor(
    lineage: &LineageId,
    fail_at: (SequenceNumber, RecordKind),
) -> UnderlyingActor<FaultJournal, MatchingExecutor, NoopFanOut, FixedClock> {
    let inner = InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()));
    UnderlyingActor::new(
        ActorConfig::new(UNDERLYING, lineage.clone(), 16),
        FaultJournal::new(inner, fail_at),
        MatchingExecutor::new(UNDERLYING),
        NoopFanOut,
        CLOCK,
    )
}

// ============================================================================
// 1. The record/replay harness + oracle
// ============================================================================

#[test]
fn test_recorded_session_replays_to_identical_events_and_top_of_book() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());

    // Non-vacuous: the session actually crossed into fills.
    assert!(
        recording.events.iter().any(|event| matches!(
            &event.outcome,
            VenueOutcome::Added { fills, .. } | VenueOutcome::Market { fills, .. } if !fills.is_empty()
        )),
        "the fixture must exercise a real match"
    );

    // The oracle: a fresh-registry replay reconstructs identical events + witness.
    assert_replay_equals(&recording, &replay(&recording));
}

#[test]
fn test_harness_record_and_replay_helpers_reconstruct_from_a_fresh_registry() {
    let lineage = LineageId::new("run-1");
    let commands = vec![
        add(
            &lineage,
            0,
            CALL,
            "m",
            0x11,
            Side::Sell,
            50_000,
            2,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            1,
            CALL,
            "t",
            0x22,
            Side::Buy,
            50_000,
            2,
            TimeInForce::Gtc,
        ),
    ];
    let recording = record(&commands, &lineage, &[sym(CALL)]);

    // record journaled a write-ahead (command, event) pair per sequence.
    assert!(
        recording
            .journal
            .contains(SequenceNumber::new(0), RecordKind::Command)
    );
    assert!(
        recording
            .journal
            .contains(SequenceNumber::new(0), RecordKind::Event)
    );
    assert!(
        recording
            .journal
            .contains(SequenceNumber::new(1), RecordKind::Command)
    );
    assert!(
        recording
            .journal
            .contains(SequenceNumber::new(1), RecordKind::Event)
    );
    assert_eq!(recording.events.len(), 2);

    // replay always starts from a FRESH registry: two independent replays agree,
    // and both agree with the recording.
    let first = replay(&recording);
    let second = replay(&recording);
    assert_eq!(first.events, second.events);
    assert_eq!(first.tops, second.tops);
    assert_replay_equals(&recording, &first);
}

#[test]
fn test_actor_journal_and_harness_record_agree() {
    let lineage = LineageId::new("run-1");
    let commands = rich_stream(&lineage);

    // Drive the SAME stream through the real per-underlying single-writer actor.
    let journal = InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()));
    let mut actor = UnderlyingActor::new(
        ActorConfig::new(UNDERLYING, lineage.clone(), 64),
        journal,
        MatchingExecutor::new(UNDERLYING),
        NoopFanOut,
        CLOCK,
    );
    for command in &commands {
        if let Err(e) = actor.handle(command.clone()) {
            panic!("actor submit failed: {e}");
        }
    }

    // The actor's journal events equal the harness recording's events — the
    // harness records exactly what the production single writer journals.
    let actor_events: Vec<VenueEvent> = read_all(actor.journal())
        .into_iter()
        .filter_map(|record| match record {
            JournalRecord::Event(event) => Some(event),
            _ => None,
        })
        .collect();
    let recording = record(&commands, &lineage, &[sym(CALL)]);
    assert_eq!(
        actor_events, recording.events,
        "the harness record mirrors the real actor's journal"
    );

    // And recovery over the actor's own journal reconstructs those events.
    match recover(actor.journal(), UNDERLYING) {
        Ok(recovered) => assert_eq!(recovered.events, actor_events),
        Err(e) => panic!("recovery over a clean actor journal failed: {e:?}"),
    }
}

// ============================================================================
// 2. Exclusions asserted AS exclusions (not silently divergent)
// ============================================================================

#[test]
fn test_excluded_analytics_are_structurally_absent_from_the_oracle() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());

    // The crossing event (seq 4) carries fills — the richest oracle artifact.
    let event = &recording.events[4];
    let fills = match &event.outcome {
        VenueOutcome::Added { fills, .. } if !fills.is_empty() => fills,
        other => panic!("expected the crossing event to carry fills, got {other:?}"),
    };

    let value = match serde_json::to_value(event) {
        Ok(v) => v,
        Err(e) => panic!("serialize event failed: {e}"),
    };
    let mut keys = BTreeSet::new();
    collect_keys(&value, &mut keys);

    // The journaled artifacts the oracle IS stated over are present...
    for present in [
        "schema",
        "underlying_sequence",
        "venue_ts",
        "command",
        "outcome",
    ] {
        assert!(
            keys.contains(present),
            "expected the journaled key {present}"
        );
    }
    // ...and every recomputed-live / process-local artifact is STRUCTURALLY absent
    // (mark price, unrealised P&L, Greeks, instrument_sequence, engine trade
    // ids / clock), so it cannot even be asserted equal — it is out of scope.
    for excluded in [
        "mark",
        "mark_price",
        "unrealized_pnl",
        "unrealised_pnl",
        "greeks",
        "delta",
        "gamma",
        "vega",
        "theta",
        "rho",
        "iv",
        "implied_volatility",
        "instrument_sequence",
        "trade_id",
        "engine_seq",
        "engine_order_id",
        "wall_clock",
    ] {
        assert!(
            !keys.contains(excluded),
            "excluded analytic {excluded} must not appear in a journaled VenueEvent"
        );
    }

    // A Fill carries only the journaled join keys — no engine trade-id / uuid /
    // clock leaked in beside them.
    let fill_value = match serde_json::to_value(&fills[0]) {
        Ok(v) => v,
        Err(e) => panic!("serialize fill failed: {e}"),
    };
    let fill_obj = match fill_value.as_object() {
        Some(obj) => obj,
        None => panic!("a Fill must serialise as an object"),
    };
    let mut fill_keys: Vec<&str> = fill_obj.keys().map(String::as_str).collect();
    fill_keys.sort_unstable();
    assert_eq!(
        fill_keys,
        vec![
            "account",
            "execution_id",
            "fee",
            "liquidity",
            "order_id",
            "owner",
            "price",
            "quantity",
            "side",
        ],
        "a Fill carries only its journaled join keys — no engine trade-id/uuid/clock"
    );
}

#[test]
fn test_live_marks_are_recomputed_and_excluded_from_the_event_oracle() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());

    // Two fresh-registry replays agree on the compared artifact (the events).
    let first = replay(&recording);
    let second = replay(&recording);
    assert_eq!(
        first.events, second.events,
        "the compared event stream is identical (the oracle)"
    );

    // Fold the replayed fills into a positions store, then mark the SAME journaled
    // fold against two DIFFERENT live marks. The net quantity (a journaled fold) is
    // identical; only the unrealised P&L — recomputed live from the non-journaled
    // mark — changes, proving it is outside the oracle, not silently divergent.
    let positions = fold_positions(&first.events);
    let account = AccountId::new("t1");
    let symbol = sym(CALL);
    let at_entry = position_at(&positions, &account, &symbol, 50_000);
    let at_higher = position_at(&positions, &account, &symbol, 70_000);

    assert_eq!(
        at_entry.net_quantity, at_higher.net_quantity,
        "the journaled position fold is mark-independent"
    );
    assert_ne!(
        at_entry.unrealized_pnl, at_higher.unrealized_pnl,
        "unrealised P&L is recomputed live from the non-journaled mark — excluded from the oracle"
    );
}

#[test]
fn test_engine_clock_value_is_excluded_from_the_captured_outcome() {
    let lineage = LineageId::new("run-1");
    let stream = rich_stream(&lineage);
    let wits = witnesses();

    // Record the same stream under two different venue clocks.
    let early = record_with(
        &stream,
        &lineage,
        &wits,
        FixedClock::new(EventTimestamp::new(1_700_000_000_000)),
    );
    let late = record_with(
        &stream,
        &lineage,
        &wits,
        FixedClock::new(EventTimestamp::new(1_888_000_000_000)),
    );

    // The captured OUTCOMES (fills / cancels / remainders) are identical — matching
    // does not depend on the clock value, so the engine clock (and its Uuid
    // trade-id namespace) is excluded from the outcome oracle.
    let outcomes_early: Vec<&VenueOutcome> = early.events.iter().map(|e| &e.outcome).collect();
    let outcomes_late: Vec<&VenueOutcome> = late.events.iter().map(|e| &e.outcome).collect();
    assert_eq!(
        outcomes_early, outcomes_late,
        "the clock value is excluded from the matching outcome"
    );
    // Only the journaled venue_ts differs — the deterministic value replay reuses.
    assert!(
        early
            .events
            .iter()
            .zip(&late.events)
            .all(|(a, b)| a.venue_ts != b.venue_ts),
        "venue_ts reflects the (journaled) clock, but the outcome does not"
    );
}

// ============================================================================
// 3. Recovery-as-re-execution
// ============================================================================

#[test]
fn test_recovery_reexecutes_clean_journal_to_events_equal_to_stored() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());
    match recover(&recording.journal, UNDERLYING) {
        Ok(recovered) => assert_eq!(
            recovered.events, recording.events,
            "recovery re-executes a clean journal to events equal to the stored ones"
        ),
        Err(e) => panic!("recovery of a clean journal must not halt: {e:?}"),
    }
}

#[test]
fn test_recovery_halts_on_corrupted_stored_event_with_exact_underlying_and_sequence() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());

    // Corrupt the stored event at the cancel (seq 6): its re-execution is a
    // deterministic `Cancelled`, so the injected `Rejected` diverges.
    let target = SequenceNumber::new(6);
    let corrupted = corrupt_event_at(&recording, target);

    match recover(&corrupted, UNDERLYING) {
        Err(JournalError::Corruption {
            underlying,
            sequence,
        }) => {
            assert_eq!(
                underlying, UNDERLYING,
                "the halt names the exact underlying"
            );
            assert_eq!(sequence, target, "the halt names the exact sequence N");
        }
        other => panic!("expected a Corruption halt at the exact (underlying, N), got {other:?}"),
    }
}

#[test]
fn test_recovery_derives_event_for_tail_command_with_no_paired_event() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());

    // Drop the paired event for the tail sequence: recovery must re-execute the
    // command to DERIVE the identical event (there is no stored event to compare).
    let journal = drop_tail_event(&recording);
    match recover(&journal, UNDERLYING) {
        Ok(recovered) => assert_eq!(
            recovered.events, recording.events,
            "a tail command with no paired event re-executes to derive the identical event"
        ),
        Err(e) => panic!("recovery of a tail-command-only journal must not halt: {e:?}"),
    }
}

#[test]
fn test_recovery_refuses_a_newer_than_binary_schema() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());
    let newer = with_newer_schema(&recording);

    match recover(&newer, UNDERLYING) {
        Err(JournalError::SchemaTooNew { found }) => assert_eq!(found, "venue.v2"),
        other => panic!("expected a SchemaTooNew refusal, got {other:?}"),
    }
}

// ============================================================================
// 4. Fault injection at both append stages, with a restart assertion
// ============================================================================

#[test]
fn test_pre_execution_append_failure_reuses_sequence_and_replay_is_gapless() {
    let lineage = LineageId::new("run-1");
    let mut actor = fault_actor(&lineage, (SequenceNumber::new(0), RecordKind::Command));

    // The first command's write-ahead append is confirmed-failed: nothing executes,
    // the book is untouched, and N=0 is REUSED (no gap, not sealed).
    let rejected = add(
        &lineage,
        0,
        CALL,
        "a",
        0x11,
        Side::Sell,
        50_000,
        2,
        TimeInForce::Gtc,
    );
    match actor.handle(rejected) {
        Err(VenueError::JournalUnavailable) => {}
        other => panic!("expected JournalUnavailable on a pre-exec append failure, got {other:?}"),
    }

    // The retry commits at the reused N=0.
    let committed = add(
        &lineage,
        0,
        CALL,
        "b",
        0x22,
        Side::Sell,
        50_500,
        3,
        TimeInForce::Gtc,
    );
    match actor.handle(committed.clone()) {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0)),
        other => panic!("expected the retry to commit at N=0, got {other:?}"),
    }

    // No gap, no tombstone: exactly one committed command in the stream.
    let command_count = read_all(actor.journal())
        .iter()
        .filter(|record| record.kind() == RecordKind::Command)
        .count();
    assert_eq!(command_count, 1, "the reused N leaves no gap");

    // Recovery reconstructs exactly the committed command's event — the reuse is
    // invisible to replay.
    let clean = record(&[committed], &lineage, &[sym(CALL)]);
    match recover(actor.journal(), UNDERLYING) {
        Ok(recovered) => assert_eq!(
            recovered.events, clean.events,
            "a pre-exec append failure reused N with no gap; replay reconstructs identically"
        ),
        Err(e) => panic!("recovery after a pre-exec fault must not halt: {e:?}"),
    }
}

#[test]
fn test_post_mutation_append_failure_seals_and_restart_reexecutes_to_identical_event() {
    let lineage = LineageId::new("run-1");
    let mut actor = fault_actor(&lineage, (SequenceNumber::new(1), RecordKind::Event));

    let maker = add(
        &lineage,
        0,
        CALL,
        "maker",
        0x11,
        Side::Sell,
        50_000,
        2,
        TimeInForce::Gtc,
    );
    let taker = add(
        &lineage,
        1,
        CALL,
        "taker",
        0x22,
        Side::Buy,
        50_000,
        2,
        TimeInForce::Gtc,
    );

    // seq 0 commits fully.
    match actor.handle(maker.clone()) {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0)),
        other => panic!("expected the maker to commit at N=0, got {other:?}"),
    }
    // seq 1 executes a crossing fill, but the post-mutation event append fails →
    // the underlying is SEALED (the write-ahead command@1 is journaled, no event).
    match actor.handle(taker.clone()) {
        Err(VenueError::JournalUnavailable) => {}
        other => {
            panic!("expected JournalUnavailable on a post-mutation append failure, got {other:?}")
        }
    }
    // Sealed: a further command is rejected without executing, fault cleared or not.
    match actor.handle(taker.clone()) {
        Err(VenueError::JournalUnavailable) => {}
        other => panic!("expected a sealed JournalUnavailable, got {other:?}"),
    }

    // Restart-as-re-execution: recovery re-executes the tail command@1 to derive
    // the identical event — the crossing fill survives the seal, never lost.
    let clean = record(&[maker, taker], &lineage, &[sym(CALL)]);
    let recovered = match recover(actor.journal(), UNDERLYING) {
        Ok(recovered) => recovered,
        Err(e) => panic!("recovery after a post-mutation seal must not halt: {e:?}"),
    };
    assert_eq!(
        recovered.events, clean.events,
        "the post-mutation seal leaves a tail command; restart re-executes to the identical event"
    );
    let event = recovered
        .events
        .iter()
        .find(|event| event.underlying_sequence == SequenceNumber::new(1))
        .expect("a derived event at the tail sequence");
    match &event.outcome {
        VenueOutcome::Added { fills, .. } => assert_eq!(
            fills.len(),
            2,
            "the fill survives the restart re-execution, not lost to the seal"
        ),
        other => panic!("expected the derived tail event to carry the fill, got {other:?}"),
    }
}

// ============================================================================
// 5. Lossless capture on the error / partial paths
// ============================================================================

#[test]
fn test_ioc_order_that_fills_and_errs_is_journaled_with_fills_and_replays() {
    let lineage = LineageId::new("run-1");
    let commands = vec![
        add(
            &lineage,
            0,
            CALL,
            "maker",
            0x11,
            Side::Sell,
            50_000,
            1,
            TimeInForce::Gtc,
        ),
        // IOC buy of 3 crosses 1, then the 2-lot remainder is unfillable — the
        // upstream `_full` leaf returns Err, but the executed fill is diff-captured.
        add(
            &lineage,
            1,
            CALL,
            "taker",
            0x22,
            Side::Buy,
            50_000,
            3,
            TimeInForce::Ioc,
        ),
    ];
    let recording = record(&commands, &lineage, &[sym(CALL)]);

    // The IOC event is journaled WITH its fill — never a bare Rejected.
    match &recording.events[1].outcome {
        VenueOutcome::Added {
            fills,
            resting_quantity,
            ..
        } => {
            assert_eq!(
                fills.len(),
                2,
                "the executed fill is captured, not lost to a bare Rejected"
            );
            assert_eq!(fills[1].quantity, 1, "one contract crossed");
            assert_eq!(*resting_quantity, 0, "an IOC never rests its remainder");
        }
        other => panic!("expected a diff-captured Added with fills, got {other:?}"),
    }

    // Replay and recovery reconstruct the identical lossless event.
    assert_replay_equals(&recording, &replay(&recording));
    match recover(&recording.journal, UNDERLYING) {
        Ok(recovered) => assert_eq!(recovered.events, recording.events),
        Err(e) => panic!("recovery of the IOC-error journal must not halt: {e:?}"),
    }
}

#[test]
fn test_partial_replace_replays_identically() {
    let lineage = LineageId::new("run-1");
    let commands = vec![
        add(
            &lineage,
            0,
            CALL,
            "acct",
            0x11,
            Side::Sell,
            50_000,
            2,
            TimeInForce::Gtc,
        ),
        // Replace: the cancel leg succeeds, but the FOK add leg cannot fill in full
        // and is rejected — a defined partial state (not rolled back).
        replace(
            &lineage,
            0,
            1,
            CALL,
            "acct",
            Side::Buy,
            Some(40_000),
            2,
            TimeInForce::Fok,
        ),
    ];
    let recording = record(&commands, &lineage, &[sym(CALL)]);

    match &recording.events[1].outcome {
        VenueOutcome::Replace { cancelled, add } => {
            assert!(*cancelled, "the cancel leg succeeded");
            assert!(
                matches!(add, AddOutcome::Rejected { .. }),
                "the FOK add leg could not fill and was rejected, got {add:?}"
            );
        }
        other => panic!("expected a partial Replace, got {other:?}"),
    }

    assert_replay_equals(&recording, &replay(&recording));
    match recover(&recording.journal, UNDERLYING) {
        Ok(recovered) => assert_eq!(
            recovered.events, recording.events,
            "a partial replace replays identically"
        ),
        Err(e) => panic!("recovery of the partial-replace journal must not halt: {e:?}"),
    }
}

// ============================================================================
// 6. Replay-stable expiries
// ============================================================================

#[test]
fn test_datetime_expiry_fixture_replays_identically() {
    let lineage = LineageId::new("run-1");

    // The fixture contracts are keyed on a canonical `ExpirationDate::DateTime`
    // instant — an absolute expiry that resolves the same on replay.
    let expiry = match SymbolParser::parse_yyyymmdd("20240329", "") {
        Ok(e) => e,
        Err(e) => panic!("parse_yyyymmdd failed: {e}"),
    };
    match validate_venue_expiry(&expiry) {
        Ok(validated) => assert!(
            matches!(validated, ExpirationDate::DateTime(_)),
            "a canonical expiry must validate to an absolute DateTime"
        ),
        Err(e) => panic!("the canonical DateTime expiry must validate: {e:?}"),
    }

    // A session over that absolute-expiry contract replays identically.
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());
    assert_replay_equals(&recording, &replay(&recording));
}

#[test]
fn test_days_relative_expiry_is_rejected_at_load() {
    // A relative `ExpirationDate::Days` expiry is wall-clock-relative and would map
    // to a different calendar date on replay, so it is refused at load — guarding
    // the invariant, never admitted onto the sequenced path.
    let days = match ExpirationDate::from_string("30") {
        Ok(e) => e,
        Err(e) => panic!("from_string failed: {e}"),
    };
    match validate_venue_expiry(&days) {
        Err(SymbolError::RelativeExpiryRefused) => {}
        other => panic!("a Days-relative expiry must be rejected at load, got {other:?}"),
    }
}

// ============================================================================
// #028: the venue clock on the sequenced path
// ============================================================================

/// A journaled `Clock` advance (and the `now_ms` journaled into an expiry sweep)
/// replays to the identical `now_ms`-derived effects: the harness `replay`
/// re-executes the journaled `VenueCommand`s and reuses each journaled `venue_ts`,
/// **never** re-reading a wall / replay clock, so the carried `now_ms` values
/// survive the round-trip byte-identically
/// ([04 §5](../docs/04-market-data-and-replay.md#5-clock-control),
/// [02 §4.1](../docs/02-matching-architecture.md#41-venue-wide-commands-marketmakercontrol--clock--simstep)).
///
/// The interleaved order is `GTC` **on purpose**: `GTC` admission is
/// time-independent, so its outcome is a genuine function of the journaled input.
/// `Day` / `GTD` admission is decided by the leaf's default `MonotonicClock`
/// (wall time), not the injected venue clock — the named upstream leaf-clock gap —
/// so it is **not** asserted here (that would be a false green); it is documented
/// by `test_day_gtd_admission_determinism_blocked_by_leaf_clock_gap` below.
#[test]
fn test_journaled_clock_advance_replays_to_identical_now_ms() {
    let lineage = LineageId::new("run-clock");
    let t1 = EventTimestamp::new(1_800_000_000_000);
    let t2 = EventTimestamp::new(1_900_000_000_000);
    let commands = vec![
        VenueCommand::Clock { now_ms: t1 },
        add(
            &lineage,
            1,
            CALL,
            "mm",
            0x11,
            Side::Sell,
            50_000,
            3,
            TimeInForce::Gtc,
        ),
        VenueCommand::Clock { now_ms: t2 },
        VenueCommand::EvictExpiredOrders { now_ms: t2 },
    ];
    let recording = record(&commands, &lineage, &[sym(CALL)]);
    let replayed = replay(&recording);

    // Ordered VenueEvent-stream equality per underlying — the whole stream,
    // including the two Clock advances, the time-independent GTC add, and the
    // expiry sweep, replays identically.
    assert_eq!(replayed.events, recording.events);

    // Explicitly: the Clock / Evict `now_ms` are reproduced FROM the command, not a
    // replay clock — the carried values are byte-identical across the round-trip.
    let carried: Vec<u64> = replayed
        .events
        .iter()
        .filter_map(|event| match &event.command {
            VenueCommand::Clock { now_ms } | VenueCommand::EvictExpiredOrders { now_ms } => {
                Some(now_ms.get())
            }
            _ => None,
        })
        .collect();
    assert_eq!(carried, vec![t1.get(), t2.get(), t2.get()]);
}

/// Two independent recordings of the same `Clock`-bearing command stream produce
/// the identical `VenueEvent` stream — the reproducibility half of the oracle for
/// a clock advance.
#[test]
fn test_clock_advance_is_reproducible_across_two_runs() {
    let lineage = LineageId::new("run-clock-2");
    let commands = vec![
        VenueCommand::Clock {
            now_ms: EventTimestamp::new(1_800_000_000_000),
        },
        add(
            &lineage,
            1,
            CALL,
            "mm",
            0x11,
            Side::Sell,
            50_000,
            3,
            TimeInForce::Gtc,
        ),
    ];
    let first = record(&commands, &lineage, &[sym(CALL)]);
    let second = record(&commands, &lineage, &[sym(CALL)]);
    assert_eq!(
        first.events, second.events,
        "the same Clock-bearing stream reproduces the same events"
    );
}

/// **Named upstream limitation (#028).** Deterministic `Day` / `GTD` time-in-force
/// *admission* requires an injected venue clock **at the leaf**, so TIF is decided
/// against venue time rather than wall time. `orderbook-rs` 0.10.5 provides the API
/// (`OrderBook::with_clock` / `Arc<dyn Clock>` / `MonotonicClock` / `StubClock`),
/// but the pinned `option-chain-orderbook` 0.7.0 does **not** thread it through its
/// lazy `get_or_create_*` leaf construction, exposes no `OptionOrderBook::with_clock`,
/// and `OrderBook::set_clock` needs `&mut self` while the venue holds vivified leaves
/// as `Arc<OptionOrderBook>` (shared). Until that named upstream work lands, the
/// injectable-leaf-clock guarantee covers **no** hierarchy leaf, and the intraday
/// expiry sweep (`EvictExpiredOrders`) is a journaled no-op the executor `Rejected`s.
/// This test pins that current, documented reality — and that it *still* replays
/// deterministically from the journaled `now_ms` — so the gap is **named, not
/// silent** ([02 §5.5b](../docs/02-matching-architecture.md#5-determinism)).
#[test]
fn test_evict_expired_orders_is_a_documented_leaf_clock_limitation() {
    let lineage = LineageId::new("run-evict");
    let commands = vec![VenueCommand::EvictExpiredOrders {
        now_ms: EventTimestamp::new(1_900_000_000_000),
    }];
    let recording = record(&commands, &lineage, &[]);
    match &recording.events[0].outcome {
        // Pending the upstream leaf clock, the intraday sweep is a journaled no-op.
        VenueOutcome::Rejected { .. } => {}
        other => panic!(
            "EvictExpiredOrders outcome changed; revisit the named leaf-clock limitation: {other:?}"
        ),
    }
    // It still replays deterministically from the journaled now_ms.
    let replayed = replay(&recording);
    assert_eq!(replayed.events, recording.events);
}

/// **Blocked, not passing — the honest form of the `Day`/`GTD`-admission
/// determinism criterion.** `#[ignore]`d on purpose: with the pinned
/// `option-chain-orderbook` 0.7.0 the venue **cannot** construct any hierarchy
/// leaf with an injected venue clock (no `OptionOrderBook::with_clock`;
/// `get_or_create_*` installs the default `MonotonicClock`; `OrderBook::set_clock`
/// needs `&mut self` while leaves are `Arc`-shared), so `Day`/`GTD` *admission*
/// still reads the leaf's wall clock and is **not** deterministic across runs.
/// Asserting it identical would be a false green, so this test is not run. It is
/// the ready-to-enable check for when the named upstream work lands (threading
/// `Arc<dyn Clock>` through the managers); at that point drop the `#[ignore]` and
/// it should pass unchanged ([02 §5.5b](../docs/02-matching-architecture.md#5-determinism)).
#[test]
#[ignore = "blocked: option-chain-orderbook 0.7.0 does not thread Arc<dyn Clock> to leaf construction, \
            so Day/GTD TIF admission reads the leaf wall clock and is not deterministic across runs"]
fn test_day_gtd_admission_determinism_blocked_by_leaf_clock_gap() {
    // Under an injected venue clock at the leaf, a Day order admitted before its
    // TIF cutoff and re-executed on replay would admit identically. Today the leaf
    // reads wall time, so record vs replay can diverge — hence #[ignore].
    let lineage = LineageId::new("run-day-gtd");
    let commands = vec![
        VenueCommand::Clock {
            now_ms: EventTimestamp::new(1_800_000_000_000),
        },
        add(
            &lineage,
            1,
            CALL,
            "mm",
            0x11,
            Side::Sell,
            50_000,
            3,
            TimeInForce::Day,
        ),
    ];
    let first = record(&commands, &lineage, &[sym(CALL)]);
    let second = record(&commands, &lineage, &[sym(CALL)]);
    assert_eq!(
        first.events, second.events,
        "Day/GTD admission is identical across runs under an injected leaf clock"
    );
}

/// Lint / grep GUARD (#028 acceptance): **no wall-clock read appears on the
/// sequenced path.** Every timestamp on the sequenced order path is obtained from
/// the injected venue clock ([`VenueClock`]) — never `SystemTime` / `Instant` /
/// `chrono`. Asserted by source inspection over `src/exchange/` so a regression
/// that reaches for the wall clock inside the actor turn or the executor fails the
/// suite (the prose "never `SystemTime`" in doc comments is not a call and is not
/// matched — only the call forms are).
///
/// The walk **recurses** subdirectories, so a future `src/exchange/<subdir>/`
/// cannot silently bypass the guard; it also asserts it actually scanned files, so
/// a mis-resolved path can't pass vacuously.
#[test]
fn test_no_wall_clock_read_on_the_sequenced_path() {
    const FORBIDDEN: [&str; 4] = [
        "SystemTime::now(",
        "Instant::now(",
        "Utc::now(",
        "Local::now(",
    ];
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/exchange");
    let mut pending = vec![root.clone()];
    let mut offenders = Vec::new();
    let mut scanned = 0_usize;
    while let Some(dir) = pending.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) => panic!("src/exchange must be readable at {}: {e}", dir.display()),
        };
        for entry in entries {
            let path = match entry {
                Ok(entry) => entry.path(),
                Err(e) => panic!("reading a src/exchange entry: {e}"),
            };
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let contents = match std::fs::read_to_string(&path) {
                Ok(contents) => contents,
                Err(e) => panic!("reading {}: {e}", path.display()),
            };
            scanned += 1;
            for needle in FORBIDDEN {
                if contents.contains(needle) {
                    offenders.push(format!("{}: {needle}", path.display()));
                }
            }
        }
    }
    assert!(
        scanned > 0,
        "the sequenced-path guard scanned no files under {} — check the path",
        root.display()
    );
    assert!(
        offenders.is_empty(),
        "wall-clock read found on the sequenced path (src/exchange); use the injected \
         venue clock instead: {offenders:?}"
    );
}

/// Lint / grep GUARD (#032 acceptance): **no wall-clock-relative
/// `ExpirationDate::Days` construction survives anywhere in the venue.** A `Days(n)`
/// expiry re-resolves against "now" on replay and maps to a different calendar date,
/// so the venue's stored / journaled / identity form is **always**
/// `ExpirationDate::DateTime`; at the clock-free kernel seams the venue converts
/// `DateTime − venue_now` → a `Days`-valued duration, and the kernel never reads
/// wall-clock ([04 §6](../docs/04-market-data-and-replay.md#6-determinism-and-seeding),
/// [02 §5.4](../docs/02-matching-architecture.md#5-determinism),
/// [ADR-0004](../docs/adr/0004-deterministic-replay-with-seeded-clock.md)).
///
/// The acceptance criterion names `src/exchange/` + `src/simulation/`; this guard
/// scans the **whole `src/`** so the allow-list is **complete and future-tight** — a
/// new `Days` use cannot hide in `src/market_maker/` (the pricer seam) either. It is
/// **non-vacuous and honest**: it recurses subdirectories, asserts it scanned files,
/// matches the real construction token (not the prose), and exempts only two
/// auditable classes:
/// - **comment lines** — a doc mention of the variant name is prose, not a call;
/// - lines (or the line directly above a match arm) carrying the greppable sentinel
///   **`days-expiry-allow`** — the explicit, enumerated allow-list of the only
///   legitimate uses, each annotated in-place with its upstream evidence:
///   1. **pricer seam** (`src/market_maker/pricer.rs`) — the clock-free pricing
///      kernel argument;
///   2. **walk x-axis nominal** (`src/simulation/walk.rs`) — where a `DateTime`
///      would call optionstratlib `Xstep::new` → `get_days()` → `Utc::now()`;
///   3. **engine defensive read-arm** (`src/market_maker/engine.rs`) — reads a
///      `Days` duration, never constructs a stored expiry;
///   4. **match-to-reject** (`src/exchange/symbol.rs`) — matches the variant only to
///      refuse it (`validate_venue_expiry`).
///
/// Because it scans the whole tree, `expiration_date`'s `Days.get_years()` being pure
/// while `DateTime.get_years()`/`get_days()` read `Utc::now()` (verified 0.2.1
/// `convert.rs:26,65,83`) is the invariant asserted at every seam at once.
#[test]
fn test_no_days_relative_expiry_survives_anywhere_in_the_venue() {
    const TOKEN: &str = "ExpirationDate::Days";
    const ALLOW: &str = "days-expiry-allow";
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let roots = [manifest.join("src")];

    let mut pending: Vec<std::path::PathBuf> = roots.to_vec();
    let mut offenders = Vec::new();
    let mut scanned = 0_usize;
    while let Some(dir) = pending.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) => panic!("a scanned root must be readable at {}: {e}", dir.display()),
        };
        for entry in entries {
            let path = match entry {
                Ok(entry) => entry.path(),
                Err(e) => panic!("reading a scanned entry: {e}"),
            };
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let contents = match std::fs::read_to_string(&path) {
                Ok(contents) => contents,
                Err(e) => panic!("reading {}: {e}", path.display()),
            };
            scanned += 1;
            let lines: Vec<&str> = contents.lines().collect();
            for (idx, line) in lines.iter().enumerate() {
                if !line.contains(TOKEN) {
                    continue;
                }
                // A doc/line comment mentioning the variant is prose, not a call.
                if line.trim_start().starts_with("//") {
                    continue;
                }
                // An explicitly-annotated legitimate clock-free use (match-to-reject
                // or the walk x-axis nominal) is allowed and auditable. The sentinel
                // may sit on the token line (a trailing comment) or on the line
                // directly above the match arm.
                let prev_allows = idx
                    .checked_sub(1)
                    .and_then(|p| lines.get(p))
                    .is_some_and(|prev| prev.contains(ALLOW));
                if line.contains(ALLOW) || prev_allows {
                    continue;
                }
                offenders.push(format!("{}:{}", path.display(), idx + 1));
            }
        }
    }
    assert!(
        scanned > 0,
        "the expiry guard scanned no files under {roots:?} — check the paths"
    );
    assert!(
        offenders.is_empty(),
        "wall-clock-relative `ExpirationDate::Days` found on the sequenced/simulation \
         path; use an absolute `ExpirationDate::DateTime` resolved against the venue \
         clock (or annotate a genuinely clock-free kernel use with `days-expiry-allow`): \
         {offenders:?}"
    );
}

// ============================================================================
// #031: stepped synthetic sessions — deterministic synthesis
// ============================================================================
//
// The chain SYNTHESIS (grid + smile-shaped IVs + DateTime expiry + instrument set)
// is a pure deterministic function of the `SessionConfig` — no clock, no RNG — so
// the same config yields the byte-identical chain every time (the deterministic
// half of the v0.3 "advances identically for the same seed" line). The stochastic
// stepped PRICE PATH is reproduced from the journal (replay), not seed-regenerated
// — that half is exercised in `tests/integration.rs`
// (`test_stepped_session_replays_from_the_journal`) via the #030 driver with the
// live requote engine muted.

/// The deterministic base instant (the venue clock's default virtual epoch).
const SESSION_BASE_MS: u64 = 1_735_689_600_000;

fn session_config() -> SessionConfig {
    SessionConfig::new(
        "BTC",
        Cents::new(5_000_000),
        30.0,
        0.20,
        WalkTypeConfig::GeometricBrownian,
    )
    .with_strike_interval(500)
    .with_chain_size(7)
    .with_smile_curve(0.6)
    .with_skew_slope(-0.2)
}

#[test]
fn test_stepped_session_synthesis_is_deterministic_for_the_same_config() {
    let config = session_config();
    let a = synthesize_chain(&config, SESSION_BASE_MS).expect("synthesise a");
    let b = synthesize_chain(&config, SESSION_BASE_MS).expect("synthesise b");
    assert_eq!(
        a, b,
        "the same session config synthesises the identical chain (grid + smile IVs)"
    );

    // Non-vacuous: the grid materialised, the smile + (negative) skew shaped the
    // surface — the downside (low) wing is raised — and every expiry is an absolute
    // DateTime (replay-stable).
    assert_eq!(a.strikes.len(), 7);
    let atm = a
        .strikes
        .iter()
        .find(|s| s.strike == 50_000)
        .expect("ATM strike");
    let low_wing = a.strikes.first().expect("a downside wing strike");
    assert!(
        low_wing.iv > atm.iv,
        "the smile + negative skew raise the downside-wing IV: {} !> {}",
        low_wing.iv,
        atm.iv
    );
    assert!(
        matches!(a.expiration, ExpirationDate::DateTime(_)),
        "a synthesised expiry is an absolute DateTime"
    );
}

#[test]
fn test_stepped_session_smile_reshapes_with_the_curve_parameter() {
    // Changing `smile_curve` changes the synthesised surface deterministically —
    // the parameter shapes the chain (the acceptance line), through optionstratlib.
    let flat = synthesize_chain(
        &session_config().with_smile_curve(0.0).with_skew_slope(0.0),
        SESSION_BASE_MS,
    )
    .expect("flat");
    let smiled = synthesize_chain(
        &session_config().with_smile_curve(1.2).with_skew_slope(0.0),
        SESSION_BASE_MS,
    )
    .expect("smiled");
    let flat_wing = flat.strikes.last().expect("wing").iv;
    let smiled_wing = smiled.strikes.last().expect("wing").iv;
    assert!(
        smiled_wing > flat_wing,
        "a larger smile_curve raises the wing IV: {smiled_wing} !> {flat_wing}"
    );
    // The ATM strike is invariant to the smile (m = 0), so only the wings move.
    let flat_atm = flat
        .strikes
        .iter()
        .find(|s| s.strike == 50_000)
        .expect("atm")
        .iv;
    let smiled_atm = smiled
        .strikes
        .iter()
        .find(|s| s.strike == 50_000)
        .expect("atm")
        .iv;
    assert!(
        (flat_atm - smiled_atm).abs() < 1e-9,
        "the ATM IV is smile-invariant"
    );
}

// ============================================================================
// Shared helpers (positions fold + JSON key walk)
// ============================================================================

/// Folds a committed event stream's fills into a positions store through the same
/// post-journal `StoreFanOut` the actor uses.
fn fold_positions(events: &[VenueEvent]) -> Arc<InMemoryPositionsStore> {
    let positions = Arc::new(InMemoryPositionsStore::new());
    let mut fan = StoreFanOut::new(
        Arc::new(InMemoryExecutionsStore::new()),
        Arc::clone(&positions),
        Arc::new(MarkPriceBook::new()),
    );
    for event in events {
        fan.emit(event);
    }
    positions
}

/// Reads a folded position marked at `mark` cents, panicking with a clear message
/// on any store failure or a missing position.
fn position_at(
    positions: &InMemoryPositionsStore,
    account: &AccountId,
    symbol: &Symbol,
    mark: u64,
) -> fauxchange::Position {
    match positions.get(account, symbol, Some(Cents::new(mark))) {
        Ok(Some(position)) => position,
        Ok(None) => panic!("expected a folded position for {account:?} / {symbol}"),
        Err(e) => panic!("positions get failed: {e}"),
    }
}

/// Recursively collects every object key in a JSON value.
fn collect_keys(value: &serde_json::Value, keys: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                keys.insert(key.clone());
                collect_keys(child, keys);
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                collect_keys(child, keys);
            }
        }
        _ => {}
    }
}

// ============================================================================
// #030: the production replay DRIVER (persistent-path oracle)
// ============================================================================
//
// These exercise `fauxchange::simulation::{replay_streams, replay_bundle}` — the
// #030 driver that reuses the ONE re-execution core (`recover`) over the native
// journal + the portable scenario bundle. Sections 1–6 above prove the harness /
// recovery reducer; these prove the driver on top of it reconstructs identical
// events, fills, top-of-book, and the executions/positions fold, per underlying.

/// A [`JournalStream`] from a recording's journal — the driver's native input.
fn driver_stream(recording: &Recording) -> JournalStream {
    JournalStream::new(
        recording.underlying.clone(),
        recording.journal.header().clone(),
        read_all(&recording.journal),
    )
}

/// Drives a command stream through the real single-writer actor for `underlying`
/// and returns its [`JournalStream`] — the same journal the live venue writes.
fn actor_stream(underlying: &str, lineage: &LineageId, commands: &[VenueCommand]) -> JournalStream {
    let header = JournalHeader::new(lineage.clone());
    let mut actor = UnderlyingActor::new(
        ActorConfig::new(underlying, lineage.clone(), 64),
        InMemoryVenueJournal::new(header.clone()),
        MatchingExecutor::new(underlying),
        NoopFanOut,
        CLOCK,
    );
    for command in commands {
        if let Err(e) = actor.handle(command.clone()) {
            panic!("actor turn must commit: {e}");
        }
    }
    JournalStream::new(underlying, header, read_all(actor.journal()))
}

#[test]
fn test_replay_driver_reproduces_events_fills_and_top_of_book() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());
    let stream = driver_stream(&recording);

    let report = match replay_streams(std::slice::from_ref(&stream)) {
        Ok(report) => report,
        Err(e) => panic!("driver replay must not halt on a clean journal: {e}"),
    };
    let replay = report.underlying(UNDERLYING).expect("BTC replay present");

    // Ordered VenueEvent-stream equality per underlying — the oracle, over the
    // production driver path.
    assert_eq!(
        replay.events, recording.events,
        "the driver re-derives the identical ordered event stream"
    );
    // The top-of-book witness after replay matches the recorded end state.
    let recorded_top = recording.tops.last().expect("a recorded top row");
    let witnessed: Vec<_> = recording
        .witnesses
        .iter()
        .map(|w| replay.top_of_book(w))
        .collect();
    assert_eq!(
        &witnessed, recorded_top,
        "reconstructed top-of-book matches"
    );
}

#[test]
fn test_replay_driver_reconstructs_executions_and_positions_fold() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());
    let stream = driver_stream(&recording);

    // Reconstruct through the driver...
    let report = replay_streams(&[stream]).expect("driver replay");

    // ...and independently fold the recorded events (the golden), then compare the
    // reconstructed executions + positions to it — a deterministic function of the
    // journal.
    let golden_positions = fold_positions(&recording.events);

    // Executions: the recording's fill legs equal the driver-reconstructed count.
    let recorded_legs: usize = recording
        .events
        .iter()
        .map(|event| match &event.outcome {
            VenueOutcome::Added { fills, .. } | VenueOutcome::Market { fills, .. } => fills.len(),
            _ => 0,
        })
        .sum();
    assert!(recorded_legs > 0, "the fixture crosses into fills");
    assert_eq!(
        report.executions.len(),
        recorded_legs,
        "the reconstructed executions store has exactly the journaled fill legs"
    );

    // Positions: the taker's reconstructed fold equals the golden fold (mark-free).
    let taker = AccountId::new("t1");
    let symbol = sym(CALL);
    let reconstructed = report
        .positions
        .get(&taker, &symbol, None)
        .expect("positions get")
        .expect("a reconstructed taker position");
    let golden = golden_positions
        .get(&taker, &symbol, None)
        .expect("positions get")
        .expect("a golden taker position");
    assert_eq!(
        reconstructed.net_quantity, golden.net_quantity,
        "the reconstructed positions fold matches the recorded fold"
    );
    assert_eq!(reconstructed.realized_pnl, golden.realized_pnl);
}

#[test]
fn test_replay_driver_mark_prices_are_recomputed_and_excluded_from_the_oracle() {
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());
    let stream = driver_stream(&recording);
    let report = replay_streams(&[stream]).expect("driver replay");

    // The reconstructed positions fold is mark-INDEPENDENT (journaled), but the
    // unrealised P&L is recomputed live from the non-journaled mark — asserting two
    // different marks change only the unrealised half proves marks are excluded.
    let taker = AccountId::new("t1");
    let symbol = sym(CALL);
    let at_entry = report
        .positions
        .get(&taker, &symbol, Some(Cents::new(50_000)))
        .expect("get")
        .expect("position");
    let at_higher = report
        .positions
        .get(&taker, &symbol, Some(Cents::new(70_000)))
        .expect("get")
        .expect("position");
    assert_eq!(
        at_entry.net_quantity, at_higher.net_quantity,
        "the journaled fold is mark-independent"
    );
    assert_ne!(
        at_entry.unrealized_pnl, at_higher.unrealized_pnl,
        "the live-recomputed mark is excluded from the reconstructed oracle"
    );
}

#[test]
fn test_multi_underlying_partial_fan_out_reproduced_per_underlying() {
    // A venue-wide `Clock` advance fans to the underlyings, but ETH's actor did NOT
    // journal it (a PARTIAL fan-out — e.g. a full mailbox / sealed underlying). Each
    // underlying's journal is the ONLY source: replaying both reproduces BTC WITH
    // the Clock and ETH WITHOUT it, per underlying — no venue-wide total order is
    // claimed, and the partial is reproduced exactly from each stream.
    let lineage = LineageId::new("run-multi");
    let clock_now = EventTimestamp::new(1_800_000_000_000);

    let btc = actor_stream(
        "BTC",
        &lineage,
        &[
            add(
                &lineage,
                0,
                CALL,
                "mm",
                0x11,
                Side::Sell,
                50_000,
                3,
                TimeInForce::Gtc,
            ),
            VenueCommand::Clock { now_ms: clock_now },
        ],
    );
    // ETH's stream is over a DIFFERENT contract and did NOT get the Clock.
    let eth = actor_stream(
        "ETH",
        &lineage,
        &[add(
            &lineage,
            0,
            "ETH-20240329-3000-C",
            "mm",
            0x11,
            Side::Sell,
            3_000,
            2,
            TimeInForce::Gtc,
        )],
    );

    let report = replay_streams(&[btc, eth]).expect("multi-underlying driver replay");

    let btc_replay = report.underlying("BTC").expect("BTC replay");
    let eth_replay = report.underlying("ETH").expect("ETH replay");

    // BTC reproduces its Clock advance from its own journal; ETH reproduces none.
    let btc_has_clock = btc_replay.events.iter().any(
        |event| matches!(&event.command, VenueCommand::Clock { now_ms } if *now_ms == clock_now),
    );
    let eth_has_clock = eth_replay
        .events
        .iter()
        .any(|event| matches!(&event.command, VenueCommand::Clock { .. }));
    assert!(btc_has_clock, "BTC reproduces the Clock it journaled");
    assert!(
        !eth_has_clock,
        "ETH reproduces no Clock — the partial fan-out is faithful to each journal"
    );
    // Each underlying's own book is reconstructed independently.
    assert_eq!(btc_replay.last_sequence, Some(SequenceNumber::new(1)));
    assert_eq!(eth_replay.last_sequence, Some(SequenceNumber::new(0)));
}

#[test]
fn test_replay_driver_datetime_expiry_is_replay_stable_via_bundle() {
    // A scenario bundle over an absolute `DateTime`-expiry contract replays
    // identically through the portable bundle path (schema + versions verified),
    // proving replay-stable expiries end to end.
    let lineage = LineageId::new("run-1");
    let recording = record(&rich_stream(&lineage), &lineage, &witnesses());
    let stream = driver_stream(&recording);
    let bundle = ScenarioBundle::new(RunManifest::new(0, ClockMode::Realtime), vec![stream]);

    let report = match replay_bundle(&bundle) {
        Ok(report) => report,
        Err(e) => panic!("a current-version bundle over a DateTime expiry must replay: {e}"),
    };
    let replay = report.underlying(UNDERLYING).expect("BTC replay");
    assert_eq!(
        replay.events, recording.events,
        "the DateTime-expiry session replays identically through the bundle path"
    );
}
