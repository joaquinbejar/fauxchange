//! # The determinism oracle — the v0.3 capstone (#033)
//!
//! `fauxchange`'s product, stated as a **bounded, testable contract** and made
//! enforceable here. **This module doc IS the oracle's index**: each clause of the
//! canonical guarantee and each documented exclusion names the test(s) that enforce
//! it, so the guarantee names exactly what the shipped code backs — no over-claim
//! ([017](../milestones/v0.1-backend-core/017-determinism-test-harness.md),
//! [033](../milestones/v0.3-replay/033-determinism-guarantee-oracle.md),
//! [02 §5–§6](../docs/02-matching-architecture.md),
//! [04 §6](../docs/04-market-data-and-replay.md#6-determinism-and-seeding),
//! [ADR-0004](../docs/adr/0004-deterministic-replay-with-seeded-clock.md),
//! [ADR-0006](../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
//! [TESTING.md §5](../docs/TESTING.md#5-determinism--replay-tests)).
//!
//! ## The canonical guarantee
//!
//! > Given the **same journal** (the `venue.v1` `VenueEvent` stream, including the
//! > `MarketMakerControl` / `Clock` / `SimStep` commands), the **same config
//! > manifest** (seed, clock mode, microstructure config, instrument seed), and the
//! > **same crate/dependency versions**, a replay reproduces **identical fills,
//! > events, and resting book state per underlying**, judged by **ordered
//! > `VenueEvent`-stream equality per underlying** (top-of-book after each event a
//! > cheap witness).
//!
//! Replay and recovery share **one algorithm** — re-execution with the stored event
//! as the integrity oracle — always into a **fresh** registry. The guarantee
//! clauses and the tests that enforce them:
//!
//! | Guarantee clause | Enforcing test(s) |
//! |------------------|-------------------|
//! | Same journal → identical events/fills/top-of-book per underlying (the flagship oracle) | [`test_recorded_session_replays_to_identical_events_and_top_of_book`], [`test_replay_driver_reproduces_events_fills_and_top_of_book`], `tests/property.rs::journal_driver_replay_reconstructs_book` |
//! | Executions store + positions fold are a deterministic function of the journal | [`test_replay_driver_reconstructs_executions_and_positions_fold`] |
//! | Journaled non-order inputs applied **from the command** (`Clock` / `SimStep` / `EvictExpiredOrders { now_ms }`) | [`test_journaled_clock_advance_replays_to_identical_now_ms`] |
//! | One algorithm with recovery (stored event = integrity oracle; corruption halts; newer schema refused) | [`test_recovery_reexecutes_clean_journal_to_events_equal_to_stored`], [`test_recovery_halts_on_corrupted_stored_event_with_exact_underlying_and_sequence`], [`test_recovery_refuses_a_newer_than_binary_schema`] |
//! | Lossless capture on the error / partial paths replays identically | [`test_ioc_order_that_fills_and_errs_is_journaled_with_fills_and_replays`], [`test_partial_replace_replays_identically`] |
//! | Fault-injection at both append stages → gapless / fail-stop restart re-executes identically | [`test_pre_execution_append_failure_reuses_sequence_and_replay_is_gapless`], [`test_post_mutation_append_failure_seals_and_restart_reexecutes_to_identical_event`] |
//! | Replay-stable expiries (`ExpirationDate::DateTime` only) | [`test_datetime_expiry_fixture_replays_identically`], [`test_days_relative_expiry_is_rejected_at_load`], [`test_no_days_relative_expiry_survives_anywhere_in_the_venue`] |
//! | A **stepped session advances identically for the same seed** (config synthesis deterministic) | [`test_stepped_session_synthesis_is_deterministic_for_the_same_config`], [`test_stepped_session_smile_reshapes_with_the_curve_parameter`] |
//! | **Seed isolation** for the venue-owned derivation (seed → lineage → id namespace) | [`test_seed_isolation_for_venue_owned_derivations`] |
//! | No wall-clock read on the sequenced path | [`test_no_wall_clock_read_on_the_sequenced_path`] |
//!
//! ## The documented exclusions — each asserted AS an exclusion (not silently divergent)
//!
//! | Exclusion (recomputed live / out of scope) | Enforcing test / basis |
//! |--------------------------------------------|------------------------|
//! | Mark price / unrealised P&L / Greeks / any derived analytic float | [`test_live_marks_are_recomputed_and_excluded_from_the_event_oracle`], [`test_replay_driver_mark_prices_are_recomputed_and_excluded_from_the_oracle`], [`test_excluded_analytics_are_structurally_absent_from_the_oracle`] |
//! | Process-global numeric registry ids (canonical symbol string is the identity) | [`test_excluded_analytics_are_structurally_absent_from_the_oracle`] (structural absence); the oracle is stated over symbols + `underlying_sequence` throughout |
//! | Engine clock + its `Uuid::new_v4()` trade-id namespace | [`test_engine_clock_value_is_excluded_from_the_captured_outcome`], [`test_excluded_analytics_are_structurally_absent_from_the_oracle`] |
//! | Cross-underlying interleaving (no venue-wide total order) — a PARTIAL fan-out reproduced per underlying | [`test_multi_underlying_partial_fan_out_reproduced_per_underlying`] |
//! | Out-of-sequencer state — an admin snapshot restore starts a NEW lineage, **not** a replay input; a restore-boundary journal fail-stops (single-epoch scope, #030) | [`test_out_of_sequencer_state_is_not_a_replay_input`] |
//! | OHLC bars — an exclusion **by derivation** (same fills ⇒ same bars, not separately asserted) | derivation basis (documented in [04 §7](../docs/04-market-data-and-replay.md#7-ohlc-aggregation)); the fills they derive from are asserted above |
//! | The synthetic price **walk** — journal-driven, **not** seed-regenerated (`optionstratlib` sampler unseedable) | asserted throughout replay (the recorded `SimStep`s replay identically); [`test_stepped_session_synthesis_is_deterministic_for_the_same_config`] separates the deterministic synthesis from the journal-driven path |
//! | Sequenced expiry sweep (`EvictExpiredOrders`) evicts explicit-deadline `Gtd` orders and replays identically | [`test_evict_expired_gtd_orders_replays_identically`], [`test_evict_expired_orders_empty_sweep_replays`] |
//! | `Day`-TIF eviction + `Day`/`Gtd` TIF *admission* determinism — still **deferred** to the upstream leaf-clock seam | [`test_day_gtd_admission_determinism_blocked_by_leaf_clock_gap`] (`#[ignore]`d, ready-to-enable) |
//! | Sequenced instrument-status gate (`SetInstrumentStatus` halts; `AddOrder` into a non-`Active` book is rejected) replays identically | [`test_order_into_halted_instrument_is_rejected_and_replays`], [`test_instrument_status_lifecycle_replays_identically`] |
//! | Sequenced hierarchy `MassCancel` (per-order affected list) replays identically | [`test_mass_cancel_replays_identically`] |
//! | The two annotated `Days` carve-outs at the clock-free kernel seams (walk x-axis, MM pricer) | [`test_no_days_relative_expiry_survives_anywhere_in_the_venue`] enumerates them as the ONLY `Days` sites (#032) |
//! | Boot-time resume of a non-empty durable journal — the reducer exists (#029/#030), the boot wiring is **not yet built** (tracked in #85) | out of this suite's runtime scope (the offline driver replays into a fresh registry, never the live venue) |
//!
//! ## The harness API (record / replay / recover)
//!
//! - [`record`] / `record_with` — drive a `VenueCommand` stream through a fresh
//!   [`MatchingExecutor`], journaling every write-ahead `(command, event)` pair (the
//!   same executor path the single-writer actor drives —
//!   `test_actor_journal_and_harness_record_agree` proves it byte-for-byte).
//! - [`replay`] — reconstruct events + witnesses by re-executing every journaled
//!   `VenueCommand` in `N` order into a **fresh** registry.
//! - [`recover`] — the production recovery reducer: the same re-execution, with the
//!   stored `VenueEvent` as the integrity oracle.
//!
//! The production driver `fauxchange::simulation::{replay_streams, replay_bundle}`
//! (#030) is exercised directly by the `test_replay_driver_*` cases; the randomised
//! sibling `journal_driver_replay_reconstructs_book` lives in `tests/property.rs`.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use fauxchange::exchange::{
    ActorConfig, AddOutcome, CancelReason, Cents, CommandExecutor, EventTimestamp,
    ExecutionContext, ExecutionFilter, ExecutionsStore, ExpirationDate, FanOut, FixedClock, Hash32,
    InMemoryExecutionsStore, InMemoryPositionsStore, InMemoryVenueJournal, InstrumentStatus,
    JournalCommand, JournalError, JournalHeader, JournalRecord, LineageId, MarkPriceBook,
    MassCancelScope, MassCancelType, MatchingExecutor, NoopFanOut, PositionsStore, RecordKind,
    RejectKind, STPMode, SequenceNumber, Side, SignedCents, StoreFanOut, Symbol, SymbolError,
    SymbolParser, TimeInForce, TopOfBook, UnderlyingActor, VenueClock, VenueCommand, VenueEvent,
    VenueJournal, VenueOutcome, recover, validate_venue_expiry,
};
use fauxchange::gateway::fix::enums::{OrdType as FixOrdType, OrderSide, TimeInForce as FixTif};
use fauxchange::gateway::fix::header::{StandardHeader, UtcTimestamp};
use fauxchange::gateway::fix::order::NewOrderSingle;
use fauxchange::gateway::fix::order_flow::to_add_command;
use fauxchange::microstructure::{
    ContractSpecsConfig, FeeConfig, FileMicrostructure, LatencyConfig, MicrostructureConfig,
    StpConfig, StpMode,
};
use fauxchange::simulation::{
    ClockMode, JournalStream, ReplayError, RunManifest, ScenarioBundle, SessionConfig,
    WalkTypeConfig, replay_bundle, replay_streams, synthesize_chain,
};
use fauxchange::state::{AppState, AppStateConfig};
use fauxchange::{AccountId, ClientOrderId, LiquidityFlag, OrderType, VenueError, VenueOrderId};
use ironfix_core::types::{CompId, SeqNum};

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

/// A limit add carrying a `client_order_id` — the account-scoped idempotency key
/// an idempotent resend reuses (#099).
#[allow(clippy::too_many_arguments)]
fn add_keyed(
    lineage: &LineageId,
    sequence: u64,
    raw_symbol: &str,
    account: &str,
    owner_byte: u8,
    side: Side,
    price: u64,
    quantity: u64,
    tif: TimeInForce,
    client_order_id: &str,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(raw_symbol),
        order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(sequence), 0),
        account: AccountId::new(account),
        owner: Hash32([owner_byte; 32]),
        client_order_id: Some(ClientOrderId::new(client_order_id)),
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
                    VenueOutcome::rejected(RejectKind::Internal, "corrupted-by-test"),
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
fn test_idempotent_resend_replays_the_stored_terminal_outcome() {
    // #099: the executor dedups an idempotent resend (same account + ClOrdID) and
    // journals the STORED terminal outcome as the resend event's outcome. Replaying
    // the journal re-executes every command into a fresh registry, so the dedup
    // fires identically and REPRODUCES that stored outcome — the resend stays a
    // deterministic function of the journal (no wall-clock, no RNG, no second fill).
    let lineage = LineageId::new("resend-run");
    let commands = vec![
        // seq0: resting maker sell 2.
        add(
            &lineage,
            0,
            CALL,
            "maker",
            0x11,
            Side::Sell,
            50_000,
            2,
            TimeInForce::Gtc,
        ),
        // seq1: the original crossing taker buy 3, KEYED — fills 2, rests 1.
        add_keyed(
            &lineage,
            1,
            CALL,
            "taker",
            0x22,
            Side::Buy,
            50_000,
            3,
            TimeInForce::Gtc,
            "dup",
        ),
        // seq2: the resend — same account + ClOrdID, a FRESH order id (the grammar id
        // for seq 2). The executor dedups: no second order, no second fill.
        add_keyed(
            &lineage,
            2,
            CALL,
            "taker",
            0x22,
            Side::Buy,
            50_000,
            3,
            TimeInForce::Gtc,
            "dup",
        ),
    ];
    let recording = record(&commands, &lineage, &[sym(CALL)]);

    // The original add crossed into a real fill (non-vacuous).
    assert!(
        matches!(
            &recording.events[1].outcome,
            VenueOutcome::Added { fills, resting_quantity: 1, .. } if fills.len() == 2
        ),
        "the original keyed add fills 2 and rests 1, got {:?}",
        recording.events[1].outcome
    );
    // The resend event carries an idempotent `Duplicate` (#099): it echoes the
    // ORIGINAL placement's identity + terminal sequence and boxes the STORED terminal
    // outcome — byte-identical to the original add's captured outcome, NOT a
    // recomputed second fill. The distinct variant is what makes every fan-out
    // projection treat the resend as a no-op (no double-fold, no phantom id).
    match &recording.events[2].outcome {
        VenueOutcome::Duplicate {
            original_order_id,
            original_sequence,
            terminal,
        } => {
            assert_eq!(
                original_sequence,
                &SequenceNumber::new(1),
                "the resend echoes the ORIGINAL terminal sequence, not the resend turn's"
            );
            assert_eq!(
                original_order_id.as_str(),
                "resend-run:BTC:1:0",
                "the resend echoes the ORIGINAL order id, not the resend turn's fresh id"
            );
            assert_eq!(
                terminal.as_ref(),
                &recording.events[1].outcome,
                "the Duplicate boxes the stored terminal outcome (no second fill)"
            );
        }
        other => panic!("the resend event must be an idempotent Duplicate, got {other:?}"),
    }

    // The oracle: a fresh-registry replay reconstructs the identical event stream —
    // including the resend's stored outcome and its untouched top-of-book witness.
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

/// The sequenced expiry sweep (#47) over an **empty** venue evicts nothing — a real
/// `Evicted { evicted: [] }`, no longer a journaled `Rejected` no-op — and replays
/// deterministically from the journaled `now_ms`.
#[test]
fn test_evict_expired_orders_empty_sweep_replays() {
    let lineage = LineageId::new("run-evict-empty");
    let commands = vec![VenueCommand::EvictExpiredOrders {
        now_ms: EventTimestamp::new(1_900_000_000_000),
    }];
    let recording = record(&commands, &lineage, &[]);
    match &recording.events[0].outcome {
        VenueOutcome::Evicted { evicted } => assert!(
            evicted.is_empty(),
            "an empty venue evicts nothing, got {evicted:?}"
        ),
        other => panic!("expected an empty Evicted sweep, got {other:?}"),
    }
    let replayed = replay(&recording);
    assert_eq!(replayed.events, recording.events);
}

/// The sequenced expiry sweep (#47) evicts a resting **explicit-deadline `Gtd`**
/// order past the journaled `now_ms`, leaves the `Gtc` control resting, and replays
/// to the identical `VenueEvent` stream + top-of-book witness. The `Gtd` deadline is
/// journaled in the `AddOrder`, the sweep cutoff in the `EvictExpiredOrders`, so the
/// eviction is a pure function of the journal.
///
/// The `Gtd` deadline is chosen **far in the future** so the leaf's default
/// wall-clock *admission* check always admits it (the documented leaf-clock gap only
/// bites when a deadline straddles wall-clock time across runs); the sweep cutoff is
/// past the deadline so the sweep fires.
#[test]
fn test_evict_expired_gtd_orders_replays_identically() {
    let lineage = LineageId::new("run-evict-gtd");
    // Far-future deadline (ms): always > the leaf wall clock, so always admitted.
    let gtd_deadline = 9_000_000_000_000_u64;
    let commands = vec![
        // A GTC control order that is never evicted.
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
        // A GTD order whose deadline is past the sweep cutoff below.
        add(
            &lineage,
            1,
            CALL,
            "mm",
            0x11,
            Side::Sell,
            50_100,
            2,
            TimeInForce::Gtd(gtd_deadline),
        ),
        // Sweep cutoff past the GTD deadline: the GTD order is evicted, the GTC is not.
        VenueCommand::EvictExpiredOrders {
            now_ms: EventTimestamp::new(gtd_deadline + 1_000),
        },
    ];
    let recording = record(&commands, &lineage, &[sym(CALL)]);

    // The GTD order's venue id (grammar id for its sequence) is the sole eviction.
    let gtd_id = lineage.venue_order_id(UNDERLYING, SequenceNumber::new(1), 0);
    match &recording.events[2].outcome {
        VenueOutcome::Evicted { evicted } => {
            assert_eq!(evicted, &vec![gtd_id], "only the GTD order is evicted");
        }
        other => panic!("expected an Evicted sweep, got {other:?}"),
    }

    // Same journal ⇒ same events + top-of-book witness on replay.
    let replayed = replay(&recording);
    assert_replay_equals(&recording, &replayed);
}

/// **Blocked, not passing — the honest form of the `Day`/`Gtd`-admission
/// determinism criterion.** `#[ignore]`d on purpose: with the pinned
/// `option-chain-orderbook` 0.7.0 the venue **cannot** construct any hierarchy
/// leaf with an injected venue clock (no `OptionOrderBook::with_clock`;
/// `get_or_create_*` installs the default `MonotonicClock`; `OrderBook::set_clock`
/// needs `&mut self` while leaves are `Arc`-shared), so `Day`/`Gtd` *admission*
/// still reads the leaf's wall clock and is **not** deterministic across runs
/// (nor is `Day`-TIF eviction, whose deadline is a wall-clock-derived market close).
/// The **explicit-deadline `Gtd` eviction** path IS deterministic and is covered by
/// `test_evict_expired_gtd_orders_replays_identically`; this residual admission gap
/// is the ready-to-enable check for when the named upstream work lands (threading
/// `Arc<dyn Clock>` through the managers); at that point drop the `#[ignore]` and it
/// should pass unchanged ([02 §5.5b](../docs/02-matching-architecture.md#5-determinism)).
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

// ============================================================================
// #47: the sequenced lifecycle + hierarchy control plane
// ============================================================================

/// A `SetInstrumentStatus` transition command.
fn set_status(symbol: &str, status: InstrumentStatus) -> VenueCommand {
    VenueCommand::SetInstrumentStatus {
        symbol: sym(symbol),
        status,
    }
}

/// An order into a **halted** instrument is rejected on the sequenced path, and the
/// same rejection replays identically — the halt is journaled state (a prior
/// `SetInstrumentStatus`), so the gate is a deterministic function of the journal
/// (#47).
#[test]
fn test_order_into_halted_instrument_is_rejected_and_replays() {
    let lineage = LineageId::new("run-halt");
    let commands = vec![
        // Halt the call, then try to rest an order on it (rejected), then rest one on
        // the (still Active) put to prove the gate is per-instrument.
        set_status(CALL, InstrumentStatus::Halted),
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
        add(
            &lineage,
            2,
            PUT,
            "mm",
            0x11,
            Side::Buy,
            30_000,
            2,
            TimeInForce::Gtc,
        ),
    ];
    let recording = record(&commands, &lineage, &[sym(CALL), sym(PUT)]);

    // The halted-call add is a no-op Rejected; the put add rests.
    match &recording.events[1].outcome {
        VenueOutcome::Rejected { reason, .. } => {
            assert!(
                reason.contains("Halted"),
                "reason names the status: {reason}"
            );
        }
        other => panic!("an order into a halted instrument must be Rejected, got {other:?}"),
    }
    assert!(matches!(
        &recording.events[2].outcome,
        VenueOutcome::Added {
            resting_quantity: 2,
            ..
        }
    ));
    // The halted call never accepted an order — no bid/ask depth on it.
    assert_eq!(
        recording.tops.last().map(|row| row[0]),
        Some(TopOfBook::default())
    );

    let replayed = replay(&recording);
    assert_replay_equals(&recording, &replayed);
}

/// A Settling and an Expired instrument also refuse new orders — the whole
/// non-`Active` set gates, replaying identically (#47).
#[test]
fn test_order_into_settling_or_expired_instrument_is_rejected_and_replays() {
    let lineage = LineageId::new("run-settle-expire");
    let commands = vec![
        set_status(CALL, InstrumentStatus::Settling),
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
        set_status(PUT, InstrumentStatus::Expired),
        add(
            &lineage,
            3,
            PUT,
            "mm",
            0x11,
            Side::Buy,
            30_000,
            2,
            TimeInForce::Gtc,
        ),
    ];
    let recording = record(&commands, &lineage, &[sym(CALL), sym(PUT)]);
    assert!(matches!(
        &recording.events[1].outcome,
        VenueOutcome::Rejected { .. }
    ));
    assert!(matches!(
        &recording.events[3].outcome,
        VenueOutcome::Rejected { .. }
    ));
    let replayed = replay(&recording);
    assert_replay_equals(&recording, &replayed);
}

/// The full lifecycle `Active → Halted → Active → Settling → Expired` plus an
/// illegal `Expired → Active` (rejected) replays to the identical event stream —
/// the transition legality is delegated to the upstream state machine, and the
/// venue registry is folded deterministically by re-execution (#47).
#[test]
fn test_instrument_status_lifecycle_replays_identically() {
    let lineage = LineageId::new("run-lifecycle");
    let commands = vec![
        set_status(CALL, InstrumentStatus::Halted),
        set_status(CALL, InstrumentStatus::Active), // resume (legal)
        set_status(CALL, InstrumentStatus::Settling),
        set_status(CALL, InstrumentStatus::Expired),
        set_status(CALL, InstrumentStatus::Active), // Expired is terminal (illegal)
    ];
    let recording = record(&commands, &lineage, &[]);

    // The four legal transitions applied; the terminal-escape is rejected.
    for (index, expected) in [
        InstrumentStatus::Halted,
        InstrumentStatus::Active,
        InstrumentStatus::Settling,
        InstrumentStatus::Expired,
    ]
    .into_iter()
    .enumerate()
    {
        match &recording.events[index].outcome {
            VenueOutcome::InstrumentStatusChanged { status, .. } => assert_eq!(*status, expected),
            other => panic!("event {index} must be a status change, got {other:?}"),
        }
    }
    assert!(matches!(
        &recording.events[4].outcome,
        VenueOutcome::Rejected { .. }
    ));

    let replayed = replay(&recording);
    assert_replay_equals(&recording, &replayed);
}

/// A hierarchy `MassCancel` sweeps the resting orders matching its scope + type,
/// emits the ordered per-order affected list, and replays to the identical event
/// stream + top-of-book witness — the affected list is sorted by venue order id,
/// so it is a deterministic function of the journal (#47).
#[test]
fn test_mass_cancel_replays_identically() {
    let lineage = LineageId::new("run-mass-cancel");
    let commands = vec![
        // The `admin` account rests two sells; other accounts rest the buys. A
        // client `BySide` sweep is account-scoped (#97 finding 1: the side filter is
        // never a cross-account authority), so both swept sells belong to `admin`.
        add(
            &lineage,
            0,
            CALL,
            "admin",
            0x11,
            Side::Sell,
            50_000,
            3,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            1,
            CALL,
            "admin",
            0x12,
            Side::Sell,
            50_100,
            2,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            2,
            CALL,
            "c",
            0x13,
            Side::Buy,
            49_900,
            4,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            3,
            PUT,
            "d",
            0x14,
            Side::Buy,
            30_000,
            5,
            TimeInForce::Gtc,
        ),
        // `admin` cancels every one of ITS OWN sells across the whole underlying
        // (both call sells, not the buys) — an account-scoped `BySide` sweep.
        VenueCommand::MassCancel {
            scope: MassCancelScope::Underlying,
            cancel_type: MassCancelType::BySide(Side::Sell),
            account: AccountId::new("admin"),
        },
    ];
    let recording = record(&commands, &lineage, &witnesses());

    // Exactly the two call sells are swept, in venue-order-id sorted order.
    let sell_0 = lineage.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0);
    let sell_1 = lineage.venue_order_id(UNDERLYING, SequenceNumber::new(1), 0);
    match &recording.events[4].outcome {
        VenueOutcome::MassCancelled { affected } => {
            let ids: Vec<_> = affected.iter().map(|leg| leg.order_id.clone()).collect();
            let mut expected = vec![sell_0, sell_1];
            expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            assert_eq!(ids, expected, "only the two sells, sorted by venue id");
            assert!(
                affected
                    .iter()
                    .all(|leg| leg.reason == CancelReason::MassCancel)
            );
        }
        other => panic!("expected a MassCancelled outcome, got {other:?}"),
    }
    // The call ask side is now empty; the buys survive.
    let call_top = recording.tops.last().map(|row| row[0]).unwrap_or_default();
    assert_eq!(call_top.best_ask, None, "both call sells were cancelled");
    assert_eq!(
        call_top.best_bid,
        Some(Cents::new(49_900)),
        "the call buy survives"
    );

    let replayed = replay(&recording);
    assert_replay_equals(&recording, &replayed);
}

/// An **owner-scoped** `MassCancel` (`MassCancelType::ByUser`) — the exact command
/// a client cancel-all (REST) / `OrderMassCancelRequest (q)` (FIX) submits (#97) —
/// sweeps only the requesting owner's resting orders, leaving every other owner's
/// untouched, and replays to the identical event stream + top-of-book witness. This
/// is what makes cross-account isolation a **journaled, deterministic** property:
/// the same journal re-cancels exactly the same owner's orders on replay.
#[test]
fn test_owner_scoped_mass_cancel_replays_identically() {
    let lineage = LineageId::new("run-mass-cancel-by-user");
    let commands = vec![
        // Owner 0x11 rests two buys; owner 0x22 rests one buy, all on the call.
        add(
            &lineage,
            0,
            CALL,
            "trader-1",
            0x11,
            Side::Buy,
            49_900,
            3,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            1,
            CALL,
            "trader-1",
            0x11,
            Side::Buy,
            49_800,
            2,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            2,
            CALL,
            "trader-2",
            0x22,
            Side::Buy,
            49_950,
            4,
            TimeInForce::Gtc,
        ),
        // Owner 0x11 cancels ALL of ITS OWN orders (ByUser) — never owner 0x22's.
        VenueCommand::MassCancel {
            scope: MassCancelScope::Underlying,
            cancel_type: MassCancelType::ByUser(Hash32([0x11; 32])),
            account: AccountId::new("trader-1"),
        },
    ];
    let recording = record(&commands, &lineage, &witnesses());

    // Exactly owner 0x11's two orders are swept, in venue-order-id sorted order; the
    // affected legs carry 0x11's owner, never 0x22's.
    let own_0 = lineage.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0);
    let own_1 = lineage.venue_order_id(UNDERLYING, SequenceNumber::new(1), 0);
    match &recording.events[3].outcome {
        VenueOutcome::MassCancelled { affected } => {
            let ids: Vec<_> = affected.iter().map(|leg| leg.order_id.clone()).collect();
            let mut expected = vec![own_0, own_1];
            expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            assert_eq!(ids, expected, "only owner 0x11's two orders, sorted by id");
            assert!(
                affected.iter().all(|leg| leg.owner == Hash32([0x11; 32])),
                "every swept leg is owner 0x11's — never 0x22's (cross-account isolation)"
            );
        }
        other => panic!("expected a MassCancelled outcome, got {other:?}"),
    }
    // Owner 0x22's resting buy at 49_950 survives as the top of book.
    let call_top = recording.tops.last().map(|row| row[0]).unwrap_or_default();
    assert_eq!(
        call_top.best_bid,
        Some(Cents::new(49_950)),
        "owner 0x22's order survives an owner-scoped sweep of owner 0x11"
    );

    let replayed = replay(&recording);
    assert_replay_equals(&recording, &replayed);
}

/// A per-`Book` `MassCancel` (`MassCancelType::All`) sweeps only the named leaf, and
/// replays identically — proving the scope filter is journal-deterministic (#47).
#[test]
fn test_mass_cancel_book_scope_replays_identically() {
    let lineage = LineageId::new("run-mass-cancel-book");
    let commands = vec![
        add(
            &lineage,
            0,
            CALL,
            "a",
            0x11,
            Side::Sell,
            50_000,
            3,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            1,
            PUT,
            "b",
            0x12,
            Side::Buy,
            30_000,
            5,
            TimeInForce::Gtc,
        ),
        VenueCommand::MassCancel {
            scope: MassCancelScope::Book(sym(CALL)),
            cancel_type: MassCancelType::All,
            account: AccountId::new("admin"),
        },
    ];
    let recording = record(&commands, &lineage, &witnesses());
    let call_id = lineage.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0);
    match &recording.events[2].outcome {
        VenueOutcome::MassCancelled { affected } => {
            assert_eq!(affected.len(), 1, "only the call leaf is swept");
            assert_eq!(affected[0].order_id, call_id);
        }
        other => panic!("expected a MassCancelled outcome, got {other:?}"),
    }
    // The put (a different book) is untouched.
    let put_top = recording.tops.last().map(|row| row[1]).unwrap_or_default();
    assert_eq!(
        put_top.best_bid,
        Some(Cents::new(30_000)),
        "the put is untouched"
    );

    let replayed = replay(&recording);
    assert_replay_equals(&recording, &replayed);
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
        let _ = fan.emit(event);
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

// ============================================================================
// #033 capstone: the remaining exclusions + seed isolation, asserted
// ============================================================================

#[test]
fn test_out_of_sequencer_state_is_not_a_replay_input() {
    // An admin snapshot restore captures STATE, not the sequence of decisions: it
    // opens a NEW journal epoch (a `SnapshotRestored` marker) over restored state
    // that the journal never produced. That restored, out-of-sequencer state is an
    // explicit REPLAY EXCLUSION — the driver re-executes ONE epoch and fail-stops at
    // the first post-restore command whose stored event only holds against the
    // (un-modeled) restored state, rather than silently reproducing it. Proven here
    // through the shipped #030 driver: a stream with a restore boundary halts with a
    // typed `JournalCorruption`, never a divergent resume.
    let lineage = LineageId::new("run-1");
    let stored = record(
        &[add(
            &lineage,
            0,
            CALL,
            "mm",
            0x11,
            Side::Sell,
            50_000,
            3,
            TimeInForce::Gtc,
        )],
        &lineage,
        &[sym(CALL)],
    );
    let mut records = read_all(&stored.journal);
    // The epoch marker opens at the continued sequence 1 (a restore boundary).
    records.push(JournalRecord::epoch(
        fauxchange::exchange::SnapshotRestored::new(
            SequenceNumber::new(1),
            EventTimestamp::new(1_700_000_000_000),
            "snap-1",
            1,
            lineage.clone(),
        ),
    ));
    // A post-restore cancel of an order that only exists in the RESTORED state — its
    // stored event claims `Cancelled`, but a from-empty re-execution rejects it.
    let restored_only = fauxchange::models::VenueOrderId::new("restored-only");
    let cancel_cmd = VenueCommand::CancelOrder {
        symbol: sym(CALL),
        order_id: restored_only.clone(),
        account: AccountId::new("acct"),
    };
    records.push(JournalRecord::command(
        SequenceNumber::new(2),
        EventTimestamp::new(1_700_000_000_000),
        cancel_cmd.clone(),
    ));
    records.push(JournalRecord::event(VenueEvent::new(
        SequenceNumber::new(2),
        EventTimestamp::new(1_700_000_000_000),
        cancel_cmd,
        VenueOutcome::Cancelled {
            order_id: restored_only,
        },
    )));

    let stream = JournalStream::new(UNDERLYING, JournalHeader::new(lineage), records);
    match replay_streams(&[stream]) {
        Err(fauxchange::simulation::ReplayError::JournalCorruption {
            underlying,
            sequence,
        }) => {
            assert_eq!(underlying, UNDERLYING);
            assert_eq!(
                sequence,
                SequenceNumber::new(2),
                "the fail-stop names the first post-restore command; restored state is not reproduced"
            );
        }
        other => panic!("out-of-sequencer restored state must not be reproduced; got {other:?}"),
    }
}

/// The run seed's derived lineage id — the venue-owned derivation the seed
/// actually controls today (`DeterminismConfig::lineage_id`, unit-tested in
/// `src/config.rs`), obtained here through the real layered config loader.
fn lineage_for_seed(seed: u64) -> LineageId {
    let config = fauxchange::config::Config::load_from(std::iter::empty::<String>(), move |key| {
        if key == "FAUXCHANGE_SEED" {
            Some(seed.to_string())
        } else {
            None
        }
    })
    .expect("config loads with a seed override");
    config.determinism.lineage_id()
}

#[test]
fn test_seed_isolation_for_venue_owned_derivations() {
    // Seed isolation is asserted for the venue-owned derivation ONLY. Today the run
    // seed deterministically derives the run LINEAGE, which namespaces every
    // venue-minted id: the same seed reproduces the same id namespace, and DISTINCT
    // seeds produce DISTINCT namespaces (no cross-run id collision). The latency
    // sub-stream (#045) is now a real seed-reproducible venue-owned draw
    // (`test_injected_latency_changes_arrival_order_only`,
    // `test_latency_config_rides_the_bundle_and_gates_replay`); persona jitter stays
    // v0.5 forward-scoped (#047); the price walk is reproduced from the journal, not
    // the seed.

    // Reproducible: the same seed derives the identical lineage → identical ids.
    let a = lineage_for_seed(7);
    let b = lineage_for_seed(7);
    assert_eq!(
        a, b,
        "the same seed derives the same run lineage (reproducible)"
    );
    let seq = SequenceNumber::new(3);
    assert_eq!(
        a.venue_order_id(UNDERLYING, seq, 0),
        b.venue_order_id(UNDERLYING, seq, 0),
        "the same seed mints the identical venue order id"
    );

    // Isolated: a different seed derives a different lineage → a disjoint id
    // namespace, so two runs never collide on a minted id.
    let other = lineage_for_seed(8);
    assert_ne!(a, other, "distinct seeds derive distinct run lineages");
    assert_ne!(
        a.venue_order_id(UNDERLYING, seq, 0),
        other.venue_order_id(UNDERLYING, seq, 0),
        "distinct seeds mint disjoint venue order ids (no cross-run collision)"
    );
    assert_ne!(
        a.execution_id(UNDERLYING, seq, 0),
        other.execution_id(UNDERLYING, seq, 0),
        "distinct seeds mint disjoint execution ids"
    );
}

// ============================================================================
// FIX order arrival is a sequenced-path input (#039): same journal → same fills
// ============================================================================

/// A `NewOrderSingle (D)` typed message for the FIX determinism scenario.
fn fix_new_order(raw_symbol: &str, side: OrderSide, price: u64, quantity: u64) -> NewOrderSingle {
    NewOrderSingle {
        header: StandardHeader::new(
            CompId::new("CLIENT").expect("comp"),
            CompId::new("FAUXCHANGE").expect("comp"),
            SeqNum::new(2),
            UtcTimestamp::from_epoch_ms(0),
        ),
        cl_ord_id: ClientOrderId::new("fix-taker-1"),
        account: None,
        symbol: sym(raw_symbol),
        side,
        transact_time: UtcTimestamp::from_epoch_ms(0),
        ord_type: FixOrdType::Limit,
        price: Some(Cents::new(price)),
        order_qty: quantity,
        time_in_force: FixTif::Gtc,
        expire_time: None,
    }
}

/// An order that arrives over FIX is translated to the **same** `VenueCommand` the
/// REST handler produces (`to_add_command`), so it is an ordinary sequenced-path
/// input: the same journal replays to identical fills, events, and top-of-book —
/// the sequenced-path determinism obligation for a FIX-arriving order (#039,
/// [TESTING.md §5](../docs/TESTING.md#5-determinism--replay-tests)).
#[test]
fn test_fix_arriving_order_replays_to_identical_fills_events_and_top_of_book() {
    let lineage = LineageId::new("fauxchange");

    // seq0: a resting maker ask (3 @ 50_000), added via the ordinary command path.
    let maker = add(
        &lineage,
        0,
        CALL,
        "maker",
        0x11,
        Side::Sell,
        50_000,
        3,
        TimeInForce::Gtc,
    );
    // seq1: the aggressing buy arrives over FIX — translated to the identical
    // AddOrder the REST `D` twin produces, then submitted onto the same path.
    let fix_order = fix_new_order(CALL, OrderSide::Buy, 50_000, 2);
    let order_id: VenueOrderId = lineage.venue_order_id(UNDERLYING, SequenceNumber::new(1), 0);
    let taker = to_add_command(
        &fix_order,
        order_id,
        AccountId::new("fix-taker"),
        Hash32([0x22; 32]),
    )
    .expect("fix D translates to an AddOrder");

    let commands = vec![maker, taker];
    let recording = record(&commands, &lineage, &witnesses());

    // The FIX-arriving order crossed and captured fills.
    let taker_event = &recording.events[1];
    match &taker_event.outcome {
        VenueOutcome::Added { fills, .. } => {
            assert_eq!(fills.len(), 2, "one crossing match = maker + taker legs");
            // The taker leg carries the FIX-arriving account.
            let taker_leg = fills
                .iter()
                .find(|leg| leg.account == AccountId::new("fix-taker"))
                .expect("the FIX taker leg");
            assert_eq!(taker_leg.price, Cents::new(50_000));
            assert_eq!(taker_leg.quantity, 2);
        }
        other => panic!("expected the FIX order to fill, got {other:?}"),
    }

    // Same journal → identical events, fills, and top-of-book on replay.
    let replay = replay(&recording);
    assert_replay_equals(&recording, &replay);
}

// ============================================================================
// #044 microstructure wiring — fees / STP / admission cap + determinism
// ============================================================================
//
// These exercise the four seams the venue wires around the upstream matching for
// #044: the fee schedule + STP mode applied at book creation, the venue-owned
// price-band admission cap at `AppState::submit`, the checked fee on every
// `ExecutionRecord`, and — the flagship #044 test — a fee/STP-sensitive scenario
// recorded live and replayed from the journal + bundled config into IDENTICAL fills
// (incl `Fill.fee`), events, and top-of-book, with a fingerprint mismatch refusing
// replay. Every mutation enters the sequenced order path through `AppState::submit`.

/// Two distinct DateTime-expiry leaves so the fee cross (leaf A) and the STP
/// self-cross (leaf B) never interact through price-time priority.
const MS_CALL_A: &str = "BTC-20260626-50000-C";
const MS_CALL_B: &str = "BTC-20260626-51000-C";

/// A resolved venue microstructure with the given fee rates and STP mode over the
/// baseline specs — the #044 config the venue applies at book creation.
fn ms_config(maker_bps: i32, taker_bps: i32, stp: StpMode) -> MicrostructureConfig {
    let file = FileMicrostructure {
        fees: Some(FeeConfig {
            maker_bps,
            taker_bps,
        }),
        stp: Some(StpConfig { mode: stp }),
        ..FileMicrostructure::default()
    };
    MicrostructureConfig::resolve(&file, &BTreeMap::new()).expect("microstructure resolves")
}

/// An in-memory single-underlying `AppState` carrying `micro` — the live venue the
/// #044 seams are wired into.
fn ms_state(micro: MicrostructureConfig) -> Arc<AppState> {
    let config = AppStateConfig::new([UNDERLYING])
        .with_lineage(LineageId::new("run-ms"))
        .with_microstructure(micro);
    match AppState::new(config) {
        Ok(state) => state,
        Err(error) => panic!("AppState with dev auth must build: {error}"),
    }
}

/// A GTC limit `AddOrder` onto the venue's BTC book. The per-command `stp_mode` is
/// unused by the executor (the leaf's configured STP governs), so it is `None`.
#[allow(clippy::too_many_arguments)]
fn ms_add(
    symbol: &str,
    order_id: &str,
    account: &str,
    owner: u8,
    side: Side,
    price: u64,
    quantity: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: Symbol::parse(symbol).expect("symbol parses"),
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

/// Runs the shared fee/STP-sensitive scenario onto `state`: a two-account crossing
/// fill on leaf A (maker rebate + taker fee) and a same-owner self-cross on leaf B
/// resolved by the configured STP mode. Every command enters the sequenced path.
async fn run_fee_stp_scenario(state: &Arc<AppState>) {
    for command in [
        ms_add(MS_CALL_A, "mk-0", "mkr", 0x11, Side::Sell, 50_000, 3),
        ms_add(MS_CALL_A, "tk-1", "tkr", 0x22, Side::Buy, 50_000, 2),
        ms_add(MS_CALL_B, "sc-2", "sc", 0x33, Side::Sell, 51_000, 2),
        ms_add(MS_CALL_B, "sc-3", "sc", 0x33, Side::Buy, 51_000, 2),
    ] {
        state.submit(command).await.expect("command sequences");
    }
}

/// The flagship #044 determinism test: a fee/STP-sensitive scenario recorded live,
/// then replayed from the journal + the bundled resolved config, reconstructs
/// IDENTICAL fills (including `Fill.fee`), events, and top-of-book — and a
/// fingerprint mismatch refuses replay.
#[tokio::test]
async fn test_fee_stp_sensitive_scenario_replays_exactly_from_bundle() {
    let micro = ms_config(-10, 35, StpMode::CancelTaker);
    let state = ms_state(micro.clone());
    run_fee_stp_scenario(&state).await;

    // The live crossing fill carried the configured fees on BOTH legs of the match:
    // notional 50_000 × 2 = 100_000 → maker rebate −10 bps = −100, taker 35 bps = +350.
    let filter = ExecutionFilter::default();
    let maker_leg = state
        .executions()
        .list(&AccountId::new("mkr"), &filter)
        .expect("list mkr");
    let taker_leg = state
        .executions()
        .list(&AccountId::new("tkr"), &filter)
        .expect("list tkr");
    assert_eq!(maker_leg.len(), 1);
    assert_eq!(taker_leg.len(), 1);
    assert_eq!(maker_leg[0].liquidity, LiquidityFlag::Maker);
    assert_eq!(taker_leg[0].liquidity, LiquidityFlag::Taker);
    assert_eq!(
        maker_leg[0].fee_cents,
        SignedCents::new(-100),
        "the maker leg carries the configured rebate"
    );
    assert_eq!(
        taker_leg[0].fee_cents,
        SignedCents::new(350),
        "the taker leg carries the configured fee"
    );
    // The self-cross was STP-prevented (no fill), so only the two-account match recorded.
    assert_eq!(state.executions().len(), 2);

    // The exported bundle carries the resolved config, and the recorded manifest
    // fingerprint matches it (the config half of the determinism tuple).
    let bundle = state.export_bundle().await.expect("export bundle");
    assert_eq!(
        bundle.microstructure, micro,
        "the bundle carries the resolved venue config"
    );
    assert_eq!(
        bundle.microstructure.fingerprint(),
        bundle.manifest.microstructure_fingerprint,
        "the recorded manifest fingerprint matches the carried config"
    );

    // Replay from the journal + bundled config. `Ok` is itself the identical-events
    // proof: the recovery oracle halts on ANY re-derived VenueEvent — which carries
    // every `Fill` including its `fee`, and the STP-cancel outcome — that ≠ the
    // stored one. So a fee/STP scenario reconstructs exactly.
    let report = replay_bundle(&bundle).expect("fee/STP scenario replays exactly");

    // And the reconstructed executions store reproduces the fee-bearing legs
    // byte-for-byte (every `ExecutionRecord` field is journal-derived, incl `fee_cents`).
    let replay_maker = report
        .executions
        .list(&AccountId::new("mkr"), &filter)
        .expect("replay mkr");
    let replay_taker = report
        .executions
        .list(&AccountId::new("tkr"), &filter)
        .expect("replay tkr");
    assert_eq!(
        replay_maker, maker_leg,
        "the maker leg (incl fee) reconstructs identically"
    );
    assert_eq!(
        replay_taker, taker_leg,
        "the taker leg (incl fee) reconstructs identically"
    );
    assert_eq!(report.executions.len(), 2);

    // Top-of-book reconstructs: leaf A rests the maker's 1 remaining at 50_000; leaf B
    // rests the self-cross seller's 2 at 51_000 (the STP-cancelled taker left none).
    let replay = report.underlying(UNDERLYING).expect("BTC replay");
    let top_a = replay.top_of_book(&Symbol::parse(MS_CALL_A).expect("A parses"));
    assert_eq!(top_a.best_ask, Some(Cents::new(50_000)));
    assert_eq!(top_a.ask_depth, 1);
    assert_eq!(top_a.best_bid, None);
    let top_b = replay.top_of_book(&Symbol::parse(MS_CALL_B).expect("B parses"));
    assert_eq!(top_b.best_ask, Some(Cents::new(51_000)));
    assert_eq!(top_b.ask_depth, 2);

    // A fingerprint mismatch refuses replay: a DIFFERENT config whose fingerprint no
    // longer equals the recorded manifest is a typed reject (like the schema/version
    // guards), never a divergent reproduction under the wrong fee/STP schedule.
    let mut tampered = bundle.clone();
    tampered.microstructure = ms_config(0, 0, StpMode::Off);
    match replay_bundle(&tampered) {
        Err(ReplayError::VersionMismatch {
            kind,
            expected,
            found,
        }) => {
            assert_eq!(kind, "microstructure_fingerprint");
            assert_eq!(expected, bundle.manifest.microstructure_fingerprint);
            assert_eq!(found, tampered.microstructure.fingerprint());
        }
        other => panic!("a fingerprint mismatch must refuse replay, got {other:?}"),
    }
}

/// A resolved venue microstructure whose **BTC** carries a per-underlying spec
/// override (tick 5) over the venue default — the #046 per-instrument profile
/// surface. The scenario prices (50_000 / 51_000) are on-tick multiples of 5.
fn ms_config_with_btc_tick() -> MicrostructureConfig {
    let file = FileMicrostructure {
        fees: Some(FeeConfig {
            maker_bps: -10,
            taker_bps: 35,
        }),
        stp: Some(StpConfig {
            mode: StpMode::CancelTaker,
        }),
        ..FileMicrostructure::default()
    };
    let mut per_underlying = BTreeMap::new();
    per_underlying.insert(
        UNDERLYING.to_string(),
        ContractSpecsConfig {
            tick_size_cents: Some(5),
            ..ContractSpecsConfig::default()
        },
    );
    MicrostructureConfig::resolve(&file, &per_underlying).expect("microstructure resolves")
}

/// #046 determinism: a **profiled instrument** (BTC's per-underlying tick 5)
/// recorded live and replayed from the journal + bundled config reconstructs
/// IDENTICAL fills, events, and top-of-book — the fresh replay book inherits the
/// per-instrument profile. An unconfigured underlying's profile inherits the venue
/// default (asserted directly), so per-instrument microstructure does not break
/// determinism.
#[tokio::test]
async fn test_per_instrument_profile_replays_exactly_from_bundle() {
    let micro = ms_config_with_btc_tick();
    // The profile resolves per instrument: BTC overrides the tick; an unconfigured
    // underlying inherits the venue default (unset knob → venue default).
    assert_eq!(micro.profile_for("BTC").specs().tick_size_cents(), 5);
    assert_eq!(micro.profile_for("ETH").specs(), micro.default_specs());

    let state = ms_state(micro.clone());
    run_fee_stp_scenario(&state).await;

    let filter = ExecutionFilter::default();
    let live_maker = state
        .executions()
        .list(&AccountId::new("mkr"), &filter)
        .expect("list mkr");
    assert_eq!(live_maker.len(), 1, "the on-tick crossing fill recorded");

    // The exported bundle carries the per-instrument profile so the fresh replay
    // book inherits it, and the recorded fingerprint matches.
    let bundle = state.export_bundle().await.expect("export bundle");
    assert_eq!(bundle.microstructure, micro);
    assert_eq!(
        bundle
            .microstructure
            .profile_for("BTC")
            .specs()
            .tick_size_cents(),
        5,
        "the bundle carries the profiled BTC tick"
    );
    assert_eq!(
        bundle.microstructure.fingerprint(),
        bundle.manifest.microstructure_fingerprint,
    );

    // Replay from the journal + bundled config: `Ok` is the identical-events proof
    // (the oracle halts on any re-derived VenueEvent that differs), and the
    // reconstructed fills reproduce byte-for-byte.
    let report = replay_bundle(&bundle).expect("profiled scenario replays exactly");
    let replay_maker = report
        .executions
        .list(&AccountId::new("mkr"), &filter)
        .expect("replay mkr");
    assert_eq!(
        replay_maker, live_maker,
        "a profiled instrument reconstructs its fills identically on replay"
    );
    let replay = report.underlying(UNDERLYING).expect("BTC replay");
    let top_a = replay.top_of_book(&Symbol::parse(MS_CALL_A).expect("A parses"));
    assert_eq!(top_a.best_ask, Some(Cents::new(50_000)));
    assert_eq!(top_a.ask_depth, 1);
}

// ============================================================================
// Latency injection (#045): a seeded, venue-owned, virtual-clock sub-stream
// ============================================================================

/// A canonical debug-string multiset of a command stream — order-independent, so
/// two permutations of the same commands compare equal (proves latency only
/// reorders, never mutates a command).
fn command_multiset(commands: &[VenueCommand]) -> Vec<String> {
    let mut rendered: Vec<String> = commands.iter().map(|c| format!("{c:?}")).collect();
    rendered.sort();
    rendered
}

/// **Latency changes arrival order ONLY.** Latency is designed to be applied at
/// the gateway edge, *before* the sequencer: its sole output is a per-message
/// virtual-clock offset used to reshape the ARRIVAL ORDER into the single writer —
/// it never touches a `VenueCommand`. The live ingress-reorder buffer that consumes
/// the offset is deferred to #111; this test synthesizes the arrival key directly to
/// prove the invariant. So for a FIXED arrival order the journal (and the fills) are
/// a pure function of the ordered commands, unperturbed by whether latency was drawn.
///
/// Distinct from the price walk: the latency sub-stream *is* seed-reproducible (the
/// draw is a pure function of `(seed, session, msg_seq)`), whereas the walk is
/// journal-driven and excluded from same-seed regeneration — the two are kept
/// separate here.
#[test]
fn test_injected_latency_changes_arrival_order_only() {
    let lineage = lineage_for_seed(0xABCD);
    // Five independent resting sells and a crossing buy — every permutation is a
    // valid arrival order (no command depends on an earlier one).
    let base: Vec<VenueCommand> = vec![
        add(
            &lineage,
            0,
            CALL,
            "m0",
            0x10,
            Side::Sell,
            50_000,
            1,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            1,
            CALL,
            "m1",
            0x11,
            Side::Sell,
            50_000,
            1,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            2,
            CALL,
            "m2",
            0x12,
            Side::Sell,
            50_000,
            1,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            3,
            CALL,
            "m3",
            0x13,
            Side::Sell,
            50_000,
            1,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            4,
            CALL,
            "m4",
            0x14,
            Side::Sell,
            50_000,
            1,
            TimeInForce::Gtc,
        ),
        add(
            &lineage,
            5,
            CALL,
            "t5",
            0x22,
            Side::Buy,
            50_000,
            3,
            TimeInForce::Gtc,
        ),
    ];

    // The gateway edge would stamp each inbound message with (session_id, msg_seq) and
    // a base virtual-clock arrival instant (1 ms apart), then add the seeded latency
    // offset to form the effective arrival key it sequences on (the live buffer is
    // #111; here the test synthesizes that key). A uniform 0..5_000 µs
    // offset over 1 ms base spacing is wide enough to reorder arrivals.
    let latency = LatencyConfig::Uniform {
        min_us: 0,
        max_us: 5_000,
    };
    let seed = 0xABCD_u64;
    let mut keyed: Vec<(u64, usize, VenueCommand)> = base
        .iter()
        .cloned()
        .enumerate()
        .map(|(seq, cmd)| {
            let base_ms = 1_000 + seq as u64;
            let arrival_us = latency
                .draw(seed, "session-A", seq as u64)
                .delayed_arrival_us(base_ms);
            (arrival_us, seq, cmd)
        })
        .collect();
    // A stable sort on the effective arrival key = the order the single writer sees.
    keyed.sort_by_key(|(arrival_us, seq, _)| (*arrival_us, *seq));
    let permuted: Vec<VenueCommand> = keyed.iter().map(|(_, _, c)| c.clone()).collect();

    // Latency actually reshaped arrival (not the identity permutation)...
    assert_ne!(
        permuted, base,
        "the latency offsets should reorder arrivals for this seed"
    );
    // ...but only as a permutation: the command multiset is unchanged (no command
    // added, dropped, or mutated).
    assert_eq!(
        command_multiset(&permuted),
        command_multiset(&base),
        "latency reorders arrivals; it never alters the command set"
    );

    // For that FIXED (latency-determined) arrival order, record → replay reconstructs
    // IDENTICAL events, fills, and top-of-book: matching is a pure function of the
    // ordered commands — not perturbed by latency.
    let recording = record(&permuted, &lineage, &witnesses());
    let replay = replay(&recording);
    assert_replay_equals(&recording, &replay);
}

/// **A latency-injected run is self-describing and replays under the oracle scope.**
/// The resolved [`LatencyConfig`] rides in the microstructure config carried by the
/// scenario bundle, its content feeds the recorded manifest fingerprint, and a
/// fingerprint mismatch (a different latency distribution) refuses replay — exactly
/// the fee/STP gate, now covering the latency sub-stream. Combined with
/// `test_injected_latency_changes_arrival_order_only` (the journal replays
/// identically) and the seed-reproducible draw (unit-tested in
/// `src/microstructure/latency.rs`), a latency-injected scenario replays exactly.
#[tokio::test]
async fn test_latency_config_rides_the_bundle_and_gates_replay() {
    let file = FileMicrostructure {
        latency: Some(fauxchange::microstructure::FileLatency {
            model: fauxchange::microstructure::LatencyModel::Lognormal,
            us: None,
            min_us: None,
            max_us: None,
            mean_us: None,
            median_us: Some(250),
            sigma: Some(0.4),
        }),
        ..FileMicrostructure::default()
    };
    let micro = MicrostructureConfig::resolve(&file, &BTreeMap::new()).expect("resolves");
    assert_eq!(
        micro.latency(),
        LatencyConfig::Lognormal {
            median_us: 250,
            sigma: 0.4,
        }
    );

    let state = ms_state(micro.clone());
    run_fee_stp_scenario(&state).await;

    // The exported bundle carries the resolved latency config, and the recorded
    // manifest fingerprint (which now folds in the latency distribution) matches it.
    let bundle = state.export_bundle().await.expect("export bundle");
    assert_eq!(
        bundle.microstructure, micro,
        "the bundle carries the resolved latency config"
    );
    assert_eq!(
        bundle.microstructure.fingerprint(),
        bundle.manifest.microstructure_fingerprint,
        "the recorded manifest fingerprint folds in the latency distribution"
    );
    assert!(
        bundle
            .manifest
            .microstructure_fingerprint
            .contains("latency=lognormal"),
        "the latency distribution is scoped by the fingerprint"
    );
    // The latency-carrying bundle replays exactly (Ok is the identical-events proof).
    replay_bundle(&bundle).expect("latency-injected scenario replays exactly");

    // A DIFFERENT latency distribution shifts the fingerprint → replay is refused,
    // never a divergent reproduction under the wrong arrival-shaping.
    let other_file = FileMicrostructure {
        latency: Some(fauxchange::microstructure::FileLatency {
            model: fauxchange::microstructure::LatencyModel::Fixed,
            us: Some(500),
            min_us: None,
            max_us: None,
            mean_us: None,
            median_us: None,
            sigma: None,
        }),
        ..FileMicrostructure::default()
    };
    let mut tampered = bundle.clone();
    tampered.microstructure =
        MicrostructureConfig::resolve(&other_file, &BTreeMap::new()).expect("resolves");
    match replay_bundle(&tampered) {
        Err(ReplayError::VersionMismatch { kind, .. }) => {
            assert_eq!(kind, "microstructure_fingerprint");
        }
        other => panic!("a latency-fingerprint mismatch must refuse replay, got {other:?}"),
    }
}

/// STP integration: two of one account's own crossing orders resolve per the
/// configured `STPMode` (keyed on the owner `Hash32`), and `off` allows the
/// self-trade — every mutation on the live sequenced path.
#[tokio::test]
async fn test_stp_mode_resolves_a_self_cross_per_configured_mode() {
    // `off`: an account's two crossing orders self-trade (both legs record).
    let off = ms_state(ms_config(0, 0, StpMode::Off));
    off.submit(ms_add(MS_CALL_A, "s0", "self", 0x44, Side::Sell, 50_000, 2))
        .await
        .expect("resting sell sequences");
    off.submit(ms_add(MS_CALL_A, "s1", "self", 0x44, Side::Buy, 50_000, 2))
        .await
        .expect("crossing buy sequences");
    assert_eq!(
        off.executions().len(),
        2,
        "off allows the self-trade (two legs recorded)"
    );

    // `cancel_taker`: the same-owner aggressor is cancelled — no self-trade prints.
    let stp = ms_state(ms_config(0, 0, StpMode::CancelTaker));
    stp.submit(ms_add(MS_CALL_A, "s0", "self", 0x44, Side::Sell, 50_000, 2))
        .await
        .expect("resting sell sequences");
    stp.submit(ms_add(MS_CALL_A, "s1", "self", 0x44, Side::Buy, 50_000, 2))
        .await
        .expect("self-cross sequences (STP-cancelled)");
    assert_eq!(
        stp.executions().len(),
        0,
        "cancel_taker prevents the self-trade"
    );

    // A DIFFERENT owner still crosses the resting sell under cancel_taker — STP is
    // keyed on the account owner `Hash32`, not the symbol.
    stp.submit(ms_add(MS_CALL_A, "o2", "other", 0x55, Side::Buy, 50_000, 2))
        .await
        .expect("distinct-owner cross sequences");
    assert_eq!(
        stp.executions().len(),
        2,
        "distinct owners cross normally under cancel_taker"
    );
}

/// The venue-owned `max_price_cents` admission cap: an over-cap `AddOrder` is
/// rejected at `AppState::submit` with `InvalidOrder` BEFORE the sequencer, so it is
/// never journaled; an at-cap order is admitted and journaled.
#[tokio::test]
async fn test_over_max_price_add_order_is_rejected_at_submit_and_never_journaled() {
    // The neutral venue uses the baseline band (max_price_cents = 100_000_000).
    let state = ms_state(MicrostructureConfig::default());
    let over = ms_add(MS_CALL_A, "hi", "acct", 0x11, Side::Buy, 100_000_001, 1);
    match state.submit(over).await {
        Err(VenueError::InvalidOrder(detail)) => assert!(
            detail.contains("max_price_cents"),
            "the rejection names the venue cap, got: {detail}"
        ),
        other => panic!("an over-cap price must be rejected at submit, got {other:?}"),
    }
    // Nothing was journaled: the command never reached the sequencer.
    let snapshot = state
        .journal_snapshot(UNDERLYING)
        .await
        .expect("snapshot BTC");
    assert!(
        snapshot.records.is_empty(),
        "a rejected-at-admission order is never journaled"
    );

    // An order at the exact cap IS admissible and reaches the sequencer.
    let at_cap = ms_add(MS_CALL_A, "cap", "acct", 0x11, Side::Buy, 100_000_000, 1);
    state
        .submit(at_cap)
        .await
        .expect("the cap price is admissible");
    let after = state
        .journal_snapshot(UNDERLYING)
        .await
        .expect("snapshot BTC again");
    assert!(
        !after.records.is_empty(),
        "an in-band order IS journaled onto the sequenced path"
    );
}

/// Fee-on-`ExecutionRecord`: a filled leg carries the integer-cents fee (a maker
/// rebate negative, a taker fee positive) net on the authoritative record — no `f64`.
#[tokio::test]
async fn test_filled_leg_carries_the_integer_cents_fee_on_the_execution_record() {
    let state = ms_state(ms_config(-10, 35, StpMode::Off));
    state
        .submit(ms_add(MS_CALL_A, "m", "mkr", 0x11, Side::Sell, 50_000, 2))
        .await
        .expect("resting sell sequences");
    state
        .submit(ms_add(MS_CALL_A, "t", "tkr", 0x22, Side::Buy, 50_000, 2))
        .await
        .expect("crossing buy sequences");

    let filter = ExecutionFilter::default();
    let maker = state
        .executions()
        .list(&AccountId::new("mkr"), &filter)
        .expect("list mkr");
    let taker = state
        .executions()
        .list(&AccountId::new("tkr"), &filter)
        .expect("list tkr");
    assert_eq!(maker.len(), 1);
    assert_eq!(taker.len(), 1);
    // notional 100_000 → maker rebate −100 cents, taker fee +350 cents (integer cents).
    assert_eq!(maker[0].fee_cents, SignedCents::new(-100));
    assert_eq!(taker[0].fee_cents, SignedCents::new(350));
}

/// P2-1 (security): the venue price band is enforced on the **replay re-execution**
/// path, not only at live `AppState::submit`. A hostile `ScenarioBundle` carrying an
/// `AddOrder` whose price bypasses the live admission seam is refused before the
/// command re-executes.
#[test]
fn test_replay_refuses_an_out_of_band_price_in_a_bundle() {
    const TS: EventTimestamp = EventTimestamp::new(1_700_000_000_000);
    // A narrow-band venue config (max_price_cents = 1_000) that still passes the
    // checked-fee proof; its fingerprint pins the manifest so the fingerprint gate
    // passes and re-execution reaches the band check.
    let file = FileMicrostructure {
        specs: Some(ContractSpecsConfig {
            max_price_cents: Some(1_000),
            ..ContractSpecsConfig::default()
        }),
        ..FileMicrostructure::default()
    };
    let config = MicrostructureConfig::resolve(&file, &BTreeMap::new()).expect("resolves");

    // A journaled AddOrder at 2_000 cents — above the 1_000 band. The live venue would
    // have rejected this at submit before journaling; here it is smuggled into the
    // bundle's journal stream directly.
    let over = ms_add(MS_CALL_A, "hostile", "acct", 0x11, Side::Buy, 2_000, 1);
    let records = vec![JournalRecord::command(SequenceNumber::new(0), TS, over)];
    let stream = JournalStream::new(
        UNDERLYING,
        JournalHeader::new(LineageId::new("run-band")),
        records,
    );
    let manifest = RunManifest::new(0, ClockMode::Realtime)
        .with_microstructure_fingerprint(config.fingerprint());
    let bundle = ScenarioBundle::new(manifest, vec![stream]).with_microstructure(config);

    match replay_bundle(&bundle) {
        Err(ReplayError::PriceOutOfBand { detail }) => assert!(
            detail.contains("2000") && detail.contains("max_price_cents"),
            "the reject names the band violation, got: {detail}"
        ),
        other => panic!("an out-of-band bundle price must be refused, got {other:?}"),
    }
}

/// P2-2 (security + architect): the checked-fee proof is re-run on a bundle's carried
/// config. `ScenarioBundle.microstructure` deserializes directly, bypassing
/// `MicrostructureConfig::resolve` (and thus the proof); the fingerprint gate is
/// tamper-DETECT, not authenticity, so a self-consistent hostile bundle self-computes
/// a matching fingerprint. The driver re-runs the proof BEFORE any command
/// re-executes → the unprovable fee config is refused with the SPECIFIC config
/// rejection, not a downstream generic `MoneyError::Overflow` at the fill seam.
#[test]
fn test_replay_refuses_an_unprovable_fee_config_in_a_bundle() {
    // A config that would saturate `FeeSchedule::calculate_fee` (taker 100% bps on a
    // u64::MAX × u64::MAX notional) — one `resolve` would REJECT, deserialized here to
    // model the bypass.
    let json = r#"{
        "fees": {"maker_bps": 0, "taker_bps": 10000},
        "stp": {"mode": "off"},
        "default_specs": {"tick_size_cents":1,"lot_size":1,"min_price_cents":1,"max_price_cents":18446744073709551615,"max_order_qty":18446744073709551615},
        "per_underlying": {}
    }"#;
    let bad: MicrostructureConfig =
        serde_json::from_str(json).expect("deserializes directly, bypassing resolve");
    // The attacker pins a MATCHING fingerprint so the equality gate cannot catch it —
    // only the re-run proof does.
    let manifest =
        RunManifest::new(0, ClockMode::Realtime).with_microstructure_fingerprint(bad.fingerprint());
    let bundle = ScenarioBundle::new(manifest, Vec::new()).with_microstructure(bad);

    match replay_bundle(&bundle) {
        Err(ReplayError::ConfigRejected { detail }) => assert!(
            detail.contains("fee") && detail.contains("notional"),
            "the reject is the specific checked-fee proof failure, got: {detail}"
        ),
        other => {
            panic!("an unprovable fee config must be refused with ConfigRejected, got {other:?}")
        }
    }
}
