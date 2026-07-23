//! Integration tests for the per-underlying single-writer actor
//! ([006](../milestones/v0.1-backend-core/006-single-writer-actor-inmem-journal.md)),
//! exercised through its **public** surface from an external crate — which also
//! proves the [`CommandExecutor`] / [`FanOut`] / [`VenueJournal`] / [`VenueClock`]
//! seams are implementable outside `fauxchange` (as #007 / #008 / #029 must).
//!
//! - `test_actor_round_trip_journals_submitted_command` — submit → receipt →
//!   the command **and** its paired event are journaled at the assigned
//!   sequence, observed through the mailbox (`submit` + `snapshot`).
//! - The determinism fault-injection rows
//!   ([TESTING.md §5](../docs/TESTING.md#5-determinism--replay-tests)):
//!   a **pre-execution** append failure leaves the book untouched (the executor
//!   never runs) and reuses `N`; a **post-mutation** event append failure
//!   **seals** the underlying and suppresses fan-out.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use fauxchange::exchange::{
    ActorConfig, CommandExecutor, EventTimestamp, ExecutionContext, FanOut, FanOutSealed,
    FixedClock, InMemoryVenueJournal, JournalError, JournalHeader, JournalRecord, LineageId,
    NoopFanOut, PlaceholderExecutor, RecordKind, SequenceNumber, Symbol, VenueClock, VenueCommand,
    VenueEvent, VenueJournal, VenueOutcome, spawn_underlying_actor,
};
use fauxchange::{AccountId, VenueError, VenueOrderId};

// ---- fixtures ------------------------------------------------------------

fn sym() -> Symbol {
    match Symbol::parse("BTC-20240329-50000-C") {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
    }
}

fn cancel(tag: &str) -> VenueCommand {
    VenueCommand::CancelOrder {
        symbol: sym(),
        order_id: VenueOrderId::new(format!("order-{tag}")),
        account: AccountId::new("acct-1"),
    }
}

fn journal() -> InMemoryVenueJournal {
    InMemoryVenueJournal::new(JournalHeader::new(LineageId::new("run-1")))
}

fn config(mailbox_capacity: usize) -> ActorConfig {
    ActorConfig::new("BTC", LineageId::new("run-1"), mailbox_capacity)
}

const CLOCK: FixedClock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));

// ---- external seam implementations --------------------------------------

/// An externally-defined [`CommandExecutor`] that counts executions — proves the
/// seam is implementable outside the crate and lets a test observe whether a
/// turn reached step 3.
struct CountingExecutor {
    calls: Arc<AtomicU32>,
}

impl CommandExecutor for CountingExecutor {
    fn execute(&mut self, _context: ExecutionContext<'_>) -> VenueOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        VenueOutcome::Cancelled {
            order_id: VenueOrderId::new("executed"),
        }
    }
}

/// An externally-defined [`FanOut`] that counts emitted events.
struct CountingFanOut {
    emits: Arc<AtomicU32>,
}

impl FanOut for CountingFanOut {
    fn emit(&mut self, _event: &VenueEvent) -> Result<(), FanOutSealed> {
        self.emits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Which append to fault, and how the durable store reported it.
#[derive(Clone, Copy)]
enum FaultMode {
    Confirmed,
    AmbiguousNotCommitted,
}

/// An externally-defined fault-injecting [`VenueJournal`] wrapping the in-memory
/// store — the determinism fault-injection substrate.
struct FaultJournal {
    inner: InMemoryVenueJournal,
    fail_at: Option<(SequenceNumber, RecordKind)>,
    mode: FaultMode,
}

impl FaultJournal {
    fn new(fail_at: (SequenceNumber, RecordKind), mode: FaultMode) -> Self {
        Self {
            inner: journal(),
            fail_at: Some(fail_at),
            mode,
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
            return match self.mode {
                FaultMode::Confirmed => Err(JournalError::AppendFailed("injected".to_string())),
                FaultMode::AmbiguousNotCommitted => {
                    Err(JournalError::Ambiguous("injected".to_string()))
                }
            };
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

/// A deterministic clock defined outside the crate (proves the seam is open).
struct TestClock(EventTimestamp);

impl VenueClock for TestClock {
    fn now_ms(&self) -> EventTimestamp {
        self.0
    }
}

// ---- round-trip ----------------------------------------------------------

#[tokio::test]
async fn test_actor_round_trip_journals_submitted_command() {
    let (handle, _shutdown, join) = spawn_underlying_actor(
        config(16),
        journal(),
        PlaceholderExecutor,
        NoopFanOut,
        CLOCK,
    );

    let receipt = match handle.submit(cancel("a")).await {
        Ok(r) => r,
        Err(e) => panic!("submit failed: {e}"),
    };
    assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0));

    // The mailbox round-trip journaled BOTH the write-ahead command and its
    // paired event at the assigned sequence.
    let snapshot = match handle.snapshot().await {
        Ok(s) => s,
        Err(e) => panic!("snapshot failed: {e}"),
    };
    assert_eq!(snapshot.last_sequence, Some(SequenceNumber::new(0)));
    assert!(
        snapshot
            .records
            .iter()
            .any(|r| r.sequence() == SequenceNumber::new(0) && r.kind() == RecordKind::Command)
    );
    assert!(
        snapshot
            .records
            .iter()
            .any(|r| r.sequence() == SequenceNumber::new(0) && r.kind() == RecordKind::Event)
    );

    // Dropping the handle is the shutdown path: the task completes cleanly.
    drop(handle);
    match join.await {
        Ok(()) => {}
        Err(e) => panic!("actor did not shut down cleanly: {e}"),
    }
}

#[tokio::test]
async fn test_actor_uses_externally_defined_seams() {
    // Every collaborator seam is implemented in this external crate.
    let executor = CountingExecutor {
        calls: Arc::new(AtomicU32::new(0)),
    };
    let calls = Arc::clone(&executor.calls);
    let (handle, _shutdown, join) = spawn_underlying_actor(
        config(16),
        journal(),
        executor,
        NoopFanOut,
        TestClock(EventTimestamp::new(42)),
    );

    match handle.submit(cancel("a")).await {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0)),
        Err(e) => panic!("submit failed: {e}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    drop(handle);
    let _ = join.await;
}

// ---- explicit mid-flight shutdown signal (#139) --------------------------

#[tokio::test]
async fn test_explicit_shutdown_signal_stops_a_live_actor_from_the_owner_handle() {
    // The #139 public capability, exercised from an EXTERNAL crate: an explicit
    // `ActorShutdown::shutdown()` stops the actor even while a data-plane
    // `ActorHandle` clone is still alive — the pre-#139 venue could only stop on
    // last-handle-drop. The kill authority lives on the owner-only `ActorShutdown`
    // the spawn returns separately, NOT on the broadly-shared `ActorHandle`.
    let (handle, shutdown, join) = spawn_underlying_actor(
        config(16),
        journal(),
        PlaceholderExecutor,
        NoopFanOut,
        CLOCK,
    );

    match handle.submit(cancel("live")).await {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0)),
        Err(e) => panic!("a pre-shutdown submit must commit: {e}"),
    }

    // Trigger the signal through the owner handle; the actor drains and exits
    // WITHOUT any data-plane handle being dropped (a clone is still held here).
    shutdown.shutdown();
    match join.await {
        Ok(()) => {}
        Err(e) => panic!("the explicit signal must stop the actor: {e}"),
    }

    // The handle is still alive, but the actor stopped because it was SIGNALLED —
    // so a further submit fails fast with `ShuttingDown` (not the generic
    // `JournalUnavailable`), the same typed vocabulary the drain uses (#139 review).
    match handle.submit(cancel("after")).await {
        Err(VenueError::ShuttingDown) => {}
        other => panic!("a submit after an explicit shutdown must be ShuttingDown, got {other:?}"),
    }

    // Idempotent: a second shutdown() on the already-stopped actor is a no-op.
    shutdown.shutdown();
}

// ---- determinism: pre-execution append failure ---------------------------

#[tokio::test]
async fn test_pre_execution_append_failure_leaves_book_untouched_and_reuses_sequence() {
    let calls = Arc::new(AtomicU32::new(0));
    let emits = Arc::new(AtomicU32::new(0));
    let executor = CountingExecutor {
        calls: Arc::clone(&calls),
    };
    let fan_out = CountingFanOut {
        emits: Arc::clone(&emits),
    };
    let fault = FaultJournal::new(
        (SequenceNumber::new(0), RecordKind::Command),
        FaultMode::Confirmed,
    );
    let (handle, _shutdown, join) =
        spawn_underlying_actor(config(16), fault, executor, fan_out, CLOCK);

    // Pre-execution write-ahead append fails: the command is rejected, nothing
    // executed, nothing fanned out.
    match handle.submit(cancel("a")).await {
        Err(VenueError::JournalUnavailable) => {}
        other => panic!("expected JournalUnavailable, got {other:?}"),
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the book must be untouched"
    );
    assert_eq!(
        emits.load(Ordering::SeqCst),
        0,
        "no fan-out on a rejected command"
    );

    // The reused sequence 0 now commits (no gap, the underlying is not sealed).
    match handle.submit(cancel("b")).await {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0)),
        Err(e) => panic!("retry failed: {e}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let snapshot = match handle.snapshot().await {
        Ok(s) => s,
        Err(e) => panic!("snapshot failed: {e}"),
    };
    // Exactly one committed sequence (the reused N=0), no gap, no tombstone.
    let command_records = snapshot
        .records
        .iter()
        .filter(|r| r.kind() == RecordKind::Command)
        .count();
    assert_eq!(command_records, 1);

    drop(handle);
    let _ = join.await;
}

// ---- determinism: ambiguous-not-committed reuses the sequence ------------

#[tokio::test]
async fn test_ambiguous_not_committed_append_reuses_sequence() {
    let calls = Arc::new(AtomicU32::new(0));
    let executor = CountingExecutor {
        calls: Arc::clone(&calls),
    };
    let fault = FaultJournal::new(
        (SequenceNumber::new(0), RecordKind::Command),
        FaultMode::AmbiguousNotCommitted,
    );
    let (handle, _shutdown, join) =
        spawn_underlying_actor(config(16), fault, executor, NoopFanOut, CLOCK);

    // Ambiguous AND the tail read-back finds nothing committed → reuse N.
    match handle.submit(cancel("a")).await {
        Err(VenueError::JournalUnavailable) => {}
        other => panic!("expected JournalUnavailable, got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    match handle.submit(cancel("b")).await {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0)),
        Err(e) => panic!("retry failed: {e}"),
    }

    drop(handle);
    let _ = join.await;
}

// ---- determinism: post-mutation append failure seals ---------------------

#[tokio::test]
async fn test_post_mutation_append_failure_seals_underlying_and_suppresses_fan_out() {
    let calls = Arc::new(AtomicU32::new(0));
    let emits = Arc::new(AtomicU32::new(0));
    let executor = CountingExecutor {
        calls: Arc::clone(&calls),
    };
    let fan_out = CountingFanOut {
        emits: Arc::clone(&emits),
    };
    let fault = FaultJournal::new(
        (SequenceNumber::new(0), RecordKind::Event),
        FaultMode::Confirmed,
    );
    let (handle, _shutdown, join) =
        spawn_underlying_actor(config(16), fault, executor, fan_out, CLOCK);

    // The command executed (the book was mutated) but the paired-event append
    // fails, so the turn seals the underlying and emits no fan-out.
    match handle.submit(cancel("a")).await {
        Err(VenueError::JournalUnavailable) => {}
        other => panic!("expected JournalUnavailable, got {other:?}"),
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the command executed before sealing"
    );
    assert_eq!(
        emits.load(Ordering::SeqCst),
        0,
        "no fan-out on a sealed turn"
    );

    // The underlying is sealed: further commands are rejected without executing,
    // even though the injected fault has cleared.
    match handle.submit(cancel("b")).await {
        Err(VenueError::JournalUnavailable) => {}
        other => panic!("expected a sealed JournalUnavailable, got {other:?}"),
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a sealed underlying never executes again"
    );

    drop(handle);
    let _ = join.await;
}
