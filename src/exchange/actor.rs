//! The per-underlying **single-writer actor** and its write-ahead durability
//! protocol — the determinism foundation every order flows through
//! ([02 §4–§5, §8](../../../docs/02-matching-architecture.md),
//! [ADR-0006 §2–§3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//!
//! ## Why an actor
//!
//! The upstream `SequencedUnderlyingOrderBook::submit(&self)` is a
//! *sequence-number allocator plus executor*, not a single writer and not a
//! write-ahead log: it takes `&self` (two callers can execute out of the order
//! they were numbered in) and its `journal.append` runs **after** the book
//! mutation. Making **one `tokio` task per underlying** the sole caller
//! neutralises the race, and the venue's own write-ahead journal
//! ([`crate::exchange::journal`]) closes the durability gap
//! ([ADR-0006](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//!
//! Gateways never touch a book or the sequencer directly: they send a
//! [`VenueCommand`] to the actor's **bounded** mailbox via [`ActorHandle::submit`]
//! and await a [`Receipt`].
//!
//! ## The turn protocol (per command `C` assigned sequence `N`)
//!
//! [`UnderlyingActor::handle`] runs the whole turn synchronously — matching is a
//! lock-free synchronous path, so the turn holds **no lock across an `.await`**:
//!
//! 1. **Write-ahead (step 1).** Append the [`VenueCommand`] envelope for
//!    `(N, C)` **before** executing. `N` is treated as advanced only on a
//!    **confirmed** append: a confirmed failure **reuses `N`** (nothing executes,
//!    book untouched, no gap); an **ambiguous** result is resolved by a durable
//!    **tail read-back** (idempotent).
//! 2. **Receipt (step 2).** The assigned `underlying_sequence` is committed to
//!    the caller.
//! 3. **Execute + capture (steps 3–4).** The [`CommandExecutor`] seam drives
//!    upstream matching and captures the lossless [`VenueOutcome`], then the
//!    paired [`VenueEvent`] is appended. **This seam is filled by #007**; #006
//!    ships the [`PlaceholderExecutor`], and the command→event **pairing** and
//!    the fan-out-after-event **ordering** are already real and tested.
//! 4. **Fan-out (step 5).** Only **after** the paired event is journaled does
//!    fan-out ([`FanOut`]) begin.
//!
//! A **post-mutation** event-append failure **seals** the underlying
//! ([`VenueError::JournalUnavailable`], no fan-out) rather than build the next
//! command on unjournaled state; a sequence **exhaustion** at `u64::MAX` seals
//! with [`VenueError::SequenceExhausted`], never wraps.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::error::VenueError;
use crate::exchange::envelope::{VenueCommand, VenueEvent, VenueOutcome};
use crate::exchange::event::{EventTimestamp, SequenceNumber};
use crate::exchange::identity::LineageId;
use crate::exchange::journal::{JournalError, JournalRecord, RecordKind, VenueJournal};

// ============================================================================
// Clock seam — the venue time service (never `SystemTime`)
// ============================================================================

/// The venue clock the actor stamps [`VenueEvent::venue_ts`] from — a **venue
/// service**, never `SystemTime`, so the sequenced path stays deterministic
/// ([01 §9](../../../docs/01-domain-model.md),
/// [02 §5.3](../../../docs/02-matching-architecture.md)).
///
/// #006 ships only the deterministic [`FixedClock`]; the seeded / stepped clock
/// service that advances on `Clock` / `SimStep` commands lands with the wider
/// clock wiring. `now_ms` must never read the wall clock on the sequenced path.
pub trait VenueClock: Send {
    /// The current venue-clock instant in **milliseconds**.
    #[must_use]
    fn now_ms(&self) -> EventTimestamp;
}

/// A deterministic fixed-instant [`VenueClock`] for #006 and tests — every event
/// is stamped with the same venue-clock value, which is sufficient because the
/// journaled total order is the `underlying_sequence`, not `venue_ts`.
#[derive(Debug, Clone, Copy)]
pub struct FixedClock(EventTimestamp);

impl FixedClock {
    /// Builds a fixed clock that always returns `instant`.
    #[must_use]
    #[inline]
    pub const fn new(instant: EventTimestamp) -> Self {
        Self(instant)
    }
}

impl VenueClock for FixedClock {
    #[inline]
    fn now_ms(&self) -> EventTimestamp {
        self.0
    }
}

// ============================================================================
// Execute seam — filled by #007
// ============================================================================

/// The read-only context the actor hands the [`CommandExecutor`] for one turn.
///
/// It carries everything #007 needs to drive upstream matching and mint
/// deterministic ids: the underlying ticker, the run [`LineageId`], the assigned
/// `sequence`, the stamped `venue_ts`, and the command itself.
#[derive(Debug)]
pub struct ExecutionContext<'a> {
    /// The underlying ticker (e.g. `"BTC"`), the id-grammar disambiguator.
    pub underlying: &'a str,
    /// The run lineage that namespaces every minted id.
    pub lineage_id: &'a LineageId,
    /// The per-underlying sequence assigned to this command.
    pub sequence: SequenceNumber,
    /// The venue-clock instant stamped on the paired event.
    pub venue_ts: EventTimestamp,
    /// The command to execute.
    pub command: &'a VenueCommand,
}

/// The **step 3–4 seam**: execute a sequenced command against the underlying's
/// books and capture the lossless [`VenueOutcome`]
/// ([ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
///
/// It is **synchronous** because the upstream matching hot path is lock-free and
/// synchronous — the actor never awaits inside a turn. **#007** implements this
/// against the upstream `SequencedUnderlyingOrderBook`, capturing the
/// `MatchResult`; #006 ships the [`PlaceholderExecutor`]. An engine error is
/// captured **into** the outcome (`Rejected`, or fills-with-remainder), never
/// surfaced as a turn failure — the paired event is always produced.
pub trait CommandExecutor: Send {
    /// Executes one command and returns the captured outcome.
    #[must_use]
    fn execute(&mut self, context: ExecutionContext<'_>) -> VenueOutcome;
}

/// The #006 placeholder executor: matching is **not yet wired** (that is #007),
/// so it captures a `Rejected` outcome. The command→event pairing, journaling,
/// and fan-out ordering around it are real; only the fill capture is pending.
#[derive(Debug, Default, Clone, Copy)]
pub struct PlaceholderExecutor;

impl CommandExecutor for PlaceholderExecutor {
    #[inline]
    fn execute(&mut self, _context: ExecutionContext<'_>) -> VenueOutcome {
        VenueOutcome::Rejected {
            reason: "matching not wired yet (pending #007)".to_string(),
        }
    }
}

// ============================================================================
// Fan-out seam — filled by #008
// ============================================================================

/// The **step 5 seam**: emit a committed [`VenueEvent`] to the fan-out consumers
/// (executions store, positions fold, WS/FIX broadcast, OHLC —
/// [02 §6](../../../docs/02-matching-architecture.md)).
///
/// The actor calls [`emit`](Self::emit) **only after** the paired event is
/// journaled, and **never** when the turn sealed on a post-mutation append
/// failure. #006 ships the [`NoopFanOut`]; #008 wires the real consumers.
pub trait FanOut: Send {
    /// Emits one committed event to the fan-out consumers.
    fn emit(&mut self, event: &VenueEvent);
}

/// The #006 no-op fan-out — the consumers land in #008.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopFanOut;

impl FanOut for NoopFanOut {
    #[inline]
    fn emit(&mut self, _event: &VenueEvent) {}
}

// ============================================================================
// Receipt + seal
// ============================================================================

/// The committed result of a submitted command — the caller's acknowledgement
/// that the command was accepted, journaled write-ahead, and assigned a place in
/// the underlying's total order ([ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
///
/// #007 extends the fan-out path with the captured outcome; #006 commits the
/// assigned sequence and venue timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    /// The per-underlying sequence assigned to the command.
    pub underlying_sequence: SequenceNumber,
    /// The venue-clock instant stamped on the paired event.
    pub venue_ts: EventTimestamp,
}

/// Why an underlying was sealed — a **permanent** rejection of all further
/// commands ([ADR-0006 §2–§3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
/// [08 §5](../../../docs/08-threat-model.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum SealReason {
    /// The `underlying_sequence` reached `u64::MAX`; it cannot advance without
    /// wrapping, which would corrupt gap detection and replay.
    SequenceExhausted,
    /// A post-mutation event append failed; the actor must not build the next
    /// command on unjournaled state.
    JournalUnavailable,
}

impl SealReason {
    /// The boundary error a sealed underlying rejects further commands with.
    #[must_use]
    #[inline]
    fn as_error(self) -> VenueError {
        match self {
            Self::SequenceExhausted => VenueError::SequenceExhausted,
            Self::JournalUnavailable => VenueError::JournalUnavailable,
        }
    }
}

/// The resolution of the write-ahead command append (step 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteAhead {
    /// The command committed at `N` (or was already durably present) — proceed.
    Committed,
    /// The command did not commit — reuse `N`, execute nothing.
    Reuse,
}

// ============================================================================
// Actor configuration
// ============================================================================

/// The construction parameters for one underlying's actor.
#[derive(Debug, Clone)]
pub struct ActorConfig {
    /// The underlying ticker this actor serves (e.g. `"BTC"`).
    pub underlying: Arc<str>,
    /// The run lineage that namespaces every id this actor mints.
    pub lineage_id: LineageId,
    /// The bounded mailbox capacity — a **DoS security control**, never
    /// unbounded ([08 §5](../../../docs/08-threat-model.md)). Clamped to at least
    /// `1` at spawn.
    pub mailbox_capacity: usize,
    /// The first sequence to assign. `SequenceNumber::START` for a fresh venue;
    /// recovery (#017) continues from the last journaled sequence, and tests seed
    /// it near `u64::MAX` to exercise exhaustion without `2^64` iterations.
    pub start_sequence: SequenceNumber,
}

impl ActorConfig {
    /// Builds a config for a fresh underlying starting at
    /// [`SequenceNumber::START`].
    #[must_use]
    #[inline]
    pub fn new(
        underlying: impl Into<Arc<str>>,
        lineage_id: LineageId,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            underlying: underlying.into(),
            lineage_id,
            mailbox_capacity,
            start_sequence: SequenceNumber::START,
        }
    }
}

// ============================================================================
// The actor
// ============================================================================

/// One underlying's single-writer actor. Owns that underlying's journal, its
/// venue-owned checked sequence counter, and its collaborator seams; it is the
/// **sole** writer to its stream ([02 §8](../../../docs/02-matching-architecture.md)).
///
/// Two underlyings run as two independent actors — independent failure and
/// ordering domains that sequence in parallel. Drive it either through the
/// spawned mailbox ([`spawn_underlying_actor`]) or, for deterministic in-process
/// replay/tests, directly via [`handle`](Self::handle).
pub struct UnderlyingActor<J, E, F, C> {
    underlying: Arc<str>,
    lineage_id: LineageId,
    journal: J,
    executor: E,
    fan_out: F,
    clock: C,
    /// The venue-owned checked counter: the next `underlying_sequence` to assign
    /// (the upstream `OptionChainSequencer::assign()` is `pub(crate)`, so the
    /// venue owns this).
    next_sequence: SequenceNumber,
    /// `Some` once sealed — every further command is rejected.
    sealed: Option<SealReason>,
}

impl<J, E, F, C> UnderlyingActor<J, E, F, C>
where
    J: VenueJournal,
    E: CommandExecutor,
    F: FanOut,
    C: VenueClock,
{
    /// Builds an actor from its config and collaborator seams.
    #[must_use]
    pub fn new(config: ActorConfig, journal: J, executor: E, fan_out: F, clock: C) -> Self {
        Self {
            underlying: config.underlying,
            lineage_id: config.lineage_id,
            journal,
            executor,
            fan_out,
            clock,
            next_sequence: config.start_sequence,
            sealed: None,
        }
    }

    /// The underlying ticker this actor serves.
    #[must_use]
    #[inline]
    pub fn underlying(&self) -> &str {
        &self.underlying
    }

    /// A read-only handle onto this actor's journal (for assertions / recovery).
    #[must_use]
    #[inline]
    pub fn journal(&self) -> &J {
        &self.journal
    }

    /// Runs one full sequenced turn for `command`, synchronously and in order.
    ///
    /// This is the write-ahead protocol of [ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md):
    /// append the command envelope before executing, execute + capture, append
    /// the paired event, then fan out — advancing the checked sequence counter
    /// only on a committed command.
    ///
    /// # Errors
    ///
    /// - [`VenueError::SequenceExhausted`] / [`VenueError::JournalUnavailable`]
    ///   if the underlying is already sealed;
    /// - [`VenueError::JournalUnavailable`] if the write-ahead command append is
    ///   confirmed to have failed (`N` is reused, book untouched) or if the
    ///   post-mutation event append fails (the underlying is **sealed**).
    pub fn handle(&mut self, command: VenueCommand) -> Result<Receipt, VenueError> {
        if let Some(reason) = self.sealed {
            return Err(reason.as_error());
        }

        let sequence = self.next_sequence;
        let venue_ts = self.clock.now_ms();

        // Step 1: write-ahead command append, BEFORE any execution.
        let command_record = JournalRecord::command(sequence, venue_ts, command.clone());
        match self.write_ahead_command(sequence, command_record) {
            WriteAhead::Committed => {}
            WriteAhead::Reuse => {
                // Confirmed pre-execution failure (or ambiguous-not-committed):
                // nothing executed, the book is untouched, `N` is reused, and the
                // underlying is NOT sealed. No cursor gap, no tombstone.
                tracing::warn!(
                    underlying = %self.underlying,
                    sequence = sequence.get(),
                    "pre-execution journal append did not commit; reusing sequence, book untouched"
                );
                return Err(VenueError::JournalUnavailable);
            }
        }

        // Steps 3–4 (#007 fills the execute seam): capture the lossless outcome.
        let outcome = self.executor.execute(ExecutionContext {
            underlying: &self.underlying,
            lineage_id: &self.lineage_id,
            sequence,
            venue_ts,
            command: &command,
        });

        // Step 4: append the paired event.
        let event = VenueEvent::new(sequence, venue_ts, command, outcome);
        if let Err(error) = self.journal.append(JournalRecord::event(event.clone())) {
            // Post-mutation append failure: the book may be mutated but the event
            // is uncommitted. Seal (fail-stop) and emit NO fan-out for N; recovery
            // re-executes the journaled command to re-derive the event.
            self.sealed = Some(SealReason::JournalUnavailable);
            tracing::error!(
                underlying = %self.underlying,
                sequence = sequence.get(),
                error = %error,
                "post-mutation event append failed; sealing underlying, no fan-out"
            );
            return Err(VenueError::JournalUnavailable);
        }

        // Step 5: fan-out ONLY after the paired event is journaled.
        self.fan_out.emit(&event);

        // Advance the venue-owned counter per COMMITTED command — checked, never
        // wrapping. Exhaustion at u64::MAX seals the underlying.
        match sequence.checked_next() {
            Some(next) => self.next_sequence = next,
            None => {
                self.sealed = Some(SealReason::SequenceExhausted);
                tracing::error!(
                    underlying = %self.underlying,
                    "underlying_sequence exhausted at u64::MAX; sealing underlying"
                );
            }
        }

        Ok(Receipt {
            underlying_sequence: sequence,
            venue_ts,
        })
    }

    /// Resolves the write-ahead command append (step 1): a confirmed commit
    /// proceeds, a confirmed failure reuses `N`, and an ambiguous result is
    /// resolved by an idempotent durable tail read-back.
    fn write_ahead_command(
        &mut self,
        sequence: SequenceNumber,
        record: JournalRecord,
    ) -> WriteAhead {
        match self.journal.append(record) {
            Ok(()) => WriteAhead::Committed,
            Err(JournalError::Ambiguous(_)) => {
                // Unknown outcome: read back the durable tail. If N's command
                // committed, proceed (the re-append was idempotent); else reuse N.
                if self.journal.contains(sequence, RecordKind::Command) {
                    WriteAhead::Committed
                } else {
                    WriteAhead::Reuse
                }
            }
            Err(JournalError::AppendFailed(_))
            | Err(JournalError::Conflict { .. })
            | Err(JournalError::Corruption { .. }) => WriteAhead::Reuse,
        }
    }
}

// ============================================================================
// Mailbox, handle, and spawn
// ============================================================================

/// A read-only view of an actor's journal, returned by [`ActorHandle::snapshot`]
/// so callers can inspect the durable stream without breaking single-writer
/// ownership (the actor produces it inside its own turn).
#[derive(Debug, Clone)]
pub struct JournalSnapshot {
    /// The highest sequence present, or `None` when empty.
    pub last_sequence: Option<SequenceNumber>,
    /// Every record, in append order.
    pub records: Vec<JournalRecord>,
}

/// One message on the actor's bounded mailbox.
enum ActorMessage {
    /// Run one sequenced command and reply with its receipt.
    Command {
        command: VenueCommand,
        reply: oneshot::Sender<Result<Receipt, VenueError>>,
    },
    /// Reply with a read-only snapshot of the journal.
    Snapshot {
        reply: oneshot::Sender<JournalSnapshot>,
    },
}

/// A cloneable handle onto one underlying's actor — the **only** way a gateway
/// reaches a book or the sequencer ([02 §8](../../../docs/02-matching-architecture.md)).
///
/// Dropping every handle closes the mailbox, which is the actor's **shutdown
/// path**: its receive loop ends and the spawned task completes.
#[derive(Debug, Clone)]
pub struct ActorHandle {
    tx: mpsc::Sender<ActorMessage>,
    underlying: Arc<str>,
}

impl ActorHandle {
    /// The underlying ticker this handle addresses.
    #[must_use]
    #[inline]
    pub fn underlying(&self) -> &str {
        &self.underlying
    }

    /// Submits a command to the actor's bounded mailbox and awaits its receipt.
    ///
    /// **Bounded backpressure.** The mailbox is bounded; a submit that cannot be
    /// enqueued because the mailbox is full returns [`VenueError::RateLimited`]
    /// immediately (fail-fast busy) rather than growing an unbounded queue — the
    /// DoS-safe posture of [08 §5](../../../docs/08-threat-model.md). If the actor
    /// has stopped (mailbox closed, or it dropped the reply), the venue is
    /// unavailable and the submit returns [`VenueError::JournalUnavailable`].
    ///
    /// # Errors
    ///
    /// Returns the actor's typed [`VenueError`] rejection, or
    /// [`VenueError::RateLimited`] / [`VenueError::JournalUnavailable`] per above.
    pub async fn submit(&self, command: VenueCommand) -> Result<Receipt, VenueError> {
        let (reply, reply_rx) = oneshot::channel();
        match self.tx.try_send(ActorMessage::Command { command, reply }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => return Err(VenueError::RateLimited),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(VenueError::JournalUnavailable);
            }
        }
        match reply_rx.await {
            Ok(result) => result,
            // The actor dropped the reply (stopped mid-turn): treat as unavailable.
            Err(_) => Err(VenueError::JournalUnavailable),
        }
    }

    /// Requests a read-only snapshot of the actor's journal.
    ///
    /// Uses the same fail-fast bounded-mailbox posture as [`Self::submit`]: a
    /// full mailbox returns [`VenueError::RateLimited`] immediately rather than
    /// queueing behind order-entry pressure, so an admin snapshot can never
    /// grow an unbounded backlog ([08 §5](../../../docs/08-threat-model.md)).
    ///
    /// # Errors
    ///
    /// Returns [`VenueError::RateLimited`] if the mailbox is full, or
    /// [`VenueError::JournalUnavailable`] if the actor has stopped.
    pub async fn snapshot(&self) -> Result<JournalSnapshot, VenueError> {
        let (reply, reply_rx) = oneshot::channel();
        match self.tx.try_send(ActorMessage::Snapshot { reply }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => return Err(VenueError::RateLimited),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(VenueError::JournalUnavailable);
            }
        }
        reply_rx.await.map_err(|_| VenueError::JournalUnavailable)
    }
}

impl<J, E, F, C> UnderlyingActor<J, E, F, C>
where
    J: VenueJournal + Send + 'static,
    E: CommandExecutor + Send + 'static,
    F: FanOut + Send + 'static,
    C: VenueClock + Send + 'static,
{
    /// The mailbox receive loop. Ends — cleanly shutting the actor down — when
    /// every [`ActorHandle`] is dropped and the mailbox closes.
    async fn run(mut self, mut rx: mpsc::Receiver<ActorMessage>) {
        while let Some(message) = rx.recv().await {
            match message {
                ActorMessage::Command { command, reply } => {
                    // `handle` is synchronous — no lock is held across an `.await`.
                    let result = self.handle(command);
                    let _ = reply.send(result);
                }
                ActorMessage::Snapshot { reply } => {
                    let snapshot = JournalSnapshot {
                        last_sequence: self.journal.last_sequence(),
                        records: self
                            .journal
                            .read_from(SequenceNumber::START)
                            .unwrap_or_else(|_| Vec::new()),
                    };
                    let _ = reply.send(snapshot);
                }
            }
        }
        tracing::info!(underlying = %self.underlying, "underlying actor stopped");
    }
}

/// Spawns one underlying's actor as a `tokio` task and returns its bounded
/// [`ActorHandle`] plus the task's [`JoinHandle`] (for graceful shutdown).
///
/// The mailbox capacity is [`ActorConfig::mailbox_capacity`], clamped to at least
/// `1`. Dropping the returned handle closes the mailbox and the task completes.
#[must_use]
pub fn spawn_underlying_actor<J, E, F, C>(
    config: ActorConfig,
    journal: J,
    executor: E,
    fan_out: F,
    clock: C,
) -> (ActorHandle, JoinHandle<()>)
where
    J: VenueJournal + Send + 'static,
    E: CommandExecutor + Send + 'static,
    F: FanOut + Send + 'static,
    C: VenueClock + Send + 'static,
{
    let capacity = config.mailbox_capacity.max(1);
    let underlying = Arc::clone(&config.underlying);
    let (tx, rx) = mpsc::channel(capacity);
    let actor = UnderlyingActor::new(config, journal, executor, fan_out, clock);
    let join = tokio::spawn(actor.run(rx));
    (ActorHandle { tx, underlying }, join)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::identity::JournalHeader;
    use crate::exchange::journal::InMemoryVenueJournal;
    use crate::exchange::symbol::Symbol;
    use crate::models::{AccountId, VenueOrderId};
    use std::sync::atomic::{AtomicU32, Ordering};

    // ---- fixtures --------------------------------------------------------

    fn sym(raw: &str) -> Symbol {
        match Symbol::parse(raw) {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
        }
    }

    fn cancel(tag: &str) -> VenueCommand {
        VenueCommand::CancelOrder {
            symbol: sym("BTC-20240329-50000-C"),
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

    fn config_from(start_sequence: SequenceNumber) -> ActorConfig {
        ActorConfig {
            underlying: Arc::from("BTC"),
            lineage_id: LineageId::new("run-1"),
            mailbox_capacity: 16,
            start_sequence,
        }
    }

    const CLOCK: FixedClock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));

    /// A [`CommandExecutor`] that counts how many times it executed — proves
    /// whether a turn reached step 3. Uses an atomic counter so it is `Send`
    /// without any `unsafe`.
    struct CountingExecutor {
        calls: Arc<AtomicU32>,
    }

    impl CountingExecutor {
        fn new() -> (Self, Arc<AtomicU32>) {
            let calls = Arc::new(AtomicU32::new(0));
            (
                Self {
                    calls: Arc::clone(&calls),
                },
                calls,
            )
        }
    }

    impl CommandExecutor for CountingExecutor {
        fn execute(&mut self, _context: ExecutionContext<'_>) -> VenueOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            VenueOutcome::Cancelled {
                order_id: VenueOrderId::new("executed"),
            }
        }
    }

    /// A [`FanOut`] that counts emitted events. Atomic, so `Send` without
    /// `unsafe`.
    struct CountingFanOut {
        emits: Arc<AtomicU32>,
    }

    impl CountingFanOut {
        fn new() -> (Self, Arc<AtomicU32>) {
            let emits = Arc::new(AtomicU32::new(0));
            (
                Self {
                    emits: Arc::clone(&emits),
                },
                emits,
            )
        }
    }

    impl FanOut for CountingFanOut {
        fn emit(&mut self, _event: &VenueEvent) {
            self.emits.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A fault-injecting [`VenueJournal`] wrapping the in-memory store: it can
    /// fail or ambiguate the append at a chosen `(sequence, kind)`.
    struct FaultJournal {
        inner: InMemoryVenueJournal,
        fail_at: Option<(SequenceNumber, RecordKind)>,
        mode: FaultMode,
    }

    #[derive(Clone, Copy)]
    enum FaultMode {
        /// Confirmed failure — the record does NOT land.
        Confirmed,
        /// Ambiguous, but the record DID land (tail read-back → committed).
        AmbiguousCommitted,
        /// Ambiguous, and the record did NOT land (tail read-back → not committed).
        AmbiguousNotCommitted,
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
                // Fire once, then clear so a retry / reuse can succeed.
                self.fail_at = None;
                return match self.mode {
                    FaultMode::Confirmed => Err(JournalError::AppendFailed("injected".to_string())),
                    FaultMode::AmbiguousCommitted => {
                        // The write actually lands despite the ambiguous signal.
                        self.inner.append(record)?;
                        Err(JournalError::Ambiguous("injected".to_string()))
                    }
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

    // ---- happy path: pairing + fan-out ordering --------------------------

    #[test]
    fn test_handle_journals_command_then_event_then_fans_out() {
        let (executor, exec_calls) = CountingExecutor::new();
        let (fan_out, emits) = CountingFanOut::new();
        let mut actor = UnderlyingActor::new(config(16), journal(), executor, fan_out, CLOCK);

        let receipt = match actor.handle(cancel("a")) {
            Ok(r) => r,
            Err(e) => panic!("handle failed: {e}"),
        };
        assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0));
        assert_eq!(exec_calls.load(Ordering::SeqCst), 1);
        assert_eq!(emits.load(Ordering::SeqCst), 1);
        // Both the command and the paired event are journaled at N=0.
        assert!(
            actor
                .journal()
                .contains(SequenceNumber::new(0), RecordKind::Command)
        );
        assert!(
            actor
                .journal()
                .contains(SequenceNumber::new(0), RecordKind::Event)
        );
    }

    // ---- checked-add monotonicity ----------------------------------------

    #[test]
    fn test_handle_assigns_monotonic_sequences_per_committed_command() {
        let mut actor = UnderlyingActor::new(
            config(16),
            journal(),
            PlaceholderExecutor,
            NoopFanOut,
            CLOCK,
        );
        for expected in 0..5 {
            let receipt = match actor.handle(cancel(&expected.to_string())) {
                Ok(r) => r,
                Err(e) => panic!("handle failed: {e}"),
            };
            assert_eq!(receipt.underlying_sequence, SequenceNumber::new(expected));
        }
        assert_eq!(
            actor.journal().last_sequence(),
            Some(SequenceNumber::new(4))
        );
    }

    // ---- exhaustion at u64::MAX ------------------------------------------

    #[test]
    fn test_handle_seals_on_sequence_exhaustion_at_u64_max() {
        // Seed the counter at u64::MAX so exhaustion is reached in one turn.
        let mut actor = UnderlyingActor::new(
            config_from(SequenceNumber::new(u64::MAX)),
            journal(),
            PlaceholderExecutor,
            NoopFanOut,
            CLOCK,
        );
        // The command AT u64::MAX commits successfully...
        let receipt = match actor.handle(cancel("max")) {
            Ok(r) => r,
            Err(e) => panic!("handle failed: {e}"),
        };
        assert_eq!(receipt.underlying_sequence, SequenceNumber::new(u64::MAX));
        // ...but the counter cannot advance, so the underlying is now sealed.
        match actor.handle(cancel("next")) {
            Err(VenueError::SequenceExhausted) => {}
            other => panic!("expected SequenceExhausted, got {other:?}"),
        }
    }

    // ---- pre-execution append failure: reuse N, book untouched -----------

    #[test]
    fn test_pre_execution_append_failure_reuses_sequence_without_executing() {
        let (executor, exec_calls) = CountingExecutor::new();
        let (fan_out, emits) = CountingFanOut::new();
        let fault = FaultJournal::new(
            (SequenceNumber::new(0), RecordKind::Command),
            FaultMode::Confirmed,
        );
        let mut actor = UnderlyingActor::new(config(16), fault, executor, fan_out, CLOCK);

        // The write-ahead command append fails: nothing executes, book untouched.
        match actor.handle(cancel("a")) {
            Err(VenueError::JournalUnavailable) => {}
            other => panic!("expected JournalUnavailable, got {other:?}"),
        }
        assert_eq!(
            exec_calls.load(Ordering::SeqCst),
            0,
            "executor must not run on a pre-exec append failure"
        );
        assert_eq!(
            emits.load(Ordering::SeqCst),
            0,
            "no fan-out on a pre-exec append failure"
        );
        assert_eq!(actor.journal().last_sequence(), None, "nothing journaled");

        // The fault cleared; the next command REUSES N=0 (no gap, not sealed).
        let receipt = match actor.handle(cancel("b")) {
            Ok(r) => r,
            Err(e) => panic!("retry failed: {e}"),
        };
        assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0));
        assert_eq!(exec_calls.load(Ordering::SeqCst), 1);
    }

    // ---- ambiguous command append: tail read-back idempotency ------------

    #[test]
    fn test_ambiguous_committed_command_proceeds_via_tail_read_back() {
        let (executor, exec_calls) = CountingExecutor::new();
        let fault = FaultJournal::new(
            (SequenceNumber::new(0), RecordKind::Command),
            FaultMode::AmbiguousCommitted,
        );
        let mut actor = UnderlyingActor::new(config(16), fault, executor, NoopFanOut, CLOCK);

        // The append signalled ambiguous but DID land: tail read-back resolves to
        // committed, so the turn proceeds — idempotently (no double append).
        let receipt = match actor.handle(cancel("a")) {
            Ok(r) => r,
            Err(e) => panic!("handle failed: {e}"),
        };
        assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0));
        assert_eq!(exec_calls.load(Ordering::SeqCst), 1);
        assert!(
            actor
                .journal()
                .contains(SequenceNumber::new(0), RecordKind::Command)
        );
    }

    #[test]
    fn test_ambiguous_not_committed_command_reuses_sequence() {
        let (executor, exec_calls) = CountingExecutor::new();
        let fault = FaultJournal::new(
            (SequenceNumber::new(0), RecordKind::Command),
            FaultMode::AmbiguousNotCommitted,
        );
        let mut actor = UnderlyingActor::new(config(16), fault, executor, NoopFanOut, CLOCK);

        // Ambiguous AND not committed: tail read-back resolves to not-committed,
        // so N is reused and nothing executes.
        match actor.handle(cancel("a")) {
            Err(VenueError::JournalUnavailable) => {}
            other => panic!("expected JournalUnavailable, got {other:?}"),
        }
        assert_eq!(exec_calls.load(Ordering::SeqCst), 0);
        // The fault cleared; the reused N=0 now commits.
        let receipt = match actor.handle(cancel("b")) {
            Ok(r) => r,
            Err(e) => panic!("retry failed: {e}"),
        };
        assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0));
    }

    // ---- post-mutation append failure: seal, no fan-out ------------------

    #[test]
    fn test_post_mutation_append_failure_seals_and_suppresses_fan_out() {
        let (executor, exec_calls) = CountingExecutor::new();
        let (fan_out, emits) = CountingFanOut::new();
        let fault = FaultJournal::new(
            (SequenceNumber::new(0), RecordKind::Event),
            FaultMode::Confirmed,
        );
        let mut actor = UnderlyingActor::new(config(16), fault, executor, fan_out, CLOCK);

        // The command appended and executed, but the paired-event append fails.
        match actor.handle(cancel("a")) {
            Err(VenueError::JournalUnavailable) => {}
            other => panic!("expected JournalUnavailable, got {other:?}"),
        }
        assert_eq!(
            exec_calls.load(Ordering::SeqCst),
            1,
            "the command executed before the event append"
        );
        assert_eq!(
            emits.load(Ordering::SeqCst),
            0,
            "no fan-out when the event append fails"
        );

        // The underlying is now SEALED: further commands are rejected without
        // executing, even though the fault has cleared.
        match actor.handle(cancel("b")) {
            Err(VenueError::JournalUnavailable) => {}
            other => panic!("expected a sealed JournalUnavailable, got {other:?}"),
        }
        assert_eq!(
            exec_calls.load(Ordering::SeqCst),
            1,
            "a sealed underlying never executes again"
        );
    }

    // ---- mailbox backpressure → typed busy -------------------------------

    #[tokio::test]
    async fn test_full_mailbox_returns_rate_limited() {
        // Build the channel directly and keep the receiver alive (so it is not
        // Closed) WITHOUT draining it, then fill it to capacity.
        let capacity = 2;
        let (tx, _rx) = mpsc::channel::<ActorMessage>(capacity);
        for _ in 0..capacity {
            let (reply, _reply_rx) = oneshot::channel();
            match tx.try_send(ActorMessage::Command {
                command: cancel("fill"),
                reply,
            }) {
                Ok(()) => {}
                Err(e) => panic!("prefill should fit within capacity: {e:?}"),
            }
        }
        let handle = ActorHandle {
            tx: tx.clone(),
            underlying: Arc::from("BTC"),
        };
        // The mailbox is full: submit returns busy immediately, never blocking or
        // growing the queue.
        match handle.submit(cancel("overflow")).await {
            Err(VenueError::RateLimited) => {}
            other => panic!("expected RateLimited on a full mailbox, got {other:?}"),
        }
    }

    // ---- single-writer ordering under concurrent submits -----------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_submits_serialise_into_a_gapless_total_order() {
        let (handle, join) = spawn_underlying_actor(
            config(256),
            journal(),
            PlaceholderExecutor,
            NoopFanOut,
            CLOCK,
        );

        let submitters = 8;
        let per_task = 8;
        let mut tasks = Vec::with_capacity(submitters);
        for task in 0..submitters {
            let handle = handle.clone();
            tasks.push(tokio::spawn(async move {
                let mut sequences = Vec::with_capacity(per_task);
                for order in 0..per_task {
                    match handle.submit(cancel(&format!("{task}-{order}"))).await {
                        Ok(receipt) => sequences.push(receipt.underlying_sequence.get()),
                        Err(e) => panic!("submit failed: {e}"),
                    }
                }
                sequences
            }));
        }

        let mut all = Vec::new();
        for task in tasks {
            match task.await {
                Ok(sequences) => all.extend(sequences),
                Err(e) => panic!("submitter task panicked: {e}"),
            }
        }

        // The single writer serialises every submit into ONE total order: the
        // assigned sequences are exactly 0..(submitters*per_task) with no gap and
        // no duplicate, regardless of the racing submit order.
        all.sort_unstable();
        let expected: Vec<u64> = (0..(submitters as u64 * per_task as u64)).collect();
        assert_eq!(all, expected);

        drop(handle);
        match join.await {
            Ok(()) => {}
            Err(e) => panic!("actor task did not shut down cleanly: {e}"),
        }
    }
}
