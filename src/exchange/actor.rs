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
use crate::exchange::envelope::{RejectKind, VenueCommand, VenueEvent, VenueOutcome};
use crate::exchange::event::{EventTimestamp, SequenceNumber};
use crate::exchange::executor::MatchingExecutor;
use crate::exchange::identity::LineageId;
use crate::exchange::journal::{
    JournalError, JournalRecord, RecordKind, SnapshotRestored, VenueJournal,
};
use crate::exchange::snapshot::{SnapshotError, SnapshotMetadata, VenueSnapshot};
use crate::exchange::stores::{InMemoryExecutionsStore, InMemoryPositionsStore, StoreFanOut};

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
        VenueOutcome::rejected(
            RejectKind::Internal,
            "matching not wired yet (pending #007)",
        )
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

/// A **tee** [`FanOut`]: emits one committed event to two consumers in order.
///
/// A pure generic combinator over the [`FanOut`] seam with no store / WS / DTO
/// knowledge — it lives here beside the trait so the application layer can compose
/// any two fan-out consumers without either owning the other. [`crate::state::AppState`]
/// uses it to compose the #008 [`StoreFanOut`]
/// (`first`, so the authoritative stores update first) with the #014
/// `WsFanOut` (`second`) — the **same** post-journal event feeds both, and neither
/// consumer's work is on the actor's critical path beyond its own synchronous
/// enqueue.
#[derive(Debug, Default, Clone, Copy)]
pub struct TeeFanOut<A, B> {
    first: A,
    second: B,
}

impl<A, B> TeeFanOut<A, B> {
    /// Composes two fan-out consumers; `first` is emitted to before `second`.
    #[must_use]
    #[inline]
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

impl<A, B> FanOut for TeeFanOut<A, B>
where
    A: FanOut,
    B: FanOut,
{
    #[inline]
    fn emit(&mut self, event: &VenueEvent) {
        self.first.emit(event);
        self.second.emit(event);
    }
}

// ============================================================================
// Receipt + seal
// ============================================================================

/// The committed result of a submitted command — the caller's acknowledgement
/// that the command was accepted, journaled write-ahead, and assigned a place in
/// the underlying's total order ([ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
///
/// It carries the **observed** [`VenueOutcome`] the single-writer turn captured,
/// not merely the accepted-and-sequenced fact: a gateway renders the observed
/// reject / applied transition / fill state from [`outcome`](Self::outcome)
/// instead of the *requested* state, so a caller reading only the live response
/// can never believe a journaled `Rejected` (a halted / `Settling` / `Expired`
/// instrument, an illegal lifecycle transition) took effect (#118). The outcome
/// is the same value journaled write-ahead, so surfacing it changes nothing on
/// replay — it adds no wall-clock and no RNG.
///
/// Not `Copy` (the [`VenueOutcome`] carries owned fill / reason data); it is
/// cloned rarely — one receipt per committed command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    /// The per-underlying sequence assigned to the command.
    pub underlying_sequence: SequenceNumber,
    /// The venue-clock instant stamped on the paired event.
    pub venue_ts: EventTimestamp,
    /// The losslessly captured outcome of the command, as journaled in the paired
    /// [`VenueEvent`]. `None` only for a control artifact that executes no
    /// [`VenueCommand`] (a snapshot [`restore`](UnderlyingActor::restore), which
    /// journals an epoch marker rather than a captured command outcome).
    pub outcome: Option<VenueOutcome>,
    /// The venue-global fan-out delivery summary, present only when this receipt
    /// is the representative of a command fanned to **every** underlying's actor
    /// (a `MarketMakerControl` / `EvictExpiredOrders` / hierarchy-wide `MassCancel`);
    /// `None` for a single-underlying command. It lets a control-plane caller
    /// report how many underlyings actually applied a partial fan-out (#118).
    pub fanout: Option<FanoutSummary>,
}

impl Receipt {
    /// A committed single-underlying command receipt carrying its captured
    /// [`VenueOutcome`].
    #[must_use]
    #[inline]
    fn committed(
        underlying_sequence: SequenceNumber,
        venue_ts: EventTimestamp,
        outcome: VenueOutcome,
    ) -> Self {
        Self {
            underlying_sequence,
            venue_ts,
            outcome: Some(outcome),
            fanout: None,
        }
    }

    /// A control-artifact receipt with no captured command outcome (a snapshot
    /// restore executes no [`VenueCommand`]).
    #[must_use]
    #[inline]
    fn control(underlying_sequence: SequenceNumber, venue_ts: EventTimestamp) -> Self {
        Self {
            underlying_sequence,
            venue_ts,
            outcome: None,
            fanout: None,
        }
    }

    /// Attaches the venue-global fan-out delivery summary, returning `self` — the
    /// application-layer builder for a fanned command's representative receipt.
    #[must_use]
    #[inline]
    pub fn with_fanout(mut self, fanout: FanoutSummary) -> Self {
        self.fanout = Some(fanout);
        self
    }
}

/// How many of a venue-global command's per-underlying fan-out deliveries
/// committed — the delivery visibility a control-plane response needs so a
/// **partial** fan-out (some underlyings committed, some rejected) is reported,
/// never hidden behind an unqualified success (#118).
///
/// The venue makes **no** promise of atomic venue-wide fan-out (there is no
/// venue-wide total order), so `ok_count < total` is a real, reportable state —
/// an emergency-stop control must not claim success when the fan-out
/// under-delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanoutSummary {
    /// How many underlyings the command committed on.
    pub ok_count: usize,
    /// How many underlyings the command was fanned to.
    pub total: usize,
}

impl FanoutSummary {
    /// Whether the command committed on **every** underlying it was fanned to.
    #[must_use]
    #[inline]
    pub fn fully_applied(&self) -> bool {
        self.ok_count == self.total
    }
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
    /// The current journal epoch — a fresh venue is `0`; each snapshot restore
    /// opens the next epoch (#009, [02 §9](../../../docs/02-matching-architecture.md)).
    epoch: u64,
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
            epoch: 0,
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

        // Surface the OBSERVED outcome on the receipt (the same value journaled in
        // the paired event, moved out — `event` is no longer read) so the gateway
        // renders the reject / applied / fill state a caller can trust (#118).
        Ok(Receipt::committed(sequence, venue_ts, event.outcome))
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
            // Any confirmed-not-committed / integrity failure reuses `N` (nothing
            // executed, book untouched). `SchemaTooNew` / `Backend` / `Corruption` /
            // `ResourceLimit` / `ConfigRejected` are read/decode / recovery-path
            // errors never returned on the durable append path, but the match stays
            // exhaustive and conservative — an unexpected failure never advances.
            Err(JournalError::AppendFailed(_))
            | Err(JournalError::Conflict { .. })
            | Err(JournalError::Corruption { .. })
            | Err(JournalError::SchemaTooNew { .. })
            | Err(JournalError::Backend { .. })
            | Err(JournalError::ResourceLimit { .. })
            | Err(JournalError::ConfigRejected { .. })
            | Err(JournalError::PriceOutOfBand { .. }) => WriteAhead::Reuse,
        }
    }
}

// ============================================================================
// Snapshot capture / restore (#009) — the consistent-cut entry points
// ============================================================================

/// Snapshot **capture** and **restore** for the default order-path wiring — the
/// real [`MatchingExecutor`] over the in-memory [`StoreFanOut`]. These are the
/// entry points the admin snapshot routes (#013) build on; they run
/// **synchronously under the single writer**, so a directly-owned actor — or a
/// quiesced spawned one — drives them without racing a turn (the mailbox plumbing
/// for the spawned path lands with #013). The #030 replay driver does **not** use
/// these — it re-executes a journal/bundle **offline** into a fresh registry, not
/// via a snapshot restore; the boot-time recovery wiring that would restore an
/// epoch on restart is tracked in #85.
impl<J, C>
    UnderlyingActor<
        J,
        MatchingExecutor,
        StoreFanOut<InMemoryExecutionsStore, InMemoryPositionsStore>,
        C,
    >
where
    J: VenueJournal,
    C: VenueClock,
{
    /// The current journal epoch (a fresh venue is `0`; each restore opens the
    /// next).
    #[must_use]
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Captures a **consistent cut** of this underlying's four derived stores plus
    /// config/version metadata, keyed by `snapshot_id`
    /// ([02 §9](../../../docs/02-matching-architecture.md)).
    ///
    /// A pure read of the leaf books (current resting quantities), the executions
    /// log, the positions fold, and the idempotency map — non-journaled analytics
    /// (mark price, unrealised P&L, Greeks, registry ids) are **excluded** and
    /// recompute live after a restore.
    ///
    /// The executions/positions stores are shared venue-wide across every
    /// per-underlying actor, so the cut is scoped to **this actor's underlying**
    /// (`capture_for`): the leaf books and idempotency map are already this
    /// underlying's, and slicing the shared stores by underlying keeps the cut a
    /// consistent, single-writer read of only this underlying's data — it never
    /// captures another underlying's concurrently-written legs.
    #[must_use]
    pub fn capture(
        &self,
        snapshot_id: impl Into<String>,
        config_fingerprint: impl Into<String>,
    ) -> VenueSnapshot {
        let metadata = SnapshotMetadata::new(
            snapshot_id,
            self.clock.now_ms(),
            self.lineage_id.clone(),
            config_fingerprint,
        );
        VenueSnapshot {
            metadata,
            executor: self.executor.capture_state(),
            executions: self
                .fan_out
                .executions()
                .capture_for(self.underlying.as_ref()),
            positions: self
                .fan_out
                .positions()
                .capture_for(self.underlying.as_ref()),
        }
    }

    /// Restores a snapshot over a consistent cut, **all-or-nothing**, opening a
    /// fresh journal epoch (§9).
    ///
    /// 1. Validate the snapshot's config/version metadata against the running
    ///    venue — a mismatch is refused with **no** mutation.
    /// 2. **Prepare** the book rebuild (fallible, non-mutating) and append the
    ///    [`SnapshotRestored`] marker at the **continued** `underlying_sequence`
    ///    (fallible). Both fallible steps run **before** any store is swapped, so
    ///    a fault here rolls back all four stores.
    /// 3. **Commit** the swap of all four stores (books, executions, positions,
    ///    idempotency map) infallibly under quiescence, then continue the
    ///    `underlying_sequence` from past the marker (it does **not** reset).
    ///
    /// The marker carries the run [`LineageId`] forward so restored ids keep
    /// minting in the same namespace. Reproducibility holds *forward from* the
    /// new epoch; the restore boundary is **outside** the determinism oracle.
    ///
    /// # Errors
    ///
    /// - [`SnapshotError::MetadataMismatch`] if the snapshot does not match the
    ///   running venue;
    /// - [`SnapshotError::RebuildFailed`] if the captured book cannot be rebuilt,
    ///   or an executions/positions cut carries a leg/fold for another underlying
    ///   (both raised in the preparation phase — rolls back, nothing swapped);
    /// - [`SnapshotError::JournalUnavailable`] if the epoch marker cannot be
    ///   journaled (rolls back), or the underlying is sealed on the journal;
    /// - [`SnapshotError::SequenceExhausted`] if the underlying is sealed on
    ///   exhaustion, the epoch counter cannot advance, or the
    ///   `underlying_sequence` cannot continue past the epoch marker — the last
    ///   is refused **before** any live state is mutated (nothing swapped).
    pub fn restore(
        &mut self,
        snapshot: &VenueSnapshot,
        config_fingerprint: &str,
    ) -> Result<Receipt, SnapshotError> {
        // Step 1: metadata validation — no mutation on a mismatch.
        snapshot
            .metadata
            .validate_against(&self.lineage_id, config_fingerprint)?;

        if let Some(reason) = self.sealed {
            return Err(match reason {
                SealReason::SequenceExhausted => SnapshotError::SequenceExhausted,
                SealReason::JournalUnavailable => SnapshotError::JournalUnavailable,
            });
        }

        let sequence = self.next_sequence; // continues — never reset to 0
        let venue_ts = self.clock.now_ms();
        let new_epoch = self
            .epoch
            .checked_add(1)
            .ok_or(SnapshotError::SequenceExhausted)?;

        // Pre-mutation sequence-capacity gate (#009 all-or-nothing): the epoch
        // marker consumes `sequence`, and the fresh epoch must be able to continue
        // PAST it. Prove that capacity now — while everything is still on the
        // detached/prepared side — so a restore that would exhaust the
        // `underlying_sequence` is refused BEFORE any live state (books, stores,
        // epoch, journal) is touched, leaving the live books exactly as they were.
        // This mirrors the re-add path's all-or-nothing contract; the genuinely
        // at-the-limit LIVE command path still seals post-commit (`handle`), but a
        // RESTORE fails closed rather than sealing after replacing the books.
        let next_sequence = sequence
            .checked_next()
            .ok_or(SnapshotError::SequenceExhausted)?;

        // Step 2a: prepare the detached book image (fallible, non-mutating).
        let prepared = self.executor.prepare_restore(
            &snapshot.executor.resting_orders,
            &snapshot.executor.idempotency,
            &snapshot.executor.instrument_statuses,
        )?;

        // Step 2a (cont.): validate the shared-store cuts belong to THIS underlying
        // before any mutation or marker append. The executions/positions stores are
        // shared venue-wide and only this actor is quiesced, so a cut carrying
        // another underlying's legs/folds is a corrupt snapshot that could inject or
        // overwrite a live underlying's rows. It is refused wholesale (all-or-nothing)
        // here, so the marker is never journaled and no store is swapped.
        self.fan_out
            .executions()
            .validate_restore(self.underlying.as_ref(), &snapshot.executions)?;
        self.fan_out
            .positions()
            .validate_restore(self.underlying.as_ref(), &snapshot.positions)?;

        // Step 2b: append the epoch marker as the first record of the fresh
        // epoch (fallible). Done before any swap so a failure rolls back cleanly.
        let marker = SnapshotRestored::new(
            sequence,
            venue_ts,
            snapshot.metadata.snapshot_id.clone(),
            new_epoch,
            self.lineage_id.clone(),
        );
        if self.journal.append(JournalRecord::epoch(marker)).is_err() {
            tracing::error!(
                underlying = %self.underlying,
                "snapshot restore could not journal the epoch marker; rolling back"
            );
            return Err(SnapshotError::JournalUnavailable);
        }

        // Step 3: commit — swap this underlying's four stores infallibly under
        // quiescence, then continue the `underlying_sequence` past the marker. The
        // executions/positions stores are shared venue-wide, so the swap is scoped to
        // **this actor's underlying** (`restore_for`): it replaces only this
        // underlying's slice and leaves every other underlying's (possibly newer)
        // legs/folds untouched — a `BTC` restore never erases `ETH`. The sequence
        // capacity was proven above (`next_sequence`), so this advance cannot exhaust
        // and the commit cannot leave a half-restored, un-advanceable book.
        self.executor.commit_restore(prepared);
        self.fan_out
            .executions()
            .restore_for(self.underlying.as_ref(), snapshot.executions.clone());
        self.fan_out
            .positions()
            .restore_for(self.underlying.as_ref(), snapshot.positions.clone());
        self.epoch = new_epoch;
        self.next_sequence = next_sequence;

        tracing::info!(
            underlying = %self.underlying,
            snapshot_id = %snapshot.metadata.snapshot_id,
            epoch = new_epoch,
            underlying_sequence = sequence.get(),
            "snapshot restored; opened a fresh journal epoch"
        );
        // A restore is a control-plane epoch operation, not a sequenced command —
        // it journals a `SnapshotRestored` marker, not a captured `VenueOutcome` —
        // so its receipt carries no command outcome (#118).
        Ok(Receipt::control(sequence, venue_ts))
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
    /// Reply with a read-only snapshot of the journal, or the read error (a
    /// journal read failure must propagate, never collapse to a false-empty
    /// snapshot while `last_sequence` still reports records present).
    Snapshot {
        reply: oneshot::Sender<Result<JournalSnapshot, VenueError>>,
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
        // Flatten: the outer `Err` is a dropped reply (actor gone); the inner
        // `Err` is a propagated journal read failure.
        match reply_rx.await {
            Ok(result) => result,
            Err(_) => Err(VenueError::JournalUnavailable),
        }
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
                    // Read the records FIRST: a journal read failure must propagate
                    // as an error, never be swallowed into an empty `records` vec
                    // while `last_sequence` still reports the journal non-empty
                    // (that internally-inconsistent snapshot would silently corrupt
                    // recovery/replay). Only on a successful read is the consistent
                    // `last_sequence` paired with it.
                    let result = match self.journal.read_from(SequenceNumber::START) {
                        Ok(records) => Ok(JournalSnapshot {
                            last_sequence: self.journal.last_sequence(),
                            records,
                        }),
                        Err(error) => {
                            tracing::error!(
                                underlying = %self.underlying,
                                %error,
                                "journal read failed while building snapshot; \
                                 propagating error instead of a false-empty snapshot"
                            );
                            Err(VenueError::JournalUnavailable)
                        }
                    };
                    let _ = reply.send(result);
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
    use crate::exchange::boundary::{Hash32, STPMode, Side, TimeInForce};
    use crate::exchange::identity::JournalHeader;
    use crate::exchange::journal::InMemoryVenueJournal;
    use crate::exchange::money::Cents;
    use crate::exchange::stores::MarkPriceBook;
    use crate::exchange::symbol::Symbol;
    use crate::models::{AccountId, ClientOrderId, OrderType, VenueOrderId};
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

    /// A [`VenueJournal`] whose `read_from` always fails while `last_sequence`
    /// still reports records present — the exact #61 inconsistency the snapshot
    /// path must propagate as an error rather than swallow into a false-empty
    /// snapshot. Appends land normally (so `last_sequence` becomes non-empty).
    struct ReadFailJournal {
        inner: InMemoryVenueJournal,
    }

    impl ReadFailJournal {
        fn new() -> Self {
            Self { inner: journal() }
        }
    }

    impl VenueJournal for ReadFailJournal {
        fn header(&self) -> &JournalHeader {
            self.inner.header()
        }

        fn append(&mut self, record: JournalRecord) -> Result<(), JournalError> {
            self.inner.append(record)
        }

        fn read_from(&self, _from: SequenceNumber) -> Result<Vec<JournalRecord>, JournalError> {
            Err(JournalError::Backend {
                operation: "snapshot_read",
            })
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

    // ---- restore refuses pre-mutation on sequence exhaustion (#009) -------

    /// A default-wired actor (real [`MatchingExecutor`] over the in-memory
    /// [`StoreFanOut`]) seeded at `start_sequence`, so a restore can be driven
    /// synchronously under the single writer.
    fn real_actor(
        start_sequence: SequenceNumber,
    ) -> UnderlyingActor<
        InMemoryVenueJournal,
        MatchingExecutor,
        StoreFanOut<InMemoryExecutionsStore, InMemoryPositionsStore>,
        FixedClock,
    > {
        let fan = StoreFanOut::new(
            Arc::new(InMemoryExecutionsStore::new()),
            Arc::new(InMemoryPositionsStore::new()),
            Arc::new(MarkPriceBook::new()),
        );
        UnderlyingActor::new(
            config_from(start_sequence),
            journal(),
            MatchingExecutor::new("BTC"),
            fan,
            CLOCK,
        )
    }

    /// A resting limit add on the shared fixture leaf.
    fn add_order(tag: &str, sequence: u64, side: Side, price: u64, quantity: u64) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym("BTC-20240329-50000-C"),
            order_id: LineageId::new("run-1").venue_order_id(
                "BTC",
                SequenceNumber::new(sequence),
                0,
            ),
            account: AccountId::new(tag),
            owner: Hash32([0xAB; 32]),
            client_order_id: Some(ClientOrderId::new(format!("cloid-{tag}"))),
            side,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        }
    }

    #[test]
    fn test_restore_that_would_exhaust_sequence_refuses_before_mutating() {
        // Source venue: a healthy actor with one resting order → the snapshot cut.
        let mut source = real_actor(SequenceNumber::START);
        if let Err(e) = source.handle(add_order("m1", 0, Side::Sell, 50_100, 3)) {
            panic!("source add failed: {e}");
        }
        let snapshot = source.capture("snap-1", "fp-1");

        // Target venue: a DISTINCT actor seeded one below the ceiling, given its
        // OWN resting order in one committed turn — which advances the counter to
        // exactly u64::MAX WITHOUT sealing (the genuinely-at-the-limit live case).
        let mut target = real_actor(SequenceNumber::new(u64::MAX - 1));
        if let Err(e) = target.handle(add_order("b1", u64::MAX - 1, Side::Buy, 49_900, 2)) {
            panic!("target add failed: {e}");
        }

        // The target's cut BEFORE the attempted restore. FixedClock ⇒ two captures
        // are byte-identical iff every store is unchanged.
        let before = target.capture("probe", "fp-1");

        // A restore whose epoch marker would consume the last assignable sequence
        // and then be unable to advance MUST be refused BEFORE any live state is
        // mutated (the all-or-nothing contract) — not sealed after a swap.
        match target.restore(&snapshot, "fp-1") {
            Err(SnapshotError::SequenceExhausted) => {}
            other => panic!("expected SequenceExhausted, got {other:?}"),
        }

        // Live books untouched: the target still carries its OWN order, not the
        // source's — the pre-fix post-mutation check would have swapped them first.
        let after = target.capture("probe", "fp-1");
        assert_eq!(
            before, after,
            "a refused restore must leave top_of_book / capture_state byte-identical"
        );
        assert_eq!(
            target.epoch(),
            0,
            "a refused restore must not open a new epoch"
        );
    }

    // ---- receipt surfaces the captured outcome (#118) --------------------

    #[test]
    fn test_handle_surfaces_captured_outcome_matching_the_journaled_event() {
        // The receipt now carries the OBSERVED `VenueOutcome`, byte-identical to the
        // paired event the turn journaled — so a gateway renders the reject / fill
        // state a caller can trust, and it is exactly the replay-stable value (#118).
        let mut actor = real_actor(SequenceNumber::START);

        let maker = match actor.handle(add_order("m", 0, Side::Sell, 50_000, 2)) {
            Ok(r) => r,
            Err(e) => panic!("maker handle failed: {e}"),
        };
        assert!(
            matches!(
                maker.outcome,
                Some(VenueOutcome::Added {
                    resting_quantity: 2,
                    ..
                })
            ),
            "a resting maker surfaces Added with the resting remainder"
        );
        assert!(
            maker.fanout.is_none(),
            "a single-underlying command has no fan-out"
        );

        let taker = match actor.handle(add_order("t", 1, Side::Buy, 50_000, 2)) {
            Ok(r) => r,
            Err(e) => panic!("taker handle failed: {e}"),
        };
        match &taker.outcome {
            Some(VenueOutcome::Added { fills, .. }) => {
                assert!(!fills.is_empty(), "the crossing taker surfaces its fills")
            }
            other => panic!("crossing taker must surface Added with fills, got {other:?}"),
        }

        // The surfaced outcome equals the journaled paired event's outcome per seq —
        // the receipt is a live view of exactly what recovery/replay reconstructs.
        let journaled: Vec<VenueOutcome> = match actor.journal().read_from(SequenceNumber::START) {
            Ok(records) => records
                .into_iter()
                .filter_map(|record| match record {
                    JournalRecord::Event(event) => Some(event.outcome),
                    _ => None,
                })
                .collect(),
            Err(e) => panic!("read_from failed: {e:?}"),
        };
        assert_eq!(maker.outcome.as_ref(), journaled.first());
        assert_eq!(taker.outcome.as_ref(), journaled.get(1));
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

    // ---- write-path per-record ceiling (#034 P1: write ≤ read symmetry) --------

    /// A [`CommandExecutor`] whose captured outcome is deliberately **over the
    /// per-record byte ceiling** — a synthetic stand-in for a monster sweep (an
    /// event with thousands of fills), built cheaply from one huge `Rejected` reason
    /// so the test needs no real book depth.
    struct OversizedOutcomeExecutor {
        calls: Arc<AtomicU32>,
    }

    impl CommandExecutor for OversizedOutcomeExecutor {
        fn execute(&mut self, _context: ExecutionContext<'_>) -> VenueOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            VenueOutcome::rejected(
                RejectKind::Internal,
                "x".repeat(crate::exchange::MAX_JOURNAL_RECORD_BYTES),
            )
        }
    }

    #[test]
    fn test_actor_seals_on_oversized_post_mutation_event() {
        // A command executes to an over-ceiling EVENT: the write-ahead command (tiny)
        // commits, matching runs, but the paired-event append is REFUSED by the write
        // ceiling (a `ResourceLimit`) — so the actor takes the EXISTING post-mutation
        // seal path (loud `JournalUnavailable`), never silently writing an event that
        // would then brick every future recovery/replay of the stream.
        let calls = Arc::new(AtomicU32::new(0));
        let executor = OversizedOutcomeExecutor {
            calls: Arc::clone(&calls),
        };
        let (fan_out, emits) = CountingFanOut::new();
        let mut actor = UnderlyingActor::new(config(16), journal(), executor, fan_out, CLOCK);

        match actor.handle(cancel("a")) {
            Err(VenueError::JournalUnavailable) => {}
            other => panic!("expected a JournalUnavailable seal, got {other:?}"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "matching ran before the event append"
        );
        assert_eq!(
            emits.load(Ordering::SeqCst),
            0,
            "no fan-out for an unjournaled event"
        );
        // The underlying is SEALED: further commands are rejected without executing.
        match actor.handle(cancel("b")) {
            Err(VenueError::JournalUnavailable) => {}
            other => panic!("expected a sealed JournalUnavailable, got {other:?}"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a sealed underlying never executes again"
        );
    }

    #[test]
    fn test_actor_reuses_sequence_on_oversized_command() {
        // An over-ceiling write-ahead COMMAND (a crafted giant field — commands carry
        // no fills, so this is ~unreachable in practice) is refused AT append: nothing
        // executes, the book is untouched, `N` is REUSED, and the underlying is NOT
        // sealed. The next legitimate command commits at the reused `N`.
        let (executor, exec_calls) = CountingExecutor::new();
        let mut actor = UnderlyingActor::new(config(16), journal(), executor, NoopFanOut, CLOCK);

        let huge = VenueCommand::CancelOrder {
            symbol: sym("BTC-20240329-50000-C"),
            order_id: VenueOrderId::new("x".repeat(crate::exchange::MAX_JOURNAL_RECORD_BYTES)),
            account: AccountId::new("acct-1"),
        };
        match actor.handle(huge) {
            Err(VenueError::JournalUnavailable) => {}
            other => panic!("expected a JournalUnavailable (command reused), got {other:?}"),
        }
        assert_eq!(
            exec_calls.load(Ordering::SeqCst),
            0,
            "an over-ceiling command never executes"
        );
        assert_eq!(
            actor.journal().last_sequence(),
            None,
            "nothing was journaled"
        );

        // The reused N=0 now commits for a legitimate command (not sealed).
        let receipt = match actor.handle(cancel("ok")) {
            Ok(r) => r,
            Err(e) => panic!("the reused sequence must commit: {e}"),
        };
        assert_eq!(receipt.underlying_sequence, SequenceNumber::new(0));
        assert_eq!(exec_calls.load(Ordering::SeqCst), 1);
    }

    // ---- snapshot: journal read failure propagates (#61) -----------------

    #[tokio::test]
    async fn test_snapshot_propagates_journal_read_failure_not_false_empty() {
        // A journal whose `read_from` fails while `last_sequence` reports records
        // present. Before #61 the actor swallowed the read error into an empty
        // `records` vec (an internally-inconsistent snapshot that silently
        // corrupts recovery); it must now propagate the error instead.
        let (handle, join) = spawn_underlying_actor(
            config(16),
            ReadFailJournal::new(),
            PlaceholderExecutor,
            NoopFanOut,
            CLOCK,
        );
        // Land one record so `last_sequence` is non-empty — the exact mismatch.
        match handle.submit(cancel("seed")).await {
            Ok(_) => {}
            Err(e) => panic!("seed submit should commit: {e}"),
        }
        match handle.snapshot().await {
            Err(VenueError::JournalUnavailable) => {}
            other => panic!("read failure must propagate, got {other:?}"),
        }
        drop(handle);
        let _ = join.await;
    }

    #[tokio::test]
    async fn test_snapshot_returns_consistent_records_on_healthy_journal() {
        let (handle, join) = spawn_underlying_actor(
            config(16),
            journal(),
            PlaceholderExecutor,
            NoopFanOut,
            CLOCK,
        );
        match handle.submit(cancel("one")).await {
            Ok(_) => {}
            Err(e) => panic!("submit should commit: {e}"),
        }
        match handle.snapshot().await {
            Ok(snapshot) => {
                assert!(
                    snapshot.last_sequence.is_some(),
                    "last_sequence must report the record present"
                );
                assert!(
                    !snapshot.records.is_empty(),
                    "records must be consistent with a non-empty last_sequence"
                );
            }
            Err(e) => panic!("a healthy journal must snapshot: {e}"),
        }
        drop(handle);
        let _ = join.await;
    }
}
