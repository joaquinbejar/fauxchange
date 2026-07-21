//! `CommandExecutor` / `VenueJournal` wrapper seams that time the real
//! per-underlying actor's turn **from the inside**, so `hp1_order_path` can
//! separate the upstream match cost and the write-ahead append cost from the
//! venue-added delta as **paired**, per-turn series — not two independent runs
//! ([07 §5](../../../docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention),
//! [020](../../../milestones/v0.1-backend-core/020-bench-hdr-harness-baseline.md)).
//!
//! Both wrappers delegate to the real, unmodified implementation
//! ([`fauxchange::exchange::MatchingExecutor`] /
//! [`fauxchange::exchange::InMemoryVenueJournal`]) and time exactly one call —
//! this is the SAME call the production actor makes, not a second, independent
//! invocation, so the two histograms this seam feeds are exact per-turn pairs
//! with the driver's own outer (uninstrumented) full-turn timer.
//!
//! Bench-only instrumentation — never linked into the shipped crate; confined
//! to `benches/`, never `src/`.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use fauxchange::exchange::{
    CommandExecutor, ExecutionContext, JournalError, JournalHeader, JournalRecord, RecordKind,
    SequenceNumber, VenueJournal, VenueOutcome,
};

#[derive(Debug, Default)]
struct TurnSlot {
    match_ns: Option<u64>,
    command_append_ns: Option<u64>,
    event_append_ns: Option<u64>,
}

/// The per-turn timing handoff [`TimingExecutor`] and [`TimingJournal`] each
/// push their measured duration into; the closed-loop driver
/// [`take`](Self::take)s all three immediately after `submit(...).await`
/// returns.
///
/// This is **exact** under closed-loop pacing (never more than one command in
/// flight — see `benches/hp1_order_path.rs`'s closed-loop section): the
/// single-writer actor processes commands strictly one at a time, so exactly
/// one `execute()` and exactly two `append()` calls (command, then event) land
/// in this slot between one `take()` and the next. It is **not** valid under
/// concurrent (open-loop) dispatch, where multiple turns can be in flight at
/// once and would race this single slot — the open-loop run therefore reports
/// only the end-to-end sojourn-time series (see [`crate::support::openloop`]),
/// never this decomposition.
///
/// **Disclosed instrumentation tax.** Locking a per-turn `Mutex` here adds a
/// small, constant cost to the *inner* measurements this seam captures (an
/// uncontended `std::sync::Mutex` push per call) that is **not** present in
/// the driver's outer, uninstrumented full-turn timer — so the reported
/// match-only / append-only figures are a slight OVER-estimate, and the
/// derived "venue delta" (full − match) a correspondingly slight
/// UNDER-estimate, of their true, uninstrumented contribution. `BENCH.md`
/// repeats this disclosure next to the numbers it produces.
#[derive(Debug, Clone, Default)]
pub struct TurnTimings(Arc<Mutex<TurnSlot>>);

impl TurnTimings {
    /// Builds an empty timing slot.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes (and clears) every sample recorded since the last call:
    /// `(match_ns, command_append_ns, event_append_ns)`.
    #[must_use]
    pub fn take(&self) -> (Option<u64>, Option<u64>, Option<u64>) {
        let mut slot = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (
            slot.match_ns.take(),
            slot.command_append_ns.take(),
            slot.event_append_ns.take(),
        )
    }
}

/// Wraps a real [`CommandExecutor`] and times each `execute()` call.
pub struct TimingExecutor<E> {
    inner: E,
    slot: TurnTimings,
}

impl<E> TimingExecutor<E> {
    /// Wraps `inner`, recording every `execute()` call's duration into `slot`.
    #[must_use]
    pub fn new(inner: E, slot: TurnTimings) -> Self {
        Self { inner, slot }
    }
}

impl<E: CommandExecutor> CommandExecutor for TimingExecutor<E> {
    fn execute(&mut self, context: ExecutionContext<'_>) -> VenueOutcome {
        let t0 = Instant::now();
        let outcome = self.inner.execute(context);
        let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        if let Ok(mut slot) = self.slot.0.lock() {
            slot.match_ns = Some(ns.max(1));
        }
        outcome
    }
}

/// Wraps a real [`VenueJournal`] and times each `append()` call, split by
/// [`RecordKind`] — the write-ahead command append (step 1) vs. the paired
/// event append (step 4), so the split docs/07 §3-HP5 calls for is visible.
pub struct TimingJournal<J> {
    inner: J,
    slot: TurnTimings,
}

impl<J> TimingJournal<J> {
    /// Wraps `inner`, recording every `append()` call's duration into `slot`.
    #[must_use]
    pub fn new(inner: J, slot: TurnTimings) -> Self {
        Self { inner, slot }
    }
}

impl<J: VenueJournal> VenueJournal for TimingJournal<J> {
    fn header(&self) -> &JournalHeader {
        self.inner.header()
    }

    fn append(&mut self, record: JournalRecord) -> Result<(), JournalError> {
        let kind = record.kind();
        let t0 = Instant::now();
        let result = self.inner.append(record);
        let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        if let Ok(mut slot) = self.slot.0.lock() {
            match kind {
                RecordKind::Command => slot.command_append_ns = Some(ns.max(1)),
                RecordKind::Event => slot.event_append_ns = Some(ns.max(1)),
                // The epoch marker (snapshot restore) never appears on the
                // HP-1 order path this wrapper measures.
                RecordKind::Epoch => {}
            }
        }
        result
    }

    fn read_from(&self, from: SequenceNumber) -> Result<Vec<JournalRecord>, JournalError> {
        self.inner.read_from(from)
    }

    fn last_sequence(&self) -> Option<SequenceNumber> {
        self.inner.last_sequence()
    }
}
