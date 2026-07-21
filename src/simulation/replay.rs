//! The **replay driver** — reload a durable/portable journal into a **fresh**
//! [`InstrumentRegistry`](option_chain_orderbook::InstrumentRegistry) and
//! re-execute it into **identical order events, fills, and top-of-book per
//! underlying**, the persistent-path half of the bounded determinism oracle
//! ([04 §4](../../docs/04-market-data-and-replay.md#4-historical-replay),
//! [02 §5–§6](../../docs/02-matching-architecture.md),
//! [ADR-0004](../../docs/adr/0004-deterministic-replay-with-seeded-clock.md),
//! [ADR-0006](../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//!
//! ## One algorithm with recovery — re-execution
//!
//! Replay and recovery share **one** algorithm and **one** production code path:
//! [`crate::exchange::recover`]. The driver builds a [`VenueJournal`] from each
//! input stream and calls `recover`, which re-executes every journaled
//! [`VenueCommand`](crate::exchange::VenueCommand) in `underlying_sequence` order
//! through the **upstream matching unchanged** into a **fresh**
//! [`MatchingExecutor`](crate::exchange::MatchingExecutor), with the stored
//! [`VenueEvent`](crate::exchange::VenueEvent) as the **integrity oracle** (ordered
//! value equality). A corrupted event **halts** with
//! [`ReplayError::JournalCorruption`] naming the exact `(underlying, sequence)`; a
//! newer-than-binary envelope schema is **refused**
//! ([`ReplayError::SchemaRefused`]). There is no second "apply the stored event"
//! path — the driver never re-implements re-execution, it reuses the recovery core
//! verbatim.
//!
//! ## Fresh registry, journal-driven, live requote engine muted
//!
//! Every underlying replays into a **fresh** [`MatchingExecutor`] (its own
//! `UnderlyingOrderBook` mints a fresh registry), because upstream id determinism
//! holds only into a fresh registry; oracle equality is stated over the **canonical
//! symbol string + `underlying_sequence`**, never process-global registry ids
//! ([02 §5.2](../../docs/02-matching-architecture.md)). Mark prices and unrealised
//! P&L are non-journaled, recomputed **live** from the reconstructed books, and are
//! **not** asserted equal across a replay — a documented exclusion. Reproduction is
//! **journal-driven**, not seed-regenerated: a journaled `SimStep`'s derived
//! market-maker `AddOrder`s are themselves journaled, so replay re-executes those
//! journaled orders and the live requote engine is **never invoked** — the offline
//! driver is structurally mute, so no cascading duplicate requotes are generated
//! (the [`set_muted`](crate::market_maker::MarketMakerEngine::set_muted) hook is the
//! live-venue-resume equivalent). Journaled non-order inputs are applied **from the
//! command** — `EvictExpiredOrders { now_ms }`, `SetInstrumentStatus`, and the
//! `Clock` / `SimStep` carried values — never from a replay clock (the executor
//! consults no wall clock).
//!
//! ## Two input formats
//!
//! 1. The **native journal** — a set of per-underlying [`JournalStream`]s (the
//!    canonical, loss-free `VenueEvent` envelope streams), replayed by
//!    [`replay_streams`].
//! 2. A **recorded scenario bundle** ([`ScenarioBundle`]) — those streams plus the
//!    [`RunManifest`] (seed, clock mode, microstructure config, instrument seed, and
//!    the pinned crate/dependency versions), so a scenario is self-describing and
//!    portable between machines. [`replay_bundle`] verifies the bundle **schema**
//!    and the manifest's pinned **versions** against the running binary first — a
//!    mismatch is a **typed reject** ([`ReplayError::VersionMismatch`]), never a
//!    silent divergent reproduction — and a bundle without a manifest fails to
//!    decode (the `manifest` field is required). The parse path is
//!    always typed-`Err`, never a panic (the full hostile-bundle corpus is #034).
//!
//! ## Single-epoch scope (snapshot-restore boundary is a documented exclusion)
//!
//! The driver re-executes a **single journal epoch** — because it reuses
//! [`recover`] verbatim (one algorithm), which walks one epoch's command stream.
//! A journal that crosses a real snapshot-restore boundary
//! ([`SnapshotRestored`](crate::exchange::SnapshotRestored)) **fails stop** at the
//! first post-restore command (its stored event, computed against the *restored*
//! state, does not equal a re-execution from an empty book) — a safe
//! [`ReplayError::JournalCorruption`] halt, never a silent divergent resume. This
//! is consistent with the oracle's own exclusion: **the restore boundary is outside
//! the determinism oracle** — reproducibility holds *forward from* a new epoch
//! ([02 §5.5](../../docs/02-matching-architecture.md), [ADR-0006](../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//! Rebuilding a restored cut additionally needs the external snapshot artifact
//! (the journal carries only the marker, not the captured state), which the bundle
//! format does not carry, so multi-epoch replay is deferred as named follow-up
//! work, not silently mis-reproduced here.
//!
//! ## Per-underlying claim, reconstructed stores
//!
//! Each underlying's stream replays **independently**; the oracle is ordered
//! event-stream equality **per underlying**, and no venue-wide total order is
//! claimed (a partial control fan-out is reproduced from each underlying's own
//! journal). The **executions store** and **positions fold** are reconstructed from
//! the same replayed events through the same post-journal
//! [`StoreFanOut`](crate::exchange::StoreFanOut) the live actor uses — a
//! deterministic function of the journal.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::exchange::{
    ExecutionsStore, FanOut, InMemoryExecutionsStore, InMemoryPositionsStore, InMemoryVenueJournal,
    JournalError, JournalHeader, JournalRecord, MarkPriceBook, MatchingExecutor, Recovered,
    SequenceNumber, StoreFanOut, Symbol, TopOfBook, VenueEvent, VenueJournal, recover,
};
use crate::models::{ReplayReportResponse, UnderlyingReplaySummary};
use crate::simulation::manifest::RunManifest;

/// The versioned scenario-bundle wire-contract tag. A bump is a major SemVer
/// event; a bundle whose `schema` is not this is a typed
/// [`ReplayError::VersionMismatch`].
pub const SCENARIO_BUNDLE_SCHEMA: &str = "scenario-bundle.v1";

// ============================================================================
// Typed replay error
// ============================================================================

/// A typed replay failure — never a panic, even on hostile bundle input
/// ([04 §4](../../docs/04-market-data-and-replay.md#4-historical-replay),
/// [08 §5](../../docs/08-threat-model.md)).
///
/// [`JournalCorruption`](Self::JournalCorruption) and
/// [`SchemaRefused`](Self::SchemaRefused) are the recovery core's own halts,
/// surfaced verbatim (one algorithm); [`VersionMismatch`](Self::VersionMismatch)
/// and [`BundleDecode`](Self::BundleDecode) are the bundle-scoping / decode rejects
/// this module adds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReplayError {
    /// A re-executed event did **not** equal the stored one — the integrity oracle
    /// halted at the exact `(underlying, sequence)` (the recovery core's
    /// [`JournalError::Corruption`]).
    #[error("journal corruption at underlying {underlying} sequence {}", sequence.get())]
    JournalCorruption {
        /// The underlying whose stream diverged.
        underlying: String,
        /// The exact sequence at which re-execution disagreed with the store.
        sequence: SequenceNumber,
    },
    /// The journal's envelope schema is **newer** than the running binary
    /// understands (the recovery core's [`JournalError::SchemaTooNew`]) — refused
    /// rather than mis-parsed.
    #[error("journal schema {found} is newer than this binary understands")]
    SchemaRefused {
        /// The forward-incompatible schema tag found in the journal.
        found: String,
    },
    /// The bundle's `schema` or the manifest's pinned versions do not match the
    /// running binary — the oracle holds only across a matching version set, so a
    /// mismatch is refused rather than reproduced divergently.
    #[error("scenario bundle {kind} mismatch: expected {expected}, found {found}")]
    VersionMismatch {
        /// Which version field mismatched (`bundle_schema` or a manifest version
        /// field).
        kind: &'static str,
        /// The running binary's value.
        expected: String,
        /// The bundle's recorded value.
        found: String,
    },
    /// The bundle (or one of its journal streams) could not be decoded into a
    /// well-formed, internally-consistent set of records — a typed reject for a
    /// malformed / hostile bundle, never a panic.
    #[error("scenario bundle could not be decoded: {0}")]
    BundleDecode(String),
    /// A durable-store read failed while building a replay input (the recovery
    /// core's [`JournalError::Backend`]) — carries only a non-secret label.
    #[error("replay journal backend failed: {operation}")]
    Backend {
        /// The non-secret operation label naming the failed durable call.
        operation: &'static str,
    },
}

impl ReplayError {
    /// Folds a recovery-core [`JournalError`] into the replay error vocabulary,
    /// preserving the exact `(underlying, sequence)` of a corruption halt.
    fn from_journal(underlying: &str, error: JournalError) -> Self {
        match error {
            JournalError::Corruption {
                underlying,
                sequence,
            } => Self::JournalCorruption {
                underlying,
                sequence,
            },
            JournalError::SchemaTooNew { found } => Self::SchemaRefused { found },
            JournalError::Backend { operation } => Self::Backend { operation },
            // A conflicting / not-committed append can only arise while *building* a
            // replay input from an inconsistent bundle (a duplicate `(N, kind)` with
            // a differing payload) — a decode-level defect, never a live-path state.
            JournalError::Conflict { sequence, kind } => Self::BundleDecode(format!(
                "conflicting record at underlying {underlying} sequence {} kind {kind:?}",
                sequence.get()
            )),
            JournalError::AppendFailed(detail) | JournalError::Ambiguous(detail) => {
                Self::BundleDecode(format!("underlying {underlying}: {detail}"))
            }
        }
    }
}

// ============================================================================
// Scenario bundle (input format 2) + its per-underlying stream
// ============================================================================

/// One underlying's journal stream — the loss-free `VenueEvent` envelope stream
/// (`header` + ordered `records`) the replay driver re-executes. This is **input
/// format 1** (the native journal), and the element of a [`ScenarioBundle`].
///
/// `ToSchema` is derived so the stream appears in the served OpenAPI document; the
/// `header` and each `record` are the complex journal envelope, kept **opaque**
/// there (`value_type`) rather than deriving `ToSchema` across the whole envelope
/// tree.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JournalStream {
    /// The underlying ticker this stream reconstructs (e.g. `"BTC"`).
    pub underlying: String,
    /// The journal header carrying the run lineage + envelope schema.
    #[schema(value_type = Object)]
    pub header: JournalHeader,
    /// The ordered journal records (write-ahead commands + paired events + any
    /// epoch marker), in append order.
    #[schema(value_type = Vec<Object>)]
    pub records: Vec<JournalRecord>,
}

impl JournalStream {
    /// Builds a stream from an underlying, header, and its ordered records.
    #[must_use]
    pub fn new(
        underlying: impl Into<String>,
        header: JournalHeader,
        records: Vec<JournalRecord>,
    ) -> Self {
        Self {
            underlying: underlying.into(),
            header,
            records,
        }
    }

    /// Rebuilds an in-memory [`VenueJournal`] from this stream, enforcing the
    /// `(sequence, kind)` uniqueness key as it appends — a duplicate key with a
    /// **differing** payload is a decode-level [`ReplayError::BundleDecode`], never
    /// a panic.
    fn build_journal(&self) -> Result<InMemoryVenueJournal, ReplayError> {
        let mut journal = InMemoryVenueJournal::new(self.header.clone());
        for record in &self.records {
            journal
                .append(record.clone())
                .map_err(|error| ReplayError::from_journal(&self.underlying, error))?;
        }
        Ok(journal)
    }
}

/// A **recorded scenario bundle** — a set of per-underlying [`JournalStream`]s plus
/// the [`RunManifest`], so a scenario is self-describing and portable between
/// machines (**input format 2**,
/// [04 §4](../../docs/04-market-data-and-replay.md#4-historical-replay)).
///
/// The `manifest` field is **required** (a bundle without it fails to decode), and
/// [`deny_unknown_fields`](serde) rejects a stray top-level field, so a malformed
/// bundle is a typed decode error. The `schema` tag and the manifest's pinned
/// versions are verified against the running binary by [`replay_bundle`] before any
/// re-execution — the oracle holds only across a matching version set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioBundle {
    /// The bundle wire-contract tag — always [`SCENARIO_BUNDLE_SCHEMA`].
    pub schema: String,
    /// The run manifest (seed, clock mode, microstructure config, instrument seed,
    /// pinned versions). **Required** — a bundle without it does not decode.
    pub manifest: RunManifest,
    /// The per-underlying journal streams to re-execute.
    pub streams: Vec<JournalStream>,
}

impl ScenarioBundle {
    /// Builds a current-schema bundle from a manifest and its per-underlying
    /// streams.
    #[must_use]
    pub fn new(manifest: RunManifest, streams: Vec<JournalStream>) -> Self {
        Self {
            schema: SCENARIO_BUNDLE_SCHEMA.to_string(),
            manifest,
            streams,
        }
    }

    /// Whether the bundle's `schema` tag is the one the running binary
    /// understands.
    #[must_use]
    pub fn is_current_schema(&self) -> bool {
        self.schema == SCENARIO_BUNDLE_SCHEMA
    }

    /// Decodes a bundle from JSON, mapping any decode failure to a typed
    /// [`ReplayError::BundleDecode`] — the parse path is never a panic (a missing
    /// `manifest`, an unknown field, or malformed bytes all decode-error).
    ///
    /// # Errors
    ///
    /// [`ReplayError::BundleDecode`] if the bytes are not a well-formed bundle.
    pub fn from_json(json: &str) -> Result<Self, ReplayError> {
        serde_json::from_str(json).map_err(|error| ReplayError::BundleDecode(error.to_string()))
    }
}

// ============================================================================
// Reconstructed replay report
// ============================================================================

/// One underlying's reconstructed replay artifacts — the re-derived ordered
/// [`VenueEvent`] stream (the oracle's primary artifact, equal to the stored one),
/// the sequence it ended at, and the rebuilt **fresh-registry** book (for the
/// top-of-book witness).
pub struct UnderlyingReplay {
    /// The underlying ticker.
    pub underlying: String,
    /// The re-derived ordered `VenueEvent` stream (equal to the stored one).
    pub events: Vec<VenueEvent>,
    /// The highest `underlying_sequence` present, or `None` for an empty stream.
    pub last_sequence: Option<SequenceNumber>,
    /// The rebuilt per-underlying book (fresh registry), for state assertions.
    pub executor: MatchingExecutor,
}

impl UnderlyingReplay {
    /// The reconstructed top-of-book for `symbol` — the cheap oracle witness
    /// asserted after replay (mark prices are recomputed live and **not** part of
    /// it).
    #[must_use]
    pub fn top_of_book(&self, symbol: &Symbol) -> TopOfBook {
        self.executor.top_of_book(symbol)
    }

    /// The number of re-derived events (the ordered per-underlying stream length).
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// A wire-safe summary of this underlying's replay (event count + ending
    /// sequence).
    #[must_use]
    fn summary(&self) -> UnderlyingReplaySummary {
        UnderlyingReplaySummary {
            underlying: self.underlying.clone(),
            event_count: self.events.len() as u64,
            last_sequence: self.last_sequence.map(SequenceNumber::get),
        }
    }
}

impl std::fmt::Debug for UnderlyingReplay {
    /// The [`MatchingExecutor`] wraps the upstream hierarchy and is not `Debug`, so
    /// it is summarised rather than dumped.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnderlyingReplay")
            .field("underlying", &self.underlying)
            .field("events", &self.events.len())
            .field("last_sequence", &self.last_sequence)
            .finish_non_exhaustive()
    }
}

/// The reconstructed artifacts a successful replay produces: the per-underlying
/// re-derived streams + books, and the **executions store** and **positions fold**
/// rebuilt from the same events (a deterministic function of the journal).
///
/// The stores are shared `Arc`s across underlyings (as in the live venue), so a
/// two-account cross-underlying position folds identically to the recorded run.
pub struct ReplayReport {
    /// The per-underlying reconstructed streams + books, in deterministic
    /// underlying order.
    pub per_underlying: Vec<UnderlyingReplay>,
    /// The reconstructed authoritative executions log (all underlyings).
    pub executions: Arc<InMemoryExecutionsStore>,
    /// The reconstructed per-`(account, symbol)` positions fold (all underlyings).
    pub positions: Arc<InMemoryPositionsStore>,
    /// The live-only mark-price book, recomputed from the reconstructed trade
    /// prints (never journaled, never asserted equal across replays).
    pub marks: Arc<MarkPriceBook>,
}

impl ReplayReport {
    /// The reconstructed replay for `underlying`, if present.
    #[must_use]
    pub fn underlying(&self, underlying: &str) -> Option<&UnderlyingReplay> {
        self.per_underlying
            .iter()
            .find(|replay| replay.underlying == underlying)
    }

    /// The total re-derived event count across all underlyings.
    #[must_use]
    pub fn total_events(&self) -> usize {
        self.per_underlying
            .iter()
            .map(UnderlyingReplay::event_count)
            .sum()
    }

    /// A wire-safe [`ReplayReportResponse`] summary (per-underlying event counts +
    /// ending sequences, plus the reconstructed executions-leg count) for the
    /// record/replay control surfaces.
    #[must_use]
    pub fn to_response(&self) -> ReplayReportResponse {
        ReplayReportResponse {
            per_underlying: self
                .per_underlying
                .iter()
                .map(UnderlyingReplay::summary)
                .collect(),
            executions: self.executions.len() as u64,
        }
    }
}

impl std::fmt::Debug for ReplayReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayReport")
            .field("per_underlying", &self.per_underlying)
            .field("executions", &self.executions.len())
            .finish_non_exhaustive()
    }
}

// ============================================================================
// The driver
// ============================================================================

/// Replays the **native journal** input format — a set of per-underlying
/// [`JournalStream`]s — reconstructing identical events, fills, and top-of-book per
/// underlying, plus the executions store and positions fold.
///
/// Each stream is replayed **independently** into a **fresh** registry via the
/// shared [`recover`] core (one algorithm with recovery); the stored event is the
/// integrity oracle. Underlyings are processed in deterministic **sorted** order so
/// the reconstructed executions log's insertion order is itself deterministic
/// across the venue (within an underlying it is the journal order; the
/// cross-underlying total order is the sorted-ticker order — never claimed as an
/// intra-underlying determinism property).
///
/// # Errors
///
/// - [`ReplayError::JournalCorruption`] if a re-derived event does not equal the
///   stored one (naming the exact `(underlying, sequence)`);
/// - [`ReplayError::SchemaRefused`] if a stream's envelope schema is newer than the
///   binary understands;
/// - [`ReplayError::BundleDecode`] if a stream's records are internally
///   inconsistent (a conflicting `(sequence, kind)` key);
/// - [`ReplayError::Backend`] on a durable-store read failure.
pub fn replay_streams(streams: &[JournalStream]) -> Result<ReplayReport, ReplayError> {
    // Deterministic per-underlying processing order (the cross-underlying total
    // order of the reconstructed executions log).
    let mut ordered: Vec<&JournalStream> = streams.iter().collect();
    ordered.sort_by(|a, b| a.underlying.cmp(&b.underlying));

    // The shared reconstructed stores — the same instances every underlying folds
    // into, exactly as the live `AppState` shares one set across its actors.
    let executions = Arc::new(InMemoryExecutionsStore::new());
    let positions = Arc::new(InMemoryPositionsStore::new());
    let marks = Arc::new(MarkPriceBook::new());

    let mut per_underlying = Vec::with_capacity(ordered.len());
    for stream in ordered {
        let journal = stream.build_journal()?;
        // THE reuse: the #029 recovery core is the single re-execution algorithm.
        // `recover` re-executes into a fresh registry with the stored event as the
        // oracle — `Ok` proves every re-derived event equalled the stored one.
        let Recovered {
            events,
            executor,
            last_sequence,
        } = recover(&journal, stream.underlying.as_str())
            .map_err(|error| ReplayError::from_journal(&stream.underlying, error))?;

        // Reconstruct the executions store + positions fold from the SAME events,
        // through the same post-journal fan-out the live actor drives.
        let mut fan_out = StoreFanOut::new(
            Arc::clone(&executions),
            Arc::clone(&positions),
            Arc::clone(&marks),
        );
        for event in &events {
            fan_out.emit(event);
        }

        per_underlying.push(UnderlyingReplay {
            underlying: stream.underlying.clone(),
            events,
            last_sequence,
            executor,
        });
    }

    Ok(ReplayReport {
        per_underlying,
        executions,
        positions,
        marks,
    })
}

/// Replays a **recorded scenario bundle** (input format 2) — verifying the bundle
/// schema and the manifest's pinned versions against the running binary **first**
/// (a mismatch is a typed reject, never a divergent reproduction), then replaying
/// its streams via [`replay_streams`].
///
/// # Errors
///
/// - [`ReplayError::VersionMismatch`] if the bundle `schema` or any pinned manifest
///   version does not match the running binary;
/// - every error [`replay_streams`] can return.
pub fn replay_bundle(bundle: &ScenarioBundle) -> Result<ReplayReport, ReplayError> {
    verify_bundle_versions(bundle)?;
    replay_streams(&bundle.streams)
}

/// Verifies a bundle's wire-contract schema and its manifest's pinned versions
/// against the running binary — the oracle-scoping gate before any re-execution.
fn verify_bundle_versions(bundle: &ScenarioBundle) -> Result<(), ReplayError> {
    if !bundle.is_current_schema() {
        return Err(ReplayError::VersionMismatch {
            kind: "bundle_schema",
            expected: SCENARIO_BUNDLE_SCHEMA.to_string(),
            found: bundle.schema.clone(),
        });
    }
    if let Some((field, expected, found)) = bundle.manifest.versions.first_mismatch() {
        return Err(ReplayError::VersionMismatch {
            kind: field,
            expected,
            found,
        });
    }
    Ok(())
}

// ============================================================================
// Recording controller (record on/off)
// ============================================================================

/// The venue-level **recording** flag the record/replay control plane flips
/// ([04 §4](../../docs/04-market-data-and-replay.md#4-historical-replay)).
///
/// The venue's write-ahead journal is **always** durable — it is the determinism
/// substrate, never toggled off — so this flag does **not** disable journaling.
/// It marks whether a **capture window** is active for scenario-bundle export (the
/// operator-facing "record on/off" the Backend never had), and both the REST and
/// WS control surfaces flip the **same** flag, giving control parity by
/// construction.
#[derive(Debug)]
pub struct RecordingController {
    recording: AtomicBool,
}

impl RecordingController {
    /// Builds a controller with the given initial recording state.
    #[must_use]
    pub fn new(initial: bool) -> Self {
        Self {
            recording: AtomicBool::new(initial),
        }
    }

    /// Sets the recording state, returning the **previous** value. Idempotent.
    pub fn set_recording(&self, on: bool) -> bool {
        self.recording.swap(on, Ordering::Release)
    }

    /// Whether a capture window is currently active.
    #[must_use]
    pub fn is_recording(&self) -> bool {
        self.recording.load(Ordering::Acquire)
    }
}

impl Default for RecordingController {
    /// A fresh venue records by default — the durable journal is always on, so the
    /// capture window is open unless an operator closes it.
    fn default() -> Self {
        Self::new(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{
        ActorConfig, Cents, EventTimestamp, FixedClock, Hash32, LineageId, NoopFanOut, STPMode,
        Side, TimeInForce, UnderlyingActor, VenueCommand, VenueOutcome,
    };
    use crate::models::{AccountId, OrderType};
    use crate::simulation::clock::ClockMode;

    const UNDERLYING: &str = "BTC";
    const CALL: &str = "BTC-20240329-50000-C";
    const CLOCK: FixedClock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));

    fn sym() -> Symbol {
        match Symbol::parse(CALL) {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
        }
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
    ) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym(),
            order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(sequence), 0),
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

    /// Drives a command stream through a real single-writer actor and returns its
    /// [`JournalStream`] — the same journal the live venue writes.
    fn record_stream(commands: &[VenueCommand], lineage: &LineageId) -> JournalStream {
        let header = JournalHeader::new(lineage.clone());
        let mut actor = UnderlyingActor::new(
            ActorConfig::new(UNDERLYING, lineage.clone(), 64),
            InMemoryVenueJournal::new(header.clone()),
            MatchingExecutor::new(UNDERLYING),
            NoopFanOut,
            CLOCK,
        );
        for command in commands {
            actor.handle(command.clone()).expect("actor turn commits");
        }
        let records = actor
            .journal()
            .read_from(SequenceNumber::START)
            .expect("read journal");
        JournalStream::new(UNDERLYING, header, records)
    }

    fn crossing_session(lineage: &LineageId) -> Vec<VenueCommand> {
        vec![
            add(lineage, 0, "maker", 0x11, Side::Sell, 50_000, 3),
            add(lineage, 1, "taker", 0x22, Side::Buy, 50_000, 2),
        ]
    }

    #[test]
    fn test_replay_streams_reconstructs_events_and_top_of_book() {
        let lineage = LineageId::new("run-1");
        let stream = record_stream(&crossing_session(&lineage), &lineage);
        let stored_events: Vec<_> = stream
            .records
            .iter()
            .filter_map(|record| match record {
                JournalRecord::Event(event) => Some(event.clone()),
                _ => None,
            })
            .collect();

        let report = match replay_streams(std::slice::from_ref(&stream)) {
            Ok(report) => report,
            Err(e) => panic!("replay must not halt on a clean stream: {e}"),
        };
        let replay = report.underlying(UNDERLYING).expect("BTC replay present");
        assert_eq!(
            replay.events, stored_events,
            "replay re-derives the identical ordered event stream"
        );
        // The taker crossed 2 of the maker's 3 → 1 rests at 50_000.
        let top = replay.top_of_book(&sym());
        assert_eq!(top.best_ask, Some(Cents::new(50_000)));
        assert_eq!(top.ask_depth, 1);
        // Reconstructed executions: one crossing match records two legs.
        assert_eq!(report.executions.len(), 2);
    }

    #[test]
    fn test_replay_bundle_requires_manifest() {
        // A bundle JSON WITHOUT a `manifest` field must fail to decode (the field
        // is required) — a typed reject, never a panic.
        let json = r#"{"schema":"scenario-bundle.v1","streams":[]}"#;
        match ScenarioBundle::from_json(json) {
            Err(ReplayError::BundleDecode(_)) => {}
            other => panic!("a manifest-less bundle must be a typed decode error, got {other:?}"),
        }
    }

    #[test]
    fn test_replay_refuses_version_mismatch() {
        let lineage = LineageId::new("run-1");
        let stream = record_stream(&crossing_session(&lineage), &lineage);
        // A bundle whose manifest pins a WRONG fauxchange version — the oracle holds
        // only across a matching version set, so replay refuses it (typed reject).
        let mut manifest = RunManifest::new(0, ClockMode::Realtime);
        manifest.versions.fauxchange = "0.0.0-mismatch".to_string();
        let bundle = ScenarioBundle::new(manifest, vec![stream]);
        match replay_bundle(&bundle) {
            Err(ReplayError::VersionMismatch {
                kind,
                expected,
                found,
            }) => {
                assert_eq!(kind, "fauxchange");
                assert_eq!(expected, env!("CARGO_PKG_VERSION"));
                assert_eq!(found, "0.0.0-mismatch");
            }
            other => panic!("expected a VersionMismatch reject, got {other:?}"),
        }
    }

    #[test]
    fn test_replay_tolerates_a_structural_epoch_marker_without_panicking() {
        use crate::exchange::SnapshotRestored;
        let lineage = LineageId::new("run-1");
        let mut stream = record_stream(
            &[add(&lineage, 0, "mm", 0x11, Side::Sell, 50_000, 3)],
            &lineage,
        );
        // A `SnapshotRestored` epoch marker is STRUCTURAL, not a re-executable
        // command — recover skips it. With no post-restore commands the surrounding
        // stream replays cleanly (never a panic); the single-epoch limitation only
        // bites when post-restore commands follow (a safe fail-stop, documented).
        let marker = SnapshotRestored::new(
            SequenceNumber::new(1),
            EventTimestamp::new(1_700_000_000_000),
            "snap-1",
            1,
            lineage,
        );
        stream.records.push(JournalRecord::epoch(marker));
        let report = replay_streams(&[stream]).expect("an epoch marker is structural, not a panic");
        let replay = report.underlying(UNDERLYING).expect("BTC replay");
        assert_eq!(replay.top_of_book(&sym()).ask_depth, 3);
        assert_eq!(replay.last_sequence, Some(SequenceNumber::new(1)));
    }

    #[test]
    fn test_replay_halts_at_a_restore_boundary_post_restore_command() {
        use crate::exchange::SnapshotRestored;
        use crate::models::VenueOrderId;

        // The single-epoch fail-stop, PROVEN (not just documented): a real
        // pre-epoch add, a `SnapshotRestored` marker, then a post-restore
        // `CancelOrder` whose stored event claims `Cancelled` for an order that only
        // exists in the (un-modeled) RESTORED state. From an empty book the driver
        // re-executes that cancel to `Rejected { order not found }`, which ≠ the
        // stored `Cancelled` → the integrity oracle halts at the exact
        // (underlying, sequence). This is a safe fail-stop, never a silent divergent
        // resume — the restore boundary is outside the determinism oracle.
        const TS: EventTimestamp = EventTimestamp::new(1_700_000_000_000);
        let lineage = LineageId::new("run-1");
        let mut stream = record_stream(
            &[add(&lineage, 0, "mm", 0x11, Side::Sell, 50_000, 3)],
            &lineage,
        );
        // The epoch marker opens at the continued sequence 1.
        stream
            .records
            .push(JournalRecord::epoch(SnapshotRestored::new(
                SequenceNumber::new(1),
                TS,
                "snap-1",
                1,
                lineage,
            )));
        // A post-restore cancel of an order that only exists in restored state.
        let restored_only = VenueOrderId::new("restored-only");
        let cancel = VenueCommand::CancelOrder {
            symbol: sym(),
            order_id: restored_only.clone(),
            account: AccountId::new("acct"),
        };
        stream.records.push(JournalRecord::command(
            SequenceNumber::new(2),
            TS,
            cancel.clone(),
        ));
        // Its stored event claims `Cancelled` — true against restored state, false
        // against a from-empty re-execution.
        stream.records.push(JournalRecord::event(VenueEvent::new(
            SequenceNumber::new(2),
            TS,
            cancel,
            VenueOutcome::Cancelled {
                order_id: restored_only,
            },
        )));

        match replay_streams(&[stream]) {
            Err(ReplayError::JournalCorruption {
                underlying,
                sequence,
            }) => {
                assert_eq!(underlying, UNDERLYING);
                assert_eq!(
                    sequence,
                    SequenceNumber::new(2),
                    "the halt names the first post-restore command"
                );
            }
            other => panic!("expected a fail-stop JournalCorruption at (BTC, 2), got {other:?}"),
        }
    }

    #[test]
    fn test_replay_refuses_newer_bundle_schema() {
        let manifest = RunManifest::new(0, ClockMode::Realtime);
        let mut bundle = ScenarioBundle::new(manifest, vec![]);
        bundle.schema = "scenario-bundle.v2".to_string();
        match replay_bundle(&bundle) {
            Err(ReplayError::VersionMismatch { kind, found, .. }) => {
                assert_eq!(kind, "bundle_schema");
                assert_eq!(found, "scenario-bundle.v2");
            }
            other => panic!("expected a bundle_schema mismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_replay_bundle_roundtrips_and_reconstructs() {
        let lineage = LineageId::new("run-1");
        let stream = record_stream(&crossing_session(&lineage), &lineage);
        let manifest = RunManifest::new(0, ClockMode::Realtime);
        let bundle = ScenarioBundle::new(manifest, vec![stream]);

        // The bundle is portable: serialise → decode → replay identically.
        let json = serde_json::to_string(&bundle).expect("serialize bundle");
        let decoded = ScenarioBundle::from_json(&json).expect("decode bundle");
        let report = replay_bundle(&decoded).expect("replay a current-version bundle");
        assert_eq!(report.total_events(), 2);
        assert_eq!(report.executions.len(), 2);
    }

    #[test]
    fn test_replay_halts_on_corrupted_event() {
        let lineage = LineageId::new("run-1");
        let mut stream = record_stream(&crossing_session(&lineage), &lineage);
        // Corrupt the stored event at sequence 1 (the crossing fill) with a
        // divergent outcome — replay's integrity oracle must halt at (BTC, 1).
        for record in &mut stream.records {
            if let JournalRecord::Event(event) = record
                && event.underlying_sequence == SequenceNumber::new(1)
            {
                *event = VenueEvent::new(
                    event.underlying_sequence,
                    event.venue_ts,
                    event.command.clone(),
                    VenueOutcome::Rejected {
                        reason: "corrupted-by-test".to_string(),
                    },
                );
            }
        }
        match replay_streams(&[stream]) {
            Err(ReplayError::JournalCorruption {
                underlying,
                sequence,
            }) => {
                assert_eq!(underlying, UNDERLYING);
                assert_eq!(sequence, SequenceNumber::new(1));
            }
            other => panic!("expected a JournalCorruption halt at (BTC, 1), got {other:?}"),
        }
    }

    #[test]
    fn test_replay_refuses_newer_journal_schema() {
        let lineage = LineageId::new("run-1");
        let mut stream = record_stream(&crossing_session(&lineage), &lineage);
        stream.header = JournalHeader {
            schema_version: "venue.v2".to_string(),
            lineage_id: lineage,
        };
        match replay_streams(&[stream]) {
            Err(ReplayError::SchemaRefused { found }) => assert_eq!(found, "venue.v2"),
            other => panic!("expected a SchemaRefused reject, got {other:?}"),
        }
    }

    #[test]
    fn test_recording_controller_toggles() {
        let controller = RecordingController::default();
        assert!(controller.is_recording(), "records by default");
        assert!(
            controller.set_recording(false),
            "swap returns the previous value"
        );
        assert!(!controller.is_recording());
        assert!(!controller.set_recording(true));
        assert!(controller.is_recording());
    }

    #[test]
    fn test_bundle_decode_rejects_unknown_top_level_field() {
        let json = r#"{"schema":"scenario-bundle.v1","manifest":{"seed":0,"clock_mode":"realtime"},"streams":[],"typo":true}"#;
        match ScenarioBundle::from_json(json) {
            Err(ReplayError::BundleDecode(_)) => {}
            other => panic!("an unknown top-level field must be a decode error, got {other:?}"),
        }
    }

    /// A journaled market-maker `AddOrder` replays as its own journaled command and
    /// the live requote engine is **never invoked** — no cascading duplicate order
    /// is generated (the offline driver is structurally mute).
    #[test]
    fn test_replay_does_not_cascade_requotes_from_journaled_mm_orders() {
        use crate::exchange::{MARKET_MAKER_OWNER, market_maker_account};

        let lineage = LineageId::new("run-1");
        // A market-maker resting order (attributed to the reserved MM account) plus a
        // SimStep price move — on a live run the SimStep would drive a requote, but
        // here the requote is ALREADY journaled as the MM AddOrder; replay must not
        // re-derive another.
        let mm_add = VenueCommand::AddOrder {
            symbol: sym(),
            order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(0), 0),
            account: market_maker_account(),
            owner: MARKET_MAKER_OWNER,
            client_order_id: None,
            side: Side::Sell,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 5,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        };
        let sim_step = VenueCommand::SimStep {
            now_ms: EventTimestamp::new(1_700_000_000_000),
            underlying: UNDERLYING.to_string(),
            price: Cents::new(50_100),
            bid: None,
            ask: None,
        };
        let stream = record_stream(&[mm_add, sim_step], &lineage);
        let report = replay_streams(&[stream]).expect("replay must not halt");
        let replay = report.underlying(UNDERLYING).expect("BTC replay");
        // Exactly two events — the MM add and the SimStep — no extra requote order.
        assert_eq!(
            replay.events.len(),
            2,
            "no cascading requote order was generated"
        );
        // The MM's resting quote is reconstructed at its journaled price/size.
        let top = replay.top_of_book(&sym());
        assert_eq!(top.best_ask, Some(Cents::new(50_000)));
        assert_eq!(top.ask_depth, 5);
    }
}
