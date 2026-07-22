//! Journal recovery — **recovery-as-re-execution**, the single algorithm of
//! [ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)
//! made production code ([02 §6](../../../docs/02-matching-architecture.md),
//! [04 §4](../../../docs/04-market-data-and-replay.md)).
//!
//! On restart the venue reconstructs a per-underlying book from its journal by
//! **re-executing** every journaled [`VenueCommand`] in `underlying_sequence`
//! order through the **same** upstream matching the live venue drives — never by
//! replaying the stored event as an apply source. The stored [`VenueEvent`] is the
//! **integrity oracle**: after re-executing command `N`, the re-derived event is
//! asserted **equal** to the stored one, and a mismatch **halts** with a typed
//! [`JournalError::Corruption`] naming the exact `(underlying, N)`. There is one
//! rule, not a "sometimes apply, sometimes re-execute" split.
//!
//! ## What this module owns (and what it defers)
//!
//! - It walks one **underlying's** stream into a **fresh** [`MatchingExecutor`],
//!   re-deriving the [`VenueEvent`] sequence and reconstructing the leaf books.
//! - It reads the [`JournalHeader`](crate::exchange::JournalHeader) **first** to
//!   rehydrate the run [`LineageId`](crate::exchange::LineageId) (so re-derived ids
//!   land in the same namespace) and to **refuse** a journal whose envelope schema
//!   is newer than the running binary understands
//!   ([`JournalError::SchemaTooNew`]) — the typed forward-incompatible-schema error
//!   the v0.1 slice deferred to here.
//! - The three tail conditions are handled exactly as the ADR states: a command
//!   **with** its paired event → re-execute + oracle-compare; a **tail command with
//!   no paired event** (a crash between write-ahead and event append) → re-execute
//!   to **derive** the event; **no command at `N`** (a confirmed pre-execution
//!   append failure reused `N`) → the stream has no `N` and the walk continues.
//!
//! **Multi-epoch (post-snapshot-restore) reload is out of scope here.** A
//! [`SnapshotRestored`](crate::exchange::SnapshotRestored) epoch marker captures
//! *state*, not the *sequence of decisions*, so it is a replay exclusion: rebuilding
//! the restored cut is the replay driver's job (#030). This reducer walks a single
//! epoch's command stream; a journal that crosses a real restore boundary fails
//! **fail-stop** at the first post-restore command (its stored event, computed
//! against the restored state, will not equal a re-execution from an empty book), a
//! safe halt rather than a silent divergent resume.

use std::sync::Arc;

use option_chain_orderbook::SymbolParser;

use crate::exchange::actor::{CommandExecutor, ExecutionContext};
use crate::exchange::clordid_index::{ClOrdIdIndex, apply_committed_correlation};
use crate::exchange::envelope::{AddOutcome, RejectKind, VenueCommand, VenueEvent, VenueOutcome};
use crate::exchange::event::SequenceNumber;
use crate::exchange::executor::MatchingExecutor;
use crate::exchange::identity::LineageId;
use crate::exchange::journal::{JournalCommand, JournalRecord, VenueJournal};
use crate::microstructure::{MicrostructureConfig, OrderAdmissionError, PriceBoundError};

pub use crate::exchange::journal::JournalError;

/// Checks a command's limit price against the venue-owned price band, resolving it by
/// the order's full **symbol** (#114 item 5) — the band-only admission the venue's
/// internal producers share (the market-maker requote sink and the price-simulator
/// step sink, #109), so a band-violating quote or price step is dropped before it is
/// sequenced.
///
/// Only `AddOrder` / `Replace` carrying a `limit_price` are checked; a market order
/// (no limit price) and every non-order command carry no price to admit. A symbol
/// that does not parse is skipped here (the executor rejects it and the integrity
/// oracle catches it), mirroring the submit seam where the router rejects it.
///
/// The band is resolved through
/// [`admit_price_for_symbol`](MicrostructureConfig::admit_price_for_symbol) in the
/// **symbol-specific → underlying → venue-default** fallback order, so a per-symbol
/// price band gates the producers too. A `SimStep` reference price has no full symbol,
/// so it is band-checked directly on its underlying ticker.
///
/// # Errors
///
/// A [`PriceBoundError`] if the order price falls outside the resolved per-symbol
/// `[min_price_cents, max_price_cents]` band.
pub(crate) fn check_price_band(
    config: &MicrostructureConfig,
    command: &VenueCommand,
) -> Result<(), PriceBoundError> {
    let (symbol, price) = match command {
        VenueCommand::AddOrder {
            symbol,
            limit_price: Some(price),
            ..
        }
        | VenueCommand::Replace {
            symbol,
            limit_price: Some(price),
            ..
        } => (symbol, *price),
        // A `SimStep` reference price is band-checked directly on its underlying
        // ticker (no symbol to parse) — so a `SimStep` entered via REST
        // `insert_price` (submitted through this same seam) is admitted against the
        // venue band identically to the simulation producer's own price step
        // (#109): the band is one venue-wide admission invariant, not a
        // per-producer policy.
        VenueCommand::SimStep {
            underlying, price, ..
        } => return config.admit_price(underlying, *price),
        _ => return Ok(()),
    };
    let Ok(_) = SymbolParser::parse(symbol.as_str()) else {
        return Ok(());
    };
    config.admit_price_for_symbol(symbol.as_str(), price)
}

/// The full venue-owned **order admission** — the per-**symbol** price band, tick,
/// lot, and max-quantity gate (#114 item 5) — resolved by the order's full symbol via
/// [`admit_order_for_symbol`](MicrostructureConfig::admit_order_for_symbol) in the
/// **symbol-specific → underlying → venue-default** fallback order.
///
/// This is the single admission the **live gateway submit** seam
/// ([`crate::state::AppState::submit`]) and the **replay/recovery re-execution** seam
/// both run **before matching**, so a per-symbol tick / lot / max-quantity /
/// price-band override genuinely accepts/rejects an order **identically live and on
/// replay**. It is a pure function of the shared config (no wall-clock / RNG), so a
/// hostile bundle carrying an order the live venue would have refused is refused
/// identically on re-execution, and a legitimate journal (whose orders the live venue
/// already admitted) re-executes unchanged.
///
/// The upstream `orderbook-rs` `ContractSpecs` is applied at the leaf **per
/// underlying** only (verified against the pinned upstream — a leaf's validation is
/// fixed at vivification from its underlying's specs, with no per-symbol / per-style
/// setter), so this venue-owned check closes the per-symbol gap ahead of the leaf; the
/// leaf keeps enforcing the per-underlying tick / lot / max-quantity, so net
/// acceptance is the intersection (a per-symbol override tighter than its underlying is
/// the enforceable direction).
///
/// Only `AddOrder` / `Replace` carry an order to admit (their `limit_price` +
/// `quantity`); a `SimStep` reference price is band-checked on its underlying (no lot /
/// tick / quantity semantics); every other command carries nothing to admit. A symbol
/// that does not parse is skipped (the router / executor rejects it).
///
/// # Errors
///
/// An [`OrderAdmissionError`] naming the specific per-symbol violation (band, tick,
/// lot, or max quantity).
pub(crate) fn check_order_admission(
    config: &MicrostructureConfig,
    command: &VenueCommand,
) -> Result<(), OrderAdmissionError> {
    let (symbol, limit_price, quantity) = match command {
        VenueCommand::AddOrder {
            symbol,
            limit_price,
            quantity,
            ..
        }
        | VenueCommand::Replace {
            symbol,
            limit_price,
            quantity,
            ..
        } => (symbol, *limit_price, *quantity),
        // A `SimStep` reference price carries no order quantity/tick — band only, on
        // the underlying (mirrors `check_price_band`'s `SimStep` arm).
        VenueCommand::SimStep {
            underlying, price, ..
        } => return config.admit_price(underlying, *price).map_err(Into::into),
        _ => return Ok(()),
    };
    let Ok(_) = SymbolParser::parse(symbol.as_str()) else {
        return Ok(());
    };
    config.admit_order_for_symbol(symbol.as_str(), limit_price, quantity)
}

/// The reconstructed artifacts a successful [`recover`] produces.
///
/// [`events`](Self::events) is the re-derived ordered [`VenueEvent`] stream — the
/// oracle's primary artifact, equal (per the recovery contract) to the stored
/// stream. [`executor`](Self::executor) is the rebuilt per-underlying book, so a
/// caller can compare reconstructed top-of-book / resting state against a live run.
/// [`last_sequence`](Self::last_sequence) is the highest `N` present in the stream
/// (including a trailing epoch marker), i.e. the sequence recovery leaves the
/// underlying at — the venue **continues** from `last_sequence + 1`, it never
/// resets.
pub struct Recovered {
    /// The re-derived ordered `VenueEvent` stream (equal to the stored one).
    pub events: Vec<VenueEvent>,
    /// The rebuilt per-underlying book, for state-reconstruction assertions.
    pub executor: MatchingExecutor,
    /// The highest `underlying_sequence` present in the recovered stream, or
    /// `None` for an empty journal.
    pub last_sequence: Option<SequenceNumber>,
}

impl std::fmt::Debug for Recovered {
    /// Summarises the recovered stream — the [`MatchingExecutor`] is not `Debug`
    /// (it wraps the upstream hierarchy), so it is omitted.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recovered")
            .field("events", &self.events.len())
            .field("last_sequence", &self.last_sequence)
            .field("underlying", &self.executor.underlying())
            .finish_non_exhaustive()
    }
}

/// Recovers one underlying's book from its journal by re-executing every journaled
/// [`VenueCommand`](crate::exchange::VenueCommand) in `underlying_sequence` order
/// into a **fresh** [`MatchingExecutor`], using the stored [`VenueEvent`] as the
/// integrity oracle.
///
/// The `journal` is any [`VenueJournal`] — the in-memory store, or the durable
/// PostgreSQL store (#029) — so the same reducer recovers both. The lineage is read
/// back from the journal header, so re-derived ids land in the run's namespace.
///
/// # Errors
///
/// - [`JournalError::SchemaTooNew`] if the journal header's envelope schema is
///   newer than this binary understands — recovery refuses to start rather than
///   mis-parse.
/// - [`JournalError::Corruption`] naming the exact `(underlying, N)` if a
///   re-executed event does **not** equal the stored event at `N`.
/// - a durable-store read failure ([`JournalError::Backend`]) if the journal cannot
///   be read.
pub fn recover<J>(journal: &J, underlying: &str) -> Result<Recovered, JournalError>
where
    J: VenueJournal + ?Sized,
{
    recover_inner(journal, underlying, None, None)
}

/// Recovers one underlying's book **and rebuilds the shared `(account, ClOrdID) →
/// order_id` correlation index** (#098) from the same journaled `AddOrder` stream —
/// the boot-recovery (#085) entry point that makes cross-session cancel/replace
/// survive a restart without a separate durable copy of the correlation.
///
/// Every re-executed placement that carries a `client_order_id` records into
/// `index` exactly as the live path did, so a recovered venue resolves the same
/// client ids it resolved before the restart. Re-execution is otherwise identical
/// to [`recover`] — the index is a pure side effect and never changes a re-derived
/// event, so the integrity oracle is unaffected.
///
/// # Errors
///
/// The same typed failures as [`recover`].
pub fn recover_with_index<J>(
    journal: &J,
    underlying: &str,
    index: &Arc<ClOrdIdIndex>,
) -> Result<Recovered, JournalError>
where
    J: VenueJournal + ?Sized,
{
    recover_inner(journal, underlying, None, Some(index))
}

/// Recovers one underlying's book with the venue [`MicrostructureConfig`] applied to
/// the fresh reconstruction book — the **determinism-critical** recovery entry point
/// the replay driver uses so a book vivified during replay inherits the identical
/// fee schedule, STP mode, and contract specs the live venue applied, and a
/// fee/STP-sensitive scenario reconstructs **exactly**
/// ([02 §5](../../../docs/02-matching-architecture.md#5-determinism),
/// [05 §4](../../../docs/05-microstructure-config.md#4-fee-schedules)). The config is
/// applied identically here and on the live book-creation path (the same
/// [`apply_to_underlying`](crate::microstructure::apply_to_underlying) call), so
/// re-execution reproduces the recorded fees/fills/events.
///
/// A [`recover`] with no config re-executes onto a **bare** book (no fee schedule,
/// STP, or contract-spec validation) — the right choice only when the recording was
/// itself bare; a config-scoped scenario **must** use this entry point.
///
/// # Errors
///
/// - [`JournalError::SchemaTooNew`] / [`JournalError::Corruption`] /
///   [`JournalError::Backend`] exactly as [`recover`];
/// - [`JournalError::ConfigRejected`] if the carried config's resolved contract
///   specs are rejected by the upstream builder (a malformed replay input).
pub fn recover_with_microstructure<J>(
    journal: &J,
    underlying: &str,
    microstructure: &MicrostructureConfig,
) -> Result<Recovered, JournalError>
where
    J: VenueJournal + ?Sized,
{
    recover_inner(journal, underlying, Some(microstructure), None)
}

/// Recovers one underlying's book with the venue [`MicrostructureConfig`] applied
/// **and** rebuilds the shared `(account, ClOrdID) → order_id` index (#098) — the
/// config-scoped boot-recovery entry point combining
/// [`recover_with_microstructure`] and [`recover_with_index`].
///
/// # Errors
///
/// The same typed failures as [`recover_with_microstructure`].
pub fn recover_with_microstructure_and_index<J>(
    journal: &J,
    underlying: &str,
    microstructure: &MicrostructureConfig,
    index: &Arc<ClOrdIdIndex>,
) -> Result<Recovered, JournalError>
where
    J: VenueJournal + ?Sized,
{
    recover_inner(journal, underlying, Some(microstructure), Some(index))
}

/// Recovers a journal into a **caller-provided** [`MatchingExecutor`] — the
/// boot-time [`AppState`](crate::state::AppState) resume seam (#85). The venue
/// builds the fresh reconstruction book with its **shared** venue-wide
/// `InstrumentRegistry` + `SymbolIndex` and microstructure applied (via
/// `MatchingExecutor::new_with_registry_and_index`), then hands it here so
/// re-execution vivifies the recovered leaves onto the **same** venue index every
/// fresh underlying shares — so a recovered instrument is visible to venue-wide
/// reads, exactly as a live one is. This is the **same** reducer as [`recover`] /
/// [`recover_with_microstructure`] (one algorithm; stored event = integrity
/// oracle); only the executor's provenance differs.
///
/// The `underlying` is taken from [`MatchingExecutor::underlying`], so the caller
/// must build the executor for the stream being recovered. `microstructure` is used
/// **only** to re-run the live price-band admission check on the re-execution path
/// (a tampered durable record is refused before it re-executes); the executor
/// already carries its own fee/STP/specs, so pass the venue config that built it, or
/// `None` for a bare reconstruction with no admission check.
///
/// `clordid_index` is the optional shared account-scoped `(account, ClOrdID) →
/// order_id` correlation index (#098): `Some` on the #085 boot-recovery path so a
/// resumed underlying **rebuilds** its cross-session correlations from the same
/// `AddOrder` / `Replace` stream (the identical mapping the live post-journal actor
/// published), and `None` for a bare reconstruction. It is populated as a pure
/// side effect of the re-derived event — it never changes a re-derived event, so
/// the integrity oracle is unaffected.
///
/// # Errors
///
/// [`JournalError::SchemaTooNew`] / [`JournalError::Corruption`] (naming the exact
/// `(underlying, N)`) / [`JournalError::Backend`] / [`JournalError::ResourceLimit`]
/// / [`JournalError::PriceOutOfBand`] exactly as the sibling entry points.
pub fn recover_into<J>(
    journal: &J,
    executor: MatchingExecutor,
    microstructure: Option<&MicrostructureConfig>,
    clordid_index: Option<&Arc<ClOrdIdIndex>>,
) -> Result<Recovered, JournalError>
where
    J: VenueJournal + ?Sized,
{
    // Refuse a forward-incompatible journal BEFORE replaying anything (the header is
    // read first so re-derived ids land in the same namespace), mirroring
    // `recover_inner`.
    let header = journal.header();
    if !header.is_current_schema() {
        return Err(JournalError::SchemaTooNew {
            found: header.schema_version.clone(),
        });
    }
    let lineage = header.lineage_id.clone();
    // The reconstruction stream is the executor's own underlying — the caller built
    // it for the stream being recovered.
    let underlying = executor.underlying().to_string();
    let records = journal.read_from(SequenceNumber::START)?;
    reduce_into_executor(
        &records,
        executor,
        &underlying,
        &lineage,
        microstructure,
        clordid_index,
    )
}

/// The shared recovery core behind [`recover`] (bare) and
/// [`recover_with_microstructure`] (config-scoped): reads + schema-checks the header,
/// then re-executes the command stream into a fresh executor built with `None` (bare)
/// or `Some(config)` (the venue microstructure) applied at book creation.
fn recover_inner<J>(
    journal: &J,
    underlying: &str,
    microstructure: Option<&MicrostructureConfig>,
    clordid_index: Option<&Arc<ClOrdIdIndex>>,
) -> Result<Recovered, JournalError>
where
    J: VenueJournal + ?Sized,
{
    // Refuse a forward-incompatible journal BEFORE replaying anything (the header is
    // read first so re-derived ids land in the same namespace).
    let header = journal.header();
    if !header.is_current_schema() {
        return Err(JournalError::SchemaTooNew {
            found: header.schema_version.clone(),
        });
    }
    let lineage = header.lineage_id.clone();
    let records = journal.read_from(SequenceNumber::START)?;
    recover_from_records(
        &records,
        underlying,
        &lineage,
        microstructure,
        clordid_index,
    )
}

/// The pure reducer over an already-read record slice — re-executes the command
/// records in `N` order and oracle-compares against the stored events. Factored out
/// so the in-memory and durable stores share one algorithm and it is unit-testable
/// without a store. `microstructure` is `None` for a bare reconstruction, or
/// `Some(config)` to apply the venue fee/STP/specs at book creation (the
/// determinism-critical replay path).
fn recover_from_records(
    records: &[JournalRecord],
    underlying: &str,
    lineage: &LineageId,
    microstructure: Option<&MicrostructureConfig>,
    clordid_index: Option<&Arc<ClOrdIdIndex>>,
) -> Result<Recovered, JournalError> {
    // The fresh reconstruction book: bare, or with the venue microstructure applied
    // BEFORE any leaf is vivified — the same apply the live book-creation path
    // performs, so a config-scoped scenario replays exactly.
    let executor = match microstructure {
        None => MatchingExecutor::new(underlying),
        Some(config) => {
            MatchingExecutor::new_with_microstructure(underlying, config).map_err(|error| {
                JournalError::ConfigRejected {
                    detail: error.to_string(),
                }
            })?
        }
    };
    reduce_into_executor(
        records,
        executor,
        underlying,
        lineage,
        microstructure,
        clordid_index,
    )
}

/// The pure reducer over an already-read record slice **and a pre-built executor**:
/// re-executes the command records in `N` order and oracle-compares against the
/// stored events. Factored out so the standalone-executor entry points
/// ([`recover`] / [`recover_with_microstructure`]) and the shared-registry
/// [`recover_into`] boot seam share **one** algorithm. `microstructure` is `None`
/// for no admission check, or `Some(config)` to re-run the live venue price-band
/// admission on the re-execution path (a tampered record is refused before it
/// re-executes).
///
/// `clordid_index` is the optional shared account-scoped `(account, ClOrdID) →
/// order_id` correlation index (#098). When `Some`, the reducer **rebuilds** the
/// cross-session correlations from the same journaled `AddOrder` / `Replace` stream
/// by applying [`apply_committed_correlation`] to each re-derived, oracle-verified
/// event — the **identical** deterministic function the live single-writer actor
/// runs post-journal, so a recovered venue resolves the same client ids it resolved
/// before the restart. Because it runs only **after** the stored event is confirmed
/// (a tail command re-executes to derive its event, then publishes), the index is a
/// pure post-journal side effect and never perturbs the integrity oracle.
fn reduce_into_executor(
    records: &[JournalRecord],
    mut executor: MatchingExecutor,
    underlying: &str,
    lineage: &LineageId,
    microstructure: Option<&MicrostructureConfig>,
    clordid_index: Option<&Arc<ClOrdIdIndex>>,
) -> Result<Recovered, JournalError> {
    let mut events = Vec::new();

    for command in command_records_in_order(records) {
        // Venue-owned order admission on the replay re-execution path (the SAME
        // per-symbol band + tick + lot + max-quantity gate `AppState::submit` runs
        // live, #114 item 5): a hostile bundle can carry an order whose price /
        // tick / lot / quantity bypasses the live admission seam, so refuse it BEFORE
        // it re-executes rather than reconstruct a book the live venue would never
        // have. A legitimate journal never trips this — the live venue admitted every
        // command before journaling it, and the fingerprint gate pins the replay
        // config (hence the per-symbol specs) to the recorded one, so admission is
        // identical live and on replay.
        if let Some(config) = microstructure {
            check_order_admission(config, &command.command).map_err(|error| {
                JournalError::PriceOutOfBand {
                    detail: error.to_string(),
                }
            })?;
        }
        let outcome = executor.execute(ExecutionContext {
            underlying,
            lineage_id: lineage,
            sequence: command.sequence,
            venue_ts: command.venue_ts,
            command: &command.command,
        });
        let derived = VenueEvent::new(
            command.sequence,
            command.venue_ts,
            command.command.clone(),
            outcome,
        );
        // The stored event (when present) is the integrity oracle, not an apply
        // source: a mismatch halts, never a silent divergent resume. The compare
        // tolerates ONLY a pre-#132 legacy reject that decoded to `Internal`
        // (see `stored_event_matches`) — every other difference is corruption.
        if let Some(stored) = stored_event_at(records, command.sequence)
            && !stored_event_matches(stored, &derived)
        {
            return Err(JournalError::Corruption {
                underlying: underlying.to_string(),
                sequence: command.sequence,
            });
        }
        // Rebuild the cross-session `(account, ClOrdID) → order_id` index (#098) from
        // the SAME committed `(command, outcome)` the live single-writer actor
        // publishes from post-journal — the identical deterministic function, run
        // here only AFTER the event is oracle-verified (already journaled), so the
        // recovered correlations are byte-for-byte the ones the live venue exposed.
        if let Some(index) = clordid_index {
            apply_committed_correlation(
                index,
                underlying,
                derived.underlying_sequence,
                &derived.command,
                &derived.outcome,
            );
        }
        events.push(derived);
    }

    // The highest sequence present across ALL records (commands, events, and any
    // trailing epoch marker) — the sequence recovery leaves the underlying at.
    let last_sequence = records.iter().map(JournalRecord::sequence).max();

    Ok(Recovered {
        events,
        executor,
        last_sequence,
    })
}

/// Whether a re-derived event matches the stored oracle event, tolerating ONLY a
/// **legacy** (pre-#132) reject whose `RejectKind` decoded to the `Internal`
/// default.
///
/// A pre-#132 binary recorded a reject as `{ reason }` with no `kind`; that
/// decodes (via `#[serde(default)]`, [envelope](crate::exchange::VenueOutcome))
/// to [`RejectKind::Internal`]. A #132+ recovery re-derives the SPECIFIC kind
/// (`NotFound`, `NotOwner`, …), so a plain exact compare would flag every such
/// legacy record as [`JournalError::Corruption`] and refuse to recover an
/// otherwise-valid journal (#132). This upgrades a stored **legacy `Internal`**
/// reject kind to the re-derived kind before comparing, so the (absent) legacy
/// kind is the ONLY tolerated difference — the reject `reason` and everything
/// else must still match exactly.
///
/// It can never mask a real corruption: the sequenced executor is deterministic,
/// so a #132+ journal's *genuine* `Internal` reject re-derives `Internal` and
/// needs no upgrade, and a stored kind that is already specific is compared
/// as-is (a specific-vs-different stored kind stays a mismatch).
fn stored_event_matches(stored: &VenueEvent, derived: &VenueEvent) -> bool {
    stored == derived || upgrade_legacy_reject_kind(stored, derived) == *derived
}

/// Clones `stored`, upgrading a legacy [`RejectKind::Internal`] reject kind to the
/// re-derived kind — the top-level [`VenueOutcome::Rejected`] and the
/// [`VenueOutcome::Replace`] add leg — for the legacy-tolerant compare in
/// [`stored_event_matches`]. A stored kind that is not `Internal` is left
/// untouched, so a genuine kind divergence still fails the compare.
fn upgrade_legacy_reject_kind(stored: &VenueEvent, derived: &VenueEvent) -> VenueEvent {
    let mut out = stored.clone();
    match (&mut out.outcome, &derived.outcome) {
        (
            VenueOutcome::Rejected { kind, .. },
            VenueOutcome::Rejected {
                kind: derived_kind, ..
            },
        ) if *kind == RejectKind::Internal => {
            *kind = *derived_kind;
        }
        (
            VenueOutcome::Replace {
                add: AddOutcome::Rejected { kind, .. },
                ..
            },
            VenueOutcome::Replace {
                add:
                    AddOutcome::Rejected {
                        kind: derived_kind, ..
                    },
                ..
            },
        ) if *kind == RejectKind::Internal => {
            *kind = *derived_kind;
        }
        _ => {}
    }
    out
}

/// The write-ahead command records in ascending `underlying_sequence` order — the
/// re-execution order. Event and epoch-marker records are skipped (events are the
/// oracle, not an apply source; an epoch marker is structural, not re-executable).
fn command_records_in_order(records: &[JournalRecord]) -> Vec<&JournalCommand> {
    let mut commands: Vec<&JournalCommand> = records
        .iter()
        .filter_map(|record| match record {
            JournalRecord::Command(command) => Some(command),
            JournalRecord::Event(_) | JournalRecord::Epoch(_) => None,
        })
        .collect();
    commands.sort_by_key(|command| command.sequence);
    commands
}

/// The stored [`VenueEvent`] at `sequence`, if its paired event was journaled (a
/// tail command with no paired event yields `None`, so recovery derives it).
fn stored_event_at(records: &[JournalRecord], sequence: SequenceNumber) -> Option<&VenueEvent> {
    records.iter().find_map(|record| match record {
        JournalRecord::Event(event) if event.underlying_sequence == sequence => Some(event),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::envelope::{RejectKind, VenueCommand, VenueOutcome};
    use crate::exchange::event::EventTimestamp;
    use crate::exchange::identity::{JournalHeader, VENUE_ENVELOPE_SCHEMA};
    use crate::exchange::journal::{InMemoryVenueJournal, VenueJournal};
    use crate::exchange::symbol::Symbol;
    use crate::models::AccountId;

    const UNDERLYING: &str = "BTC";
    const TS: EventTimestamp = EventTimestamp::new(1_700_000_000_000);

    fn lineage() -> LineageId {
        LineageId::new("run-1")
    }

    fn sym(raw: &str) -> Symbol {
        match Symbol::parse(raw) {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
        }
    }

    /// A cancel command — the cheapest deterministic outcome (`Cancelled`) for the
    /// oracle to compare re-execution against.
    fn cancel(seq: u64) -> VenueCommand {
        VenueCommand::CancelOrder {
            symbol: sym("BTC-20240329-50000-C"),
            order_id: lineage().venue_order_id(UNDERLYING, SequenceNumber::new(seq), 0),
            account: AccountId::new("acct-1"),
        }
    }

    /// Journals a `(command, event)` pair for `seq`, re-deriving the event through a
    /// throwaway executor so the stored event matches a clean re-execution.
    fn record_pair(journal: &mut InMemoryVenueJournal, seq: u64) {
        let mut executor = MatchingExecutor::new(UNDERLYING);
        // Replay any earlier commands so this executor is at the right state, then
        // capture the outcome for `seq` — a self-contained clean pair.
        let command = cancel(seq);
        let outcome = executor.execute(ExecutionContext {
            underlying: UNDERLYING,
            lineage_id: &lineage(),
            sequence: SequenceNumber::new(seq),
            venue_ts: TS,
            command: &command,
        });
        let seq_n = SequenceNumber::new(seq);
        journal
            .append(JournalRecord::command(seq_n, TS, command.clone()))
            .expect("append command");
        journal
            .append(JournalRecord::event(VenueEvent::new(
                seq_n, TS, command, outcome,
            )))
            .expect("append event");
    }

    fn journal_with_header(header: JournalHeader) -> InMemoryVenueJournal {
        InMemoryVenueJournal::new(header)
    }

    #[test]
    fn test_recover_clean_journal_reexecutes_to_stored_events() {
        let mut journal = journal_with_header(JournalHeader::new(lineage()));
        for seq in 0..3 {
            record_pair(&mut journal, seq);
        }
        let stored = match journal.read_from(SequenceNumber::START) {
            Ok(records) => records,
            Err(e) => panic!("read failed: {e}"),
        };
        let stored_events: Vec<_> = stored
            .iter()
            .filter_map(|r| match r {
                JournalRecord::Event(e) => Some(e.clone()),
                _ => None,
            })
            .collect();

        match recover(&journal, UNDERLYING) {
            Ok(recovered) => {
                assert_eq!(
                    recovered.events, stored_events,
                    "recovery re-executes a clean journal to events equal to the stored ones"
                );
                assert_eq!(recovered.last_sequence, Some(SequenceNumber::new(2)));
            }
            Err(e) => panic!("clean recovery must not halt: {e:?}"),
        }
    }

    #[test]
    fn test_recover_refuses_newer_schema_with_typed_error() {
        let header = JournalHeader {
            schema_version: "venue.v2".to_string(),
            lineage_id: lineage(),
        };
        let journal = journal_with_header(header);
        match recover(&journal, UNDERLYING) {
            Err(JournalError::SchemaTooNew { found }) => assert_eq!(found, "venue.v2"),
            other => panic!("expected a SchemaTooNew refusal, got {other:?}"),
        }
    }

    #[test]
    fn test_recover_halts_on_corrupted_stored_event_naming_underlying_and_sequence() {
        let mut journal = journal_with_header(JournalHeader::new(lineage()));
        record_pair(&mut journal, 0);
        // Overwrite the stored event at seq 0 with a divergent outcome by rebuilding
        // the journal (the store refuses a differing re-append, so build afresh).
        let mut corrupted = journal_with_header(JournalHeader::new(lineage()));
        let command = cancel(0);
        corrupted
            .append(JournalRecord::command(
                SequenceNumber::new(0),
                TS,
                command.clone(),
            ))
            .expect("append command");
        corrupted
            .append(JournalRecord::event(VenueEvent::new(
                SequenceNumber::new(0),
                TS,
                command,
                VenueOutcome::rejected(RejectKind::Internal, "corrupted-by-test"),
            )))
            .expect("append corrupted event");

        match recover(&corrupted, UNDERLYING) {
            Err(JournalError::Corruption {
                underlying,
                sequence,
            }) => {
                assert_eq!(underlying, UNDERLYING);
                assert_eq!(sequence, SequenceNumber::new(0));
            }
            other => panic!("expected a Corruption halt, got {other:?}"),
        }
    }

    #[test]
    fn test_recover_tolerates_a_legacy_pre_132_reject_without_kind() {
        // A pre-#132 journal recorded a reject with no `kind`, which decodes to
        // `RejectKind::Internal`. Recovery re-derives the SPECIFIC kind; the compare
        // must upgrade the legacy `Internal` to that kind and RECOVER, not report the
        // record as corruption (#132). Same-reason is required — only the absent
        // legacy kind is tolerated.
        let command = cancel(0);
        let mut ex = MatchingExecutor::new(UNDERLYING);
        let derived = ex.execute(ExecutionContext {
            underlying: UNDERLYING,
            lineage_id: &lineage(),
            sequence: SequenceNumber::new(0),
            venue_ts: TS,
            command: &command,
        });
        let (specific_kind, reason) = match &derived {
            VenueOutcome::Rejected { kind, reason } => (*kind, reason.clone()),
            other => panic!("a cancel of an unknown order must reject, got {other:?}"),
        };
        assert_ne!(
            specific_kind,
            RejectKind::Internal,
            "the live reject carries a specific kind (else this test is vacuous)"
        );

        // A LEGACY stored event: the SAME reason, kind defaulted to `Internal`.
        let mut journal = journal_with_header(JournalHeader::new(lineage()));
        journal
            .append(JournalRecord::command(
                SequenceNumber::new(0),
                TS,
                command.clone(),
            ))
            .expect("append command");
        journal
            .append(JournalRecord::event(VenueEvent::new(
                SequenceNumber::new(0),
                TS,
                command,
                VenueOutcome::rejected(RejectKind::Internal, reason),
            )))
            .expect("append legacy event");

        match recover(&journal, UNDERLYING) {
            Ok(recovered) => match &recovered.events[0].outcome {
                VenueOutcome::Rejected { kind, .. } => assert_eq!(
                    *kind, specific_kind,
                    "recovery re-derives the specific kind for the recovered event"
                ),
                other => panic!("expected a re-derived Rejected, got {other:?}"),
            },
            Err(e) => panic!("a legacy pre-#132 reject must recover, not corrupt: {e:?}"),
        }
    }

    #[test]
    fn test_recover_derives_event_for_tail_command_with_no_paired_event() {
        let mut journal = journal_with_header(JournalHeader::new(lineage()));
        record_pair(&mut journal, 0);
        // A tail command at seq 1 with NO paired event (crash between steps 1 and 4).
        let command = cancel(1);
        journal
            .append(JournalRecord::command(SequenceNumber::new(1), TS, command))
            .expect("append tail command");

        match recover(&journal, UNDERLYING) {
            Ok(recovered) => {
                assert_eq!(
                    recovered.events.len(),
                    2,
                    "the tail command re-executes to derive its event"
                );
                assert_eq!(recovered.last_sequence, Some(SequenceNumber::new(1)));
            }
            Err(e) => panic!("tail-command recovery must not halt: {e:?}"),
        }
    }

    #[test]
    fn test_recover_empty_journal_yields_no_events() {
        let journal = journal_with_header(JournalHeader::new(lineage()));
        match recover(&journal, UNDERLYING) {
            Ok(recovered) => {
                assert!(recovered.events.is_empty());
                assert_eq!(recovered.last_sequence, None);
            }
            Err(e) => panic!("empty recovery must not halt: {e:?}"),
        }
        // Sanity: the current schema is what the reducer accepts.
        assert_eq!(VENUE_ENVELOPE_SCHEMA, "venue.v1");
    }

    #[test]
    fn test_check_price_band_admits_sim_step_reference_price_like_the_producer() {
        use std::collections::BTreeMap;

        use crate::exchange::Cents;
        use crate::microstructure::{
            ContractSpecsConfig, FileMicrostructure, MicrostructureConfig,
        };

        // A band capped at 1_000_000 cents — the same shape the sim producer's own
        // test uses. A `SimStep` reference price is now band-checked at this seam,
        // so a REST `insert_price` (submitted as a `SimStep`) is admitted against
        // the band identically to the simulation producer (#109).
        let file = FileMicrostructure {
            specs: Some(ContractSpecsConfig {
                max_price_cents: Some(1_000_000),
                ..ContractSpecsConfig::default()
            }),
            ..FileMicrostructure::default()
        };
        let config = MicrostructureConfig::resolve(&file, &BTreeMap::new())
            .expect("narrow-band config resolves");

        let sim_step = |cents: u64| VenueCommand::SimStep {
            now_ms: EventTimestamp::new(0),
            underlying: UNDERLYING.to_string(),
            price: Cents::new(cents),
            bid: None,
            ask: None,
        };

        assert!(
            check_price_band(&config, &sim_step(1_000_001)).is_err(),
            "a SimStep reference price above the band is rejected (REST ≡ sim producer)"
        );
        assert!(
            check_price_band(&config, &sim_step(1_000_000)).is_ok(),
            "an at-cap SimStep is admitted (the band is inclusive)"
        );
    }

    #[test]
    fn test_check_order_admission_enforces_per_symbol_tick_lot_and_max_qty() {
        use std::collections::BTreeMap;

        use crate::exchange::Cents;
        use crate::exchange::boundary::{Hash32, STPMode, Side, TimeInForce};
        use crate::microstructure::{
            ContractSpecsConfig, FileMicrostructure, MicrostructureConfig,
        };
        use crate::models::OrderType;

        // A per-symbol override TIGHTER than its underlying: a 5-cent tick, 2-lot,
        // 10-contract cap on one BTC contract, over a wide underlying default.
        let file = FileMicrostructure::default();
        let mut specs = BTreeMap::new();
        specs.insert(
            "BTC-20240329-50000-C".to_string(),
            ContractSpecsConfig {
                tick_size_cents: Some(5),
                lot_size: Some(2),
                max_order_qty: Some(10),
                ..ContractSpecsConfig::default()
            },
        );
        let config = MicrostructureConfig::resolve(&file, &specs).expect("resolves");

        let add = |contract: &str, seq: u64, price: u64, quantity: u64| VenueCommand::AddOrder {
            symbol: sym(contract),
            order_id: lineage().venue_order_id(UNDERLYING, SequenceNumber::new(seq), 0),
            account: AccountId::new("acct-1"),
            owner: Hash32([0x11; 32]),
            client_order_id: None,
            side: Side::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        };
        let overridden = "BTC-20240329-50000-C";

        // Satisfies the per-symbol gate → admitted.
        assert!(check_order_admission(&config, &add(overridden, 0, 500, 4)).is_ok());
        // Off the per-symbol 5-cent tick → rejected (the underlying's 1-cent tick would
        // have admitted 503; the per-symbol gate closes it before the leaf).
        assert!(check_order_admission(&config, &add(overridden, 0, 503, 4)).is_err());
        // Off the per-symbol 2-lot → rejected.
        assert!(check_order_admission(&config, &add(overridden, 0, 500, 3)).is_err());
        // Above the per-symbol 10-contract cap → rejected.
        assert!(check_order_admission(&config, &add(overridden, 0, 500, 12)).is_err());

        // A sibling contract with no per-symbol override falls back to the venue
        // default (1-cent tick, 1-lot, 1_000_000 cap): 503 / 3 all admitted.
        assert!(
            check_order_admission(&config, &add("BTC-20240329-60000-C", 1, 503, 3)).is_ok(),
            "a sibling contract falls back to the (looser) venue default"
        );
    }
}
