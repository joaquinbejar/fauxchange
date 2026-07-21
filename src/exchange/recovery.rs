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

use option_chain_orderbook::SymbolParser;

use crate::exchange::actor::{CommandExecutor, ExecutionContext};
use crate::exchange::envelope::{VenueCommand, VenueEvent};
use crate::exchange::event::SequenceNumber;
use crate::exchange::executor::MatchingExecutor;
use crate::exchange::identity::LineageId;
use crate::exchange::journal::{JournalCommand, JournalRecord, VenueJournal};
use crate::microstructure::{MicrostructureConfig, PriceBoundError};

pub use crate::exchange::journal::JournalError;

/// Checks a command's limit price against the venue-owned price band, resolving the
/// underlying from the command's symbol **exactly** as the live [`AppState::submit`]
/// admission seam does — the single admission-band check shared by the gateway submit
/// path ([`crate::state::AppState`]) and the replay/recovery re-execution path, so an
/// over-band price is refused identically on both.
///
/// Only `AddOrder` / `Replace` carrying a `limit_price` are checked; a market order
/// (no limit price) and every non-order command carry no price to admit. A symbol
/// that does not parse is skipped here (the executor rejects it and the integrity
/// oracle catches it), mirroring the submit seam where the router rejects it.
///
/// # Errors
///
/// A [`PriceBoundError`] if the order price falls outside the underlying's
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
        _ => return Ok(()),
    };
    let Ok(parsed) = SymbolParser::parse(symbol.as_str()) else {
        return Ok(());
    };
    config.admit_price(parsed.underlying(), price)
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
    recover_inner(journal, underlying, None)
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
    recover_inner(journal, underlying, Some(microstructure))
}

/// The shared recovery core behind [`recover`] (bare) and
/// [`recover_with_microstructure`] (config-scoped): reads + schema-checks the header,
/// then re-executes the command stream into a fresh executor built with `None` (bare)
/// or `Some(config)` (the venue microstructure) applied at book creation.
fn recover_inner<J>(
    journal: &J,
    underlying: &str,
    microstructure: Option<&MicrostructureConfig>,
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
    recover_from_records(&records, underlying, &lineage, microstructure)
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
) -> Result<Recovered, JournalError> {
    // The fresh reconstruction book: bare, or with the venue microstructure applied
    // BEFORE any leaf is vivified — the same apply the live book-creation path
    // performs, so a config-scoped scenario replays exactly.
    let mut executor = match microstructure {
        None => MatchingExecutor::new(underlying),
        Some(config) => {
            MatchingExecutor::new_with_microstructure(underlying, config).map_err(|error| {
                JournalError::ConfigRejected {
                    detail: error.to_string(),
                }
            })?
        }
    };
    let mut events = Vec::new();

    for command in command_records_in_order(records) {
        // Venue-owned price-band admission on the replay re-execution path (the same
        // check `AppState::submit` runs live): a hostile bundle can carry an order
        // whose price bypasses the live admission seam, so refuse it BEFORE it
        // re-executes rather than reconstruct an out-of-band book. A legitimate
        // journal never trips this — the live venue admitted every command before
        // journaling it, and the fingerprint gate pins the replay band to the
        // recorded one.
        if let Some(config) = microstructure {
            check_price_band(config, &command.command).map_err(|error| {
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
        // source: a mismatch halts, never a silent divergent resume.
        if let Some(stored) = stored_event_at(records, command.sequence)
            && stored != &derived
        {
            return Err(JournalError::Corruption {
                underlying: underlying.to_string(),
                sequence: command.sequence,
            });
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
}
