//! The venue's in-memory, append-only, **write-ahead** command/event journal —
//! the durability substrate the single-writer actor writes through
//! ([02 §6](../../../docs/02-matching-architecture.md),
//! [ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//!
//! ## Physical schema
//!
//! The journal is **one append-only record stream per underlying** (this type is
//! that stream for one underlying; the actor owns exactly one). Each
//! `underlying_sequence` `N` carries **two records**, appended in write-ahead
//! order — the [`VenueCommand`] envelope **before** matching runs, and the paired
//! [`VenueEvent`] **after** the outcome is captured
//! ([02 §6](../../../docs/02-matching-architecture.md)):
//!
//! | Record                     | When                    | Uniqueness key           |
//! |----------------------------|-------------------------|--------------------------|
//! | [`RecordKind::Command`]    | step 1, before execute  | `(underlying, N, kind)`  |
//! | [`RecordKind::Event`]      | step 4, after capture   | `(underlying, N, kind)`  |
//!
//! `(N, kind)` (the underlying is implicit in the stream) is the uniqueness key,
//! so a command is never appended twice and the idempotent re-append of the
//! ambiguous-result recovery path (below) is a **no-op**, never a gap or a
//! duplicate.
//!
//! ## Contract, fixed here; store, swapped later
//!
//! [`VenueJournal`] names the methods to **match the upstream
//! `OptionChainJournal` trait shape** (`append` / `read_from` / `last_sequence`),
//! so the durable PostgreSQL store (#029) swaps in behind the **same contract**
//! at the trait boundary. A caveat for that swap: `append` is a **synchronous**
//! method called inside the actor's async `run` loop, which is correct for this
//! in-memory store but means a #029 store performing blocking I/O here would
//! block a `tokio` worker — so #029 must run the turn off the async worker
//! (dedicated writer thread / per-actor current-thread runtime, or
//! `spawn_blocking`), or make the trait async, before it lands
//! (`rules/global_rules.md` *Concurrency*). The one deliberate deviation is the
//! receiver:
//! `append` takes `&mut self` (not the upstream `&self` + interior `Mutex`)
//! because the per-underlying actor is the journal's **exclusive owner and sole
//! writer** — that is strictly stronger than the upstream single-writer
//! convention and needs no lock, so the actor can never hold one across an
//! `.await` (`rules/global_rules.md` *Concurrency*).
//!
//! This is the in-memory implementation; it is not `Send`-shared and holds no
//! lock. The upstream ships only its own `InMemoryOptionChainJournal`, so the
//! whole venue journal is `fauxchange` work.

use serde::{Deserialize, Serialize};

use crate::exchange::envelope::{VenueCommand, VenueEvent};
use crate::exchange::event::{EventTimestamp, SequenceNumber};
use crate::exchange::identity::{JournalHeader, LineageId, VENUE_ENVELOPE_SCHEMA};

/// Which of the records at a sequence `N` this is
/// ([02 §6](../../../docs/02-matching-architecture.md)). Part of the
/// `(underlying, N, kind)` uniqueness key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum RecordKind {
    /// The write-ahead [`VenueCommand`] envelope, appended **before** matching
    /// runs (step 1).
    Command,
    /// The paired [`VenueEvent`], appended **after** the outcome is captured
    /// (step 4).
    Event,
    /// A [`SnapshotRestored`] epoch marker — the **first** record of a fresh
    /// journal epoch after a snapshot restore
    /// ([02 §9](../../../docs/02-matching-architecture.md)). Unlike a
    /// command/event pair it is **not** re-executable: recovery treats it as an
    /// epoch boundary and does not replay prior epochs past the restored cut.
    Epoch,
}

/// The `SnapshotRestored { snapshot_id, epoch, lineage_id }` epoch marker — the
/// first record of a fresh journal epoch opened by a snapshot **restore**
/// ([02 §9](../../../docs/02-matching-architecture.md#9-snapshots-and-restore),
/// [01 §6.1](../../../docs/01-domain-model.md#61-order-identity-and-cross-protocol-idempotency)).
///
/// A restore captures *state*, not the *sequence of decisions*, so it is an
/// explicit **replay exclusion**: rather than inject a book the journal never
/// produced, it starts a new epoch over the restored consistent cut. The marker
/// carries the run's [`LineageId`] forward so restored ids keep minting in the
/// **same** namespace (the lineage is never regenerated on restore), and it
/// records the `underlying_sequence` it opens at — the sequence **continues**
/// from the last journaled value, it does **not** reset. It is a `venue.v1` wire
/// addition and carries the mandatory [`schema`](Self::schema) tag with the same
/// `deny_unknown_fields` discipline as [`VenueEvent`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct SnapshotRestored {
    /// The mandatory schema tag — always [`VENUE_ENVELOPE_SCHEMA`]
    /// (`"venue.v1"`); a missing `schema` is a hard decode error.
    pub schema: String,
    /// The per-underlying sequence this marker opens the new epoch at — the
    /// **continued** sequence, never reset to `0`.
    pub underlying_sequence: SequenceNumber,
    /// The venue-clock timestamp the restore was stamped with, in **ms**.
    pub venue_ts: EventTimestamp,
    /// The identifier of the restored snapshot.
    pub snapshot_id: String,
    /// The monotonically increasing epoch number this restore opens (a fresh
    /// venue is epoch `0`; the first restore opens epoch `1`).
    pub epoch: u64,
    /// The run lineage carried forward so id derivation continues in the same
    /// namespace ([01 §6.1](../../../docs/01-domain-model.md)).
    pub lineage_id: LineageId,
}

impl SnapshotRestored {
    /// Builds a `venue.v1` epoch marker, stamping the mandatory
    /// [`schema`](Self::schema) tag.
    #[must_use]
    #[inline]
    pub fn new(
        underlying_sequence: SequenceNumber,
        venue_ts: EventTimestamp,
        snapshot_id: impl Into<String>,
        epoch: u64,
        lineage_id: LineageId,
    ) -> Self {
        Self {
            schema: VENUE_ENVELOPE_SCHEMA.to_string(),
            underlying_sequence,
            venue_ts,
            snapshot_id: snapshot_id.into(),
            epoch,
            lineage_id,
        }
    }

    /// Returns `true` iff this marker's `schema` tag is the one the running
    /// binary understands ([`VENUE_ENVELOPE_SCHEMA`]).
    #[must_use]
    #[inline]
    pub fn is_current_schema(&self) -> bool {
        self.schema == VENUE_ENVELOPE_SCHEMA
    }
}

/// The write-ahead [`RecordKind::Command`] record — the [`VenueCommand`] envelope
/// stamped with its assigned sequence and venue-clock timestamp, journaled
/// **before** matching runs so a crash can never leave a mutation the journal
/// never recorded ([ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
///
/// On recovery this record is **re-executed** to re-derive the paired event; the
/// stored event (when present) is only the integrity oracle
/// ([02 §6](../../../docs/02-matching-architecture.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct JournalCommand {
    /// The per-underlying sequence this command was assigned.
    pub sequence: SequenceNumber,
    /// The venue-clock timestamp stamped at assignment, in **milliseconds**.
    pub venue_ts: EventTimestamp,
    /// The write-ahead command envelope.
    pub command: VenueCommand,
}

/// One journaled record — a write-ahead [`JournalCommand`] or its paired
/// [`VenueEvent`].
///
/// The durable, replayable **unit** at a sequence `N` is the `(command, event)`
/// pair; this enum is one physical record of that pair. Both variants expose
/// their [`sequence`](Self::sequence) and [`kind`](Self::kind) so the stream can
/// enforce the `(N, kind)` uniqueness key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum JournalRecord {
    /// The write-ahead command record (step 1).
    Command(JournalCommand),
    /// The paired event record (step 4).
    Event(VenueEvent),
    /// The [`SnapshotRestored`] epoch marker opening a fresh epoch (§9).
    Epoch(SnapshotRestored),
}

impl JournalRecord {
    /// Builds a write-ahead [`RecordKind::Command`] record.
    #[must_use]
    #[inline]
    pub fn command(
        sequence: SequenceNumber,
        venue_ts: EventTimestamp,
        command: VenueCommand,
    ) -> Self {
        Self::Command(JournalCommand {
            sequence,
            venue_ts,
            command,
        })
    }

    /// Builds a paired [`RecordKind::Event`] record.
    #[must_use]
    #[inline]
    pub fn event(event: VenueEvent) -> Self {
        Self::Event(event)
    }

    /// Builds a [`RecordKind::Epoch`] marker record.
    #[must_use]
    #[inline]
    pub fn epoch(marker: SnapshotRestored) -> Self {
        Self::Epoch(marker)
    }

    /// The per-underlying sequence `N` this record belongs to.
    #[must_use]
    #[inline]
    pub fn sequence(&self) -> SequenceNumber {
        match self {
            Self::Command(command) => command.sequence,
            Self::Event(event) => event.underlying_sequence,
            Self::Epoch(marker) => marker.underlying_sequence,
        }
    }

    /// Whether this is the command, the paired event, or the epoch marker at its
    /// sequence.
    #[must_use]
    #[inline]
    pub fn kind(&self) -> RecordKind {
        match self {
            Self::Command(_) => RecordKind::Command,
            Self::Event(_) => RecordKind::Event,
            Self::Epoch(_) => RecordKind::Epoch,
        }
    }
}

/// A typed journal failure ([ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
///
/// The store distinguishes a **confirmed** failure (the write definitely did not
/// commit — the actor safely reuses `N`) from an **ambiguous** result (the
/// outcome is unknown — the actor reads back the durable tail to decide). The
/// in-memory store here only ever returns [`Conflict`](Self::Conflict); the
/// confirmed / ambiguous / corruption variants exist so the **contract is fixed
/// now** for the durable store (#029) and the recovery reducer (#017).
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// The append is **confirmed not committed** — the durable store reported the
    /// write did not land. The actor reuses `N`; nothing executed, the book is
    /// untouched, and there is no cursor gap.
    #[error("journal append failed (confirmed not committed): {0}")]
    AppendFailed(String),
    /// The append outcome is **unknown** (e.g. a store timeout). The actor must
    /// read back the durable tail to determine whether `N` committed before
    /// proceeding; the re-append is idempotent either way.
    #[error("journal append result ambiguous: {0}")]
    Ambiguous(String),
    /// A record with this `(sequence, kind)` already exists with a **different**
    /// payload — an integrity violation the append refuses rather than overwrite.
    #[error("journal record conflict at sequence {} kind {kind:?}", sequence.get())]
    Conflict {
        /// The conflicting sequence.
        sequence: SequenceNumber,
        /// The conflicting record kind.
        kind: RecordKind,
    },
    /// The recovery reducer's re-executed event does **not** equal the stored
    /// event at `(underlying, sequence)` — journal corruption. Recovery
    /// ([`crate::exchange::recover`]) halts here rather than resume on divergent
    /// state; never constructed on the live submit path
    /// ([02 §6](../../../docs/02-matching-architecture.md)).
    #[error("journal corruption at underlying {underlying} sequence {}", sequence.get())]
    Corruption {
        /// The underlying whose stream diverged.
        underlying: String,
        /// The exact sequence at which re-execution disagreed with the store.
        sequence: SequenceNumber,
    },
    /// The journal's envelope schema is **newer** than the running binary
    /// understands (a forward-incompatible `venue.v1+` journal written by a later
    /// version). Recovery **refuses to start** rather than mis-parse — a schema bump
    /// is a major SemVer event ([SEMVER.md](../../../docs/SEMVER.md),
    /// [ADR-0006 §3 Version mismatch](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
    /// This is the typed production error the v0.1 slice deferred (it existed only as
    /// the `JournalHeader::is_current_schema()` predicate plus a test-local halt);
    /// the recovery reducer (#029) makes it real, matching the [`Corruption`](Self::Corruption)
    /// sibling. No `venue.v1` wire shape changes.
    #[error("journal schema {found} is newer than this binary understands")]
    SchemaTooNew {
        /// The forward-incompatible schema tag found in the journal header.
        found: String,
    },
    /// A durable-store read or query failed (a connection/decode failure on
    /// [`read_from`](VenueJournal::read_from) / [`last_sequence`](VenueJournal::last_sequence)),
    /// mapped from the underlying `sqlx::Error` at the [`crate::db`] boundary and
    /// carrying only a **non-secret** operation label — never the SQL, the row data,
    /// or the `DATABASE_URL`. The in-memory store never returns it.
    #[error("journal store backend failed: {operation}")]
    Backend {
        /// The non-secret operation label naming the failed durable call.
        operation: &'static str,
    },
}

/// The append-only journal contract, named to match the upstream
/// `OptionChainJournal` trait shape (`append` / `read_from` / `last_sequence`) so
/// the durable store (#029) swaps in behind it
/// ([02 §6](../../../docs/02-matching-architecture.md)).
///
/// The owning actor is the **exclusive writer**, so [`append`](Self::append)
/// takes `&mut self` — no interior mutability, no lock (stronger than the upstream
/// `&self` + `Mutex`). Reads are `&self` queries.
pub trait VenueJournal {
    /// The journal header carrying the run [`crate::exchange::LineageId`] and the
    /// envelope schema version, read first on recovery so re-derived ids land in
    /// the same namespace.
    #[must_use]
    fn header(&self) -> &JournalHeader;

    /// Appends one record, enforcing the `(sequence, kind)` uniqueness key.
    ///
    /// A re-append of an **identical** record is an idempotent **no-op** (`Ok`) —
    /// this is what makes the ambiguous-result recovery path gap-free.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Conflict`] if a record already exists at this
    /// `(sequence, kind)` with a different payload. The durable store (#029) may
    /// additionally return [`JournalError::AppendFailed`] or
    /// [`JournalError::Ambiguous`].
    fn append(&mut self, record: JournalRecord) -> Result<(), JournalError>;

    /// Reads every record at `from` or later, in append order.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] if the durable store cannot be read (the
    /// in-memory store never errors).
    fn read_from(&self, from: SequenceNumber) -> Result<Vec<JournalRecord>, JournalError>;

    /// The highest sequence `N` present in the stream, or `None` when empty.
    #[must_use]
    fn last_sequence(&self) -> Option<SequenceNumber>;

    /// Whether a record with this exact `(sequence, kind)` is present — the
    /// durable **tail read-back** the actor uses to resolve an ambiguous append.
    ///
    /// A read failure is treated as "not present" so the caller conservatively
    /// reuses `N` rather than assuming a commit it cannot confirm.
    #[must_use]
    fn contains(&self, sequence: SequenceNumber, kind: RecordKind) -> bool {
        match self.read_from(sequence) {
            Ok(records) => records
                .iter()
                .any(|record| record.sequence() == sequence && record.kind() == kind),
            Err(_) => false,
        }
    }
}

/// The in-memory paired command/event stream for one underlying — the #006
/// [`VenueJournal`] implementation.
///
/// Records live in a single append-ordered `Vec`; the actor's turn order **is**
/// the append order because it is the sole writer, so no external ordering is
/// imposed here. The durable store (#029) replaces this behind [`VenueJournal`].
#[derive(Debug, Clone)]
pub struct InMemoryVenueJournal {
    header: JournalHeader,
    records: Vec<JournalRecord>,
}

impl InMemoryVenueJournal {
    /// Builds an empty in-memory journal with the given header.
    #[must_use]
    #[inline]
    pub fn new(header: JournalHeader) -> Self {
        Self {
            header,
            records: Vec::new(),
        }
    }

    /// The number of physical records (two per fully-committed sequence).
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the journal holds no records yet.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl VenueJournal for InMemoryVenueJournal {
    #[inline]
    fn header(&self) -> &JournalHeader {
        &self.header
    }

    fn append(&mut self, record: JournalRecord) -> Result<(), JournalError> {
        let sequence = record.sequence();
        let kind = record.kind();
        if let Some(existing) = self
            .records
            .iter()
            .find(|candidate| candidate.sequence() == sequence && candidate.kind() == kind)
        {
            // Idempotent re-append of the identical record is a no-op; a differing
            // payload at the same key is an integrity violation.
            if *existing == record {
                return Ok(());
            }
            return Err(JournalError::Conflict { sequence, kind });
        }
        self.records.push(record);
        Ok(())
    }

    fn read_from(&self, from: SequenceNumber) -> Result<Vec<JournalRecord>, JournalError> {
        Ok(self
            .records
            .iter()
            .filter(|record| record.sequence() >= from)
            .cloned()
            .collect())
    }

    fn last_sequence(&self) -> Option<SequenceNumber> {
        self.records.iter().map(JournalRecord::sequence).max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::envelope::VenueOutcome;
    use crate::exchange::identity::LineageId;
    use crate::exchange::symbol::Symbol;
    use crate::models::AccountId;

    fn header() -> JournalHeader {
        JournalHeader::new(LineageId::new("run-1"))
    }

    fn sym(raw: &str) -> Symbol {
        match Symbol::parse(raw) {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
        }
    }

    fn cancel(seq: u64) -> VenueCommand {
        VenueCommand::CancelOrder {
            symbol: sym("BTC-20240329-50000-C"),
            order_id: crate::models::VenueOrderId::new(format!("order-{seq}")),
            account: AccountId::new("acct-1"),
        }
    }

    fn command_record(seq: u64) -> JournalRecord {
        JournalRecord::command(
            SequenceNumber::new(seq),
            EventTimestamp::new(1),
            cancel(seq),
        )
    }

    fn event_record(seq: u64) -> JournalRecord {
        JournalRecord::event(VenueEvent::new(
            SequenceNumber::new(seq),
            EventTimestamp::new(1),
            cancel(seq),
            VenueOutcome::Cancelled {
                order_id: crate::models::VenueOrderId::new(format!("order-{seq}")),
            },
        ))
    }

    #[test]
    fn test_empty_journal_has_no_last_sequence() {
        let journal = InMemoryVenueJournal::new(header());
        assert!(journal.is_empty());
        assert_eq!(journal.last_sequence(), None);
    }

    #[test]
    fn test_append_records_pair_and_report_last_sequence() {
        let mut journal = InMemoryVenueJournal::new(header());
        assert!(journal.append(command_record(0)).is_ok());
        assert!(journal.append(event_record(0)).is_ok());
        assert!(journal.append(command_record(1)).is_ok());
        // Highest sequence present is 1 even though the pair at 1 is half-written.
        assert_eq!(journal.last_sequence(), Some(SequenceNumber::new(1)));
        assert_eq!(journal.len(), 3);
    }

    #[test]
    fn test_record_sequence_and_kind_are_exposed() {
        let command = command_record(4);
        let event = event_record(4);
        assert_eq!(command.sequence(), SequenceNumber::new(4));
        assert_eq!(command.kind(), RecordKind::Command);
        assert_eq!(event.sequence(), SequenceNumber::new(4));
        assert_eq!(event.kind(), RecordKind::Event);
    }

    #[test]
    fn test_identical_reappend_is_idempotent_noop() {
        let mut journal = InMemoryVenueJournal::new(header());
        assert!(journal.append(command_record(0)).is_ok());
        // Re-appending the identical record does not duplicate it.
        assert!(journal.append(command_record(0)).is_ok());
        assert_eq!(journal.len(), 1);
    }

    #[test]
    fn test_conflicting_reappend_at_same_key_is_rejected() {
        let mut journal = InMemoryVenueJournal::new(header());
        assert!(journal.append(command_record(0)).is_ok());
        // A different command at the same (sequence, kind) is an integrity error.
        let conflicting =
            JournalRecord::command(SequenceNumber::new(0), EventTimestamp::new(999), cancel(42));
        match journal.append(conflicting) {
            Err(JournalError::Conflict { sequence, kind }) => {
                assert_eq!(sequence, SequenceNumber::new(0));
                assert_eq!(kind, RecordKind::Command);
            }
            other => panic!("expected a Conflict, got {other:?}"),
        }
    }

    #[test]
    fn test_read_from_returns_tail_in_order() {
        let mut journal = InMemoryVenueJournal::new(header());
        for seq in 0..3 {
            assert!(journal.append(command_record(seq)).is_ok());
            assert!(journal.append(event_record(seq)).is_ok());
        }
        let tail = match journal.read_from(SequenceNumber::new(1)) {
            Ok(records) => records,
            Err(e) => panic!("read_from failed: {e}"),
        };
        // Two records each for sequences 1 and 2 (0 is excluded).
        assert_eq!(tail.len(), 4);
        assert!(tail.iter().all(|r| r.sequence() >= SequenceNumber::new(1)));
    }

    #[test]
    fn test_contains_detects_committed_command() {
        let mut journal = InMemoryVenueJournal::new(header());
        assert!(!journal.contains(SequenceNumber::new(0), RecordKind::Command));
        assert!(journal.append(command_record(0)).is_ok());
        assert!(journal.contains(SequenceNumber::new(0), RecordKind::Command));
        // The event half is not present until step 4.
        assert!(!journal.contains(SequenceNumber::new(0), RecordKind::Event));
    }

    #[test]
    fn test_header_carries_lineage_and_schema() {
        let journal = InMemoryVenueJournal::new(header());
        assert!(journal.header().is_current_schema());
        assert_eq!(journal.header().lineage_id, LineageId::new("run-1"));
    }

    #[test]
    fn test_record_roundtrips_through_serde() {
        let record = event_record(7);
        let json = match serde_json::to_string(&record) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        match serde_json::from_str::<JournalRecord>(&json) {
            Ok(back) => assert_eq!(back, record),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }

    #[test]
    fn test_corruption_error_names_underlying_and_sequence() {
        // Constructed by the recovery reducer (#017); asserted here so the fixed
        // contract stays exercised on the #006 branch.
        let err = JournalError::Corruption {
            underlying: "BTC".to_string(),
            sequence: SequenceNumber::new(9),
        };
        assert!(err.to_string().contains("BTC"));
        assert!(err.to_string().contains('9'));
    }

    // ---- epoch marker (#009) ---------------------------------------------

    fn snapshot_restored(seq: u64, epoch: u64) -> SnapshotRestored {
        SnapshotRestored::new(
            SequenceNumber::new(seq),
            EventTimestamp::new(1_700_000_000_000),
            "snap-1",
            epoch,
            LineageId::new("run-1"),
        )
    }

    #[test]
    fn test_epoch_marker_exposes_its_sequence_and_kind() {
        let marker = snapshot_restored(9, 1);
        assert!(marker.is_current_schema());
        let record = JournalRecord::epoch(marker);
        assert_eq!(record.sequence(), SequenceNumber::new(9));
        assert_eq!(record.kind(), RecordKind::Epoch);
    }

    #[test]
    fn test_epoch_marker_appends_and_reads_back_as_the_first_epoch_record() {
        let mut journal = InMemoryVenueJournal::new(header());
        // A pre-restore command/event pair at sequence 5.
        assert!(journal.append(command_record(5)).is_ok());
        assert!(journal.append(event_record(5)).is_ok());
        // The epoch marker opens at the CONTINUED sequence 6 (not reset to 0).
        assert!(
            journal
                .append(JournalRecord::epoch(snapshot_restored(6, 1)))
                .is_ok()
        );
        assert_eq!(journal.last_sequence(), Some(SequenceNumber::new(6)));
        assert!(journal.contains(SequenceNumber::new(6), RecordKind::Epoch));
    }

    #[test]
    fn test_epoch_marker_roundtrips_through_serde() {
        let record = JournalRecord::epoch(snapshot_restored(6, 2));
        let json = match serde_json::to_string(&record) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        match serde_json::from_str::<JournalRecord>(&json) {
            Ok(back) => assert_eq!(back, record),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }

    #[test]
    fn test_epoch_marker_missing_schema_is_a_decode_error() {
        // The schema tag is mandatory on the marker, like the event envelope.
        let json = r#"{"underlying_sequence":6,"venue_ts":1,"snapshot_id":"snap-1","epoch":1,"lineage_id":"run-1"}"#;
        match serde_json::from_str::<SnapshotRestored>(json) {
            Err(_) => {}
            Ok(parsed) => panic!("expected a missing-schema decode error, parsed {parsed:?}"),
        }
    }
}
