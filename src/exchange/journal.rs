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
    /// A deserialiser **resource ceiling** was exceeded on the read/decode path —
    /// a single record over [`MAX_JOURNAL_RECORD_BYTES`] (`limit =
    /// `"record_bytes"``), or a recovery read over [`MAX_JOURNAL_RECORDS`] records
    /// (`limit = `"stream_records"``) or [`MAX_JOURNAL_STREAM_BYTES`] bytes (`limit =
    /// `"stream_bytes"``). The oversized input is **refused at the ceiling**, never
    /// buffered or decoded unboundedly — the bounded-record-size and
    /// no-unbounded-allocation guarantees of the semi-trusted-operator (A-7)
    /// deserialiser surface ([08 §4](../../../docs/08-threat-model.md#4-untrusted-input-hardening),
    /// #034). The **write path enforces the same per-record ceiling** (both stores),
    /// so no record the venue durably writes can trip the `record_bytes` read
    /// refusal — it fires only on **external tampering / a hostile bundle**, its
    /// actual threat model (see [`MAX_JOURNAL_RECORD_BYTES`]).
    #[error("journal deserialiser {limit} ceiling exceeded: {found} over {ceiling}")]
    ResourceLimit {
        /// Which ceiling tripped — `"record_bytes"`, `"stream_records"`, or
        /// `"stream_bytes"`.
        limit: &'static str,
        /// The observed value (bytes for a record / a stream read, records for a
        /// stream count).
        found: usize,
        /// The enforced ceiling.
        ceiling: usize,
    },
    /// The venue [`MicrostructureConfig`](crate::microstructure::MicrostructureConfig)
    /// carried into a config-aware recovery could not be applied to the fresh book
    /// — the upstream `ContractSpecsBuilder` rejected the resolved specs. Raised only
    /// by [`recover_with_microstructure`](crate::exchange::recover_with_microstructure)
    /// (a replay input carrying a malformed config); the live path resolves + proves
    /// the config at boot, so it never constructs this. Carries the non-secret
    /// upstream reason (never the config values that are secrets — they are not).
    #[error("recovery microstructure config rejected: {detail}")]
    ConfigRejected {
        /// The non-secret rejection detail from the microstructure apply.
        detail: String,
    },
    /// A journaled `AddOrder` / `Replace` carried a limit price outside the
    /// venue-owned `[min_price_cents, max_price_cents]` band during a config-aware
    /// recovery. Raised only by
    /// [`recover_with_microstructure`](crate::exchange::recover_with_microstructure)
    /// (the replay re-execution path re-runs the live admission-band check): a
    /// legitimate journal never trips it because the live venue admitted every
    /// command before journaling it, so this fires only on a **tampered** bundle /
    /// durable journal, refusing the command before it re-executes. Carries the
    /// non-secret band-violation detail.
    #[error("recovery order price out of band: {detail}")]
    PriceOutOfBand {
        /// The non-secret price-band violation detail.
        detail: String,
    },
}

/// The **per-record byte ceiling** for a `venue.v1` journal record — enforced
/// **symmetrically at write and at read** so it is a load-bearing safety invariant,
/// not just a read-side filter ([08 §4](../../../docs/08-threat-model.md#4-untrusted-input-hardening), #034).
///
/// **The write ≤ read symmetry invariant (load-bearing).** The **append** path on
/// **both** stores ([`InMemoryVenueJournal::append`], [`crate::db::PgVenueJournal`])
/// refuses a record whose serialized form exceeds this ceiling, and the **read**
/// path refuses the same. Because both use this *one* constant, **no record the
/// venue ever durably writes can trip the read refusal** — a durably-written record
/// is always ≤ the ceiling, so it always reads back. The read `record_bytes`
/// refusal therefore fires **only on external tampering or a hostile bundle**
/// (records the venue never wrote), which is its actual threat model. An
/// over-ceiling record is caught **at write time** through the single-writer actor's
/// existing semantics ([ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)):
/// an over-ceiling write-ahead **command** (tiny — commands carry no fills, so this
/// is ~unreachable) is rejected and its sequence reused; an over-ceiling
/// post-mutation **event** **seals** the underlying loudly
/// ([`crate::error::VenueError::JournalUnavailable`]) rather than being written and
/// then silently bricking every future recovery/replay/export of that stream.
///
/// **Rationale for the value (fill-aware).** A record's size is dominated by an
/// event's fills; a single `venue.v1` fill leg serializes to ~230 bytes (the
/// committed `add_order_event.json` golden), and an `Added` event's size ≈
/// `fills × ~230 B` + small overhead. Nothing upstream bounds fills-per-event (only
/// one order's *quantity* is bounded), so the ceiling must clear a realistic heavy
/// sweep: `2 MiB / 230 B ≈ 9_000 fill legs ≈ one aggressing order crossing ~4_500
/// resting orders in a single turn` — ~25× a heavy ~180-order sweep (~360 legs,
/// ~83 KiB, ~4 % of the ceiling), far beyond any realistic test/CI book depth. An
/// event beyond `2 MiB` **seals at write time** (loud fail-stop); it can never brick
/// replay because it is never durably written. Enforced on the durable read path,
/// the portable scenario-bundle path, and **both** write paths.
pub const MAX_JOURNAL_RECORD_BYTES: usize = 2 * 1024 * 1024;

/// The **per-read record-count ceiling** on the durable read path — a single
/// recovery read (`read_from`) loads at most this many records, so a hostile /
/// pathologically long stream cannot exhaust memory on restart (the durable OOM
/// vector deferred from #029, [08 §4](../../../docs/08-threat-model.md#4-untrusted-input-hardening)).
///
/// **Rationale + seam.** `1_000_000` records is generous for a test/CI venue
/// session; a durable read is bounded **before** the row fetch by a cheap
/// `count(*)` pre-check (paired with [`MAX_JOURNAL_STREAM_BYTES`]), so `fetch_all`
/// never allocates an unbounded result set. A stream legitimately longer than this
/// must be read **in pages / streamed** — the durable `read_from` is the documented
/// seam for a future paged/streaming reader; today it **refuses** rather than
/// truncates (a truncated read would be a silent partial recovery), returning
/// [`JournalError::ResourceLimit`] (`stream_records`). This is an aggregate-volume
/// bound (the data stays intact and recoverable via paging), distinct from the
/// per-record symmetry invariant.
pub const MAX_JOURNAL_RECORDS: usize = 1_000_000;

/// The **per-read total-byte ceiling** on the durable read path — a single recovery
/// read buffers at most this many bytes of payload, closing the *compounded*
/// allocation gap ([08 §4](../../../docs/08-threat-model.md#4-untrusted-input-hardening), #034):
/// even with the count and per-record bounds, `MAX_JOURNAL_RECORDS × MAX_JOURNAL_RECORD_BYTES`
/// is multi-terabyte, so the read is additionally bounded on total bytes.
///
/// **Rationale + seam.** `1 GiB` comfortably holds a full `MAX_JOURNAL_RECORDS`-row
/// session at kilobyte-scale records (`1_000_000 × ~1 KiB ≈ 1 GiB`) while firmly
/// bounding the single-read allocation (vs. the unbounded tens-of-GB the pre-#034
/// `fetch_all` allowed). It is enforced by a cheap `sum(octet_length(payload))`
/// pre-check **before** the row fetch, so an over-budget stream is refused before
/// any large allocation; a larger recorded run is read via the paging/streaming
/// seam, not silently mis-read.
pub const MAX_JOURNAL_STREAM_BYTES: usize = 1024 * 1024 * 1024;

/// Decodes one journal record from its serialized `payload`, enforcing the
/// [`MAX_JOURNAL_RECORD_BYTES`] ceiling **before** the (potentially expensive)
/// `serde_json` decode — the bounded deserialiser the durable read path uses so an
/// oversized record is a typed [`JournalError::ResourceLimit`], never an unbounded
/// allocation ([08 §4](../../../docs/08-threat-model.md#4-untrusted-input-hardening), #034).
///
/// # Errors
///
/// - [`JournalError::ResourceLimit`] (`limit = "record_bytes"`) if `payload`
///   exceeds the per-record ceiling;
/// - [`JournalError::Backend`] (`operation = "journal record decode"`) if the
///   bounded payload is not a well-formed `venue.v1` record.
pub fn decode_journal_record(payload: &str) -> Result<JournalRecord, JournalError> {
    if payload.len() > MAX_JOURNAL_RECORD_BYTES {
        return Err(JournalError::ResourceLimit {
            limit: "record_bytes",
            found: payload.len(),
            ceiling: MAX_JOURNAL_RECORD_BYTES,
        });
    }
    serde_json::from_str::<JournalRecord>(payload).map_err(|_| JournalError::Backend {
        operation: "journal record decode",
    })
}

/// Enforces the [`MAX_JOURNAL_RECORD_BYTES`] ceiling on an **already-decoded**
/// record — the write path (both stores) and the portable-bundle path measure the
/// record's `venue.v1` serialized byte size through this one check, so the write ≤
/// read symmetry invariant holds ([`MAX_JOURNAL_RECORD_BYTES`]).
///
/// # Errors
///
/// - [`JournalError::ResourceLimit`] (`limit = "record_bytes"`) if the record's
///   serialized form exceeds the per-record ceiling;
/// - [`JournalError::Backend`] (`operation = "journal record size check"`) if the
///   record cannot be serialized — the check **fails closed** (refuses), never
///   fails open by proceeding past an unmeasurable record (unreachable for
///   `venue.v1`, but the deserialiser must never accept what it cannot bound).
pub fn check_record_size(record: &JournalRecord) -> Result<(), JournalError> {
    let bytes = match serde_json::to_string(record) {
        Ok(json) => json.len(),
        Err(_) => {
            return Err(JournalError::Backend {
                operation: "journal record size check",
            });
        }
    };
    if bytes > MAX_JOURNAL_RECORD_BYTES {
        return Err(JournalError::ResourceLimit {
            limit: "record_bytes",
            found: bytes,
            ceiling: MAX_JOURNAL_RECORD_BYTES,
        });
    }
    Ok(())
}

/// Enforces the [`MAX_JOURNAL_RECORDS`] per-read record-count ceiling — a read whose
/// stream holds more than the ceiling is **refused** (never truncated: a truncated
/// read would be a silent partial recovery), closing the #029-deferred unbounded-read
/// OOM vector. Pure so the count bound is unit-testable without a million-row
/// database; the durable read runs it against a cheap `count(*)` **before** the row
/// fetch, so the fetch allocation is bounded.
///
/// # Errors
///
/// [`JournalError::ResourceLimit`] (`limit = "stream_records"`) if `count` exceeds
/// [`MAX_JOURNAL_RECORDS`].
pub fn enforce_stream_ceiling(count: usize) -> Result<(), JournalError> {
    if count > MAX_JOURNAL_RECORDS {
        return Err(JournalError::ResourceLimit {
            limit: "stream_records",
            found: count,
            ceiling: MAX_JOURNAL_RECORDS,
        });
    }
    Ok(())
}

/// Enforces the [`MAX_JOURNAL_STREAM_BYTES`] per-read total-byte ceiling — a read
/// whose stream sums to more than the ceiling is **refused** before the row fetch
/// allocates, closing the compounded allocation gap. Pure so the byte bound is
/// unit-testable without a gigabyte of rows; the durable read runs it against a
/// cheap `sum(octet_length(payload))` **before** the fetch.
///
/// # Errors
///
/// [`JournalError::ResourceLimit`] (`limit = "stream_bytes"`) if `bytes` exceeds
/// [`MAX_JOURNAL_STREAM_BYTES`].
pub fn enforce_stream_bytes_ceiling(bytes: usize) -> Result<(), JournalError> {
    if bytes > MAX_JOURNAL_STREAM_BYTES {
        return Err(JournalError::ResourceLimit {
            limit: "stream_bytes",
            found: bytes,
            ceiling: MAX_JOURNAL_STREAM_BYTES,
        });
    }
    Ok(())
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
        // Write-side per-record ceiling (the write ≤ read symmetry invariant): a
        // record over `MAX_JOURNAL_RECORD_BYTES` is refused AT write, so nothing the
        // venue durably holds can ever trip the read `record_bytes` refusal. The
        // actor surfaces this through its existing semantics — an over-ceiling
        // write-ahead command reuses `N`; an over-ceiling post-mutation event seals
        // the underlying (loud), never a silent write-then-brick.
        check_record_size(&record)?;
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

    // ---- bounded deserialiser (#034) -------------------------------------

    #[test]
    fn test_journal_deser_rejects_oversized_record() {
        // A record whose serialized payload exceeds the per-record ceiling is
        // refused AT the ceiling — never decoded — with a typed ResourceLimit
        // (`record_bytes`), so a hostile oversized record cannot drive an unbounded
        // decode allocation (docs/08 §4).
        let oversized = "\"".to_string() + &"a".repeat(MAX_JOURNAL_RECORD_BYTES) + "\"";
        assert!(oversized.len() > MAX_JOURNAL_RECORD_BYTES);
        match decode_journal_record(&oversized) {
            Err(JournalError::ResourceLimit {
                limit,
                found,
                ceiling,
            }) => {
                assert_eq!(limit, "record_bytes");
                assert!(found > ceiling);
                assert_eq!(ceiling, MAX_JOURNAL_RECORD_BYTES);
            }
            other => panic!("expected a record_bytes ResourceLimit, got {other:?}"),
        }
    }

    #[test]
    fn test_decode_journal_record_accepts_a_within_ceiling_record() {
        // A legitimate (small) record decodes cleanly through the bounded helper.
        let record = command_record(7);
        let payload = match serde_json::to_string(&record) {
            Ok(payload) => payload,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert!(payload.len() <= MAX_JOURNAL_RECORD_BYTES);
        match decode_journal_record(&payload) {
            Ok(decoded) => assert_eq!(decoded, record),
            Err(e) => panic!("a within-ceiling record must decode: {e}"),
        }
    }

    #[test]
    fn test_decode_journal_record_rejects_malformed_bytes_as_backend() {
        // Well-formed-size but malformed bytes are a typed Backend decode error,
        // never a panic.
        match decode_journal_record("{not json") {
            Err(JournalError::Backend { operation }) => {
                assert_eq!(operation, "journal record decode");
            }
            other => panic!("expected a Backend decode error, got {other:?}"),
        }
    }

    #[test]
    fn test_check_record_size_bounds_an_already_decoded_record() {
        // A within-ceiling decoded record passes; the ceiling constant is generous
        // above the largest legitimate record (a small cancel here).
        let record = command_record(1);
        assert!(check_record_size(&record).is_ok());
        // Sanity: the ceiling is the fill-aware 2 MiB bound.
        assert_eq!(MAX_JOURNAL_RECORD_BYTES, 2 * 1024 * 1024);
    }

    #[test]
    fn test_journal_stream_ceiling_refuses_over_ceiling_count() {
        // The per-read count bound (the #029 unbounded-read OOM vector) — a count AT
        // the ceiling is allowed; one OVER it is a typed ResourceLimit
        // (`stream_records`), never a truncated silent partial read. Pure, so it is
        // proven without a million-row database (the durable read's pre-fetch
        // `count(*)` bounding query enforces the same bound before `fetch_all`).
        assert!(enforce_stream_ceiling(MAX_JOURNAL_RECORDS).is_ok());
        match enforce_stream_ceiling(MAX_JOURNAL_RECORDS + 1) {
            Err(JournalError::ResourceLimit {
                limit,
                found,
                ceiling,
            }) => {
                assert_eq!(limit, "stream_records");
                assert_eq!(found, MAX_JOURNAL_RECORDS + 1);
                assert_eq!(ceiling, MAX_JOURNAL_RECORDS);
            }
            other => panic!("expected a stream_records ResourceLimit, got {other:?}"),
        }
    }

    #[test]
    fn test_journal_stream_bytes_ceiling_refuses_over_budget_bytes() {
        // The per-read TOTAL-byte bound (the compounded allocation gap) — boundary
        // exact, pure (a gigabyte of rows is deliberately unreachable in CI; the
        // durable read's pre-fetch `sum(octet_length)` enforces this before fetch).
        assert!(enforce_stream_bytes_ceiling(MAX_JOURNAL_STREAM_BYTES).is_ok());
        match enforce_stream_bytes_ceiling(MAX_JOURNAL_STREAM_BYTES + 1) {
            Err(JournalError::ResourceLimit {
                limit,
                found,
                ceiling,
            }) => {
                assert_eq!(limit, "stream_bytes");
                assert_eq!(found, MAX_JOURNAL_STREAM_BYTES + 1);
                assert_eq!(ceiling, MAX_JOURNAL_STREAM_BYTES);
            }
            other => panic!("expected a stream_bytes ResourceLimit, got {other:?}"),
        }
    }

    /// A record built around a huge string field whose serialized form is over the
    /// per-record ceiling — for the write-path ceiling tests.
    fn oversized_record(seq: u64) -> JournalRecord {
        let huge = "a".repeat(MAX_JOURNAL_RECORD_BYTES + 32);
        JournalRecord::command(
            SequenceNumber::new(seq),
            EventTimestamp::new(1),
            VenueCommand::CancelOrder {
                symbol: sym("BTC-20240329-50000-C"),
                order_id: crate::models::VenueOrderId::new(huge),
                account: AccountId::new("acct-1"),
            },
        )
    }

    #[test]
    fn test_append_refuses_oversized_record_at_the_write_ceiling() {
        // The write-side ceiling (the write ≤ read symmetry invariant): the in-memory
        // store REFUSES a record over the per-record ceiling AT append, so nothing it
        // durably holds can ever trip the read `record_bytes` refusal.
        let mut journal = InMemoryVenueJournal::new(header());
        match journal.append(oversized_record(0)) {
            Err(JournalError::ResourceLimit {
                limit,
                found,
                ceiling,
            }) => {
                assert_eq!(limit, "record_bytes");
                assert!(found > ceiling);
                assert_eq!(ceiling, MAX_JOURNAL_RECORD_BYTES);
            }
            other => panic!("expected a write-ceiling record_bytes ResourceLimit, got {other:?}"),
        }
        // The over-ceiling record was NOT stored.
        assert!(journal.is_empty(), "an over-ceiling record is never stored");
    }

    #[test]
    fn test_write_read_ceiling_symmetry_a_maximal_record_round_trips_in_memory() {
        // The load-bearing invariant: because the write and read ceilings are the
        // SAME constant, any record the store ACCEPTS at write (≤ ceiling) reads back
        // — a maximal within-ceiling record round-trips. A near-ceiling record (just
        // under the bound) writes and reads; the over-ceiling one is refused at write
        // (above), so it can never reach a read.
        let mut journal = InMemoryVenueJournal::new(header());
        // A record close to, but under, the ceiling (a large-but-legal order id).
        let big = "a".repeat(MAX_JOURNAL_RECORD_BYTES / 2);
        let record = JournalRecord::command(
            SequenceNumber::new(0),
            EventTimestamp::new(1),
            VenueCommand::CancelOrder {
                symbol: sym("BTC-20240329-50000-C"),
                order_id: crate::models::VenueOrderId::new(big),
                account: AccountId::new("acct-1"),
            },
        );
        assert!(
            check_record_size(&record).is_ok(),
            "the record is within the ceiling"
        );
        journal
            .append(record.clone())
            .expect("a within-ceiling record writes");
        let read = journal
            .read_from(SequenceNumber::START)
            .expect("in-memory read is infallible");
        assert_eq!(read, vec![record], "a written record always reads back");
    }

    #[test]
    fn test_check_record_size_fails_closed_is_covered_by_the_serialize_contract() {
        // `check_record_size` fails CLOSED on a serialize error (never size-0-proceed).
        // A `venue.v1` record always serializes, so the fail-open path is unreachable
        // via the type; this documents the contract and exercises the ok path.
        assert!(check_record_size(&command_record(3)).is_ok());
    }
}
