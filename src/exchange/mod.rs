//! Domain: the venue core — the sequenced order path onto the upstream
//! `option-chain-orderbook` matching stack, sequencer/journal wiring,
//! snapshot/restore, and the executions and positions stores.
//!
//! This first slice lands the load-bearing domain types that every DTO,
//! journal envelope, and FIX field will consume ([01 §3–§5, §9](../../docs/01-domain-model.md)):
//!
//! - [`money`] — the integer-cents newtypes [`Cents`] / [`SignedCents`] /
//!   [`Notional`] with checked arithmetic.
//! - [`boundary`] — the upstream matching-seam newtypes re-exported (never
//!   redefined): [`OrderId`], [`Side`], [`Price`], [`Quantity`],
//!   [`TimeInForce`], [`STPMode`], [`OptionStyle`], [`ExpirationDate`],
//!   [`TimestampMs`], [`Hash32`], [`InstrumentStatus`], plus the [`SymbolParser`]
//!   grammar.
//! - [`event`] — the venue-owned [`EventTimestamp`] and [`SequenceNumber`].
//! - [`symbol`] — the [`Symbol`] grammar and the venue-expiry replay invariant.
//! - [`instrument`] — the [`Instrument`] value object.
//! - [`identity`] — the run [`LineageId`], the deterministic composite-id
//!   grammar, and the [`JournalHeader`].
//! - [`envelope`] — the versioned [`VenueCommand`] / [`VenueEvent`] v1 envelope,
//!   the lossless [`VenueOutcome`] shapes, and the internal [`Fill`] projection.
//! - [`journal`] — the venue's append-only, write-ahead command/event journal
//!   ([`VenueJournal`] / [`InMemoryVenueJournal`] / [`JournalRecord`]), named to
//!   match the upstream `OptionChainJournal` shape so the durable PostgreSQL store
//!   ([`crate::db::PgVenueJournal`], #029) swaps in behind the **same** contract.
//! - [`recovery`] — recovery-as-re-execution ([`recover`]): rebuild a per-underlying
//!   book from any [`VenueJournal`] (in-memory or durable) by re-executing every
//!   journaled command in `N` order with the stored [`VenueEvent`] as the integrity
//!   oracle, refusing a newer-than-binary schema and halting on corruption.
//! - [`actor`] — the per-underlying **single-writer actor**
//!   ([`UnderlyingActor`] / [`ActorHandle`] / [`spawn_underlying_actor`]): the
//!   bounded mailbox, the venue-owned checked sequence counter, and the
//!   write-ahead durability protocol every book mutation flows through. The
//!   fan-out seam ([`FanOut`]) is filled by #008.
//! - [`executor`] — the real [`CommandExecutor`] ([`MatchingExecutor`]): routes
//!   `AddOrder` / `CancelOrder` / `Replace` / market orders onto the upstream
//!   `option-chain-orderbook` matching **unchanged** and captures the lossless
//!   [`VenueOutcome`] (two-leg fills, resting remainder, STP removals), with the
//!   ergonomic [`spawn_matching_actor`] wiring it into the actor.
//! - [`stores`] — the in-memory executions and positions stores behind the
//!   backend-agnostic [`ExecutionsStore`] / [`PositionsStore`] contract, and the
//!   [`StoreFanOut`] that fills the actor's [`FanOut`] seam: each committed
//!   [`VenueEvent`] fill leg becomes an authoritative
//!   [`ExecutionRecord`](crate::ExecutionRecord) and folds into a
//!   per-`(account, symbol)` [`Position`](crate::Position), marked live-only
//!   against the upstream [`MarkPriceBook`].
//!
//! The **live boot-time replay driver** (reload a snapshot + re-execute into a
//! running `AppState`) lands with #030; the envelope types remain **pure data**.
//!
//! Governed by `docs/02-matching-architecture.md` and `docs/01-domain-model.md`.

pub mod actor;
pub mod boundary;
pub mod envelope;
pub mod event;
pub mod executor;
pub mod identity;
pub mod instrument;
pub mod journal;
pub mod mm_identity;
pub mod money;
pub mod recovery;
pub mod snapshot;
pub mod stores;
pub mod symbol;

pub use self::actor::{
    ActorConfig, ActorHandle, CommandExecutor, ExecutionContext, FanOut, FixedClock,
    JournalSnapshot, NoopFanOut, PlaceholderExecutor, Receipt, TeeFanOut, UnderlyingActor,
    VenueClock, spawn_underlying_actor,
};
pub use self::boundary::{
    ExpirationDate, Hash32, InstrumentStatus, OptionStyle, OrderId, ParsedSymbol, Price, Quantity,
    STPMode, Side, SymbolParser, TimeInForce, TimestampMs,
};
pub use self::envelope::{
    AddOutcome, CancelReason, CancelledLeg, Fill, MassCancelScope, MassCancelType, VenueCommand,
    VenueEvent, VenueOutcome,
};
pub use self::event::{EventTimestamp, SequenceNumber};
pub use self::executor::{
    MatchingExecutor, PreparedRestore, TopOfBook, spawn_matching_actor,
    spawn_matching_actor_with_registry_and_index,
};
pub use self::identity::{JournalHeader, LineageId, VENUE_ENVELOPE_SCHEMA};
pub use self::instrument::Instrument;
pub use self::journal::{
    InMemoryVenueJournal, JournalCommand, JournalError, JournalRecord, MAX_JOURNAL_RECORD_BYTES,
    MAX_JOURNAL_RECORDS, MAX_JOURNAL_STREAM_BYTES, RecordKind, SnapshotRestored, VenueJournal,
    check_record_size, decode_journal_record, enforce_stream_bytes_ceiling, enforce_stream_ceiling,
};
pub use self::mm_identity::{
    MARKET_MAKER_ACCOUNT, MARKET_MAKER_OWNER, is_market_maker_account, is_market_maker_command,
    market_maker_account,
};
pub use self::money::{Cents, MoneyError, Notional, SignedCents};
pub(crate) use self::recovery::check_price_band;
pub use self::recovery::{Recovered, recover, recover_with_microstructure};
pub use self::snapshot::{
    ExecutionCapture, ExecutorState, IdempotencyEntry, IdempotencyFingerprint, IdempotencyKey,
    IdempotencyMap, IdempotencyRecord, PositionCapture, RestingOrderCapture, SnapshotError,
    SnapshotMetadata, VenueSnapshot,
};
pub use self::stores::{
    ExecutionFilter, ExecutionsStore, InMemoryExecutionsStore, InMemoryPositionsStore,
    MarkPriceBook, MarkSource, NoMarks, PositionLeg, PositionsStore, StoreError, StoreFanOut,
};
pub use self::symbol::{Symbol, SymbolError, validate_venue_expiry};
