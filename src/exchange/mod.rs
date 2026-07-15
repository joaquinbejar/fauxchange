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
//!
//! The single-writer actor, journal store, snapshot/restore, and store wiring
//! land in later issues; the envelope types here are **pure data**.
//!
//! Governed by `docs/02-matching-architecture.md` and `docs/01-domain-model.md`.

pub mod boundary;
pub mod envelope;
pub mod event;
pub mod identity;
pub mod instrument;
pub mod money;
pub mod symbol;

pub use self::boundary::{
    ExpirationDate, Hash32, InstrumentStatus, OptionStyle, OrderId, ParsedSymbol, Price, Quantity,
    STPMode, Side, SymbolParser, TimeInForce, TimestampMs,
};
pub use self::envelope::{
    AddOutcome, CancelReason, CancelledLeg, Fill, MassCancelScope, MassCancelType, VenueCommand,
    VenueEvent, VenueOutcome,
};
pub use self::event::{EventTimestamp, SequenceNumber};
pub use self::identity::{JournalHeader, LineageId, VENUE_ENVELOPE_SCHEMA};
pub use self::instrument::Instrument;
pub use self::money::{Cents, MoneyError, Notional, SignedCents};
pub use self::symbol::{Symbol, SymbolError, validate_venue_expiry};
