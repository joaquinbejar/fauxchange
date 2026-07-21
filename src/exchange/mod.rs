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
//!   [`TimeInForce`], [`OptionStyle`], [`ExpirationDate`], [`TimestampMs`],
//!   [`Hash32`], [`InstrumentStatus`], plus the [`SymbolParser`] grammar.
//! - [`event`] — the venue-owned [`EventTimestamp`] and [`SequenceNumber`].
//! - [`symbol`] — the [`Symbol`] grammar and the venue-expiry replay invariant.
//! - [`instrument`] — the [`Instrument`] value object.
//!
//! The sequencer, journal, snapshot/restore, and store wiring land in later
//! issues.
//!
//! Governed by `docs/02-matching-architecture.md` and `docs/01-domain-model.md`.

pub mod boundary;
pub mod event;
pub mod instrument;
pub mod money;
pub mod symbol;

pub use self::boundary::{
    ExpirationDate, Hash32, InstrumentStatus, OptionStyle, OrderId, ParsedSymbol, Price, Quantity,
    Side, SymbolParser, TimeInForce, TimestampMs,
};
pub use self::event::{EventTimestamp, SequenceNumber};
pub use self::instrument::Instrument;
pub use self::money::{Cents, MoneyError, Notional, SignedCents};
pub use self::symbol::{Symbol, SymbolError, validate_venue_expiry};
