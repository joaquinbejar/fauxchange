//! The `Instrument` value object.
//!
//! An `Instrument` is a thin, validated projection over the upstream option-chain
//! coordinates (`Underlying → Expiration → Strike → OptionOrderBook`); the leaf
//! book itself is owned upstream ([01 §5](../../../docs/01-domain-model.md)). It
//! is constructed only through [`Instrument::try_new`], which enforces the venue
//! invariants at the seam:
//!
//! - the expiry is validated by [`validate_venue_expiry`] — an absolute,
//!   canonical `23:59:59 UTC` `ExpirationDate::DateTime`, never a `Days` expiry;
//!   and
//! - the symbol is built through the upstream `SymbolParser` grammar (via
//!   [`ParsedSymbol`]), so the symbol, strike, and style are mutually
//!   consistent and the underlying is non-empty with a positive strike.
//!
//! Fields are private so an instrument that skipped validation cannot be
//! constructed by a struct literal; read them through the accessors.

use option_chain_orderbook::utils::format_expiration_yyyymmdd;
use option_chain_orderbook::{InstrumentStatus, ParsedSymbol};
use optionstratlib::{ExpirationDate, OptionStyle};
use serde::{Deserialize, Serialize};

use crate::exchange::symbol::{Symbol, SymbolError, validate_venue_expiry};

/// A resolved venue instrument identified by its canonical symbol.
///
/// Equality is by value. Construct through [`Instrument::try_new`]; the fields
/// are private to keep the canonical-expiry and symbol-consistency invariants.
///
/// Deserialisation routes through [`Instrument::try_new`] via
/// [`InstrumentWire`] (`#[serde(try_from = ...)]`), so a persisted/config record
/// is **re-validated** on decode: a relative `ExpirationDate::Days` expiry, a
/// non-canonical instant, or a `symbol` that disagrees with its
/// `underlying`/`expiration`/`strike`/`style` coordinates is a hard decode error,
/// not a silently-admitted mismatch that would break replay determinism. The
/// serialised (wire) form is unchanged — all six fields are still emitted — so a
/// valid instrument round-trips byte-for-byte.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "InstrumentWire")]
pub struct Instrument {
    /// Canonical symbol: `UNDERLYING-YYYYMMDD-STRIKE-STYLE`.
    symbol: Symbol,
    /// Underlying ticker (e.g. `BTC`), as encoded in the symbol.
    underlying: String,
    /// Absolute expiry instant — canonical `23:59:59 UTC` `DateTime` only.
    expiration: ExpirationDate,
    /// Strike in whole units (the leaf book keys strikes as integers).
    strike: u64,
    /// Call or put.
    style: OptionStyle,
    /// Upstream lifecycle status (`Active`, `Halted`, `Settling`, `Expired`).
    status: InstrumentStatus,
}

impl Instrument {
    /// Builds a validated `Instrument` from its coordinates.
    ///
    /// The expiry is canonicalized and checked ([`validate_venue_expiry`]), and
    /// the symbol is minted through the upstream `SymbolParser` grammar so the
    /// symbol, strike, and style stay mutually consistent.
    ///
    /// # Errors
    ///
    /// - [`SymbolError::RelativeExpiryRefused`] if `expiration` is a `Days`
    ///   expiry.
    /// - [`SymbolError::NonCanonicalExpiryInstant`] if the expiry instant is not
    ///   the canonical `23:59:59 UTC` for its date (the aliasing rule).
    /// - [`SymbolError::InvalidSymbol`] if `underlying` is empty or `strike` is
    ///   zero (rejected by the upstream grammar).
    /// - [`SymbolError::UnresolvableExpiry`] if the expiry date cannot be
    ///   resolved or formatted.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::{Instrument, InstrumentStatus, OptionStyle, SymbolParser};
    /// let expiry = SymbolParser::parse_yyyymmdd("20240329", "")?;
    /// let inst = Instrument::try_new("BTC", expiry, 50_000, OptionStyle::Call, InstrumentStatus::Active)?;
    /// assert_eq!(inst.symbol().as_str(), "BTC-20240329-50000-C");
    /// assert_eq!(inst.strike(), 50_000);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn try_new(
        underlying: &str,
        expiration: ExpirationDate,
        strike: u64,
        style: OptionStyle,
        status: InstrumentStatus,
    ) -> Result<Self, SymbolError> {
        let canonical = validate_venue_expiry(&expiration)?;
        let yyyymmdd = format_expiration_yyyymmdd(&canonical).map_err(|e| {
            SymbolError::UnresolvableExpiry {
                reason: e.to_string(),
            }
        })?;

        let style_char = match style {
            OptionStyle::Call => 'C',
            OptionStyle::Put => 'P',
        };
        let parsed = ParsedSymbol::try_new(underlying, &yyyymmdd, strike, style).map_err(|e| {
            SymbolError::InvalidSymbol {
                symbol: format!("{underlying}-{yyyymmdd}-{strike}-{style_char}"),
                reason: e.to_string(),
            }
        })?;

        Ok(Self {
            symbol: Symbol::from_parsed(&parsed),
            underlying: parsed.underlying().to_string(),
            expiration: canonical,
            strike: parsed.strike(),
            style: parsed.option_style(),
            status,
        })
    }

    /// Returns the canonical symbol.
    #[must_use]
    #[inline]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// Returns the underlying ticker.
    #[must_use]
    #[inline]
    pub fn underlying(&self) -> &str {
        &self.underlying
    }

    /// Returns the absolute expiry instant (canonical `DateTime`).
    // `ExpirationDate` is itself `#[must_use]`, so no attribute here
    // (clippy::double_must_use).
    #[inline]
    pub const fn expiration(&self) -> ExpirationDate {
        self.expiration
    }

    /// Returns the strike in whole units.
    #[must_use]
    #[inline]
    pub const fn strike(&self) -> u64 {
        self.strike
    }

    /// Returns the option style (call or put).
    #[must_use]
    #[inline]
    pub const fn style(&self) -> OptionStyle {
        self.style
    }

    /// Returns the upstream lifecycle status.
    #[must_use]
    #[inline]
    pub const fn status(&self) -> InstrumentStatus {
        self.status
    }
}

/// The plain, field-for-field decode mirror of [`Instrument`] — the `from`-type
/// of its `#[serde(try_from = ...)]` deserialisation.
///
/// It is deserialised leniently (each field by its own type; [`Symbol`] already
/// re-validates through the upstream grammar) and then handed to
/// [`Instrument::try_from`], which re-runs the venue invariants so a decoded
/// record can never skip [`Instrument::try_new`]. It is deliberately **not**
/// public: it exists only as the serde seam.
#[derive(Deserialize)]
struct InstrumentWire {
    symbol: Symbol,
    underlying: String,
    expiration: ExpirationDate,
    strike: u64,
    style: OptionStyle,
    status: InstrumentStatus,
}

impl TryFrom<InstrumentWire> for Instrument {
    type Error = SymbolError;

    /// Rebuilds a validated [`Instrument`] from a decoded [`InstrumentWire`],
    /// enforcing the venue invariants the derived field-by-field decode would
    /// otherwise bypass.
    ///
    /// The coordinates are rebuilt through [`Instrument::try_new`] (which refuses
    /// a `Days` / non-canonical expiry and an empty underlying / zero strike), and
    /// the decoded `symbol` must equal the canonical symbol those coordinates
    /// mint — the symbol encodes `underlying`/`date`/`strike`/`style`, so this one
    /// comparison rejects any cross-coordinate mismatch. The returned instrument is
    /// the canonical rebuild, never the raw wire struct.
    fn try_from(wire: InstrumentWire) -> Result<Self, Self::Error> {
        let rebuilt = Self::try_new(
            &wire.underlying,
            wire.expiration,
            wire.strike,
            wire.style,
            wire.status,
        )?;
        if rebuilt.symbol != wire.symbol {
            return Err(SymbolError::InvalidSymbol {
                symbol: String::from(wire.symbol),
                reason: format!(
                    "symbol does not match its coordinates (canonical form is '{}')",
                    rebuilt.symbol.as_str()
                ),
            });
        }
        Ok(rebuilt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use option_chain_orderbook::SymbolParser;

    fn canonical_expiry(yyyymmdd: &str) -> ExpirationDate {
        match SymbolParser::parse_yyyymmdd(yyyymmdd, "") {
            Ok(e) => e,
            Err(e) => panic!("parse_yyyymmdd failed: {e}"),
        }
    }

    #[test]
    fn test_instrument_try_new_builds_canonical_symbol() {
        let inst = match Instrument::try_new(
            "BTC",
            canonical_expiry("20240329"),
            50_000,
            OptionStyle::Call,
            InstrumentStatus::Active,
        ) {
            Ok(i) => i,
            Err(e) => panic!("expected Ok, got {e:?}"),
        };
        assert_eq!(inst.symbol().as_str(), "BTC-20240329-50000-C");
        assert_eq!(inst.underlying(), "BTC");
        assert_eq!(inst.strike(), 50_000);
        assert_eq!(inst.style(), OptionStyle::Call);
        assert_eq!(inst.status(), InstrumentStatus::Active);
    }

    #[test]
    fn test_instrument_try_new_refuses_days_expiry() {
        let days = match ExpirationDate::from_string("30") {
            Ok(e) => e,
            Err(e) => panic!("from_string failed: {e}"),
        };
        match Instrument::try_new(
            "BTC",
            days,
            50_000,
            OptionStyle::Put,
            InstrumentStatus::Active,
        ) {
            Err(SymbolError::RelativeExpiryRefused) => {}
            other => panic!("expected RelativeExpiryRefused, got {other:?}"),
        }
    }

    #[test]
    fn test_instrument_try_new_rejects_non_canonical_expiry() {
        let midday = match ExpirationDate::from_string("2024-03-29T14:30:00Z") {
            Ok(e) => e,
            Err(e) => panic!("from_string failed: {e}"),
        };
        match Instrument::try_new(
            "BTC",
            midday,
            50_000,
            OptionStyle::Call,
            InstrumentStatus::Active,
        ) {
            Err(SymbolError::NonCanonicalExpiryInstant { date }) => assert_eq!(date, "20240329"),
            other => panic!("expected NonCanonicalExpiryInstant, got {other:?}"),
        }
    }

    #[test]
    fn test_instrument_try_new_rejects_zero_strike() {
        match Instrument::try_new(
            "BTC",
            canonical_expiry("20240329"),
            0,
            OptionStyle::Call,
            InstrumentStatus::Active,
        ) {
            Err(SymbolError::InvalidSymbol { .. }) => {}
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }
    }

    #[test]
    fn test_instrument_try_new_rejects_empty_underlying() {
        match Instrument::try_new(
            "",
            canonical_expiry("20240329"),
            50_000,
            OptionStyle::Call,
            InstrumentStatus::Active,
        ) {
            Err(SymbolError::InvalidSymbol { .. }) => {}
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }
    }

    #[test]
    fn test_instrument_deserialize_rejects_days_expiry() {
        // A serialized instrument whose `expiration` is a relative `Days` expiry
        // must fail to decode: `Days` is wall-clock-relative and breaks replay, so
        // the venue is `DateTime`-only. The derived field-by-field decode would
        // have admitted it; routing through `try_new` refuses it.
        let inst = match Instrument::try_new(
            "BTC",
            canonical_expiry("20240329"),
            50_000,
            OptionStyle::Call,
            InstrumentStatus::Active,
        ) {
            Ok(i) => i,
            Err(e) => panic!("expected Ok, got {e:?}"),
        };
        let mut value = match serde_json::to_value(&inst) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        let days = match ExpirationDate::from_string("30") {
            Ok(e) => e,
            Err(e) => panic!("from_string failed: {e}"),
        };
        value["expiration"] = match serde_json::to_value(days) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        match serde_json::from_value::<Instrument>(value) {
            Err(_) => {}
            Ok(back) => panic!("expected a Days-expiry decode error, parsed {back:?}"),
        }
    }

    #[test]
    fn test_instrument_deserialize_rejects_coordinate_mismatch() {
        // A serialized instrument whose `strike` no longer matches its canonical
        // `symbol` is internally inconsistent and must fail to decode, rather than
        // silently admit a mismatched contract identity.
        let inst = match Instrument::try_new(
            "BTC",
            canonical_expiry("20240329"),
            50_000,
            OptionStyle::Call,
            InstrumentStatus::Active,
        ) {
            Ok(i) => i,
            Err(e) => panic!("expected Ok, got {e:?}"),
        };
        let mut value = match serde_json::to_value(&inst) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        // The symbol still says 50000, but the strike coordinate is tampered.
        value["strike"] = serde_json::json!(99_999);
        match serde_json::from_value::<Instrument>(value) {
            Err(_) => {}
            Ok(back) => panic!("expected a coordinate-mismatch decode error, parsed {back:?}"),
        }
    }

    #[test]
    fn test_instrument_serde_roundtrip_preserves_value() {
        let inst = match Instrument::try_new(
            "ETH",
            canonical_expiry("20251222"),
            3_000,
            OptionStyle::Put,
            InstrumentStatus::Active,
        ) {
            Ok(i) => i,
            Err(e) => panic!("expected Ok, got {e:?}"),
        };
        let json = match serde_json::to_string(&inst) {
            Ok(j) => j,
            Err(e) => panic!("serialize failed: {e}"),
        };
        match serde_json::from_str::<Instrument>(&json) {
            Ok(back) => assert_eq!(back, inst),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }
}
