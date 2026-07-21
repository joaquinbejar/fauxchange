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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
