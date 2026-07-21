//! The canonical symbol grammar and the venue-expiry replay invariant.
//!
//! A single contract is named by the canonical symbol
//! `UNDERLYING-YYYYMMDD-STRIKE-STYLE` (e.g. `BTC-20240329-50000-C`). The symbol
//! is the venue's wire and journal identity, and `fauxchange` **never
//! hand-parses it**: every parse routes through the upstream [`SymbolParser`],
//! the single source of truth, so the parsed expiry instant and strike key
//! match whatever created the chain ([01 §5](../../../docs/01-domain-model.md)).
//!
//! The `SymbolParser` maps the `YYYYMMDD` date to the canonical **`23:59:59 UTC`**
//! instant of that day (verified `option-chain-orderbook` v0.7.0). Because the
//! symbol carries no time-of-day, two `ExpirationDate::DateTime` values on the
//! same calendar day would collide on one symbol. The venue therefore enforces
//! two invariants through [`validate_venue_expiry`]:
//!
//! - every expiry is `ExpirationDate::DateTime` — a relative `Days` expiry is
//!   wall-clock-relative and breaks replay, so it is **refused**; and
//! - the instant must be the canonical `23:59:59 UTC` for its date — a
//!   different time-of-day would silently alias a second contract, so it is a
//!   config error, not a hidden duplicate.

use std::fmt;

use option_chain_orderbook::utils::format_expiration_yyyymmdd;
use option_chain_orderbook::{ParsedSymbol, SymbolParser};
use optionstratlib::ExpirationDate;
use serde::{Deserialize, Serialize};

/// Error raised by symbol parsing and venue-expiry validation.
///
/// A crate-wide `VenueError` lands in a later issue; until then this local error
/// maps as follows ([01 §5, §11](../../../docs/01-domain-model.md)):
/// [`SymbolError::InvalidSymbol`] → `VenueError::InvalidOrder` (HTTP `400`),
/// while the expiry variants are startup `ConfigError`s (an invalid venue
/// configuration, refused before any order is admitted).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SymbolError {
    /// The symbol string did not match `UNDERLYING-YYYYMMDD-STRIKE-STYLE`. The
    /// offending symbol and the upstream reason are carried for diagnostics.
    #[error("invalid symbol '{symbol}': {reason}")]
    InvalidSymbol {
        /// The symbol string that failed to parse.
        symbol: String,
        /// The upstream `SymbolParser` rejection reason.
        reason: String,
    },
    /// A relative `ExpirationDate::Days` expiry was supplied on the venue path.
    /// It is wall-clock-relative and would map to a different calendar date on
    /// replay, so the venue requires an absolute `ExpirationDate::DateTime`.
    #[error(
        "relative Days expiry is refused on the venue path; use an absolute ExpirationDate::DateTime"
    )]
    RelativeExpiryRefused,
    /// An absolute expiry whose time-of-day is not the canonical `23:59:59 UTC`
    /// for its date. It would format to the same `YYYYMMDD` symbol as the
    /// canonical contract and silently alias it, so it is rejected.
    #[error(
        "expiry time-of-day for date {date} is not canonical 23:59:59 UTC; it would alias the symbol"
    )]
    NonCanonicalExpiryInstant {
        /// The `YYYYMMDD` date whose canonical instant was violated.
        date: String,
    },
    /// The expiry date could not be resolved or formatted through the upstream
    /// grammar. Carries the upstream reason.
    #[error("could not resolve expiry date: {reason}")]
    UnresolvableExpiry {
        /// The upstream reason for the resolution failure.
        reason: String,
    },
}

/// A validated canonical venue symbol: `UNDERLYING-YYYYMMDD-STRIKE-STYLE`.
///
/// The inner `String` is private and always the canonical, normalized form
/// produced by the upstream [`SymbolParser`] (e.g. a lowercase `c` style is
/// normalized to `C`). Constructing a `Symbol` guarantees it re-parses to the
/// same value, so it is safe to use as a map key and wire identity.
///
/// On the wire it is a bare JSON string (`#[serde(try_from = "String", into =
/// "String")]`), and deserialisation re-validates through `SymbolParser` — an
/// invalid string cannot bypass the grammar.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Symbol(String);

impl Symbol {
    /// Parses a canonical symbol string through the upstream [`SymbolParser`].
    ///
    /// The stored value is the normalized canonical form, so
    /// `Symbol::parse(s)?.as_str()` re-parses to itself.
    ///
    /// # Errors
    ///
    /// Returns [`SymbolError::InvalidSymbol`] if `raw` is not a valid
    /// `UNDERLYING-YYYYMMDD-STRIKE-STYLE` symbol (wrong shape, empty
    /// underlying, non-date, non-positive or non-numeric strike, or an option
    /// style other than `C`/`P`).
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::Symbol;
    /// let s = Symbol::parse("BTC-20240329-50000-C")?;
    /// assert_eq!(s.as_str(), "BTC-20240329-50000-C");
    /// // A lowercase style normalizes to the canonical uppercase form.
    /// assert_eq!(Symbol::parse("ETH-20251222-3000-p")?.as_str(), "ETH-20251222-3000-P");
    /// # Ok::<(), fauxchange::exchange::SymbolError>(())
    /// ```
    #[inline]
    pub fn parse(raw: &str) -> Result<Self, SymbolError> {
        let parsed = SymbolParser::parse(raw).map_err(|e| SymbolError::InvalidSymbol {
            symbol: raw.to_string(),
            reason: e.to_string(),
        })?;
        Ok(Self::from_parsed(&parsed))
    }

    /// Builds a `Symbol` from an already-validated upstream [`ParsedSymbol`],
    /// without re-parsing.
    #[inline]
    pub(crate) fn from_parsed(parsed: &ParsedSymbol) -> Self {
        Self(parsed.to_symbol())
    }

    /// Returns the canonical symbol string.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for Symbol {
    type Error = SymbolError;

    #[inline]
    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<Symbol> for String {
    #[inline]
    fn from(symbol: Symbol) -> Self {
        symbol.0
    }
}

/// Validates a configured expiry against the venue's replay-stability
/// invariant, returning the canonical `ExpirationDate::DateTime`.
///
/// The venue admits only absolute, canonical-instant expiries so that a symbol
/// round-trips and replay is deterministic
/// ([01 §5](../../../docs/01-domain-model.md)):
///
/// - `ExpirationDate::Days(_)` is refused (it is wall-clock-relative); and
/// - an `ExpirationDate::DateTime` must sit at the canonical `23:59:59 UTC`
///   instant of its date, or it would alias the canonical symbol for that day.
///
/// # Errors
///
/// - [`SymbolError::RelativeExpiryRefused`] if `expiration` is a `Days` expiry.
/// - [`SymbolError::NonCanonicalExpiryInstant`] if the instant is not the
///   canonical `23:59:59 UTC` for its date (the aliasing rule).
/// - [`SymbolError::UnresolvableExpiry`] if the date cannot be resolved or
///   formatted through the upstream grammar.
///
/// # Examples
///
/// ```
/// use fauxchange::exchange::{validate_venue_expiry, SymbolParser};
/// // `parse_yyyymmdd` yields the canonical 23:59:59 UTC instant, so it validates.
/// let canonical = SymbolParser::parse_yyyymmdd("20240329", "")?;
/// let validated = validate_venue_expiry(&canonical)?;
/// assert_eq!(validated, canonical);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[inline]
pub fn validate_venue_expiry(expiration: &ExpirationDate) -> Result<ExpirationDate, SymbolError> {
    let instant = match expiration {
        ExpirationDate::Days(_) => return Err(SymbolError::RelativeExpiryRefused),
        ExpirationDate::DateTime(instant) => instant,
    };

    let yyyymmdd =
        format_expiration_yyyymmdd(expiration).map_err(|e| SymbolError::UnresolvableExpiry {
            reason: e.to_string(),
        })?;

    // Route the date back through the single source of truth: it always yields
    // the canonical 23:59:59 UTC instant for that day.
    let canonical = SymbolParser::parse_yyyymmdd(&yyyymmdd, "").map_err(|e| {
        SymbolError::UnresolvableExpiry {
            reason: e.to_string(),
        }
    })?;

    match &canonical {
        // Compare the concrete instants (chrono `DateTime<Utc>` equality is
        // exact) rather than `ExpirationDate` equality, which is a fuzzy,
        // wall-clock-relative day-count comparison upstream.
        ExpirationDate::DateTime(canon) if instant == canon => Ok(canonical),
        ExpirationDate::DateTime(_) => {
            Err(SymbolError::NonCanonicalExpiryInstant { date: yyyymmdd })
        }
        // `parse_yyyymmdd` always returns a `DateTime`; this arm is defensive.
        ExpirationDate::Days(_) => Err(SymbolError::UnresolvableExpiry {
            reason: "canonical parse did not yield an absolute instant".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_parse_accepts_canonical_call() {
        match Symbol::parse("BTC-20240329-50000-C") {
            Ok(s) => assert_eq!(s.as_str(), "BTC-20240329-50000-C"),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }

    #[test]
    fn test_symbol_parse_normalizes_lowercase_style() {
        match Symbol::parse("ETH-20251222-3000-p") {
            Ok(s) => assert_eq!(s.as_str(), "ETH-20251222-3000-P"),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }

    #[test]
    fn test_symbol_parse_rejects_wrong_part_count() {
        match Symbol::parse("BTC-20240329-50000") {
            Err(SymbolError::InvalidSymbol { symbol, .. }) => {
                assert_eq!(symbol, "BTC-20240329-50000")
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }
    }

    #[test]
    fn test_symbol_parse_rejects_bad_style() {
        match Symbol::parse("BTC-20240329-50000-X") {
            Err(SymbolError::InvalidSymbol { .. }) => {}
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }
    }

    #[test]
    fn test_symbol_parse_rejects_zero_strike() {
        match Symbol::parse("BTC-20240329-0-C") {
            Err(SymbolError::InvalidSymbol { .. }) => {}
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }
    }

    #[test]
    fn test_symbol_display_matches_canonical_string() {
        let s = match Symbol::parse("AAPL-20240119-190-P") {
            Ok(s) => s,
            Err(e) => panic!("expected Ok, got {e:?}"),
        };
        assert_eq!(s.to_string(), "AAPL-20240119-190-P");
    }

    #[test]
    fn test_symbol_serde_roundtrips_as_bare_string() {
        let s = match Symbol::parse("BTC-20240329-50000-C") {
            Ok(s) => s,
            Err(e) => panic!("expected Ok, got {e:?}"),
        };
        let json = match serde_json::to_string(&s) {
            Ok(j) => j,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(json, "\"BTC-20240329-50000-C\"");
        match serde_json::from_str::<Symbol>(&json) {
            Ok(back) => assert_eq!(back, s),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }

    #[test]
    fn test_symbol_deserialize_rejects_invalid_string() {
        match serde_json::from_str::<Symbol>("\"not-a-symbol\"") {
            Err(_) => {}
            Ok(s) => panic!("expected deserialize error, parsed {s:?}"),
        }
    }

    #[test]
    fn test_validate_venue_expiry_accepts_canonical_instant() {
        let canonical = match SymbolParser::parse_yyyymmdd("20240329", "") {
            Ok(e) => e,
            Err(e) => panic!("parse_yyyymmdd failed: {e}"),
        };
        match validate_venue_expiry(&canonical) {
            Ok(validated) => assert_eq!(validated, canonical),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }

    #[test]
    fn test_validate_venue_expiry_refuses_days() {
        // `from_string` of a bare number yields a relative `Days` expiry.
        let days = match ExpirationDate::from_string("30") {
            Ok(e) => e,
            Err(e) => panic!("from_string failed: {e}"),
        };
        match validate_venue_expiry(&days) {
            Err(SymbolError::RelativeExpiryRefused) => {}
            other => panic!("expected RelativeExpiryRefused, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_venue_expiry_rejects_non_canonical_time_of_day() {
        // Same calendar day as the canonical instant, but 14:30 UTC instead of
        // 23:59:59 UTC — this would alias the symbol for 2024-03-29.
        let midday = match ExpirationDate::from_string("2024-03-29T14:30:00Z") {
            Ok(e) => e,
            Err(e) => panic!("from_string failed: {e}"),
        };
        match validate_venue_expiry(&midday) {
            Err(SymbolError::NonCanonicalExpiryInstant { date }) => assert_eq!(date, "20240329"),
            other => panic!("expected NonCanonicalExpiryInstant, got {other:?}"),
        }
    }

    #[test]
    fn test_symbol_roundtrip_parse_format_expiration_equals_input() {
        // parse(format(dt)) == dt exactly for a canonical-instant expiry.
        let dt = match SymbolParser::parse_yyyymmdd("20240329", "") {
            Ok(e) => e,
            Err(e) => panic!("parse_yyyymmdd failed: {e}"),
        };
        let yyyymmdd = match format_expiration_yyyymmdd(&dt) {
            Ok(s) => s,
            Err(e) => panic!("format failed: {e}"),
        };
        let reparsed = match SymbolParser::parse_yyyymmdd(&yyyymmdd, "") {
            Ok(e) => e,
            Err(e) => panic!("reparse failed: {e}"),
        };
        assert_eq!(reparsed, dt);
    }
}
