//! The market-maker persona-substrate configuration and its NaN-rejecting range
//! guards, plus the [`MarketMakerEvent`] broadcast vocabulary
//! ([015](../../milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md),
//! [05 §8](../../docs/05-microstructure-config.md)).
//!
//! The three knobs — `spread_multiplier`, `size_scalar`, `directional_skew` —
//! are **dimensionless `f64` multipliers**, the documented float exception (they
//! are not money). Each is validated by [`validate_control_value`], which
//! **rejects** `NaN`/`±Inf` and any value outside its exact range (rule 4) — the
//! engine leaves the config untouched on a rejection rather than silently
//! coercing the value. Personas themselves are v0.5; this is the substrate they
//! build on.

use std::collections::HashMap;

use crate::exchange::Cents;
use crate::models::{ExecutionId, VenueOrderId};

/// Minimum accepted spread multiplier.
pub const SPREAD_MULTIPLIER_MIN: f64 = 0.1;
/// Maximum accepted spread multiplier.
pub const SPREAD_MULTIPLIER_MAX: f64 = 10.0;
/// Minimum accepted size scalar.
pub const SIZE_SCALAR_MIN: f64 = 0.0;
/// Maximum accepted size scalar.
pub const SIZE_SCALAR_MAX: f64 = 1.0;
/// Minimum accepted directional skew.
pub const DIRECTIONAL_SKEW_MIN: f64 = -1.0;
/// Maximum accepted directional skew.
pub const DIRECTIONAL_SKEW_MAX: f64 = 1.0;

/// Validates a market-maker control value is finite and within `[min, max]`.
///
/// The knobs are `f64`, and `f64::clamp` returns `NaN` for a `NaN` input, so a
/// non-finite value would slip through a plain clamp and poison quoting. This is
/// the single gate: `RangeInclusive::contains` rejects `NaN` (every comparison
/// with `NaN` is false) and both infinities (outside any finite range), so one
/// containment check covers finiteness and range together.
///
/// # Errors
///
/// Returns a client-safe message naming the `field`, the accepted range, and the
/// offending value (which contains no secret) when `value` is non-finite or
/// outside `[min, max]`.
#[inline]
pub fn validate_control_value(field: &str, value: f64, min: f64, max: f64) -> Result<f64, String> {
    if (min..=max).contains(&value) {
        Ok(value)
    } else {
        Err(format!(
            "{field} must be finite and within [{min}, {max}], got {value}"
        ))
    }
}

/// The market-maker persona-substrate configuration.
///
/// The three knobs are held within their documented ranges by **rejection**: the
/// setters refuse a `NaN` or out-of-range value and leave the config unchanged
/// (rule 4), so a stored value is always finite and in range. Cheap to clone for
/// the per-requote snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketMakerConfig {
    /// Whether quoting is globally enabled (the master kill switch).
    pub enabled: bool,
    /// Global spread multiplier, within `[0.1, 10.0]` (out-of-range rejected).
    pub spread_multiplier: f64,
    /// Global size scalar, within `[0.0, 1.0]` (out-of-range rejected).
    pub size_scalar: f64,
    /// Global directional skew, within `[-1.0, 1.0]` (out-of-range rejected).
    pub directional_skew: f64,
    /// Per-symbol quoting-enabled overrides (absent ⇒ enabled).
    pub symbol_enabled: HashMap<String, bool>,
}

impl Default for MarketMakerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            spread_multiplier: 1.0,
            size_scalar: 1.0,
            directional_skew: 0.0,
            symbol_enabled: HashMap::new(),
        }
    }
}

/// The events the [`MarketMakerEngine`](crate::market_maker::MarketMakerEngine)
/// broadcasts on its bounded channel — the domain-side signal the WS `config` /
/// `quote` surfaces and observers consume.
#[derive(Debug, Clone, PartialEq)]
pub enum MarketMakerEvent {
    /// A fresh two-sided quote was generated (and its cancel/add commands routed
    /// onto the sequenced path).
    QuoteUpdated {
        /// The canonical contract symbol.
        symbol: String,
        /// The strike in **cents**.
        strike_cents: u64,
        /// Call or put (`"call"`/`"put"`).
        style: String,
        /// Bid price in **cents**.
        bid_price: Cents,
        /// Ask price in **cents**.
        ask_price: Cents,
        /// Bid size in **contracts**.
        bid_size: u64,
        /// Ask size in **contracts**.
        ask_size: u64,
    },
    /// One of the engine's resting quotes was (partially) filled.
    OrderFilled {
        /// The venue order id of the filled market-maker leg.
        order_id: VenueOrderId,
        /// The execution id of the fill (cross-surface join key), when known.
        execution_id: Option<ExecutionId>,
        /// The canonical contract symbol.
        symbol: String,
        /// Order side (`"buy"`/`"sell"`).
        side: String,
        /// Filled quantity in **contracts**.
        quantity: u64,
        /// Fill price in **cents**.
        price: Cents,
        /// Captured edge in **cents per contract**, against the quote-time theo.
        edge: i64,
    },
    /// The configuration changed (a knob update or the kill switch).
    ConfigChanged {
        /// Whether quoting is enabled.
        enabled: bool,
        /// Spread multiplier.
        spread_multiplier: f64,
        /// Size scalar.
        size_scalar: f64,
        /// Directional skew.
        directional_skew: f64,
    },
    /// An underlying price was updated.
    PriceUpdated {
        /// The underlying ticker.
        symbol: String,
        /// The price in **cents**.
        price_cents: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_control_value_accepts_in_range_and_bounds() {
        assert_eq!(validate_control_value("spread", 2.0, 0.1, 10.0), Ok(2.0));
        assert_eq!(validate_control_value("size", 0.0, 0.0, 1.0), Ok(0.0));
        assert_eq!(validate_control_value("size", 1.0, 0.0, 1.0), Ok(1.0));
        assert_eq!(validate_control_value("skew", -1.0, -1.0, 1.0), Ok(-1.0));
    }

    #[test]
    fn test_validate_control_value_rejects_non_finite() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = validate_control_value("spread_multiplier", bad, 0.1, 10.0)
                .expect_err("non-finite must be rejected");
            assert!(err.contains("spread_multiplier"));
            assert!(err.contains("must be finite and within"));
        }
    }

    #[test]
    fn test_validate_control_value_rejects_out_of_range() {
        assert!(validate_control_value("spread_multiplier", 0.05, 0.1, 10.0).is_err());
        assert!(validate_control_value("spread_multiplier", 10.5, 0.1, 10.0).is_err());
        assert!(validate_control_value("directional_skew", -1.5, -1.0, 1.0).is_err());
        assert!(validate_control_value("directional_skew", 1.5, -1.0, 1.0).is_err());
    }

    #[test]
    fn test_default_config_is_enabled_and_neutral() {
        let config = MarketMakerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.spread_multiplier, 1.0);
        assert_eq!(config.size_scalar, 1.0);
        assert_eq!(config.directional_skew, 0.0);
        assert!(config.symbol_enabled.is_empty());
    }
}
