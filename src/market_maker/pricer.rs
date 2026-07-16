//! Option pricing for market making — the Black-Scholes theoretical value and
//! the first-order Greeks, computed **entirely through `optionstratlib`**
//! ([015](../../milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md),
//! CLAUDE.md *`optionstratlib` for options math*).
//!
//! ## Why this is not the Backend's hand-rolled pricer
//!
//! The upstream `option-chain-orderbook-backend` `OptionPricer` *documented*
//! itself as a "Black-Scholes approximation via `optionstratlib`" but in fact
//! hand-rolled `d1`/`d2`, a `norm_cdf`/`erf` approximation, and every Greek in
//! `f64`. CLAUDE.md forbids that in `fauxchange`: pricing and Greeks go through
//! `optionstratlib`. This pricer therefore builds an [`optionstratlib::Options`]
//! and calls [`optionstratlib::pricing::black_scholes`] for the theoretical value
//! and the [`optionstratlib::greeks::Greeks`] trait (`delta` / `gamma` / `vega` /
//! `theta`) for the Greeks — no Black-Scholes or Greek formula is written here.
//!
//! ## The `f64` boundary is guarded (rule 2)
//!
//! Spot / strike / IV / time-to-expiry feed `f64` math that can produce
//! `NaN`/`Inf` (`ln`, a zero strike, an expired option, a huge IV). Every entry
//! point returns **`Option<f64>`** and yields `None` — never a poisoned number —
//! when an input is non-finite / non-positive, when `optionstratlib` errors, or
//! when the resulting value is non-finite. The [`crate::market_maker::Quoter`]
//! turns a `None` into "skip quoting this instrument", so a `NaN`/`Inf` can never
//! reach a `QuoteParams`, an `AddOrder`, or a broadcast event.
//!
//! ## Determinism (rule 5)
//!
//! Time-to-expiry is passed in as a **relative day count** (`days_to_expiry`)
//! derived by the caller from the **venue clock**, never the wall clock: the
//! pricer builds [`ExpirationDate::Days`], whose year fraction is
//! `days / DAYS_IN_A_YEAR` (a pure function, verified clock-free upstream). It
//! never constructs [`ExpirationDate::DateTime`], whose `optionstratlib`
//! conversion reads `Utc::now()`. So for a fixed input the pricer returns the
//! identical value on every call, on a live run and on a replay alike.

use optionstratlib::greeks::Greeks;
use optionstratlib::model::types::{OptionType, Side as OptSide};
use optionstratlib::prelude::{Decimal, Positive, ToPrimitive};
use optionstratlib::pricing::black_scholes;
use optionstratlib::{ExpirationDate, OptionStyle, Options};

/// The default annualized risk-free rate the market-maker pricer assumes
/// (`5%`) — the documented default carried over from the Backend
/// ([specs §3](../../docs/specs/option-chain-orderbook-backend.md#3-market-maker)).
pub const DEFAULT_RISK_FREE_RATE: f64 = 0.05;

/// The default implied volatility the pricer assumes when a quote carries no IV
/// override (`30%`).
pub const DEFAULT_IV: f64 = 0.30;

/// The venue's synthetic underlying symbol for the pricing `Options` value — a
/// pricing-kernel detail, never a venue symbol or a book key.
const PRICER_UNDERLYING: &str = "MM";

/// A Black-Scholes option pricer for market making.
///
/// Holds the two model constants (`risk_free_rate`, `default_iv`) and builds an
/// [`optionstratlib::Options`] per valuation so the math kernel is
/// `optionstratlib`'s, not a local formula. Cheap to clone (two `f64`s).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OptionPricer {
    /// Annualized risk-free rate (e.g. `0.05` for 5%).
    risk_free_rate: f64,
    /// Default implied volatility applied when a quote carries no IV override.
    default_iv: f64,
}

impl OptionPricer {
    /// Creates a pricer from the annualized `risk_free_rate` and the
    /// `default_iv` applied when a quote supplies no IV override.
    #[must_use]
    #[inline]
    pub fn new(risk_free_rate: f64, default_iv: f64) -> Self {
        Self {
            risk_free_rate,
            default_iv,
        }
    }

    /// The configured risk-free rate.
    #[must_use]
    #[inline]
    pub fn risk_free_rate(&self) -> f64 {
        self.risk_free_rate
    }

    /// The configured default implied volatility.
    #[must_use]
    #[inline]
    pub fn default_iv(&self) -> f64 {
        self.default_iv
    }

    /// Builds the `optionstratlib` [`Options`] for a valuation, or `None` when an
    /// input is degenerate — the single `f64`-boundary gate every method flows
    /// through.
    ///
    /// Rejects (returns `None`) a non-finite or non-positive spot / strike /
    /// IV / `days_to_expiry`, so the constructed `Options` always carries finite,
    /// strictly-positive [`Positive`] values and the Black-Scholes `ln`/`sqrt`
    /// kernel never sees a zero or negative argument. Uses
    /// [`ExpirationDate::Days`] (clock-free), never `DateTime` (wall-clock).
    #[must_use]
    fn build_option(
        &self,
        spot: f64,
        strike: f64,
        days_to_expiry: f64,
        style: OptionStyle,
        iv: Option<f64>,
    ) -> Option<Options> {
        let sigma = iv.unwrap_or(self.default_iv);
        // Reject non-finite / non-positive inputs before they reach the kernel.
        if !(spot.is_finite() && spot > 0.0)
            || !(strike.is_finite() && strike > 0.0)
            || !(sigma.is_finite() && sigma > 0.0)
            || !(days_to_expiry.is_finite() && days_to_expiry > 0.0)
        {
            return None;
        }
        // `Positive::new` additionally rejects NaN and non-positive values.
        let strike_price = Positive::new(strike).ok()?;
        let underlying_price = Positive::new(spot).ok()?;
        let implied_volatility = Positive::new(sigma).ok()?;
        let days = Positive::new(days_to_expiry).ok()?;
        let risk_free_rate = Decimal::from_f64_retain(self.risk_free_rate)?;

        Some(Options::new(
            OptionType::European,
            OptSide::Long,
            PRICER_UNDERLYING.to_string(),
            strike_price,
            ExpirationDate::Days(days),
            implied_volatility,
            Positive::ONE,
            underlying_price,
            risk_free_rate,
            style,
            Positive::ZERO,
            None,
        ))
    }

    /// The theoretical option value, in the same units as `spot`/`strike`, via
    /// [`optionstratlib::pricing::black_scholes`].
    ///
    /// Returns `None` on a degenerate input (a non-finite or non-positive
    /// spot / strike / IV / `days_to_expiry`), an `optionstratlib` pricing error,
    /// or a non-finite result — the caller skips quoting rather than emit a
    /// poisoned value.
    #[must_use]
    pub fn theoretical_value(
        &self,
        spot: f64,
        strike: f64,
        days_to_expiry: f64,
        style: OptionStyle,
        iv: Option<f64>,
    ) -> Option<f64> {
        let option = self.build_option(spot, strike, days_to_expiry, style, iv)?;
        finite(black_scholes(&option).ok()?.to_f64()?)
    }

    /// Delta via the [`optionstratlib::greeks::Greeks`] trait. `None` on a
    /// degenerate input, an `optionstratlib` error, or a non-finite result.
    #[must_use]
    pub fn delta(
        &self,
        spot: f64,
        strike: f64,
        days_to_expiry: f64,
        style: OptionStyle,
        iv: Option<f64>,
    ) -> Option<f64> {
        let option = self.build_option(spot, strike, days_to_expiry, style, iv)?;
        finite(option.delta().ok()?.to_f64()?)
    }

    /// Gamma via the [`optionstratlib::greeks::Greeks`] trait. `None` on a
    /// degenerate input, an `optionstratlib` error, or a non-finite result.
    #[must_use]
    pub fn gamma(
        &self,
        spot: f64,
        strike: f64,
        days_to_expiry: f64,
        style: OptionStyle,
        iv: Option<f64>,
    ) -> Option<f64> {
        let option = self.build_option(spot, strike, days_to_expiry, style, iv)?;
        finite(option.gamma().ok()?.to_f64()?)
    }

    /// Vega via the [`optionstratlib::greeks::Greeks`] trait. `None` on a
    /// degenerate input, an `optionstratlib` error, or a non-finite result.
    #[must_use]
    pub fn vega(
        &self,
        spot: f64,
        strike: f64,
        days_to_expiry: f64,
        style: OptionStyle,
        iv: Option<f64>,
    ) -> Option<f64> {
        let option = self.build_option(spot, strike, days_to_expiry, style, iv)?;
        finite(option.vega().ok()?.to_f64()?)
    }

    /// Theta via the [`optionstratlib::greeks::Greeks`] trait (units are
    /// `optionstratlib`'s). `None` on a degenerate input, an `optionstratlib`
    /// error, or a non-finite result.
    #[must_use]
    pub fn theta(
        &self,
        spot: f64,
        strike: f64,
        days_to_expiry: f64,
        style: OptionStyle,
        iv: Option<f64>,
    ) -> Option<f64> {
        let option = self.build_option(spot, strike, days_to_expiry, style, iv)?;
        finite(option.theta().ok()?.to_f64()?)
    }
}

impl Default for OptionPricer {
    /// The documented defaults: risk-free `0.05`, default IV `0.30`.
    #[inline]
    fn default() -> Self {
        Self::new(DEFAULT_RISK_FREE_RATE, DEFAULT_IV)
    }
}

/// Passes a finite value through, mapping `NaN`/`±Inf` to `None` — the last gate
/// before an `f64` crosses back toward integer cents.
#[must_use]
#[inline]
fn finite(value: f64) -> Option<f64> {
    if value.is_finite() { Some(value) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THIRTY_DAYS: f64 = 30.0;

    #[test]
    fn test_atm_call_value_is_positive_and_reasonable() {
        let pricer = OptionPricer::default();
        let theo = pricer
            .theoretical_value(100.0, 100.0, THIRTY_DAYS, OptionStyle::Call, Some(0.20))
            .expect("a finite theo for a well-posed ATM call");
        assert!(theo > 0.0, "an ATM call has positive value, got {theo}");
        assert!(
            theo < 10.0,
            "an ATM 30-day call is a few dollars, got {theo}"
        );
    }

    #[test]
    fn test_atm_put_value_is_positive() {
        let pricer = OptionPricer::default();
        let theo = pricer
            .theoretical_value(100.0, 100.0, THIRTY_DAYS, OptionStyle::Put, Some(0.20))
            .expect("a finite theo for a well-posed ATM put");
        assert!(theo > 0.0, "an ATM put has positive value, got {theo}");
    }

    #[test]
    fn test_atm_call_delta_is_around_half() {
        let pricer = OptionPricer::default();
        let delta = pricer
            .delta(100.0, 100.0, THIRTY_DAYS, OptionStyle::Call, Some(0.20))
            .expect("a finite delta");
        assert!(
            delta > 0.4 && delta < 0.6,
            "ATM call delta ~0.5, got {delta}"
        );
    }

    #[test]
    fn test_atm_put_delta_is_around_negative_half() {
        let pricer = OptionPricer::default();
        let delta = pricer
            .delta(100.0, 100.0, THIRTY_DAYS, OptionStyle::Put, Some(0.20))
            .expect("a finite delta");
        assert!(
            delta > -0.6 && delta < -0.4,
            "ATM put delta ~-0.5, got {delta}"
        );
    }

    #[test]
    fn test_gamma_and_vega_are_finite_and_positive() {
        let pricer = OptionPricer::default();
        let gamma = pricer
            .gamma(100.0, 100.0, THIRTY_DAYS, OptionStyle::Call, Some(0.20))
            .expect("a finite gamma");
        let vega = pricer
            .vega(100.0, 100.0, THIRTY_DAYS, OptionStyle::Call, Some(0.20))
            .expect("a finite vega");
        assert!(
            gamma > 0.0,
            "gamma is positive for a long option, got {gamma}"
        );
        assert!(vega > 0.0, "vega is positive for a long option, got {vega}");
    }

    #[test]
    fn test_theta_is_finite() {
        let pricer = OptionPricer::default();
        assert!(
            pricer
                .theta(100.0, 100.0, THIRTY_DAYS, OptionStyle::Call, Some(0.20))
                .is_some(),
            "theta must be a finite value"
        );
    }

    // ---- the f64 boundary is guarded --------------------------------------

    #[test]
    fn test_non_finite_iv_yields_none_not_a_poisoned_value() {
        let pricer = OptionPricer::default();
        for bad_iv in [f64::INFINITY, f64::NAN, f64::NEG_INFINITY, 0.0, -0.2] {
            assert!(
                pricer
                    .theoretical_value(100.0, 100.0, THIRTY_DAYS, OptionStyle::Call, Some(bad_iv))
                    .is_none(),
                "a degenerate iv={bad_iv} must yield None, never a poisoned theo"
            );
        }
    }

    #[test]
    fn test_expired_or_non_positive_time_yields_none() {
        let pricer = OptionPricer::default();
        for bad_days in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(
                pricer
                    .theoretical_value(100.0, 100.0, bad_days, OptionStyle::Call, Some(0.20))
                    .is_none(),
                "a non-positive/non-finite time-to-expiry ({bad_days}) must yield None"
            );
        }
    }

    #[test]
    fn test_zero_or_negative_spot_or_strike_yields_none() {
        let pricer = OptionPricer::default();
        assert!(
            pricer
                .theoretical_value(0.0, 100.0, THIRTY_DAYS, OptionStyle::Call, Some(0.20))
                .is_none()
        );
        assert!(
            pricer
                .theoretical_value(100.0, 0.0, THIRTY_DAYS, OptionStyle::Call, Some(0.20))
                .is_none()
        );
        assert!(
            pricer
                .theoretical_value(-100.0, 100.0, THIRTY_DAYS, OptionStyle::Call, Some(0.20))
                .is_none()
        );
    }

    #[test]
    fn test_theoretical_value_is_deterministic_for_a_fixed_input() {
        // No wall clock or RNG: the same input yields the identical value.
        let pricer = OptionPricer::new(0.05, 0.30);
        let a = pricer.theoretical_value(123.0, 130.0, 17.0, OptionStyle::Call, Some(0.42));
        let b = pricer.theoretical_value(123.0, 130.0, 17.0, OptionStyle::Call, Some(0.42));
        assert_eq!(a, b, "the pricer must be a pure function of its input");
        assert!(a.is_some());
    }

    #[test]
    fn test_default_constants() {
        let pricer = OptionPricer::default();
        assert!((pricer.risk_free_rate() - DEFAULT_RISK_FREE_RATE).abs() < f64::EPSILON);
        assert!((pricer.default_iv() - DEFAULT_IV).abs() < f64::EPSILON);
    }
}
