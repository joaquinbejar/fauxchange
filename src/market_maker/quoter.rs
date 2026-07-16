//! Two-sided quote generation for market making — the persona-substrate
//! [`Quoter`] ported from the Backend, re-pointed at the `optionstratlib`
//! [`OptionPricer`] and the venue integer-cents money types
//! ([015](../../milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md)).
//!
//! ## Determinism (rule 5)
//!
//! [`Quoter::generate_quote`] is a **pure function** of its [`QuoteInput`]: no
//! clock, no RNG, no map iteration. The same input yields the same
//! [`QuoteParams`] on every call. Time-to-expiry enters as a pre-computed,
//! venue-clock-derived `days_to_expiry` (see [`QuoteInput`]), so nothing in the
//! quote path reads the wall clock. The documented invariants hold: `ask > bid`,
//! `bid >= 1`, sizes `>= 1`, a wider `spread_multiplier` widens the spread, skew
//! shifts bid/ask by the same signed amount, and `size_scalar` scales size.
//!
//! ## The `f64` boundary is guarded (rule 2)
//!
//! The Black-Scholes value is `f64`; a degenerate input can make it `NaN`/`±Inf`.
//! `generate_quote` returns **`None`** rather than cast a non-finite value into
//! integer [`Cents`] (a `NaN` would silently become `0`, a `+Inf` `u64::MAX`),
//! so a poisoned value never reaches a `QuoteParams`, an `AddOrder`, or a
//! broadcast. Money stays integer cents on the way out; `f64` lives only inside
//! the kernel and is rounded back deterministically.

use crate::exchange::Cents;
use crate::exchange::OptionStyle;
use crate::market_maker::OptionPricer;

/// Basis-points denominator: 1 bp = 1/10_000, applied as `price * bps / 10_000`.
const BPS_DENOMINATOR: f64 = 10_000.0;

/// Fraction of the half-spread used as the maximum directional price skew: at
/// full skew (`±1.0`) the parallel shift is at most half the half-spread, so the
/// quote re-centers without crossing the theoretical value.
const SKEW_PRICE_WEIGHT: f64 = 0.5;

/// How much directional skew shrinks the size on the side the maker is less
/// willing to trade: that side is scaled by `1 - skew.abs() * SKEW_SIZE_WEIGHT`
/// (down to 70% at full skew).
const SKEW_SIZE_WEIGHT: f64 = 0.3;

/// Default base spread in basis points (1%) for [`Quoter::default`].
pub const DEFAULT_BASE_SPREAD_BPS: u64 = 100;

/// Default base quote size for [`Quoter::default`].
pub const DEFAULT_BASE_SIZE: u64 = 10;

/// The generated two-sided quote for one option, in venue money.
///
/// Money is integer [`Cents`]; sizes are contracts. `theo_price` is stored so the
/// captured edge can be computed when a leg fills
/// ([`Quoter::calculate_edge`]). All invariants hold by construction:
/// `ask_price > bid_price`, `bid_price >= 1`, and both sizes `>= 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteParams {
    /// Bid price in **cents**.
    pub bid_price: Cents,
    /// Ask price in **cents**.
    pub ask_price: Cents,
    /// Bid size in **contracts**.
    pub bid_size: u64,
    /// Ask size in **contracts**.
    pub ask_size: u64,
    /// The theoretical value in **cents** the quote was built around.
    pub theo_price: Cents,
}

/// The inputs to [`Quoter::generate_quote`].
///
/// Time-to-expiry is `days_to_expiry` — a **relative day count the venue derives
/// from its clock** (not the wall clock) — so the quote stays a pure,
/// deterministic function of its input (rule 5). This is the venue adaptation of
/// the Backend's `expiration: &ExpirationDate`, which routed a `DateTime` expiry
/// through `Utc::now()`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteInput {
    /// Current underlying price in **cents**.
    pub spot_cents: u64,
    /// Strike price in **cents**.
    pub strike_cents: u64,
    /// Relative time-to-expiry in **days**, derived from the venue clock.
    pub days_to_expiry: f64,
    /// Call or put.
    pub style: OptionStyle,
    /// Spread multiplier (`1.0` = base). Range-validated by the engine to
    /// `[0.1, 10.0]` (out-of-range rejected).
    pub spread_multiplier: f64,
    /// Size scalar (`0.0`–`1.0`). Range-validated by the engine.
    pub size_scalar: f64,
    /// Directional skew (`-1.0`–`1.0`, positive = bullish). Range-validated by
    /// the engine.
    pub directional_skew: f64,
    /// Optional implied-volatility override; `None` uses the pricer default.
    pub iv: Option<f64>,
}

/// Generates persona-shaped two-sided quotes for options around an
/// [`OptionPricer`] theoretical value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quoter {
    pricer: OptionPricer,
    /// Base spread in basis points.
    base_spread_bps: u64,
    /// Base quote size in contracts.
    base_size: u64,
}

impl Quoter {
    /// Creates a quoter around `pricer` with a `base_spread_bps` and `base_size`.
    #[must_use]
    #[inline]
    pub fn new(pricer: OptionPricer, base_spread_bps: u64, base_size: u64) -> Self {
        Self {
            pricer,
            base_spread_bps,
            base_size,
        }
    }

    /// The pricer backing this quoter.
    #[must_use]
    #[inline]
    pub fn pricer(&self) -> &OptionPricer {
        &self.pricer
    }

    /// Generates a two-sided quote, or `None` when the theoretical value is
    /// non-finite and no safe quote can be produced.
    ///
    /// A **pure function** of `input` (rule 5). The `f64 → cents` boundary is
    /// guarded (rule 2): a non-finite theoretical value — or a non-finite
    /// scaled-to-cents intermediate — returns `None` rather than casting garbage
    /// into [`Cents`]. Invariants of the returned quote: `ask_price > bid_price`,
    /// `bid_price >= 1`, both sizes `>= 1`.
    #[must_use]
    pub fn generate_quote(&self, input: &QuoteInput) -> Option<QuoteParams> {
        let spot = input.spot_cents as f64 / 100.0;
        let strike = input.strike_cents as f64 / 100.0;

        // Theoretical value via `optionstratlib` (None on any degenerate input).
        let theo = self.pricer.theoretical_value(
            spot,
            strike,
            input.days_to_expiry,
            input.style,
            input.iv,
        )?;

        // Guard the scaled-to-cents intermediate: a finite-but-huge theo can
        // overflow to ±Inf when multiplied by 100.
        let theo_scaled = theo * 100.0;
        if !theo_scaled.is_finite() || theo_scaled < 0.0 {
            return None;
        }
        // The rounded cents value. Bound it to `i64::MAX` (not just `u64::MAX`):
        // the signed spread math below casts `theo_cents as i64`, which would wrap
        // to a negative value for a theo above `i64::MAX`. Refusing to quote such
        // an (astronomically large, practically unreachable) theo makes the cast
        // provably wrap-free rather than merely improbable.
        let theo_cents = theo_scaled.round();
        if !(0.0..=(i64::MAX as f64)).contains(&theo_cents) {
            return None;
        }
        let theo_cents = theo_cents as u64;

        // Half-spread from the base bps and the (range-validated) spread multiplier.
        let half_spread_bps =
            (self.base_spread_bps as f64 * input.spread_multiplier / 2.0).max(0.0);
        let half_spread_cents =
            ((theo_cents as f64 * half_spread_bps) / BPS_DENOMINATOR).max(1.0) as i64;

        // Directional skew: a symmetric same-signed PARALLEL shift of both legs
        // (magnitude at most `half_spread_cents * 0.5`), so the spread width is
        // preserved. A put's value moves opposite the underlying, so mirror it.
        let skew_adjustment =
            (half_spread_cents as f64 * input.directional_skew * SKEW_PRICE_WEIGHT) as i64;
        let (bid_adjustment, ask_adjustment) = match input.style {
            OptionStyle::Call => (skew_adjustment, skew_adjustment),
            OptionStyle::Put => (-skew_adjustment, -skew_adjustment),
        };

        // Computed in i64, then floored (bid >= 1, ask >= bid + 1) so a negative
        // adjustment can never underflow the cents value. `theo_cents <= i64::MAX`
        // (bounded above), so this cast is exact and cannot wrap.
        let theo_i = theo_cents as i64;
        let bid_i = (theo_i - half_spread_cents + bid_adjustment).max(1);
        let ask_i = (theo_i + half_spread_cents + ask_adjustment).max(bid_i + 1);
        let bid_price = Cents::new(bid_i as u64);
        let ask_price = Cents::new(ask_i as u64);

        // Base size scaled by the (range-validated) size scalar, then skewed down on the
        // side the maker is less willing to trade.
        let base_size = (self.base_size as f64 * input.size_scalar).max(1.0) as u64;
        let skew_size_factor = 1.0 - input.directional_skew.abs() * SKEW_SIZE_WEIGHT;
        let (bid_size, ask_size) = if input.directional_skew > 0.0 {
            (base_size, (base_size as f64 * skew_size_factor) as u64)
        } else if input.directional_skew < 0.0 {
            ((base_size as f64 * skew_size_factor) as u64, base_size)
        } else {
            (base_size, base_size)
        };

        Some(QuoteParams {
            bid_price,
            ask_price,
            bid_size: bid_size.max(1),
            ask_size: ask_size.max(1),
            theo_price: Cents::new(theo_cents),
        })
    }

    /// The captured **edge** for a fill, in **cents per contract** (positive =
    /// favorable, negative = adverse).
    ///
    /// Buying below theo, or selling above theo, is positive. Integer and
    /// overflow-safe: the operands are `u64` cents widened to `i64` and
    /// subtracted, and a realistic cents value fits `i64` with room to spare.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::market_maker::Quoter;
    /// assert_eq!(Quoter::calculate_edge(100, 105, true), 5); // bought 5 below theo
    /// assert_eq!(Quoter::calculate_edge(110, 105, false), 5); // sold 5 above theo
    /// assert_eq!(Quoter::calculate_edge(110, 105, true), -5); // bought 5 above theo
    /// ```
    #[must_use]
    #[inline]
    pub fn calculate_edge(fill_price_cents: u64, theo_cents: u64, is_buy: bool) -> i64 {
        let fill = i128::from(fill_price_cents);
        let theo = i128::from(theo_cents);
        // Buy: theo - fill (want to buy below theo). Sell: fill - theo.
        let edge = if is_buy { theo - fill } else { fill - theo };
        // Realistic cents differences fit i64; clamp defensively rather than wrap.
        edge.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }
}

impl Default for Quoter {
    /// Default pricer, 1% base spread, base size 10.
    #[inline]
    fn default() -> Self {
        Self::new(
            OptionPricer::default(),
            DEFAULT_BASE_SPREAD_BPS,
            DEFAULT_BASE_SIZE,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THIRTY_DAYS: f64 = 30.0;

    fn input(style: OptionStyle) -> QuoteInput {
        QuoteInput {
            spot_cents: 10_000,
            strike_cents: 10_000,
            days_to_expiry: THIRTY_DAYS,
            style,
            spread_multiplier: 1.0,
            size_scalar: 1.0,
            directional_skew: 0.0,
            iv: Some(0.20),
        }
    }

    #[test]
    fn test_generate_quote_holds_invariants() {
        let quoter = Quoter::default();
        let q = quoter
            .generate_quote(&input(OptionStyle::Call))
            .expect("a finite theo yields a quote");
        assert!(q.ask_price > q.bid_price, "ask must exceed bid");
        assert!(q.bid_price.get() >= 1, "bid floored at 1");
        assert!(q.bid_size >= 1 && q.ask_size >= 1, "sizes floored at 1");
    }

    #[test]
    fn test_generate_quote_is_deterministic() {
        let quoter = Quoter::default();
        let a = quoter.generate_quote(&input(OptionStyle::Call));
        let b = quoter.generate_quote(&input(OptionStyle::Call));
        assert_eq!(a, b, "generate_quote must be a pure function of its input");
    }

    #[test]
    fn test_wider_spread_multiplier_widens_the_spread() {
        let quoter = Quoter::default();
        // Large theo + wide multiplier so the integer half-spread is not truncated.
        let base = QuoteInput {
            spot_cents: 1_000_000,
            strike_cents: 1_000_000,
            iv: Some(0.50),
            ..input(OptionStyle::Call)
        };
        let narrow = quoter
            .generate_quote(&QuoteInput {
                spread_multiplier: 1.0,
                ..base
            })
            .expect("quote");
        let wide = quoter
            .generate_quote(&QuoteInput {
                spread_multiplier: 5.0,
                ..base
            })
            .expect("quote");
        let narrow_spread = narrow.ask_price.get() - narrow.bid_price.get();
        let wide_spread = wide.ask_price.get() - wide.bid_price.get();
        assert!(
            wide_spread > narrow_spread,
            "a wider multiplier widens the spread: {wide_spread} !> {narrow_spread}"
        );
    }

    #[test]
    fn test_directional_skew_is_symmetric_parallel_shift() {
        let quoter = Quoter::default();
        let base = QuoteInput {
            spot_cents: 1_000_000,
            strike_cents: 1_000_000,
            spread_multiplier: 10.0,
            iv: Some(0.50),
            ..input(OptionStyle::Call)
        };
        let neutral = quoter
            .generate_quote(&QuoteInput {
                directional_skew: 0.0,
                ..base
            })
            .expect("quote");
        let bullish = quoter
            .generate_quote(&QuoteInput {
                directional_skew: 0.5,
                ..base
            })
            .expect("quote");
        let bearish = quoter
            .generate_quote(&QuoteInput {
                directional_skew: -0.5,
                ..base
            })
            .expect("quote");

        let bid_delta = bullish.bid_price.get() as i128 - neutral.bid_price.get() as i128;
        let ask_delta = bullish.ask_price.get() as i128 - neutral.ask_price.get() as i128;
        assert!(
            bid_delta >= 1,
            "bullish call raises the bid, got {bid_delta}"
        );
        assert_eq!(bid_delta, ask_delta, "bid and ask shift by the same amount");

        let bear_bid = bearish.bid_price.get() as i128 - neutral.bid_price.get() as i128;
        assert_eq!(
            bid_delta, -bear_bid,
            "bullish and bearish are opposite shifts"
        );

        // Spread width is preserved under a parallel shift.
        let neutral_spread = neutral.ask_price.get() - neutral.bid_price.get();
        assert_eq!(
            bullish.ask_price.get() - bullish.bid_price.get(),
            neutral_spread
        );
        assert_eq!(
            bearish.ask_price.get() - bearish.bid_price.get(),
            neutral_spread
        );
    }

    #[test]
    fn test_size_scalar_scales_size() {
        let quoter = Quoter::default();
        let full = quoter
            .generate_quote(&QuoteInput {
                size_scalar: 1.0,
                ..input(OptionStyle::Call)
            })
            .expect("quote");
        let half = quoter
            .generate_quote(&QuoteInput {
                size_scalar: 0.5,
                ..input(OptionStyle::Call)
            })
            .expect("quote");
        assert!(
            half.bid_size < full.bid_size,
            "a smaller scalar shrinks size"
        );
        assert!(half.bid_size >= 1, "size floored at 1");
    }

    #[test]
    fn test_generate_quote_skips_non_finite_theo() {
        let quoter = Quoter::default();
        for bad_iv in [f64::INFINITY, f64::NAN] {
            let bad = QuoteInput {
                iv: Some(bad_iv),
                ..input(OptionStyle::Call)
            };
            assert!(
                quoter.generate_quote(&bad).is_none(),
                "a non-finite theo must skip quoting (iv={bad_iv})"
            );
        }
    }

    #[test]
    fn test_calculate_edge_signs() {
        assert_eq!(Quoter::calculate_edge(100, 105, true), 5);
        assert_eq!(Quoter::calculate_edge(110, 105, false), 5);
        assert_eq!(Quoter::calculate_edge(110, 105, true), -5);
        assert_eq!(Quoter::calculate_edge(100, 105, false), -5);
    }

    #[test]
    fn test_calculate_edge_is_overflow_safe() {
        // Extreme (unrealistic) cents cannot wrap or panic.
        assert_eq!(Quoter::calculate_edge(u64::MAX, 0, false), i64::MAX);
        assert_eq!(Quoter::calculate_edge(0, u64::MAX, false), i64::MIN);
    }
}
