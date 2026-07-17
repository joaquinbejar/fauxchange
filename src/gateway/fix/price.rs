//! The checked decimal-`Price` ↔ integer-`Cents` seam at the FIX edge — the
//! single place a FIX `Price` decimal string crosses into the venue's canonical
//! integer cents ([fix-dialect §1](../../../docs/specs/fix-dialect.md#1-economic-fields-units-scale-and-the-checked-seam),
//! [ADR-0003](../../../docs/adr/0003-money-as-integer-cents.md)).
//!
//! Internally and on REST/WS/DB, money is integer [`Cents`]. On the FIX wire,
//! `Price (44)` and every price-typed tag are standard FIX `Price` — a **decimal
//! string in currency units** (`44=500.05`, never raw cents). This module is the
//! exact-or-reject conversion between the two:
//!
//! - **The decimal exists only as a string at the FIX edge.** There is **no
//!   `f64` anywhere** in this seam: the string is parsed to integer cents with
//!   checked integer arithmetic ([`fold_digits`]), and the reverse renders cents
//!   as a decimal string with fixed [`CENTS_SCALE`] fractional digits. `Cents` is
//!   the canonical value the moment the frame is parsed.
//! - **One venue tick is one cent**, so the currency scale is a venue-wide
//!   constant [`CENTS_SCALE`] = 2 (a cent is `1/100` of a currency unit). A price
//!   carrying **more fractional digits than the scale** (sub-cent precision) is
//!   rejected, never rounded — that check is scale-only and instrument-independent
//!   ([`parse_decimal_to_cents`]).
//! - **Off-tick rejection is keyed on the instrument tick.** [`PriceScale`]
//!   carries the instrument's `ContractSpecs` tick size in cents
//!   ([05 §7](../../../docs/05-microstructure-config.md#7-contract-specs-tick-and-lot));
//!   [`PriceScale::decimal_to_cents`] is the full seam (scale **and** off-tick).
//!   The tick is resolved from the order's `Symbol (55)` by the acceptor / order
//!   path (#037/#039), so a typed message decodes the price scale-only and the
//!   tick check runs once the instrument is known.
//!
//! Golden fixtures assert the round trip in both directions, including the
//! fractional display price `44=500.05` ↔ `50005` cents, so the checked seam is
//! proven lossless ([fix-dialect §6](../../../docs/specs/fix-dialect.md#6-golden-fixtures-required)).

use std::num::NonZeroU64;

use crate::exchange::{Cents, SignedCents};

use super::limits::truncate_untrusted;

/// The venue-wide FIX `Price` decimal scale: one currency unit is 100 cents, so
/// a `Price` carries exactly two fractional digits. "One venue tick is one cent."
///
/// This is a **venue property**, not a per-instrument one — every `fauxchange`
/// instrument prices in whole cents ([fix-dialect §1](../../../docs/specs/fix-dialect.md#1-economic-fields-units-scale-and-the-checked-seam)),
/// so a `Price` with more than [`CENTS_SCALE`] fractional digits is sub-cent
/// precision the venue cannot represent and is rejected at the wire edge.
pub const CENTS_SCALE: u32 = 2;

/// A failure converting between a FIX `Price` decimal string and integer cents.
///
/// Every variant is exact-or-reject: a malformed, sub-cent, out-of-range, or
/// off-tick price is a **typed error**, never a silent round. The variants split
/// into wire-format failures (instrument-independent: [`Empty`](Self::Empty),
/// [`Malformed`](Self::Malformed), [`TooManyFractionalDigits`](Self::TooManyFractionalDigits),
/// [`OutOfRange`](Self::OutOfRange)) and the tick-keyed economic failure
/// ([`OffTick`](Self::OffTick)); [`Self::is_wire_format`] distinguishes them so
/// the acceptor routes a format failure to a session `Reject (3)` and an
/// off-tick price to the order's own `OrdRejReason (103)` reject.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PriceSeamError {
    /// The price string was empty.
    #[error("price is empty")]
    Empty,
    /// The price string was not a well-formed non-negative decimal (a non-digit
    /// character, a sign, an exponent, more than one `.`, a missing integer
    /// digit, or a trailing `.`).
    #[error("price '{value}' is not a well-formed non-negative decimal")]
    Malformed {
        /// The offending price string.
        value: String,
    },
    /// The price carried more than [`CENTS_SCALE`] fractional digits — sub-cent
    /// precision the venue cannot represent. Rejected, never rounded.
    #[error(
        "price '{value}' has more than 2 fractional digits (sub-cent precision is not representable)"
    )]
    TooManyFractionalDigits {
        /// The offending price string.
        value: String,
    },
    /// The price, converted to cents, exceeds the representable `u64` cents range.
    #[error("price '{value}' exceeds the representable cents range")]
    OutOfRange {
        /// The offending price string.
        value: String,
    },
    /// The price does not land on the instrument's tick — an off-tick price.
    /// Rejected, never rounded to the nearest tick.
    #[error("price {cents} cents does not land on the {tick_size_cents}-cent tick")]
    OffTick {
        /// The price in cents that failed the tick check.
        cents: u64,
        /// The instrument tick size in cents the price had to be a multiple of.
        tick_size_cents: u64,
    },
    /// A [`PriceScale`] was constructed with a zero tick size, which cannot
    /// define an on-tick grid.
    #[error("tick size must be a positive number of cents")]
    ZeroTick,
}

impl PriceSeamError {
    /// Returns `true` for a wire-format failure (instrument-independent) and
    /// `false` for the tick-keyed economic [`OffTick`](Self::OffTick) /
    /// [`ZeroTick`](Self::ZeroTick).
    ///
    /// The acceptor uses this to route: a wire-format failure is a session-level
    /// data-format problem, while an off-tick price is a business rejection in
    /// the order's own context ([fix-dialect §5](../../../docs/specs/fix-dialect.md#5-validation-and-reject-behaviour)).
    #[must_use]
    #[inline]
    pub const fn is_wire_format(&self) -> bool {
        matches!(
            self,
            Self::Empty
                | Self::Malformed { .. }
                | Self::TooManyFractionalDigits { .. }
                | Self::OutOfRange { .. }
        )
    }
}

/// Folds a slice of ASCII digit bytes into a `u128` with **checked** integer
/// arithmetic — no float, no wrap. Returns `None` on a non-digit byte or on
/// overflow.
#[inline]
fn fold_digits(bytes: &[u8]) -> Option<u128> {
    let mut acc: u128 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(u128::from(b - b'0'))?;
    }
    Some(acc)
}

/// Parses a FIX `Price` decimal string into [`Cents`] with the venue-wide
/// [`CENTS_SCALE`], **scale-only** (no tick check) — the instrument-independent
/// half of the seam a typed message decodes with.
///
/// The conversion is exact: the string must be a non-negative decimal with at
/// most [`CENTS_SCALE`] fractional digits, parsed with checked integer
/// arithmetic (`f64` is never involved). Apply [`PriceScale::decimal_to_cents`]
/// instead when the instrument tick is known and off-tick prices must be
/// rejected too.
///
/// # Errors
///
/// - [`PriceSeamError::Empty`] if `value` is empty.
/// - [`PriceSeamError::Malformed`] if `value` is not a well-formed non-negative
///   decimal (sign, exponent, extra `.`, missing integer digit, or trailing `.`).
/// - [`PriceSeamError::TooManyFractionalDigits`] if it carries sub-cent precision.
/// - [`PriceSeamError::OutOfRange`] if the cents value overflows `u64`.
///
/// # Examples
///
/// ```
/// use fauxchange::gateway::fix::price::parse_decimal_to_cents;
/// use fauxchange::exchange::Cents;
/// assert_eq!(parse_decimal_to_cents("500.05")?, Cents::new(50005));
/// assert_eq!(parse_decimal_to_cents("500.5")?, Cents::new(50050));
/// assert_eq!(parse_decimal_to_cents("500")?, Cents::new(50000));
/// assert!(parse_decimal_to_cents("500.055").is_err()); // sub-cent
/// # Ok::<(), fauxchange::gateway::fix::price::PriceSeamError>(())
/// ```
pub fn parse_decimal_to_cents(value: &str) -> Result<Cents, PriceSeamError> {
    if value.is_empty() {
        return Err(PriceSeamError::Empty);
    }
    let malformed = || PriceSeamError::Malformed {
        value: truncate_untrusted(value),
    };

    // Only ASCII digits and at most one `.` — no sign, exponent, or whitespace.
    let bytes = value.as_bytes();
    let mut dot_index: Option<usize> = None;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'0'..=b'9' => {}
            b'.' if dot_index.is_none() => dot_index = Some(i),
            _ => return Err(malformed()),
        }
    }

    let (int_bytes, frac_bytes) = match dot_index {
        Some(i) => (&bytes[..i], &bytes[i + 1..]),
        None => (bytes, &[][..]),
    };

    // Require an integer digit ("0.05", not ".05") and reject a trailing dot.
    if int_bytes.is_empty() {
        return Err(malformed());
    }
    if dot_index.is_some() && frac_bytes.is_empty() {
        return Err(malformed());
    }
    if frac_bytes.len() > CENTS_SCALE as usize {
        return Err(PriceSeamError::TooManyFractionalDigits {
            value: truncate_untrusted(value),
        });
    }

    let out_of_range = || PriceSeamError::OutOfRange {
        value: truncate_untrusted(value),
    };
    // Integer currency units → cents: `int * 100 + frac`, all checked.
    let int_value = fold_digits(int_bytes).ok_or_else(out_of_range)?;
    let frac_value = fractional_cents(frac_bytes);
    let cents_u128 = int_value
        .checked_mul(100)
        .and_then(|scaled| scaled.checked_add(u128::from(frac_value)))
        .ok_or_else(out_of_range)?;
    let cents = u64::try_from(cents_u128).map_err(|_| out_of_range())?;
    Ok(Cents::new(cents))
}

/// Interprets 0..=[`CENTS_SCALE`] fractional digit bytes as whole cents, padding
/// on the right to the scale (so `"5"` is 50 cents and `"05"` is 5 cents). The
/// caller has already checked the length and that every byte is a digit.
#[inline]
fn fractional_cents(frac_bytes: &[u8]) -> u64 {
    // Right-pad to exactly CENTS_SCALE digits, then fold — bounded to 0..=99, so
    // the fold cannot overflow and the u64 cast is lossless.
    let mut padded = [b'0'; CENTS_SCALE as usize];
    for (slot, &b) in padded.iter_mut().zip(frac_bytes.iter()) {
        *slot = b;
    }
    fold_digits(&padded).unwrap_or(0) as u64
}

/// Renders integer [`Cents`] as a FIX `Price` decimal string with exactly
/// [`CENTS_SCALE`] fractional digits — the outbound half of the seam.
///
/// The split is exact euclidean division by the nonzero constant `100`, so it
/// never rounds and never panics: `50005` → `"500.05"`, `50000` → `"500.00"`,
/// `5` → `"0.05"`.
///
/// # Examples
///
/// ```
/// use fauxchange::gateway::fix::price::render_cents_to_decimal;
/// use fauxchange::exchange::Cents;
/// assert_eq!(render_cents_to_decimal(Cents::new(50005)), "500.05");
/// assert_eq!(render_cents_to_decimal(Cents::new(5)), "0.05");
/// ```
#[must_use]
pub fn render_cents_to_decimal(cents: Cents) -> String {
    let value = cents.get();
    // 100 is a nonzero constant: exact split into whole units and remainder
    // cents, no rounding, no panic.
    let whole = value / 100;
    let remainder = value % 100;
    format!("{whole}.{remainder:02}")
}

/// Parses a signed FIX amount (e.g. `Commission (12)`, which can be a maker
/// **rebate**) into [`SignedCents`] with the [`CENTS_SCALE`] scale.
///
/// Accepts an optional leading `-`; the magnitude is parsed by the same checked
/// integer path as [`parse_decimal_to_cents`] (no `f64`), then signed.
///
/// # Errors
///
/// Every [`parse_decimal_to_cents`] error on the magnitude, plus
/// [`PriceSeamError::OutOfRange`] if the signed value leaves the `i64` range.
pub fn parse_signed_decimal_to_cents(value: &str) -> Result<SignedCents, PriceSeamError> {
    let (negative, magnitude) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    let cents = parse_decimal_to_cents(magnitude)?.get();
    let signed = i64::try_from(cents).map_err(|_| PriceSeamError::OutOfRange {
        value: truncate_untrusted(value),
    })?;
    Ok(SignedCents::new(if negative { -signed } else { signed }))
}

/// Renders [`SignedCents`] as a signed FIX amount decimal string with
/// [`CENTS_SCALE`] fractional digits (a leading `-` for a rebate).
///
/// # Examples
///
/// ```
/// use fauxchange::gateway::fix::price::render_signed_cents_to_decimal;
/// use fauxchange::exchange::SignedCents;
/// assert_eq!(render_signed_cents_to_decimal(SignedCents::new(250)), "2.50");
/// assert_eq!(render_signed_cents_to_decimal(SignedCents::new(-10)), "-0.10");
/// ```
#[must_use]
pub fn render_signed_cents_to_decimal(value: SignedCents) -> String {
    let magnitude = Cents::new(value.get().unsigned_abs());
    if value.get() < 0 {
        format!("-{}", render_cents_to_decimal(magnitude))
    } else {
        render_cents_to_decimal(magnitude)
    }
}

/// The per-instrument FIX `Price` scale the checked seam is keyed on: the venue
/// tick size in cents, from the instrument's `ContractSpecs`
/// ([05 §7](../../../docs/05-microstructure-config.md#7-contract-specs-tick-and-lot)).
///
/// A price must land on a whole multiple of the tick; the acceptor resolves the
/// tick from the order's `Symbol (55)` before applying the seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceScale {
    tick_size_cents: NonZeroU64,
}

impl PriceScale {
    /// Builds a scale from a positive tick size in cents.
    ///
    /// # Errors
    ///
    /// Returns [`PriceSeamError::ZeroTick`] if `tick_size_cents` is zero.
    #[inline]
    pub fn new(tick_size_cents: u64) -> Result<Self, PriceSeamError> {
        NonZeroU64::new(tick_size_cents)
            .map(|tick_size_cents| Self { tick_size_cents })
            .ok_or(PriceSeamError::ZeroTick)
    }

    /// Returns the tick size in cents.
    #[must_use]
    #[inline]
    pub const fn tick_size_cents(&self) -> u64 {
        self.tick_size_cents.get()
    }

    /// The full checked seam: parse a decimal `Price` string into [`Cents`] and
    /// reject an off-tick price. Exact-or-reject on both the scale and the tick.
    ///
    /// # Errors
    ///
    /// Every [`parse_decimal_to_cents`] error, plus [`PriceSeamError::OffTick`]
    /// if the (well-formed) price does not land on the tick.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::gateway::fix::price::PriceScale;
    /// use fauxchange::exchange::Cents;
    /// let scale = PriceScale::new(5)?; // 5-cent tick
    /// assert_eq!(scale.decimal_to_cents("500.05")?, Cents::new(50005));
    /// assert!(scale.decimal_to_cents("500.03").is_err()); // off-tick
    /// # Ok::<(), fauxchange::gateway::fix::price::PriceSeamError>(())
    /// ```
    pub fn decimal_to_cents(&self, value: &str) -> Result<Cents, PriceSeamError> {
        let cents = parse_decimal_to_cents(value)?;
        self.ensure_on_tick(cents)?;
        Ok(cents)
    }

    /// Returns `Ok(())` iff `cents` lands on the tick.
    ///
    /// # Errors
    ///
    /// Returns [`PriceSeamError::OffTick`] if `cents` is not a whole multiple of
    /// the tick size.
    #[inline]
    pub fn ensure_on_tick(&self, cents: Cents) -> Result<(), PriceSeamError> {
        // NonZeroU64 divisor — the multiple check cannot panic.
        if cents.get().is_multiple_of(self.tick_size_cents.get()) {
            Ok(())
        } else {
            Err(PriceSeamError::OffTick {
                cents: cents.get(),
                tick_size_cents: self.tick_size_cents.get(),
            })
        }
    }

    /// Renders [`Cents`] as a decimal `Price` string ([`render_cents_to_decimal`]).
    /// Outbound cents are venue-produced and already on-tick, so this is
    /// infallible.
    #[must_use]
    #[inline]
    pub fn cents_to_decimal(&self, cents: Cents) -> String {
        render_cents_to_decimal(cents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cents(value: &str) -> Result<Cents, PriceSeamError> {
        parse_decimal_to_cents(value)
    }

    #[test]
    fn test_parse_decimal_to_cents_two_fractional_digits_is_exact() {
        assert_eq!(cents("500.05"), Ok(Cents::new(50005)));
    }

    #[test]
    fn test_parse_decimal_to_cents_one_fractional_digit_pads_to_cents() {
        // 500.5 dollars is 500 dollars 50 cents, not 5 cents.
        assert_eq!(cents("500.5"), Ok(Cents::new(50050)));
    }

    #[test]
    fn test_parse_decimal_to_cents_whole_units_is_exact() {
        assert_eq!(cents("500"), Ok(Cents::new(50000)));
    }

    #[test]
    fn test_parse_decimal_to_cents_sub_dollar_is_exact() {
        assert_eq!(cents("0.05"), Ok(Cents::new(5)));
        assert_eq!(cents("0.5"), Ok(Cents::new(50)));
        assert_eq!(cents("0.00"), Ok(Cents::new(0)));
    }

    #[test]
    fn test_parse_decimal_to_cents_more_fractional_digits_than_scale_is_rejected() {
        match cents("500.055") {
            Err(PriceSeamError::TooManyFractionalDigits { value }) => assert_eq!(value, "500.055"),
            other => panic!("expected TooManyFractionalDigits, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_decimal_to_cents_rejects_negative_sign() {
        match cents("-1.00") {
            Err(PriceSeamError::Malformed { value }) => assert_eq!(value, "-1.00"),
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_decimal_to_cents_rejects_exponent_and_whitespace() {
        assert!(matches!(
            cents("1e5"),
            Err(PriceSeamError::Malformed { .. })
        ));
        assert!(matches!(
            cents(" 500"),
            Err(PriceSeamError::Malformed { .. })
        ));
        assert!(matches!(
            cents("500 "),
            Err(PriceSeamError::Malformed { .. })
        ));
        assert!(matches!(
            cents("+500"),
            Err(PriceSeamError::Malformed { .. })
        ));
    }

    #[test]
    fn test_parse_decimal_to_cents_rejects_multiple_dots() {
        assert!(matches!(
            cents("5.0.0"),
            Err(PriceSeamError::Malformed { .. })
        ));
    }

    #[test]
    fn test_parse_decimal_to_cents_rejects_missing_integer_digit() {
        assert!(matches!(
            cents(".05"),
            Err(PriceSeamError::Malformed { .. })
        ));
    }

    #[test]
    fn test_parse_decimal_to_cents_rejects_trailing_dot() {
        assert!(matches!(
            cents("500."),
            Err(PriceSeamError::Malformed { .. })
        ));
    }

    #[test]
    fn test_parse_decimal_to_cents_rejects_empty() {
        assert_eq!(cents(""), Err(PriceSeamError::Empty));
    }

    #[test]
    fn test_parse_decimal_to_cents_overflow_is_out_of_range() {
        // A 30-digit integer part overflows u64 cents.
        match cents("1000000000000000000000000000000") {
            Err(PriceSeamError::OutOfRange { .. }) => {}
            other => panic!("expected OutOfRange, got {other:?}"),
        }
    }

    #[test]
    fn test_render_cents_to_decimal_round_trips_the_fractional_display_price() {
        assert_eq!(render_cents_to_decimal(Cents::new(50005)), "500.05");
        assert_eq!(render_cents_to_decimal(Cents::new(50000)), "500.00");
        assert_eq!(render_cents_to_decimal(Cents::new(5)), "0.05");
        assert_eq!(render_cents_to_decimal(Cents::new(0)), "0.00");
    }

    #[test]
    fn test_seam_round_trips_losslessly_in_both_directions() {
        // The dialect §1 canonical case: 44=500.05 <-> 50005 cents.
        let text = "500.05";
        let parsed = match parse_decimal_to_cents(text) {
            Ok(c) => c,
            Err(e) => panic!("parse failed: {e:?}"),
        };
        assert_eq!(parsed, Cents::new(50005));
        assert_eq!(render_cents_to_decimal(parsed), text);
    }

    #[test]
    fn test_price_scale_new_rejects_zero_tick() {
        assert_eq!(PriceScale::new(0), Err(PriceSeamError::ZeroTick));
    }

    #[test]
    fn test_price_scale_decimal_to_cents_accepts_on_tick() {
        let scale = match PriceScale::new(5) {
            Ok(s) => s,
            Err(e) => panic!("scale build failed: {e:?}"),
        };
        assert_eq!(scale.decimal_to_cents("500.05"), Ok(Cents::new(50005)));
    }

    #[test]
    fn test_price_scale_decimal_to_cents_rejects_off_tick() {
        let scale = match PriceScale::new(5) {
            Ok(s) => s,
            Err(e) => panic!("scale build failed: {e:?}"),
        };
        // 50003 is not a multiple of 5 cents.
        match scale.decimal_to_cents("500.03") {
            Err(PriceSeamError::OffTick {
                cents,
                tick_size_cents,
            }) => {
                assert_eq!(cents, 50003);
                assert_eq!(tick_size_cents, 5);
            }
            other => panic!("expected OffTick, got {other:?}"),
        }
    }

    #[test]
    fn test_price_scale_off_tick_is_not_wire_format() {
        let scale = match PriceScale::new(5) {
            Ok(s) => s,
            Err(e) => panic!("scale build failed: {e:?}"),
        };
        let err = match scale.decimal_to_cents("500.03") {
            Err(e) => e,
            Ok(c) => panic!("expected off-tick error, got {c:?}"),
        };
        assert!(
            !err.is_wire_format(),
            "off-tick is a business, not wire-format, failure"
        );
    }

    #[test]
    fn test_wire_format_errors_classify_as_wire_format() {
        assert!(PriceSeamError::Empty.is_wire_format());
        assert!(
            PriceSeamError::Malformed {
                value: "x".to_string()
            }
            .is_wire_format()
        );
        assert!(
            PriceSeamError::TooManyFractionalDigits {
                value: "1.234".to_string()
            }
            .is_wire_format()
        );
    }

    #[test]
    fn test_signed_cents_round_trips_including_a_maker_rebate() {
        for raw in [
            SignedCents::new(0),
            SignedCents::new(250),
            SignedCents::new(-10),
        ] {
            let rendered = render_signed_cents_to_decimal(raw);
            match parse_signed_decimal_to_cents(&rendered) {
                Ok(back) => assert_eq!(back, raw, "round trip of {rendered}"),
                Err(e) => panic!("signed parse of {rendered} failed: {e:?}"),
            }
        }
    }

    #[test]
    fn test_render_signed_cents_uses_sign_for_rebate_only() {
        assert_eq!(
            render_signed_cents_to_decimal(SignedCents::new(250)),
            "2.50"
        );
        assert_eq!(
            render_signed_cents_to_decimal(SignedCents::new(-10)),
            "-0.10"
        );
        assert_eq!(render_signed_cents_to_decimal(SignedCents::new(0)), "0.00");
    }

    #[test]
    fn test_tick_size_one_cent_admits_every_cents_value() {
        let scale = match PriceScale::new(1) {
            Ok(s) => s,
            Err(e) => panic!("scale build failed: {e:?}"),
        };
        // With a one-cent tick every integer-cents price is on-tick.
        for raw in ["0.00", "0.01", "500.05", "123.99"] {
            assert!(
                scale.decimal_to_cents(raw).is_ok(),
                "{raw} should be on a 1-cent tick"
            );
        }
    }
}
