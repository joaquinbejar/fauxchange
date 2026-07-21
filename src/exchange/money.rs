//! Integer-cents money newtypes for the venue seam.
//!
//! Every monetary quantity in `fauxchange` is **integer cents** internally and
//! on every boundary — REST DTO, WS message, FIX field, journal record, and DB
//! column — never `f64` and (repo Override O-1) never `rust_decimal`
//! ([ADR-0003](../../../docs/adr/0003-money-as-integer-cents.md),
//! [governance-precedence §2](../../../docs/governance-precedence.md)). One
//! venue price tick is one cent, so the [`Cents`] → `u128` conversion at the
//! `orderbook-rs` leaf seam ([`Cents::as_u128`]) is the identity on the integer
//! value.
//!
//! Three newtypes carry money ([01 §3](../../../docs/01-domain-model.md)):
//! [`Cents`] (absolute), [`SignedCents`] (P&L / edge), and [`Notional`]
//! (`price × quantity` products, which can exceed `u64`). Their inner fields are
//! private, construction validates, arithmetic is **checked** (never
//! `saturating_*` / `wrapping_*`), and the wire form is the bare integer via
//! `#[serde(transparent)]`.

use serde::{Deserialize, Serialize};

/// Error raised by the money newtypes' validated construction and checked
/// arithmetic.
///
/// This is a local, focused domain error: the crate-wide `VenueError` lands in
/// a later issue, at which point [`MoneyError::Overflow`] maps onto
/// `VenueError::Overflow` (HTTP `500`) and [`MoneyError::NegativeCents`] onto
/// `VenueError::InvalidOrder` (HTTP `400`)
/// ([01 §11](../../../docs/01-domain-model.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MoneyError {
    /// A checked addition or multiplication of cents overflowed its integer
    /// width. Never a wrap or a saturate — the operation fails loudly.
    #[error("monetary arithmetic overflow")]
    Overflow,
    /// An absolute-cents value was constructed from a negative signed integer,
    /// violating the non-negative invariant of [`Cents`]. The offending value
    /// (cents) is carried for the error message.
    #[error("negative value {0} cents is invalid for absolute Cents")]
    NegativeCents(i64),
}

/// Absolute money in **integer cents** — prices, fees, premiums.
///
/// The inner `u64` is private so the non-negative invariant cannot be bypassed.
/// The wire form is a bare integer (`#[serde(transparent)]`), never a float.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cents(u64);

impl Cents {
    /// Constructs `Cents` from an in-range unsigned cents value.
    ///
    /// Infallible: every `u64` is a valid non-negative cents amount. Use
    /// [`Cents::try_new`] when the value arrives as a signed integer (a DB
    /// `BIGINT`, or the result of signed arithmetic) and the non-negative
    /// invariant must be re-established.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::Cents;
    /// let price = Cents::new(500);
    /// assert_eq!(price.get(), 500);
    /// ```
    #[must_use]
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Constructs `Cents` from a signed integer, enforcing the non-negative
    /// invariant.
    ///
    /// This is the admission constructor for a value that reaches the venue as
    /// an `i64` — a `BIGINT` column read, or the output of signed P&L
    /// arithmetic that is expected to be non-negative.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyError::NegativeCents`] if `value` is negative.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::{Cents, MoneyError};
    /// assert_eq!(Cents::try_new(500)?.get(), 500);
    /// assert_eq!(Cents::try_new(-1), Err(MoneyError::NegativeCents(-1)));
    /// # Ok::<(), MoneyError>(())
    /// ```
    #[inline]
    pub fn try_new(value: i64) -> Result<Self, MoneyError> {
        if value < 0 {
            return Err(MoneyError::NegativeCents(value));
        }
        // `value >= 0`, so the cast to `u64` is lossless.
        Ok(Self(value as u64))
    }

    /// Returns the raw cents value.
    #[must_use]
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Widens to `u128` for the `orderbook-rs` leaf submission path.
    ///
    /// One venue tick is one cent, so this conversion is the **identity** on
    /// the integer value: the number of cents is exactly the `u128` price the
    /// leaf book receives ([02 §3](../../../docs/02-matching-architecture.md)).
    #[must_use]
    #[inline]
    pub const fn as_u128(self) -> u128 {
        self.0 as u128
    }

    /// Checked cents addition.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyError::Overflow`] if the sum exceeds `u64::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::{Cents, MoneyError};
    /// let total = Cents::new(100).checked_add(Cents::new(50))?;
    /// assert_eq!(total.get(), 150);
    /// # Ok::<(), MoneyError>(())
    /// ```
    #[inline]
    pub fn checked_add(self, rhs: Self) -> Result<Self, MoneyError> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(MoneyError::Overflow)
    }

    /// Checked cents subtraction.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyError::Overflow`] if `rhs` exceeds `self` (an underflow
    /// below zero, which is invalid for absolute cents).
    #[inline]
    pub fn checked_sub(self, rhs: Self) -> Result<Self, MoneyError> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(MoneyError::Overflow)
    }

    /// Checked multiplication of cents by an integer factor (e.g. a quantity).
    ///
    /// # Errors
    ///
    /// Returns [`MoneyError::Overflow`] if the product exceeds `u64::MAX`. Use
    /// [`Notional::from_price_quantity`] when the wider `u128` product is
    /// wanted instead of a bounded `Cents`.
    #[inline]
    pub fn checked_mul(self, factor: u64) -> Result<Self, MoneyError> {
        self.0
            .checked_mul(factor)
            .map(Self)
            .ok_or(MoneyError::Overflow)
    }
}

/// Signed money in **integer cents** — realised / unrealised P&L, per-fill edge,
/// and any value that can be negative (a maker rebate).
///
/// The wire form is a bare signed integer (`#[serde(transparent)]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SignedCents(i64);

impl SignedCents {
    /// Constructs `SignedCents` from a signed cents value.
    ///
    /// Infallible: every `i64` is a valid signed cents amount (there is no
    /// invariant to violate — negatives are meaningful here).
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::SignedCents;
    /// let rebate = SignedCents::new(-2);
    /// assert_eq!(rebate.get(), -2);
    /// ```
    #[must_use]
    #[inline]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns the raw signed cents value.
    #[must_use]
    #[inline]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Checked signed-cents addition.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyError::Overflow`] if the sum leaves the `i64` range.
    #[inline]
    pub fn checked_add(self, rhs: Self) -> Result<Self, MoneyError> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(MoneyError::Overflow)
    }

    /// Checked signed-cents subtraction.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyError::Overflow`] if the difference leaves the `i64`
    /// range.
    #[inline]
    pub fn checked_sub(self, rhs: Self) -> Result<Self, MoneyError> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(MoneyError::Overflow)
    }
}

/// A `price × quantity` product in **integer cents** — notional exposure.
///
/// A `u64` cents price times a `u64` quantity can exceed `u64`, so notional is
/// carried in `u128`. It is a *computed* value used for exposure and the
/// fee-bound proof; it is never a persisted money column
/// ([governance-precedence §2.1](../../../docs/governance-precedence.md)). The
/// wire form is a bare integer (`#[serde(transparent)]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Notional(u128);

impl Notional {
    /// Constructs `Notional` from a raw `u128` cents value.
    ///
    /// Infallible: every `u128` is a valid notional. Prefer
    /// [`Notional::from_price_quantity`] to build one from a price and a
    /// quantity so the multiplication stays checked.
    #[must_use]
    #[inline]
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// Returns the raw notional cents value.
    #[must_use]
    #[inline]
    pub const fn get(self) -> u128 {
        self.0
    }

    /// Computes the notional `price × quantity` with checked multiplication.
    ///
    /// The product of a `u64` cents price and a `u64` quantity always fits in
    /// `u128` (max `(2^64 - 1)^2 < 2^128`), so this cannot overflow for the
    /// leaf types; the checked form is kept per the arithmetic rule and so the
    /// signature stays honest if the input widths ever change.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyError::Overflow`] if the `u128` product overflows.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::{Cents, Notional, MoneyError};
    /// let exposure = Notional::from_price_quantity(Cents::new(500), 3)?;
    /// assert_eq!(exposure.get(), 1_500);
    /// # Ok::<(), MoneyError>(())
    /// ```
    #[inline]
    pub fn from_price_quantity(price: Cents, quantity: u64) -> Result<Self, MoneyError> {
        price
            .as_u128()
            .checked_mul(u128::from(quantity))
            .map(Self)
            .ok_or(MoneyError::Overflow)
    }

    /// Checked notional addition (aggregating exposure across contracts).
    ///
    /// # Errors
    ///
    /// Returns [`MoneyError::Overflow`] if the sum exceeds `u128::MAX`.
    #[inline]
    pub fn checked_add(self, rhs: Self) -> Result<Self, MoneyError> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(MoneyError::Overflow)
    }

    /// Checked notional multiplication by an integer factor.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyError::Overflow`] if the product exceeds `u128::MAX`.
    #[inline]
    pub fn checked_mul(self, factor: u128) -> Result<Self, MoneyError> {
        self.0
            .checked_mul(factor)
            .map(Self)
            .ok_or(MoneyError::Overflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cents_new_roundtrips_value() {
        let c = Cents::new(1_234);
        assert_eq!(c.get(), 1_234);
    }

    #[test]
    fn test_cents_try_new_accepts_non_negative() {
        match Cents::try_new(0) {
            Ok(c) => assert_eq!(c.get(), 0),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
        match Cents::try_new(i64::MAX) {
            Ok(c) => assert_eq!(c.get(), i64::MAX as u64),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }

    #[test]
    fn test_cents_try_new_rejects_negative() {
        assert_eq!(Cents::try_new(-1), Err(MoneyError::NegativeCents(-1)));
        assert_eq!(
            Cents::try_new(i64::MIN),
            Err(MoneyError::NegativeCents(i64::MIN))
        );
    }

    #[test]
    fn test_cents_as_u128_is_identity_on_value() {
        assert_eq!(Cents::new(50_000).as_u128(), 50_000_u128);
        assert_eq!(Cents::new(u64::MAX).as_u128(), u128::from(u64::MAX));
    }

    #[test]
    fn test_cents_checked_add_sums_within_range() {
        match Cents::new(100).checked_add(Cents::new(50)) {
            Ok(c) => assert_eq!(c.get(), 150),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }

    #[test]
    fn test_cents_checked_add_overflow_is_typed_error() {
        assert_eq!(
            Cents::new(u64::MAX).checked_add(Cents::new(1)),
            Err(MoneyError::Overflow)
        );
    }

    #[test]
    fn test_cents_checked_sub_underflow_is_typed_error() {
        assert_eq!(
            Cents::new(0).checked_sub(Cents::new(1)),
            Err(MoneyError::Overflow)
        );
    }

    #[test]
    fn test_cents_checked_mul_overflow_is_typed_error() {
        assert_eq!(
            Cents::new(u64::MAX).checked_mul(2),
            Err(MoneyError::Overflow)
        );
    }

    #[test]
    fn test_signed_cents_new_allows_negative() {
        assert_eq!(SignedCents::new(-42).get(), -42);
    }

    #[test]
    fn test_signed_cents_checked_add_overflow_is_typed_error() {
        assert_eq!(
            SignedCents::new(i64::MAX).checked_add(SignedCents::new(1)),
            Err(MoneyError::Overflow)
        );
    }

    #[test]
    fn test_signed_cents_checked_sub_underflow_is_typed_error() {
        assert_eq!(
            SignedCents::new(i64::MIN).checked_sub(SignedCents::new(1)),
            Err(MoneyError::Overflow)
        );
    }

    #[test]
    fn test_notional_from_price_quantity_multiplies_exactly() {
        match Notional::from_price_quantity(Cents::new(50_000), 10) {
            Ok(n) => assert_eq!(n.get(), 500_000_u128),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }

    #[test]
    fn test_notional_from_price_quantity_max_leaf_inputs_fit_u128() {
        // The widest leaf inputs still fit u128, so this never overflows.
        match Notional::from_price_quantity(Cents::new(u64::MAX), u64::MAX) {
            Ok(n) => assert_eq!(n.get(), u128::from(u64::MAX) * u128::from(u64::MAX)),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }

    #[test]
    fn test_notional_checked_add_overflow_is_typed_error() {
        assert_eq!(
            Notional::new(u128::MAX).checked_add(Notional::new(1)),
            Err(MoneyError::Overflow)
        );
    }

    #[test]
    fn test_notional_checked_mul_overflow_is_typed_error() {
        assert_eq!(
            Notional::new(u128::MAX).checked_mul(2),
            Err(MoneyError::Overflow)
        );
    }

    #[test]
    fn test_cents_serialises_as_bare_integer() {
        let json = match serde_json::to_string(&Cents::new(12_345)) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(json, "12345");
    }

    #[test]
    fn test_signed_cents_serialises_as_bare_integer() {
        let json = match serde_json::to_string(&SignedCents::new(-7)) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(json, "-7");
    }

    #[test]
    fn test_notional_serialises_as_bare_integer() {
        let json = match serde_json::to_string(&Notional::new(9_876_543_210)) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(json, "9876543210");
    }

    #[test]
    fn test_cents_deserialises_from_bare_integer() {
        match serde_json::from_str::<Cents>("42") {
            Ok(c) => assert_eq!(c, Cents::new(42)),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }
}
