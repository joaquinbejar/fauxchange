//! `[instruments."<SYM>".specs]` / `[microstructure.specs]` — per-instrument
//! contract specs (tick / lot / order-size limits) surfacing the upstream
//! `ContractSpecs` + `ValidationConfig`, plus the **venue-owned** price band the
//! upstream has no equivalent for.
//!
//! Tick size, lot size, and the maximum order quantity are **upstream**:
//! `orderbook-rs` enforces them at the leaf from a `ContractSpecs` (which derives a
//! `ValidationConfig`), and `fauxchange` exposes them as config
//! ([05 §7](../../../docs/05-microstructure-config.md#7-contract-specs-tick-and-lot)).
//! The `min_price_cents` / `max_price_cents` band is **venue-owned**: the verified
//! upstream `ValidationConfig` carries no price bound (checked against the pinned
//! `option-chain-orderbook` 0.7.0 / `orderbook-rs` 0.10.5), so the venue defines
//! its own admission band, applied **before matching** at every order-admission and
//! replay seam. `max_price_cents` also anchors the checked-fee proof and keeps the
//! persisted `BIGINT` cents columns lossless
//! ([05 §4.1](../../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable),
//! [governance-precedence §2.1](../../../docs/governance-precedence.md#21-cents-at-the-database-boundary-lossless-encoding)).
//!
//! ## Inheritance
//!
//! Each knob is optional in [`ContractSpecsConfig`]. A per-underlying
//! `[instruments."BTC".specs]` resolves over the venue-default `[microstructure.specs]`,
//! which in turn resolves over the hard-coded [`ResolvedContractSpecs::baseline`] —
//! so an unset knob inherits the venue default, and an unset venue default inherits
//! the baseline ([05 §7](../../../docs/05-microstructure-config.md#7-contract-specs-tick-and-lot)).

use option_chain_orderbook::ContractSpecs;
use serde::{Deserialize, Serialize};

use crate::exchange::Cents;
use crate::microstructure::error::{MicrostructureConfigError, PriceBoundError};

/// `[instruments."<SYM>".specs]` / `[microstructure.specs]` — the declarative
/// contract-spec knobs, each optional so an unset knob inherits the venue default.
///
/// - `tick_size_cents`, `lot_size`, `max_order_qty` surface the upstream
///   `ContractSpecs` (applied at the leaf);
/// - `min_price_cents`, `max_price_cents` are the **venue-owned** admission band
///   (no upstream equivalent).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractSpecsConfig {
    /// Minimum price increment in **cents** (upstream `ContractSpecs::tick_size`).
    #[serde(default)]
    pub tick_size_cents: Option<u64>,
    /// Minimum quantity increment in contracts (upstream `ContractSpecs::lot_size`).
    #[serde(default)]
    pub lot_size: Option<u64>,
    /// Venue-owned minimum admissible price in **cents**.
    #[serde(default)]
    pub min_price_cents: Option<u64>,
    /// Venue-owned maximum admissible price in **cents** (the admission cap).
    #[serde(default)]
    pub max_price_cents: Option<u64>,
    /// Maximum order quantity in contracts (upstream `ContractSpecs::max_order_size`).
    #[serde(default)]
    pub max_order_qty: Option<u64>,
}

impl ContractSpecsConfig {
    /// Resolves this config over `default`, filling each unset knob from the
    /// default, then validates the result.
    ///
    /// # Errors
    ///
    /// A [`MicrostructureConfigError`] if a resolved knob is out of range (a zero
    /// tick/lot/min-price/max-qty, or a `max_price_cents` below `min_price_cents`).
    pub fn resolve_over(
        self,
        default: ResolvedContractSpecs,
    ) -> Result<ResolvedContractSpecs, MicrostructureConfigError> {
        let resolved = ResolvedContractSpecs {
            tick_size_cents: self.tick_size_cents.unwrap_or(default.tick_size_cents),
            lot_size: self.lot_size.unwrap_or(default.lot_size),
            min_price_cents: self.min_price_cents.unwrap_or(default.min_price_cents),
            max_price_cents: self.max_price_cents.unwrap_or(default.max_price_cents),
            max_order_qty: self.max_order_qty.unwrap_or(default.max_order_qty),
        };
        resolved.validate()?;
        Ok(resolved)
    }
}

/// Fully-resolved contract specs after venue-default inheritance — every knob
/// concrete and range-validated.
///
/// `Serialize`/`Deserialize` are derived so the resolved specs ride inside the
/// portable [`MicrostructureConfig`](crate::microstructure::MicrostructureConfig)
/// carried by a recorded scenario bundle (the config manifest is part of the
/// determinism tuple). A deserialised value **bypasses** [`validate`](Self::validate)
/// (serde fills the private fields directly), but the values only reach a leaf via
/// [`to_contract_specs`](Self::to_contract_specs), which re-runs the upstream
/// `ContractSpecsBuilder` validation and returns a **typed** error for any
/// out-of-range knob — never a panic — and the bundle's fingerprint equality gate
/// refuses a config that does not match its recorded manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedContractSpecs {
    tick_size_cents: u64,
    lot_size: u64,
    min_price_cents: u64,
    max_price_cents: u64,
    max_order_qty: u64,
}

impl ResolvedContractSpecs {
    /// The hard-coded venue baseline every unset knob ultimately inherits from:
    /// a 1-cent tick, 1-contract lot, a 1-cent price floor, a `$1,000,000.00`
    /// (`100_000_000`-cent) price cap, and a `1_000_000`-contract max order.
    ///
    /// The cap is chosen so the widest default notional stays far inside the fee
    /// proof and the persisted `BIGINT` cents stay lossless
    /// ([governance-precedence §2.1](../../../docs/governance-precedence.md#21-cents-at-the-database-boundary-lossless-encoding)).
    #[must_use]
    #[inline]
    pub const fn baseline() -> Self {
        Self {
            tick_size_cents: 1,
            lot_size: 1,
            min_price_cents: 1,
            max_price_cents: 100_000_000,
            max_order_qty: 1_000_000,
        }
    }

    /// The price tick in **cents**.
    #[must_use]
    #[inline]
    pub const fn tick_size_cents(&self) -> u64 {
        self.tick_size_cents
    }

    /// The lot size in contracts.
    #[must_use]
    #[inline]
    pub const fn lot_size(&self) -> u64 {
        self.lot_size
    }

    /// The venue-owned minimum admissible price in **cents**.
    #[must_use]
    #[inline]
    pub const fn min_price_cents(&self) -> u64 {
        self.min_price_cents
    }

    /// The venue-owned maximum admissible price in **cents** (the admission cap).
    #[must_use]
    #[inline]
    pub const fn max_price_cents(&self) -> u64 {
        self.max_price_cents
    }

    /// The maximum order quantity in contracts.
    #[must_use]
    #[inline]
    pub const fn max_order_qty(&self) -> u64 {
        self.max_order_qty
    }

    /// Validates every knob is in range: tick / lot / min-price / max-qty at least
    /// one, and the `max_price_cents` cap at or above the `min_price_cents` floor.
    ///
    /// # Errors
    ///
    /// - [`MicrostructureConfigError::SpecKnobZero`] for a zero tick / lot /
    ///   min-price / max-qty;
    /// - [`MicrostructureConfigError::MaxPriceBelowMin`] if the cap is below the
    ///   floor.
    pub fn validate(&self) -> Result<(), MicrostructureConfigError> {
        for (field, value) in [
            ("tick_size_cents", self.tick_size_cents),
            ("lot_size", self.lot_size),
            ("min_price_cents", self.min_price_cents),
            ("max_order_qty", self.max_order_qty),
        ] {
            if value == 0 {
                return Err(MicrostructureConfigError::SpecKnobZero { field });
            }
        }
        if self.max_price_cents < self.min_price_cents {
            return Err(MicrostructureConfigError::MaxPriceBelowMin {
                min: self.min_price_cents,
                max: self.max_price_cents,
            });
        }
        Ok(())
    }

    /// The widest admissible notional in cents (`max_price_cents × max_order_qty`)
    /// — the input to the checked-fee proof.
    ///
    /// # Errors
    ///
    /// [`MicrostructureConfigError::ProofArithmeticOverflow`] if the product
    /// overflows `u128` (unreachable for the `u64`-bounded knobs; checked per the
    /// arithmetic rule).
    pub fn max_notional(&self) -> Result<u128, MicrostructureConfigError> {
        u128::from(self.max_price_cents)
            .checked_mul(u128::from(self.max_order_qty))
            .ok_or(MicrostructureConfigError::ProofArithmeticOverflow)
    }

    /// The upstream `ContractSpecs` this resolves to — the tick / lot / order-size
    /// limits `orderbook-rs` applies at the leaf (which derives its
    /// `ValidationConfig`). The venue-owned price band is **not** part of it (the
    /// upstream has no price bound); it is enforced separately via
    /// [`price_bounds`](Self::price_bounds).
    ///
    /// # Errors
    ///
    /// [`MicrostructureConfigError::ContractSpecsRejected`] if the upstream builder
    /// rejects the knobs (unreachable for the range-validated values; surfaced
    /// rather than unwrapped).
    pub fn to_contract_specs(&self) -> Result<ContractSpecs, MicrostructureConfigError> {
        ContractSpecs::builder()
            .tick_size(u128::from(self.tick_size_cents))
            .lot_size(self.lot_size)
            .min_order_size(1)
            .max_order_size(self.max_order_qty)
            .build()
            .map_err(|error| MicrostructureConfigError::ContractSpecsRejected {
                reason: error.to_string(),
            })
    }

    /// The venue-owned price-band admission bounds.
    #[must_use]
    #[inline]
    pub fn price_bounds(&self) -> PriceBounds {
        PriceBounds {
            min: Cents::new(self.min_price_cents),
            max: Cents::new(self.max_price_cents),
        }
    }
}

/// The venue-owned `[min_price_cents, max_price_cents]` admission band, checked per
/// order **before matching** at every order-admission and replay seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceBounds {
    min: Cents,
    max: Cents,
}

impl PriceBounds {
    /// The minimum admissible price in **cents**.
    #[must_use]
    #[inline]
    pub const fn min(&self) -> Cents {
        self.min
    }

    /// The maximum admissible price in **cents** (the admission cap).
    #[must_use]
    #[inline]
    pub const fn max(&self) -> Cents {
        self.max
    }

    /// Admits `price` if it lies within `[min, max]`, else rejects it — the
    /// venue-owned cap the order-admission seam applies before a price reaches the
    /// leaf.
    ///
    /// # Errors
    ///
    /// - [`PriceBoundError::BelowMin`] if `price` is below `min_price_cents`;
    /// - [`PriceBoundError::AboveMax`] if `price` is above `max_price_cents`.
    #[inline]
    pub fn admit(&self, price: Cents) -> Result<(), PriceBoundError> {
        let value = price.get();
        if value < self.min.get() {
            return Err(PriceBoundError::BelowMin {
                price: value,
                min: self.min.get(),
            });
        }
        if value > self.max.get() {
            return Err(PriceBoundError::AboveMax {
                price: value,
                max: self.max.get(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_baseline_is_range_valid() {
        assert_eq!(ResolvedContractSpecs::baseline().validate(), Ok(()));
    }

    #[test]
    fn test_resolve_over_inherits_unset_knobs_from_default() {
        // Only the tick is overridden; every other knob inherits the baseline.
        let config = ContractSpecsConfig {
            tick_size_cents: Some(5),
            ..ContractSpecsConfig::default()
        };
        let resolved = config
            .resolve_over(ResolvedContractSpecs::baseline())
            .expect("resolves");
        assert_eq!(resolved.tick_size_cents(), 5);
        assert_eq!(resolved.lot_size(), 1);
        assert_eq!(resolved.min_price_cents(), 1);
        assert_eq!(resolved.max_price_cents(), 100_000_000);
        assert_eq!(resolved.max_order_qty(), 1_000_000);
    }

    #[test]
    fn test_resolve_over_two_layers_per_underlying_wins() {
        // Venue default overrides the baseline max_price; the per-underlying layer
        // then overrides the tick — a full instrument OR underlying resolution.
        let venue_default = ContractSpecsConfig {
            max_price_cents: Some(200_000_000),
            ..ContractSpecsConfig::default()
        }
        .resolve_over(ResolvedContractSpecs::baseline())
        .expect("venue default resolves");
        let per_underlying = ContractSpecsConfig {
            tick_size_cents: Some(5),
            ..ContractSpecsConfig::default()
        }
        .resolve_over(venue_default)
        .expect("per-underlying resolves");
        assert_eq!(per_underlying.tick_size_cents(), 5);
        assert_eq!(per_underlying.max_price_cents(), 200_000_000);
    }

    #[test]
    fn test_resolve_rejects_zero_tick() {
        let config = ContractSpecsConfig {
            tick_size_cents: Some(0),
            ..ContractSpecsConfig::default()
        };
        assert_eq!(
            config.resolve_over(ResolvedContractSpecs::baseline()),
            Err(MicrostructureConfigError::SpecKnobZero {
                field: "tick_size_cents"
            })
        );
    }

    #[test]
    fn test_resolve_rejects_max_price_below_min() {
        let config = ContractSpecsConfig {
            min_price_cents: Some(1_000),
            max_price_cents: Some(500),
            ..ContractSpecsConfig::default()
        };
        assert_eq!(
            config.resolve_over(ResolvedContractSpecs::baseline()),
            Err(MicrostructureConfigError::MaxPriceBelowMin {
                min: 1_000,
                max: 500,
            })
        );
    }

    #[test]
    fn test_to_contract_specs_surfaces_tick_lot_and_max_qty() {
        let resolved = ContractSpecsConfig {
            tick_size_cents: Some(5),
            lot_size: Some(2),
            max_order_qty: Some(10_000),
            ..ContractSpecsConfig::default()
        }
        .resolve_over(ResolvedContractSpecs::baseline())
        .expect("resolves");
        let specs = resolved.to_contract_specs().expect("builds");
        assert_eq!(specs.tick_size(), 5);
        assert_eq!(specs.lot_size(), 2);
        assert_eq!(specs.min_order_size(), 1);
        assert_eq!(specs.max_order_size(), 10_000);
        // The derived validation config carries the same tick/lot/max-qty.
        let validation = specs.to_validation_config();
        assert_eq!(validation.tick_size(), Some(5));
        assert_eq!(validation.max_order_size(), Some(10_000));
    }

    #[test]
    fn test_price_bounds_admit_within_band() {
        let bounds = ResolvedContractSpecs::baseline().price_bounds();
        assert_eq!(bounds.admit(Cents::new(50_000)), Ok(()));
        assert_eq!(bounds.admit(bounds.min()), Ok(()));
        assert_eq!(bounds.admit(bounds.max()), Ok(()));
    }

    #[test]
    fn test_price_bounds_reject_above_cap() {
        let resolved = ContractSpecsConfig {
            max_price_cents: Some(1_000),
            ..ContractSpecsConfig::default()
        }
        .resolve_over(ResolvedContractSpecs::baseline())
        .expect("resolves");
        let bounds = resolved.price_bounds();
        assert_eq!(
            bounds.admit(Cents::new(1_001)),
            Err(PriceBoundError::AboveMax {
                price: 1_001,
                max: 1_000,
            })
        );
    }

    #[test]
    fn test_price_bounds_reject_below_floor() {
        let resolved = ContractSpecsConfig {
            min_price_cents: Some(100),
            ..ContractSpecsConfig::default()
        }
        .resolve_over(ResolvedContractSpecs::baseline())
        .expect("resolves");
        let bounds = resolved.price_bounds();
        assert_eq!(
            bounds.admit(Cents::new(99)),
            Err(PriceBoundError::BelowMin {
                price: 99,
                min: 100,
            })
        );
    }

    #[test]
    fn test_max_notional_is_price_times_qty() {
        let resolved = ContractSpecsConfig {
            max_price_cents: Some(100_000_000),
            max_order_qty: Some(10_000),
            ..ContractSpecsConfig::default()
        }
        .resolve_over(ResolvedContractSpecs::baseline())
        .expect("resolves");
        assert_eq!(resolved.max_notional(), Ok(100_000_000u128 * 10_000u128));
    }

    #[test]
    fn test_specs_config_rejects_unknown_field() {
        let error = toml::from_str::<ContractSpecsConfig>("tick_size_cent = 5\n");
        assert!(error.is_err(), "an unknown spec field must be rejected");
    }
}
