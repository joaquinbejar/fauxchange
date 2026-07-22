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
use crate::microstructure::error::{
    MicrostructureConfigError, OrderAdmissionError, PriceBoundError,
};

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

    /// The durable `BIGINT` (`i64`) domain ceiling every persisted-cents knob must
    /// stay within so a fill records losslessly in the durable store.
    pub const DB_DOMAIN_CEILING: u64 = i64::MAX as u64;

    /// Validates every knob is in range: tick / lot / min-price / max-qty at least
    /// one, the persisted `max_price_cents` / `max_order_qty` at or below the
    /// durable `BIGINT` (`i64`) domain, and the `max_price_cents` cap at or above
    /// the `min_price_cents` floor.
    ///
    /// # Errors
    ///
    /// - [`MicrostructureConfigError::SpecKnobZero`] for a zero tick / lot /
    ///   min-price / max-qty;
    /// - [`MicrostructureConfigError::SpecKnobAboveDbDomain`] if `max_price_cents`
    ///   or `max_order_qty` exceeds `i64::MAX` (the durable `BIGINT` domain);
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
        // The two persisted knobs must fit the durable BIGINT (i64) domain, so a
        // fill admitted against them is never rejected by the store's ValueRange at
        // commit. `max_price_cents` also anchors the widest notional the checked-fee
        // proof bounds, so an over-domain price would fail loud there too — this
        // gives the specific, actionable diagnostic first, at boot.
        for (field, value) in [
            ("max_price_cents", self.max_price_cents),
            ("max_order_qty", self.max_order_qty),
        ] {
            if value > Self::DB_DOMAIN_CEILING {
                return Err(MicrostructureConfigError::SpecKnobAboveDbDomain {
                    field,
                    value,
                    ceiling: Self::DB_DOMAIN_CEILING,
                });
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

    /// Admits a full order (`limit_price` + `quantity`) against these resolved specs:
    /// the venue-owned **price band**, the **tick** (price a whole multiple), the
    /// **lot** (quantity a whole multiple), and the **max order quantity** — the
    /// venue-owned per-symbol admission the sequenced order path runs **before** the
    /// leaf (#114 item 5).
    ///
    /// A **pure function** of the resolved specs (no wall-clock / RNG), so the same
    /// order resolves the same accept/reject decision live and on replay. `limit_price`
    /// is `None` for a market order, which carries no price to band- or tick-check;
    /// the lot and max-quantity checks always apply. The tick / lot moduli are guarded
    /// against a zero divisor (a validated config has both `>= 1`, but a directly
    /// **deserialized** [`ResolvedContractSpecs`] bypasses [`validate`](Self::validate)),
    /// so a degenerate spec never panics on the admission path.
    ///
    /// # Errors
    ///
    /// An [`OrderAdmissionError`]: [`PriceBand`](OrderAdmissionError::PriceBand) when
    /// the price is outside the band, [`OffTick`](OrderAdmissionError::OffTick) when
    /// the price is off-tick, [`OffLot`](OrderAdmissionError::OffLot) when the quantity
    /// is off-lot, or [`AboveMaxQty`](OrderAdmissionError::AboveMaxQty) when the
    /// quantity exceeds the maximum.
    pub fn admit_order(
        &self,
        limit_price: Option<Cents>,
        quantity: u64,
    ) -> Result<(), OrderAdmissionError> {
        if let Some(price) = limit_price {
            // Band first (the venue-owned cap), then tick alignment.
            self.price_bounds().admit(price)?;
            let value = price.get();
            // The zero guard keeps a degenerate (deserialized, unvalidated) tick from
            // dividing by zero; a validated config always has `tick >= 1`.
            if self.tick_size_cents != 0 && !value.is_multiple_of(self.tick_size_cents) {
                return Err(OrderAdmissionError::OffTick {
                    price: value,
                    tick: self.tick_size_cents,
                });
            }
        }
        if self.lot_size != 0 && !quantity.is_multiple_of(self.lot_size) {
            return Err(OrderAdmissionError::OffLot {
                quantity,
                lot: self.lot_size,
            });
        }
        if quantity > self.max_order_qty {
            return Err(OrderAdmissionError::AboveMaxQty {
                quantity,
                max: self.max_order_qty,
            });
        }
        Ok(())
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
    fn test_resolve_rejects_max_price_above_db_domain() {
        // A persisted `max_price_cents` above the durable BIGINT (i64) domain is a
        // typed startup rejection — an over-domain price would be admitted yet
        // rejected by the durable store's ValueRange at the first fill (#114 item 2).
        let over_domain = (i64::MAX as u64) + 1;
        assert_eq!(
            ContractSpecsConfig {
                max_price_cents: Some(over_domain),
                ..ContractSpecsConfig::default()
            }
            .resolve_over(ResolvedContractSpecs::baseline()),
            Err(MicrostructureConfigError::SpecKnobAboveDbDomain {
                field: "max_price_cents",
                value: over_domain,
                ceiling: i64::MAX as u64,
            })
        );
    }

    #[test]
    fn test_resolve_rejects_max_order_qty_above_db_domain() {
        // The mirror bound: an over-domain `max_order_qty` is refused at boot.
        let over_domain = (i64::MAX as u64) + 1;
        assert_eq!(
            ContractSpecsConfig {
                max_order_qty: Some(over_domain),
                ..ContractSpecsConfig::default()
            }
            .resolve_over(ResolvedContractSpecs::baseline()),
            Err(MicrostructureConfigError::SpecKnobAboveDbDomain {
                field: "max_order_qty",
                value: over_domain,
                ceiling: i64::MAX as u64,
            })
        );
    }

    #[test]
    fn test_db_domain_ceiling_is_admissible() {
        // Exactly `i64::MAX` is inside the domain (the ceiling is inclusive).
        let resolved = ContractSpecsConfig {
            max_price_cents: Some(i64::MAX as u64),
            max_order_qty: Some(i64::MAX as u64),
            ..ContractSpecsConfig::default()
        }
        .resolve_over(ResolvedContractSpecs::baseline())
        .expect("the i64::MAX ceiling itself is admissible");
        assert_eq!(resolved.max_price_cents(), i64::MAX as u64);
        assert_eq!(resolved.max_order_qty(), i64::MAX as u64);
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
    fn test_admit_order_enforces_band_tick_lot_and_max_qty() {
        // A resolved spec with a 5-cent tick, 2-contract lot, [100, 1_000] band, and a
        // 10-contract max order — the per-symbol gate `admit_order` runs before the leaf.
        let specs = ContractSpecsConfig {
            tick_size_cents: Some(5),
            lot_size: Some(2),
            min_price_cents: Some(100),
            max_price_cents: Some(1_000),
            max_order_qty: Some(10),
        }
        .resolve_over(ResolvedContractSpecs::baseline())
        .expect("resolves");

        // A price + quantity satisfying every knob is admitted.
        assert_eq!(specs.admit_order(Some(Cents::new(500)), 4), Ok(()));
        // Band edges are inclusive.
        assert_eq!(specs.admit_order(Some(Cents::new(100)), 2), Ok(()));
        assert_eq!(specs.admit_order(Some(Cents::new(1_000)), 10), Ok(()));

        // Off-tick price (503 not a multiple of 5).
        assert_eq!(
            specs.admit_order(Some(Cents::new(503)), 4),
            Err(OrderAdmissionError::OffTick {
                price: 503,
                tick: 5,
            })
        );
        // Off-lot quantity (3 not a multiple of 2).
        assert_eq!(
            specs.admit_order(Some(Cents::new(500)), 3),
            Err(OrderAdmissionError::OffLot {
                quantity: 3,
                lot: 2,
            })
        );
        // Above the max order quantity.
        assert_eq!(
            specs.admit_order(Some(Cents::new(500)), 12),
            Err(OrderAdmissionError::AboveMaxQty {
                quantity: 12,
                max: 10,
            })
        );
        // Below the band floor — the band is folded in as the `PriceBand` variant.
        assert_eq!(
            specs.admit_order(Some(Cents::new(50)), 4),
            Err(OrderAdmissionError::PriceBand(PriceBoundError::BelowMin {
                price: 50,
                min: 100,
            }))
        );
        // A market order (no limit price) skips band + tick but still lot/max-checks.
        assert_eq!(specs.admit_order(None, 4), Ok(()));
        assert_eq!(
            specs.admit_order(None, 3),
            Err(OrderAdmissionError::OffLot {
                quantity: 3,
                lot: 2,
            })
        );
    }

    #[test]
    fn test_admit_order_never_divides_by_zero_on_degenerate_deserialized_specs() {
        // A directly-deserialized `ResolvedContractSpecs` bypasses `validate`, so a
        // hostile bundle could carry a zero tick / lot. `admit_order` must never panic
        // on the modulo (rules/global_rules.md: no panic on inbound) — it guards the
        // zero divisor and simply skips that knob's alignment check.
        let degenerate: ResolvedContractSpecs = serde_json::from_str(
            r#"{"tick_size_cents":0,"lot_size":0,"min_price_cents":1,"max_price_cents":100000000,"max_order_qty":1000000}"#,
        )
        .expect("deserializes");
        assert_eq!(degenerate.admit_order(Some(Cents::new(3)), 3), Ok(()));
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

    /// Rejection-matrix entry (#49): every out-of-range `[instruments."X".specs]`
    /// knob is refused at load with a typed [`MicrostructureConfigError`] (folded
    /// into `ConfigError::Microstructure` at the config seam) — a zero tick, a zero
    /// lot, and a `max_price_cents` below the `min_price_cents` floor. A degenerate
    /// spec never reaches a leaf.
    #[test]
    fn test_config_rejects_out_of_range_contract_specs() {
        // A zero tick — an increment must be at least one cent.
        assert_eq!(
            ContractSpecsConfig {
                tick_size_cents: Some(0),
                ..ContractSpecsConfig::default()
            }
            .resolve_over(ResolvedContractSpecs::baseline()),
            Err(MicrostructureConfigError::SpecKnobZero {
                field: "tick_size_cents"
            })
        );

        // A zero lot — a quantity increment must be at least one contract.
        assert_eq!(
            ContractSpecsConfig {
                lot_size: Some(0),
                ..ContractSpecsConfig::default()
            }
            .resolve_over(ResolvedContractSpecs::baseline()),
            Err(MicrostructureConfigError::SpecKnobZero { field: "lot_size" })
        );

        // A `max_price_cents` cap below the `min_price_cents` floor — an empty band.
        assert_eq!(
            ContractSpecsConfig {
                min_price_cents: Some(1_000),
                max_price_cents: Some(500),
                ..ContractSpecsConfig::default()
            }
            .resolve_over(ResolvedContractSpecs::baseline()),
            Err(MicrostructureConfigError::MaxPriceBelowMin {
                min: 1_000,
                max: 500,
            })
        );

        // A zero max order quantity — a per-order cap must admit at least one contract.
        assert_eq!(
            ContractSpecsConfig {
                max_order_qty: Some(0),
                ..ContractSpecsConfig::default()
            }
            .resolve_over(ResolvedContractSpecs::baseline()),
            Err(MicrostructureConfigError::SpecKnobZero {
                field: "max_order_qty"
            })
        );
    }
}
