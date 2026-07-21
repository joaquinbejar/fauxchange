//! `[microstructure.fees]` — the venue fee schedule, surfacing the upstream
//! `FeeSchedule` as declarative maker/taker basis-point config.
//!
//! Fees are **upstream**: `orderbook-rs` computes maker/taker fees at the leaf
//! from a `FeeSchedule`, and `fauxchange` exposes that schedule as config rather
//! than inventing a fee mechanism ([05 §4](../../../docs/05-microstructure-config.md#4-fee-schedules)).
//! A [`FeeConfig`] resolves to a `FeeSchedule` via [`FeeConfig::to_fee_schedule`];
//! the fee is computed in integer cents against the fill notional and reported on
//! the `ExecutionRecord` (`edge_cents` net of fee,
//! [01 §7](../../../docs/01-domain-model.md)).
//!
//! ## The checked-fee proof lives here
//!
//! The upstream `FeeSchedule::calculate_fee(notional, is_maker) -> i128`
//! **saturates** its intermediate product; the venue's `per_leg_fee` calls it, so
//! a config whose widest notional could reach that branch would journal a clamped,
//! unverifiable fee — a silent saturation that violates the repo's checked-money
//! rule (Override O-1). [`FeeConfig::validate_notional_bound`] is the startup proof
//! that makes the saturating branch **provably unreachable** by bounding config:
//! the venue rejects a configuration unless the widest admissible notional stays at
//! or below the upstream multiplication-safety bound
//! `FeeSchedule::max_guaranteed_exact_notional()`, and the worst-case fee still
//! fits the persisted `i64` cents. Within those bounds `calculate_fee` is exact
//! (equals `try_calculate_fee`), so fee replay and cross-protocol fee parity are
//! exact ([05 §4.1](../../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable)).

use option_chain_orderbook::FeeSchedule;
use serde::{Deserialize, Serialize};

use crate::microstructure::error::MicrostructureConfigError;

/// The basis-point denominator: 1 bps = 1 / 10_000 of the notional (matches the
/// upstream `FeeSchedule` `BPS_DENOMINATOR`).
const BPS_DENOMINATOR: u128 = 10_000;

/// `[microstructure.fees]` — the venue maker/taker fee schedule in basis points.
///
/// A negative `maker_bps` is a maker **rebate**; `taker_bps` must be non-negative
/// (the upstream `FeeSchedule` contract). The default is a zero-fee schedule
/// (no fees), so a venue with no `[microstructure.fees]` section charges nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeConfig {
    /// The maker fee in basis points; negative models a rebate (default `0`).
    #[serde(default)]
    pub maker_bps: i32,
    /// The taker fee in basis points; must be non-negative (default `0`).
    #[serde(default)]
    pub taker_bps: i32,
}

impl FeeConfig {
    /// Resolves this config to the upstream `FeeSchedule` applied at the leaf.
    ///
    /// The venue never constructs fees itself — it hands the schedule to
    /// `orderbook-rs`, which computes maker/taker fees in integer cents.
    #[must_use]
    #[inline]
    pub fn to_fee_schedule(self) -> FeeSchedule {
        FeeSchedule::new(self.maker_bps, self.taker_bps)
    }

    /// The larger of `|maker_bps|` and `|taker_bps|` — the worst-case fee rate
    /// used by the persisted-cents half of the checked-fee proof.
    #[must_use]
    #[inline]
    pub fn max_abs_bps(self) -> u32 {
        self.maker_bps
            .unsigned_abs()
            .max(self.taker_bps.unsigned_abs())
    }

    /// Validates the fee rates in isolation (before the notional-bound proof).
    ///
    /// # Errors
    ///
    /// [`MicrostructureConfigError::TakerFeeNegative`] if `taker_bps` is negative.
    #[inline]
    pub fn validate(self) -> Result<(), MicrostructureConfigError> {
        if self.taker_bps < 0 {
            return Err(MicrostructureConfigError::TakerFeeNegative {
                taker_bps: self.taker_bps,
            });
        }
        Ok(())
    }

    /// **The checked-fee startup proof.** Rejects this fee schedule unless the
    /// widest admissible `max_notional` (a resolved spec's
    /// `max_price_cents × max_order_qty`) keeps `FeeSchedule::calculate_fee` off
    /// its saturating branch **and** the worst-case fee fits the persisted `i64`
    /// cents.
    ///
    /// - **Part A (saturation unreachable):** `max_notional` must be at or below
    ///   the upstream `FeeSchedule::max_guaranteed_exact_notional()` (the minimum
    ///   over both legs of the `u128::MAX / |bps|` multiplication-safety bound). At
    ///   or below it, `try_calculate_fee` is always `Ok` and `calculate_fee`
    ///   returns the exact `⌊notional × |bps| / 10_000⌋` — the `saturating_mul` /
    ///   `i128::MAX` branch is unreachable.
    /// - **Part B (persisted fits i64):** the worst-case fee magnitude
    ///   `max_notional × max_abs_bps / 10_000` must be at or below `i64::MAX`, so a
    ///   fill's `fee_cents` records losslessly in a durable `BIGINT`.
    ///
    /// Part B implies Part A numerically, but both are checked and reported
    /// distinctly: Part A ties the guarantee to the upstream contract (and to the
    /// property test that asserts `try_calculate_fee` never errs on an accepted
    /// config), Part B to the DB-lossless persistence bound.
    ///
    /// # Errors
    ///
    /// - [`MicrostructureConfigError::FeeBoundUnprovable`] if part A fails;
    /// - [`MicrostructureConfigError::FeePersistOverflow`] if part B fails;
    /// - [`MicrostructureConfigError::ProofArithmeticOverflow`] if the checked
    ///   proof arithmetic overflows `u128` (unreachable for the `u64`-bounded
    ///   knobs; the proof fails loud rather than wrap).
    pub fn validate_notional_bound(
        self,
        max_notional: u128,
    ) -> Result<(), MicrostructureConfigError> {
        let schedule = self.to_fee_schedule();

        // Part A: the widest notional must stay within the upstream
        // multiplication-safety bound, so the saturating branch is unreachable.
        let guaranteed_bound = schedule.max_guaranteed_exact_notional();
        if max_notional > guaranteed_bound {
            return Err(MicrostructureConfigError::FeeBoundUnprovable {
                max_notional,
                guaranteed_bound,
                maker_bps: self.maker_bps,
                taker_bps: self.taker_bps,
            });
        }

        // Part B: the worst-case fee magnitude must fit the persisted i64 cents.
        // Within Part A's bound the product cannot overflow u128, but the multiply
        // is checked per the arithmetic rule (never saturating/wrapping).
        let max_abs_bps = self.max_abs_bps();
        let product = max_notional
            .checked_mul(u128::from(max_abs_bps))
            .ok_or(MicrostructureConfigError::ProofArithmeticOverflow)?;
        let fee_magnitude = product / BPS_DENOMINATOR;
        if fee_magnitude > i64::MAX as u128 {
            return Err(MicrostructureConfigError::FeePersistOverflow {
                fee_magnitude,
                max_notional,
                max_abs_bps,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fee_config_default_is_zero_fee() {
        let fees = FeeConfig::default();
        assert_eq!(fees.maker_bps, 0);
        assert_eq!(fees.taker_bps, 0);
        assert!(fees.to_fee_schedule().is_zero_fee());
    }

    #[test]
    fn test_fee_config_maker_rebate_is_negative_taker_positive() {
        let fees = FeeConfig {
            maker_bps: -10,
            taker_bps: 35,
        };
        let schedule = fees.to_fee_schedule();
        assert_eq!(schedule.maker_fee_bps, -10);
        assert_eq!(schedule.taker_fee_bps, 35);
        assert!(schedule.has_maker_rebate());
    }

    #[test]
    fn test_fee_config_deserialises_negative_maker_from_toml() {
        let fees: FeeConfig =
            toml::from_str("maker_bps = -10\ntaker_bps = 35\n").expect("fee config parses");
        assert_eq!(fees.maker_bps, -10);
        assert_eq!(fees.taker_bps, 35);
    }

    #[test]
    fn test_fee_config_rejects_unknown_field() {
        let error = toml::from_str::<FeeConfig>("maker_bps = 1\ntaker_fee = 2\n");
        assert!(error.is_err(), "an unknown field must be rejected");
    }

    #[test]
    fn test_fee_config_validate_rejects_negative_taker() {
        let fees = FeeConfig {
            maker_bps: -10,
            taker_bps: -1,
        };
        assert_eq!(
            fees.validate(),
            Err(MicrostructureConfigError::TakerFeeNegative { taker_bps: -1 })
        );
    }

    #[test]
    fn test_max_abs_bps_takes_the_larger_leg() {
        assert_eq!(
            FeeConfig {
                maker_bps: -40,
                taker_bps: 35
            }
            .max_abs_bps(),
            40
        );
        assert_eq!(
            FeeConfig {
                maker_bps: -2,
                taker_bps: 50
            }
            .max_abs_bps(),
            50
        );
    }

    #[test]
    fn test_notional_bound_accepts_realistic_config() {
        // $1,000,000 max price × 10_000 max qty = 10^12 cents-contracts notional,
        // at 35 bps → fee 3.5×10^9 cents, well within both bounds.
        let fees = FeeConfig {
            maker_bps: -10,
            taker_bps: 35,
        };
        let max_notional = 100_000_000u128 * 10_000u128;
        assert_eq!(fees.validate_notional_bound(max_notional), Ok(()));
    }

    #[test]
    fn test_notional_bound_zero_fee_never_rejects() {
        // A zero-fee schedule has an infinite guaranteed-exact bound (u128::MAX),
        // so no notional can be rejected.
        let fees = FeeConfig::default();
        assert_eq!(fees.validate_notional_bound(u128::MAX), Ok(()));
    }

    #[test]
    fn test_notional_bound_rejects_persisted_overflow() {
        // A notional whose fee magnitude exceeds i64::MAX is rejected (part B),
        // even though it stays under the u128 guaranteed-exact bound (part A) for
        // this small rate.
        let fees = FeeConfig {
            maker_bps: 0,
            taker_bps: 1,
        };
        // fee magnitude = notional × 1 / 10_000; choose notional so it exceeds i64::MAX.
        let max_notional = (i64::MAX as u128) * BPS_DENOMINATOR + BPS_DENOMINATOR;
        match fees.validate_notional_bound(max_notional) {
            Err(MicrostructureConfigError::FeePersistOverflow { .. }) => {}
            other => panic!("expected FeePersistOverflow, got {other:?}"),
        }
    }

    #[test]
    fn test_notional_bound_rejects_when_saturation_reachable() {
        // A huge rate shrinks the guaranteed-exact bound below the notional, so
        // part A rejects before part B (the saturating branch would be reachable).
        let fees = FeeConfig {
            maker_bps: 0,
            taker_bps: i32::MAX,
        };
        match fees.validate_notional_bound(u128::MAX) {
            Err(MicrostructureConfigError::FeeBoundUnprovable { .. }) => {}
            other => panic!("expected FeeBoundUnprovable, got {other:?}"),
        }
    }

    /// Rejection-matrix entry (#49): the `[microstructure.fees]` knobs are refused
    /// at load for every out-of-range shape — a negative taker rate, a fee whose
    /// widest notional would reach the upstream saturating branch (beyond the
    /// checked-fee-proof bound), and a fee whose worst-case magnitude would not fit
    /// the persisted `i64` cents. Each is a typed [`MicrostructureConfigError`]
    /// (folded into `ConfigError::Microstructure` at the config seam), never a
    /// silent acceptance.
    #[test]
    fn test_config_rejects_out_of_range_fee_bps() {
        // A negative taker rate — the upstream `FeeSchedule` contract forbids it.
        assert_eq!(
            FeeConfig {
                maker_bps: -10,
                taker_bps: -1,
            }
            .validate(),
            Err(MicrostructureConfigError::TakerFeeNegative { taker_bps: -1 })
        );

        // A fee rate so large the widest notional passes the checked-fee-proof
        // bound (part A): the saturating branch would be reachable — refused.
        let saturating = FeeConfig {
            maker_bps: 0,
            taker_bps: i32::MAX,
        };
        match saturating.validate_notional_bound(u128::MAX) {
            Err(MicrostructureConfigError::FeeBoundUnprovable { .. }) => {}
            other => {
                panic!(
                    "beyond the checked-fee-proof bound must be FeeBoundUnprovable, got {other:?}"
                )
            }
        }

        // A small rate but a notional whose worst-case fee exceeds `i64::MAX`
        // (part B): the persisted cents column could not record it — refused.
        let over_persist = (i64::MAX as u128) * BPS_DENOMINATOR + BPS_DENOMINATOR;
        let tiny_rate = FeeConfig {
            maker_bps: 0,
            taker_bps: 1,
        };
        match tiny_rate.validate_notional_bound(over_persist) {
            Err(MicrostructureConfigError::FeePersistOverflow { .. }) => {}
            other => {
                panic!(
                    "a fee past the persisted-i64 bound must be FeePersistOverflow, got {other:?}"
                )
            }
        }
    }
}
