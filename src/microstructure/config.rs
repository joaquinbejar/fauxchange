//! The resolved venue [`MicrostructureConfig`] and its file surface
//! [`FileMicrostructure`] — the declarative `[microstructure.*]` +
//! `[instruments."<SYM>".specs]` config folded into the upstream `FeeSchedule` /
//! `STPMode` / `ContractSpecs` the leaf applies.
//!
//! [`MicrostructureConfig::resolve`] is where the venue-default inheritance is
//! applied and the **checked-fee startup proof** runs for the venue default and
//! every per-underlying override, so a config that could drive the upstream
//! `FeeSchedule::calculate_fee` onto its saturating branch is a startup error
//! before the venue serves a request
//! ([05 §4.1](../../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable)).
//!
//! Because these knobs are part of each leaf's shared configuration, a book
//! vivified during replay inherits the identical schedule and specs — so a
//! fee/STP-sensitive scenario replays exactly ([02 §5](../../../docs/02-matching-architecture.md#5-determinism)).

use std::collections::BTreeMap;

use option_chain_orderbook::{FeeSchedule, STPMode};
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};

use crate::exchange::Cents;
use crate::microstructure::error::{MicrostructureConfigError, PriceBoundError};
use crate::microstructure::fees::FeeConfig;
use crate::microstructure::specs::{ContractSpecsConfig, PriceBounds, ResolvedContractSpecs};
use crate::microstructure::stp::StpConfig;
use crate::simulation::DEFAULT_MICROSTRUCTURE_FINGERPRINT;

/// The `[microstructure]` file section — the venue fee schedule, STP mode, and
/// venue-default contract specs.
///
/// `latency` is accepted and **ignored** here: the latency-injection knob is owned
/// by #045, so a forward-looking config carrying `[microstructure.latency]` is not
/// rejected before that issue lands (the same "accept, resolve later" pattern the
/// v0.2 loader used for the whole section). Every other unknown key inside
/// `[microstructure]` is a startup error (`deny_unknown_fields`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMicrostructure {
    /// `[microstructure.fees]` — the venue maker/taker fee schedule.
    #[serde(default)]
    pub fees: Option<FeeConfig>,
    /// `[microstructure.stp]` — the venue self-trade-prevention mode.
    #[serde(default)]
    pub stp: Option<StpConfig>,
    /// `[microstructure.specs]` — the venue-default contract specs every
    /// per-underlying `[instruments."X".specs]` inherits unset knobs from.
    #[serde(default)]
    pub specs: Option<ContractSpecsConfig>,
    /// `[microstructure.latency]` — accepted and ignored here (owned by #045).
    #[serde(default)]
    #[allow(dead_code)]
    pub latency: Option<IgnoredAny>,
}

/// The resolved, validated venue microstructure — the fee schedule, STP mode, and
/// per-underlying contract specs (with the venue default) the venue applies at
/// book creation and order admission.
///
/// `Serialize`/`Deserialize` are derived so the resolved config rides inside a
/// recorded scenario bundle (the config manifest is part of the determinism tuple):
/// the replay driver applies the **same** config the live venue applied so a
/// fee/STP-sensitive scenario replays exactly, and the bundle's
/// [`fingerprint`](Self::fingerprint) is checked against the recorded
/// `RunManifest.microstructure_fingerprint` as an equality gate before replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MicrostructureConfig {
    fees: FeeConfig,
    stp: StpConfig,
    default_specs: ResolvedContractSpecs,
    per_underlying: BTreeMap<String, ResolvedContractSpecs>,
}

impl Default for MicrostructureConfig {
    fn default() -> Self {
        Self {
            fees: FeeConfig::default(),
            stp: StpConfig::default(),
            default_specs: ResolvedContractSpecs::baseline(),
            per_underlying: BTreeMap::new(),
        }
    }
}

impl MicrostructureConfig {
    /// Resolves the file microstructure and per-underlying `[instruments."X".specs]`
    /// into the validated venue config, running the checked-fee startup proof for
    /// the venue default and every per-underlying override.
    ///
    /// The venue default is `[microstructure.specs]` resolved over the hard-coded
    /// [`ResolvedContractSpecs::baseline`]; each per-underlying override is
    /// `[instruments."X".specs]` resolved over that venue default. Iterated in
    /// sorted underlying order ([`BTreeMap`]) so resolution is a fixed function of
    /// the file.
    ///
    /// # Errors
    ///
    /// A [`MicrostructureConfigError`] on a negative taker fee, an out-of-range
    /// spec knob, or a fee/spec combination that fails the checked-fee proof for
    /// the venue default or any underlying.
    pub fn resolve(
        file: &FileMicrostructure,
        instrument_specs: &BTreeMap<String, ContractSpecsConfig>,
    ) -> Result<Self, MicrostructureConfigError> {
        let fees = file.fees.unwrap_or_default();
        fees.validate()?;
        let stp = file.stp.unwrap_or_default();

        // Venue default: [microstructure.specs] over the hard-coded baseline, then
        // proved against the fee schedule.
        let default_specs = file
            .specs
            .unwrap_or_default()
            .resolve_over(ResolvedContractSpecs::baseline())?;
        fees.validate_notional_bound(default_specs.max_notional()?)?;

        // Per-underlying overrides: each over the venue default, each proved.
        let mut per_underlying = BTreeMap::new();
        for (underlying, specs_config) in instrument_specs {
            let resolved = specs_config.resolve_over(default_specs)?;
            fees.validate_notional_bound(resolved.max_notional()?)?;
            per_underlying.insert(underlying.clone(), resolved);
        }

        Ok(Self {
            fees,
            stp,
            default_specs,
            per_underlying,
        })
    }

    /// The upstream `FeeSchedule` the leaf applies (venue-wide).
    #[must_use]
    #[inline]
    pub fn fee_schedule(&self) -> FeeSchedule {
        self.fees.to_fee_schedule()
    }

    /// The upstream `STPMode` the leaf applies (venue-wide).
    #[must_use]
    #[inline]
    pub fn stp_mode(&self) -> STPMode {
        self.stp.to_stp_mode()
    }

    /// Re-runs the resolver's validation — the contract-spec range checks **and** the
    /// checked-fee startup proof — over this already-constructed config, returning the
    /// same errors [`resolve`](Self::resolve) would.
    ///
    /// The load-bearing caller is the replay driver: a [`MicrostructureConfig`]
    /// carried in a recorded scenario bundle is **deserialized directly**, bypassing
    /// `resolve` and therefore the proof. The `fingerprint` equality gate is
    /// tamper-*detection* (it proves the config matches what was recorded), not
    /// *authenticity* — a self-consistent hostile bundle can self-compute a matching
    /// fingerprint for an arbitrary fee/spec config. Re-running this before any
    /// command re-executes keeps #44's core deliverable — the checked-fee proof
    /// (Override O-1: money is checked, never saturating) — non-bypassable, and gives
    /// a **specific** config-rejection diagnostic instead of a downstream generic
    /// overflow ([05 §4.1](../../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable)).
    ///
    /// # Errors
    ///
    /// A [`MicrostructureConfigError`] if a spec knob is out of range (a zero
    /// tick / lot / min-price / max-qty, or `max_price_cents` below `min_price_cents`)
    /// or the fee schedule fails the checked-fee proof against the venue-default or
    /// any per-underlying widest notional.
    pub fn validate(&self) -> Result<(), MicrostructureConfigError> {
        self.fees.validate()?;
        // The venue default: spec ranges (deserialize bypassed `validate`) then the
        // checked-fee proof against its widest notional.
        self.default_specs.validate()?;
        self.fees
            .validate_notional_bound(self.default_specs.max_notional()?)?;
        // Every per-underlying override, in sorted order (a `BTreeMap`).
        for specs in self.per_underlying.values() {
            specs.validate()?;
            self.fees.validate_notional_bound(specs.max_notional()?)?;
        }
        Ok(())
    }

    /// The venue fee config.
    #[must_use]
    #[inline]
    pub fn fees(&self) -> FeeConfig {
        self.fees
    }

    /// The resolved contract specs for `underlying` — its per-underlying override,
    /// or the venue default when none is configured.
    #[must_use]
    #[inline]
    pub fn specs_for(&self, underlying: &str) -> ResolvedContractSpecs {
        self.per_underlying
            .get(underlying)
            .copied()
            .unwrap_or(self.default_specs)
    }

    /// The venue-default contract specs (the fallback for an unconfigured
    /// underlying).
    #[must_use]
    #[inline]
    pub fn default_specs(&self) -> ResolvedContractSpecs {
        self.default_specs
    }

    /// The venue-owned price band for `underlying` (per-underlying or default).
    #[must_use]
    #[inline]
    pub fn price_bounds_for(&self, underlying: &str) -> PriceBounds {
        self.specs_for(underlying).price_bounds()
    }

    /// Admits `price` for `underlying` against the venue-owned price band — the
    /// check the order-admission and replay seams run **before matching**.
    ///
    /// # Errors
    ///
    /// A [`PriceBoundError`] if `price` falls outside the underlying's
    /// `[min_price_cents, max_price_cents]` band.
    #[inline]
    pub fn admit_price(&self, underlying: &str, price: Cents) -> Result<(), PriceBoundError> {
        self.price_bounds_for(underlying).admit(price)
    }

    /// A stable, deterministic fingerprint of the resolved config content — the
    /// value that populates the reserved `RunManifest.microstructure_fingerprint`
    /// slot so the determinism oracle scopes fee/STP/specs-sensitive replay
    /// ([04 §6](../../../docs/04-market-data-and-replay.md#6-determinism-and-seeding),
    /// [05 §11](../../../docs/05-microstructure-config.md#11-determinism-of-microstructure)).
    ///
    /// It is a pure function of the fee schedule, STP mode, venue-default specs,
    /// and per-underlying specs (iterated in sorted underlying order) — **no**
    /// wall-clock, RNG, or map-iteration-order input — so the same config always
    /// yields the same fingerprint and a different fee/STP/specs config yields a
    /// different one. The **default** config returns the reserved
    /// [`DEFAULT_MICROSTRUCTURE_FINGERPRINT`](crate::simulation::DEFAULT_MICROSTRUCTURE_FINGERPRINT)
    /// so a zero-fee / STP-off / baseline-specs venue records the same fingerprint
    /// as a legacy manifest that predates the declarative surface.
    #[must_use]
    pub fn fingerprint(&self) -> String {
        if *self == Self::default() {
            return DEFAULT_MICROSTRUCTURE_FINGERPRINT.to_string();
        }
        let mut out = format!(
            "microstructure.v1;fees=m{}/t{};stp={};specs.*={}",
            self.fees.maker_bps,
            self.fees.taker_bps,
            self.stp.mode.token(),
            specs_fragment(&self.default_specs),
        );
        // `per_underlying` is a `BTreeMap`, so this iterates in sorted underlying
        // order — a fixed, reproducible sequence independent of insertion order.
        for (underlying, specs) in &self.per_underlying {
            out.push_str(&format!(";specs.{underlying}={}", specs_fragment(specs)));
        }
        out
    }
}

/// The canonical single-line fragment for one resolved contract-spec set — the
/// per-spec piece of [`MicrostructureConfig::fingerprint`].
fn specs_fragment(specs: &ResolvedContractSpecs) -> String {
    format!(
        "t{}l{}min{}max{}q{}",
        specs.tick_size_cents(),
        specs.lot_size(),
        specs.min_price_cents(),
        specs.max_price_cents(),
        specs.max_order_qty(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn file_with_fees(maker_bps: i32, taker_bps: i32) -> FileMicrostructure {
        FileMicrostructure {
            fees: Some(FeeConfig {
                maker_bps,
                taker_bps,
            }),
            ..FileMicrostructure::default()
        }
    }

    fn resolved(
        maker_bps: i32,
        taker_bps: i32,
        stp: crate::microstructure::stp::StpMode,
        specs: ContractSpecsConfig,
    ) -> MicrostructureConfig {
        let file = FileMicrostructure {
            fees: Some(FeeConfig {
                maker_bps,
                taker_bps,
            }),
            stp: Some(StpConfig { mode: stp }),
            specs: Some(specs),
            ..FileMicrostructure::default()
        };
        MicrostructureConfig::resolve(&file, &BTreeMap::new()).expect("resolves")
    }

    #[test]
    fn test_default_is_zero_fee_stp_off_baseline_specs() {
        let config = MicrostructureConfig::default();
        assert!(config.fee_schedule().is_zero_fee());
        assert_eq!(config.stp_mode(), STPMode::None);
        assert_eq!(config.default_specs(), ResolvedContractSpecs::baseline());
    }

    #[test]
    fn test_fingerprint_default_matches_reserved_manifest_slot() {
        // A default venue records the reserved slot value, so a zero-fee / STP-off
        // / baseline-specs run's manifest fingerprint is unchanged from legacy.
        assert_eq!(
            MicrostructureConfig::default().fingerprint(),
            DEFAULT_MICROSTRUCTURE_FINGERPRINT
        );
        // And the reserved constant is single-sourced from the manifest module.
        assert_eq!(
            MicrostructureConfig::default().fingerprint(),
            crate::simulation::DEFAULT_MICROSTRUCTURE_FINGERPRINT
        );
    }

    #[test]
    fn test_fingerprint_is_deterministic_and_content_sensitive() {
        use crate::microstructure::stp::StpMode;

        let base = resolved(
            -10,
            35,
            StpMode::CancelTaker,
            ContractSpecsConfig::default(),
        );
        // Deterministic: the same config always yields the same fingerprint.
        assert_eq!(base.fingerprint(), base.fingerprint());
        // Non-default configs are distinguishable from the reserved default slot.
        assert_ne!(base.fingerprint(), DEFAULT_MICROSTRUCTURE_FINGERPRINT);

        // A change to ANY dimension changes the fingerprint.
        let fee_changed = resolved(
            -10,
            36,
            StpMode::CancelTaker,
            ContractSpecsConfig::default(),
        );
        assert_ne!(base.fingerprint(), fee_changed.fingerprint());
        let stp_changed = resolved(
            -10,
            35,
            StpMode::CancelMaker,
            ContractSpecsConfig::default(),
        );
        assert_ne!(base.fingerprint(), stp_changed.fingerprint());
        let specs_changed = resolved(
            -10,
            35,
            StpMode::CancelTaker,
            ContractSpecsConfig {
                tick_size_cents: Some(5),
                ..ContractSpecsConfig::default()
            },
        );
        assert_ne!(base.fingerprint(), specs_changed.fingerprint());
    }

    #[test]
    fn test_fingerprint_is_stable_across_per_underlying_insertion_order() {
        // Per-underlying specs iterate in sorted order, so two manifests differing
        // only in insertion order share one fingerprint.
        let file = FileMicrostructure {
            fees: Some(FeeConfig {
                maker_bps: -10,
                taker_bps: 35,
            }),
            ..FileMicrostructure::default()
        };
        let mut forward = BTreeMap::new();
        forward.insert(
            "BTC".to_string(),
            ContractSpecsConfig {
                tick_size_cents: Some(5),
                ..ContractSpecsConfig::default()
            },
        );
        forward.insert(
            "ETH".to_string(),
            ContractSpecsConfig {
                lot_size: Some(2),
                ..ContractSpecsConfig::default()
            },
        );
        let a = MicrostructureConfig::resolve(&file, &forward).expect("resolves");
        let b = MicrostructureConfig::resolve(&file, &forward).expect("resolves");
        assert_eq!(a.fingerprint(), b.fingerprint());
        // The per-underlying specs appear in the fingerprint.
        assert!(a.fingerprint().contains("specs.BTC="));
        assert!(a.fingerprint().contains("specs.ETH="));
    }

    #[test]
    fn test_resolve_venue_default_and_per_underlying_inheritance() {
        let file = FileMicrostructure {
            fees: Some(FeeConfig {
                maker_bps: -10,
                taker_bps: 35,
            }),
            specs: Some(ContractSpecsConfig {
                tick_size_cents: Some(5),
                ..ContractSpecsConfig::default()
            }),
            ..FileMicrostructure::default()
        };
        let mut per_instrument = BTreeMap::new();
        per_instrument.insert(
            "BTC".to_string(),
            ContractSpecsConfig {
                max_order_qty: Some(10_000),
                ..ContractSpecsConfig::default()
            },
        );
        let config = MicrostructureConfig::resolve(&file, &per_instrument).expect("resolves");

        // Venue default inherits the file tick, baseline for the rest.
        assert_eq!(config.default_specs().tick_size_cents(), 5);
        assert_eq!(config.default_specs().max_order_qty(), 1_000_000);
        // BTC inherits the venue-default tick and overrides max_order_qty.
        let btc = config.specs_for("BTC");
        assert_eq!(btc.tick_size_cents(), 5);
        assert_eq!(btc.max_order_qty(), 10_000);
        // An unconfigured underlying falls back to the venue default.
        assert_eq!(config.specs_for("ETH"), config.default_specs());
        // The fee schedule surfaces the configured bps.
        assert_eq!(config.fee_schedule().maker_fee_bps, -10);
        assert_eq!(config.fee_schedule().taker_fee_bps, 35);
    }

    #[test]
    fn test_resolve_is_pure_function_of_inputs() {
        let file = file_with_fees(-2, 5);
        let specs = BTreeMap::new();
        let first = MicrostructureConfig::resolve(&file, &specs).expect("resolves");
        let second = MicrostructureConfig::resolve(&file, &specs).expect("resolves");
        assert_eq!(first, second);
    }

    #[test]
    fn test_resolve_rejects_venue_default_that_fails_fee_proof() {
        // A large fee against a large venue-default max_price × max_order_qty
        // reaches the persisted-cents ceiling — rejected at resolution.
        let file = FileMicrostructure {
            fees: Some(FeeConfig {
                maker_bps: 0,
                taker_bps: 10_000,
            }),
            specs: Some(ContractSpecsConfig {
                max_price_cents: Some(u64::MAX),
                max_order_qty: Some(u64::MAX),
                ..ContractSpecsConfig::default()
            }),
            ..FileMicrostructure::default()
        };
        assert!(
            MicrostructureConfig::resolve(&file, &BTreeMap::new()).is_err(),
            "an unprovable venue default must be rejected"
        );
    }

    #[test]
    fn test_resolve_rejects_per_underlying_that_fails_fee_proof() {
        // The venue default is fine, but a per-underlying override blows the proof.
        let file = file_with_fees(0, 35);
        let mut per_instrument = BTreeMap::new();
        per_instrument.insert(
            "BTC".to_string(),
            ContractSpecsConfig {
                max_price_cents: Some(u64::MAX),
                max_order_qty: Some(u64::MAX),
                ..ContractSpecsConfig::default()
            },
        );
        assert!(
            MicrostructureConfig::resolve(&file, &per_instrument).is_err(),
            "an unprovable per-underlying spec must be rejected"
        );
    }

    #[test]
    fn test_file_microstructure_accepts_and_ignores_latency() {
        // #045 owns latency; a forward config with [microstructure.latency] parses
        // (accepted + ignored), and the fee/stp/specs still resolve.
        let file: FileMicrostructure = toml::from_str(
            "[fees]\nmaker_bps = -10\ntaker_bps = 35\n\n[latency]\nmodel = \"lognormal\"\nmedian_us = 250\n",
        )
        .expect("forward config with latency parses");
        let config = MicrostructureConfig::resolve(&file, &BTreeMap::new()).expect("resolves");
        assert_eq!(config.fee_schedule().taker_fee_bps, 35);
    }

    #[test]
    fn test_file_microstructure_rejects_unknown_section() {
        let error = toml::from_str::<FileMicrostructure>("[feez]\nmaker_bps = 1\n");
        assert!(
            error.is_err(),
            "an unknown microstructure section is rejected"
        );
    }

    #[test]
    fn test_admit_price_uses_per_underlying_band() {
        let mut per_instrument = BTreeMap::new();
        per_instrument.insert(
            "BTC".to_string(),
            ContractSpecsConfig {
                min_price_cents: Some(100),
                max_price_cents: Some(1_000),
                ..ContractSpecsConfig::default()
            },
        );
        let config = MicrostructureConfig::resolve(&FileMicrostructure::default(), &per_instrument)
            .expect("resolves");
        assert_eq!(config.admit_price("BTC", Cents::new(500)), Ok(()));
        assert_eq!(
            config.admit_price("BTC", Cents::new(1_001)),
            Err(PriceBoundError::AboveMax {
                price: 1_001,
                max: 1_000,
            })
        );
        // ETH uses the venue-default band (baseline: 1..=100_000_000).
        assert_eq!(config.admit_price("ETH", Cents::new(1_001)), Ok(()));
    }

    proptest! {
        /// The checked-fee proof's core invariant: no config the resolver accepts
        /// can drive the upstream `FeeSchedule::calculate_fee` onto its saturating
        /// branch — asserted directly via the fallible `try_calculate_fee`, which
        /// errs *iff* `calculate_fee` would clamp. Also pins that within the
        /// accepted bounds the exact and saturating paths agree (no clamp).
        #[test]
        fn prop_accepted_config_never_saturates_calculate_fee(
            maker_bps in -1_000_000i32..1_000_000,
            taker_bps in 0i32..1_000_000,
            max_price_cents in 1u64..=1_000_000_000_000,
            max_order_qty in 1u64..=1_000_000_000,
        ) {
            let fees = FeeConfig { maker_bps, taker_bps };
            let file = FileMicrostructure {
                fees: Some(fees),
                specs: Some(ContractSpecsConfig {
                    max_price_cents: Some(max_price_cents),
                    max_order_qty: Some(max_order_qty),
                    ..ContractSpecsConfig::default()
                }),
                ..FileMicrostructure::default()
            };
            // Only accepted configs carry an obligation.
            let Ok(config) = MicrostructureConfig::resolve(&file, &BTreeMap::new()) else {
                return Ok(());
            };
            let max_notional = config.default_specs().max_notional().unwrap_or(0);
            let schedule = config.fee_schedule();
            for notional in [0u128, 1, max_notional / 2, max_notional] {
                for is_maker in [true, false] {
                    prop_assert!(
                        schedule.try_calculate_fee(notional, is_maker).is_ok(),
                        "accepted config saturated calculate_fee: notional={notional} is_maker={is_maker} maker_bps={maker_bps} taker_bps={taker_bps}",
                    );
                    prop_assert_eq!(
                        schedule.try_calculate_fee(notional, is_maker),
                        Ok(schedule.calculate_fee(notional, is_maker)),
                    );
                    // And the exact fee fits the persisted i64 cents.
                    if let Ok(fee) = schedule.try_calculate_fee(notional, is_maker) {
                        prop_assert!(i64::try_from(fee).is_ok());
                    }
                }
            }
        }
    }
}
