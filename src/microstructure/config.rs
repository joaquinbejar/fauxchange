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

use option_chain_orderbook::{FeeSchedule, STPMode, SymbolParser};
use serde::{Deserialize, Serialize};

use crate::exchange::Cents;
use crate::microstructure::error::{MicrostructureConfigError, PriceBoundError};
use crate::microstructure::fees::FeeConfig;
use crate::microstructure::latency::{FileLatency, LatencyConfig};
use crate::microstructure::specs::{ContractSpecsConfig, PriceBounds, ResolvedContractSpecs};
use crate::microstructure::stp::StpConfig;
use crate::simulation::DEFAULT_MICROSTRUCTURE_FINGERPRINT;

/// The `[microstructure]` file section — the venue fee schedule, STP mode,
/// venue-default contract specs, and latency-injection distribution.
///
/// Every unknown key inside `[microstructure]` is a startup error
/// (`deny_unknown_fields`).
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
    /// `[microstructure.instrument_specs."<KEY>"]` — per-instrument contract-spec
    /// overrides keyed by a **full option symbol** (`UNDERLYING-YYYYMMDD-STRIKE-STYLE`)
    /// OR a bare **underlying** (#114 item 5). A full-symbol key resolves **before**
    /// its underlying's override; an underlying key resolves before the venue
    /// default. Standalone from the `[instruments.*]` seed table, so a per-symbol
    /// override needs no seed instrument. On a key collision with an
    /// `[instruments."X".specs]` entry, this dedicated section wins.
    #[serde(default)]
    pub instrument_specs: Option<BTreeMap<String, ContractSpecsConfig>>,
    /// `[microstructure.latency]` — the seeded latency-injection distribution
    /// (#045). Absent ⇒ no latency injection.
    #[serde(default)]
    pub latency: Option<FileLatency>,
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
///
/// `Eq` is **not** derived: the [`latency`](Self::latency) config carries an `f64`
/// `sigma` (`normal` / `lognormal`), for which `Eq`'s reflexivity contract does not
/// hold. `PartialEq` is enough for the fingerprint's default check and the bundle's
/// equality; no consumer bounds on `Eq` (consistent with `Config` and
/// `ScenarioBundle`, which are also `PartialEq`-only).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MicrostructureConfig {
    fees: FeeConfig,
    stp: StpConfig,
    default_specs: ResolvedContractSpecs,
    per_underlying: BTreeMap<String, ResolvedContractSpecs>,
    /// Per-**symbol** contract-spec overrides keyed by the full option symbol
    /// (#114 item 5), resolved over the underlying's specs (or the venue default).
    /// `#[serde(default)]` so a legacy bundle without it decodes as empty, preserving
    /// the reserved default fingerprint and the recorded manifest fingerprint of a
    /// venue that predates per-symbol specs.
    #[serde(default)]
    per_symbol: BTreeMap<String, ResolvedContractSpecs>,
    /// The seeded latency-injection distribution (#045). `#[serde(default)]` so a
    /// legacy bundle without it decodes as [`LatencyConfig::Disabled`], preserving
    /// the reserved default fingerprint.
    #[serde(default)]
    latency: LatencyConfig,
}

impl Default for MicrostructureConfig {
    fn default() -> Self {
        Self {
            fees: FeeConfig::default(),
            stp: StpConfig::default(),
            default_specs: ResolvedContractSpecs::baseline(),
            per_underlying: BTreeMap::new(),
            per_symbol: BTreeMap::new(),
            latency: LatencyConfig::default(),
        }
    }
}

impl MicrostructureConfig {
    /// Resolves the file microstructure and per-underlying `[instruments."X".specs]`
    /// into the validated venue config, running the checked-fee startup proof for
    /// the venue default and every per-underlying override.
    ///
    /// The venue default is `[microstructure.specs]` resolved over the hard-coded
    /// [`ResolvedContractSpecs::baseline`]; each **per-underlying** override resolves
    /// over that venue default, and each **per-symbol** override resolves over its
    /// underlying's specs (its per-underlying override if present, else the venue
    /// default). A key in `instrument_specs` that parses as a full option symbol
    /// (`UNDERLYING-YYYYMMDD-STRIKE-STYLE`, via the upstream [`SymbolParser`]) is a
    /// per-symbol override; any other key is an underlying (#114 item 5). Both maps
    /// iterate in sorted key order ([`BTreeMap`]) so resolution is a fixed function
    /// of the file.
    ///
    /// # Errors
    ///
    /// A [`MicrostructureConfigError`] on a negative taker fee, an out-of-range
    /// spec knob, or a fee/spec combination that fails the checked-fee proof for
    /// the venue default, any underlying, or any symbol.
    pub fn resolve(
        file: &FileMicrostructure,
        instrument_specs: &BTreeMap<String, ContractSpecsConfig>,
    ) -> Result<Self, MicrostructureConfigError> {
        let fees = file.fees.unwrap_or_default();
        fees.validate()?;
        let stp = file.stp.unwrap_or_default();

        // Latency: resolve + validate the `[microstructure.latency]` distribution
        // (missing/negative params, non-finite/negative sigma, min > max).
        let latency = match &file.latency {
            Some(file_latency) => file_latency.resolve()?,
            None => LatencyConfig::default(),
        };

        // Venue default: [microstructure.specs] over the hard-coded baseline, then
        // proved against the fee schedule.
        let default_specs = file
            .specs
            .unwrap_or_default()
            .resolve_over(ResolvedContractSpecs::baseline())?;
        fees.validate_notional_bound(default_specs.max_notional()?)?;

        // Pass 1 — per-**underlying** overrides (a key that is NOT a full option
        // symbol): each over the venue default, each proved. A full-symbol key is
        // deferred to pass 2 so a per-symbol override can inherit its underlying's
        // resolved specs.
        let mut per_underlying = BTreeMap::new();
        for (key, specs_config) in instrument_specs {
            if SymbolParser::parse(key).is_ok() {
                continue;
            }
            let resolved = specs_config.resolve_over(default_specs)?;
            fees.validate_notional_bound(resolved.max_notional()?)?;
            per_underlying.insert(key.clone(), resolved);
        }

        // Pass 2 — per-**symbol** overrides (a full option-symbol key): each over its
        // underlying's specs (the per-underlying override if present, else the venue
        // default), each proved. Deterministic: the underlying's specs are resolved
        // in pass 1, the key set is a sorted `BTreeMap`, and the underlying is the
        // upstream parser's canonical mapping — no wall-clock / RNG / iteration-order
        // input.
        let mut per_symbol = BTreeMap::new();
        for (key, specs_config) in instrument_specs {
            let Ok(parsed) = SymbolParser::parse(key) else {
                continue;
            };
            let base = per_underlying
                .get(parsed.underlying())
                .copied()
                .unwrap_or(default_specs);
            let resolved = specs_config.resolve_over(base)?;
            fees.validate_notional_bound(resolved.max_notional()?)?;
            per_symbol.insert(key.clone(), resolved);
        }

        Ok(Self {
            fees,
            stp,
            default_specs,
            per_underlying,
            per_symbol,
            latency,
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
    /// tick / lot / min-price / max-qty, a persisted knob above the durable `BIGINT`
    /// domain, or `max_price_cents` below `min_price_cents`) or the fee schedule
    /// fails the checked-fee proof against the venue-default or any per-underlying /
    /// per-symbol widest notional.
    pub fn validate(&self) -> Result<(), MicrostructureConfigError> {
        self.fees.validate()?;
        // The latency distribution (deserialize bypassed `FileLatency::resolve`).
        self.latency.validate()?;
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
        // Every per-symbol override (#114 item 5), in sorted order.
        for specs in self.per_symbol.values() {
            specs.validate()?;
            self.fees.validate_notional_bound(specs.max_notional()?)?;
        }
        Ok(())
    }

    /// The resolved latency-injection distribution the gateway edge draws against
    /// per inbound message (#045). [`LatencyConfig::Disabled`] when unconfigured.
    #[must_use]
    #[inline]
    pub fn latency(&self) -> LatencyConfig {
        self.latency
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

    /// The resolved contract specs for a full option `symbol`, resolving in the
    /// **symbol-specific → underlying → venue-default** fallback order (#114 item 5):
    /// its per-symbol override if one is configured, else its underlying's
    /// per-underlying override (via [`specs_for`](Self::specs_for)), else the venue
    /// default.
    ///
    /// A **pure function** of the shared config — the symbol → underlying mapping is
    /// the upstream [`SymbolParser`] (never hand-parsed), and both override maps are
    /// point lookups — so a book vivified during replay resolves identical specs. A
    /// `symbol` that does not parse falls back to the venue default.
    #[must_use]
    pub fn specs_for_symbol(&self, symbol: &str) -> ResolvedContractSpecs {
        if let Some(specs) = self.per_symbol.get(symbol) {
            return *specs;
        }
        match SymbolParser::parse(symbol) {
            Ok(parsed) => self.specs_for(parsed.underlying()),
            Err(_) => self.default_specs,
        }
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
        self.profile_for(underlying).price_bounds()
    }

    /// Resolves the active [`MicrostructureProfile`] for `underlying` — the fee
    /// schedule, STP mode, contract specs, latency distribution, and venue-owned
    /// price band, with every **unset** per-instrument knob **inherited from the
    /// venue default** (#046, [05 §7](../../../docs/05-microstructure-config.md#7-contract-specs-tick-and-lot)).
    ///
    /// This is the per-instrument resolution the order path consults ([`admit_price`](Self::admit_price)
    /// resolves through it). It is a **pure function** of the shared config — no
    /// wall-clock, RNG, or map-iteration-order input — so a book vivified during
    /// replay resolves an identical profile and a profiled instrument replays
    /// exactly ([02 §5](../../../docs/02-matching-architecture.md#5-determinism)).
    ///
    /// Only the contract specs (and their price band) carry a per-underlying
    /// override surface today; the fee schedule, STP mode, and latency distribution
    /// are venue-wide, so every underlying inherits the venue default for them. The
    /// per-instrument **persona** knob joins this profile with #047.
    #[must_use]
    pub fn profile_for(&self, underlying: &str) -> MicrostructureProfile {
        MicrostructureProfile {
            fees: self.fees,
            stp: self.stp,
            specs: self.specs_for(underlying),
            latency: self.latency,
        }
    }

    /// Resolves the active [`MicrostructureProfile`] for a full option `symbol`,
    /// resolving its contract specs in the **symbol-specific → underlying →
    /// venue-default** fallback order (#114 item 5) via
    /// [`specs_for_symbol`](Self::specs_for_symbol). The fee schedule, STP mode, and
    /// latency distribution are venue-wide, so they are the same as
    /// [`profile_for`](Self::profile_for). A pure function of the shared config, so a
    /// profiled symbol resolves identically during replay.
    #[must_use]
    pub fn profile_for_symbol(&self, symbol: &str) -> MicrostructureProfile {
        MicrostructureProfile {
            fees: self.fees,
            stp: self.stp,
            specs: self.specs_for_symbol(symbol),
            latency: self.latency,
        }
    }

    /// Admits `price` for `underlying` against the venue-owned price band — the
    /// check the order-admission and replay seams run **before matching**, resolved
    /// through the per-instrument [`profile_for`](Self::profile_for).
    ///
    /// # Errors
    ///
    /// A [`PriceBoundError`] if `price` falls outside the underlying's
    /// `[min_price_cents, max_price_cents]` band.
    #[inline]
    pub fn admit_price(&self, underlying: &str, price: Cents) -> Result<(), PriceBoundError> {
        self.profile_for(underlying).admit_price(price)
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
        // Per-symbol overrides (#114 item 5), also in sorted order under a distinct
        // `specs.sym.` prefix so they never collide with an underlying fragment and a
        // per-symbol change yields a distinct fingerprint. Appended AFTER the
        // per-underlying fragments, so a config with no per-symbol override records
        // the identical fingerprint it did before per-symbol specs existed.
        for (symbol, specs) in &self.per_symbol {
            out.push_str(&format!(";specs.sym.{symbol}={}", specs_fragment(specs)));
        }
        // The latency distribution (empty fragment when disabled), so a latency-only
        // config change still yields a distinct fingerprint.
        out.push_str(&self.latency.fingerprint_fragment());
        out
    }
}

/// The **resolved microstructure profile** for a single instrument / underlying —
/// the active fee schedule, STP mode, contract specs, latency distribution, and
/// venue-owned price band, with every unset per-instrument knob **inherited from
/// the venue default**.
///
/// Produced by [`MicrostructureConfig::profile_for`] on the order path. Because it
/// is a pure function of the shared config (`Copy`, no interior state), a book
/// vivified during replay resolves an identical profile — per-instrument
/// microstructure does not break determinism ([05 §11](../../../docs/05-microstructure-config.md#11-determinism-of-microstructure)).
///
/// `Eq` is **not** derived: the [`latency`](Self::latency) config carries an `f64`
/// `sigma`, for which `Eq`'s reflexivity contract does not hold (consistent with
/// [`MicrostructureConfig`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MicrostructureProfile {
    fees: FeeConfig,
    stp: StpConfig,
    specs: ResolvedContractSpecs,
    latency: LatencyConfig,
}

impl MicrostructureProfile {
    /// The venue fee config for this instrument.
    #[must_use]
    #[inline]
    pub fn fees(&self) -> FeeConfig {
        self.fees
    }

    /// The upstream `FeeSchedule` the leaf applies for this instrument.
    #[must_use]
    #[inline]
    pub fn fee_schedule(&self) -> FeeSchedule {
        self.fees.to_fee_schedule()
    }

    /// The upstream `STPMode` the leaf applies for this instrument.
    #[must_use]
    #[inline]
    pub fn stp_mode(&self) -> STPMode {
        self.stp.to_stp_mode()
    }

    /// The resolved contract specs for this instrument (its per-underlying override,
    /// or the inherited venue default).
    #[must_use]
    #[inline]
    pub fn specs(&self) -> ResolvedContractSpecs {
        self.specs
    }

    /// The latency-injection distribution for this instrument (venue-wide today).
    #[must_use]
    #[inline]
    pub fn latency(&self) -> LatencyConfig {
        self.latency
    }

    /// The venue-owned price-band admission bounds for this instrument.
    #[must_use]
    #[inline]
    pub fn price_bounds(&self) -> PriceBounds {
        self.specs.price_bounds()
    }

    /// Admits `price` against this instrument's venue-owned price band — the check
    /// the order-admission and replay seams run **before matching**.
    ///
    /// # Errors
    ///
    /// A [`PriceBoundError`] if `price` falls outside the instrument's
    /// `[min_price_cents, max_price_cents]` band.
    #[inline]
    pub fn admit_price(&self, price: Cents) -> Result<(), PriceBoundError> {
        self.price_bounds().admit(price)
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
    fn test_specs_for_symbol_resolves_symbol_then_underlying_then_default() {
        // #114 item 5: a full-symbol override resolves BEFORE the underlying default,
        // and an unlisted symbol falls back (symbol → underlying → venue-default).
        let file = FileMicrostructure {
            fees: Some(FeeConfig {
                maker_bps: -10,
                taker_bps: 35,
            }),
            ..FileMicrostructure::default()
        };
        let mut specs = BTreeMap::new();
        // A per-underlying override for BTC (max_order_qty).
        specs.insert(
            "BTC".to_string(),
            ContractSpecsConfig {
                max_order_qty: Some(10_000),
                ..ContractSpecsConfig::default()
            },
        );
        // A per-symbol override for one BTC contract (tick), inheriting BTC's qty.
        specs.insert(
            "BTC-20240329-50000-C".to_string(),
            ContractSpecsConfig {
                tick_size_cents: Some(5),
                ..ContractSpecsConfig::default()
            },
        );
        let config = MicrostructureConfig::resolve(&file, &specs).expect("resolves");

        // The symbol-specific override wins, and inherits its underlying's qty.
        let sym = config.specs_for_symbol("BTC-20240329-50000-C");
        assert_eq!(
            sym.tick_size_cents(),
            5,
            "the per-symbol tick override wins"
        );
        assert_eq!(
            sym.max_order_qty(),
            10_000,
            "the per-symbol override inherits the underlying's qty"
        );

        // A different BTC contract (no per-symbol override) falls back to the BTC
        // underlying: baseline tick, the underlying's qty.
        let other = config.specs_for_symbol("BTC-20240329-60000-C");
        assert_eq!(
            other.tick_size_cents(),
            1,
            "falls back to the baseline tick"
        );
        assert_eq!(other.max_order_qty(), 10_000, "falls back to the BTC qty");
        assert_eq!(other, config.specs_for("BTC"));

        // An ETH contract (no per-symbol, no per-underlying) falls back to the venue
        // default across every knob.
        let eth = config.specs_for_symbol("ETH-20240329-50000-C");
        assert_eq!(eth, config.default_specs());

        // `specs_for(underlying)` is unchanged and never sees per-symbol overrides.
        assert_eq!(config.specs_for("BTC").tick_size_cents(), 1);
        assert_eq!(config.specs_for("BTC").max_order_qty(), 10_000);

        // A malformed symbol falls back to the venue default (never a panic).
        assert_eq!(
            config.specs_for_symbol("not-a-symbol"),
            config.default_specs()
        );
        // The per-symbol profile resolves the same specs the direct lookup does.
        assert_eq!(
            config.profile_for_symbol("BTC-20240329-50000-C").specs(),
            sym
        );
    }

    #[test]
    fn test_fingerprint_is_sensitive_to_per_symbol_override() {
        // A per-symbol override changes the fingerprint (a distinct `specs.sym.`
        // fragment), and is stable/deterministic across insertion order.
        let file = FileMicrostructure {
            fees: Some(FeeConfig {
                maker_bps: -10,
                taker_bps: 35,
            }),
            ..FileMicrostructure::default()
        };
        let base = MicrostructureConfig::resolve(&file, &BTreeMap::new()).expect("resolves");
        let mut specs = BTreeMap::new();
        specs.insert(
            "BTC-20240329-50000-C".to_string(),
            ContractSpecsConfig {
                tick_size_cents: Some(5),
                ..ContractSpecsConfig::default()
            },
        );
        let with_symbol = MicrostructureConfig::resolve(&file, &specs).expect("resolves");
        assert_ne!(base.fingerprint(), with_symbol.fingerprint());
        assert!(
            with_symbol
                .fingerprint()
                .contains("specs.sym.BTC-20240329-50000-C=")
        );
        // Deterministic across repeated resolution.
        assert_eq!(
            with_symbol.fingerprint(),
            MicrostructureConfig::resolve(&file, &specs)
                .expect("resolves")
                .fingerprint()
        );
    }

    #[test]
    fn test_profile_resolves_default_when_unset() {
        // A per-instrument profile resolves on the order path; an unset knob inherits
        // the venue default (#046). BTC overrides its tick; ETH has no override, so
        // its profile equals the venue-default profile.
        let file = FileMicrostructure {
            fees: Some(FeeConfig {
                maker_bps: -10,
                taker_bps: 35,
            }),
            specs: Some(ContractSpecsConfig {
                max_price_cents: Some(200_000_000),
                ..ContractSpecsConfig::default()
            }),
            ..FileMicrostructure::default()
        };
        let mut per_instrument = BTreeMap::new();
        per_instrument.insert(
            "BTC".to_string(),
            ContractSpecsConfig {
                tick_size_cents: Some(5),
                ..ContractSpecsConfig::default()
            },
        );
        let config = MicrostructureConfig::resolve(&file, &per_instrument).expect("resolves");

        // An unconfigured underlying inherits the venue default across every knob.
        let eth = config.profile_for("ETH");
        assert_eq!(eth.specs(), config.default_specs());
        assert_eq!(eth.fee_schedule().taker_fee_bps, 35);
        assert_eq!(eth.stp_mode(), config.stp_mode());
        assert_eq!(eth.latency(), config.latency());
        assert_eq!(eth.price_bounds(), config.price_bounds_for("ETH"));

        // BTC's profile overrides the tick but inherits the venue-default max_price
        // (an unset per-instrument knob inherits the venue default).
        let btc = config.profile_for("BTC");
        assert_eq!(btc.specs().tick_size_cents(), 5);
        assert_eq!(btc.specs().max_price_cents(), 200_000_000);
        // Venue-wide knobs (fees/stp/latency) are identical across instruments.
        assert_eq!(
            btc.fee_schedule().maker_fee_bps,
            eth.fee_schedule().maker_fee_bps
        );
        assert_eq!(btc.stp_mode(), eth.stp_mode());
    }

    #[test]
    fn test_profile_is_pure_function_for_replay() {
        // Determinism: two resolutions of the same underlying's profile are equal, so
        // a book vivified during replay resolves an identical profile.
        let file = FileMicrostructure {
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
        assert_eq!(config.profile_for("BTC"), config.profile_for("BTC"));
        assert_eq!(config.profile_for("ETH"), config.profile_for("ETH"));
        // The profiled BTC book admits within its band; an over-band price is refused
        // identically on every resolution (the order-path admission check).
        let btc = config.profile_for("BTC");
        assert_eq!(btc.admit_price(Cents::new(50_000)), Ok(()));
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
    fn test_file_microstructure_resolves_latency() {
        // #045: `[microstructure.latency]` is a real, resolved section — the doc
        // example parses and resolves alongside the fee schedule.
        let file: FileMicrostructure = toml::from_str(
            "[fees]\nmaker_bps = -10\ntaker_bps = 35\n\n[latency]\nmodel = \"lognormal\"\nmedian_us = 250\nsigma = 0.4\n",
        )
        .expect("config with latency parses");
        let config = MicrostructureConfig::resolve(&file, &BTreeMap::new()).expect("resolves");
        assert_eq!(config.fee_schedule().taker_fee_bps, 35);
        assert_eq!(
            config.latency(),
            crate::microstructure::LatencyConfig::Lognormal {
                median_us: 250,
                sigma: 0.4,
            }
        );
    }

    #[test]
    fn test_resolve_rejects_invalid_latency() {
        // A latency section missing its required `sigma` fails resolution as a
        // typed microstructure config error.
        let file: FileMicrostructure =
            toml::from_str("[latency]\nmodel = \"normal\"\nmean_us = 100\n")
                .expect("parses (validation is at resolve)");
        match MicrostructureConfig::resolve(&file, &BTreeMap::new()) {
            Err(MicrostructureConfigError::Latency(_)) => {}
            other => panic!("expected a latency config error, got {other:?}"),
        }
    }

    #[test]
    fn test_fingerprint_is_sensitive_to_latency() {
        // A latency-only config change (fees/stp/specs all default) still yields a
        // fingerprint distinct from the reserved default slot.
        let file = FileMicrostructure {
            latency: Some(crate::microstructure::FileLatency {
                model: crate::microstructure::LatencyModel::Fixed,
                us: Some(250),
                min_us: None,
                max_us: None,
                mean_us: None,
                median_us: None,
                sigma: None,
            }),
            ..FileMicrostructure::default()
        };
        let config = MicrostructureConfig::resolve(&file, &BTreeMap::new()).expect("resolves");
        assert_ne!(config.fingerprint(), DEFAULT_MICROSTRUCTURE_FINGERPRINT);
        assert!(config.fingerprint().contains("latency=fixed"));
        // And a different latency yields a different fingerprint.
        let other_file = FileMicrostructure {
            latency: Some(crate::microstructure::FileLatency {
                model: crate::microstructure::LatencyModel::Fixed,
                us: Some(500),
                min_us: None,
                max_us: None,
                mean_us: None,
                median_us: None,
                sigma: None,
            }),
            ..FileMicrostructure::default()
        };
        let other = MicrostructureConfig::resolve(&other_file, &BTreeMap::new()).expect("resolves");
        assert_ne!(config.fingerprint(), other.fingerprint());
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
