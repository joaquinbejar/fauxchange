//! `[microstructure.latency]` — seeded latency injection on the **virtual clock**
//! ([05 §3](../../../docs/05-microstructure-config.md#3-latency-injection)).
//!
//! Every inbound message can be delayed by a per-message draw against a
//! configurable distribution (`fixed` / `uniform` / `normal` / `lognormal`). The
//! delay is a **virtual-clock offset** ([#028](../../simulation/clock.rs)), not a
//! `tokio::time::sleep`, so it is part of the reproducible timeline and replays
//! identically ([04 §5](../../../docs/04-market-data-and-replay.md#5-clock-control)).
//! It is designed to be applied at the **gateway edge, before the sequencer**, so
//! it shapes the *arrival order* into the single-writer actor without perturbing
//! matching itself — the failure mode (a slow client losing the queue race) real
//! venues only exhibit under load. This module lands the config, the seeded draw,
//! and the [`LatencyOffset`]; the **live gateway-edge application** — the
//! deterministic ingress-reorder buffer
//! ([03 §6.1](../../../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering))
//! that actually consumes the offset to reshape arrival order into the single
//! writer — is deferred to
//! [#111](https://github.com/joaquinbejar/fauxchange/issues/111). The determinism
//! tests here prove the load-bearing invariant (reordering arrivals only permutes
//! order, never mutates a command, and a fixed permuted order replays to identical
//! fills), so #111 wires the mechanism onto a proven contract.
//!
//! ## The seeded sub-stream (independent, order-free, replayable)
//!
//! The draw is a **pure function** of `(run_seed, session_id, msg_seq)`:
//! [`LatencyConfig::draw`] hashes those three inputs — under a fixed
//! [`LATENCY_SUBSTREAM_DOMAIN`] tag so the latency stream is **independent** of any
//! other venue-owned seeded sub-stream (persona jitter #047, etc.) drawn from the
//! same run seed — into a [`SplitMix64`] state, then draws once. Because the draw
//! depends only on the message's stable identity and never on a shared counter,
//! wall clock, or the order messages happen to arrive in:
//!
//! - **two runs with the same seed produce identical draws**, and different seeds
//!   diverge;
//! - a latency-injected run **replays identically** — the journal records the
//!   resulting arrival order and the draw itself is reproducible from the seed; and
//! - injected latency changes *arrival order only*: the offset is a `u64` this
//!   module returns, it never touches a `VenueCommand`, so for a fixed arrival
//!   order the fills are unchanged (matching is downstream of the sequencer).
//!
//! This is distinct from the **price walk**, which is *excluded* from same-seed
//! regeneration because `optionstratlib`'s sampler is not seedable
//! ([04 §2](../../../docs/04-market-data-and-replay.md#2-synthetic-price-generation));
//! latency is a venue-owned sub-stream and therefore *is* seed-reproducible — the
//! two must not be conflated.
//!
//! ## The `f64` boundary is guarded
//!
//! The `normal` / `lognormal` draws run through `ln` / `sqrt` / `cos` / `exp`.
//! The `normal` draw is clamped `≥ 0` (the doc contract), and the final
//! float→`u64` conversion ([`micros_from_f64`]) rejects any non-finite value to a
//! documented fail-safe ceiling, so NaN / Inf can never reach the returned
//! [`LatencyOffset`].

use serde::{Deserialize, Serialize};

use crate::microstructure::error::LatencyConfigError;
use crate::rng::{SplitMix64, fnv1a_64, mix64};

// ============================================================================
// Seeded keyed PRNG — the shared SplitMix64 sub-stream, latency-keyed
// ============================================================================

/// The domain tag mixed into every latency stream key, separating the latency
/// sub-stream from every **other** venue-owned seeded sub-stream (persona jitter,
/// #047, …) that derives from the same run seed. Two sub-streams with the same
/// `(run_seed, key)` but different domains never correlate.
const LATENCY_SUBSTREAM_DOMAIN: u64 = 0x_4661_7578_4C61_7443; // "FauxLatC"

/// The documented fail-safe delay a non-finite draw (an unreachable NaN, or a
/// `lognormal` `exp` overflow under an absurd `sigma`) clamps to, so the `f64`
/// boundary never leaks a non-finite value into a [`LatencyOffset`]. `u64::MAX`
/// microseconds is "as late as the virtual clock can represent" — a fail-safe, not
/// a value a sane config reaches.
const NON_FINITE_CLAMP_US: u64 = u64::MAX;

/// Derives the seeded latency sub-stream for one message identity from the shared
/// [`SplitMix64`] primitive: the run seed and message `(session_id, msg_seq)` folded
/// through the avalanche mix under the latency domain tag, so distinct messages get
/// uncorrelated streams and the stream is independent of the price walk and every
/// other sub-stream. Byte-identical to the module's original inline folding — the
/// primitive moved to [`crate::rng`], the latency keying stayed here.
#[inline]
fn keyed_latency(run_seed: u64, session_id: &str, msg_seq: u64) -> SplitMix64 {
    let mut acc = mix64(run_seed ^ LATENCY_SUBSTREAM_DOMAIN);
    acc = mix64(acc ^ fnv1a_64(session_id.as_bytes()));
    acc = mix64(acc ^ msg_seq);
    SplitMix64::from_state(acc)
}

/// Converts a drawn `f64` microsecond delay into a guarded `u64`, keeping the
/// `f64` boundary honest: a non-finite value (an unreachable NaN, or an `exp`
/// overflow) clamps to [`NON_FINITE_CLAMP_US`], a negative value clamps to `0`,
/// and an in-range value rounds to the nearest microsecond (saturating at
/// `u64::MAX`). No NaN / Inf ever escapes into a [`LatencyOffset`].
#[inline]
fn micros_from_f64(value: f64) -> u64 {
    if !value.is_finite() {
        return NON_FINITE_CLAMP_US;
    }
    let rounded = value.max(0.0).round();
    if rounded >= u64::MAX as f64 {
        u64::MAX
    } else {
        // Saturating float→int cast is a fail-safe only; the `>=` guard above
        // already bounds `rounded` to `[0, u64::MAX)`.
        rounded as u64
    }
}

// ============================================================================
// LatencyModel — the closed distribution vocabulary
// ============================================================================

/// The latency distribution family selected by `[microstructure.latency] model`.
///
/// A closed set (`#[repr(u8)]`, stable ordering) surfaced as the lowercase tokens
/// `fixed` / `uniform` / `normal` / `lognormal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum LatencyModel {
    /// A constant delay of `us` microseconds.
    Fixed = 0,
    /// A uniform draw in `[min_us, max_us]`.
    Uniform = 1,
    /// A `N(mean_us, sigma)` draw, clamped `≥ 0`.
    Normal = 2,
    /// A heavy-tailed lognormal draw with median `median_us` and shape `sigma`.
    Lognormal = 3,
}

impl LatencyModel {
    /// The canonical lowercase token — matches the `model` config value.
    #[must_use]
    #[inline]
    pub fn as_token(self) -> &'static str {
        match self {
            LatencyModel::Fixed => "fixed",
            LatencyModel::Uniform => "uniform",
            LatencyModel::Normal => "normal",
            LatencyModel::Lognormal => "lognormal",
        }
    }
}

// ============================================================================
// FileLatency — the `[microstructure.latency]` file surface
// ============================================================================

/// The `[microstructure.latency]` file section: a `model` selector plus the union
/// of every model's parameters, each optional. Which parameters are required is a
/// function of the selected `model`, resolved and validated by [`Self::resolve`].
///
/// The `_us` delay parameters are `i64` (not `u64`) so a **negative** value parses
/// and is rejected with a typed [`LatencyConfigError::NegativeMicros`] rather than a
/// generic TOML type error; `sigma` is `f64` so `nan` / `inf` parse and are
/// rejected as [`LatencyConfigError::SigmaNotFinite`]. `#[serde(deny_unknown_fields)]`
/// rejects a stray key.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileLatency {
    /// The distribution family.
    pub model: LatencyModel,
    /// `fixed`: the constant delay, microseconds.
    #[serde(default)]
    pub us: Option<i64>,
    /// `uniform`: the band floor, microseconds.
    #[serde(default)]
    pub min_us: Option<i64>,
    /// `uniform`: the band ceiling, microseconds.
    #[serde(default)]
    pub max_us: Option<i64>,
    /// `normal`: the mean delay, microseconds.
    #[serde(default)]
    pub mean_us: Option<i64>,
    /// `lognormal`: the median delay, microseconds.
    #[serde(default)]
    pub median_us: Option<i64>,
    /// `normal` / `lognormal`: the distribution shape (spread) parameter.
    #[serde(default)]
    pub sigma: Option<f64>,
}

impl FileLatency {
    /// Resolves and validates this file section into a [`LatencyConfig`].
    ///
    /// The required parameters for the selected `model` must be present and every
    /// `_us` delay non-negative; `sigma` (for `normal` / `lognormal`) must be
    /// finite and non-negative; a `uniform` band must have `min_us ≤ max_us`.
    ///
    /// # Errors
    ///
    /// A [`LatencyConfigError`] on a missing required parameter, a negative delay, a
    /// non-finite / negative `sigma`, or `min_us > max_us`.
    pub fn resolve(&self) -> Result<LatencyConfig, LatencyConfigError> {
        let model = self.model;
        match model {
            LatencyModel::Fixed => {
                let us = self.micros("us", self.us)?;
                Ok(LatencyConfig::Fixed { us })
            }
            LatencyModel::Uniform => {
                let min_raw = self.require("min_us", self.min_us)?;
                let max_raw = self.require("max_us", self.max_us)?;
                let min_us = check_micros("min_us", min_raw)?;
                let max_us = check_micros("max_us", max_raw)?;
                if min_raw > max_raw {
                    return Err(LatencyConfigError::MinExceedsMax {
                        min_us: min_raw,
                        max_us: max_raw,
                    });
                }
                Ok(LatencyConfig::Uniform { min_us, max_us })
            }
            LatencyModel::Normal => {
                let mean_us = self.micros("mean_us", self.mean_us)?;
                let sigma = self.sigma_param()?;
                Ok(LatencyConfig::Normal { mean_us, sigma })
            }
            LatencyModel::Lognormal => {
                let median_us = self.micros("median_us", self.median_us)?;
                let sigma = self.sigma_param()?;
                Ok(LatencyConfig::Lognormal { median_us, sigma })
            }
        }
    }

    /// Requires a delay parameter to be present and non-negative, returning it as
    /// `u64` microseconds.
    fn micros(&self, param: &'static str, value: Option<i64>) -> Result<u64, LatencyConfigError> {
        let raw = self.require(param, value)?;
        check_micros(param, raw)
    }

    /// Requires a parameter to be present, naming the model in the error.
    fn require<T>(&self, param: &'static str, value: Option<T>) -> Result<T, LatencyConfigError> {
        value.ok_or(LatencyConfigError::MissingParam {
            model: self.model.as_token(),
            param,
        })
    }

    /// Requires `sigma` present, finite, and non-negative.
    fn sigma_param(&self) -> Result<f64, LatencyConfigError> {
        let sigma = self.require("sigma", self.sigma)?;
        if !sigma.is_finite() {
            return Err(LatencyConfigError::SigmaNotFinite { value: sigma });
        }
        if sigma < 0.0 {
            return Err(LatencyConfigError::SigmaNegative { value: sigma });
        }
        Ok(sigma)
    }
}

/// Rejects a negative microsecond delay, returning it as `u64`.
fn check_micros(param: &'static str, value: i64) -> Result<u64, LatencyConfigError> {
    u64::try_from(value).map_err(|_| LatencyConfigError::NegativeMicros { param, value })
}

// ============================================================================
// LatencyConfig — the resolved, validated distribution
// ============================================================================

/// The resolved, validated latency distribution — the value the gateway edge draws
/// against per inbound message.
///
/// [`Disabled`](Self::Disabled) (the default) injects no delay, so a venue with no
/// `[microstructure.latency]` section behaves as before. `Serialize` / `Deserialize`
/// are derived so the resolved config rides inside a recorded scenario bundle (the
/// config manifest is part of the determinism tuple) and contributes to the
/// microstructure fingerprint. `Eq` is not derived — the `Normal` / `Lognormal`
/// variants carry an `f64` `sigma`.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub enum LatencyConfig {
    /// No latency injection — every message arrives at the current virtual instant.
    #[default]
    Disabled,
    /// A constant `us`-microsecond delay.
    Fixed {
        /// The constant delay, microseconds.
        us: u64,
    },
    /// A uniform draw in `[min_us, max_us]`.
    Uniform {
        /// The band floor, microseconds.
        min_us: u64,
        /// The band ceiling, microseconds.
        max_us: u64,
    },
    /// A `N(mean_us, sigma)` draw, clamped `≥ 0`.
    Normal {
        /// The mean delay, microseconds.
        mean_us: u64,
        /// The distribution spread.
        sigma: f64,
    },
    /// A heavy-tailed lognormal draw: `median_us × exp(sigma × Z)`, `Z ~ N(0, 1)`.
    Lognormal {
        /// The median delay, microseconds.
        median_us: u64,
        /// The distribution shape.
        sigma: f64,
    },
}

impl LatencyConfig {
    /// Whether any latency is injected (`false` for [`Disabled`](Self::Disabled)).
    #[must_use]
    #[inline]
    pub fn is_enabled(&self) -> bool {
        !matches!(self, LatencyConfig::Disabled)
    }

    /// Re-validates an already-resolved config — the defence-in-depth check the
    /// replay/bundle path runs, since a deserialized [`LatencyConfig`] bypasses
    /// [`FileLatency::resolve`]. Mirrors the checked-fee proof's `validate` re-run
    /// on the bundle path.
    ///
    /// # Errors
    ///
    /// A [`LatencyConfigError`] if a deserialized band has `min_us > max_us` or a
    /// deserialized `sigma` is non-finite or negative.
    pub fn validate(&self) -> Result<(), LatencyConfigError> {
        match *self {
            LatencyConfig::Disabled | LatencyConfig::Fixed { .. } => Ok(()),
            LatencyConfig::Uniform { min_us, max_us } => {
                if min_us > max_us {
                    // Re-express against the resolved `u64` band; the raw i64s are
                    // gone, but both fit i64 (they resolved from non-negative i64).
                    return Err(LatencyConfigError::MinExceedsMax {
                        min_us: i64::try_from(min_us).unwrap_or(i64::MAX),
                        max_us: i64::try_from(max_us).unwrap_or(i64::MAX),
                    });
                }
                Ok(())
            }
            LatencyConfig::Normal { sigma, .. } | LatencyConfig::Lognormal { sigma, .. } => {
                if !sigma.is_finite() {
                    return Err(LatencyConfigError::SigmaNotFinite { value: sigma });
                }
                if sigma < 0.0 {
                    return Err(LatencyConfigError::SigmaNegative { value: sigma });
                }
                Ok(())
            }
        }
    }

    /// The per-message seeded draw — the delay for the message identified by
    /// `(session_id, msg_seq)` under the run-level `run_seed`.
    ///
    /// A **pure function** of its inputs: no clock, no shared counter, no unseeded
    /// RNG, no map-iteration order — so two runs with the same seed draw identically
    /// and the draw is independent of the order messages arrive in. The `normal`
    /// draw is clamped `≥ 0`; every draw is guarded finite before it becomes a
    /// [`LatencyOffset`].
    #[must_use]
    pub fn draw(&self, run_seed: u64, session_id: &str, msg_seq: u64) -> LatencyOffset {
        let micros = match *self {
            LatencyConfig::Disabled => 0,
            LatencyConfig::Fixed { us } => us,
            LatencyConfig::Uniform { min_us, max_us } => {
                let mut rng = keyed_latency(run_seed, session_id, msg_seq);
                draw_uniform(&mut rng, min_us, max_us)
            }
            LatencyConfig::Normal { mean_us, sigma } => {
                let mut rng = keyed_latency(run_seed, session_id, msg_seq);
                let sample = mean_us as f64 + sigma * rng.standard_normal();
                micros_from_f64(sample)
            }
            LatencyConfig::Lognormal { median_us, sigma } => {
                let mut rng = keyed_latency(run_seed, session_id, msg_seq);
                let sample = (median_us as f64) * (sigma * rng.standard_normal()).exp();
                micros_from_f64(sample)
            }
        };
        LatencyOffset(micros)
    }

    /// The canonical single-line fingerprint fragment — the piece of the
    /// microstructure fingerprint ([05 §11](../../../docs/05-microstructure-config.md#11-determinism-of-microstructure))
    /// that scopes the determinism oracle to this latency config. [`Disabled`](Self::Disabled)
    /// contributes the empty string, so a venue with no latency records the reserved
    /// default fingerprint unchanged.
    #[must_use]
    pub fn fingerprint_fragment(&self) -> String {
        match *self {
            LatencyConfig::Disabled => String::new(),
            LatencyConfig::Fixed { us } => format!(";latency=fixed(us{us})"),
            LatencyConfig::Uniform { min_us, max_us } => {
                format!(";latency=uniform(min{min_us}max{max_us})")
            }
            LatencyConfig::Normal { mean_us, sigma } => {
                format!(";latency=normal(mean{mean_us}sigma{})", fmt_sigma(sigma))
            }
            LatencyConfig::Lognormal { median_us, sigma } => {
                format!(
                    ";latency=lognormal(median{median_us}sigma{})",
                    fmt_sigma(sigma)
                )
            }
        }
    }
}

/// The uniform draw over the inclusive band `[min, max]`, guarding the width
/// arithmetic (`min == max` is a constant; a full-`u64` band uses the raw output).
#[inline]
fn draw_uniform(rng: &mut SplitMix64, min: u64, max: u64) -> u64 {
    if min >= max {
        return min;
    }
    let span = max - min; // >= 1
    match span.checked_add(1) {
        Some(width) => min + (rng.next_u64() % width),
        None => rng.next_u64(), // the full u64 band (min == 0, max == u64::MAX)
    }
}

/// A deterministic, bit-stable rendering of a `sigma` for the fingerprint — a fixed
/// decimal form so the same `sigma` always yields the same fragment.
fn fmt_sigma(sigma: f64) -> String {
    format!("{sigma:.6}")
}

// ============================================================================
// LatencyOffset — the virtual-clock offset the gateway edge will apply (#111)
// ============================================================================

/// The drawn arrival delay, in **microseconds on the virtual clock**.
///
/// This is *not* a real sleep: it is meant to be added to the message's virtual
/// arrival instant *before* the sequencer, so it reshapes the arrival order into
/// the single-writer actor while remaining part of the reproducible timeline.
/// Matching is downstream of the sequencer and never sees this value. The live
/// consumption of this offset — the deterministic ingress-reorder buffer at the
/// gateway edge (03 §6.1) — is deferred to
/// [#111](https://github.com/joaquinbejar/fauxchange/issues/111); this type and
/// its seeded draw are the contract #111 wires in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct LatencyOffset(u64);

impl LatencyOffset {
    /// A zero offset (no delay).
    pub const ZERO: LatencyOffset = LatencyOffset(0);

    /// The delay in microseconds.
    #[must_use]
    #[inline]
    pub const fn micros(self) -> u64 {
        self.0
    }

    /// The delayed **virtual arrival instant, in microseconds**, given the current
    /// virtual instant `now_ms` (the venue clock read at the gateway edge). This is
    /// the ordering key the ingress path uses to sequence the message into the
    /// single writer; the addition is checked (never wrapping), saturating at
    /// `u64::MAX` microseconds as a fail-safe on an absurd offset.
    #[must_use]
    #[inline]
    pub fn delayed_arrival_us(self, now_ms: u64) -> u64 {
        now_ms
            .checked_mul(1_000)
            .and_then(|base_us| base_us.checked_add(self.0))
            .unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const SEED: u64 = 0x1234_5678_9ABC_DEF0;

    fn file(model: LatencyModel) -> FileLatency {
        FileLatency {
            model,
            us: None,
            min_us: None,
            max_us: None,
            mean_us: None,
            median_us: None,
            sigma: None,
        }
    }

    // ---- config resolution + rejection -------------------------------------

    #[test]
    fn test_latency_config_resolves_fixed() {
        let resolved = FileLatency {
            us: Some(250),
            ..file(LatencyModel::Fixed)
        }
        .resolve()
        .expect("fixed resolves");
        assert_eq!(resolved, LatencyConfig::Fixed { us: 250 });
    }

    #[test]
    fn test_latency_config_resolves_lognormal_from_doc_example() {
        // The `[microstructure.latency]` example from docs/05 §2.
        let file: FileLatency =
            toml::from_str("model = \"lognormal\"\nmedian_us = 250\nsigma = 0.4\n")
                .expect("doc example parses");
        assert_eq!(
            file.resolve().expect("resolves"),
            LatencyConfig::Lognormal {
                median_us: 250,
                sigma: 0.4,
            }
        );
    }

    #[test]
    fn test_latency_config_rejects_negative_us() {
        let error = FileLatency {
            us: Some(-5),
            ..file(LatencyModel::Fixed)
        }
        .resolve()
        .expect_err("a negative us is rejected");
        assert_eq!(
            error,
            LatencyConfigError::NegativeMicros {
                param: "us",
                value: -5,
            }
        );
    }

    #[test]
    fn test_latency_config_rejects_missing_required_param() {
        // `fixed` without `us`.
        let error = file(LatencyModel::Fixed)
            .resolve()
            .expect_err("missing us is rejected");
        assert_eq!(
            error,
            LatencyConfigError::MissingParam {
                model: "fixed",
                param: "us",
            }
        );
    }

    #[test]
    fn test_latency_config_rejects_non_finite_sigma() {
        // NaN != NaN, so match on the variant + finiteness rather than by equality.
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = FileLatency {
                mean_us: Some(100),
                sigma: Some(bad),
                ..file(LatencyModel::Normal)
            }
            .resolve()
            .expect_err("a non-finite sigma is rejected");
            match error {
                LatencyConfigError::SigmaNotFinite { value } => assert!(!value.is_finite()),
                other => panic!("expected SigmaNotFinite, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_latency_config_rejects_negative_sigma() {
        let error = FileLatency {
            median_us: Some(100),
            sigma: Some(-0.1),
            ..file(LatencyModel::Lognormal)
        }
        .resolve()
        .expect_err("a negative sigma is rejected");
        assert_eq!(error, LatencyConfigError::SigmaNegative { value: -0.1 });
    }

    #[test]
    fn test_latency_config_rejects_min_above_max() {
        let error = FileLatency {
            min_us: Some(500),
            max_us: Some(100),
            ..file(LatencyModel::Uniform)
        }
        .resolve()
        .expect_err("min_us > max_us is rejected");
        assert_eq!(
            error,
            LatencyConfigError::MinExceedsMax {
                min_us: 500,
                max_us: 100,
            }
        );
    }

    #[test]
    fn test_file_latency_rejects_unknown_field() {
        let error = toml::from_str::<FileLatency>("model = \"fixed\"\nus = 10\nbogus = 1\n");
        assert!(error.is_err(), "an unknown latency field must be rejected");
    }

    #[test]
    fn test_latency_validate_rejects_deserialized_bad_band() {
        // A hostile bundle could deserialize an inverted band, bypassing resolve.
        let hostile = LatencyConfig::Uniform {
            min_us: 500,
            max_us: 100,
        };
        assert!(hostile.validate().is_err(), "an inverted band is rejected");
        // A well-formed band validates.
        assert_eq!(
            LatencyConfig::Uniform {
                min_us: 100,
                max_us: 500,
            }
            .validate(),
            Ok(())
        );
    }

    // ---- draw bounds (per model) -------------------------------------------

    #[test]
    fn test_draw_fixed_is_constant() {
        let config = LatencyConfig::Fixed { us: 250 };
        for seq in 0..1_000 {
            assert_eq!(config.draw(SEED, "sess", seq).micros(), 250);
        }
    }

    #[test]
    fn test_draw_uniform_stays_within_band() {
        let config = LatencyConfig::Uniform {
            min_us: 100,
            max_us: 200,
        };
        for seq in 0..10_000 {
            let drawn = config.draw(SEED, "sess", seq).micros();
            assert!(
                (100..=200).contains(&drawn),
                "uniform draw {drawn} out of band"
            );
        }
    }

    #[test]
    fn test_draw_normal_is_clamped_non_negative() {
        // A mean of 0 with a wide sigma would otherwise draw negatives.
        let config = LatencyConfig::Normal {
            mean_us: 0,
            sigma: 1_000.0,
        };
        for seq in 0..10_000 {
            // A u64 is inherently >= 0; the assertion is that no draw panics and the
            // clamp holds (a negative sample maps to 0, never a huge wrapped value).
            let drawn = config.draw(SEED, "sess", seq).micros();
            assert!(
                drawn < 1_000_000_000,
                "normal draw {drawn} left the clamp band"
            );
        }
    }

    #[test]
    fn test_draw_lognormal_is_positive_and_heavy_tailed() {
        let config = LatencyConfig::Lognormal {
            median_us: 250,
            sigma: 0.8,
        };
        let mut max_seen = 0u64;
        let mut above_median = 0u32;
        for seq in 0..10_000 {
            let drawn = config.draw(SEED, "sess", seq).micros();
            max_seen = max_seen.max(drawn);
            if drawn > 250 {
                above_median += 1;
            }
        }
        // Heavy tail: the max vastly exceeds the median, and roughly half the mass
        // sits above the median (the lognormal median is exp(0) × median_us).
        assert!(max_seen > 2_500, "lognormal tail too thin: max {max_seen}");
        assert!(
            (3_000..=7_000).contains(&above_median),
            "≈half the mass should exceed the median, got {above_median}/10000"
        );
    }

    // ---- determinism: same seed identical, different seed diverges ----------

    #[test]
    fn test_draw_same_seed_is_identical() {
        let config = LatencyConfig::Lognormal {
            median_us: 250,
            sigma: 0.4,
        };
        for seq in 0..1_000 {
            let a = config.draw(SEED, "session-A", seq);
            let b = config.draw(SEED, "session-A", seq);
            assert_eq!(a, b, "same (seed, session, seq) must draw identically");
        }
    }

    #[test]
    fn test_draw_different_seed_diverges() {
        let config = LatencyConfig::Lognormal {
            median_us: 250,
            sigma: 0.4,
        };
        let mut differences = 0u32;
        for seq in 0..1_000 {
            let a = config.draw(SEED, "sess", seq);
            let b = config.draw(SEED ^ 0xFFFF_FFFF, "sess", seq);
            if a != b {
                differences += 1;
            }
        }
        assert!(
            differences > 900,
            "different seeds should diverge on nearly every draw, got {differences}/1000"
        );
    }

    #[test]
    fn test_draw_distinct_sessions_diverge() {
        let config = LatencyConfig::Uniform {
            min_us: 0,
            max_us: 1_000_000,
        };
        let mut differences = 0u32;
        for seq in 0..1_000 {
            let a = config.draw(SEED, "session-A", seq);
            let b = config.draw(SEED, "session-B", seq);
            if a != b {
                differences += 1;
            }
        }
        assert!(
            differences > 900,
            "distinct sessions should draw independently, got {differences}/1000"
        );
    }

    // ---- arrival-order-only: the draw does not depend on request order ------

    #[test]
    fn test_draw_is_order_independent() {
        // Latency shapes arrival ORDER only: a message's own draw is a pure
        // function of its identity, so drawing the same messages in a different
        // ORDER yields the same per-message values (no shared counter state).
        let config = LatencyConfig::Normal {
            mean_us: 500,
            sigma: 0.5,
        };
        let forward: Vec<u64> = (0..500)
            .map(|seq| config.draw(SEED, "sess", seq).micros())
            .collect();
        let reversed: Vec<u64> = (0..500)
            .rev()
            .map(|seq| config.draw(SEED, "sess", seq).micros())
            .collect();
        let mut reversed_back = reversed;
        reversed_back.reverse();
        assert_eq!(
            forward, reversed_back,
            "a message's draw must not depend on the order draws are requested"
        );
    }

    // ---- LatencyOffset application (virtual-clock offset) -------------------

    #[test]
    fn test_offset_applies_as_virtual_clock_micros() {
        let offset = LatencyOffset(250);
        // now_ms is virtual milliseconds; the arrival key is virtual microseconds.
        assert_eq!(offset.delayed_arrival_us(1_000), 1_000 * 1_000 + 250);
        // A zero offset preserves the base instant exactly.
        assert_eq!(LatencyOffset::ZERO.delayed_arrival_us(1_000), 1_000_000);
    }

    #[test]
    fn test_offset_application_never_reorders_relative_to_base_without_latency() {
        // With NO latency every message keeps its base virtual instant, so arrival
        // order == submission order (the offset cannot perturb a fixed order).
        let disabled = LatencyConfig::Disabled;
        for (seq, now_ms) in [(0u64, 10u64), (1, 20), (2, 30)] {
            let offset = disabled.draw(SEED, "sess", seq);
            assert_eq!(offset, LatencyOffset::ZERO);
            assert_eq!(offset.delayed_arrival_us(now_ms), now_ms * 1_000);
        }
    }

    #[test]
    fn test_fingerprint_fragment_is_empty_when_disabled_and_stable_otherwise() {
        assert_eq!(LatencyConfig::Disabled.fingerprint_fragment(), "");
        let a = LatencyConfig::Lognormal {
            median_us: 250,
            sigma: 0.4,
        };
        // Deterministic and content-sensitive.
        assert_eq!(a.fingerprint_fragment(), a.fingerprint_fragment());
        let b = LatencyConfig::Lognormal {
            median_us: 250,
            sigma: 0.5,
        };
        assert_ne!(a.fingerprint_fragment(), b.fingerprint_fragment());
    }

    proptest! {
        /// The property the AC names: every model draws within its configured range
        /// (`fixed` constant; `uniform` in `[min, max]`; `normal` clamped `≥ 0`;
        /// `lognormal` finite and positive) for any seed / session / seq.
        #[test]
        fn latency_draw_within_configured_range(
            run_seed in any::<u64>(),
            msg_seq in any::<u64>(),
            session in "[a-zA-Z0-9._-]{0,32}",
            fixed_us in 0u64..1_000_000,
            lo in 0u64..1_000_000,
            hi in 0u64..1_000_000,
            mean_us in 0u64..1_000_000,
            median_us in 1u64..1_000_000,
            sigma in 0.0f64..3.0,
        ) {
            // fixed: exactly the constant.
            let fixed = LatencyConfig::Fixed { us: fixed_us };
            prop_assert_eq!(fixed.draw(run_seed, &session, msg_seq).micros(), fixed_us);

            // uniform: within [min, max] inclusive (normalise the band).
            let (min_us, max_us) = (lo.min(hi), lo.max(hi));
            let uniform = LatencyConfig::Uniform { min_us, max_us };
            let drawn = uniform.draw(run_seed, &session, msg_seq).micros();
            prop_assert!((min_us..=max_us).contains(&drawn));

            // normal: clamped >= 0 (a u64 is inherently non-negative — the assertion
            // is that no NaN/Inf leaked and no huge wrap occurred).
            let normal = LatencyConfig::Normal { mean_us, sigma };
            let n = normal.draw(run_seed, &session, msg_seq).micros();
            // With bounded |Z| <= ~8.6 and sigma <= 3, the draw stays finite and
            // near the mean — never the non-finite fail-safe.
            prop_assert!(n < NON_FINITE_CLAMP_US);

            // lognormal: finite and positive (median > 0, exp > 0).
            let lognormal = LatencyConfig::Lognormal { median_us, sigma };
            let ln = lognormal.draw(run_seed, &session, msg_seq).micros();
            prop_assert!(ln < NON_FINITE_CLAMP_US);
        }

        /// Same seed ⇒ identical draw; the venue-owned latency sub-stream is fully
        /// seed-reproducible (unlike the non-seedable price walk).
        #[test]
        fn latency_draw_same_seed_identical(
            run_seed in any::<u64>(),
            msg_seq in any::<u64>(),
            session in "[a-zA-Z0-9._-]{0,32}",
        ) {
            let config = LatencyConfig::Lognormal { median_us: 300, sigma: 0.5 };
            prop_assert_eq!(
                config.draw(run_seed, &session, msg_seq),
                config.draw(run_seed, &session, msg_seq),
            );
        }
    }
}
