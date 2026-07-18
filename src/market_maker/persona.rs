//! Market-maker **personas** — a named bundle of quoting knobs the venue applies
//! per instrument / per underlying, plus the seeded per-`(persona, symbol)` jitter
//! sub-stream ([05 §8](../../docs/05-microstructure-config.md#8-market-maker-personas),
//! [047](../../milestones/v0.5-microstructure/047-personas-liquidity-halt.md)).
//!
//! ## What a persona is
//!
//! A [`PersonaConfig`] carries the construction-time `base_spread_bps` / `base_size`
//! (the size the maker rests, [05 §9](../../docs/05-microstructure-config.md#9-partial-fill-and-liquidity-shaping))
//! **plus** the three live persona-substrate knobs — `spread_multiplier`,
//! `size_scalar`, `directional_skew` — each held in its documented range by the
//! Backend's NaN-rejecting [`validate_control_value`] clamps (rule 4). Personas are
//! validated at **load** ([`crate::config`]) and at **runtime control**, so an
//! out-of-range / `NaN` value is a typed rejection at both seams, never a coerced or
//! poisoned quote.
//!
//! Running `tight` on a liquid underlying and `wide_skewed` on an illiquid strike is
//! just two different [`PersonaConfig`]s resolved per instrument (via the #046
//! `MicrostructureProfile` seam) — the quotes they produce differ in their
//! **journaled** `AddOrder`s.
//!
//! ## Seeded jitter (rule 3, deterministic)
//!
//! [`PersonaJitter`] adds a small, bounded, reproducible perturbation to the rested
//! **size** and the **skew** so a persona does not quote a mechanically identical
//! ladder on every instrument. The draw is a **pure function** of
//! `(run_seed, persona, symbol)` through the shared [`crate::rng::SplitMix64`]
//! primitive under an **independent** domain tag ([`PERSONA_JITTER_DOMAIN`]): the
//! same seed reproduces the same jitter, a different seed diverges, and the stream
//! never correlates with the latency sub-stream (#045). There is **no** wall clock
//! and **no** unseeded RNG — the jitter is reproducible for replay.
//!
//! ## Not a fill-probability draw
//!
//! The jitter shapes only the size the maker **rests**; it is never a
//! fill-probability or slice-sizing draw. Partial fills come from **real** matching
//! against that finite resting size ([05 §9](../../docs/05-microstructure-config.md#9-partial-fill-and-liquidity-shaping)),
//! so this module deliberately does not — and must not — model the fill itself.
//!
//! [`validate_control_value`]: crate::market_maker::config::validate_control_value

use crate::market_maker::config::{
    DIRECTIONAL_SKEW_MAX, DIRECTIONAL_SKEW_MIN, SIZE_SCALAR_MAX, SIZE_SCALAR_MIN,
    SPREAD_MULTIPLIER_MAX, SPREAD_MULTIPLIER_MIN, validate_control_value,
};
use crate::market_maker::quoter::{DEFAULT_BASE_SIZE, DEFAULT_BASE_SPREAD_BPS};
use crate::rng::SplitMix64;

/// The domain tag mixed into every persona-jitter stream key, separating the
/// persona-jitter sub-stream from every **other** venue-owned seeded sub-stream
/// (latency #045) drawn from the same run seed — so the two never correlate.
const PERSONA_JITTER_DOMAIN: u64 = 0x_4661_7578_5065_7273; // "FauxPers"

/// The maximum fraction the size jitter trims a quote leg by: the effective size
/// scalar is multiplied by a factor in `(1 - PERSONA_SIZE_JITTER, 1]`, so jitter
/// only **reduces** rested size (never inflates it past the persona's nominal), and
/// the jittered scalar stays within `[0, 1]`.
const PERSONA_SIZE_JITTER: f64 = 0.20;

/// The maximum magnitude the skew jitter shifts a persona's directional skew by (an
/// additive delta in `[-PERSONA_SKEW_JITTER, PERSONA_SKEW_JITTER)`); small enough not
/// to dominate the persona's own skew, and the sum is re-clamped to `[-1, 1]`.
const PERSONA_SKEW_JITTER: f64 = 0.05;

/// A market-maker **persona**: the construction-time base spread / size the maker
/// rests **plus** the three range-validated persona knobs.
///
/// Constructed through [`PersonaConfig::try_new`], which **rejects** any non-finite
/// or out-of-range knob (rule 4), so a stored persona always carries finite,
/// in-range knobs. Cheap to copy for the per-requote snapshot.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PersonaConfig {
    /// Base spread in **basis points** — the construction-time half-width the
    /// `spread_multiplier` scales.
    pub base_spread_bps: u64,
    /// Base quote size in **contracts** — the size the maker rests before the
    /// `size_scalar` (and jitter) trim it.
    pub base_size: u64,
    /// Spread multiplier, within `[0.1, 10.0]` (out-of-range rejected).
    pub spread_multiplier: f64,
    /// Size scalar, within `[0.0, 1.0]` (out-of-range rejected).
    pub size_scalar: f64,
    /// Directional skew, within `[-1.0, 1.0]` (out-of-range rejected).
    pub directional_skew: f64,
}

/// A persona knob outside its documented range (or non-finite) — the typed
/// rejection surfaced at **load** and at **runtime control** (rule 4).
///
/// Its [`Display`](std::fmt::Display) is the client-safe message from
/// [`validate_control_value`](crate::market_maker::config::validate_control_value):
/// it names the field, the accepted range, and the (non-secret) offending value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PersonaError(String);

impl PersonaError {
    /// The client-safe reason string.
    #[must_use]
    #[inline]
    pub fn reason(&self) -> &str {
        &self.0
    }
}

impl PersonaConfig {
    /// Builds a persona from its base spread / size and the three knobs, **rejecting**
    /// any non-finite or out-of-range knob (rule 4).
    ///
    /// # Errors
    ///
    /// A [`PersonaError`] naming the first out-of-range / `NaN` knob — the same
    /// message the runtime control seam returns (mapped to a `400` at the boundary).
    #[must_use = "a validated persona must be used"]
    pub fn try_new(
        base_spread_bps: u64,
        base_size: u64,
        spread_multiplier: f64,
        size_scalar: f64,
        directional_skew: f64,
    ) -> Result<Self, PersonaError> {
        let spread_multiplier = validate_control_value(
            "spread_multiplier",
            spread_multiplier,
            SPREAD_MULTIPLIER_MIN,
            SPREAD_MULTIPLIER_MAX,
        )
        .map_err(PersonaError)?;
        let size_scalar =
            validate_control_value("size_scalar", size_scalar, SIZE_SCALAR_MIN, SIZE_SCALAR_MAX)
                .map_err(PersonaError)?;
        let directional_skew = validate_control_value(
            "directional_skew",
            directional_skew,
            DIRECTIONAL_SKEW_MIN,
            DIRECTIONAL_SKEW_MAX,
        )
        .map_err(PersonaError)?;
        Ok(Self {
            base_spread_bps,
            base_size,
            spread_multiplier,
            size_scalar,
            directional_skew,
        })
    }
}

impl Default for PersonaConfig {
    /// The neutral persona: the default base spread / size, unit multipliers, no
    /// skew — the behaviour of an unconfigured instrument.
    #[inline]
    fn default() -> Self {
        Self {
            base_spread_bps: DEFAULT_BASE_SPREAD_BPS,
            base_size: DEFAULT_BASE_SIZE,
            spread_multiplier: 1.0,
            size_scalar: 1.0,
            directional_skew: 0.0,
        }
    }
}

/// One reproducible persona-jitter draw for a `(persona, symbol)` pair: a bounded
/// size trim factor and a bounded additive skew shift.
///
/// Applied by the engine to the persona's knobs before quoting. Both fields are
/// finite and bounded by construction ([`PersonaJitter::draw`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PersonaJitterDraw {
    /// The size trim factor in `(1 - PERSONA_SIZE_JITTER, 1]` — multiplies the
    /// persona's `size_scalar`, so the jittered scalar stays within `[0, 1]`.
    pub size_factor: f64,
    /// The additive skew shift in `[-PERSONA_SKEW_JITTER, PERSONA_SKEW_JITTER)` —
    /// added to the persona's `directional_skew` (the sum re-clamped to `[-1, 1]`).
    pub skew_delta: f64,
}

impl PersonaJitterDraw {
    /// The identity jitter (no perturbation) — the draw a zero-seed / disabled
    /// persona would use, and the safe fallback.
    #[must_use]
    #[inline]
    pub const fn identity() -> Self {
        Self {
            size_factor: 1.0,
            skew_delta: 0.0,
        }
    }
}

/// The seeded persona-jitter sub-stream — a pure function of
/// `(run_seed, persona, symbol)` (rule 3).
///
/// Carries only the run seed; the persona name and symbol key each [`draw`](Self::draw),
/// so one `PersonaJitter` serves every instrument reproducibly.
#[derive(Debug, Clone, Copy)]
pub struct PersonaJitter {
    run_seed: u64,
}

impl PersonaJitter {
    /// Builds the jitter sub-stream for a run seed.
    #[must_use]
    #[inline]
    pub const fn new(run_seed: u64) -> Self {
        Self { run_seed }
    }

    /// The reproducible jitter draw for a `(persona, symbol)` pair.
    ///
    /// A **pure function** of `(run_seed, persona, symbol)` through the shared
    /// [`SplitMix64`] primitive under [`PERSONA_JITTER_DOMAIN`]: the same inputs
    /// always yield the same draw (rule 3), and both returned values are finite and
    /// bounded. No wall clock, no shared counter, no unseeded RNG.
    #[must_use]
    pub fn draw(&self, persona: &str, symbol: &str) -> PersonaJitterDraw {
        let mut rng = SplitMix64::keyed(self.run_seed, PERSONA_JITTER_DOMAIN, &[persona, symbol]);
        let u_size = rng.next_unit(); // [0, 1)
        let u_skew = rng.next_unit(); // [0, 1)
        PersonaJitterDraw {
            // (1 - J, 1]: jitter only trims rested size, keeping scalar*factor in [0, 1].
            size_factor: 1.0 - PERSONA_SIZE_JITTER * u_size,
            // [-J, J): a small symmetric additive skew shift.
            skew_delta: PERSONA_SKEW_JITTER * (2.0 * u_skew - 1.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persona_try_new_accepts_in_range() {
        let persona = PersonaConfig::try_new(120, 3, 2.5, 0.3, -0.4).expect("in-range persona");
        assert_eq!(persona.base_spread_bps, 120);
        assert_eq!(persona.base_size, 3);
        assert_eq!(persona.spread_multiplier, 2.5);
        assert_eq!(persona.size_scalar, 0.3);
        assert_eq!(persona.directional_skew, -0.4);
    }

    #[test]
    fn test_persona_rejects_out_of_range_multiplier() {
        // Below 0.1, above 10.0, and NaN are all rejected at construction (rule 4).
        for bad in [0.05, 10.5, f64::NAN, f64::INFINITY] {
            let err = PersonaConfig::try_new(100, 10, bad, 1.0, 0.0)
                .expect_err("out-of-range spread_multiplier must be rejected");
            assert!(err.reason().contains("spread_multiplier"), "reason: {err}");
        }
    }

    #[test]
    fn test_persona_rejects_out_of_range_size_and_skew() {
        assert!(PersonaConfig::try_new(100, 10, 1.0, 1.5, 0.0).is_err());
        assert!(PersonaConfig::try_new(100, 10, 1.0, -0.1, 0.0).is_err());
        assert!(PersonaConfig::try_new(100, 10, 1.0, 1.0, 1.5).is_err());
        assert!(PersonaConfig::try_new(100, 10, 1.0, 1.0, f64::NAN).is_err());
    }

    /// Rejection-matrix entry (#49): each persona clamp refuses every out-of-range
    /// value AND `NaN`/`±Inf` at construction, so a stored persona always carries
    /// finite, in-range knobs (rule 4). The typed [`PersonaError`] is the same
    /// message the load seam folds into `ConfigError::SeedInvalidPersona`.
    #[test]
    fn test_config_rejects_out_of_range_persona_knobs() {
        // spread_multiplier ∈ [0.1, 10.0]: below, above, and non-finite are refused.
        for bad in [0.05, 10.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = PersonaConfig::try_new(100, 10, bad, 1.0, 0.0)
                .expect_err("out-of-range spread_multiplier is rejected");
            assert!(err.reason().contains("spread_multiplier"), "reason: {err}");
        }
        // size_scalar ∈ [0.0, 1.0]: below, above, and non-finite are refused.
        for bad in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
            let err = PersonaConfig::try_new(100, 10, 1.0, bad, 0.0)
                .expect_err("out-of-range size_scalar is rejected");
            assert!(err.reason().contains("size_scalar"), "reason: {err}");
        }
        // directional_skew ∈ [-1.0, 1.0]: below, above, and non-finite are refused.
        for bad in [-1.5, 1.5, f64::NAN, f64::NEG_INFINITY] {
            let err = PersonaConfig::try_new(100, 10, 1.0, 1.0, bad)
                .expect_err("out-of-range directional_skew is rejected");
            assert!(err.reason().contains("directional_skew"), "reason: {err}");
        }
    }

    #[test]
    fn test_default_persona_is_neutral() {
        let persona = PersonaConfig::default();
        assert_eq!(persona.spread_multiplier, 1.0);
        assert_eq!(persona.size_scalar, 1.0);
        assert_eq!(persona.directional_skew, 0.0);
        assert_eq!(persona.base_spread_bps, DEFAULT_BASE_SPREAD_BPS);
        assert_eq!(persona.base_size, DEFAULT_BASE_SIZE);
    }

    #[test]
    fn test_persona_jitter_is_reproducible_for_a_fixed_seed() {
        let a = PersonaJitter::new(1234);
        let b = PersonaJitter::new(1234);
        let da = a.draw("tight", "BTC-20240329-50000-C");
        let db = b.draw("tight", "BTC-20240329-50000-C");
        assert_eq!(da, db, "same seed + persona + symbol reproduces the jitter");
    }

    #[test]
    fn test_persona_jitter_diverges_across_seed_persona_and_symbol() {
        let base = PersonaJitter::new(1).draw("tight", "BTC-A");
        assert_ne!(base, PersonaJitter::new(2).draw("tight", "BTC-A"), "seed");
        assert_ne!(base, PersonaJitter::new(1).draw("wide", "BTC-A"), "persona");
        assert_ne!(base, PersonaJitter::new(1).draw("tight", "BTC-B"), "symbol");
    }

    #[test]
    fn test_persona_jitter_is_bounded_and_finite() {
        let jitter = PersonaJitter::new(0xDEAD_BEEF);
        for i in 0..2_000 {
            let symbol = format!("BTC-2024032{}-5000{}-C", i % 9, i % 9);
            let draw = jitter.draw("wide_skewed", &symbol);
            assert!(draw.size_factor.is_finite());
            assert!(
                draw.size_factor > 1.0 - PERSONA_SIZE_JITTER && draw.size_factor <= 1.0,
                "size_factor out of (1-J, 1]: {}",
                draw.size_factor
            );
            assert!(draw.skew_delta.is_finite());
            assert!(
                draw.skew_delta >= -PERSONA_SKEW_JITTER && draw.skew_delta < PERSONA_SKEW_JITTER,
                "skew_delta out of [-J, J): {}",
                draw.skew_delta
            );
        }
    }

    #[test]
    fn test_jittered_size_scalar_stays_within_unit_range() {
        // The jittered scalar (persona.size_scalar * size_factor) never leaves [0, 1],
        // so it is a valid `QuoteInput.size_scalar` without extra clamping.
        let jitter = PersonaJitter::new(42);
        for ss in [0.0, 0.3, 0.8, 1.0] {
            for i in 0..200 {
                let symbol = format!("ETH-20240329-{}-P", 1000 + i);
                let factor = jitter.draw("p", &symbol).size_factor;
                let jittered = ss * factor;
                assert!(
                    (0.0..=1.0).contains(&jittered),
                    "jittered scalar {jittered} left [0,1] for ss={ss}"
                );
            }
        }
    }
}
