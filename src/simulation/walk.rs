//! The venue's surfaced walk types and the `optionstratlib` walk-generation
//! adapter — the price walk runs **entirely through `optionstratlib`**, never a
//! hand-rolled stochastic process
//! ([016](../../milestones/v0.1-backend-core/016-price-simulator-walks.md),
//! [04 §2](../../docs/04-market-data-and-replay.md#2-synthetic-price-generation),
//! CLAUDE.md *`optionstratlib` for options math*).
//!
//! ## The surfaced set (v1)
//!
//! [`WalkTypeConfig`] surfaces exactly the three walks the design pins for v1,
//! each mapped 1:1 onto an [`optionstratlib::simulation::WalkType`] variant:
//!
//! | [`WalkTypeConfig`]                       | `optionstratlib::simulation::WalkType` |
//! |------------------------------------------|----------------------------------------|
//! | [`GeometricBrownian`](WalkTypeConfig::GeometricBrownian) | `WalkType::GeometricBrownian` |
//! | [`MeanReverting`](WalkTypeConfig::MeanReverting)         | `WalkType::MeanReverting` (OU) |
//! | [`JumpDiffusion`](WalkTypeConfig::JumpDiffusion)         | `WalkType::JumpDiffusion` |
//!
//! `optionstratlib::simulation::WalkType` also carries `Brownian`, `LogReturns`,
//! `Garch`, `Heston`, `Custom`, `Telegraph`, and `Historical`; those are a later
//! config addition (not a fork of the walk engine) and are **not** surfaced here
//! ([04 §2](../../docs/04-market-data-and-replay.md#2-synthetic-price-generation)).
//! The OU mean-reversion speed and the jump intensity / size constants are fixed
//! in v1 (documented below).
//!
//! ## The determinism seam (journal-driven, not seed-regenerated)
//!
//! `optionstratlib`'s sampler (`decimal_normal_sample`) constructs its **own**
//! `rand::rng()` per draw and cannot consume the run seed, so the walk is
//! **excluded** from same-seed regeneration. The generated price steps are
//! journaled as `SimStep` commands (and the requotes they cause as market-maker
//! `AddOrder`s), so **replay is journal-driven**: it re-executes the recorded
//! commands and reproduces the exact path regardless of the sampler's RNG
//! ([04 §2, §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
//! A venue-owned *seeded* adapter awaits an injectable-RNG seam upstream.
//!
//! ## The `f64` boundary is guarded (rule 2)
//!
//! The walk works in `f64` dollars inside the `optionstratlib` kernel; every
//! generated value is converted back to integer [`Cents`] through a boundary
//! guard that rejects a non-finite, negative, or out-of-`u64` value rather than
//! casting garbage — so a `NaN`/`Inf` can never reach a `SimStep`, a
//! `PriceUpdate`, or a broadcast.

use optionstratlib::ExpirationDate;
use optionstratlib::prelude::{Decimal, Positive};
use optionstratlib::simulation::steps::Step;
use optionstratlib::simulation::{WalkParams, WalkType, WalkTypeAble};
use optionstratlib::utils::TimeFrame;
use serde::{Deserialize, Serialize};

use crate::exchange::Cents;

/// Milliseconds in the venue's fixed 365-day year — the basis for turning a
/// per-step millisecond duration into the `dt` year-fraction `optionstratlib`'s
/// walks consume. A fixed constant (never a wall-clock year) keeps generation
/// reproducible from the same inputs.
const MILLIS_PER_YEAR: f64 = 365.0 * 24.0 * 60.0 * 60.0 * 1_000.0;

/// v1 fixed mean-reversion speed for [`WalkTypeConfig::MeanReverting`] (the OU
/// pull-back rate toward the reversion level). Surfacing it as a knob is a later
/// config addition.
const MEAN_REVERT_SPEED: f64 = 1.0;

/// v1 fixed jump intensity for [`WalkTypeConfig::JumpDiffusion`] — the expected
/// number of jumps per year.
const JUMP_INTENSITY: f64 = 12.0;

/// v1 fixed jump-size mean for [`WalkTypeConfig::JumpDiffusion`] (zero-mean
/// jumps: no directional bias).
const JUMP_MEAN: f64 = 0.0;

/// v1 fixed jump-size volatility for [`WalkTypeConfig::JumpDiffusion`].
const JUMP_VOLATILITY: f64 = 0.1;

/// The minimum path length a generator must produce (a start plus at least one
/// step), so a served path is never degenerate.
const MIN_PATH_STEPS: usize = 2;

// ============================================================================
// Errors
// ============================================================================

/// A price-walk failure — a degenerate walk parameter, an `optionstratlib`
/// generation error, or a non-finite / out-of-range generated price. The
/// simulator turns any of these into "back this asset off dormant" rather than
/// busy-looping or poisoning a price (rule 2).
#[derive(Debug, thiserror::Error)]
pub enum SimError {
    /// A walk parameter was non-finite or otherwise invalid before generation.
    #[error("invalid walk parameter: {0}")]
    InvalidParameter(String),
    /// `optionstratlib` failed to generate the walk.
    #[error("walk generation failed: {0}")]
    WalkFailed(String),
    /// A generated price was non-finite, negative, or exceeded the `u64` cents
    /// range and could not cross back onto the integer-cents surface.
    #[error("walk produced a non-representable price: {0}")]
    NonRepresentablePrice(f64),
    /// A programmatic override named an underlying the simulator does not host.
    #[error("no such simulated underlying: {0}")]
    UnknownUnderlying(String),
    /// The deterministic virtual timeline is exhausted — the step counter or the
    /// virtual clock reached the `u64` ceiling. Unreachable in practice (a
    /// `2^64`-step / `2^64`-ms horizon), but the simulator fails **closed**
    /// (halts) rather than emitting a wrapped index or a clamped, non-monotonic
    /// instant that would corrupt replay (rule 3).
    #[error("simulation virtual timeline exhausted")]
    TimelineExhausted,
}

// ============================================================================
// WalkTypeConfig
// ============================================================================

/// The venue's surfaced walk type — the v1 set mapped onto
/// [`optionstratlib::simulation::WalkType`]. The per-asset `drift` and
/// `volatility` are carried on the asset config, not here, so this stays a small
/// stable enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum WalkTypeConfig {
    /// Geometric Brownian motion (log-normal increments): `WalkType::GeometricBrownian`.
    GeometricBrownian,
    /// Ornstein-Uhlenbeck mean reversion toward the initial price:
    /// `WalkType::MeanReverting` (fixed speed constant in v1).
    MeanReverting,
    /// Jump diffusion (Gaussian increments with occasional Poisson jumps):
    /// `WalkType::JumpDiffusion` (fixed intensity + jump-size constants in v1).
    JumpDiffusion,
}

impl WalkTypeConfig {
    /// Maps this config onto the concrete [`optionstratlib::simulation::WalkType`],
    /// filling the v1 fixed constants for the parameters this config does not
    /// surface. `mean` is the OU reversion level (the asset's initial price, in
    /// dollars) and is ignored by the non-OU variants.
    ///
    /// # Errors
    ///
    /// [`SimError::InvalidParameter`] if a fixed constant cannot be represented
    /// as the required `optionstratlib` numeric type (never in practice — the
    /// constants are well-formed).
    #[must_use = "the mapped WalkType is the input to the optionstratlib generator"]
    pub fn to_walk_type(
        self,
        dt: Positive,
        drift: Decimal,
        volatility: Positive,
        mean: Positive,
    ) -> Result<WalkType, SimError> {
        Ok(match self {
            Self::GeometricBrownian => WalkType::GeometricBrownian {
                dt,
                drift,
                volatility,
            },
            Self::MeanReverting => WalkType::MeanReverting {
                dt,
                volatility,
                speed: positive(MEAN_REVERT_SPEED)?,
                mean,
            },
            Self::JumpDiffusion => WalkType::JumpDiffusion {
                dt,
                drift,
                volatility,
                intensity: positive(JUMP_INTENSITY)?,
                jump_mean: decimal(JUMP_MEAN)?,
                jump_volatility: positive(JUMP_VOLATILITY)?,
            },
        })
    }
}

// ============================================================================
// Generation
// ============================================================================

/// A zero-sized `optionstratlib` walker: the trait's default methods carry the
/// stochastic algorithms, so an empty impl is the whole walker. `Clone` (via
/// `Copy`) satisfies the object-safe `WalkTypeAbleClone` blanket impl the boxed
/// walker in [`WalkParams`] needs.
#[derive(Debug, Clone, Copy)]
struct VenueWalker;

impl WalkTypeAble<f64, f64> for VenueWalker {}

/// Pre-generates a price path of `size` steps for one asset, entirely through
/// `optionstratlib`'s walk kernel, and converts each step back onto the
/// integer-cents surface.
///
/// The walk runs in `f64` dollars (spot = `initial_price` in cents / 100). The
/// returned path always starts at `initial_price` (the generators seed the first
/// element with the start value), so a fresh path is continuous with the seed.
///
/// # Errors
///
/// - [`SimError::InvalidParameter`] if `drift` / `volatility` / the derived `dt`
///   are non-finite or non-positive (the `f64` boundary, guarded before the
///   kernel);
/// - [`SimError::WalkFailed`] if `optionstratlib` rejects the walk;
/// - [`SimError::NonRepresentablePrice`] if a generated price cannot cross back
///   onto the `u64`-cents surface.
#[must_use = "the generated path is the simulator's pre-generated horizon"]
pub(crate) fn generate_path(
    walk_type: WalkTypeConfig,
    initial_price: Cents,
    drift: f64,
    volatility: f64,
    step_ms: u64,
    size: usize,
) -> Result<Vec<Cents>, SimError> {
    let start_dollars = initial_price.get() as f64 / 100.0;

    // Guard the `f64` boundary before the kernel: `dt` (year-fraction per step),
    // drift, and volatility must be finite and (where required) positive.
    let dt = positive(step_ms as f64 / MILLIS_PER_YEAR)?;
    let drift_dec = decimal(drift)?;
    let vol = positive(volatility)?;
    // The OU reversion level is the initial price; ignored by the other walks.
    let mean = positive(start_dollars.max(f64::MIN_POSITIVE))?;
    let mapped = walk_type.to_walk_type(dt, drift_dec, vol, mean)?;

    let size = size.max(MIN_PATH_STEPS);
    // The `Step`'s `ExpirationDate::Days` is a clock-free nominal for the walk's
    // x-axis only; it is never journaled (the walk output is), so it does not
    // touch the `DateTime`-only sequenced-path rule.
    let init_step = Step::new(
        0.0_f64,
        TimeFrame::Minute,
        ExpirationDate::Days(Positive::ONE),
        start_dollars,
    );
    let params = WalkParams {
        size,
        init_step,
        walk_type: mapped,
        walker: Box::new(VenueWalker),
    };

    // Dispatch onto the matching `optionstratlib` generator; the method reads
    // `params.walk_type`, which we built from `walk_type` above.
    let walker = VenueWalker;
    let prices = match walk_type {
        WalkTypeConfig::GeometricBrownian => walker.geometric_brownian(&params),
        WalkTypeConfig::MeanReverting => walker.mean_reverting(&params),
        WalkTypeConfig::JumpDiffusion => walker.jump_diffusion(&params),
    }
    .map_err(|error| SimError::WalkFailed(error.to_string()))?;

    let mut path = Vec::with_capacity(prices.len());
    for price in prices {
        path.push(dollars_to_cents(price.to_f64())?);
    }
    Ok(path)
}

/// Converts a walk's `f64` dollar price back onto the integer-cents surface,
/// guarding the boundary: a non-finite, negative, or out-of-`u64` value is
/// rejected rather than cast into a poisoned [`Cents`] (rule 2).
///
/// # Errors
///
/// [`SimError::NonRepresentablePrice`] if `dollars` is `NaN`/`±Inf`, negative,
/// or would overflow `u64` cents.
#[must_use = "the converted cents value is the price crossing back onto the surface"]
fn dollars_to_cents(dollars: f64) -> Result<Cents, SimError> {
    if !dollars.is_finite() || dollars < 0.0 {
        return Err(SimError::NonRepresentablePrice(dollars));
    }
    let cents = (dollars * 100.0).round();
    if !(0.0..=(u64::MAX as f64)).contains(&cents) {
        return Err(SimError::NonRepresentablePrice(dollars));
    }
    // Bounded above by `u64::MAX` and non-negative: the cast is exact.
    Ok(Cents::new(cents as u64))
}

/// Builds a **strictly-positive** `optionstratlib` [`Positive`] from `value`,
/// mapping a non-finite or non-positive input to a typed error — the `f64`
/// boundary gate for a walk parameter (`Positive::new` itself admits `0.0`, which
/// would make a zero-volatility / zero-`dt` walk degenerate, so this rejects it
/// up front, matching the pricer's `sigma > 0.0` discipline).
#[inline]
fn positive(value: f64) -> Result<Positive, SimError> {
    if !(value.is_finite() && value > 0.0) {
        return Err(SimError::InvalidParameter(format!(
            "expected a finite, strictly-positive value, got {value}"
        )));
    }
    Positive::new(value)
        .map_err(|error| SimError::InvalidParameter(format!("expected a positive value: {error}")))
}

/// Builds an `optionstratlib` [`Decimal`] from `value`, mapping a non-finite
/// input to a typed error.
#[inline]
fn decimal(value: f64) -> Result<Decimal, SimError> {
    Decimal::from_f64_retain(value).ok_or_else(|| {
        SimError::InvalidParameter(format!("value is not a finite decimal: {value}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const INITIAL: Cents = Cents::new(5_000_000); // $50,000
    const STEP_MS: u64 = 60_000; // one virtual minute

    #[test]
    fn test_walk_type_config_maps_to_geometric_brownian() {
        let mapped = WalkTypeConfig::GeometricBrownian
            .to_walk_type(
                Positive::ONE,
                Decimal::ZERO,
                Positive::ONE,
                Positive::HUNDRED,
            )
            .expect("mapping is well-formed");
        assert!(matches!(mapped, WalkType::GeometricBrownian { .. }));
    }

    #[test]
    fn test_walk_type_config_maps_to_mean_reverting_with_fixed_speed() {
        let mapped = WalkTypeConfig::MeanReverting
            .to_walk_type(
                Positive::ONE,
                Decimal::ZERO,
                Positive::ONE,
                Positive::HUNDRED,
            )
            .expect("mapping is well-formed");
        // The OU mean is the supplied reversion level; the speed is the v1 fixed
        // constant.
        match mapped {
            WalkType::MeanReverting { mean, speed, .. } => {
                assert_eq!(mean, Positive::HUNDRED);
                assert_eq!(speed, positive(MEAN_REVERT_SPEED).expect("const"));
            }
            other => panic!("expected MeanReverting, got {other}"),
        }
    }

    #[test]
    fn test_walk_type_config_maps_to_jump_diffusion() {
        let mapped = WalkTypeConfig::JumpDiffusion
            .to_walk_type(
                Positive::ONE,
                Decimal::ZERO,
                Positive::ONE,
                Positive::HUNDRED,
            )
            .expect("mapping is well-formed");
        assert!(matches!(mapped, WalkType::JumpDiffusion { .. }));
    }

    #[test]
    fn test_generate_path_length_and_starts_at_initial() {
        for walk_type in [
            WalkTypeConfig::GeometricBrownian,
            WalkTypeConfig::MeanReverting,
            WalkTypeConfig::JumpDiffusion,
        ] {
            let path = generate_path(walk_type, INITIAL, 0.05, 0.20, STEP_MS, 32)
                .expect("a well-posed walk generates a path");
            assert_eq!(path.len(), 32, "the path has exactly `size` steps");
            assert_eq!(
                path[0], INITIAL,
                "the generator seeds the first step with the start price"
            );
        }
    }

    #[test]
    fn test_generate_path_clamps_size_to_minimum() {
        let path = generate_path(
            WalkTypeConfig::GeometricBrownian,
            INITIAL,
            0.0,
            0.2,
            STEP_MS,
            0,
        )
        .expect("a walk generates a path");
        assert!(
            path.len() >= MIN_PATH_STEPS,
            "size is floored to a valid path"
        );
    }

    #[test]
    fn test_generate_path_rejects_non_finite_volatility() {
        for bad_vol in [0.0, -0.2, f64::NAN, f64::INFINITY] {
            assert!(
                generate_path(
                    WalkTypeConfig::GeometricBrownian,
                    INITIAL,
                    0.05,
                    bad_vol,
                    STEP_MS,
                    16
                )
                .is_err(),
                "a degenerate volatility ({bad_vol}) must be rejected at the boundary"
            );
        }
    }

    #[test]
    fn test_generate_path_rejects_non_finite_drift() {
        for bad_drift in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                generate_path(
                    WalkTypeConfig::GeometricBrownian,
                    INITIAL,
                    bad_drift,
                    0.2,
                    STEP_MS,
                    16
                )
                .is_err(),
                "a non-finite drift ({bad_drift}) must be rejected"
            );
        }
    }

    #[test]
    fn test_dollars_to_cents_guards_the_boundary() {
        assert_eq!(dollars_to_cents(50.0).expect("finite"), Cents::new(5_000));
        for bad in [f64::NAN, f64::INFINITY, -1.0, f64::NEG_INFINITY] {
            assert!(
                dollars_to_cents(bad).is_err(),
                "a non-representable dollar value ({bad}) must not cross onto the surface"
            );
        }
    }
}
