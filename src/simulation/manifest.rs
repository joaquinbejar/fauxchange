//! The **run manifest** — the self-describing record of the inputs that fix a
//! run's determinism, so a replay can assert it is reproducing the same run
//! ([04 §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
//!
//! Per [04 §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding)
//! the full manifest carries the run **seed**, the **clock mode**, the
//! microstructure config, the instrument seed, and the **pinned crate/dependency
//! versions**. #028 recorded the first two (`seed` / `clock_mode`); #030 (the
//! replay driver) extends it with the remaining fields, so a recorded scenario
//! bundle is self-describing and portable between machines and the oracle is
//! **scoped to a matching set of versions**
//! ([04 §4](../../docs/04-market-data-and-replay.md#4-historical-replay)).
//!
//! The manifest is **recorded** (logged at boot, and — from #029 — written
//! alongside the durable journal / carried in the #030 scenario bundle), not a
//! wire DTO. It is deliberately **not** on the `#[serde(deny_unknown_fields)]`
//! contract, and every #030 field is `#[serde(default)]`, so a manifest written by
//! an **older** binary (only `seed` + `clock_mode`) still decodes here (a missing
//! field defaults) — the manifest stays **backward-readable** across versions.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::exchange::VENUE_ENVELOPE_SCHEMA;
use crate::simulation::clock::ClockMode;

/// The default microstructure-config fingerprint recorded when a run carries no
/// explicit microstructure profile. The declarative microstructure surface is
/// v0.5 (#044–#050); until it lands the venue runs the built-in default, so the
/// recorded fingerprint is this stable token — an honest placeholder the later
/// microstructure work replaces with the real config fingerprint.
pub const DEFAULT_MICROSTRUCTURE_FINGERPRINT: &str = "microstructure.default.v1";

/// The pinned crate/dependency versions that **scope** the determinism oracle: a
/// replay reproduces identical fills/events only across a **matching** version set
/// ([04 §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding),
/// [05 §11](../../docs/05-microstructure-config.md#11-determinism-of-microstructure)).
///
/// Every field is captured at **compile time** from a real source — never a
/// fabricated number:
///
/// - [`fauxchange`](Self::fauxchange) is `env!("CARGO_PKG_VERSION")`. Because the
///   crate's `Cargo.lock` pins the whole matching stack
///   (`option-chain-orderbook` / `orderbook-rs` / `pricelevel`), a matching-affecting
///   dependency bump is a `fauxchange` SemVer event ([SEMVER.md](../../docs/SEMVER.md)),
///   so this version transitively pins that stack.
/// - [`optionstratlib`](Self::optionstratlib) is the pricing/walk crate's own
///   `optionstratlib::VERSION` (`env!("CARGO_PKG_VERSION")` inside that crate),
///   named directly because it is a **direct** dependency at the `f64` math seam.
/// - [`envelope_schema`](Self::envelope_schema) is [`VENUE_ENVELOPE_SCHEMA`] — the
///   journal wire contract a bump of which is refused by recovery
///   ([`JournalError::SchemaTooNew`](crate::exchange::JournalError)).
///
/// A replay compares the bundle's recorded set against [`current`](Self::current);
/// a mismatch is a **typed reject**, never a silent divergent reproduction.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
pub struct DependencyVersions {
    /// The `fauxchange` crate version (`env!("CARGO_PKG_VERSION")`).
    #[serde(default)]
    pub fauxchange: String,
    /// The `optionstratlib` crate version (`optionstratlib::VERSION`).
    #[serde(default)]
    pub optionstratlib: String,
    /// The venue envelope schema tag ([`VENUE_ENVELOPE_SCHEMA`]).
    #[serde(default)]
    pub envelope_schema: String,
}

impl DependencyVersions {
    /// The version set of the **running** binary — the reference a bundle's
    /// recorded set is compared against.
    #[must_use]
    pub fn current() -> Self {
        Self {
            fauxchange: env!("CARGO_PKG_VERSION").to_string(),
            optionstratlib: optionstratlib::VERSION.to_string(),
            envelope_schema: VENUE_ENVELOPE_SCHEMA.to_string(),
        }
    }

    /// Whether this recorded set matches the running binary's exactly (the oracle
    /// scope holds).
    #[must_use]
    pub fn matches_current(&self) -> bool {
        *self == Self::current()
    }

    /// The first field that differs from the running binary, as
    /// `(field, expected, found)` — `expected` is the running binary's value and
    /// `found` is this recorded value — or `None` when the sets match. Used to
    /// build a precise typed version-mismatch reject.
    #[must_use]
    pub fn first_mismatch(&self) -> Option<(&'static str, String, String)> {
        let current = Self::current();
        let fields: [(&'static str, &String, &String); 3] = [
            ("fauxchange", &current.fauxchange, &self.fauxchange),
            (
                "optionstratlib",
                &current.optionstratlib,
                &self.optionstratlib,
            ),
            (
                "envelope_schema",
                &current.envelope_schema,
                &self.envelope_schema,
            ),
        ];
        fields
            .into_iter()
            .find(|(_, expected, found)| expected != found)
            .map(|(field, expected, found)| (field, expected.clone(), found.clone()))
    }
}

/// The determinism inputs recorded for a run — the `seed` + `clock_mode` #028
/// owns, extended by #030 with the `instrument_seed`, the microstructure-config
/// fingerprint, and the pinned crate/dependency [`versions`](Self::versions).
///
/// `ToSchema` is derived so the manifest carried in a #030 scenario bundle appears
/// in the served OpenAPI document (the bundle is a portable wire artifact); the
/// complex journal envelope inside the bundle stays opaque there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RunManifest {
    /// The one run-level seed every stochastic sub-stream derives from
    /// ([04 §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
    pub seed: u64,
    /// The venue clock mode token (`realtime` / `accelerated` / `stepped`) the run
    /// executed under.
    pub clock_mode: String,
    /// The seed that populated the instrument set (chain synthesis / scenario
    /// seed). Defaults to the run [`seed`](Self::seed) — a single-seed run derives
    /// its instrument set from the same seed — and is `#[serde(default)]` so an
    /// older manifest without it still decodes.
    #[serde(default)]
    pub instrument_seed: u64,
    /// The microstructure-config fingerprint that scopes the oracle (fees, tick /
    /// lot, STP, latency). The declarative surface is v0.5; until it lands this is
    /// [`DEFAULT_MICROSTRUCTURE_FINGERPRINT`]. `#[serde(default)]` for
    /// backward-readability.
    #[serde(default = "default_microstructure_fingerprint")]
    pub microstructure_fingerprint: String,
    /// The pinned crate/dependency [`DependencyVersions`] that scope the oracle.
    /// `#[serde(default)]` so an older manifest without it still decodes (it then
    /// carries the empty set, which a replay's version check refuses — an
    /// unversioned bundle cannot be proven to reproduce).
    #[serde(default)]
    pub versions: DependencyVersions,
}

/// The `#[serde(default = …)]` provider for
/// [`RunManifest::microstructure_fingerprint`].
fn default_microstructure_fingerprint() -> String {
    DEFAULT_MICROSTRUCTURE_FINGERPRINT.to_string()
}

impl RunManifest {
    /// Records a manifest from the run `seed` and the venue clock `mode`, pinning
    /// the running binary's [`DependencyVersions`] and defaulting the
    /// `instrument_seed` to `seed` and the microstructure fingerprint to
    /// [`DEFAULT_MICROSTRUCTURE_FINGERPRINT`].
    #[must_use]
    pub fn new(seed: u64, mode: ClockMode) -> Self {
        Self {
            seed,
            clock_mode: mode.as_token().to_string(),
            instrument_seed: seed,
            microstructure_fingerprint: DEFAULT_MICROSTRUCTURE_FINGERPRINT.to_string(),
            versions: DependencyVersions::current(),
        }
    }

    /// Overrides the recorded instrument seed (the scenario / chain-synthesis
    /// seed), when it differs from the run seed.
    #[must_use]
    pub fn with_instrument_seed(mut self, instrument_seed: u64) -> Self {
        self.instrument_seed = instrument_seed;
        self
    }

    /// Overrides the recorded microstructure-config fingerprint.
    #[must_use]
    pub fn with_microstructure_fingerprint(mut self, fingerprint: impl Into<String>) -> Self {
        self.microstructure_fingerprint = fingerprint.into();
        self
    }

    /// A secret-free one-line summary for the boot log.
    #[must_use]
    pub fn summary(&self) -> String {
        format!("seed={} clock_mode={}", self.seed, self.clock_mode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_manifest_records_seed_and_clock_mode() {
        let manifest = RunManifest::new(42, ClockMode::Stepped { step_ms: 60_000 });
        assert_eq!(manifest.seed, 42);
        assert_eq!(manifest.clock_mode, "stepped");
        assert_eq!(manifest.summary(), "seed=42 clock_mode=stepped");
    }

    #[test]
    fn test_run_manifest_roundtrips_through_json() {
        let manifest = RunManifest::new(7, ClockMode::Accelerated { multiplier: 60 });
        let json = match serde_json::to_string(&manifest) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        match serde_json::from_str::<RunManifest>(&json) {
            Ok(back) => assert_eq!(back, manifest),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }

    #[test]
    fn test_run_manifest_records_realtime() {
        let manifest = RunManifest::new(0, ClockMode::Realtime);
        assert_eq!(manifest.clock_mode, "realtime");
    }

    #[test]
    fn test_run_manifest_new_pins_current_versions_and_defaults_instrument_seed() {
        // #030: `new` records the running binary's version set and defaults the
        // instrument seed to the run seed and the microstructure fingerprint.
        let manifest = RunManifest::new(99, ClockMode::Realtime);
        assert_eq!(
            manifest.instrument_seed, 99,
            "instrument seed defaults to the run seed"
        );
        assert_eq!(
            manifest.microstructure_fingerprint,
            DEFAULT_MICROSTRUCTURE_FINGERPRINT
        );
        assert_eq!(manifest.versions, DependencyVersions::current());
        assert!(manifest.versions.matches_current());
    }

    #[test]
    fn test_dependency_versions_current_captures_real_compile_time_values() {
        let versions = DependencyVersions::current();
        // Real, non-fabricated: the fauxchange crate version + the optionstratlib
        // crate version + the venue envelope schema.
        assert_eq!(versions.fauxchange, env!("CARGO_PKG_VERSION"));
        assert_eq!(versions.optionstratlib, optionstratlib::VERSION);
        assert_eq!(versions.envelope_schema, VENUE_ENVELOPE_SCHEMA);
        assert!(versions.first_mismatch().is_none());
    }

    #[test]
    fn test_dependency_versions_first_mismatch_names_the_field() {
        let mut versions = DependencyVersions::current();
        versions.fauxchange = "0.0.0-not-a-real-version".to_string();
        match versions.first_mismatch() {
            Some((field, expected, found)) => {
                assert_eq!(field, "fauxchange");
                assert_eq!(expected, env!("CARGO_PKG_VERSION"));
                assert_eq!(found, "0.0.0-not-a-real-version");
            }
            None => panic!("a wrong fauxchange version must be a mismatch"),
        }
        assert!(!versions.matches_current());
    }

    #[test]
    fn test_manifest_is_backward_readable_from_the_028_shape() {
        // A manifest written by an OLDER binary carried only `seed` + `clock_mode`.
        // It must still decode here (the #030 fields default) — backward-readable.
        let legacy = r#"{"seed":5,"clock_mode":"stepped"}"#;
        match serde_json::from_str::<RunManifest>(legacy) {
            Ok(manifest) => {
                assert_eq!(manifest.seed, 5);
                assert_eq!(manifest.clock_mode, "stepped");
                assert_eq!(
                    manifest.instrument_seed, 0,
                    "a missing instrument_seed defaults"
                );
                assert_eq!(
                    manifest.microstructure_fingerprint,
                    DEFAULT_MICROSTRUCTURE_FINGERPRINT
                );
                // An older manifest carries no versions → the empty set, which a
                // replay's version check refuses (it cannot be proven to reproduce).
                assert_eq!(manifest.versions, DependencyVersions::default());
                assert!(!manifest.versions.matches_current());
            }
            Err(e) => panic!("a legacy #028 manifest must still decode: {e}"),
        }
    }
}
