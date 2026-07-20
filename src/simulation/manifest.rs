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
    /// `found` is this recorded value — or `None` when the sets match. This is the
    /// **exact bit-reproducibility** predicate (paired with [`matches_current`]): a
    /// difference in *any* field means the run is not guaranteed bit-for-bit
    /// reproducible, and it drives the honest non-blocking WARN on the replay load
    /// path. It is **not** the load-admission gate — see [`first_incompatibility`].
    ///
    /// [`first_incompatibility`]: Self::first_incompatibility
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

    /// The **load-admission** compatibility check — the gate the replay/recovery
    /// path runs to decide whether a recorded journal may be re-executed **at all**,
    /// distinct from the exact bit-reproducibility predicate [`first_mismatch`].
    ///
    /// Returns the first field that makes this recorded set **incompatible** with the
    /// running binary, as `(field, expected, found)`, or `None` when the set is
    /// load-compatible. Per [SEMVER.md](../../docs/SEMVER.md) the **schema tag is the
    /// primary version pin, the crate version secondary**, so the rule is:
    ///
    /// - [`envelope_schema`](Self::envelope_schema) must match **exactly** — a bump is
    ///   a major SemVer event and a forward-incompatible journal is refused, mirroring
    ///   the per-stream recovery schema gate
    ///   ([`JournalError::SchemaTooNew`](crate::exchange::JournalError)).
    /// - [`fauxchange`](Self::fauxchange) must be **SemVer-compatible, directionally**:
    ///   for a `>= 1.0.0` binary, the **same MAJOR** and `recorded_minor <= running_minor`
    ///   (a `v1.x` journal replays on any *later-or-equal* `v1.y`, the SEMVER promise —
    ///   but a *future*-minor bundle is refused, not admitted); for a `0.x` binary, the
    ///   same `(MAJOR, MINOR)` exactly (SemVer treats a `0.MINOR` bump as breaking). A
    ///   version that does not parse as a full `MAJOR.MINOR.PATCH` (`1.2`, `1.2.3.4`) is
    ///   treated as incompatible — refused, never a panic.
    /// - [`optionstratlib`](Self::optionstratlib) is a **secondary** dependency and is
    ///   **not** a load gate — a difference does not refuse the load (the integrity
    ///   oracle is the backstop). [`first_mismatch`] still reports it for the WARN.
    ///
    /// [`first_mismatch`]: Self::first_mismatch
    #[must_use]
    pub fn first_incompatibility(&self) -> Option<(&'static str, String, String)> {
        let current = Self::current();
        if self.envelope_schema != current.envelope_schema {
            return Some((
                "envelope_schema",
                current.envelope_schema,
                self.envelope_schema.clone(),
            ));
        }
        if !fauxchange_versions_compatible(&current.fauxchange, &self.fauxchange) {
            return Some(("fauxchange", current.fauxchange, self.fauxchange.clone()));
        }
        None
    }
}

/// Parses and **validates a full `MAJOR.MINOR.PATCH` SemVer** string, returning the
/// `(major, minor)` the load-admission gate keys on. A pre-release (`-…`) and/or build
/// (`+…`) suffix attaches to the **patch** (the third component) and is stripped before
/// parsing, so it never affects `MAJOR` / `MINOR`.
///
/// The core must be **exactly three** numeric dot-separated components — a version with
/// a missing or extra component (`1.2`, `1.2.3.4`) is malformed and yields `None`, which
/// the caller treats as incompatible (refused). This is full-structure validation, not a
/// lenient "read the first two numbers" scan: a `1.2.3.4` bundle can no longer slip
/// through as `(1, 2)`. Never panics.
#[must_use]
pub(crate) fn parse_major_minor(version: &str) -> Option<(u64, u64)> {
    // Build metadata (`+…`) then pre-release (`-…`) both hang off the patch; strip them
    // so the remaining core is bare `MAJOR.MINOR.PATCH`.
    let core = version.split('+').next()?.split('-').next()?;
    let mut parts = core.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    // A numeric PATCH must be present (rejects `1.2`) …
    let _patch = parts.next()?.parse::<u64>().ok()?;
    // … and nothing may follow it (rejects `1.2.3.4`).
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor))
}

/// Whether a `recorded` `fauxchange` version is load-compatible with the `running`
/// binary, **directionally from recorded to running**. An unparseable / malformed
/// version on either side is incompatible (refused, never a panic). This is admission
/// only — it does not assert bit-reproducibility.
///
/// - Same `MAJOR` is always required (a cross-major journal is refused).
/// - `>= 1.x`: a recorded journal replays on any **later-or-equal** running minor
///   (`recorded_minor <= running_minor`) — the SEMVER.md "a `v1.x` journal replays on
///   any later `v1.y`" promise, read **directionally**. A bundle recorded by a *future*
///   minor (e.g. a `1.99` bundle on a `1.0` runtime) is **refused**, not admitted — the
///   older runtime cannot prove it understands a newer minor's records.
/// - `0.x`: every `0.MINOR` bump is a breaking boundary (SemVer 0.x), so the
///   `(MAJOR, MINOR)` must match **exactly** (direction is moot under equality).
#[must_use]
fn fauxchange_versions_compatible(running: &str, recorded: &str) -> bool {
    let (Some((run_major, run_minor)), Some((rec_major, rec_minor))) =
        (parse_major_minor(running), parse_major_minor(recorded))
    else {
        return false;
    };
    if run_major != rec_major {
        return false;
    }
    if run_major == 0 {
        run_minor == rec_minor
    } else {
        rec_minor <= run_minor
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
    fn test_parse_major_minor_handles_prerelease_and_rejects_malformed() {
        assert_eq!(parse_major_minor("0.0.1"), Some((0, 0)));
        assert_eq!(parse_major_minor("1.2.3"), Some((1, 2)));
        // A pre-release / build suffix attaches to the patch, never major/minor.
        assert_eq!(parse_major_minor("0.0.0-attacker"), Some((0, 0)));
        assert_eq!(parse_major_minor("0.1.0-mismatch"), Some((0, 1)));
        // Malformed / truncated versions are `None` (the caller refuses them).
        assert_eq!(parse_major_minor("garbage"), None);
        assert_eq!(parse_major_minor(""), None);
        assert_eq!(parse_major_minor("1"), None);
        // A missing PATCH or an EXTRA component is malformed — full-structure
        // validation, so `1.2.3.4` can no longer slip through as `(1, 2)`.
        assert_eq!(parse_major_minor("1.2"), None);
        assert_eq!(parse_major_minor("1.2.3.4"), None);
        assert_eq!(parse_major_minor("1.2.x"), None);
    }

    #[test]
    fn test_fauxchange_versions_compatible_is_directional_at_v1() {
        // `>= 1.x` admits an older-or-equal recorded minor on a newer running binary…
        assert!(fauxchange_versions_compatible("1.5.0", "1.2.0"));
        assert!(fauxchange_versions_compatible("1.5.0", "1.5.9"));
        // …but REFUSES a future-minor bundle on an older runtime (the finding's case:
        // a `1.99` bundle must not load on a `1.0` binary).
        assert!(!fauxchange_versions_compatible("1.0.0", "1.99.0"));
        // A different major is always refused, either direction.
        assert!(!fauxchange_versions_compatible("2.0.0", "1.0.0"));
        assert!(!fauxchange_versions_compatible("1.0.0", "2.0.0"));
        // A malformed version on either side is refused, never a panic.
        assert!(!fauxchange_versions_compatible("1.2.3.4", "1.2.0"));
        assert!(!fauxchange_versions_compatible("1.2.0", "1.2.3.4"));
        // `0.x` stays exact-minor in both directions.
        assert!(fauxchange_versions_compatible("0.7.3", "0.7.0"));
        assert!(!fauxchange_versions_compatible("0.7.0", "0.8.0"));
        assert!(!fauxchange_versions_compatible("0.8.0", "0.7.0"));
    }

    #[test]
    fn test_first_incompatibility_admits_a_compatible_differing_patch() {
        // A recorded set differing ONLY in the crate PATCH (same major.minor) and
        // carrying the current envelope schema is LOAD-compatible — but NOT
        // bit-identical, so it drives the honest WARN (`matches_current` is false).
        let current = DependencyVersions::current();
        let (major, minor) = parse_major_minor(&current.fauxchange).expect("current parses");
        let mut recorded = current.clone();
        recorded.fauxchange = format!("{major}.{minor}.99");
        assert_ne!(
            recorded.fauxchange, current.fauxchange,
            "the patch genuinely differs"
        );
        assert!(
            recorded.first_incompatibility().is_none(),
            "a same-(major,minor) differing-patch set loads"
        );
        assert!(
            !recorded.matches_current(),
            "but it is not bit-identical (the WARN path)"
        );
    }

    #[test]
    fn test_first_incompatibility_refuses_a_different_major() {
        let current = DependencyVersions::current();
        let (major, _) = parse_major_minor(&current.fauxchange).expect("current parses");
        let mut recorded = current.clone();
        recorded.fauxchange = format!("{}.0.0", major + 1);
        match recorded.first_incompatibility() {
            Some((field, expected, found)) => {
                assert_eq!(field, "fauxchange");
                assert_eq!(expected, current.fauxchange);
                assert_eq!(found, recorded.fauxchange);
            }
            None => panic!("a different major must be a load incompatibility"),
        }
    }

    #[test]
    fn test_first_incompatibility_refuses_a_different_minor_at_zero_major() {
        // At the current `0.x` base a `0.MINOR` bump is a breaking boundary; a
        // `>= 1.x` binary instead treats a minor bump as compatible (asserted by the
        // major test above), so this clause only applies at major 0.
        let current = DependencyVersions::current();
        let (major, minor) = parse_major_minor(&current.fauxchange).expect("current parses");
        if major != 0 {
            return;
        }
        let mut recorded = current.clone();
        recorded.fauxchange = format!("0.{}.0", minor + 1);
        assert!(
            matches!(recorded.first_incompatibility(), Some(("fauxchange", _, _))),
            "a differing minor at 0.x is refused"
        );
    }

    #[test]
    fn test_first_incompatibility_refuses_a_differing_envelope_schema() {
        // The envelope schema tag is the PRIMARY pin — an exact-match gate.
        let mut recorded = DependencyVersions::current();
        recorded.envelope_schema = "venue.v2".to_string();
        assert!(matches!(
            recorded.first_incompatibility(),
            Some(("envelope_schema", _, _))
        ));
    }

    #[test]
    fn test_first_incompatibility_admits_a_differing_optionstratlib() {
        // optionstratlib is a SECONDARY dep — a difference does NOT refuse the load
        // (the integrity oracle is the backstop), though `matches_current` reports it.
        let mut recorded = DependencyVersions::current();
        recorded.optionstratlib = "0.0.0-different".to_string();
        assert!(
            recorded.first_incompatibility().is_none(),
            "optionstratlib is not a load-admission gate"
        );
        assert!(!recorded.matches_current());
    }

    #[test]
    fn test_first_incompatibility_refuses_an_unparseable_fauxchange_version() {
        let mut recorded = DependencyVersions::current();
        recorded.fauxchange = "not-a-version".to_string();
        assert!(
            matches!(recorded.first_incompatibility(), Some(("fauxchange", _, _))),
            "an unparseable version is refused, never a panic"
        );
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
