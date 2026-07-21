//! The **run manifest** — the self-describing record of the inputs that fix a
//! run's determinism, so a replay can assert it is reproducing the same run
//! ([04 §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
//!
//! Per [04 §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding)
//! the full manifest carries the run **seed**, the **clock mode**, the
//! microstructure config, the instrument seed, and the pinned crate/dependency
//! versions. #028 owns and records the two this issue is responsible for — the
//! `seed` and the `clock_mode` — and leaves the struct forward-extensible: the
//! remaining fields land with the durable journal and replay driver
//! (#029 / #030), each a new field here.
//!
//! The manifest is **recorded** (logged at boot, and — from #029 — written
//! alongside the durable journal), not a wire DTO; it is deliberately not on the
//! `#[serde(deny_unknown_fields)]` contract so a newer field added by #029/#030
//! does not fail an older reader.

use serde::{Deserialize, Serialize};

use crate::simulation::clock::ClockMode;

/// The determinism inputs recorded for a run — the seed and the clock mode #028
/// owns, extended by later replay issues.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunManifest {
    /// The one run-level seed every stochastic sub-stream derives from
    /// ([04 §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
    pub seed: u64,
    /// The venue clock mode token (`realtime` / `accelerated` / `stepped`) the run
    /// executed under.
    pub clock_mode: String,
}

impl RunManifest {
    /// Records a manifest from the run `seed` and the venue clock `mode`.
    #[must_use]
    pub fn new(seed: u64, mode: ClockMode) -> Self {
        Self {
            seed,
            clock_mode: mode.as_token().to_string(),
        }
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
}
