//! The packaged **`fauxchange conformance`** harness — the single runnable
//! artifact a consumer executes to decide whether to trust the venue
//! ([051](../../milestones/v1.0-stability/051-conformance-harness.md),
//! [03 §7](../../docs/03-protocol-surfaces.md#7-protocol-parity-guarantees),
//! [TESTING.md §6](../../docs/TESTING.md#6-conformance--parity-rest--ws--fix)).
//!
//! [`run`] spins a set of ephemeral, in-process venues (a real REST server + a
//! real FIX 4.4 acceptor + the WS subscription manager over one identically-
//! seeded [`AppState`](crate::state::AppState) per case) and drives the *frozen*
//! parity + conformance suites (#018/#041) across all three surfaces, returning a
//! machine-readable [`ConformanceReport`] a downstream CI gates on
//! ([`ConformanceReport::exit_code`]). It packages the existing assertions behind
//! one entrypoint — it never re-derives parity and never opens a private matching
//! path; every order flows through the same sequenced order path the gateways use.
//!
//! The scope is the documented, milestone-scoped parity contract:
//!
//! - **Order-entry parity is REST ≡ FIX** (WS is not an order-entry surface):
//!   place / partial-fill / cancel-replace / STP outcome / per-leg fees /
//!   rejection / same-payload retry, one identically-seeded fresh venue per
//!   surface, compared under the §7 normalization rule.
//! - **Observation parity is REST/WS/FIX**: one committed fill's join keys, the
//!   anonymised WS `fill`, and FIX `W`/`X` ≡ WS market data.
//! - **Control parity is REST/WS only**: the same knob over both, and the
//!   asserted absence of any FIX control message.
//! - **FIX conformance**: session admin + order + market data + every reject row
//!   of the [03 §8](../../docs/03-protocol-surfaces.md#8-error-mapping-across-surfaces)
//!   matrix, each with a redacted `Text (58)`.
//! - **REST/WS conformance**: the OpenAPI route shape, the `/health` auth
//!   exemption, permission gating, and WS snapshot→delta sequencing.

mod cases;
mod harness;
mod parity;
pub mod report;

pub use report::{
    CaseReport, ConformanceReport, SCHEMA_VERSION, SuiteReport, Surface, SurfaceSummary, Totals,
    Verdict,
};

/// Runs the full packaged conformance harness across REST, WS, and FIX, returning
/// the machine-readable report. Case assertion failures are captured into the
/// report (never a panic); the report's [`ConformanceReport::exit_code`] is `0`
/// when every case passes and `1` on any surface failure.
pub async fn run() -> ConformanceReport {
    let suites = vec![
        cases::run_order_entry_parity().await,
        cases::run_observation_parity().await,
        cases::run_control_parity().await,
        cases::run_fix_conformance().await,
        cases::run_rest_ws_conformance().await,
    ];
    let report = ConformanceReport::from_suites(suites);
    tracing::info!(
        verdict = ?report.verdict,
        cases = report.totals.cases,
        passed = report.totals.passed,
        failed = report.totals.failed,
        "conformance run complete"
    );
    report
}

#[cfg(test)]
mod tests {
    use super::harness::{UNDERLYING, VenueServer, http, journaled_events};
    use super::*;

    #[tokio::test]
    async fn test_venue_server_spins_up_and_serves_rest_and_fix() {
        // Harness plumbing: a fresh server binds both gateways on loopback, serves
        // an auth-exempt /health tokenless, and journals nothing before any order.
        let server = VenueServer::start().await.expect("venue server must start");
        let health = http(server.rest_addr(), "GET", "/health", None, None)
            .await
            .expect("health must respond");
        assert_eq!(health.status, 200, "health is reachable tokenless");

        let events = journaled_events(server.state(), UNDERLYING)
            .await
            .expect("journal snapshot");
        assert!(events.is_empty(), "a fresh venue has journaled nothing");
        // Dropping `server` here tears down both gateway tasks.
    }

    #[tokio::test]
    async fn test_rest_ws_conformance_suite_runs_green() {
        // A light plumbing check that one suite runs and reports; the full run is the
        // integration test (`tests/conformance_harness.rs`).
        let suite = cases::run_rest_ws_conformance().await;
        assert_eq!(
            suite.failed, 0,
            "the REST/WS suite must be green: {suite:?}"
        );
        assert!(suite.passed >= 5, "the suite must record its cases");
    }

    #[test]
    fn test_report_from_a_single_suite_round_trips_json() {
        let suite = SuiteReport {
            name: "s".to_string(),
            cases: Vec::new(),
            passed: 0,
            failed: 0,
        };
        let report = ConformanceReport::from_suites(vec![suite]);
        let json = serde_json::to_string_pretty(&report).expect("serialise");
        let parsed: ConformanceReport = serde_json::from_str(&json).expect("round-trip");
        assert_eq!(parsed.schema_version, SCHEMA_VERSION);
        assert!(parsed.passed(), "an empty run has no failures");
    }
}
