//! The **packaged conformance harness** integration test (#051) — the `fauxchange
//! conformance` run itself, over ephemeral in-process venues.
//!
//! This is the milestone's core acceptance: the packaged harness runs **green
//! across REST, WS, and FIX** and emits a machine-readable report a downstream CI
//! gates on ([051](../milestones/v1.0-stability/051-conformance-harness.md),
//! [TESTING.md §6-§7](../docs/TESTING.md#6-conformance--parity-rest--ws--fix)). It
//! asserts the run is green, every documented suite + surface is exercised, the
//! order-entry / observation / control / FIX reject coverage is present by stable
//! case id, and the report round-trips its `deny_unknown_fields` JSON contract.
//!
//! The granular per-assertion parity suites live in `tests/parity.rs` +
//! `tests/conformance/`; this test proves the PACKAGING (one run, one report,
//! one exit code), not the individual parity rows those own.

use fauxchange::conformance::{self, ConformanceReport, SCHEMA_VERSION, Surface, Verdict};

/// A readable dump of every failing case, so a red run explains itself.
fn failing_cases(report: &ConformanceReport) -> String {
    let mut lines = Vec::new();
    for suite in &report.suites {
        for case in &suite.cases {
            if case.verdict == Verdict::Fail {
                let detail = case.detail.as_deref().unwrap_or("<no detail>");
                lines.push(format!("  {}/{}: {}", suite.name, case.id, detail));
            }
        }
    }
    if lines.is_empty() {
        "  (none)".to_string()
    } else {
        lines.join("\n")
    }
}

fn suite<'a>(
    report: &'a ConformanceReport,
    name: &str,
) -> &'a fauxchange::conformance::SuiteReport {
    report
        .suites
        .iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("suite {name} must be present"))
}

fn case_ids(report: &ConformanceReport, suite_name: &str) -> Vec<String> {
    suite(report, suite_name)
        .cases
        .iter()
        .map(|c| c.id.clone())
        .collect()
}

#[tokio::test]
async fn test_packaged_conformance_run_is_green_across_all_surfaces() {
    let report = conformance::run().await;

    // Green + exit 0 — the acceptance criterion.
    assert!(
        report.passed(),
        "conformance must be green; failing cases:\n{}",
        failing_cases(&report)
    );
    assert_eq!(report.exit_code(), 0, "a green run exits zero");
    assert_eq!(report.verdict, Verdict::Pass);
    assert!(
        report.failed_surfaces().is_empty(),
        "no surface may have a failing case"
    );
    assert!(
        report.totals.cases >= 20,
        "the run must exercise many cases"
    );
    assert_eq!(report.totals.failed, 0);
    assert_eq!(report.totals.passed, report.totals.cases);
}

#[tokio::test]
async fn test_report_covers_every_documented_suite_surface_and_case() {
    let report = conformance::run().await;

    // The five documented suites are present.
    let names: Vec<&str> = report.suites.iter().map(|s| s.name.as_str()).collect();
    for wanted in [
        "order_entry_parity",
        "observation_parity",
        "control_parity",
        "fix_conformance",
        "rest_ws_conformance",
    ] {
        assert!(names.contains(&wanted), "suite {wanted} must be present");
    }

    // Every surface is exercised and clean.
    for surface in Surface::ALL {
        let summary = report
            .surfaces
            .iter()
            .find(|s| s.surface == surface)
            .unwrap_or_else(|| panic!("surface {surface:?} must have a summary"));
        assert!(summary.cases > 0, "surface {surface:?} must be exercised");
        assert_eq!(summary.failed, 0, "surface {surface:?} must be clean");
    }

    // Order-entry parity packages all seven documented cases (REST ≡ FIX).
    let order_entry = case_ids(&report, "order_entry_parity");
    for wanted in [
        "order_entry.place",
        "order_entry.partial_fill",
        "order_entry.cancel_replace",
        "order_entry.stp_rejection",
        "order_entry.per_leg_fees",
        "order_entry.rejection",
        "order_entry.same_payload_retry",
    ] {
        assert!(
            order_entry.iter().any(|id| id == wanted),
            "order-entry case {wanted} must be packaged"
        );
    }

    // Observation parity: one fill across REST/WS/FIX join keys.
    assert!(
        case_ids(&report, "observation_parity")
            .iter()
            .any(|id| id == "observation.one_fill_rest_ws_fix"),
        "the one-fill REST/WS/FIX observation case must be packaged"
    );

    // Control parity asserts the no-FIX-control rule and drives a REAL /ws socket.
    let control = case_ids(&report, "control_parity");
    for wanted in [
        "control.no_fix_control_message",
        "control.ws_live_permission_gate",
    ] {
        assert!(
            control.iter().any(|id| id == wanted),
            "control case {wanted} must be packaged"
        );
    }

    // FIX conformance packages every reject row of the 03 §8 matrix plus session
    // admin and sequence reset.
    let fix = case_ids(&report, "fix_conformance");
    for wanted in [
        "fix.session_admin_order_market_data",
        "fix.sequence_reset",
        "fix.reject_3_malformed_frame",
        "fix.reject_8_conflicting_clordid",
        "fix.reject_9_cancel_unknown",
        "fix.reject_y_unsupported_market_data",
        "fix.reject_j_unsupported_application",
        "fix.logout_5_credential_failure",
    ] {
        assert!(
            fix.iter().any(|id| id == wanted),
            "FIX conformance row {wanted} must be packaged"
        );
    }
}

#[tokio::test]
async fn test_report_is_machine_readable_and_round_trips() {
    let report = conformance::run().await;

    // Pretty JSON (the subcommand's stdout shape) round-trips under
    // deny_unknown_fields.
    let json = serde_json::to_string_pretty(&report).expect("serialise report");
    let parsed: ConformanceReport = serde_json::from_str(&json).expect("round-trip report");
    assert_eq!(parsed.schema_version, SCHEMA_VERSION);
    assert_eq!(parsed.totals.cases, report.totals.cases);
    assert_eq!(parsed.verdict, report.verdict);

    // Every failure detail (there are none in a green run, but the contract holds)
    // is bounded and control-free — the redaction backstop.
    for suite in &parsed.suites {
        for case in &suite.cases {
            if let Some(detail) = &case.detail {
                assert!(detail.len() <= 540, "a detail must be bounded");
                assert!(
                    !detail.contains('\u{0}'),
                    "a detail must carry no control bytes"
                );
            }
        }
    }
}
