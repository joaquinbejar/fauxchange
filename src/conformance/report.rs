//! The **machine-readable conformance report** — the artifact a downstream CI
//! gates on ([051](../../milestones/v1.0-stability/051-conformance-harness.md)).
//!
//! The report is a stable, self-describing JSON document
//! ([`SCHEMA_VERSION`] `conformance.v1`): a list of [`SuiteReport`]s, each a list
//! of [`CaseReport`]s carrying a stable machine `id`, the [`Surface`]s the case
//! spans, and a [`Verdict`]; a per-[`Surface`] rollup ([`SurfaceSummary`]); the
//! [`Totals`]; and a single top-level [`Verdict`]. A consumer gates on the
//! process **exit code** ([`ConformanceReport::exit_code`], `0` green / `1` on
//! any surface failure) and/or parses the JSON for a per-case breakdown.
//!
//! Every DTO carries `#[serde(deny_unknown_fields)]` so a stale consumer that
//! adds a field fails loudly rather than silently. Failure [`CaseReport::detail`]
//! is a **controlled, redacted** message ([`redact`]) — it never carries a
//! secret, a JWT, a `DATABASE_URL`, or a raw credential echo, exactly as the FIX
//! `Text (58)` redaction rule demands
//! ([03 §8](../../docs/03-protocol-surfaces.md#8-error-mapping-across-surfaces)).

use serde::{Deserialize, Serialize};

/// The report schema version — bumped only on a breaking shape change so a
/// consumer can pin the contract it parses.
pub const SCHEMA_VERSION: &str = "conformance.v1";

/// The inclusive maximum length **in bytes** of a redacted failure
/// [`CaseReport::detail`]; a longer message is truncated (at a UTF-8 char
/// boundary) so no case can emit an unbounded dump and a multibyte detail can
/// never exceed this byte ceiling.
pub const MAX_DETAIL_LEN: usize = 512;

/// The placeholder a secret-shaped substring is masked to by [`redact`].
const REDACTED: &str = "<redacted>";

/// A protocol surface a conformance case exercises. Order-entry parity spans
/// `[rest, fix]`; observation parity spans `[rest, ws, fix]`; control parity
/// spans `[rest, ws]`.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Surface {
    /// The REST gateway (`src/gateway/rest`).
    Rest,
    /// The WebSocket gateway (`src/gateway/ws`).
    Ws,
    /// The FIX 4.4 gateway (`src/gateway/fix`).
    Fix,
}

impl Surface {
    /// Every surface, in the fixed report order (`rest`, `ws`, `fix`).
    pub const ALL: [Surface; 3] = [Surface::Rest, Surface::Ws, Surface::Fix];
}

/// The verdict of a case, a suite, or the whole run.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    /// Every assertion held.
    Pass,
    /// At least one assertion failed.
    Fail,
}

/// The outcome of running one case: `Ok(())` is a pass; `Err(detail)` is a
/// failure carrying a human-readable reason (redacted before it reaches the
/// report).
pub type CaseOutcome = Result<(), String>;

/// One conformance / parity case result.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct CaseReport {
    /// A stable machine id (e.g. `order_entry.partial_fill`) a CI can pin.
    pub id: String,
    /// A one-line human description of what the case asserts.
    pub description: String,
    /// The surfaces this case spans (its failure counts against each).
    pub surfaces: Vec<Surface>,
    /// The case verdict.
    pub verdict: Verdict,
    /// A redacted failure reason, present only on a [`Verdict::Fail`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// A named group of cases (one suite per parity/conformance concern).
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SuiteReport {
    /// The suite name (e.g. `order_entry_parity`).
    pub name: String,
    /// The suite's cases in run order.
    pub cases: Vec<CaseReport>,
    /// How many cases passed.
    pub passed: usize,
    /// How many cases failed.
    pub failed: usize,
}

/// The per-surface rollup: how many cases touched a surface and how they fared.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SurfaceSummary {
    /// The surface.
    pub surface: Surface,
    /// How many cases exercised this surface.
    pub cases: usize,
    /// How many of those passed.
    pub passed: usize,
    /// How many of those failed.
    pub failed: usize,
}

/// The run totals across every suite.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Totals {
    /// Total cases run.
    pub cases: usize,
    /// Total cases passed.
    pub passed: usize,
    /// Total cases failed.
    pub failed: usize,
}

/// The top-level machine-readable conformance report.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConformanceReport {
    /// The report schema version ([`SCHEMA_VERSION`]).
    pub schema_version: String,
    /// The per-suite results.
    pub suites: Vec<SuiteReport>,
    /// The per-surface rollup.
    pub surfaces: Vec<SurfaceSummary>,
    /// The run totals.
    pub totals: Totals,
    /// The overall verdict — [`Verdict::Fail`] if any case failed.
    pub verdict: Verdict,
}

impl ConformanceReport {
    /// Assembles the top-level report from the finished suites, computing the
    /// per-surface rollup, the totals, and the overall verdict.
    #[must_use]
    pub fn from_suites(suites: Vec<SuiteReport>) -> Self {
        let mut totals = Totals {
            cases: 0,
            passed: 0,
            failed: 0,
        };
        let mut summaries: Vec<SurfaceSummary> = Surface::ALL
            .into_iter()
            .map(|surface| SurfaceSummary {
                surface,
                cases: 0,
                passed: 0,
                failed: 0,
            })
            .collect();

        for suite in &suites {
            for case in &suite.cases {
                totals.cases += 1;
                let pass = case.verdict == Verdict::Pass;
                if pass {
                    totals.passed += 1;
                } else {
                    totals.failed += 1;
                }
                for surface in &case.surfaces {
                    if let Some(summary) = summaries.iter_mut().find(|s| s.surface == *surface) {
                        summary.cases += 1;
                        if pass {
                            summary.passed += 1;
                        } else {
                            summary.failed += 1;
                        }
                    }
                }
            }
        }

        let verdict = if totals.failed == 0 {
            Verdict::Pass
        } else {
            Verdict::Fail
        };

        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            suites,
            surfaces: summaries,
            totals,
            verdict,
        }
    }

    /// Whether every case passed.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.verdict == Verdict::Pass
    }

    /// The process exit code a consumer gates on: `0` when green, `1` on any
    /// surface failure.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        i32::from(!self.passed())
    }

    /// The surfaces that had at least one failing case.
    #[must_use]
    pub fn failed_surfaces(&self) -> Vec<Surface> {
        self.surfaces
            .iter()
            .filter(|s| s.failed > 0)
            .map(|s| s.surface)
            .collect()
    }
}

/// Accumulates the cases of one suite, tagging each with its redacted verdict.
pub struct SuiteRecorder {
    name: String,
    cases: Vec<CaseReport>,
}

impl SuiteRecorder {
    /// Opens a recorder for the named suite.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            cases: Vec::new(),
        }
    }

    /// Records one case's outcome. A failure `detail` is redacted before it is
    /// stored, so no case can leak a secret or an unbounded dump into the report.
    pub fn record(
        &mut self,
        id: impl Into<String>,
        description: impl Into<String>,
        surfaces: Vec<Surface>,
        outcome: CaseOutcome,
    ) {
        let (verdict, detail) = match outcome {
            Ok(()) => (Verdict::Pass, None),
            Err(reason) => (Verdict::Fail, Some(redact(&reason))),
        };
        self.cases.push(CaseReport {
            id: id.into(),
            description: description.into(),
            surfaces,
            verdict,
            detail,
        });
    }

    /// Closes the suite, computing its pass/fail counts.
    #[must_use]
    pub fn finish(self) -> SuiteReport {
        let passed = self
            .cases
            .iter()
            .filter(|c| c.verdict == Verdict::Pass)
            .count();
        let failed = self.cases.len() - passed;
        SuiteReport {
            name: self.name,
            cases: self.cases,
            passed,
            failed,
        }
    }
}

/// Redacts a failure detail before it reaches the report. Case detail strings are
/// authored to never format a token, password, or key into the message; this is
/// the enforced backstop that keeps that true even if a raw wire value ever slips
/// through:
///
/// 1. **control-strip** — every control byte (NUL / SOH from a raw FIX frame, …)
///    becomes a space, so a report line never carries wire control noise;
/// 2. **secret-scrub** — a `Bearer <token>` value, a value following a
///    `token` / `password` / `secret` / `authorization` keyword, or a standalone
///    JWT/base64url-shaped run (20+ chars of `[A-Za-z0-9_-./+=]` containing a
///    `.`) is replaced with [`REDACTED`], so a JWT / API key / password can never
///    reach the report;
/// 3. **byte-bound** — the result is truncated at a UTF-8 char boundary so it
///    never exceeds [`MAX_DETAIL_LEN`] **bytes** plus the truncation marker.
#[must_use]
pub fn redact(detail: &str) -> String {
    let cleaned: String = detail
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let scrubbed = scrub_secrets(&cleaned);
    let trimmed = scrubbed.trim();
    let (head, truncated) = truncate_at_char_boundary(trimmed, MAX_DETAIL_LEN);
    if truncated {
        format!("{head}…(truncated)")
    } else {
        head.to_string()
    }
}

/// The keywords whose following token (or inline `key=value` / `key:value`) is a
/// credential to mask.
const SECRET_KEYWORDS: &[&str] = &[
    "bearer",
    "token",
    "password",
    "passwd",
    "secret",
    "authorization",
    "apikey",
    "api_key",
];

/// The minimum length a keyword-following value must reach before it is masked.
const KEYWORD_VALUE_MIN_LEN: usize = 12;

/// The minimum length a standalone JWT/base64url-shaped run must reach.
const SECRET_RUN_MIN_LEN: usize = 20;

/// Whether every char is in the base64url / JWT / base64 alphabet (`[A-Za-z0-9]`
/// plus `_ - . / + =`) — the charset a token, JWT, or key is built from.
fn is_secret_charset(token: &str) -> bool {
    !token.is_empty()
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/' | '+' | '='))
}

/// A standalone JWT/base64url-shaped run: long, secret-charset, and dotted.
fn is_jwt_shaped(token: &str) -> bool {
    token.len() >= SECRET_RUN_MIN_LEN && token.contains('.') && is_secret_charset(token)
}

/// Masks credential-shaped substrings. Tokenised on whitespace (control bytes are
/// already spaces), so it is allocation-light and needs no regex dependency.
fn scrub_secrets(input: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut prev_is_keyword = false;
    for token in input.split_whitespace() {
        // Inline `key=value` / `key:value` (e.g. `password=…`, `Authorization:…`).
        if let Some((key, value)) = token.split_once(['=', ':'])
            && SECRET_KEYWORDS.contains(&key.trim().to_ascii_lowercase().as_str())
            && value.len() >= KEYWORD_VALUE_MIN_LEN
            && is_secret_charset(value)
        {
            out.push(format!("{key}={REDACTED}"));
            prev_is_keyword = false;
            continue;
        }

        let bare = token.trim_end_matches([':', '=', ',', '.']);
        let is_keyword = SECRET_KEYWORDS.contains(&bare.to_ascii_lowercase().as_str());

        let mask = is_jwt_shaped(token)
            || (prev_is_keyword
                && token.len() >= KEYWORD_VALUE_MIN_LEN
                && is_secret_charset(token));
        if mask {
            out.push(REDACTED.to_string());
        } else {
            out.push(token.to_string());
        }
        prev_is_keyword = is_keyword;
    }
    out.join(" ")
}

/// Truncates `s` to at most `max` bytes at a UTF-8 char boundary, reporting
/// whether it was cut.
fn truncate_at_char_boundary(s: &str, max: usize) -> (&str, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(id: &str, surfaces: Vec<Surface>, verdict: Verdict) -> CaseReport {
        CaseReport {
            id: id.to_string(),
            description: "d".to_string(),
            surfaces,
            verdict,
            detail: if verdict == Verdict::Fail {
                Some("boom".to_string())
            } else {
                None
            },
        }
    }

    #[test]
    fn test_all_green_report_passes_and_exits_zero() {
        let suite = SuiteReport {
            name: "s".to_string(),
            cases: vec![
                case("a", vec![Surface::Rest, Surface::Fix], Verdict::Pass),
                case("b", vec![Surface::Ws], Verdict::Pass),
            ],
            passed: 2,
            failed: 0,
        };
        let report = ConformanceReport::from_suites(vec![suite]);
        assert!(report.passed());
        assert_eq!(report.exit_code(), 0);
        assert_eq!(report.totals.cases, 2);
        assert_eq!(report.totals.failed, 0);
        assert!(report.failed_surfaces().is_empty());
    }

    #[test]
    fn test_any_case_failure_flips_verdict_and_exit_code() {
        // The injected-failure contract: one failing case exits non-zero and
        // names the affected surfaces in the rollup.
        let suite = SuiteReport {
            name: "s".to_string(),
            cases: vec![
                case("a", vec![Surface::Rest, Surface::Fix], Verdict::Pass),
                case("b", vec![Surface::Fix], Verdict::Fail),
            ],
            passed: 1,
            failed: 1,
        };
        let report = ConformanceReport::from_suites(vec![suite]);
        assert!(!report.passed());
        assert_eq!(report.exit_code(), 1);
        assert_eq!(report.failed_surfaces(), vec![Surface::Fix]);
        // The FIX surface saw two cases (one pass, one fail); REST saw one pass.
        let fix = report
            .surfaces
            .iter()
            .find(|s| s.surface == Surface::Fix)
            .expect("fix summary present");
        assert_eq!(fix.cases, 2);
        assert_eq!(fix.failed, 1);
    }

    #[test]
    fn test_recorder_redacts_and_round_trips() {
        let mut recorder = SuiteRecorder::new("order_entry_parity");
        recorder.record(
            "order_entry.place",
            "place normalizes equal",
            vec![Surface::Rest, Surface::Fix],
            Ok(()),
        );
        recorder.record(
            "order_entry.reject",
            "reject journals nothing",
            vec![Surface::Rest, Surface::Fix],
            Err("expected empty stream\u{0}\u{1}with control noise".to_string()),
        );
        let suite = recorder.finish();
        assert_eq!(suite.passed, 1);
        assert_eq!(suite.failed, 1);
        // The control noise from the (hypothetical) FIX frame is scrubbed.
        let detail = suite.cases[1]
            .detail
            .as_ref()
            .expect("failure detail present");
        assert!(!detail.contains('\u{0}'));
        assert!(!detail.contains('\u{1}'));

        let report = ConformanceReport::from_suites(vec![suite]);
        let json = serde_json::to_string(&report).expect("serialize");
        let parsed: ConformanceReport = serde_json::from_str(&json).expect("round-trip");
        assert_eq!(parsed.verdict, Verdict::Fail);
        assert_eq!(parsed.schema_version, SCHEMA_VERSION);
    }

    #[test]
    fn test_redact_truncates_unbounded_detail_by_bytes() {
        // A multibyte detail must never exceed MAX_DETAIL_LEN *bytes* plus the marker.
        let long = "é".repeat(MAX_DETAIL_LEN); // 2 bytes each → 2 * 512 bytes
        let redacted = redact(&long);
        assert!(
            redacted.len() <= MAX_DETAIL_LEN + "…(truncated)".len(),
            "the byte length must be bounded, got {}",
            redacted.len()
        );
        assert!(redacted.ends_with("(truncated)"));
    }

    #[test]
    fn test_redact_scrubs_a_bearer_jwt_shaped_detail() {
        // A JWT (dotted base64url) following `Bearer` — the exact secret shape a raw
        // Authorization header would carry — is masked, never echoed.
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0cmFkZXItMSJ9.dGhpc0lzTm90QVJlYWxTaWc";
        let detail = format!("auth failed for Bearer {jwt} on POST /orders");
        let redacted = redact(&detail);
        assert!(
            !redacted.contains(jwt),
            "the JWT must never appear in the redacted detail: {redacted}"
        );
        assert!(
            redacted.contains(REDACTED),
            "the JWT must be masked to the placeholder: {redacted}"
        );
        // The non-secret context survives so the detail is still diagnostic.
        assert!(redacted.contains("POST /orders"));
    }

    #[test]
    fn test_redact_scrubs_inline_password_and_leaves_paths() {
        let detail = "control rejected password=hunter2SuperSecretValue at /api/v1/controls/enable";
        let redacted = redact(detail);
        assert!(!redacted.contains("hunter2SuperSecretValue"));
        assert!(redacted.contains("password=<redacted>"));
        // A route path (secret-charset but not dotted, not keyword-following) survives.
        assert!(redacted.contains("/api/v1/controls/enable"));
    }
}
