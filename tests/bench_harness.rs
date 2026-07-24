//! Unit tests for the `bench-hdr` harness's histogram/quantile plumbing
//! ([020](../milestones/v0.1-backend-core/020-bench-hdr-harness-baseline.md)
//! "Tests required": "a known distribution reports the expected quantiles" —
//! proving the harness itself is right, not just its callers.
//!
//! Pulls in `benches/support/hdr.rs` via `#[path]` (the same file every
//! `benches/*.rs` binary uses) rather than duplicating it, so this is the
//! SAME code under `cargo test --all-features`, not a parallel
//! reimplementation that could silently drift from what the benches actually
//! run.

#[path = "../benches/support/fix_fixtures.rs"]
mod fix_fixtures;
#[path = "../benches/support/hdr.rs"]
mod hdr;

use std::time::Duration;

use fauxchange::gateway::fix::{DecodedMessage, FixBody, decode};
use hdr::{Quantiles, new_histogram, record_duration};

/// A uniform `1..=1_000_000` (ns) distribution: `p50` should land near
/// `500_000`, `p99` near `990_000`, `p99.9` near `999_000`, `p99.99` near
/// `999_900` — `hdrhistogram`'s 3-significant-figure bucketing means these are
/// approximate, not exact, so the assertions below use a generous but
/// meaningful tolerance band, not an exact-equality check that would be
/// fragile to the library's internal bucket boundaries.
#[test]
fn test_hdr_known_uniform_distribution_reports_expected_quantiles() {
    let mut hist = new_histogram();
    for ns in 1..=1_000_000_u64 {
        record_duration(&mut hist, Duration::from_nanos(ns));
    }

    let q = Quantiles::from_histogram(&hist);
    assert_eq!(
        q.samples, 1_000_000,
        "every recorded sample must be counted"
    );
    assert!(
        (495_000..=505_000).contains(&q.p50_ns),
        "p50 {} outside the expected ~500_000 band",
        q.p50_ns
    );
    assert!(
        (985_000..=995_000).contains(&q.p99_ns),
        "p99 {} outside the expected ~990_000 band",
        q.p99_ns
    );
    assert!(
        (994_500..=999_999).contains(&q.p999_ns),
        "p99.9 {} outside the expected ~999_000 band",
        q.p999_ns
    );
    assert!(
        (999_400..=1_000_000).contains(&q.p9999_ns),
        "p99.99 {} outside the expected ~999_900 band",
        q.p9999_ns
    );
    assert_eq!(q.min_ns, 1);
    // `hdrhistogram`'s 3-sig-fig bucketing is lossy at the top of a 1..=1e6
    // range (empirically ~0.04% here, well inside the documented ~0.1%
    // envelope for 3 significant figures) — `max` is not returned bit-exact,
    // only within the configured resolution, so this is a range, not an
    // equality.
    assert!(
        (999_000..=1_001_000).contains(&q.max_ns),
        "max {} outside the expected ~1_000_000 band",
        q.max_ns
    );
}

/// A degenerate single-value distribution: every quantile collapses onto that
/// one value.
#[test]
fn test_hdr_known_constant_distribution_reports_that_constant_at_every_quantile() {
    let mut hist = new_histogram();
    for _ in 0..10_000 {
        record_duration(&mut hist, Duration::from_nanos(42));
    }

    let q = Quantiles::from_histogram(&hist);
    assert_eq!(q.samples, 10_000);
    assert_eq!(q.min_ns, 42);
    assert_eq!(q.max_ns, 42);
    // 3-sig-fig bucketing at this small magnitude is exact.
    assert_eq!(q.p50_ns, 42);
    assert_eq!(q.p99_ns, 42);
    assert_eq!(q.p999_ns, 42);
    assert_eq!(q.p9999_ns, 42);
}

/// An empty histogram reports all-zero quantiles rather than panicking.
#[test]
fn test_hdr_empty_histogram_reports_zero_quantiles_without_panicking() {
    let hist = new_histogram();
    let q = Quantiles::from_histogram(&hist);
    assert_eq!(q.samples, 0);
    assert_eq!(q.min_ns, 0);
    assert_eq!(q.max_ns, 0);
}

/// A known bimodal distribution (many fast samples, a meaningful slow-outlier
/// tail) is the shape every hot-path bench actually produces; the tail
/// quantiles must track the slow cluster while the median tracks the fast
/// one. The split is deliberately **90/10**, not 99/1: at an exact 99/1 split
/// the 99th-percentile sample index lands exactly on the boundary between the
/// two clusters, so whether `value_at_quantile(0.99)` reports the fast or the
/// slow cluster is an implementation-defined boundary call, not a meaningful
/// assertion — 90/10 puts p99 solidly inside the slow cluster instead.
#[test]
fn test_hdr_known_bimodal_distribution_separates_median_from_tail() {
    let mut hist = new_histogram();
    // 90_000 fast samples at ~100 ns.
    for _ in 0..90_000 {
        record_duration(&mut hist, Duration::from_nanos(100));
    }
    // 10_000 slow outliers at ~1_000_000 ns (1 ms) — the top 10%, so p99 (the
    // 99_000th of 100_000 ordered samples) sits solidly inside this cluster.
    for _ in 0..10_000 {
        record_duration(&mut hist, Duration::from_nanos(1_000_000));
    }

    let q = Quantiles::from_histogram(&hist);
    assert_eq!(q.samples, 100_000);
    assert!(
        (95..=105).contains(&q.p50_ns),
        "p50 {} should track the fast cluster",
        q.p50_ns
    );
    assert!(
        (990_000..=1_001_000).contains(&q.p99_ns),
        "p99 {} should track the slow cluster",
        q.p99_ns
    );
    assert!(
        (999_000..=1_001_000).contains(&q.max_ns),
        "max {} outside the expected ~1_000_000 band",
        q.max_ns
    );
}

/// `report(...)` returns the same [`Quantiles`] it prints — asserting the
/// return value is not accidentally decoupled from what gets printed to
/// stdout (a `BENCH.md` transcription bug source if the two ever diverged).
#[test]
fn test_hdr_report_return_value_matches_from_histogram() {
    let mut hist = new_histogram();
    for ns in [10_u64, 20, 30, 40, 50] {
        record_duration(&mut hist, Duration::from_nanos(ns));
    }
    let expected = Quantiles::from_histogram(&hist);
    let reported = hdr::report("test_scenario", &hist);
    assert_eq!(reported, expected);
}

/// HP-3 (#043) fixture smoke test: the bench must never silently measure a
/// reject path. `benches/support/fix_fixtures.rs::new_order_single_frame`
/// already panics internally if this fails; this test additionally proves the
/// decoded value round-trips to the identical fixture (not just "decodes to
/// *something*").
#[test]
fn test_hp3_new_order_single_fixture_decodes_to_itself() {
    let fixture = fix_fixtures::new_order_single_fixture();
    let frame = fix_fixtures::new_order_single_frame();
    match decode(&frame) {
        Ok(DecodedMessage::NewOrderSingle(back)) => assert_eq!(back, fixture),
        other => panic!("HP-3 D fixture must decode to NewOrderSingle, got {other:?}"),
    }
}

/// The `ExecutionReport (8)` encode fixture round-trips through the real
/// decode path too, so the encode bench's fixed input is provably not a
/// degenerate/rejectable message either.
#[test]
fn test_hp3_execution_report_fixture_round_trips() {
    let report = fix_fixtures::execution_report_fixture();
    let bytes = FixBody::encode(&report).expect("test encode");
    match decode(&bytes) {
        Ok(DecodedMessage::ExecutionReport(back)) => assert_eq!(back, report),
        other => panic!("HP-3 8 fixture must round-trip through decode, got {other:?}"),
    }
}

/// HP-3 (#043) golden-equality: the reconstructed `NewOrderSingle (D)` fixture
/// must encode byte-for-byte to the committed #036 golden that the bench's
/// decode span consumes directly (`tests/golden/fix/new_order_single_D.txt`), so
/// the fixture and the golden can never silently drift apart — the gap #115
/// closes. `new_order_single_frame()` returns the golden bytes (and asserts this
/// same equality off the bench's timed path); comparing here proves it under
/// `cargo test`, independently of a bench run.
#[test]
fn test_hp3_new_order_single_fixture_matches_committed_golden() {
    let reconstructed =
        FixBody::encode(&fix_fixtures::new_order_single_fixture()).expect("test encode");
    assert_eq!(reconstructed, fix_fixtures::new_order_single_frame());
}

/// HP-3 (#043) golden-equality for the encode span: the reconstructed
/// `ExecutionReport (8)` fixture must encode byte-for-byte to the committed #036
/// golden (`tests/golden/fix/execution_report_8.txt`) that the encode bench pins
/// its output against.
#[test]
fn test_hp3_execution_report_fixture_matches_committed_golden() {
    let reconstructed =
        FixBody::encode(&fix_fixtures::execution_report_fixture()).expect("test encode");
    assert_eq!(reconstructed, fix_fixtures::execution_report_golden_frame());
}
