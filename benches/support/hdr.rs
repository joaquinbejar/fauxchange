//! The `hdrhistogram`-backed distribution report — **never** criterion's
//! default mean/std ([07 §5](../../../docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention)).
//!
//! This is the piece the milestone's unit test targets directly
//! (`tests/bench_harness.rs`): feed a known distribution, assert the reported
//! quantiles are correct, proving the harness itself — not just its callers —
//! is right.

use std::time::Duration;

use hdrhistogram::Histogram;

/// Builds a histogram sized for `1 ns .. 10 s` at 3 significant figures — three
/// sig-figs is enough resolution to tell `p99` from `p99.9` an order of
/// magnitude apart while staying memory-cheap (mirrors the `orderbook-rs`
/// sibling `bench-hdr` skill's convention).
///
/// # Panics
///
/// Panics if `hdrhistogram` rejects these (fixed, always-valid) bounds — this
/// would indicate a broken build, not a runtime condition callers should
/// handle.
#[must_use]
pub fn new_histogram() -> Histogram<u64> {
    match Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3) {
        Ok(h) => h,
        Err(e) => panic!("hdrhistogram bounds rejected (fixed, always-valid bounds): {e}"),
    }
}

/// Records one already-measured [`Duration`] into `hist`, in nanoseconds,
/// clamped to at least `1` (`hdrhistogram` rejects a recorded `0`).
///
/// # Panics
///
/// Panics if the record is rejected as out-of-range for `hist`'s configured
/// bounds — [`new_histogram`]'s `10 s` ceiling is generous for every hot path
/// this harness measures, so a rejection here means a bench workload is
/// pathologically slow, worth failing loudly on rather than silently dropping
/// a sample.
pub fn record_duration(hist: &mut Histogram<u64>, elapsed: Duration) {
    let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    if let Err(e) = hist.record(ns.max(1)) {
        panic!("hdrhistogram record rejected {ns} ns (out of configured bounds?): {e}");
    }
}

/// The four quantiles `bench-hdr` reports — **p50 / p99 / p99.9 / p99.99** —
/// plus sample count / min / max for context. Mean is deliberately absent:
/// [07 §3](../../../docs/07-performance-budgets.md#3-latency-budgets-design-targets)
/// calls it "a vanity metric on this workload," never a target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quantiles {
    /// The number of recorded samples this report summarises.
    pub samples: u64,
    /// The 50th percentile, in nanoseconds.
    pub p50_ns: u64,
    /// The 99th percentile, in nanoseconds.
    pub p99_ns: u64,
    /// The 99.9th percentile, in nanoseconds.
    pub p999_ns: u64,
    /// The 99.99th percentile, in nanoseconds.
    pub p9999_ns: u64,
    /// The smallest recorded sample, in nanoseconds (`0` when empty).
    pub min_ns: u64,
    /// The largest recorded sample, in nanoseconds (`0` when empty).
    pub max_ns: u64,
}

impl Quantiles {
    /// Reads the four quantiles (plus sample count / min / max) off `hist`.
    #[must_use]
    pub fn from_histogram(hist: &Histogram<u64>) -> Self {
        let empty = hist.is_empty();
        Self {
            samples: hist.len(),
            p50_ns: hist.value_at_quantile(0.50),
            p99_ns: hist.value_at_quantile(0.99),
            p999_ns: hist.value_at_quantile(0.999),
            p9999_ns: hist.value_at_quantile(0.9999),
            min_ns: if empty { 0 } else { hist.min() },
            max_ns: if empty { 0 } else { hist.max() },
        }
    }
}

impl std::fmt::Display for Quantiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "  samples : {}", self.samples)?;
        writeln!(f, "  p50     : {} ns", self.p50_ns)?;
        writeln!(f, "  p99     : {} ns", self.p99_ns)?;
        writeln!(f, "  p99.9   : {} ns", self.p999_ns)?;
        writeln!(f, "  p99.99  : {} ns", self.p9999_ns)?;
        writeln!(f, "  min     : {} ns", self.min_ns)?;
        write!(f, "  max     : {} ns", self.max_ns)
    }
}

/// Prints a named quantile report to stdout in the `bench-hdr` convention and
/// returns the parsed [`Quantiles`] for further use (e.g. the HP-2 N-sweep's
/// flat-in-N comparison).
pub fn report(name: &str, hist: &Histogram<u64>) -> Quantiles {
    let q = Quantiles::from_histogram(hist);
    println!("--- {name} ---");
    println!("{q}");
    q
}

// The harness's own histogram/quantile plumbing is unit-tested from
// `tests/bench_harness.rs`, which pulls this file in via `#[path]` — the SAME
// code `cargo test --all-features` (the CI `test` job) actually runs, rather
// than a second, same-file copy that could silently drift from it.
