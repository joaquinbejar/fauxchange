//! A supplementary, standard `criterion`-orchestrated micro-benchmark —
//! **not** a source of `BENCH.md` evidence. `BENCH.md`'s tail-latency figures
//! come exclusively from the `hdrhistogram`-native `hp1_order_path` /
//! `hp2_ws_fanout` / `alloc_profile` binaries; `mean`/`std-dev` (criterion's
//! own default report, printed to stdout by `cargo bench` when this target
//! runs) is explicitly **not** an accepted quantile report
//! ([07 §5](../docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention):
//! "Mean is a vanity metric on this workload and is not a target").
//!
//! This file exists so `bench-hdr`'s named convention — "criterion for
//! orchestration, hdrhistogram for the reported distribution" — has a real,
//! working example of the criterion half in the suite, alongside the
//! hdrhistogram-native benches, mirroring how the `orderbook-rs` sibling repo
//! keeps its mean-centric Criterion benches alongside its own `_hdr` suite
//! (`.claude/skills/bench-hdr/SKILL.md` §10 in that repo). It benchmarks
//! deterministic workload construction ([`support::workload::build_workload`])
//! — CPU-bound, synchronous, and independent of the actor/tokio machinery the
//! other benches measure, so it stays a clean, idiomatic `criterion` example
//! rather than a second, competing measurement of HP-1 itself.
//!
//! `harness = false` (required by `criterion_main!`, which supplies its own
//! `fn main`); run: `cargo bench --bench criterion_match_cost`.

#[path = "support/mod.rs"]
mod support;

use criterion::{Criterion, criterion_group, criterion_main};
use fauxchange::exchange::LineageId;
use support::workload::build_workload;

fn bench_build_workload(c: &mut Criterion) {
    support::print_run_conditions("criterion_match_cost");
    let lineage = LineageId::new("criterion-bench");
    c.bench_function("build_workload_1000", |b| {
        b.iter(|| build_workload(std::hint::black_box(1_000), 0xA5A5_A5A5_A5A5_A5A5, &lineage));
    });
}

criterion_group!(benches, bench_build_workload);
criterion_main!(benches);
