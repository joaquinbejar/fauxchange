//! Shared `bench-hdr` harness code for `fauxchange`'s benches
//! ([020](../../milestones/v0.1-backend-core/020-bench-hdr-harness-baseline.md),
//! [07 §5](../../docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention)).
//!
//! Every `benches/*.rs` binary pulls this in via
//! `#[path = "support/mod.rs"] mod support;` rather than as a crate dependency:
//! Cargo auto-discovers a `.rs` file placed directly under `benches/` as its
//! own bench target, but a file nested one level deeper (`benches/support/*`)
//! is invisible to that scan (the auto-discovery patterns are `benches/*.rs`
//! and `benches/*/main.rs` — neither matches `benches/support/mod.rs` or its
//! siblings), so this module never becomes a spurious empty bench target of
//! its own. `tests/bench_harness.rs` pulls in [`hdr`] the same way, so the
//! pure histogram/quantile plumbing runs under `cargo test`, not only
//! `cargo bench`.
//!
//! Never linked into the shipped `fauxchange` library — this is bench-only
//! tooling, confined to `benches/` (never `src/`).
//!
//! `#![allow(dead_code)]`: this module is included, whole, into six
//! independent binaries (`hp1_order_path`, `hp2_ws_fanout`, `hp3_fix_parse`,
//! `hp5_durable_append`, `alloc_profile`, `criterion_match_cost`, and
//! `tests/bench_harness.rs`'s narrower `#[path]` pull of just `hdr.rs` /
//! `fix_fixtures.rs`), and each one only calls the subset of this shared
//! toolkit it actually needs — the same reason `tests/common/` helper modules
//! conventionally carry this allow. `-D warnings` (the `clippy --all-targets`
//! CI gate) would otherwise fail on, say, `hp2_ws_fanout` never calling
//! [`timing::TimingExecutor`], which only `hp1_order_path` needs.
#![allow(dead_code)]

pub mod fix_fixtures;
pub mod hdr;
pub mod openloop;
pub mod timing;
pub mod workload;

/// Prints the run-conditions header every `bench-hdr` binary starts with, so a
/// pasted terminal capture is never separated from *how* it was produced
/// ([07 §5]). The rest of the run-conditions table (CPU model, governor,
/// toolchain, git commit, pinned upstream crate versions) is recorded by hand
/// in `BENCH.md` — the parts a running binary cannot reliably self-report on
/// every platform.
pub fn print_run_conditions(bench_name: &str) {
    println!("=== bench-hdr: {bench_name} ===");
    println!("  fauxchange version : {}", env!("CARGO_PKG_VERSION"));
    println!(
        "  build profile       : {}",
        if cfg!(debug_assertions) {
            "debug (WARNING: run `cargo bench`, which always builds --release; a debug run is not a valid measurement)"
        } else {
            "release"
        }
    );
    println!(
        "  target arch/os      : {}/{}",
        std::env::consts::ARCH,
        std::env::consts::OS
    );
}
