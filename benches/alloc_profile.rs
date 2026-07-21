//! Allocation-counting bench profile — [07 §4](../docs/07-performance-budgets.md#4-throughput-scaling-and-isolation-budgets):
//! "the steady-state turn (append → match → append → enqueue) targets zero
//! heap allocation on the common path," verified by "an allocation-counting
//! bench harness (e.g. a counting allocator in the bench profile)."
//!
//! ## Method
//!
//! Installs [`stats_alloc::StatsAlloc<System>`] as the `#[global_allocator]`
//! — a `std::alloc::System` wrapper with atomic alloc/dealloc/realloc/byte
//! counters — stable Rust, no nightly `#[feature]` needed. `fauxchange`'s
//! `src/lib.rs` keeps `#![forbid(unsafe_code)]` unconditionally; this bench
//! needs no `unsafe` of its own because `stats_alloc`'s `unsafe impl
//! GlobalAlloc` is vendored inside that (dev-only, bench-scoped) crate — see
//! the audit note on the `stats_alloc` dependency in `Cargo.toml`. Because a
//! global allocator can only be set **once per binary**, this is deliberately
//! its own bench target, never sharing a process with `hp1_order_path` /
//! `hp2_ws_fanout`.
//!
//! Two sections, reported separately, because they measure two genuinely
//! different things:
//!
//! 1. **`UnderlyingActor::handle` directly** (no `tokio` runtime at all) — the
//!    exact "steady-state turn (append → match → append → enqueue)" docs/07
//!    §4 names, driven synchronously via the actor's own documented in-process
//!    entry point ([`fauxchange::exchange::UnderlyingActor::handle`]).
//! 2. **`ActorHandle::submit` round-trip** (real `tokio` mailbox +
//!    `oneshot` reply) — the production gateway-facing API. This section is
//!    expected to allocate more than section 1: creating an `mpsc` send slot
//!    and a fresh `oneshot::channel()` per call is a real, separate cost of
//!    the async submit API, not part of the "steady-state turn" claim itself.
//!    Reported so the two are never conflated.
//!
//! ## What this does and does not prove
//!
//! This counts **every** allocation the process makes while the measured loop
//! runs — a process-wide allocation-pressure profile of the measured window,
//! not a call-stack-scoped instrumentation of `handle`/`submit` alone (that
//! would need a per-call profiler this environment does not have). Read
//! `allocs_per_op` / `bytes_per_op` as "how much the measured loop allocates
//! per submitted command at steady state" — a strong, honest proxy for the
//! DESIGN TARGET, not a formal proof that zero allocation happens inside any
//! one function. `0` allocs/op across a long, warmed-up measured window (once
//! the book/maps have grown past their initial capacity) is the strongest
//! evidence available on stable Rust without a call-stack profiler; a non-zero
//! count after warmup is the regression signal this bench exists to catch.
//! `allocs_per_op` counts `allocations + reallocations` (a realloc is a
//! distinct allocator event, tracked separately by `stats_alloc::Stats`, but
//! folded into the same "did the allocator do work" count this bench has
//! always reported); `bytes_per_op` is `bytes_allocated` alone, which
//! `stats_alloc` computes as the true net-growth bytes of every alloc and
//! realloc (a realloc that shrinks an allocation adds to `bytes_deallocated`
//! instead — a more accurate accounting than counting a realloc's full new
//! size as "allocated" regardless of direction).
//!
//! `harness = false`; run: `cargo bench --bench alloc_profile`.

#[path = "support/mod.rs"]
mod support;

use std::alloc::System;

use std::sync::Arc;

use stats_alloc::{Stats, StatsAlloc};

use fauxchange::exchange::{
    ActorConfig, EventTimestamp, FixedClock, InMemoryExecutionsStore, InMemoryPositionsStore,
    InMemoryVenueJournal, JournalHeader, LineageId, MarkPriceBook, MatchingExecutor, NoopFanOut,
    StoreFanOut, TeeFanOut, UnderlyingActor, spawn_underlying_actor,
};

use support::workload::{UNDERLYING, build_workload};

/// The instrumented global allocator for this bench binary. `stats_alloc`'s
/// own `unsafe impl GlobalAlloc` is vendored inside that crate (a dev-only,
/// bench-scoped dependency — see the audit note on `stats_alloc` in
/// `Cargo.toml`); this file, like every other file in this crate, contains
/// zero `unsafe`. `StatsAlloc::system()` is a stable, non-nightly-gated
/// `const fn`.
#[global_allocator]
static ALLOC: StatsAlloc<System> = StatsAlloc::system();

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn report_window(label: &str, before: Stats, after: Stats, ops: usize) {
    let delta = after - before;
    let alloc_events = (delta.allocations + delta.reallocations) as u64;
    #[allow(clippy::cast_precision_loss)]
    let allocs_per_op = alloc_events as f64 / ops.max(1) as f64;
    #[allow(clippy::cast_precision_loss)]
    let bytes_per_op = delta.bytes_allocated as f64 / ops.max(1) as f64;

    println!("\n[alloc-profile] {label}: {ops} measured ops");
    println!("  allocations           : {}", delta.allocations);
    println!("  reallocations         : {}", delta.reallocations);
    println!("  deallocations         : {}", delta.deallocations);
    println!("  bytes_allocated       : {}", delta.bytes_allocated);
    println!("  bytes_deallocated     : {}", delta.bytes_deallocated);
    println!("  bytes_reallocated_net : {}", delta.bytes_reallocated);
    println!("  allocs/op             : {allocs_per_op:.3}");
    println!("  bytes_alloc/op        : {bytes_per_op:.1}");
}

fn main() {
    support::print_run_conditions("alloc_profile");

    let warmup_ops = env_usize("ALLOC_WARMUP_OPS", 5_000);
    let measured_ops = env_usize("ALLOC_MEASURED_OPS", 50_000);
    let seed = 0xA5A5_A5A5_A5A5_A5A5_u64;

    println!("config: warmup_ops={warmup_ops} measured_ops={measured_ops}");

    // ---- Section 1: `UnderlyingActor::handle` directly, no tokio at all -----
    {
        let lineage = LineageId::new("bench-alloc-direct");
        let mut workload = build_workload(warmup_ops + measured_ops, seed, &lineage);

        let executions = Arc::new(InMemoryExecutionsStore::new());
        let positions = Arc::new(InMemoryPositionsStore::new());
        let marks = Arc::new(MarkPriceBook::new());
        let fan_out = TeeFanOut::new(
            StoreFanOut::new(
                Arc::clone(&executions),
                Arc::clone(&positions),
                Arc::clone(&marks),
            ),
            NoopFanOut,
        );
        let config = ActorConfig::new(UNDERLYING, lineage.clone(), 4_096);
        let clock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));
        let journal = InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()));
        let executor = MatchingExecutor::new(UNDERLYING);
        let mut actor = UnderlyingActor::new(config, journal, executor, fan_out, clock);

        for command in workload.drain(..warmup_ops) {
            let _ = actor.handle(command);
        }

        let before = ALLOC.stats();
        let n = workload.len();
        for command in workload {
            let _ = actor.handle(command);
        }
        let after = ALLOC.stats();
        report_window(
            "UnderlyingActor::handle (direct, no tokio)",
            before,
            after,
            n,
        );
    }

    // ---- Section 2: `ActorHandle::submit` round-trip (real tokio mailbox) ---
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => panic!("failed to build the bench tokio runtime: {e}"),
    };
    runtime.block_on(async move {
        let lineage = LineageId::new("bench-alloc-submit");
        let mut workload = build_workload(warmup_ops + measured_ops, seed, &lineage);

        let executions = Arc::new(InMemoryExecutionsStore::new());
        let positions = Arc::new(InMemoryPositionsStore::new());
        let marks = Arc::new(MarkPriceBook::new());
        let fan_out = TeeFanOut::new(
            StoreFanOut::new(
                Arc::clone(&executions),
                Arc::clone(&positions),
                Arc::clone(&marks),
            ),
            NoopFanOut,
        );
        let config = ActorConfig::new(UNDERLYING, lineage.clone(), 4_096);
        let clock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));
        let journal = InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()));
        let executor = MatchingExecutor::new(UNDERLYING);
        let (handle, _join) = spawn_underlying_actor(config, journal, executor, fan_out, clock);

        for command in workload.drain(..warmup_ops) {
            let _ = handle.submit(command).await;
        }

        let before = ALLOC.stats();
        let n = workload.len();
        for command in workload {
            let _ = handle.submit(command).await;
        }
        let after = ALLOC.stats();
        report_window(
            "ActorHandle::submit (async mailbox + oneshot reply)",
            before,
            after,
            n,
        );

        drop(handle);
    });
}
