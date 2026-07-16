//! Allocation-counting bench profile — [07 §4](../docs/07-performance-budgets.md#4-throughput-scaling-and-isolation-budgets):
//! "the steady-state turn (append → match → append → enqueue) targets zero
//! heap allocation on the common path," verified by "an allocation-counting
//! bench harness (e.g. a counting allocator in the bench profile)."
//!
//! ## Method
//!
//! Installs a `#[global_allocator]` [`CountingAllocator`] wrapping
//! `std::alloc::System` with `AtomicU64` alloc/dealloc/byte counters — stable
//! Rust, no nightly `#[feature]` needed. Because a global allocator can only
//! be set **once per binary**, this is deliberately its own bench target,
//! never sharing a process with `hp1_order_path` / `hp2_ws_fanout`.
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
//!
//! `harness = false`; run: `cargo bench --bench alloc_profile`.

#[path = "support/mod.rs"]
mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fauxchange::exchange::{
    ActorConfig, EventTimestamp, FixedClock, InMemoryExecutionsStore, InMemoryPositionsStore,
    InMemoryVenueJournal, JournalHeader, LineageId, MarkPriceBook, MatchingExecutor, NoopFanOut,
    StoreFanOut, TeeFanOut, UnderlyingActor, spawn_underlying_actor,
};

use support::workload::{UNDERLYING, build_workload};

/// A `GlobalAlloc` wrapper around `System` that counts allocations,
/// deallocations, and bytes moved. Bench-only instrumentation, confined to
/// this file — never part of the shipped crate (`fauxchange`'s `src/lib.rs`
/// keeps `#![forbid(unsafe_code)]` unconditionally; this is a separate,
/// independent bench binary, and `unsafe` here is the `GlobalAlloc` trait's
/// own requirement, not a workaround).
struct CountingAllocator {
    allocs: AtomicU64,
    deallocs: AtomicU64,
    bytes_alloc: AtomicU64,
    bytes_dealloc: AtomicU64,
}

impl CountingAllocator {
    const fn new() -> Self {
        Self {
            allocs: AtomicU64::new(0),
            deallocs: AtomicU64::new(0),
            bytes_alloc: AtomicU64::new(0),
            bytes_dealloc: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.allocs.load(Ordering::Relaxed),
            self.deallocs.load(Ordering::Relaxed),
            self.bytes_alloc.load(Ordering::Relaxed),
            self.bytes_dealloc.load(Ordering::Relaxed),
        )
    }
}

// SAFETY: every method below delegates to `System` (the platform default
// allocator) with the exact `Layout` (and, for `realloc`, the exact new size)
// it was given, adding only an atomic counter increment around the delegated
// call. `System` already upholds the `GlobalAlloc` safety contract, and
// forwarding its arguments unchanged preserves it.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_alloc
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.deallocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_dealloc
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_alloc
            .fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_alloc
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn report_window(
    label: &str,
    before: (u64, u64, u64, u64),
    after: (u64, u64, u64, u64),
    ops: usize,
) {
    let allocs = after.0.saturating_sub(before.0);
    let deallocs = after.1.saturating_sub(before.1);
    let bytes_alloc = after.2.saturating_sub(before.2);
    let bytes_dealloc = after.3.saturating_sub(before.3);
    #[allow(clippy::cast_precision_loss)]
    let allocs_per_op = allocs as f64 / ops.max(1) as f64;
    #[allow(clippy::cast_precision_loss)]
    let bytes_per_op = bytes_alloc as f64 / ops.max(1) as f64;

    println!("\n[alloc-profile] {label}: {ops} measured ops");
    println!("  allocs         : {allocs}");
    println!("  deallocs       : {deallocs}");
    println!("  bytes_alloc    : {bytes_alloc}");
    println!("  bytes_dealloc  : {bytes_dealloc}");
    println!("  allocs/op      : {allocs_per_op:.3}");
    println!("  bytes_alloc/op : {bytes_per_op:.1}");
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

        let before = ALLOC.snapshot();
        let n = workload.len();
        for command in workload {
            let _ = actor.handle(command);
        }
        let after = ALLOC.snapshot();
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

        let before = ALLOC.snapshot();
        let n = workload.len();
        for command in workload {
            let _ = handle.submit(command).await;
        }
        let after = ALLOC.snapshot();
        report_window(
            "ActorHandle::submit (async mailbox + oneshot reply)",
            before,
            after,
            n,
        );

        drop(handle);
    });
}
