//! The v0.5 requote-isolation assertion (#50 — the "acceptance criterion that
//! matters most" for the milestone,
//! [050](../milestones/v0.5-microstructure/050-requote-budget-isolation.md)):
//! a market-maker requote runs on the sim/MM task and enqueues onto the
//! bounded actor mailbox, **off the client's order path**
//! ([07 §3-4](../docs/07-performance-budgets.md#3-latency-budgets-design-targets)).
//! This proves a heavy, continuous, concurrent requote workload does not blow
//! up a client `AddOrder`'s HP-1-style p99 latency on the SAME underlying's
//! actor beyond a documented, bounded factor.
//!
//! Reuses the `bench-hdr` harness's histogram/quantile plumbing
//! (`benches/support/hdr.rs`), workload generator (`benches/support/workload.rs`),
//! and the HP-4 market-maker fixture (`benches/support/mm_workload.rs`) via
//! `#[path]` — the same pattern `tests/bench_harness.rs` already uses, so this
//! is the SAME code every `bench-hdr` bench runs, not a parallel
//! reimplementation that could silently drift from it.
//!
//! ## Method
//!
//! Two fresh [`AppState`]s (never sharing journal depth across the two
//! conditions, so the comparison is apples-to-apples — the same "fresh
//! journal per measurement" convention `benches/hp1_order_path.rs` uses for
//! its own open-loop section), each hosting one underlying (`BTC`):
//!
//! - **Quiet**: the client-only closed-loop HP-1-shaped workload
//!   ([`workload::build_workload`]), submitted directly via
//!   [`AppState::submit`] — no market-maker activity at all.
//! - **Concurrent**: the IDENTICAL client workload, run WHILE a background
//!   task continuously drives a REAL, persona-driven [`MarketMakerEngine`]
//!   (#47, [`mm_workload::build_engine`]) through repeated `update_price`
//!   calls against a small 10-contract option chain — each tick enqueues up
//!   to 40 commands through the REAL `ActorCommandSink` onto the SAME
//!   underlying's actor mailbox the client is submitting to. This is the
//!   realistic, harder case (a maker quoting the same underlying its clients
//!   trade), not an easier cross-underlying setup that would sidestep the
//!   architecture's one-mailbox-per-underlying sharing entirely.
//!
//! ## Why a documented tolerance factor, not bit-equality
//!
//! The venue's single-writer actor design means a client `AddOrder` and a
//! concurrent MM `CancelOrder`/`AddOrder` pair genuinely share ONE FIFO
//! mailbox when they target the same underlying (`src/exchange/actor.rs`) —
//! commands from both sources interleave in submission order, so some added
//! queueing from the MM traffic sharing the queue is an expected, structural
//! consequence of the architecture, not a bug to eliminate. What this test
//! guards against is UNBOUNDED inflation (a "slow requote" that starves or
//! stalls the client), not any added queueing whatsoever. The tolerance factor
//! below is chosen from real, disclosed measurements on this host (see the
//! `[isolation]` println! this test emits — run with `--nocapture` to see
//! them), not picked in advance and hoped to pass; it is wide enough to
//! absorb this shared, un-pinned developer laptop's own run-to-run scheduler
//! noise (`BENCH.md` §3.1 discloses a ~13% p99 swing on HP-1 itself with ZERO
//! code change) while still catching a genuine multi-x regression.

// `mm_workload.rs` / `workload.rs` are shared bench fixtures (`#[path]`-included
// whole, like `tests/bench_harness.rs` already does for `hdr.rs` /
// `fix_fixtures.rs`); this test only exercises a subset of each — the same
// `#[allow(dead_code)]` precedent `benches/support/mod.rs` documents for
// `tests/common/`-style helper modules.
#![allow(dead_code)]

#[path = "../benches/support/hdr.rs"]
mod hdr;
#[path = "../benches/support/mm_workload.rs"]
mod mm_workload;
#[path = "../benches/support/workload.rs"]
mod workload;

use std::sync::Arc;
use std::time::{Duration, Instant};

use fauxchange::exchange::LineageId;
use fauxchange::state::{AppState, AppStateConfig};

const WARMUP_OPS: usize = 500;
const MEASURED_OPS: usize = 3_000;
const SEED: u64 = 0xA5A5_A5A5_A5A5_A5A5;
/// Larger than the venue default (`DEFAULT_MAILBOX_CAPACITY = 1_024`,
/// `src/state.rs`) so this test measures genuine queueing/scheduling
/// contention, not an artifact of an unrealistically small mailbox — matches
/// `benches/hp1_order_path.rs`'s own `4_096` bench convention.
const MAILBOX_CAPACITY: usize = 4_096;
/// The simulated price-tick cadence driving the concurrent MM requote loop —
/// a realistic fast-moving-underlying cadence (50 ticks/s), not an
/// artificially extreme flood.
const REQUOTE_TICK_INTERVAL: Duration = Duration::from_millis(20);

/// Runs the client-only closed-loop HP-1-shaped workload against a fresh
/// `AppState`, optionally with a concurrent, continuous market-maker requote
/// task sharing the SAME underlying's actor mailbox, returning the measured
/// quantiles. Panics (loudly, per `benches/hp1_order_path.rs`'s own
/// closed-loop convention) if a client submission is ever rejected — a
/// rejection means the mailbox saturated, a visible signal either way.
async fn run_client_workload(with_requote: bool) -> hdr::Quantiles {
    let state = AppState::new(
        AppStateConfig::new([mm_workload::MM_UNDERLYING]).with_mailbox_capacity(MAILBOX_CAPACITY),
    )
    .expect("AppState with dev auth");

    let requote_task = if with_requote {
        let engine = Arc::clone(state.market_maker());
        engine.set_venue_now_ms(mm_workload::MM_VENUE_NOW_MS);
        let persona = mm_workload::bench_persona();
        for symbol in mm_workload::chain_symbols() {
            engine.register_instrument_with_persona(&symbol, None, "isolation", persona);
        }
        Some(tokio::spawn(async move {
            let mut tick: u64 = 0;
            loop {
                // A small deterministic jitter around a fixed spot so
                // successive ticks are not byte-identical (mirrors
                // `benches/support/workload.rs::jitter_stream`'s rationale) —
                // this loop does not need reproducibility itself (it is
                // aborted, never asserted against), so a cheap modulo is
                // enough.
                let jitter = i64::try_from(tick % 41).unwrap_or(0) - 20;
                let price = u64::try_from(5_000_000_i64 + jitter).unwrap_or(5_000_000);
                engine.update_price(mm_workload::MM_UNDERLYING, price);
                tick = tick.wrapping_add(1);
                tokio::time::sleep(REQUOTE_TICK_INTERVAL).await;
            }
        }))
    } else {
        None
    };

    let lineage = LineageId::new("isolation-client");
    let mut stream = workload::build_workload(WARMUP_OPS + MEASURED_OPS, SEED, &lineage);

    for command in stream.drain(..WARMUP_OPS) {
        let result = state.submit(command).await;
        assert!(
            result.is_ok(),
            "warmup client submit must not be rejected: {result:?}"
        );
    }

    let mut hist = hdr::new_histogram();
    for command in stream {
        let t0 = Instant::now();
        let result = state.submit(command).await;
        let elapsed = t0.elapsed();
        assert!(
            result.is_ok(),
            "measured client submit must not be rejected at mailbox_capacity={MAILBOX_CAPACITY} \
             (with_requote={with_requote}): {result:?} — a rejection means the mailbox \
             saturated under this configuration, which would itself be an isolation failure"
        );
        hdr::record_duration(&mut hist, elapsed);
    }

    if let Some(task) = requote_task {
        task.abort();
    }

    let label = if with_requote {
        "requote_isolation_client_concurrent"
    } else {
        "requote_isolation_client_quiet"
    };
    hdr::report(label, &hist)
}

/// The v0.5 acceptance criterion: a heavy, continuous, concurrent market-maker
/// requote sharing the client's own underlying mailbox does not inflate the
/// client's HP-1-style p99 beyond a documented, bounded factor.
///
/// `flavor = "multi_thread", worker_threads = 4"`: the concurrent condition
/// runs a real actor + a real `ActorCommandSink` forwarder + the requote task
/// + the client driver all at once — a single-threaded runtime would
/// serialize all of that onto one OS thread, which is not what "concurrent"
/// is supposed to model (mirrors `benches/mm_requote_hdr.rs`'s own
/// 4-worker choice and its documented reasoning).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_requote_does_not_inflate_client_hp1_p99_beyond_tolerance() {
    let quiet = run_client_workload(false).await;
    let concurrent = run_client_workload(true).await;

    println!(
        "[isolation] quiet:      p50={} ns p99={} ns p99.9={} ns",
        quiet.p50_ns, quiet.p99_ns, quiet.p999_ns
    );
    println!(
        "[isolation] concurrent: p50={} ns p99={} ns p99.9={} ns",
        concurrent.p50_ns, concurrent.p99_ns, concurrent.p999_ns
    );

    // Bound = `max(quiet.p99, 200µs) × 6`. Be honest about what this catches:
    // the observed quiet p99 (~50µs) sits BELOW the 200µs floor, so the floor
    // dominates and the effective bound is ~1.2ms (≈24× the observed ~50µs
    // concurrent p99), NOT 6×. That is deliberately loose — this assertion
    // backstops against UNBOUNDED inflation (a stalled/starved client dragged
    // toward the millisecond scale), not against ordinary FIFO-mailbox-sharing
    // queueing (an expected structural consequence of the single-writer actor).
    // The real isolation evidence is the measured ~1.0× ratio across runs plus
    // the 1ms-cadence sensitivity diagnostic (BENCH.md §12.3), not this
    // threshold; the floor only stops a near-zero quiet p99 making the ratio
    // spuriously tight, and the width keeps this noisy, un-pinned laptop
    // (BENCH.md §3.1: ~13% p99 swing on HP-1 with ZERO code change) from flaking.
    const TOLERANCE_FACTOR: u64 = 6;
    const FLOOR_NS: u64 = 200_000;
    let bound = quiet.p99_ns.max(FLOOR_NS).saturating_mul(TOLERANCE_FACTOR);
    assert!(
        concurrent.p99_ns <= bound,
        "a continuous, concurrent market-maker requote inflated client HP-1 p99 from \
         {} ns (quiet) to {} ns (concurrent) — beyond the documented {TOLERANCE_FACTOR}x \
         tolerance (bound={bound} ns); the requote is not staying off the client's order path",
        quiet.p99_ns,
        concurrent.p99_ns,
    );
}
