//! HP-2 — WS broadcast fan-out isolation
//! ([07 §2, §4](../docs/07-performance-budgets.md),
//! [020](../milestones/v0.1-backend-core/020-bench-hdr-harness-baseline.md)).
//!
//! A committed `VenueEvent` → serialised → enqueued to N subscriber broadcast
//! slots, for `N ∈ {1, 10, 100, 1_000}`, reusing the **real**
//! `TeeFanOut(StoreFanOut, WsFanOut)` / `OrderbookSubscriptionManager` from
//! #008/#014 (the same fan-out `AppState` wires). The DESIGN TARGET
//! ([07 §4](../docs/07-performance-budgets.md#4-throughput-scaling-and-isolation-budgets))
//! is that the order-path's own p99 stays **flat in N** — the actor's fan-out
//! step is an O(1) enqueue onto a bounded `tokio::broadcast`, not a synchronous
//! send to N sockets, so a slow/idle subscriber degrades its own stream, never
//! the venue's matching latency.
//!
//! Every N run holds its subscribers **without draining them** — an idle,
//! never-polled `tokio::broadcast::Receiver` never causes `Sender::send` to
//! block or slow down (the ring buffer just overwrites old slots and the
//! laggard re-snapshots later), which is exactly the "off the critical path"
//! claim this bench exists to check.
//!
//! `harness = false`; run: `cargo bench --bench hp2_ws_fanout`. Reduced
//! sample: `HP2_MEASURED_OPS=5000 cargo bench --bench hp2_ws_fanout`.
//!
//! **Self-check flatness assertion.** This bench prints the p99 delta of
//! every swept N vs the N=1 baseline unconditionally. Set
//! `HP2_FLATNESS_ASSERT=1` to turn that into a hard `panic!` (failing this
//! `cargo bench` run) when `|delta%|` exceeds `HP2_FLATNESS_TOLERANCE_PCT`
//! (default `40.0`, mirroring the tolerance the `bench-regression` CI job's
//! reduced-sample PR path uses for the same reason: at a small local sample
//! count the tail is noisier, so a tight bound would be spuriously red on an
//! unrelated run). Left unset (the default), this is report-only — so a
//! developer running a tiny local `HP2_MEASURED_OPS` is never spuriously
//! failed by sampling noise alone. This in-bench check is this bench's own
//! self-check, not the definitive regression gate: the authoritative,
//! cross-run gate is the `bench-regression` CI job (introduced at #53),
//! which compares against a documented ceiling table rather than a
//! same-process N=1 baseline.

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::Instant;

use fauxchange::exchange::{
    ActorConfig, EventTimestamp, FixedClock, InMemoryExecutionsStore, InMemoryPositionsStore,
    InMemoryVenueJournal, JournalHeader, LineageId, MarkPriceBook, MatchingExecutor, StoreFanOut,
    TeeFanOut, spawn_underlying_actor,
};
use fauxchange::subscription::{OrderbookSubscriptionManager, WsFanOut};
use hdrhistogram::Histogram;

use support::hdr::{new_histogram, record_duration, report};
use support::workload::{UNDERLYING, build_workload};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_flag(key: &str) -> bool {
    std::env::var(key).ok().as_deref() == Some("1")
}

fn main() {
    support::print_run_conditions("hp2_ws_fanout");

    let warmup_ops = env_usize("HP2_WARMUP_OPS", 2_000);
    let measured_ops = env_usize("HP2_MEASURED_OPS", 30_000);
    let seed = 0xA5A5_A5A5_A5A5_A5A5_u64;
    let ns_to_sweep: [usize; 4] = [1, 10, 100, 1_000];

    // See the module doc's "Self-check flatness assertion" section: unset
    // (default) is report-only so a reduced local sample is never
    // spuriously red; `HP2_FLATNESS_ASSERT=1` (CI) turns a breach into a
    // failing `panic!`. The definitive cross-run gate is the
    // `bench-regression` CI job (#53), not this same-process self-check.
    let flatness_assert = env_flag("HP2_FLATNESS_ASSERT");
    let flatness_tolerance_pct = env_f64("HP2_FLATNESS_TOLERANCE_PCT", 40.0);

    println!(
        "config: warmup_ops={warmup_ops} measured_ops={measured_ops} N={ns_to_sweep:?} \
         flatness_assert={flatness_assert} flatness_tolerance_pct={flatness_tolerance_pct}"
    );

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => panic!("failed to build the bench tokio runtime: {e}"),
    };

    runtime.block_on(async move {
        let mut baseline_p99: Option<u64> = None;
        for &n in &ns_to_sweep {
            let (hist, rejected) = run_one_n(n, warmup_ops, measured_ops, seed).await;
            println!(
                "\n[HP-2] N={n} subscribers, {measured_ops} ops after {warmup_ops} warmup, {rejected} rejected:"
            );
            let q = report(&format!("hp2_fanout_n{n}"), &hist);
            if baseline_p99.is_none() {
                baseline_p99 = Some(q.p99_ns);
            }
            if let Some(base) = baseline_p99 {
                let delta = i128::from(q.p99_ns) - i128::from(base);
                #[allow(clippy::cast_precision_loss)]
                let pct = if base == 0 { 0.0 } else { 100.0 * delta as f64 / base as f64 };
                println!("  p99 delta vs N=1 baseline: {delta:+} ns ({pct:+.1}%)");
                // Self-check only (see module doc) — the definitive cross-run
                // regression gate is the `bench-regression` CI job (#53).
                if flatness_assert {
                    assert!(
                        pct.abs() <= flatness_tolerance_pct,
                        "HP-2 flatness self-check failed: N={n} p99={p99_ns} ns vs N=1 \
                         baseline={base} ns, delta {pct:+.2}% exceeds the configured \
                         ±{flatness_tolerance_pct:.1}% tolerance \
                         (HP2_FLATNESS_TOLERANCE_PCT). This is this bench's own \
                         same-process self-check, gated by HP2_FLATNESS_ASSERT=1; the \
                         definitive cross-run gate is the bench-regression CI job (#53).",
                        p99_ns = q.p99_ns,
                    );
                }
            }
        }
    });
}

async fn run_one_n(
    n: usize,
    warmup_ops: usize,
    measured_ops: usize,
    seed: u64,
) -> (Histogram<u64>, usize) {
    let lineage = LineageId::new(format!("bench-hp2-n{n}"));
    let mut workload = build_workload(warmup_ops + measured_ops, seed, &lineage);

    let subscriptions = Arc::new(OrderbookSubscriptionManager::new());
    // N idle subscribers: held, never drained (see module docs above).
    let receivers: Vec<_> = (0..n).map(|_| subscriptions.subscribe()).collect();

    let executions = Arc::new(InMemoryExecutionsStore::new());
    let positions = Arc::new(InMemoryPositionsStore::new());
    let marks = Arc::new(MarkPriceBook::new());
    let fan_out = TeeFanOut::new(
        StoreFanOut::new(
            Arc::clone(&executions),
            Arc::clone(&positions),
            Arc::clone(&marks),
        ),
        WsFanOut::new(Arc::clone(&subscriptions)),
    );
    let config = ActorConfig::new(UNDERLYING, lineage.clone(), 4_096);
    let clock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));
    let journal = InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()));
    let executor = MatchingExecutor::new(UNDERLYING);
    let (handle, _join) = spawn_underlying_actor(config, journal, executor, fan_out, clock);

    for command in workload.drain(..warmup_ops) {
        let _ = handle.submit(command).await;
    }

    let mut hist = new_histogram();
    let mut rejected = 0usize;
    for command in workload {
        let t0 = Instant::now();
        let result = handle.submit(command).await;
        let elapsed = t0.elapsed();
        if result.is_err() {
            rejected += 1;
            continue;
        }
        record_duration(&mut hist, elapsed);
    }

    drop(receivers);
    drop(handle);
    (hist, rejected)
}
