//! HP-1 — the in-memory sequenced order path
//! ([07 §2, §3](../docs/07-performance-budgets.md),
//! [ADR-0006](../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
//! [020](../milestones/v0.1-backend-core/020-bench-hdr-harness-baseline.md)).
//!
//! Span measured, in-process, single underlying (`BTC`), in-memory journal:
//! `submit` received → write-ahead `VenueCommand` append → upstream match
//! (captured separately) → `VenueEvent` append → fan-out enqueued (a real,
//! single-subscriber `TeeFanOut(StoreFanOut, WsFanOut)`, matching the wiring
//! `AppState` builds — [`fauxchange::state::AppState`]).
//!
//! Five reports, in this order:
//!
//! 1. `hp1_full_turn_closed_loop` — the flagship number: `ActorHandle::submit`
//!    round-trip, closed-loop (one in flight at a time).
//! 2. `hp1_match_only` — the upstream `MatchingExecutor::execute` cost alone,
//!    paired per turn (same turns as (1), timed from the inside via
//!    [`support::timing::TimingExecutor`]) — matching-engine throughput is
//!    out of budget ([07 §7]); this isolates it as its own series so it is
//!    never misattributed to the venue.
//! 3. `hp1_venue_delta` — `full − match`, paired per turn: the venue-added
//!    overhead the gateways / actor / journal / fan-out contribute.
//! 4. `hp1_command_append` / `hp1_event_append` — the write-ahead append's own
//!    cost (steps 1 and 4 of ADR-0006 §3), so the append's share of (1) is
//!    visible, not assumed (docs/07 §3-HP5).
//! 5. `hp1_open_loop_sojourn` — the same full-turn span under an **open-loop**
//!    schedule (coordinated-omission corrected — [`support::openloop`]).
//!
//! `harness = false` (see `Cargo.toml`'s `[[bench]]` registration): a plain
//! binary controlling its own measurement loop, not criterion's default
//! statistical-convergence harness, because per-sample `hdrhistogram` capture
//! does not fit that model ([07 §5]).
//!
//! Run: `cargo bench --bench hp1_order_path` (always `--release`). Every knob
//! is overridable via env var for a reduced-sample local run, e.g.
//! `HP1_MEASURED_OPS=5000 HP1_WARMUP_OPS=500 cargo bench --bench hp1_order_path`.

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use fauxchange::exchange::{
    ActorConfig, EventTimestamp, FixedClock, InMemoryExecutionsStore, InMemoryPositionsStore,
    InMemoryVenueJournal, JournalHeader, LineageId, MarkPriceBook, MatchingExecutor, StoreFanOut,
    TeeFanOut, spawn_underlying_actor,
};
use fauxchange::subscription::{OrderbookSubscriptionManager, WsFanOut};

use support::hdr::{new_histogram, record_duration, report};
use support::openloop::run_open_loop;
use support::timing::{TimingExecutor, TimingJournal, TurnTimings};
use support::workload::{UNDERLYING, build_workload};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    support::print_run_conditions("hp1_order_path");

    let warmup_ops = env_usize("HP1_WARMUP_OPS", 5_000);
    let measured_ops = env_usize("HP1_MEASURED_OPS", 100_000);
    let open_loop_ops = env_usize("HP1_OPEN_LOOP_OPS", 3_000);
    // `tokio::time::sleep`'s timer-wheel resolution is coarse (empirically
    // ~1 ms — see `support::openloop`'s doc comment); 2 ms is comfortably
    // above that floor.
    let open_loop_interval_us = env_u64("HP1_OPEN_LOOP_INTERVAL_US", 2_000);
    let seed = env_u64("HP1_SEED", 0xA5A5_A5A5_A5A5_A5A5);

    println!(
        "config: warmup_ops={warmup_ops} measured_ops={measured_ops} \
         open_loop_ops={open_loop_ops} open_loop_interval_us={open_loop_interval_us} \
         seed=0x{seed:016X}"
    );

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => panic!("failed to build the bench tokio runtime: {e}"),
    };

    runtime.block_on(run(
        warmup_ops,
        measured_ops,
        open_loop_ops,
        open_loop_interval_us,
        seed,
    ));
}

/// Builds one real actor stack, matching `AppState`'s own wiring
/// (`state.rs`): `TeeFanOut(StoreFanOut, WsFanOut)` over a single WS
/// subscriber (HP-1's own number is the realistic single-underlying,
/// single-connection case; `hp2_ws_fanout` sweeps N), with the write-ahead
/// journal and the match executor each wrapped for [`TurnTimings`] pairing.
fn spawn_bench_actor(lineage: &LineageId, slot: TurnTimings) -> fauxchange::exchange::ActorHandle {
    let executor = TimingExecutor::new(MatchingExecutor::new(UNDERLYING), slot.clone());
    let journal = TimingJournal::new(
        InMemoryVenueJournal::new(JournalHeader::new(lineage.clone())),
        slot,
    );
    let executions = Arc::new(InMemoryExecutionsStore::new());
    let positions = Arc::new(InMemoryPositionsStore::new());
    let marks = Arc::new(MarkPriceBook::new());
    let subscriptions = Arc::new(OrderbookSubscriptionManager::new());
    // Held, never drained (a realistic idle WS client) — leaked into the
    // fan-out's manager via the `Arc`, so it stays alive for the actor's
    // lifetime without a named binding here.
    std::mem::forget(subscriptions.subscribe());
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
    let (handle, _shutdown, join) =
        spawn_underlying_actor(config, journal, executor, fan_out, clock);
    drop(join);
    handle
}

async fn run(
    warmup_ops: usize,
    measured_ops: usize,
    open_loop_ops: usize,
    open_loop_interval_us: u64,
    seed: u64,
) {
    let lineage = LineageId::new("bench-hp1");
    let mut closed_loop_workload = build_workload(warmup_ops + measured_ops, seed, &lineage);

    let slot = TurnTimings::new();
    let handle = spawn_bench_actor(&lineage, slot.clone());

    // ---- warmup (discarded) --------------------------------------------------
    for command in closed_loop_workload.drain(..warmup_ops) {
        let _ = handle.submit(command).await;
        let _ = slot.take();
    }

    // ---- closed-loop measurement ----------------------------------------------
    let mut full_hist = new_histogram();
    let mut match_hist = new_histogram();
    let mut delta_hist = new_histogram();
    let mut command_append_hist = new_histogram();
    let mut event_append_hist = new_histogram();

    for command in closed_loop_workload {
        let t0 = Instant::now();
        let result = handle.submit(command).await;
        let full_elapsed = t0.elapsed();
        let (match_ns, command_ns, event_ns) = slot.take();

        if result.is_err() {
            // Closed-loop pacing guarantees at most one in-flight command, so
            // the bounded mailbox can never be full here — a rejection would
            // mean the actor sealed (a real failure), which would silently
            // truncate the measured series if merely skipped.
            panic!("closed-loop HP-1 submit failed unexpectedly: {result:?}");
        }

        record_duration(&mut full_hist, full_elapsed);
        let full_ns = u64::try_from(full_elapsed.as_nanos()).unwrap_or(u64::MAX);
        if let Some(ns) = match_ns {
            record_duration(&mut match_hist, Duration::from_nanos(ns));
            let delta_ns = full_ns.saturating_sub(ns).max(1);
            record_duration(&mut delta_hist, Duration::from_nanos(delta_ns));
        }
        if let Some(ns) = command_ns {
            record_duration(&mut command_append_hist, Duration::from_nanos(ns));
        }
        if let Some(ns) = event_ns {
            record_duration(&mut event_append_hist, Duration::from_nanos(ns));
        }
    }

    println!(
        "\n[HP-1] full turn (submit -> receipt), closed-loop, {measured_ops} ops after {warmup_ops} warmup:"
    );
    report("hp1_full_turn_closed_loop", &full_hist);

    println!("\n[HP-1] upstream match cost only (paired per turn, out of budget per 07 §7):");
    report("hp1_match_only", &match_hist);

    println!("\n[HP-1] venue-added delta (full - match, paired per turn):");
    report("hp1_venue_delta", &delta_hist);

    println!("\n[HP-1] write-ahead command append (ADR-0006 step 1):");
    report("hp1_command_append", &command_append_hist);

    println!("\n[HP-1] paired event append (ADR-0006 step 4):");
    report("hp1_event_append", &event_append_hist);
    drop(handle);

    // ---- open-loop, coordinated-omission-corrected measurement ----------------
    //
    // Deliberately on a FRESH actor / fresh in-memory journal, not the one the
    // closed-loop section just grew to `warmup_ops + measured_ops` records.
    // Since #091 this journal's `append` is O(1) — an index-backed
    // `(sequence, kind)` uniqueness check plus a size-check fast path replaced
    // the old O(current size) linear scan + per-append serialize (BENCH.md
    // §3.7) — so service time no longer grows with journal depth. The fresh
    // actor is retained anyway as clean methodology: it keeps "genuine
    // open-loop queueing delay" isolated from any residual warm-cache / grown-
    // `Vec` effects of the closed-loop phase, so the open-loop number stays a
    // single, unconfounded measurement (see BENCH.md's HP-1 interpretation).
    //
    // `support::openloop::wait_until` paces to genuine microsecond accuracy
    // (a coarse `tokio::time::sleep` for the bulk of the wait, then a
    // cooperative-yield spin for the final ~2 ms — `tokio::time::sleep`
    // alone is not fit for sub-millisecond pacing on this host, empirically
    // ~1 ms timer-wheel resolution). The 2 ms default interval is a
    // deliberately conservative, easily-sustainable rate, not a floor this
    // pacing mechanism itself imposes.
    let open_loop_lineage = LineageId::new("bench-hp1-open-loop");
    let open_loop_workload =
        build_workload(open_loop_ops, seed.wrapping_add(1), &open_loop_lineage);
    let open_loop_slot = TurnTimings::new();
    let open_loop_handle = spawn_bench_actor(&open_loop_lineage, open_loop_slot);

    let interval = Duration::from_micros(open_loop_interval_us.max(1));
    let (sojourn_hist, rejected) =
        run_open_loop(open_loop_handle.clone(), open_loop_workload, interval).await;
    println!(
        "\n[HP-1] full turn, OPEN-loop sojourn time (intended-send -> completion), \
         fresh actor/journal, {open_loop_ops} ops at ~{open_loop_interval_us}us intended \
         interval, {rejected} rejected (mailbox fail-fast, not queued):"
    );
    report("hp1_open_loop_sojourn", &sojourn_hist);

    drop(open_loop_handle);
}
