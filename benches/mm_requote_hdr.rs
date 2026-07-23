//! HP-4 — the market-maker requote pipeline
//! ([07 §2, §3](../docs/07-performance-budgets.md),
//! [050](../milestones/v0.5-microstructure/050-requote-budget-isolation.md)).
//!
//! Span measured: an underlying price update
//! ([`MarketMakerEngine::update_price`]) → `requote_symbol` → the
//! persona-driven edge calc (`Quoter::generate_quote` inside `update_quote`,
//! #47) → the generated [`VenueCommand`]s handed to the [`CommandSink`].
//! `update_price` is the engine's only **public** entry point onto this
//! pipeline — `requote_symbol` / `update_quote` are private to
//! `src/market_maker/engine.rs` — so every report below times a REAL call to
//! it, never a stand-in for the #47 persona-driven requote path.
//!
//! Two sections, mirroring `alloc_profile.rs`'s "direct vs round-trip" shape,
//! because they answer two different questions:
//!
//! 1. **Engine-only** ([`support::mm_workload::CountingSink`], no channel, no
//!    actor): the PURE requote-compute cost — price update → `requote_symbol`
//!    → edge calc → `update_quote` → [`CommandSink::enqueue`] (a bare atomic
//!    increment, no `tokio` at all). This never touches matching, the actor,
//!    or the journal ([07 §5]'s match/overhead separation) — so it is, by
//!    construction, "the requote venue overhead, not upstream matching."
//! 2. **Mailbox-wired** (the REAL [`fauxchange::market_maker::ActorCommandSink`],
//!    wired to a REAL spawned actor): the same computation, but each generated
//!    command is handed to the production `ActorCommandSink::enqueue` — a
//!    non-blocking `try_send` onto a bounded channel. The channel's (and the
//!    actor mailbox's) capacity is sized so this run's total generated command
//!    count cannot exceed it (`sized_capacity`, below) — a simple arithmetic
//!    guarantee of zero drops regardless of how fast the actor's forwarder
//!    task happens to drain, isolating the ENQUEUE's own added cost from the
//!    actor's downstream processing rate (a different question this bench
//!    deliberately does not exercise — see `tests/requote_isolation.rs` for
//!    that one).
//!
//! Because `update_price` never awaits the actor's own turn (the sink's
//! `enqueue` is `try_send`, non-blocking, fire-and-forget —
//! `src/market_maker/sink.rs`'s documented "off the client path"), matching
//! ([`fauxchange::exchange::MatchingExecutor::execute`]) never runs inside
//! either timed span: it happens later, asynchronously, on the actor's own
//! task, off this bench entirely. This is the structural reason "match time
//! stays separated from venue overhead" here — there is no fused number to
//! decompose, because the two are decoupled by the production wiring itself,
//! not by a bench-side approximation.
//!
//! Registered chain ([`support::mm_workload::chain_symbols`]): 5 strikes ×
//! {call, put} = 10 instruments per engine (a realistic small option chain),
//! each bound to a shared persona (#47's persona-driven `update_quote`
//! branch), so a steady-state requote tick enqueues up to 4 × 10 = 40
//! commands (20 cancels + 20 fresh adds; the very first tick is 20 adds only,
//! no prior legs to cancel).
//!
//! Four reports, in this order:
//!
//! 1. `hp4_requote_engine_only_closed_loop` — the flagship venue-overhead
//!    number.
//! 2. `hp4_requote_mailbox_closed_loop` — the same computation plus a real
//!    bounded-channel enqueue.
//! 3. `hp4_requote_engine_only_open_loop_sojourn` — (1) under an **open-loop**
//!    schedule, coordinated-omission corrected
//!    ([`support::openloop::run_open_loop_pure`] — `update_price` has no
//!    bounded-mailbox/rejection concept of its own, the same reason HP-3 uses
//!    this generator for `decode`/`encode`).
//! 4. `hp4_requote_mailbox_open_loop_sojourn` — (2) under the same schedule.
//!
//! `harness = false` (see `Cargo.toml`'s `[[bench]]` registration): a plain
//! binary controlling its own measurement loop, matching every other
//! `bench-hdr` target in this suite ([07 §5]).
//!
//! Run: `cargo bench --bench mm_requote_hdr` (always `--release`). Every knob
//! is overridable via env var for a reduced-sample local run, e.g.
//! `HP4_MEASURED_OPS=1000 HP4_WARMUP_OPS=200 cargo bench --bench mm_requote_hdr`.

#[path = "support/mod.rs"]
mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use fauxchange::exchange::{
    ActorConfig, ActorHandle, EventTimestamp, FixedClock, InMemoryExecutionsStore,
    InMemoryPositionsStore, InMemoryVenueJournal, JournalHeader, LineageId, MarkPriceBook,
    MatchingExecutor, NoopFanOut, StoreFanOut, TeeFanOut, spawn_underlying_actor,
};
use fauxchange::market_maker::{ActorCommandSink, CommandSink};
use fauxchange::microstructure::MicrostructureConfig;

use support::hdr::{new_histogram, record_duration, report};
use support::mm_workload::{CountingSink, MM_UNDERLYING, build_engine, chain_len};
use support::openloop::run_open_loop_pure;
use support::workload::jitter_stream;

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

/// Spawns one real underlying actor plus a real [`ActorCommandSink`] wired to
/// it, both sized to `capacity` — the "`VenueCommand`s enqueued onto the actor
/// mailbox" span. Callers size `capacity` to exceed this run's total possible
/// enqueued-command count (see the module doc comment), so this bench never
/// needs the forwarder to keep pace with the producer to stay drop-free.
fn spawn_mailbox_sink(capacity: usize) -> (Arc<ActorCommandSink>, ActorHandle) {
    let lineage = LineageId::new("bench-hp4-mailbox");
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
    let config = ActorConfig::new(MM_UNDERLYING, lineage.clone(), capacity);
    let clock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));
    let journal = InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()));
    let executor = MatchingExecutor::new(MM_UNDERLYING);
    let (handle, _shutdown, join) =
        spawn_underlying_actor(config, journal, executor, fan_out, clock);
    drop(join);
    let mut handles = HashMap::new();
    handles.insert(Arc::from(MM_UNDERLYING), handle.clone());
    // The default baseline band admits the in-band bench workload prices; the sink
    // admits each requote against it exactly as the live venue does (#109).
    let sink = ActorCommandSink::with_capacity(
        handles,
        Arc::new(MicrostructureConfig::default()),
        capacity,
    );
    (sink, handle)
}

/// The upper bound on commands one [`support::mm_workload::build_engine`]
/// requote tick can enqueue: 2 cancels + 2 adds per registered instrument
/// (steady state; the very first tick has no cancels).
fn commands_per_tick() -> usize {
    4 * chain_len()
}

fn main() {
    support::print_run_conditions("mm_requote_hdr");

    let warmup_ops = env_usize("HP4_WARMUP_OPS", 1_000);
    let measured_ops = env_usize("HP4_MEASURED_OPS", 5_000);
    let open_loop_ops = env_usize("HP4_OPEN_LOOP_OPS", 3_000);
    // Same coarse-timer-wheel reasoning as HP-1/HP-3's default
    // (`support::openloop`'s doc comment) — 2 ms is comfortably above the
    // ~1 ms empirical floor.
    let open_loop_interval_us = env_u64("HP4_OPEN_LOOP_INTERVAL_US", 2_000);
    let seed = env_u64("HP4_SEED", 0xA5A5_A5A5_A5A5_A5A5);
    let n_instruments = chain_len();

    println!(
        "config: warmup_ops={warmup_ops} measured_ops={measured_ops} \
         open_loop_ops={open_loop_ops} open_loop_interval_us={open_loop_interval_us} \
         n_instruments={n_instruments} seed=0x{seed:016X}"
    );

    // 4 workers, not HP-1/HP-3's 2: this bench's mailbox-wired sections run a
    // REAL `ActorCommandSink` forwarder + a REAL actor continuously draining
    // a (deliberately oversized) backlog in the background, long after the
    // timed section's own dispatch window ends (Section 2/4's capacity is
    // sized in the tens/hundreds of thousands of commands). At 2 workers this
    // background drain measurably starved the open-loop dispatch tasks for
    // CPU (empirically: `hp4_requote_mailbox_open_loop_sojourn` p50 ~440-480 µs
    // / p99 ~1.7-1.9 ms at 2 workers vs ~140-150 µs / ~180 µs at 4 — a real,
    // reproduced 3-4x scheduler-contention effect from an unrelated background
    // task, not the enqueue cost this section exists to isolate); 4 workers
    // gives the background forwarder+actor task room without starving the
    // section under measurement. See BENCH.md's HP-4 interpretation for the
    // disclosed before/after numbers.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
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

async fn run(
    warmup_ops: usize,
    measured_ops: usize,
    open_loop_ops: usize,
    open_loop_interval_us: u64,
    seed: u64,
) {
    let per_tick = commands_per_tick();

    // ---- Section 1: engine-only (pure compute), closed-loop -------------------
    {
        let sink: Arc<dyn CommandSink> = Arc::new(CountingSink::default());
        let engine = build_engine(sink, "bench-hp4-engine-only");
        let prices = jitter_stream(warmup_ops + measured_ops, seed, 5_000_000, 2_000);

        for &price in &prices[..warmup_ops] {
            engine.update_price(MM_UNDERLYING, price);
        }

        let mut hist = new_histogram();
        for &price in &prices[warmup_ops..] {
            let t0 = Instant::now();
            engine.update_price(MM_UNDERLYING, price);
            record_duration(&mut hist, t0.elapsed());
        }
        println!(
            "\n[HP-4] engine-only requote (price update -> requote_symbol -> edge calc \
             -> update_quote -> CountingSink::enqueue), closed-loop, {measured_ops} ops \
             after {warmup_ops} warmup, {} registered instruments, pure venue overhead \
             (no channel, no actor, no match):",
            chain_len()
        );
        report("hp4_requote_engine_only_closed_loop", &hist);
    }

    // ---- Section 2: mailbox-wired (real ActorCommandSink + real actor), -------
    // ---- closed-loop -----------------------------------------------------------
    {
        let total_ops = warmup_ops + measured_ops;
        // Sized so this section's total generated commands cannot exceed the
        // sink channel's (or the actor mailbox's) capacity — see the module
        // doc comment's "mathematically guarantees zero drops" note.
        let capacity = total_ops.saturating_mul(per_tick).saturating_add(64);
        let (sink, handle) = spawn_mailbox_sink(capacity);
        let engine = build_engine(sink.clone(), "bench-hp4-mailbox");
        let prices = jitter_stream(total_ops, seed.wrapping_add(1), 5_000_000, 2_000);

        for &price in &prices[..warmup_ops] {
            engine.update_price(MM_UNDERLYING, price);
        }

        let mut hist = new_histogram();
        for &price in &prices[warmup_ops..] {
            let t0 = Instant::now();
            engine.update_price(MM_UNDERLYING, price);
            record_duration(&mut hist, t0.elapsed());
        }
        println!(
            "\n[HP-4] mailbox-wired requote (same computation, real ActorCommandSink \
             try_send onto a real spawned actor's forwarder), closed-loop, {measured_ops} \
             ops after {warmup_ops} warmup, sink+mailbox capacity={capacity} (mathematically \
             drop-free at this op count x {per_tick} commands/tick upper bound):"
        );
        report("hp4_requote_mailbox_closed_loop", &hist);

        drop(engine);
        drop(sink);
        drop(handle);
    }

    // ---- Section 3: engine-only, OPEN-loop sojourn, coordinated-omission ------
    // ---- corrected --------------------------------------------------------------
    //
    // `update_price` has no bounded-mailbox/rejection concept of its own (that
    // concept lives downstream, in the CommandSink) — the same reason HP-3
    // uses `run_open_loop_pure` for its `decode`/`encode` spans rather than
    // HP-1's `ActorHandle`-shaped `run_open_loop`.
    {
        let engine = Arc::new(build_engine(
            Arc::new(CountingSink::default()),
            "bench-hp4-engine-only-open-loop",
        ));
        let prices = Arc::new(jitter_stream(
            open_loop_ops,
            seed.wrapping_add(2),
            5_000_000,
            2_000,
        ));
        let index = Arc::new(AtomicUsize::new(0));
        let interval = Duration::from_micros(open_loop_interval_us.max(1));

        let sojourn = run_open_loop_pure(open_loop_ops, interval, move || {
            let i = index.fetch_add(1, Ordering::Relaxed) % prices.len().max(1);
            engine.update_price(MM_UNDERLYING, prices[i]);
        })
        .await;
        println!(
            "\n[HP-4] engine-only requote, OPEN-loop sojourn time (intended-send -> \
             completion), {open_loop_ops} ops at ~{open_loop_interval_us}us intended interval:"
        );
        report("hp4_requote_engine_only_open_loop_sojourn", &sojourn);
    }

    // ---- Section 4: mailbox-wired, OPEN-loop sojourn ---------------------------
    {
        let capacity = open_loop_ops.saturating_mul(per_tick).saturating_add(64);
        let (sink, handle) = spawn_mailbox_sink(capacity);
        let engine = Arc::new(build_engine(sink.clone(), "bench-hp4-mailbox-open-loop"));
        let prices = Arc::new(jitter_stream(
            open_loop_ops,
            seed.wrapping_add(3),
            5_000_000,
            2_000,
        ));
        let index = Arc::new(AtomicUsize::new(0));
        let interval = Duration::from_micros(open_loop_interval_us.max(1));

        let sojourn = run_open_loop_pure(open_loop_ops, interval, move || {
            let i = index.fetch_add(1, Ordering::Relaxed) % prices.len().max(1);
            engine.update_price(MM_UNDERLYING, prices[i]);
        })
        .await;
        println!(
            "\n[HP-4] mailbox-wired requote, OPEN-loop sojourn time (intended-send -> \
             completion), {open_loop_ops} ops at ~{open_loop_interval_us}us intended \
             interval, sink+mailbox capacity={capacity}:"
        );
        report("hp4_requote_mailbox_open_loop_sojourn", &sojourn);

        drop(sink);
        drop(handle);
    }
}
