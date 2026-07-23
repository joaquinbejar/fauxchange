//! HP-5 — durable PostgreSQL journal append, and the persistent-mode order
//! path ([07 §2, §3](../docs/07-performance-budgets.md),
//! [ADR-0006](../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
//! [029](../milestones/v0.3-replay/029-durable-journal-swap.md),
//! [035](../milestones/v0.3-replay/035-persistent-order-path-budget.md)).
//!
//! The durable [`PgVenueJournal`](fauxchange::db::PgVenueJournal) is
//! write-ahead — the append happens on the synchronous critical path, not a
//! background flush ([ADR-0006 §3]) — so its cost is a first-class, separately
//! budgeted number, never folded into HP-1's sub-millisecond **in-memory**
//! target (docs/07 §3-HP5). This bench reuses HP-1's exact closed-loop /
//! open-loop / `TimingJournal`+`TimingExecutor` methodology
//! (`benches/hp1_order_path.rs`, `benches/support/timing.rs`) with the SAME
//! real actor stack, swapping only the journal store — so the append-only
//! series and the full-turn series are **paired, per turn**, against a REAL
//! ephemeral `postgres:18-alpine` (`testcontainers`), never mocked.
//!
//! Four reports, in this order:
//!
//! 1. `hp5_persistent_full_turn_closed_loop` — the MEASURED fused
//!    persistent-mode order path: `ActorHandle::submit` round-trip through a
//!    real actor wired with a durable `PgVenueJournal`, closed-loop. This is
//!    the "if you can measure the fused path cheaply, even better" case
//!    (#035) — an empirical cross-check against the arithmetic composition
//!    "in-memory HP-1 + durable append(s)" `BENCH.md` also reports.
//! 2. `hp5_match_only` — the upstream match cost, paired per turn (identical
//!    code path to `hp1_match_only`; reported here so a reader can confirm by
//!    inspection that persistent mode does not change it — the append is the
//!    only thing that moved).
//! 3. `hp5_venue_delta` — `full - match`, paired per turn: everything the
//!    durable mode adds over raw matching, dominated by the two write-ahead
//!    appends.
//! 4. `hp5_command_append` / `hp5_event_append` — **the flagship HP-5
//!    numbers**: the durable append's own cost (ADR-0006 steps 1 and 4), each
//!    one real INSERT round-trip to the ephemeral Postgres container.
//!
//! Then a fifth, **open-loop, coordinated-omission-corrected** report,
//! `hp5_open_loop_sojourn`, on a genuinely fresh actor against a **second**
//! ephemeral container (not just a fresh in-process `Vec` — the durable case's
//! "fresh journal" is a fresh container, so the open-loop measurement is never
//! confounded by rows the closed-loop section already wrote, mirroring HP-1's
//! own fresh-actor rationale, `benches/hp1_order_path.rs`).
//!
//! `harness = false`; run: `cargo bench --bench hp5_durable_append` (needs a
//! local Docker daemon — `testcontainers` starts real `postgres:18-alpine`
//! containers). Every knob is overridable via env var for a reduced-sample
//! local run, e.g. `HP5_MEASURED_OPS=500 cargo bench --bench
//! hp5_durable_append`. Sample sizes default far smaller than HP-1's: a
//! durable append is a real network/disk round-trip, not an in-memory
//! `Vec::push`, so 100 000 measured ops would take unreasonably long for a
//! routine local run.

#[path = "support/mod.rs"]
mod support;

use std::time::{Duration, Instant};

use fauxchange::db::{DatabasePool, DbPoolConfig, PgVenueJournal};
use fauxchange::exchange::{
    ActorConfig, EventTimestamp, FixedClock, InMemoryExecutionsStore, InMemoryPositionsStore,
    JournalHeader, LineageId, MarkPriceBook, MatchingExecutor, StoreFanOut, TeeFanOut,
    spawn_underlying_actor,
};
use fauxchange::subscription::{OrderbookSubscriptionManager, WsFanOut};
use std::sync::Arc;

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
    support::print_run_conditions("hp5_durable_append");

    let warmup_ops = env_usize("HP5_WARMUP_OPS", 200);
    let measured_ops = env_usize("HP5_MEASURED_OPS", 2_000);
    let open_loop_ops = env_usize("HP5_OPEN_LOOP_OPS", 500);
    // A real durable round-trip is on the order of a millisecond on a local
    // Docker daemon (measured below), not HP-1's open-loop microsecond-scale
    // interval — 10 ms is a deliberately conservative, easily-sustainable
    // rate for THIS workload, not a floor `wait_until` itself imposes.
    let open_loop_interval_ms = env_u64("HP5_OPEN_LOOP_INTERVAL_MS", 10);
    let seed = env_u64("HP5_SEED", 0xA5A5_A5A5_A5A5_A5A5);

    println!(
        "config: warmup_ops={warmup_ops} measured_ops={measured_ops} \
         open_loop_ops={open_loop_ops} open_loop_interval_ms={open_loop_interval_ms} \
         seed=0x{seed:016X}"
    );

    // `enable_all` (not just `enable_time`, unlike HP-1/HP-2): `sqlx`'s
    // Postgres driver needs the IO driver too — a real TCP connection to the
    // ephemeral container. `worker_threads(4)` (vs HP-1/HP-2's 2): the
    // durable append's `block_in_place` (the sync-journal-over-async-sqlx
    // bridge, `src/db/journal.rs`) asks the runtime to hand the current
    // worker off while it blocks, so a little more headroom avoids
    // starving the open-loop section's concurrently-dispatched submitter
    // tasks.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => panic!("failed to build the bench tokio runtime: {e}"),
    };

    runtime.block_on(run(
        warmup_ops,
        measured_ops,
        open_loop_ops,
        open_loop_interval_ms,
        seed,
    ));
}

/// Starts an ephemeral `postgres:18-alpine`, opens the pool, and runs the
/// embedded migrations — the SAME pattern `tests/integration.rs`'s durable
/// journal tests use. Never a mocked DB.
async fn start_pg() -> (
    testcontainers_modules::testcontainers::ContainerAsync<
        testcontainers_modules::postgres::Postgres,
    >,
    DatabasePool,
) {
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::{ImageExt, runners::AsyncRunner};

    let container = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .unwrap_or_else(|e| panic!("start postgres:18-alpine container for HP-5: {e}"));
    let host = container
        .get_host()
        .await
        .unwrap_or_else(|e| panic!("container host: {e}"));
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .unwrap_or_else(|e| panic!("container port: {e}"));
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let db = DatabasePool::connect_and_migrate(
        &url,
        DbPoolConfig {
            max_connections: 5,
            slow_acquire: Duration::from_millis(500),
        },
    )
    .await
    .unwrap_or_else(|e| panic!("open pool and run migrations for HP-5: {e}"));
    (container, db)
}

/// Builds one real actor stack wired with a **durable** `PgVenueJournal`
/// (wrapped for `TimingJournal` pairing, matching `hp1_order_path.rs`'s
/// `spawn_bench_actor` exactly except for the journal store) — the SAME
/// `TeeFanOut(StoreFanOut, WsFanOut)` wiring `AppState` builds, over a single
/// idle WS subscriber (matching HP-1's own single-connection case).
fn spawn_bench_actor_durable(
    db: &DatabasePool,
    underlying: &str,
    lineage: &LineageId,
    slot: TurnTimings,
) -> fauxchange::exchange::ActorHandle {
    let header = JournalHeader::new(lineage.clone());
    let journal = match PgVenueJournal::open(db, underlying, header) {
        Ok(journal) => journal,
        Err(e) => panic!("failed to open the durable HP-5 bench journal: {e}"),
    };
    let journal = TimingJournal::new(journal, slot.clone());
    let executor = TimingExecutor::new(MatchingExecutor::new(underlying), slot);
    let executions = Arc::new(InMemoryExecutionsStore::new());
    let positions = Arc::new(InMemoryPositionsStore::new());
    let marks = Arc::new(MarkPriceBook::new());
    let subscriptions = Arc::new(OrderbookSubscriptionManager::new());
    // Held, never drained (a realistic idle WS client) — see hp1's identical
    // rationale.
    std::mem::forget(subscriptions.subscribe());
    let fan_out = TeeFanOut::new(
        StoreFanOut::new(
            Arc::clone(&executions),
            Arc::clone(&positions),
            Arc::clone(&marks),
        ),
        WsFanOut::new(Arc::clone(&subscriptions)),
    );
    let config = ActorConfig::new(underlying, lineage.clone(), 4_096);
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
    open_loop_interval_ms: u64,
    seed: u64,
) {
    // ---- closed-loop section --------------------------------------------
    println!("starting an ephemeral postgres:18-alpine for the closed-loop section...");
    let (container, db) = start_pg().await;
    println!("postgres ready; running the closed-loop HP-5 workload");

    let lineage = LineageId::new("bench-hp5");
    let mut workload = build_workload(warmup_ops + measured_ops, seed, &lineage);

    let slot = TurnTimings::new();
    let handle = spawn_bench_actor_durable(&db, UNDERLYING, &lineage, slot.clone());

    for command in workload.drain(..warmup_ops) {
        let _ = handle.submit(command).await;
        let _ = slot.take();
    }

    let mut full_hist = new_histogram();
    let mut match_hist = new_histogram();
    let mut delta_hist = new_histogram();
    let mut command_append_hist = new_histogram();
    let mut event_append_hist = new_histogram();
    // Turns whose full-turn timing came in below the match-only timing (an
    // inconsistent pair from scheduler noise / clock skew). Counted + disclosed
    // rather than clamped to a fabricated 1 ns delta that would contaminate the
    // venue-overhead histogram.
    let mut delta_anomalies = 0u64;

    for command in workload {
        let t0 = Instant::now();
        let result = handle.submit(command).await;
        let full_elapsed = t0.elapsed();
        let (match_ns, command_ns, event_ns) = slot.take();

        if result.is_err() {
            // Closed-loop pacing guarantees at most one in-flight command
            // (the same invariant HP-1 relies on) — a rejection would mean
            // the actor sealed, a real failure worth failing loudly on
            // rather than silently truncating the measured series.
            panic!("closed-loop HP-5 submit failed unexpectedly: {result:?}");
        }

        record_duration(&mut full_hist, full_elapsed);
        let full_ns = u64::try_from(full_elapsed.as_nanos()).unwrap_or(u64::MAX);
        if let Some(ns) = match_ns {
            record_duration(&mut match_hist, Duration::from_nanos(ns));
            match full_ns.checked_sub(ns) {
                Some(delta_ns) => {
                    record_duration(&mut delta_hist, Duration::from_nanos(delta_ns));
                }
                // full < match: an inconsistent timing pair. Skip it (do not
                // clamp to 1 ns) so the venue-overhead histogram stays honest.
                None => delta_anomalies += 1,
            }
        }
        if let Some(ns) = command_ns {
            record_duration(&mut command_append_hist, Duration::from_nanos(ns));
        }
        if let Some(ns) = event_ns {
            record_duration(&mut event_append_hist, Duration::from_nanos(ns));
        }
    }

    println!(
        "\n[HP-5] MEASURED FUSED persistent-mode full turn (submit -> receipt), \
         closed-loop, {measured_ops} ops after {warmup_ops} warmup, durable PgVenueJournal:"
    );
    report("hp5_persistent_full_turn_closed_loop", &full_hist);

    println!(
        "\n[HP-5] upstream match cost only (paired per turn — identical code path to hp1_match_only):"
    );
    report("hp5_match_only", &match_hist);

    println!("\n[HP-5] venue-added delta (full - match, paired per turn, durable mode):");
    report("hp5_venue_delta", &delta_hist);
    if delta_anomalies > 0 {
        println!(
            "  NOTE: {delta_anomalies} inconsistent full<match timing pair(s) were excluded from the delta histogram (not clamped)."
        );
    }

    println!(
        "\n[HP-5] durable write-ahead command append (ADR-0006 step 1, real Postgres round-trip):"
    );
    report("hp5_command_append", &command_append_hist);

    println!("\n[HP-5] durable paired event append (ADR-0006 step 4, real Postgres round-trip):");
    report("hp5_event_append", &event_append_hist);

    drop(handle);
    drop(container);

    // ---- open-loop, coordinated-omission-corrected measurement ----------
    //
    // A SECOND, fresh ephemeral container — not just a fresh in-process
    // `Vec` (HP-1's in-memory equivalent) — so the open-loop phase's journal
    // stream shares no rows with the closed-loop phase above (the same
    // "genuinely fresh journal" rationale HP-1 documents for its own
    // open-loop section, `benches/hp1_order_path.rs`).
    println!("\nstarting a SECOND ephemeral postgres:18-alpine for the open-loop section...");
    let (open_loop_container, open_loop_db) = start_pg().await;
    println!("postgres ready; running the open-loop HP-5 workload");

    let open_loop_lineage = LineageId::new("bench-hp5-open-loop");
    // The open-loop workload derives its seed as `seed + 1`; reject a `HP5_SEED`
    // of u64::MAX rather than wrapping it silently to 0 (which would select an
    // unexpected workload) — checked, per the arithmetic rule.
    let open_loop_seed = seed.checked_add(1).unwrap_or_else(|| {
        panic!("HP5_SEED must be < u64::MAX so the open-loop derived seed (seed + 1) does not wrap")
    });
    let open_loop_workload = build_workload(open_loop_ops, open_loop_seed, &open_loop_lineage);
    let open_loop_slot = TurnTimings::new();
    let open_loop_handle = spawn_bench_actor_durable(
        &open_loop_db,
        UNDERLYING,
        &open_loop_lineage,
        open_loop_slot,
    );

    let interval = Duration::from_millis(open_loop_interval_ms.max(1));
    let (sojourn_hist, rejected) =
        run_open_loop(open_loop_handle.clone(), open_loop_workload, interval).await;
    println!(
        "\n[HP-5] full turn, OPEN-loop sojourn time (intended-send -> completion), \
         fresh actor/journal/container, {open_loop_ops} ops at ~{open_loop_interval_ms}ms intended \
         interval, {rejected} rejected (mailbox fail-fast, not queued):"
    );
    report("hp5_open_loop_sojourn", &sojourn_hist);

    drop(open_loop_handle);
    drop(open_loop_container);
}
