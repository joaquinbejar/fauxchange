//! HP-3 — FIX parse/encode, pure venue overhead
//! ([07 §2, §3](../docs/07-performance-budgets.md),
//! [043](../milestones/v0.4-fix-gateway/043-fix-parse-encode-budget.md)).
//!
//! Span measured: a framed inbound `NewOrderSingle (D)` → typed struct
//! (`fauxchange::gateway::fix::decode`, the EXACT function the acceptor's
//! `dispatch` calls, `src/gateway/fix/acceptor.rs`), and the reverse — a typed
//! `ExecutionReport (8)` → encoded frame (`FixBody::encode`, the EXACT method
//! `ExecutionReport`'s own outbound rendering calls, #039). Neither direction
//! touches the order path, matching, or the actor/journal — this is pure
//! venue overhead at the wire seam ([07 §5]'s match/overhead separation),
//! never fused with HP-1.
//!
//! Fixtures are the identical `D`/`8` shapes that `tests/golden_fix.rs`
//! golden-tests (`benches/support/fix_fixtures.rs`, reusing #036's pinned
//! dialect shapes rather than a parallel construction that could drift).
//!
//! Four reports, in this order:
//!
//! 1. `hp3_decode_d_closed_loop` — decode the fixed `D` frame, closed-loop
//!    (tight loop, no queueing).
//! 2. `hp3_encode_8_closed_loop` — encode the fixed `ExecutionReport`,
//!    closed-loop.
//! 3. `hp3_decode_d_open_loop_sojourn` — the same decode span under an
//!    **open-loop** schedule (coordinated-omission corrected —
//!    [`support::openloop::run_open_loop_pure`]).
//! 4. `hp3_encode_8_open_loop_sojourn` — the same encode span, open-loop.
//!
//! `harness = false` (see `Cargo.toml`'s `[[bench]]` registration): a plain
//! binary controlling its own measurement loop, not criterion's default
//! statistical-convergence harness, matching every other `bench-hdr` target
//! in this suite ([07 §5]).
//!
//! Run: `cargo bench --bench hp3_fix_parse` (always `--release`). Every knob
//! is overridable via env var for a reduced-sample local run, e.g.
//! `HP3_MEASURED_OPS=5000 HP3_WARMUP_OPS=500 cargo bench --bench hp3_fix_parse`.

#[path = "support/mod.rs"]
mod support;

use std::time::{Duration, Instant};

use fauxchange::gateway::fix::{DecodedMessage, FixBody, decode};

use support::fix_fixtures::{
    execution_report_fixture, execution_report_golden_frame, new_order_single_frame,
};
use support::hdr::{new_histogram, record_duration, report};
use support::openloop::run_open_loop_pure;

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
    support::print_run_conditions("hp3_fix_parse");

    let warmup_ops = env_usize("HP3_WARMUP_OPS", 5_000);
    let measured_ops = env_usize("HP3_MEASURED_OPS", 100_000);
    let open_loop_ops = env_usize("HP3_OPEN_LOOP_OPS", 3_000);
    // Same coarse-timer-wheel reasoning as HP-1's default
    // (`support::openloop`'s doc comment) — 2 ms is comfortably above the
    // ~1 ms empirical floor.
    let open_loop_interval_us = env_u64("HP3_OPEN_LOOP_INTERVAL_US", 2_000);

    println!(
        "config: warmup_ops={warmup_ops} measured_ops={measured_ops} \
         open_loop_ops={open_loop_ops} open_loop_interval_us={open_loop_interval_us}"
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
    ));
}

async fn run(
    warmup_ops: usize,
    measured_ops: usize,
    open_loop_ops: usize,
    open_loop_interval_us: u64,
) {
    // Built ONCE, outside every measured loop — the fixed decode input and the
    // fixed encode input. `new_order_single_frame()` returns the committed #036
    // golden bytes directly (asserting the reconstructed fixture still encodes
    // to them, and that they decode to `NewOrderSingle`), so a broken or drifted
    // fixture cannot silently turn this bench into a reject-path or stale-shape
    // measurement.
    let decode_frame = new_order_single_frame();
    let encode_report = execution_report_fixture();

    // Off the timed path: pin the encode span to the committed #036 golden
    // (tests/golden/fix/execution_report_8.txt). `cargo bench` builds --release,
    // so this is a plain `assert_eq!` — a `debug_assert!` would be compiled out
    // and never fire on a real bench run. A dialect change the fixture missed
    // fails loudly here instead of measuring a stale shape.
    assert_eq!(
        FixBody::encode(&encode_report).expect("bench encode"),
        execution_report_golden_frame(),
        "HP-3 encode fixture drifted from tests/golden/fix/execution_report_8.txt; \
         regenerate the golden (UPDATE_GOLDEN=1 cargo test --test golden_fix) and \
         re-check execution_report_fixture"
    );

    // ---- closed-loop decode ---------------------------------------------------
    for _ in 0..warmup_ops {
        match decode(&decode_frame) {
            Ok(DecodedMessage::NewOrderSingle(_)) => {}
            other => panic!("HP-3 decode warmup must hit the accept path: {other:?}"),
        }
    }
    let mut decode_hist = new_histogram();
    for _ in 0..measured_ops {
        let t0 = Instant::now();
        let result = decode(&decode_frame);
        let elapsed = t0.elapsed();
        match result {
            Ok(DecodedMessage::NewOrderSingle(_)) => {}
            other => panic!("HP-3 decode must hit the accept path every iteration: {other:?}"),
        }
        record_duration(&mut decode_hist, elapsed);
    }
    println!(
        "\n[HP-3] decode(D -> NewOrderSingle), closed-loop, {measured_ops} ops after \
         {warmup_ops} warmup, pure venue overhead (no match, no order path):"
    );
    report("hp3_decode_d_closed_loop", &decode_hist);

    // ---- closed-loop encode -----------------------------------------------
    for _ in 0..warmup_ops {
        let bytes = FixBody::encode(&encode_report).expect("bench encode");
        if bytes.is_empty() {
            panic!("HP-3 encode warmup produced an empty frame");
        }
    }
    let mut encode_hist = new_histogram();
    for _ in 0..measured_ops {
        let t0 = Instant::now();
        let bytes = FixBody::encode(&encode_report).expect("bench encode");
        let elapsed = t0.elapsed();
        if bytes.is_empty() {
            panic!("HP-3 encode must produce a non-empty frame every iteration");
        }
        record_duration(&mut encode_hist, elapsed);
    }
    println!(
        "\n[HP-3] encode(ExecutionReport -> 8), closed-loop, {measured_ops} ops after \
         {warmup_ops} warmup, pure venue overhead (no match, no order path):"
    );
    report("hp3_encode_8_closed_loop", &encode_hist);

    // ---- open-loop, coordinated-omission-corrected decode ---------------------
    //
    // `run_open_loop_pure` dispatches each decode call as its own task on a
    // fixed intended-send schedule, independent of completion, recording
    // `completion - intended` (sojourn time) rather than plain service time —
    // the same coordinated-omission correction HP-1's open-loop section uses,
    // generalised off `ActorHandle::submit` since `decode`/`encode` have no
    // bounded mailbox to reject against ([`support::openloop::run_open_loop_pure`]'s
    // doc comment).
    let interval = Duration::from_micros(open_loop_interval_us.max(1));
    let decode_frame_for_open_loop = decode_frame.clone();
    let decode_sojourn = run_open_loop_pure(open_loop_ops, interval, move || {
        match decode(&decode_frame_for_open_loop) {
            Ok(DecodedMessage::NewOrderSingle(_)) => {}
            other => panic!("HP-3 open-loop decode must hit the accept path: {other:?}"),
        }
    })
    .await;
    println!(
        "\n[HP-3] decode(D -> NewOrderSingle), OPEN-loop sojourn time (intended-send -> \
         completion), {open_loop_ops} ops at ~{open_loop_interval_us}us intended interval:"
    );
    report("hp3_decode_d_open_loop_sojourn", &decode_sojourn);

    // ---- open-loop, coordinated-omission-corrected encode ---------------------
    let encode_report_for_open_loop = encode_report.clone();
    let encode_sojourn = run_open_loop_pure(open_loop_ops, interval, move || {
        let bytes = FixBody::encode(&encode_report_for_open_loop).expect("bench encode");
        if bytes.is_empty() {
            panic!("HP-3 open-loop encode must produce a non-empty frame");
        }
    })
    .await;
    println!(
        "\n[HP-3] encode(ExecutionReport -> 8), OPEN-loop sojourn time (intended-send -> \
         completion), {open_loop_ops} ops at ~{open_loop_interval_us}us intended interval:"
    );
    report("hp3_encode_8_open_loop_sojourn", &encode_sojourn);
}
