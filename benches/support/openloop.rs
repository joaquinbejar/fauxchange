//! The open-loop load generator — dispatches submissions on a fixed schedule
//! independent of completion, recording **sojourn time**
//! (`completion_instant - intended_send_instant`) rather than plain service
//! time, so a queueing tail is never hidden by coordinated omission
//! ([07 §5](../../../docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention),
//! [020](../../../milestones/v0.1-backend-core/020-bench-hdr-harness-baseline.md)).
//!
//! A **closed-loop** driver (wait for op *i* to finish before issuing op
//! *i+1*) systematically hides queueing delay: if the actor briefly stalls, a
//! closed-loop driver simply issues its next request later, so the stall never
//! shows up as *anyone's* measured latency. This generator instead commits to
//! a fixed *intended* send time per operation up front and measures against
//! that fixed schedule regardless of when the operation actually got sent or
//! finished — a stall shows up as inflated sojourn time on every operation
//! queued behind it, exactly as a real, independent client population would
//! experience it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fauxchange::exchange::{ActorHandle, VenueCommand};
use hdrhistogram::Histogram;

use super::hdr::{new_histogram, record_duration};

/// The margin [`wait_until`] leaves for `tokio::time::sleep`'s coarse
/// timer-wheel resolution — empirically ~1 ms on the reference host (a
/// requested 48 µs sleep measured ~1.2 ms actual). Sleeping only down to
/// `intended - MARGIN`, then finishing with a cooperative spin, keeps the
/// pacing accurate to a few microseconds instead of the timer wheel's
/// native ~1 ms granularity, without ever blocking the runtime worker (the
/// spin `.await`s `yield_now` every iteration).
const SLEEP_MARGIN: Duration = Duration::from_millis(2);

/// Waits until `intended`, precisely: a coarse `tokio::time::sleep` for the
/// bulk of the wait (when there is enough of it to be worth sleeping through
/// rather than spinning), followed by a cooperative-yield spin for the final
/// stretch. `tokio::time::sleep` alone is not fit for sub-millisecond pacing
/// on this host (see [`SLEEP_MARGIN`]'s doc comment) — sleeping only misses
/// low, and the spin closes the gap to genuine microsecond accuracy.
async fn wait_until(intended: Instant) {
    let now = Instant::now();
    if now >= intended {
        return;
    }
    let want = intended - now;
    if want > SLEEP_MARGIN {
        tokio::time::sleep(want - SLEEP_MARGIN).await;
    }
    while Instant::now() < intended {
        tokio::task::yield_now().await;
    }
}

/// Runs `workload` against `handle` on a fixed-interval open-loop schedule,
/// recording the **sojourn time** of every successful submission into the
/// returned histogram.
///
/// Each submission is dispatched as its own task at (or as soon as possible
/// after) its intended send time, independent of whether earlier submissions
/// have completed. [`ActorHandle::submit`]'s mailbox is bounded and
/// **fail-fast** — a submission the mailbox cannot enqueue returns
/// `VenueError::RateLimited` immediately rather than queueing unboundedly (a
/// deliberate DoS-safe design,
/// [08 §5](../../../docs/08-threat-model.md#5-denial-of-service-posture)) — so
/// overload in this system manifests as **rejections**, not unbounded
/// queueing delay. Returns `(sojourn_histogram, rejected_count)`; a non-zero
/// `rejected_count` means the chosen `interval` drove the mailbox past its
/// capacity and is reported, never hidden.
pub async fn run_open_loop(
    handle: ActorHandle,
    workload: Vec<VenueCommand>,
    interval: Duration,
) -> (Histogram<u64>, usize) {
    let sojourn = Arc::new(Mutex::new(new_histogram()));
    let rejected = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let mut join_set = tokio::task::JoinSet::new();

    for (i, command) in workload.into_iter().enumerate() {
        let offset = u32::try_from(i).unwrap_or(u32::MAX);
        let intended = start + interval.saturating_mul(offset);
        wait_until(intended).await;

        let handle = handle.clone();
        let sojourn = Arc::clone(&sojourn);
        let rejected = Arc::clone(&rejected);
        join_set.spawn(async move {
            let result = handle.submit(command).await;
            let elapsed = intended.elapsed();
            match result {
                Ok(_) => {
                    if let Ok(mut hist) = sojourn.lock() {
                        record_duration(&mut hist, elapsed);
                    }
                }
                Err(_) => {
                    rejected.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    while join_set.join_next().await.is_some() {}

    let hist = match Arc::try_unwrap(sojourn) {
        Ok(mutex) => mutex
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        // Every spawned task has joined by this point, so every clone of
        // `sojourn` has already been dropped and this branch is unreachable
        // in practice; handled anyway rather than assumed.
        Err(shared) => match shared.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        },
    };
    let rejected_count = rejected.load(Ordering::Relaxed);
    (hist, rejected_count)
}

/// The coordinated-omission-corrected open-loop runner for a **pure,
/// synchronous** operation that has no bounded-mailbox / rejection concept —
/// HP-3's `decode`/`encode` calls are plain function calls, not an
/// `ActorHandle::submit` round-trip, so [`run_open_loop`]'s
/// `VenueCommand`/rejection-counting shape does not fit; this is the same
/// fixed-schedule, sojourn-time-recording generator with that shape
/// generalised away.
///
/// Each call to `op` is dispatched as its own task at (or as soon as possible
/// after) its intended send time, independent of whether earlier calls have
/// completed — recording `completion − intended`, never `completion −
/// actual_send`, exactly like [`run_open_loop`]. Spawning onto the Tokio
/// worker pool this way is itself a faithful model of the real acceptor
/// (`src/gateway/fix/acceptor.rs`), which runs `decode`/`encode` inline inside
/// each connection's own per-connection async task: several connections'
/// frames arriving concurrently means several `decode` calls genuinely
/// contending for worker threads, exactly what dispatching each call as an
/// independent task recreates. At a light dispatch rate the workers never
/// queue and sojourn time should track the closed-loop service time; at a
/// high enough rate this generator's own concurrent scheduling pressure
/// becomes visible as inflated sojourn time — a real, disclosed effect of
/// worker contention, not an artifact of the generator.
pub async fn run_open_loop_pure<F>(ops: usize, interval: Duration, op: F) -> Histogram<u64>
where
    F: Fn() + Clone + Send + 'static,
{
    let sojourn = Arc::new(Mutex::new(new_histogram()));
    let start = Instant::now();
    let mut join_set = tokio::task::JoinSet::new();

    for i in 0..ops {
        let offset = u32::try_from(i).unwrap_or(u32::MAX);
        let intended = start + interval.saturating_mul(offset);
        wait_until(intended).await;

        let op = op.clone();
        let sojourn = Arc::clone(&sojourn);
        join_set.spawn(async move {
            op();
            let elapsed = intended.elapsed();
            if let Ok(mut hist) = sojourn.lock() {
                record_duration(&mut hist, elapsed);
            }
        });
    }

    while join_set.join_next().await.is_some() {}

    match Arc::try_unwrap(sojourn) {
        Ok(mutex) => mutex
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        // Every spawned task has joined by this point, so every clone of
        // `sojourn` has already been dropped and this branch is unreachable
        // in practice; handled anyway rather than assumed (mirrors
        // `run_open_loop`'s identical fallback above).
        Err(shared) => match shared.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        },
    }
}
