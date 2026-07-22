//! Call-stack-attributed heap profile of the common actor turn (#126) — the
//! per-call-site breakdown the process-wide `stats_alloc` counter in
//! `benches/alloc_profile.rs` (§1) cannot produce.
//!
//! ## Why this exists
//!
//! `benches/alloc_profile.rs` reports a TOTAL `allocs/op` for
//! [`UnderlyingActor::handle`](fauxchange::exchange::UnderlyingActor::handle) but
//! attributes nothing — it is a process-wide counter with no call-stack view
//! (its own doc says so). Issue #126 needed to reconcile a divergence between a
//! stale committed figure (62–83 allocs/op) and the freshly-reproducible
//! steady-state (~180–200 allocs/op) — a total-only counter cannot say WHERE
//! the extra allocations come from. This bench's breakdown showed ~57 % of the
//! turn is the upstream `pricelevel::Hash32::to_hex` per-byte `format!` path,
//! reached through the `#34` `check_record_size` serialization — proving the
//! stale figure was PRE-`#34` code, not the current tree (see BENCH.md §6's
//! Root cause block). [`dhat::Alloc`] captures a backtrace per allocation and
//! writes a `dhat-alloc.json` program-point tree; this bench drives the SAME
//! section-1 workload as `alloc_profile.rs` under it, then post-processes that
//! tree into a per-call-site allocation table (blocks = allocation events,
//! bytes = total bytes) so each slice of the steady-state turn is attributed to
//! a concrete call stack.
//!
//! ## Method
//!
//! - The profiler is built **after** warmup, so only the measured steady-state
//!   window is recorded (warmup book/journal/map growth is excluded).
//! - Reuses `support::workload::build_workload` and the exact `StoreFanOut` +
//!   `MatchingExecutor` + `InMemoryVenueJournal` wiring of `alloc_profile.rs`
//!   §1 — no `tokio`, synchronous `UnderlyingActor::handle`, so every recorded
//!   allocation is on the sequenced order-path turn.
//! - `dhat::Alloc` swaps the global allocator, so `harness = false` and its own
//!   bench binary (a global allocator is set once per binary), never shared with
//!   `alloc_profile.rs`'s `stats_alloc` allocator.
//!
//! ## Gating and safety
//!
//! Everything here is behind the OFF-by-default `dhat-heap` feature: the default
//! `cargo bench`/`cargo test` compile a no-op `main`, and the `dhat`
//! dev-dependency is pulled in only under this feature (or `--all-features`).
//! `dhat`'s `unsafe impl GlobalAlloc` is vendored inside that dev-only crate;
//! this file, like every file in the crate, contains zero `unsafe` and
//! `src/lib.rs` keeps `#![forbid(unsafe_code)]`. Never in the shipped build.
//!
//! Run (full symbol names need debug info, which the `bench` profile omits):
//!
//! ```text
//! RUSTFLAGS="-C debuginfo=1" cargo bench --bench alloc_dhat --features dhat-heap
//! ```

#[cfg(feature = "dhat-heap")]
#[path = "support/mod.rs"]
mod support;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(feature = "dhat-heap")]
fn main() {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use serde::Deserialize;

    use fauxchange::exchange::{
        ActorConfig, EventTimestamp, FixedClock, InMemoryExecutionsStore, InMemoryPositionsStore,
        InMemoryVenueJournal, JournalHeader, LineageId, MarkPriceBook, MatchingExecutor,
        NoopFanOut, StoreFanOut, TeeFanOut, UnderlyingActor,
    };

    use support::workload::{UNDERLYING, build_workload};

    fn env_usize(key: &str, default: usize) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    // The dhat-heap.json v2 shape — only the fields this summary reads. Serde
    // ignores the rest.
    #[derive(Deserialize)]
    struct DhatFile {
        pps: Vec<ProgramPoint>,
        ftbl: Vec<String>,
    }
    #[derive(Deserialize)]
    struct ProgramPoint {
        /// Total bytes allocated at this program point over the window.
        tb: u64,
        /// Total blocks (allocation events) at this program point.
        tbk: u64,
        /// Frame indices into `ftbl`, allocation-site first, callers after.
        fs: Vec<usize>,
    }

    // Frames that are pure allocator / profiler / std-container plumbing — never
    // the "who asked for the memory" answer we want to attribute to.
    fn is_noise(frame: &str) -> bool {
        const NEEDLES: [&str; 14] = [
            "dhat::",
            "backtrace",
            "GlobalAlloc",
            "__rust_alloc",
            "__rg_",
            "alloc::alloc",
            "alloc::raw_vec",
            "RawVec",
            "raw_vec",
            "Allocator::",
            "realloc",
            "reserve",
            "core::alloc",
            "std::alloc",
        ];
        NEEDLES.iter().any(|n| frame.contains(n))
    }

    // A short, order-agnostic call-site signature: the first few non-noise
    // frames, so allocations from the same subsystem group together whichever
    // end of `fs` the leaf sits at.
    fn signature(pp: &ProgramPoint, ftbl: &[String]) -> String {
        let mut parts: Vec<&str> = Vec::new();
        for &idx in &pp.fs {
            let Some(frame) = ftbl.get(idx) else { continue };
            if is_noise(frame) {
                continue;
            }
            // Trim the leading "0x… : " address dhat prefixes each frame with,
            // and any generic-arg / hash-suffix tail, to a compact symbol.
            let sym = frame
                .rsplit(": ")
                .next()
                .unwrap_or(frame)
                .split("::{{closure}}")
                .next()
                .unwrap_or(frame);
            if parts.last() != Some(&sym) {
                parts.push(sym);
            }
            if parts.len() == 3 {
                break;
            }
        }
        if parts.is_empty() {
            "<all-noise stack>".to_string()
        } else {
            parts.join("  <-  ")
        }
    }

    support::print_run_conditions("alloc_dhat");

    let warmup_ops = env_usize("ALLOC_WARMUP_OPS", 3_000);
    let measured_ops = env_usize("ALLOC_MEASURED_OPS", 3_000);
    let seed = 0xA5A5_A5A5_A5A5_A5A5_u64;
    let out_file = "dhat-alloc.json";
    println!("config: warmup_ops={warmup_ops} measured_ops={measured_ops} out={out_file}");

    let lineage = LineageId::new("bench-alloc-dhat");
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

    // Warmup OUTSIDE the profiler window: book / journal / idempotency maps grow
    // to steady state here, uncounted.
    for command in workload.drain(..warmup_ops) {
        let _ = actor.handle(command);
    }

    // Record ONLY the steady-state measured window.
    let measured = workload.len();
    {
        let _profiler = dhat::Profiler::builder().file_name(out_file).build();
        for command in workload {
            let _ = actor.handle(command);
        }
        // `_profiler` drops here, flushing `dhat-alloc.json`.
    }

    // Post-process the program-point tree into a per-call-site table. The file
    // read + parse happen AFTER the profiler dropped, so they are not counted.
    let raw = match std::fs::read_to_string(out_file) {
        Ok(raw) => raw,
        Err(e) => {
            eprintln!("could not read {out_file}: {e}");
            return;
        }
    };
    let parsed: DhatFile = match serde_json::from_str(&raw) {
        Ok(parsed) => parsed,
        Err(e) => {
            eprintln!("could not parse {out_file}: {e}");
            return;
        }
    };

    let total_blocks: u64 = parsed.pps.iter().map(|p| p.tbk).sum();
    let total_bytes: u64 = parsed.pps.iter().map(|p| p.tb).sum();

    let mut by_site: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for pp in &parsed.pps {
        let entry = by_site.entry(signature(pp, &parsed.ftbl)).or_insert((0, 0));
        entry.0 += pp.tbk;
        entry.1 += pp.tb;
    }
    let mut ranked: Vec<(String, u64, u64)> = by_site
        .into_iter()
        .map(|(k, (blocks, bytes))| (k, blocks, bytes))
        .collect();
    ranked.sort_by_key(|entry| std::cmp::Reverse(entry.1));

    #[allow(clippy::cast_precision_loss)]
    let per_op = |n: u64| n as f64 / measured.max(1) as f64;

    println!("\n[alloc-dhat] measured window: {measured} ops");
    println!(
        "  total allocation blocks : {total_blocks}  ({:.3} allocs/op)",
        per_op(total_blocks)
    );
    println!(
        "  total bytes allocated   : {total_bytes}  ({:.1} bytes/op)",
        per_op(total_bytes)
    );
    println!("\n  per-call-site (blocks = allocation events; leaf-first signature):");
    println!(
        "  {:>10}  {:>9}  {:>12}  call site",
        "allocs/op", "% blocks", "bytes/op"
    );
    for (site, blocks, bytes) in ranked.iter().take(25) {
        #[allow(clippy::cast_precision_loss)]
        let pct = *blocks as f64 * 100.0 / total_blocks.max(1) as f64;
        println!(
            "  {:>10.3}  {:>8.1}%  {:>12.1}  {}",
            per_op(*blocks),
            pct,
            per_op(*bytes),
            site
        );
    }
}

#[cfg(not(feature = "dhat-heap"))]
fn main() {
    eprintln!(
        "alloc_dhat is a no-op without the `dhat-heap` feature. Run:\n  \
         RUSTFLAGS=\"-C debuginfo=1\" cargo bench --bench alloc_dhat --features dhat-heap"
    );
}
