# BENCH.md — fauxchange `bench-hdr` baseline

| Field       | Value                                                              |
|-------------|---------------------------------------------------------------------|
| Status      | First baseline (`#020`)                                             |
| Recorded    | 2026-07-16                                                           |
| Commit      | `de07a26dfba97f598d43818048e74fa43822ceb8` + this branch's uncommitted `#020` changes (`stack/20-bench-hdr`) |
| Methodology | [`docs/07-performance-budgets.md` §5](docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention) |

> **Every number in this document is a DESIGN TARGET comparison, never an
> achieved SLO.** These are the first real `bench-hdr` measurements taken on
> this codebase. They are honest, reproducible, and were produced by actually
> running `cargo bench` on this machine — not estimated, not carried over from
> another repo, not rounded to a "nicer" number. Where a measurement could not
> be taken cleanly, that is stated explicitly below rather than a plausible
> number being invented. `HP-1`'s own DESIGN TARGET (docs/07 §3: "sub-millisecond
> (< 1 ms) at p99") is **not yet reliably met at sustained scale** — see the
> HP-1 interpretation below, and the follow-up this baseline surfaces.

## 1. Run conditions

| Item | Value |
|---|---|
| Machine class | Apple M4 Max (developer laptop, not a tuned bench rig) |
| CPU | Apple M4 Max, 16 cores (16 logical, unified — `sysctl hw.ncpu`/`hw.physicalcpu` both report 16) |
| OS | macOS 26.5.2, Darwin 25.5.0, `arm64` |
| CPU governor / pinning | Not applicable on macOS (no `cpufreq` governor, no `taskset`); benches ran un-pinned, on battery/AC state not controlled |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)`, stable, matches `rust-toolchain.toml` |
| Build | `cargo bench` (always `--release`; the `bench` Cargo profile) |
| `RUSTFLAGS` | unset |
| Allocator | system allocator (macOS `libmalloc`); `alloc_profile`'s `CountingAllocator` wraps `std::alloc::System`, it does not swap the allocator |
| fauxchange crate version | `0.0.1` |
| Pinned upstream crates | `option-chain-orderbook` `0.7.0`, `orderbook-rs` `0.10.5`, `pricelevel` `0.8.4`, `optionstratlib` `0.17.3` (from `Cargo.lock` on this branch) |
| `hdrhistogram` / `criterion` | `7.5.4` / `0.8.2` (from `Cargo.lock`) |
| Journal mode | **in-memory** (`InMemoryVenueJournal`) — the durable PostgreSQL journal append (HP-5 persistent) lands with #035 (v0.3), out of scope for #020 |
| `tokio` runtime | `hp1_order_path` / `hp2_ws_fanout`: multi-thread, 2 workers; `alloc_profile` Section 1: none (synchronous `UnderlyingActor::handle`); Section 2: current-thread |
| Machine otherwise idle | Standard developer laptop session (editor, terminal, no other CPU-heavy load intentionally running); not a dedicated, isolated bench host — see Limitations |

## 2. How to reproduce

```bash
cargo bench --bench hp1_order_path
cargo bench --bench hp2_ws_fanout
cargo bench --bench alloc_profile
cargo bench --bench criterion_match_cost   # supplementary, NOT BENCH.md evidence (§6)

# Reduced-sample local runs (every knob is an env var):
HP1_WARMUP_OPS=500 HP1_MEASURED_OPS=5000 HP1_OPEN_LOOP_OPS=500 cargo bench --bench hp1_order_path
HP2_WARMUP_OPS=500 HP2_MEASURED_OPS=5000 cargo bench --bench hp2_ws_fanout
ALLOC_WARMUP_OPS=1000 ALLOC_MEASURED_OPS=10000 cargo bench --bench alloc_profile
```

The harness's own histogram/quantile plumbing is unit-tested (a known
distribution reports the expected quantiles) via `cargo test --test
bench_harness` — 5/5 passing on this branch.

## 3. HP-1 — sequenced order path, in-memory journal

Span: gateway (`ActorHandle::submit`) → write-ahead `VenueCommand` append →
upstream match (`MatchingExecutor::execute`) → `VenueEvent` append → fan-out
enqueued (real `TeeFanOut(StoreFanOut, WsFanOut)`, one WS subscriber). Single
underlying (`BTC`), single-writer actor, `benches/hp1_order_path.rs`.

Workload: a self-contained, seeded (`0xA5A5A5A5A5A5A5A5`) xorshift64 stream —
mostly `AddOrder` in a tight ±2-cent band around 50 000 (so a healthy fraction
cross and produce real fills, not a pure resting-insert workload), plus
~1-in-10 `CancelOrder` once the book has resting orders. 5 000 warmup ops
(discarded), 100 000 measured ops, closed-loop (one command in flight at a
time — the actor is single-writer, so this is also the realistic case: a
gateway never has two outstanding writes to the same underlying).

### 3.1 Full turn (`hp1_full_turn_closed_loop`) — the flagship HP-1 number

| Quantile | Latency |
|---|---|
| p50    | 338 431 ns (338 µs) |
| p99    | 931 839 ns (932 µs) |
| p99.9  | 1 174 527 ns (1.17 ms) |
| p99.99 | 1 842 175 ns (1.84 ms) |
| min / max | 24 704 ns / 5 435 391 ns |

**Interpretation — DESIGN TARGET status.** docs/07 §3 states the HP-1 budget
as "sub-millisecond (< 1 ms) at p99" — a ceiling to beat. At this
sample's journal depth (the actor has processed ~105 000 commands by the end
of the measured window), **p99 (932 µs) is just inside the ceiling; p99.9
(1.17 ms) and p99.99 (1.84 ms) are past it.** This is not jitter — §3.3 below
identifies the concrete, measured cause: the in-memory journal's `append`
does a **linear scan** over every existing record to enforce its
`(sequence, kind)` uniqueness key (`InMemoryVenueJournal::append`,
`src/exchange/journal.rs`), so append cost — and therefore the full turn —
grows with journal depth within a single run. §3.4 (small-N reference) shows
the same code path easily clears the budget (p99 = 33 µs) at low journal
depth. The DESIGN TARGET is not yet reliably met once an underlying has
accumulated tens of thousands of records in a single run; a follow-up (an
index-backed uniqueness check — e.g. a `HashSet<(SequenceNumber, RecordKind)>`
alongside the `Vec`, sized to make the check O(1)) is worth `matching-expert`
/ `architect` evaluating against this exact measured baseline.
**Run-to-run variance, disclosed:** a repeat run at the identical
configuration on this same host produced p50 306 175 ns / p99 1 049 599 ns /
p99.9 1 477 631 ns / p99.99 2 036 735 ns — i.e. p99 straddles the 1 ms line
run to run on this shared, un-pinned developer laptop (§7); "just inside the
ceiling" above should be read as "right at the boundary," not as a
comfortable margin either way.

### 3.2 Upstream match cost only (`hp1_match_only`) — out of budget, reported for context

Paired per turn with §3.1 (the *same* `MatchingExecutor::execute` call the
production actor makes, timed from the inside — not a second, independent
run; see `benches/support/timing.rs`).

| Quantile | Latency |
|---|---|
| p50    | 5 335 ns |
| p99    | 27 135 ns |
| p99.9  | 39 647 ns |
| p99.99 | 112 959 ns |
| min / max | 208 ns / 4 636 671 ns |

**Interpretation.** Matching-engine throughput is explicitly out of budget
([07 §7](docs/07-performance-budgets.md#7-what-is-explicitly-out-of-budget)) —
reported here only so it is never misattributed to the venue. At p50 it is
~64× smaller than the full turn (5.3 µs vs 338 µs); the append cost (§3.3),
not matching, dominates the tail.

### 3.3 Venue-added delta (`hp1_venue_delta`) — full turn minus match, paired per turn

| Quantile | Latency |
|---|---|
| p50    | 331 007 ns |
| p99    | 916 991 ns |
| p99.9  | 1 150 975 ns |
| p99.99 | 1 718 271 ns |

The write-ahead command append (step 1) and paired event append (step 4),
reported on their own so the append's share of the delta is visible, not
assumed (docs/07 §3-HP5):

| | `hp1_command_append` (step 1) | `hp1_event_append` (step 4) |
|---|---|---|
| p50    | 160 255 ns | 155 647 ns |
| p99    | 453 119 ns | 447 999 ns |
| p99.9  | 564 735 ns | 568 319 ns |
| p99.99 | 855 551 ns | 843 775 ns |

**Interpretation.** The two appends together account for essentially the
whole venue delta (160 255 + 155 647 = 315 902 ns vs the 331 007 ns delta
p50 — the ~15 µs gap is bookkeeping outside the timed append calls: the
`FanOut::emit` enqueue, the mpsc/oneshot round-trip inside `ActorHandle::submit`,
and the `Mutex` handoff this harness's own `TimingExecutor`/`TimingJournal`
instrumentation adds — see the disclosed instrumentation tax in
`benches/support/timing.rs`'s doc comment: an uncontended `std::sync::Mutex`
push per timed call, present in the *inner* (match/append) measurements but
not the driver's outer full-turn timer, so match-only/append-only are a
slight OVER-estimate and the derived delta a slight UNDER-estimate of their
true contribution). This confirms §3.1's diagnosis: **the append, not
matching or fan-out, is the dominant, measured cost**, and it is the append
whose cost is journal-depth-dependent (§3.4).

### 3.4 Small-N reference (same code path, fresh journal: 200 warmup + 2 000 measured)

Run with `HP1_WARMUP_OPS=200 HP1_MEASURED_OPS=2000`:

| | `hp1_full_turn_closed_loop` | `hp1_command_append` | `hp1_event_append` |
|---|---|---|---|
| p50 | 15 295 ns | 2 625 ns | 2 543 ns |
| p99 | 33 311 ns | 5 127 ns | 4 919 ns |
| p99.9 | 61 055 ns | 35 775 ns | 7 627 ns |
| p99.99 | 138 495 ns | 121 279 ns | 11 583 ns |

**Interpretation.** At ~2 200 total records, the full turn's p99 (33 µs) is
**~28× smaller** than at ~105 000 records (932 µs), and command-append p50
drops from 160 255 ns to 2 625 ns (~61×) — consistent with an
O(current-journal-size) cost per append, not a fixed per-call overhead. This
is the strongest evidence available (without instrumenting the journal's
internal scan directly) that §3.1's tail is a journal-growth artifact of the
current in-memory store, not the actor/mailbox/fan-out machinery around it.

### 3.5 Open-loop sojourn time, coordinated-omission corrected (`hp1_open_loop_sojourn`)

Run on a **fresh actor / fresh journal**, deliberately separate from the
closed-loop section above (chaining it onto the already-~105 000-record
journal would confound genuine open-loop queueing with the journal-growth
effect §3.4 already isolates — see `benches/hp1_order_path.rs`'s comment at
the open-loop call site). 3 000 ops at a 2 ms intended send interval
(500 ops/s), 0 mailbox rejections.

| Quantile | Latency |
|---|---|
| p50    | 26 047 ns |
| p99    | 69 631 ns |
| p99.9  | 145 407 ns |
| p99.99 | 399 871 ns |
| min / max | 8 368 ns / 399 871 ns |

**Coordinated-omission disclosure.** The generator (`benches/support/openloop.rs`)
is genuinely open-loop: each submission's *intended* send time is fixed up
front (`start + i × interval`) and dispatched as its own task independent of
whether earlier submissions have completed; the reported latency is
`completion − intended`, not `completion − actual_send`. **Methodological
note, disclosed rather than hidden:** `tokio::time::sleep` alone is not fit
for sub-millisecond pacing on this host — an isolated diagnostic measured a
requested 48 µs sleep completing ~1.2 ms late (the timer wheel's native
resolution). An early version of this bench paced directly on `sleep` and
produced a **spurious, monotonically growing "sojourn time"** (median rising
into the hundreds of microseconds to low milliseconds) that was **not**
genuine actor queueing — it was cumulative drift between the arithmetic
`intended` schedule and the timer's coarse real wake-ups. The generator now
paces via `support::openloop::wait_until`: a coarse `sleep` for the bulk of
the wait, then a cooperative-yield spin (`tokio::task::yield_now`, never
blocking a worker thread) for the final ~2 ms, closing the gap to genuine
microsecond accuracy. With that fix, the reported p50/p99 here track the
closed-loop, fresh-journal numbers in §3.4 closely (26 µs vs 15 µs p50; both
comfortably sub-millisecond), which is the expected result at this journal
depth and light load (500 ops/s, far under the ~30–60k ops/s a fresh journal
can sustain per §3.4) — i.e. no meaningful queueing at this rate, as
expected; the p99.99 (400 µs, driven by a single sample at this size) is a
plausible one-off scheduling stall on a shared, un-pinned developer laptop,
not a repeatable finding at this sample count (500–3 000 samples resolves
p99.99 to roughly its own single worst observation — a wider run is needed
before reading anything into that specific figure).

## 4. HP-2 — WS broadcast fan-out isolation

`benches/hp2_ws_fanout.rs`: a committed `VenueEvent` → serialised → enqueued
to N subscriber broadcast slots, reusing the real
`TeeFanOut(StoreFanOut, WsFanOut)` / `OrderbookSubscriptionManager` from
#008/#014. Subscribers are held, **never drained** (a realistic idle WS
client) — `tokio::broadcast::Sender::send` is a ring-buffer write, not a
per-receiver copy, so an idle, unpolled receiver should not slow the
producer; that claim is exactly what this sweep checks. 2 000 warmup + 30 000
measured ops per N, fresh actor/journal per N (so §3's journal-depth effect is
**identical** across all four columns and cancels out of the N-comparison).

| N | p50 (ns) | p99 (ns) | p99.9 (ns) | p99.99 (ns) | p99 Δ vs N=1 |
|---|---|---|---|---|---|
| 1     | 79 103 | 172 543 | 272 383 | 442 367 | — |
| 10    | 79 487 | 175 999 | 240 895 | 573 439 | +3 456 ns (+2.0 %) |
| 100   | 77 759 | 155 775 | 183 935 | 618 495 | −16 768 ns (−9.7 %) |
| 1 000 | 73 407 | 153 983 | 180 735 | 615 423 | −18 560 ns (−10.8 %) |

**Interpretation — DESIGN TARGET met.** docs/07 §4's target is "HP-1 p99 is
flat in N." Across a 1 000× increase in subscriber count, p99 **does not
grow** — it moves by at most ~2 % up and trends slightly *down* at higher N
(within ordinary run-to-run noise on a shared, un-pinned host; not read as a
genuine "more subscribers is faster" effect). This is the expected result of
the architecture: `WsFanOut::emit` → `OrderbookSubscriptionManager::on_committed_event`
→ `broadcast::Sender::send` is an O(1) ring-buffer write regardless of
receiver count, and none of the N receivers here are ever polled (so no
per-receiver wakeup fan-out cost is incurred either). The absolute p50 (~74–79 µs)
is smaller than HP-1's full-100k-journal number (§3.1, 338 µs) because each
N-run here only grows its journal to 32 000 records, not 105 000 — consistent
with §3.4's journal-depth finding, and irrelevant to the N-sweep conclusion
since it is identical across all four columns.

## 5. Allocation profile (`alloc_profile`)

docs/07 §4: "the steady-state turn (append → match → append → enqueue)
targets zero heap allocation on the common path." `benches/alloc_profile.rs`
installs a `#[global_allocator]` `CountingAllocator` (a `std::alloc::System`
wrapper with `AtomicU64` counters — stable Rust, no nightly feature) and
reports the delta across a 50 000-op measured window (after 5 000 warmup ops,
same seeded workload as HP-1) in two sections.

| Section | allocs/op | bytes_alloc/op |
|---|---|---|
| `UnderlyingActor::handle` directly (no `tokio`, the exact "append → match → append → enqueue" turn) | **78.153** | 11 526.3 |
| `ActorHandle::submit` round-trip (real `tokio` mailbox + `oneshot` reply — the production gateway-facing API) | **63.189** | 11 301.2 |

**Method and what this does / does not prove.** This is a **process-wide**
allocation-pressure profile of the measured loop (every allocation on any
thread during the window), not a call-stack-scoped instrumentation of
`handle`/`submit` alone — that needs a call-stack profiler (e.g. `dhat`,
`heaptrack`, Instruments) this environment does not have available, and no
such tool was used; **this bench does not attribute allocations to a
specific call site**, and no claim below should be read as one. What it
proves: **the steady-state turn is measurably far from the zero-allocation
DESIGN TARGET** — roughly 78 (direct) / 63 (async) allocations per submitted
command, not the `0` the target names. (The async-submit section allocating
*fewer* than the direct section, despite adding a real `oneshot::channel()`
+ `mpsc` send per call, is itself notable — plausibly because the direct
section's workload starts a fresh journal at 0 records while the async
section's workload runs immediately after it in the same process on a
*second* fresh actor at a *different* point in the run, so the two are not
perfectly controlled against each other; not investigated further here.)
Structurally-plausible, **unattributed** candidate contributors, named from
reading the code (not measured individually, so not claimed as the
explanation): `VenueCommand::clone()` for the write-ahead journal record
(`UnderlyingActor::handle`, step 1) clones every owned-`String`-backed field
(`Symbol`, `AccountId`, `ClientOrderId`); `MatchingExecutor`'s
`resting`/`venue_to_engine`/`idempotency` maps insert new owned keys per
order; the upstream matching engine's own allocation behavior is unmeasured
here and not excluded as a contributor (its *latency* is out of budget per
docs/07 §7, but this bench does not carry that exclusion through to
allocation counting). **This non-zero, honestly-reported number is exactly
the regression-signal baseline docs/07 §4 asks for** — a future PR that
changes it materially (either direction) without an explanation is the
signal to investigate with a real call-stack profiler.

## 6. Supplementary: `criterion_match_cost` (not BENCH.md evidence)

`benches/criterion_match_cost.rs` is a small, standard `criterion`-orchestrated
benchmark of deterministic workload construction (`build_workload`), added so
the `bench-hdr` convention's "criterion for orchestration" half
([07 §5](docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention))
has a real, working example in the suite alongside the `hdrhistogram`-native
benches above (mirroring the `orderbook-rs` sibling repo's own coexistence of
mean-centric Criterion benches with its `_hdr` tail-latency suite). A real
run reported (criterion's own output, `mean [min max]`, not hdrhistogram
quantiles): `build_workload_1000` — `145.19 µs 145.66 µs 146.18 µs`. **This
figure is criterion's own mean/interval estimate and is explicitly NOT cited
as a DESIGN TARGET comparison anywhere in this document** — docs/07 §5 is
unambiguous that mean is a vanity metric on this workload; it is reported
here only to show the target genuinely runs, not skipped to make the suite
"complete."

## 7. What was not measured, and why

- **HP-3 (FIX session parse)** — out of scope for #020; lands with v0.4 (#043)
  once the FIX wire dialect is pinned, per docs/07 §3-HP3 and the #020
  milestone's explicit "Out" scope.
- **HP-4 (market-maker requote)** — out of scope for #020; lands v0.5 (#050).
- **HP-5, durable/PostgreSQL journal append** — out of scope for #020; lands
  v0.3 (#035). Only the in-memory journal mode is measured here.
- **A CI `bench-regression` gate** — deliberately not wired by this change
  (out of scope per the #020 milestone; armed before v1.0, #053). Nothing in
  CI fails a PR on these numbers today; `clippy --all-targets --all-features
  -- -D warnings` only confirms the benches **compile**.
- **A per-call-site allocation attribution** — §5 explains why (no call-stack
  profiler available in this environment); the reported numbers are honest
  and real, but a finer breakdown was not attempted rather than guessed at.
- **A dedicated, isolated bench host** — every number above was recorded on a
  shared developer laptop (§1), not a pinned, quiesced bench rig. Absolute
  figures will move on different hardware; the *shape* of each finding (the
  append's journal-depth dependence, HP-2's flatness in N, the non-zero
  allocation count) is expected to reproduce qualitatively.

## 8. Files

- `benches/hp1_order_path.rs`, `benches/hp2_ws_fanout.rs`,
  `benches/alloc_profile.rs`, `benches/criterion_match_cost.rs` — the four
  registered `[[bench]]` targets (`harness = false`), `Cargo.toml`.
- `benches/support/` — the reusable `bench-hdr` harness: `hdr.rs` (the
  `hdrhistogram` quantile report — unit-tested via `tests/bench_harness.rs`),
  `workload.rs` (the seeded, deterministic command-stream builder),
  `timing.rs` (the paired `TimingExecutor`/`TimingJournal` instrumentation
  seams), `openloop.rs` (the coordinated-omission-corrected load generator).
- `tests/bench_harness.rs` — 5 unit tests proving the histogram/quantile
  plumbing itself is correct against known distributions (uniform, constant,
  bimodal, empty, and a `report`-return-value consistency check).
