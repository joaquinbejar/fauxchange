# BENCH.md — fauxchange `bench-hdr` baseline

| Field       | Value                                                              |
|-------------|---------------------------------------------------------------------|
| Status      | First baseline (`#020`), extended with the persistent-mode HP-5 durable append, the #34 in-memory-append delta, a re-verified HP-2 N-sweep (`#035`), and the HP-3 FIX parse/encode budget (`#043`, §11); §5 re-measured 2026-07-18 after the `#75`/`#112` `alloc_profile` allocator fix (see §5's methodology note); §5's allocation numbers are further disclosed as a **not-yet-met** target, not a passed one (see §5's target-status note, tracked #126/#138) |
| Recorded    | 2026-07-16 (§§1-4, 6-8); 2026-07-17 (`#035`, `#043` addenda); 2026-07-18 (§5 only), on routinely-rebased working trees at those dates |
| Commit      | **Not pinned to a single SHA.** These baselines were measured on actively developed, routinely-rebased branches (`stack/20-bench-hdr`, `stack/35-persistent-budget`, `stack/43-fix-bench`) with uncommitted changes in flight — any SHA recorded here would stop identifying the measured tree the moment the branch moves, which is misleading rather than precise. The authoritative, immutable-commit re-measurement is deferred to the release-pinned tree once code is tagged (tracked: #138); until then, read every number below as a DESIGN TARGET comparison taken on a moving working tree, per the callout immediately below. |
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
> **Provenance:** these numbers were measured on a working tree during
> active, routinely-rebased development, not an immutable released commit —
> see the "Commit" row above. Do not read any date in this document as
> "re-measured on \<date\> at \<some SHA\>"; the SHA that produced a given
> number stops identifying the tree as soon as the branch moves. The
> authoritative, commit-pinned re-measurement happens on the release-pinned
> tree (#138).

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
| Allocator | system allocator (macOS `libmalloc`); `alloc_profile`'s `stats_alloc::StatsAlloc<System>` wraps `std::alloc::System`, it does not swap the allocator |
| fauxchange crate version | `0.0.1` |
| Pinned upstream crates | `option-chain-orderbook` `0.7.0`, `orderbook-rs` `0.10.5`, `pricelevel` `0.8.4`, `optionstratlib` `0.17.3` (from `Cargo.lock` on this branch — unchanged since `#020`) |
| `hdrhistogram` / `criterion` | `7.5.4` / `0.8.2` (from `Cargo.lock`) |
| Journal mode | **in-memory** (`InMemoryVenueJournal`) for HP-1/HP-2/allocation profile; **durable** (`PgVenueJournal` against a real ephemeral `postgres:18-alpine`, `testcontainers`) for HP-5 (§5, new in `#035`) |
| Docker | `29.6.1` (HP-5's `testcontainers` containers only; every other bench needs no Docker) |
| `tokio` runtime | `hp1_order_path` / `hp2_ws_fanout`: multi-thread, 2 workers, `enable_time`; `hp5_durable_append`: multi-thread, 4 workers, `enable_all` (the durable append's sync→async `sqlx` bridge needs the IO driver too, `src/db/journal.rs`); `alloc_profile` Section 1: none (synchronous `UnderlyingActor::handle`); Section 2: current-thread |
| Machine otherwise idle | Standard developer laptop session (editor, terminal, no other CPU-heavy load intentionally running); not a dedicated, isolated bench host — see Limitations |

## 2. How to reproduce

```bash
cargo bench --bench hp1_order_path
cargo bench --bench hp2_ws_fanout
cargo bench --bench hp3_fix_parse          # #043 — no Docker, no order path (pure decode/encode)
cargo bench --bench hp5_durable_append     # needs a local Docker daemon (testcontainers)
cargo bench --bench alloc_profile
cargo bench --bench criterion_match_cost   # supplementary, NOT BENCH.md evidence (§7)

# Reduced-sample local runs (every knob is an env var):
HP1_WARMUP_OPS=500 HP1_MEASURED_OPS=5000 HP1_OPEN_LOOP_OPS=500 cargo bench --bench hp1_order_path
HP2_WARMUP_OPS=500 HP2_MEASURED_OPS=5000 cargo bench --bench hp2_ws_fanout
HP3_WARMUP_OPS=500 HP3_MEASURED_OPS=5000 HP3_OPEN_LOOP_OPS=500 cargo bench --bench hp3_fix_parse
HP5_WARMUP_OPS=50 HP5_MEASURED_OPS=200 HP5_OPEN_LOOP_OPS=50 cargo bench --bench hp5_durable_append
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
run to run on this shared, un-pinned developer laptop (§8); "just inside the
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

### 3.6 The `#34` delta — in-memory append after the bounded-deserialiser size check (`#035`)

`#34` (the security-audit adversarial-fixtures milestone) added
`check_record_size` (`src/exchange/journal.rs`) to the START of
`InMemoryVenueJournal::append` — a **full `serde_json::to_string(record)`
serialization pass**, done ONLY to measure the record's byte length against
`MAX_JOURNAL_RECORD_BYTES` before the existing `(sequence, kind)` linear
scan runs. This is a genuinely NEW cost on the in-memory HP-1 path: before
`#34`, the in-memory store never serialized a record at all (it stores the
owned `JournalRecord` value directly in a `Vec`); the durable store already
paid this cost (it serializes to build the SQL `payload` parameter anyway, so
its own size check is ~free, reusing that same string — see §5 below), but
the in-memory store did not. `#035` re-runs `hp1_order_path` at the IDENTICAL
configuration as the `#020` baseline (§3.1/§3.3) to quantify the delta
honestly, as the milestone's tracked follow-up requires.

| | `#020` baseline (pre-`#34`) | `#035` run 1 (post-`#34`) | `#035` run 2 (post-`#34`) |
|---|---|---|---|
| `hp1_full_turn_closed_loop` p50 | 338 431 ns | 344 063 ns (+1.7 %) | 332 031 ns (−1.9 %) |
| `hp1_full_turn_closed_loop` p99 | 931 839 ns | 1 244 159 ns (+33.5 %) | 1 498 111 ns (+60.8 %) |
| `hp1_full_turn_closed_loop` p99.9 | 1 174 527 ns | 1 637 375 ns (+39.4 %) | 2 174 975 ns (+85.2 %) |
| `hp1_full_turn_closed_loop` p99.99 | 1 842 175 ns | 2 010 111 ns (+9.1 %) | 4 730 879 ns (+156.8 %) |
| `hp1_venue_delta` p50 | 331 007 ns | 336 127 ns (+1.5 %) | 324 095 ns (−2.1 %) |
| `hp1_venue_delta` p99 | 916 991 ns | 1 227 775 ns (+33.9 %) | 1 476 607 ns (+61.0 %) |
| `hp1_venue_delta` p99.9 | 1 150 975 ns | 1 607 679 ns (+39.7 %) | 2 123 775 ns (+84.5 %) |
| `hp1_venue_delta` p99.99 | 1 718 271 ns | 1 856 511 ns (+8.0 %) | 4 464 639 ns (+159.8 %) |
| `hp1_command_append` p50 | 160 255 ns | 161 919 ns (+1.0 %) | 155 647 ns (−2.9 %) |
| `hp1_command_append` p99 | 453 119 ns | 607 231 ns (+34.0 %) | 732 671 ns (+61.7 %) |
| `hp1_command_append` p99.9 | 564 735 ns | 886 783 ns (+57.0 %) | 1 093 631 ns (+93.6 %) |
| `hp1_event_append` p50 | 155 647 ns | 157 311 ns (+1.1 %) | 151 807 ns (−2.5 %) |
| `hp1_event_append` p99 | 447 999 ns | 607 231 ns (+35.6 %) | 729 599 ns (+62.9 %) |
| `hp1_event_append` p99.9 | 568 319 ns | 878 079 ns (+54.5 %) | 1 044 991 ns (+83.8 %) |

Same machine, same toolchain, same pinned upstream crate versions, same
`HP1_WARMUP_OPS=5000 HP1_MEASURED_OPS=100000` config, same seed — the ONLY
code change between the baseline column and the two post-`#34` columns is
`check_record_size`'s addition to `append`.

**Interpretation — a real, disclosed, unattributed-in-detail regression, not
noise.** p50 is essentially unchanged (±1–3 %, inside the baseline's own
disclosed run-to-run variance, §3.1). p99 and beyond are **consistently and
substantially worse in BOTH post-`#34` runs**, on BOTH `command_append` and
`event_append` (two independent append call sites) — a pattern this
consistent across two independent quantiles × two independent call sites ×
two independent runs is not plausibly pure noise, even though the
`#020` baseline's OWN disclosed repeat-run variance (§3.1: p99 932 µs vs
1 050 µs, ~+13 %) means the exact percentage in any one run should not be
read too precisely. The likely mechanism, named but **not measured
individually** (no call-stack profiler available, matching §6's disclosed
limitation): `check_record_size` adds an allocation (`serde_json::to_string`
builds a fresh `String`, immediately dropped) on EVERY append, on top of the
PRE-EXISTING `O(current journal depth)` linear scan (§3.1's own diagnosed
tail driver) — the two are structurally likely to COMPOUND rather than
merely add, since more append-time allocation pressure plausibly interacts
with the same growing-journal conditions that already dominate the tail, but
this bench does not isolate that interaction from a flat per-call
serialization constant. Either way, the number this run adds to the record is
honest: **the in-memory HP-1 DESIGN TARGET ("sub-millisecond at p99",
docs/07 §3), already only "just inside the ceiling" at the `#020` baseline,
is now measurably further from being met** at this journal depth. This
strengthens, not creates, the existing `#020` follow-up recommendation (an
index-backed `(SequenceNumber, RecordKind)` uniqueness check —
`matching-expert` / `architect` should now evaluate it against BOTH the
original linear-scan cost AND this added serialization cost together, since
the two are now compounding on the same code path). **Both tail-cost sources
are now tracked as [issue #91](https://github.com/joaquinbejar/fauxchange/issues/91)**
(a size-check fast path preserving the #34 symmetry invariant + the
index-backed uniqueness check), scheduled to land before #53 arms the CI
bench-regression gate over HP-1.

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

**Re-verified under `#035`** (post-`#34`; the bench now also prints an
explicit flatness verdict against a stated tolerance — see below):

| N | p50 (ns) | p99 (ns) | p99.9 (ns) | p99.99 (ns) | p99 Δ vs N=1 |
|---|---|---|---|---|---|
| 1     | 86 783 | 222 079 | 293 119 | 642 047 | — |
| 10    | 88 127 | 222 207 | 305 151 | 543 743 | +128 ns (+0.1 %) |
| 100   | 85 183 | 213 759 | 303 103 | 710 143 | −8 320 ns (−3.7 %) |
| 1 000 | 85 695 | 229 503 | 325 375 | 791 551 | +7 424 ns (+3.3 %) |

`[HP-2] flatness verdict: worst |p99 delta| across N was 3.7% (tolerance:
15%)` → **PASS**. The absolute p99 values here (213–230 µs) are higher than
the original `#020` run (156–176 µs) — consistent with `#34`'s added
per-append serialization cost (§3.6) also touching HP-2's own actor stack,
not a fan-out regression: the N-comparison itself (the thing this bench
exists to check) stays flat either way, since the added cost is identical
across all four N columns.

**Interpretation — DESIGN TARGET met, tolerance stated explicitly.** docs/07
§4's target is "HP-1 p99 is flat in N." The bench now judges this against an
explicit **±15 percentage-point tolerance** (`FLATNESS_TOLERANCE_PCT`,
`benches/hp2_ws_fanout.rs`) — wide enough to absorb the run-to-run noise
already disclosed elsewhere in this document (§3.1's baseline repeat-run
swung ~13 % at p99 with ZERO code change), narrow enough to catch a genuine
O(N) regression, which would show as a swept-N p99 many TIMES the baseline,
not a percentage-point wobble. Across a 1 000× increase in subscriber count,
the worst observed |p99 delta| was 3.7 %, well inside tolerance — flat, not
noise dressed up as flat. This is the expected result of the architecture:
`WsFanOut::emit` → `OrderbookSubscriptionManager::on_committed_event` →
`broadcast::Sender::send` is an O(1) ring-buffer write regardless of
receiver count, and none of the N receivers here are ever polled (so no
per-receiver wakeup fan-out cost is incurred either). The absolute p50
(~85–88 µs) is smaller than HP-1's full-100k-journal number (§3.1, ~332–344 µs
post-`#34`) because each N-run here only grows its journal to 32 000 records,
not 105 000 — consistent with §3.4's journal-depth finding, and irrelevant to
the N-sweep conclusion since it is identical across all four columns.

## 5. HP-5 — durable PostgreSQL journal append, and the persistent-mode order path (`#035`)

`benches/hp5_durable_append.rs`: the SAME real actor stack HP-1 measures
(`TeeFanOut(StoreFanOut, WsFanOut)`, one idle WS subscriber, the identical
seeded workload generator), with the journal swapped from
`InMemoryVenueJournal` to the durable `PgVenueJournal` (#029,
`src/db/journal.rs`) against a REAL ephemeral `postgres:18-alpine`
(`testcontainers`), never mocked. `TimingJournal`/`TimingExecutor`
(`benches/support/timing.rs`) pair the append-only and match-only series
against the SAME turns the full-turn timer measures — identical methodology
to HP-1, only the journal store differs. docs/07 §3-HP5 is explicit that this
number is **never folded into HP-1's in-memory sub-millisecond target** (§3
above); it is its own, separately budgeted, and here separately MEASURED
series.

200 warmup + 2 000 measured closed-loop ops (far smaller than HP-1's 100 000
— a durable append is a real network/disk round-trip, not an in-memory
`Vec::push`, so HP-1-scale sample counts would take unreasonably long for a
routine local run), plus a 500-op open-loop section on a **second**, fresh
ephemeral container (never sharing rows with the closed-loop section, for the
same "genuinely fresh journal" reason HP-1 isolates its own open-loop section
from its closed-loop journal growth, §3.5).

### 5.1 Closed-loop: the MEASURED fused persistent-mode full turn, and the isolated durable append

Two real runs, same config (`HP5_WARMUP_OPS=200 HP5_MEASURED_OPS=2000`, same
seed), same machine/toolchain as §1/§3.6:

| | Run 1 | Run 2 |
|---|---|---|
| `hp5_persistent_full_turn_closed_loop` p50 | 602 111 ns | 559 615 ns |
| `hp5_persistent_full_turn_closed_loop` p99 | 1 041 919 ns | 715 263 ns |
| `hp5_persistent_full_turn_closed_loop` p99.9 | 4 165 631 ns | 789 503 ns |
| `hp5_persistent_full_turn_closed_loop` p99.99 | 4 399 103 ns | 3 821 567 ns |
| `hp5_match_only` p50 | 6 251 ns | 5 295 ns |
| `hp5_match_only` p99 | 22 879 ns | 19 215 ns |
| `hp5_venue_delta` p50 | 595 967 ns | 553 471 ns |
| `hp5_venue_delta` p99 | 1 032 703 ns | 707 071 ns |
| `hp5_command_append` p50 | 284 415 ns | 262 655 ns |
| `hp5_command_append` p99 | 494 847 ns | 350 463 ns |
| `hp5_command_append` p99.9 | 2 136 063 ns | 441 599 ns |
| `hp5_event_append` p50 | 292 607 ns | 274 687 ns |
| `hp5_event_append` p99 | 532 479 ns | 375 039 ns |
| `hp5_event_append` p99.9 | 1 723 391 ns | 414 719 ns |

**Interpretation.** `hp5_match_only` (p50 ~5.3–6.3 µs) is, as expected,
indistinguishable in order of magnitude from HP-1's in-memory match cost
(§3.2, p50 5.3 µs) — the SAME `MatchingExecutor::execute` call, unaffected by
which journal backs the write-ahead append; reported here so a reader can
confirm by inspection that the durable mode's added cost is entirely
attributable to the append, not to matching moving. The **durable append
dominates the turn**: `hp5_command_append` + `hp5_event_append` p50
(284 415 + 292 607 = 577 022 ns, run 1; 262 655 + 274 687 = 537 342 ns, run 2)
accounts for essentially the whole `hp5_venue_delta` p50 (595 967 ns / 553 471
ns) — the same "the append, not matching or fan-out, is the dominant cost"
finding §3.3 makes for the in-memory case, now confirmed for the durable
case too. **At p50 the durable append is only ~1.7–1.9× the post-`#34`
in-memory append** (durable `hp5_command_append`/`hp5_event_append` p50
~263–293 µs vs in-memory `hp1_command_append`/`hp1_event_append` p50
~152–162 µs, §3.6) — a real Postgres round-trip over local Docker loopback is
NOT dramatically slower than the in-memory store's own (now `#34`-inflated)
append cost at the median, which is itself a notable finding: the in-memory
store's linear-scan-plus-serialize cost has grown close enough to a real
network+disk round-trip that "in-memory is obviously cheap" is no longer a
safe assumption at this journal depth. At the tail the comparison is NOT
stable enough to state a reliable multiplier: run 1's durable
`hp5_command_append` p99.9 (2.14 ms) is ~2.4× its in-memory counterpart
(0.89 ms, §3.6 run 1), but run 2's durable p99.9 (0.44 ms) is roughly
**one-quarter** its in-memory counterpart's tail (1.09 ms, §3.6 run 2) — the
two runs disagree on which mode has the worse tail, so no tail multiplier is
claimed here (see the tail-instability disclosure immediately below). The
p99.9/p99.99 spread between the two runs (789 µs vs 4.17 ms for the full turn) is
substantial — a real, disclosed **tail instability** this small a sample size
(2 000 ops) cannot yet distinguish from genuine Postgres/Docker scheduling
variance (WAL fsync stalls, container CPU-share jitter, connection-pool
contention) vs a systematic effect; a wider run and/or a pinned bench host
would be needed to resolve which.

### 5.2 The persistent-mode order-path composition: arithmetic vs measured-fused

docs/07 §3-HP5's framing, and this issue's acceptance criterion: report the
persistent budget as **"in-memory HP-1 + one durable append round-trip,"**
two distinct measured series composed arithmetically — never a fabricated
fused number. Because the fused path was ALSO cheap to measure directly here
(§5.1's `hp5_persistent_full_turn_closed_loop`), both are reported side by
side as a cross-check, per this issue's "if you can measure the fused path
cheaply, even better":

| | In-memory HP-1 delta (§3.6, post-`#34`, run 1) | + one HP-5 append (§5.1, run 1) | = arithmetic composition | Measured-fused (§5.1, run 1) |
|---|---|---|---|---|
| p50 | 336 127 ns | 284 415 ns | 620 542 ns | 602 111 ns |
| p99 | 1 227 775 ns | 494 847 ns | 1 722 622 ns | 1 041 919 ns |

**Read this table honestly, not as a precise identity.** The arithmetic
composition ("in-memory delta" + "one durable append") is presented because
it is what the acceptance criterion asks for literally — but a REAL
persistent-mode turn incurs **TWO** durable round-trips per submitted order
(the command append AND the paired event append, exactly like HP-1 breaks
its own in-memory delta into `hp1_command_append` + `hp1_event_append`, §3.3),
not one; "one durable append round-trip" in docs/07 §3-HP5's own prose
describes the unit cost of a single write-ahead append, not a claim that only
one occurs per turn. The measured-fused number (§5.1) is the actual ground
truth — a real actor, wired with the real durable journal, timed end to end —
and it sits BELOW even the single-append arithmetic composition at p50
(602 111 ns measured vs 620 542 ns arithmetic) and further below the
two-append arithmetic sum (336 127 + 284 415 + 292 607 ≈ 913 149 ns) would
predict. This is expected and not a contradiction: the in-memory §3.6 delta
bakes in fan-out/mailbox/bookkeeping overhead that does NOT double when the
journal moves to durable storage (only the two appends themselves change
cost), so a naive "delta + 2×append" sum over-counts that shared overhead.
**Use the measured-fused number (§5.1) as the persistent-mode budget** — it
is the real, empirically-observed figure; the arithmetic decompositions above
exist to show WHERE that number comes from (match ≈ unchanged, append ≈
dominant), not to replace it.

### 5.3 Open-loop, coordinated-omission corrected (`hp5_open_loop_sojourn`)

500 ops at a 10 ms intended interval (100 ops/s — chosen conservatively above
the ~0.6–1 ms closed-loop full-turn cost §5.1 measures, so the mailbox should
not need to queue at this rate), on a fresh actor against a SECOND fresh
container, 0 rejected in both runs:

| | Run 1 | Run 2 |
|---|---|---|
| p50 | 2 244 607 ns | 2 095 103 ns |
| p99 | 6 328 319 ns | 3 735 551 ns |
| p99.9 | 8 921 087 ns | 6 823 935 ns |
| min | 897 024 ns | 993 792 ns |

**Interpretation — an honest, unresolved anomaly, disclosed rather than
hidden.** The open-loop sojourn p50 (~2.1–2.2 ms) is **~3.5–4× the
closed-loop full-turn p50** (§5.1, ~0.56–0.60 ms), despite 0 rejections (so
the mailbox never saturated — this is not the fail-fast-under-load behavior
`benches/support/openloop.rs`'s doc comment describes) and despite the 10 ms
interval being an order of magnitude above the closed-loop service time. Two
candidate, **unattributed** explanations (no call-stack profiler available,
matching §6): (1) **connection/pool cold-start** — the open-loop section's
fresh container gets NO warmup phase (mirroring HP-1's own open-loop section,
which also skips warmup — §3.5), so the first several of only 500 samples pay
a real TCP/auth handshake cost the closed-loop section's 200-op warmup phase
already absorbed before its own measured window started, and 500 samples is
few enough that this could measurably skew a whole distribution rather than
average out; (2) **`block_in_place` compensation overhead under concurrent
dispatch** — `run_open_loop` spawns each submission as its own concurrently-
dispatched task (`benches/support/openloop.rs`), and while the single-writer
actor still processes turns strictly sequentially, the durable append's
sync-over-async bridge (`tokio::task::block_in_place` +
`Handle::block_on`, `src/db/journal.rs`) asks the runtime to hand off the
current worker EVERY append call; more concurrently-live tasks around that
handoff (the open-loop submitters awaiting their oneshot replies) is a
plausible, structurally real source of added scheduling overhead the
closed-loop section's strictly-sequential dispatch never exercises. This
reproduced across both runs (2.24 ms and 2.10 ms p50 — consistent, not a
one-off), so it is a genuine finding, not noise; which of the two candidate
causes dominates (or whether both contribute) is **not resolved by this
bench** and is named as a worthwhile follow-up rather than guessed at.

## 6. Allocation profile (`alloc_profile`)

docs/07 §4: "the steady-state turn (append → match → append → enqueue)
targets zero heap allocation on the common path." `benches/alloc_profile.rs`
installs `stats_alloc::StatsAlloc<System>` as the `#[global_allocator]` — a
`std::alloc::System` wrapper with atomic alloc/dealloc/realloc/byte counters
— and reports the delta across a 50 000-op measured window (after 5 000
warmup ops, same seeded workload as HP-1) in two sections.

> **Methodology note (updated after `#75`/`#112`).** The first baseline
> (recorded 2026-07-16, see the git history of this file) used a hand-rolled
> `CountingAllocator` with a local `unsafe impl GlobalAlloc`, which a
> self-review flagged as a **critical violation of the repo's absolute
> no-`unsafe`-anywhere rule** (CLAUDE.md / rules/global_rules.md / ADR-0008;
> an inline `// SAFETY:` comment is not a governance decision). The bench now
> uses `stats_alloc` (MIT, zero transitive dependencies, dev-only, bench-scoped
> — audit note on the `Cargo.toml` dependency), whose `unsafe impl GlobalAlloc`
> is vendored inside that crate; `fauxchange`'s own code — including this bench
> — contains zero `unsafe`. `stats_alloc` also tracks `realloc` more precisely
> than the old counter did (a realloc's byte delta is attributed to growth
> *or* shrinkage instead of always adding the full new size to "bytes
> allocated"), so the numbers below were **re-measured, not carried over**;
> see the run-to-run variance disclosure below before reading the table as a
> tight point estimate.

| Section | allocs/op | bytes_alloc/op |
|---|---|---|
| `UnderlyingActor::handle` directly (no `tokio`, the exact "append → match → append → enqueue" turn) | **77.374** | 10 881.6 |
| `ActorHandle::submit` round-trip (real `tokio` mailbox + `oneshot` reply — the production gateway-facing API) | **82.657** | 11 102.3 |

**Target status: NOT MET — disclosed gap, not partial credit.** docs/07 §4's
criterion is *zero* steady-state allocation on the common path; the measured
common actor turn allocates roughly 60–80 times per submitted command. This
is failed-target evidence, reported honestly rather than framed as "close
enough": the zero-steady-state-allocation DESIGN TARGET is open, and the
measured numbers below are the disclosed size of that gap, not a partial
pass. The run-to-run instability of this measurement is itself tracked as an
open item (#126); a dedicated re-measure once #126 is resolved — ideally
paired with a call-stack profiler so these ~60–80 allocs/op can be
attributed to a concrete call site instead of process-wide — is tracked as
the #138 follow-up.

**Run-to-run variance, disclosed.** Three consecutive runs at the identical
default configuration (`ALLOC_WARMUP_OPS=5000 ALLOC_MEASURED_OPS=50000`) on
this host produced allocs/op of 62.577 / 79.710 / 77.374 (direct) and
61.630 / 79.153 / 82.657 (async) — a wider spread than this document's other
sections disclose, run to run. The table above reports the third (most
recent) run; the other two are named here rather than discarded. This is
consistent with — though not directly measured as caused by — early-lifetime
container-growth timing (e.g. `DashMap`'s default randomized hasher shifting
exactly when an internal shard's table resizes within a fixed 50 000-op
window that starts from a freshly constructed actor each run), the same
general class of "container still growing" effect §3.4 isolates for the
journal specifically; not investigated further here.

**Method and what this does / does not prove.** This is a **process-wide**
allocation-pressure profile of the measured loop (every allocation on any
thread during the window), not a call-stack-scoped instrumentation of
`handle`/`submit` alone — that needs a call-stack profiler (e.g. `dhat`,
`heaptrack`, Instruments) this environment does not have available, and no
such tool was used; **this bench does not attribute allocations to a
specific call site**, and no claim below should be read as one. What it
proves is the failed-target finding stated above: the steady-state turn is
measurably far from the zero-allocation DESIGN TARGET, at roughly 60–80
allocations per submitted command in both sections, not the `0` the target
names. (The earlier baseline read the
async-submit section as allocating *fewer* than the direct section and called
that "notable"; across these three repeat runs the two sections are close
enough, and swap ordering run to run, that no reliable direction — async
higher or lower than direct — is claimed here; the given-workload deltas
above are within the same run-to-run noise band shown for both sections.)
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

## 7. Supplementary: `criterion_match_cost` (not BENCH.md evidence)

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

## 8. What was not measured, and why

- **HP-3 (FIX parse/encode) — now measured (`#043`, §11)**, no longer an
  omission: out of scope for `#020` (the FIX wire dialect was not yet pinned);
  `#043` adds the real decode(`D`)/encode(`8`) quantiles, closed- and
  open-loop, once the dialect landed (#036) — see §11 for the numbers and the
  open-loop-overhead disclosure there in particular.
- **HP-4 (market-maker requote)** — out of scope for #020; lands v0.5 (#050).
- **HP-5, durable/PostgreSQL journal append — now measured (`#035`, §5)**, no
  longer an omission: out of scope for `#020` (in-memory journal mode only),
  `#035` adds the real durable-append quantiles, the persistent-mode
  composition, and the open-loop sojourn — see §5 for the numbers and their
  caveats (the tail-instability and open-loop-anomaly disclosures there in
  particular). What §5 does NOT cover, still open: HP-5 was measured only
  against a **local Docker container over loopback**, never a
  network-separated Postgres host — a real deployment's DB could sit on a
  different host/AZ with materially higher network RTT, so §5's numbers
  measure the write-ahead append MECHANISM's cost on this host, not every
  deployment topology's absolute figure.
- **A CI `bench-regression` gate** — deliberately not wired by this change
  (out of scope per the #020 milestone and `#035`; armed before v1.0, #053).
  Nothing in CI fails a PR on these numbers today; `clippy --all-targets
  --all-features -- -D warnings` only confirms the benches **compile**
  (including the new `hp5_durable_append`, `#035`).
- **A per-call-site allocation attribution** — §6 explains why (no call-stack
  profiler available in this environment); the reported numbers are honest
  and real, but a finer breakdown was not attempted rather than guessed at.
  The same limitation applies to §3.6's `#34` delta and §5.3's open-loop
  anomaly — both name plausible, unattributed candidate causes rather than a
  profiler-confirmed one.
- **A dedicated, isolated bench host** — every number above was recorded on a
  shared developer laptop (§1), not a pinned, quiesced bench rig. Absolute
  figures will move on different hardware; the *shape* of each finding (the
  append's journal-depth dependence, HP-2's flatness in N, the non-zero
  allocation count) is expected to reproduce qualitatively.

## 9. Cold-bring-up (NFR-3, wall-clock — not a `bench-hdr` quantile)

`tests/docker_smoke.rs` (#027, `DOCKER=1`-gated) is the enforcement
mechanism for [PRD NFR-3](docs/PRD.md#4-non-functional-requirements) / [07
§7](docs/07-performance-budgets.md#7-what-is-explicitly-out-of-budget)'s
**cold bring-up budget**: `docker compose -f docker/docker-compose.yml up -d`
(image already built, untimed — compilation is explicitly excluded from this
budget) → the first successful `GET /health` `200`. The REST listener binds
only AFTER the bounded seeding phase completes and `AppState::begin_serving()`
flips (`src/main.rs`), so a live `/health` IS the "serving, seeded chain"
signal — there is no separate race to account for. This is a **single
wall-clock duration**, deliberately NOT a `bench-hdr` p50/p99/p99.9/p99.99
distribution (docs/07 §7 is explicit: cold start is a wall-clock NFR, not a
hot-path latency quantile) — one real measurement against a fixed budget, not
a statistical sample.

| Item | Value |
|---|---|
| DESIGN TARGET (NFR-3) | < 30 s cold |
| Measured, run 1 (image freshly built by the same test invocation) | **0.556 s** |
| Measured, run 2 (image already built, `docker`-layer-cached) | **0.483 s** |
| Image | `fauxchange:local`, 187 MB (`docker compose -f docker/docker-compose.yml build`, `runtime-slim` target) |
| Compose profile | DB-less default (no `postgres` service) |
| Machine | Apple M4 Max developer laptop (macOS, Darwin 25.5.0, `arm64`) — same class as §1, not a tuned bench rig |
| Docker | 29.6.1 |

Both runs were real `DOCKER=1 cargo test --test docker_smoke -- --nocapture`
invocations against the actual `docker compose up -d` → first `GET /health`
`200` window ([`tests/docker_smoke.rs`](tests/docker_smoke.rs)); the image
`build` step itself is excluded from the timed window in both (run 1 still
paid a real, untimed build inside the SAME test invocation since no image was
cached beforehand; run 2's untimed build step was a cache hit). Both numbers
are real, not estimated — the ~14× headroom under the 30 s budget reflects the
DB-less default (fully in-memory, no Postgres wait) and the current small seed
manifest (two underlyings, a handful of contracts, `seeds/default.toml`) on a
fast local NVMe/SSD host, not a claim about every environment; a
Postgres-backed `--profile persistent` cold start, a much larger seed
manifest, or a slower CI runner disk could all push this number up — none of
those variants are measured here, only the DB-less default the smoke test
exercises.

**Interpretation.** This is a v0.2 (#027) wall-clock NFR assertion, not a
hot-path `bench-hdr` budget — it belongs in `BENCH.md` because it is a real,
measured number this document tracks, but it is reported separately from §3's
quantile tables on purpose (a single duration against a fixed budget, not a
p50/p99/p99.9/p99.99 distribution). The DESIGN TARGET is comfortably met on
this host today; re-measure here (not re-estimate) if the seed manifest grows
materially or the compose topology changes. The durable-append separation
this same issue establishes is recorded in §8's HP-5 bullet above (now
superseded by §5's real measurements, `#035`), not duplicated here.

## 10. Files

- `benches/hp1_order_path.rs`, `benches/hp2_ws_fanout.rs`,
  `benches/hp3_fix_parse.rs` (`#043`), `benches/hp5_durable_append.rs`
  (`#035`), `benches/alloc_profile.rs`, `benches/criterion_match_cost.rs` —
  the six registered `[[bench]]` targets (`harness = false`), `Cargo.toml`.
- `benches/support/` — the reusable `bench-hdr` harness: `hdr.rs` (the
  `hdrhistogram` quantile report — unit-tested via `tests/bench_harness.rs`),
  `workload.rs` (the seeded, deterministic command-stream builder),
  `timing.rs` (the paired `TimingExecutor`/`TimingJournal` instrumentation
  seams, reused unchanged by `hp5_durable_append` against the durable
  journal), `openloop.rs` (the coordinated-omission-corrected load
  generator; `#043` adds `run_open_loop_pure` alongside the original
  `ActorHandle`-shaped `run_open_loop`), `fix_fixtures.rs` (`#043` — the
  fixed, golden-shaped `NewOrderSingle (D)` / `ExecutionReport (8)`
  fixtures HP-3 measures).
- `tests/bench_harness.rs` — 7 unit tests: the original 5 proving the
  histogram/quantile plumbing itself is correct against known distributions
  (uniform, constant, bimodal, empty, and a `report`-return-value consistency
  check), plus 2 added by `#043` proving the HP-3 `D`/`8` fixtures decode to
  themselves (never a silent reject-path measurement).
- `tests/docker_smoke.rs` (#027) — the Docker e2e smoke test that measures §9's
  cold-bring-up number and proves the one-order REST → WS-fill round-trip
  against the real container.
- `src/db/journal.rs` (`PgVenueJournal`, #029), `src/exchange/journal.rs`
  (`InMemoryVenueJournal`, `check_record_size`) — the two journal
  implementations §3.6 and §5 measure; neither changed in `#035` (a pure
  measurement issue, no `src/` change).

## 11. HP-3 — FIX parse/encode, pure venue overhead (`#043`)

`benches/hp3_fix_parse.rs`: a framed inbound `NewOrderSingle (D)` →
`fauxchange::gateway::fix::decode` → typed struct, and the reverse — a typed
`ExecutionReport (8)` → `FixBody::encode` → wire frame. Both calls are the
EXACT functions the live acceptor's `dispatch` seam calls
(`src/gateway/fix/acceptor.rs`: `super::decode(frame)`), not a
reimplementation. Neither span touches matching, the order path, the actor,
or the journal — this is pure wire-seam venue overhead
([07 §2](docs/07-performance-budgets.md#2-hot-paths), [07
§5](docs/07-performance-budgets.md#5-benchmark-methodology-the-bench-hdr-convention)'s
match/overhead separation), never fused with HP-1's numbers. Fixtures
(`benches/support/fix_fixtures.rs`) are the IDENTICAL `D`/`8` shapes that
`tests/golden_fix.rs` golden-tests against `tests/golden/fix/new_order_single_D.txt`
/ `tests/golden/fix/execution_report_8.txt` (#036) — reused, not
reconstructed, so the bench cannot silently drift from the pinned dialect;
`tests/bench_harness.rs` adds two unit tests proving both fixtures decode to
themselves (never a reject-path measurement).

Run conditions are identical to §1 (same host, same toolchain, no Postgres/
Docker needed — this bench is pure in-process CPU work) plus the FIX-specific
pinned crate versions (from `Cargo.lock` on this branch): `ironfix-core`
`0.3.0`, `ironfix-tagvalue` `0.3.0`, `ironfix-dictionary` `0.3.0`,
`ironfix-transport` `0.3.0`, `tokio-util` `0.7.18`, `bytes` `1.12.1`.

### 11.1 Closed-loop, 5 000 warmup + 100 000 measured ops (discarded warmup)

Three real, independent `cargo bench --bench hp3_fix_parse` runs on this
machine, identical configuration, disclosed side by side rather than
collapsed into one (the same "show the variance, don't hide it" convention
§3.1/§3.6 use):

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `hp3_decode_d_closed_loop` p50 | 875 ns | 875 ns | 750 ns |
| `hp3_decode_d_closed_loop` p99 | 2 251 ns | 1 125 ns | 1 084 ns |
| `hp3_decode_d_closed_loop` p99.9 | 2 543 ns | 1 250 ns | 1 167 ns |
| `hp3_decode_d_closed_loop` p99.99 | 20 047 ns | 7 375 ns | 2 793 ns |
| `hp3_decode_d_closed_loop` min / max | 750 / 99 839 ns | 708 / 41 567 ns | 666 / 23 423 ns |
| `hp3_encode_8_closed_loop` p50 | 417 ns | 458 ns | 458 ns |
| `hp3_encode_8_closed_loop` p99 | 583 ns | 625 ns | 625 ns |
| `hp3_encode_8_closed_loop` p99.9 | 667 ns | 750 ns | 667 ns |
| `hp3_encode_8_closed_loop` p99.99 | 792 ns | 6 419 ns | 875 ns |
| `hp3_encode_8_closed_loop` min / max | 333 / 10 127 ns | 375 / 17 055 ns | 333 / 5 335 ns |

**Interpretation — DESIGN TARGET grounding, not yet a stated number.**
docs/07 §3-HP3 has, until now, deliberately carried NO numeric budget for
HP-3 ("Budget stated once the FIX wire dialect is pinned … the bench lands
with v0.4, not before, so the number is grounded in the real message
schema"). This is that grounding measurement: across three independent runs,
decode `p50` is **sub-microsecond** (750–875 ns) with a low-single-digit-µs
`p99`/`p99.9` tail (1.08–2.54 µs), while encode is **sub-microsecond through
`p99.9`** (417–750 ns) — both one to two orders of magnitude inside even a
generous "sub-millisecond" reading, and
decode is consistently ~1.6–2× the cost of encode (a `FieldBag::collect` +
per-tag UTF-8/parse pass on untrusted bytes is real work the encoder's
straight-line field-write does not do). `p99.99` is the one quantile that
moves meaningfully run to run (decode: 2 793 ns – 20 047 ns; encode:
792 ns – 6 419 ns) — at 100 000 samples this quantile is resolved by roughly
the 10 slowest samples, so a single OS-scheduler preemption on this shared,
un-pinned developer laptop (background process, GC-style pause, whatever) can
move it by an order of magnitude without the underlying decode/encode code
doing anything different; this is disclosed exactly as HP-1's own p99.99
run-to-run variance is (§3.1, §3.5). **Stating the actual numeric HP-3
budget in `docs/07-performance-budgets.md` §3-HP3 is an `architect` follow-up
against this grounding data** — outside this bench's own scope (measure and
report, not set the design-doc target), consistent with how #020 refined
HP-1's target only once real quantiles existed.

### 11.2 Open-loop, coordinated-omission corrected, 3 000 ops at a ~2 ms intended interval

`support::openloop::run_open_loop_pure` (new in `#043`, extending
`benches/support/openloop.rs` alongside the pre-existing `ActorHandle`-shaped
`run_open_loop`): each call is dispatched as its own Tokio task at a fixed,
up-front `intended = start + i × interval` schedule, independent of whether
earlier calls have completed, recording `completion − intended` (sojourn
time), never `completion − actual_send` — the identical CO-correction
`run_open_loop` uses, generalised off `ActorHandle::submit` because
`decode`/`encode` have no bounded mailbox to reject against (there is no
"rejected count" for this hot path — every dispatched call always completes).

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `hp3_decode_d_open_loop_sojourn` p50 | 11 543 ns | 11 007 ns | 11 295 ns |
| `hp3_decode_d_open_loop_sojourn` p99 | 32 863 ns | 23 919 ns | 22 463 ns |
| `hp3_decode_d_open_loop_sojourn` p99.9 | 87 999 ns | 61 023 ns | 62 975 ns |
| `hp3_decode_d_open_loop_sojourn` p99.99 | 339 967 ns | 87 231 ns | 226 303 ns |
| `hp3_encode_8_open_loop_sojourn` p50 | 9 503 ns | 10 375 ns | 10 591 ns |
| `hp3_encode_8_open_loop_sojourn` p99 | 28 639 ns | 21 759 ns | 21 375 ns |
| `hp3_encode_8_open_loop_sojourn` p99.9 | 117 183 ns | 52 031 ns | 47 135 ns |
| `hp3_encode_8_open_loop_sojourn` p99.99 | 1 510 399 ns | 741 375 ns | 65 599 ns |

**Interpretation — an honest, disclosed harness-overhead effect, not a
decode/encode regression.** The open-loop sojourn p50 (~9.5–11.5 µs across
both spans) is **~13–25× the closed-loop p50** (§11.1: 750–875 ns decode,
417–458 ns encode) — a MUCH larger gap than HP-1's own open-loop section saw
relative to its closed-loop number (§3.5: 26 µs vs 15 µs, under 2×). The
reason is scale, not queueing: HP-1's actor turn costs hundreds of
microseconds, so `run_open_loop`'s own per-call dispatch overhead (Tokio task
spawn + scheduling latency until a worker actually polls the task, `JoinSet`
bookkeeping, a `Mutex`-guarded histogram write, two `Instant` reads) is
negligible next to it. HP-3's decode/encode cost under a microsecond, so that
SAME dispatch overhead — unchanged code, reused as-is from `run_open_loop`'s
pattern in the new `run_open_loop_pure` — is now the dominant contributor to
the reported sojourn time. **Read §11.2 as measuring "Tokio task-spawn +
schedule + op" cost, not an isolated decode/encode figure** — §11.1's
closed-loop numbers are the right DESIGN TARGET comparison for the wire-seam
cost itself; §11.2 remains genuinely useful as the honest answer to "what
does a FIX frame's dispatch-to-completion sojourn look like under an
independent-arrival-schedule load," which is a real and different question
from "how expensive is one decode call." The `p99.99` column is, again, a
single-sample artifact at 3 000 samples (encode run 1: 1.51 ms driven by one
outlier — a plausible one-off scheduling stall on this shared, un-pinned
host, not a repeatable finding, mirroring §3.5's identical disclosure at
comparable sample counts) and should not be read as a stable figure.

### 11.3 What this bench does and does not prove

- **Proves**: `fauxchange::gateway::fix::decode` (p50 sub-microsecond, p99/p99.9
  a low-single-digit-µs tail) and `ExecutionReport`'s `FixBody::encode`
  (sub-microsecond through p99.9) are one to two orders of magnitude inside a
  sub-millisecond reading on this host — real measured numbers from the ACTUAL
  acceptor code path (not a reimplementation), reusing the pinned #036 golden
  fixtures.
- **Does not prove**: a production SLA (this is one un-pinned developer
  laptop, §1's own disclosed limitation, not a dedicated bench rig); a stated
  HP-3 numeric budget in `docs/07-performance-budgets.md` (an `architect`
  follow-up against this data, §11.1); or a clean isolation of decode/encode
  cost under open-loop dispatch (§11.2's harness-overhead disclosure).
- **CI regression gate**: not armed by this change — `#043` is scope-limited
  to landing the measured baseline; the CI `bench-regression` gate arms
  before v1.0 (#053, [07 §6](docs/07-performance-budgets.md#6-ci-regression-gate)),
  same as every other hot path in this document.
