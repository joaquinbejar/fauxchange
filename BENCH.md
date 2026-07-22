# BENCH.md — fauxchange `bench-hdr` baseline

| Field       | Value                                                              |
|-------------|---------------------------------------------------------------------|
| Status      | First baseline (`#020`), extended with the persistent-mode HP-5 durable append, the #34 in-memory-append delta, a re-verified HP-2 N-sweep (`#035`), the HP-3 FIX parse/encode budget (`#043`, §11), the HP-4 market-maker requote budget and requote-isolation assertion (`#050`, §12, v0.5), the CI `bench-regression` gate armed with a re-verification + documented ceilings (`#053`, §13, v1.0), the v1.0 stability soak (`#054`, §14, v1.0), and the `#091` in-memory HP-1 append tail-latency fix (index-backed uniqueness + size-check fast path, §3.7); the allocation profile (§6) re-measured 2026-07-18 after the `#75`/`#112` `alloc_profile` allocator fix, the HP-4 requote section reduced 343→232 allocs/op by `#122` (§6/§12), then the actor-turn baseline **corrected + root-caused 2026-07-22 (`#126`, RESOLVED)** — the §6 sections 1/2 baseline was stale pre-`#34` code carried over, the true steady-state is ~180–205 allocs/op, attributed by a new `dhat` call-stack bench (`benches/alloc_dhat.rs`), see §6's Baseline-correction + Root-cause blocks and §13.3; the allocation numbers remain a **not-yet-met** zero-alloc target (dominant term is upstream `Hash32::to_hex`, follow-up #165) |
| Recorded    | 2026-07-16 (§§1-4, 6-8); 2026-07-17 (`#035`, `#043` addenda); 2026-07-18 (§6 alloc profile, first stats_alloc run); 2026-07-18 (§12, `#050`); 2026-07-19 (§13, `#053`); 2026-07-19 (§14, `#054`); 2026-07-22 (§6 requote reduced `#122`; sections 1/2 re-measured + root-caused, §13.3 resolved, `#126`), on routinely-rebased working trees at those dates |
| Commit      | **Not pinned to a single SHA.** These baselines were measured on actively developed, routinely-rebased branches (`stack/20-bench-hdr`, `stack/35-persistent-budget`, `stack/43-fix-bench`, `stack/50-requote-bench`, `stack/53-regression-gate`, `stack/54-stability-soak`) with uncommitted changes in flight — any SHA recorded here would stop identifying the measured tree the moment the branch moves, which is misleading rather than precise. The authoritative, immutable-commit re-measurement is deferred to the release-pinned tree once code is tagged (tracked: #165); until then, read every number below as a DESIGN TARGET comparison taken on a moving working tree, per the callout immediately below. |
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
> tree (#165).

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
| `tokio` runtime | `hp1_order_path` / `hp2_ws_fanout`: multi-thread, 2 workers, `enable_time`; `hp5_durable_append`: multi-thread, 4 workers, `enable_all` (the durable append's sync→async `sqlx` bridge needs the IO driver too, `src/db/journal.rs`); `mm_requote_hdr` (HP-4): multi-thread, 4 workers, `enable_all` (§12.2's 2-vs-4-worker finding); `requote_isolation` test: multi-thread, `enable_all`; `alloc_profile` Section 1: none (synchronous `UnderlyingActor::handle`); Section 2: current-thread; Section 3: none (synchronous `MarketMakerEngine::update_price`) |
| Machine otherwise idle | Standard developer laptop session (editor, terminal, no other CPU-heavy load intentionally running); not a dedicated, isolated bench host — see Limitations |

## 2. How to reproduce

```bash
cargo bench --bench hp1_order_path
cargo bench --bench hp2_ws_fanout
cargo bench --bench hp3_fix_parse          # #043 — no Docker, no order path (pure decode/encode)
cargo bench --bench hp5_durable_append     # needs a local Docker daemon (testcontainers)
cargo bench --bench mm_requote_hdr         # #050 — no Docker, in-process only
cargo bench --bench alloc_profile
cargo bench --bench criterion_match_cost   # supplementary, NOT BENCH.md evidence (§7)
cargo test --test requote_isolation -- --nocapture   # #050 — the requote-isolation assertion

# Reduced-sample local runs (every knob is an env var):
HP1_WARMUP_OPS=500 HP1_MEASURED_OPS=5000 HP1_OPEN_LOOP_OPS=500 cargo bench --bench hp1_order_path
HP2_WARMUP_OPS=500 HP2_MEASURED_OPS=5000 cargo bench --bench hp2_ws_fanout
HP3_WARMUP_OPS=500 HP3_MEASURED_OPS=5000 HP3_OPEN_LOOP_OPS=500 cargo bench --bench hp3_fix_parse
HP5_WARMUP_OPS=50 HP5_MEASURED_OPS=200 HP5_OPEN_LOOP_OPS=50 cargo bench --bench hp5_durable_append
HP4_WARMUP_OPS=200 HP4_MEASURED_OPS=1000 HP4_OPEN_LOOP_OPS=300 cargo bench --bench mm_requote_hdr
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
/ `architect` evaluating against this exact measured baseline. **Update
(`#091`): that follow-up has since landed** — the O(1) index-backed uniqueness
check plus a size-check fast path removed both this scan and `#34`'s
per-append serialization; the same-machine before/after in §3.7 restores the
append `p50` to 125 ns and the full-turn `p99`/`p99.9`/`p99.99` to well inside
the 1 ms target at this same journal depth.
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
read too precisely. The mechanism — since `#126`, **confirmed** by the
`dhat` call-stack profiler (`benches/alloc_dhat.rs`; §6's Root cause block
attributes ~57 % of the actor turn's allocations to exactly this path):
`check_record_size` adds an allocation (`serde_json::to_string` builds a fresh
`String`, immediately dropped — and serializing the record's `owner: Hash32`
drags in the upstream per-byte `Hash32::to_hex`) on EVERY append, on top of the
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
were tracked as [issue #91](https://github.com/joaquinbejar/fauxchange/issues/91)**
(a size-check fast path preserving the #34 symmetry invariant + the
index-backed uniqueness check), which **has now landed and is measured in §3.7
below** — both costs are removed and the HP-1 append tail is restored to well
inside the sub-millisecond DESIGN TARGET, ahead of #53 arming the CI
bench-regression gate over HP-1.

### 3.7 `#091` — index-backed uniqueness + size-check fast path (the append tail-latency fix)

Recorded 2026-07-22 (M4 Max dev laptop, same host/toolchain as §3.1/§3.6).

`#091` replaced the two measured in-memory-append tail-latency costs §3.1/§3.6
diagnosed with equal-guarantee accelerators (`src/exchange/journal.rs`):

1. the **`O(journal-depth)` uniqueness linear scan** (`self.records.iter().find(...)`
   over every prior record) → an **`O(1)`** `HashMap<(SequenceNumber, RecordKind), usize>`
   index alongside the ordered `Vec` (the `Vec` stays the source of truth; the
   index is a uniqueness accelerator only, never iterated for output, so no
   map-iteration order enters any journal output and determinism is untouched);
2. `#34`'s **unconditional `serde_json::to_string` size check on every append** →
   a **size-check fast path**: a cheap conservative UPPER-BOUND estimate of the
   serialized size (from the record's field byte-sizes + fill/leg count, no
   allocation) skips the exact serialize for records clearly under the ceiling,
   falling back to the exact `check_record_size` only when the estimate
   approaches the ceiling. The estimate never under-estimates past the ceiling
   (worst-case JSON string-escape expansion ×6, generous per-element and base
   structural constants), so the `#34` write ≤ read symmetry invariant stays
   **exact** — any over-ceiling record still falls through to the exact check
   and is refused (proven by a new same-key soundness test:
   `estimate ≥ serde_json::to_string(record).len()` across a spread of record
   shapes, plus the unchanged size-ceiling refusal test).

Neither change alters the journal's output, ordering, uniqueness/conflict
semantics, or recovery re-execution — the determinism + golden + adversarial
suites are untouched and green. This is a pure tail-latency optimization.

**Same-machine before/after (the honest A/B).** Because the committed §3.6/§020
baseline was measured on an earlier, moving working tree, the "before" column
here was reproduced **on this same machine in the same session** by temporarily
reverting *only* the two `append` changes (the linear scan + the unconditional
serialize) — a genuine A/B, not a cross-machine comparison. It closely
reproduces the committed §3.6 post-`#34` baseline (`hp1_command_append` p50
158 µs here, squarely inside §3.6's post-`#34` ~156–162 µs; full-turn p99
1.11 ms vs §3.6's 1.24 ms), which
validates the "before" as a faithful stand-in for the shipped pre-`#091` code.
Two runs each (same `HP1_WARMUP_OPS=5000 HP1_MEASURED_OPS=100000` config, same
seed, same toolchain), reported run 1 with run 2 in parentheses for run-to-run
variance, per this document's "disclose variance, don't hide it" convention.

| | Before — pre-`#091` (run 1 / run 2) | After — `#091` (run 1 / run 2) | Δ (run 1) |
|---|---|---|---|
| `hp1_command_append` p50    | 158 335 ns / 148 223 ns | **125 ns** / 125 ns   | −99.9 % (~1 267×) |
| `hp1_command_append` p99    | 537 599 ns / 546 815 ns | **1 541 ns** / 1 458 ns | −99.7 % (~349×) |
| `hp1_command_append` p99.9  | 764 415 ns / 1 200 127 ns | **2 375 ns** / 3 209 ns | −99.7 % (~322×) |
| `hp1_command_append` p99.99 | 1 572 863 ns / 3 115 007 ns | **25 711 ns** / 23 631 ns | −98.4 % (~61×) |
| `hp1_event_append` p50    | 154 367 ns / 144 511 ns | **125 ns** / 125 ns   | −99.9 % (~1 235×) |
| `hp1_event_append` p99    | 540 671 ns / 527 359 ns | **1 458 ns** / 1 416 ns | −99.7 % (~371×) |
| `hp1_event_append` p99.9  | 762 367 ns / 998 911 ns | **2 167 ns** / 2 959 ns | −99.7 % (~352×) |
| `hp1_event_append` p99.99 | 1 317 887 ns / 3 467 263 ns | **11 671 ns** / 13 839 ns | −99.1 % (~113×) |
| `hp1_full_turn_closed_loop` p50    | 335 871 ns / 316 159 ns | **11 375 ns** / 11 671 ns | −96.6 % (~29.5×) |
| `hp1_full_turn_closed_loop` p99    | 1 113 087 ns / 1 153 023 ns | **32 639 ns** / 35 071 ns | −97.1 % (~34×) |
| `hp1_full_turn_closed_loop` p99.9  | 1 579 007 ns / 2 887 679 ns | **95 551 ns** / 98 175 ns | −93.9 % (~16.5×) |
| `hp1_full_turn_closed_loop` p99.99 | 3 051 519 ns / 8 495 103 ns | **292 607 ns** / 203 519 ns | −90.4 % (~10.4×) |

**Interpretation — DESIGN TARGET now met with margin, at journal depth.** After
`#091`, both write-ahead appends collapse to a `p50` of **125 ns** — an
`O(1)` `HashMap` insert + `Vec::push`, with no per-append serialization — at the
same ~105 000-record journal depth where §3.1/§3.6 measured 148–160 µs. The
full-turn `p99` (33 µs) is now **~34× inside** the sub-millisecond HP-1 DESIGN
TARGET (docs/07 §3), and — the headline — **`p99.9` (96 µs) and `p99.99`
(293 µs) are now BOTH comfortably inside 1 ms too**, where §3.1 had them *past*
the ceiling (1.17 ms / 1.84 ms at the `#020` baseline, worse post-`#34`). The
acceptance criterion "in-memory append `p99`/`p99.9` restored to (or better
than) the `#020` baseline envelope" is met with large margin: `p99` 33 µs vs
`#020` 932 µs, `p99.9` 96 µs vs `#020` 1.17 ms. The full turn is now dominated
by the upstream match cost and the actor/mailbox round-trip (`hp1_venue_delta`
p50 8.3 µs, of which the two appends are now ~0.25 µs), not the journal — the
journal-depth-dependent tail §3.4 isolated is **eliminated**. **Disclosed
honestly:** the paired `hp1_match_only` p50 differs between the before and after
columns (5.6 µs before vs 2.9 µs after) even though it is the *same*
`MatchingExecutor::execute` call — this is a whole-system load artifact, not a
matching change: the pre-`#091` heavy per-append allocation + growing-scan
pressure inflates even the paired inner timing (which shares the harness's
`std::sync::Mutex` instrumentation, §3.3's disclosed instrumentation tax);
removing that pressure lightens the whole process. The append numbers above are
the direct, first-order measurement and are not subject to that confound.

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
candidate, **unattributed** explanations (no *latency* call-stack profiler
such as `perf`/Instruments available here — `#126`'s `dhat` bench attributes
allocations, not wall-time, so it does not help with this sojourn anomaly):
(1) **connection/pool cold-start** — the open-loop section's
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
warmup ops, same seeded workload as HP-1) in three sections (`#050` adds the
third).

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
> allocated"), so the numbers were **re-measured, not carried over**, at that
> `#75`/`#112` allocator swap (commit `ab756ab`). **Caveat (added 2026-07-22):**
> "re-measured" was true *at `ab756ab`* — but sections 1/2 were then NOT re-run
> after the later order-path changes (`#34`/`#44`/`#47`), so their committed
> figures went stale; the Baseline-correction note immediately below supersedes
> them. See also the run-to-run variance disclosure before reading the table as
> a tight point estimate.

> **Baseline correction (2026-07-22, `#126` — sections 1/2 RE-MEASURED, and the
> divergence root-caused).** The first two rows previously read `77.374`
> (direct) / `82.657` (submit). Those figures were **measured at commit
> `ab756ab`** (the `#75`/`#112` `stats_alloc` swap — the oldest bench commit)
> and then **carried over unchanged** through the `#050` BENCH.md edit, which
> only appended the third (`MarketMakerEngine`) row and never re-ran sections
> 1/2. Between `ab756ab` and this measurement the **order-path code changed**:
> `#34` (`9e5a537`) added [`check_record_size`](src/exchange/journal.rs) — a
> `serde_json::to_string` of **every** journal record on **every** append (two
> per actor turn: the write-ahead command record and the paired event record) —
> as a DoS / write-≤-read-symmetry ceiling. That serialization walks each
> record's `owner: Hash32` field through the upstream `pricelevel::Hash32` serde
> impl, whose `Hash32::to_hex()` allocates ~32 tiny `String`s per hash (one
> `format!("{:02x}")` per byte). This runs **more than twice per turn**: beyond
> the two record wrappers, the *event* record also serializes one `owner: Hash32`
> **per fill leg** (a crossing match produces two legs, a sweep more), so a
> fill-bearing turn walks ~3.4 `to_hex` calls, not two — which is why the
> measured cost exceeds the naive `2 × 32 = 64`. A call-stack profiler
> (`benches/alloc_dhat.rs`, `dhat`, added by `#126`) attributes **~111 allocs/op
> (~57 % of the turn)** to that single `to_hex` path. So `77.374`/`82.657` describe **pre-`#34` code**
> and never applied to the tree they were committed against; the honest
> steady-state on the current tree is the re-measured cluster below (stable,
> two independent profilers agree). Full reconciliation and the per-call-site
> breakdown are in the **Root cause** block after the table.

| Section | allocs/op | bytes_alloc/op |
|---|---|---|
| `UnderlyingActor::handle` directly (no `tokio`, the exact "append → match → append → enqueue" turn) | **~195** (192.036 shown; range 180.4–203.3 over 10 runs) | ~13 300 |
| `ActorHandle::submit` round-trip (real `tokio` mailbox + `oneshot` reply — the production gateway-facing API) | **~189** (189.532 shown; range 181.0–202.7 over 10 runs) | ~13 250 |
| `MarketMakerEngine::update_price` steady-state requote (HP-4, `#050`/`#122`, no `tokio` — a 10-contract chain, `CountingSink`) | **232.000** (was 343.000 pre-`#122`) | 3 513.3 (was 6 663.3) |

**Target status: NOT MET — disclosed gap, not partial credit.** docs/07 §4's
criterion is *zero* steady-state allocation on the common path; the measured
common actor turn allocates **~180–205 times** per submitted command (the
re-measured cluster, not the stale 60–80 the pre-`#34` baseline showed). This
is failed-target evidence, reported honestly rather than framed as "close
enough": the zero-steady-state-allocation DESIGN TARGET is open, and the
measured numbers above are the disclosed size of that gap, not a partial
pass. As of `#126` the number is now **attributed to concrete call sites**
(`benches/alloc_dhat.rs`, `dhat`): ~57 % of the turn is the upstream
`pricelevel::Hash32::to_hex()` per-byte `format!` path, reached through the
`#34` `check_record_size` serialization — see the **Root cause** block below.
The gap is largely UPSTREAM (the `to_hex` allocation lives in `pricelevel`,
which the venue wraps, never forks) and partly a deliberate venue security
control (`check_record_size` serializes to enforce the per-record DoS
ceiling). Closing it materially is a design change tracked as the `#165`
follow-up, not a fix bundled into this reconciliation — the two candidate
levers are (a) an **upstream** `Hash32::to_hex` that writes hex straight into
the formatter / a fixed stack buffer instead of one `format!` `String` per
byte (a `pricelevel` change, benefiting every serializer of a `Hash32`,
including the durable Postgres journal which pays the same cost); and (b) a
**venue** change that measures a journal record's size without a full
serde round-trip on the in-memory hot path (e.g. a byte-counting `io::Write`
sink, or enforcing the per-record ceiling only where records are already
serialized) — the latter touches a security control, so it needs `architect`
+ `api-security-auditor` review, not a unilateral edit here.

**Root cause (`#126`, RESOLVED — the divergence was a stale carried-over
baseline, not run-to-run instability).** The previously-committed 60–80
allocs/op and today's ~180–205 are BOTH honest measurements — **of different
code**. §13.3 disclosed a "~2.3–2.6× divergence with no code change" and
listed candidate causes (warmup, first-touch, workload size, `stats_alloc`
drift, a genuine regression); `#126` ruled them out one by one with a
call-stack profiler and a scaling sweep:

- **Not warmup / first-touch.** `benches/alloc_dhat.rs` builds the `dhat`
  profiler *after* warmup, so only the steady-state window is recorded, and it
  still reports 195.4 allocs/op — a per-op steady-state cost, not a one-time
  lazy-init. Sweeping warmup 500→20 000 and window 5 000→100 000 (via
  `ALLOC_WARMUP_OPS`/`ALLOC_MEASURED_OPS`) holds the number flat at ~181–198;
  it does not grow with journal/book depth, so the workload IS in steady state
  and a larger warmup does not converge it toward 60–80.
- **Not `stats_alloc` drift.** A completely independent profiler (`dhat`)
  reproduces the same ~195 allocs/op the `stats_alloc` bench reports — two
  tools agree, so the counter is not miscounting.
- **It is a stale baseline over a genuine post-baseline increase.** The 60–80
  figures were measured at `ab756ab` (topologically the OLDEST bench commit —
  its `InMemoryVenueJournal::append` went straight to `records.push`, with no
  serialization) and carried over unchanged through the `#050` edit. `#34`
  (`9e5a537`) later inserted `check_record_size` — `serde_json::to_string` on
  every append — on top of that baseline, and it was never re-measured.

**Per-call-site breakdown** (`benches/alloc_dhat.rs`, `dhat` 0.3, 3 000-op
steady-state window, 195.4 allocs/op total, aggregated leaf-first):

| allocs/op | % | call site (leaf → caller) |
|---|---|---|
| **~111** | **57 %** | `pricelevel::Hash32::to_hex` (upstream `src/orders/base.rs:104`) — one `format!("{:02x}")` per byte, ×32 bytes — reached via `Hash32::serialize` inside `check_record_size` → `serde_json::to_string` (`src/exchange/journal.rs:452`), the command record + the event record every turn |
| ~42 | 21 % | `String::clone` — the `VenueCommand`/`VenueEvent` envelope clones for the write-ahead journal (`command.clone()`, `event.clone()`) plus `Symbol`/`AccountId`/`ClientOrderId`/`VenueOrderId` field clones and the idempotency-map inserts (present since the early envelope work; in the pre-`#34` baseline too) |
| ~16 | 8 % | upstream `orderbook_rs::OrderBook::untrack_order_by_id` → `dashmap::IterMut` — the matching engine's own owner-index bookkeeping per add |
| ~6.5 | 3.3 % | `serde_json::to_vec::<JournalRecord>` output buffer (the size-check serializer's own `Vec<u8>`, distinct from the `Hash32::to_hex` intermediates above) |
| ~6 | 3 % | upstream `pricelevel` / `crossbeam-skiplist` price-level + trade-list + `[u8]::to_vec` allocations per fill (several sub-1/op sites, grouped) |
| ~0.4 | 0.2 % | `MatchingExecutor::build_fills` (`src/exchange/executor.rs:1238`) — the venue's own fill `Vec` |

The single dominant term — `Hash32::to_hex` at ~57 % — is exactly the cost
`#34` introduced on the append path and that `ab756ab`'s pre-`#34` baseline
never paid, which fully accounts for the ~2.3–2.6× jump (60–80 → ~190). It is
**upstream** allocation (the venue wraps `pricelevel`, never forks it),
triggered by the venue's `check_record_size` serializing to measure record
size. The `String::clone` term (~21 %) was present in the 60–80 baseline too,
so it is not part of the divergence.

**Run-to-run variance, disclosed.** Ten runs at the default configuration
(`ALLOC_WARMUP_OPS=5000 ALLOC_MEASURED_OPS=50000`) — §13.3's seven plus three
fresh on 2026-07-22 — produced 180.4–203.3 allocs/op (direct) and 181.0–202.7
(async); the table reports a representative run and discloses the range. The
spread is ordinary early-lifetime container-growth timing (`DashMap`'s
randomized per-instance hasher shifting exactly when an internal shard resizes
within a fixed window from a freshly constructed actor), the same class of
effect §3.4 isolates for the journal — a ~±6 % band around ~190, NOT the
~150 % gap the stale baseline implied.

**The `#050` requote section, disclosed separately.** Unlike the two sections
above, three consecutive runs of the `MarketMakerEngine::update_price` section
(`ALLOC_MM_WARMUP_OPS=1000 ALLOC_MM_MEASURED_OPS=5000`) produced the
**IDENTICAL** `343.000` allocs/op and `6 663.3` bytes/op every time — no
variance at all (the `#050` baseline; `#122` below has since reduced this
section to an equally-reproducible `232.000` allocs/op / `3 513.3` bytes/op).
This is expected, not suspicious: this section runs entirely
synchronously with no `tokio` runtime at all (`CountingSink::enqueue` is a
bare atomic increment), driven by a fixed, seeded price stream against a
`MarketMakerEngine` built fresh each run in the same sequence — there is no
async task scheduling, no `DashMap`/hasher randomization in this path, and no
other source of run-to-run nondeterminism the two `tokio`-driven sections
above are subject to. See §12 for the full interpretation (a 10-contract
requote, non-zero and honestly reported as the DESIGN TARGET's
regression-signal baseline, matching the framing below).

**The `#122` reduction (measured 2026-07-22), disclosed before/after.** `#122`
drove the `MarketMakerEngine::update_price` section down from **343.000
allocs/op / 6 663.3 bytes/op** to **232.000 allocs/op / 3 513.3 bytes/op** — a
measured **−111 allocs/op (−32.4%)** and **−3 150 bytes/op (−47.3%)**, on the
same host/toolchain/`Cargo.lock`, and equally reproducible (`232.000` exactly
on every re-run, same zero-variance property as the `#050` baseline). The
reduction is three purely-internal, **wire-form-preserving** representation
changes on the venue's own requote plumbing (the produced `VenueCommand`
stream is byte-identical — asserted by
`market_maker::engine::tests::test_requote_output_is_byte_identical_across_identical_runs`):

- `Symbol` now stores its canonical string as `Arc<str>` instead of `String`
  (`src/exchange/symbol.rs`), so the ~7 `Symbol` clones a single-instrument
  requote fans across two tracking maps and up to four `VenueCommand`s become
  reference-count bumps, not heap allocations. Wire form is unchanged: `Symbol`
  serialises through `#[serde(try_from = "String", into = "String")]`, not a
  transparent `Arc<str>` forward, so the JSON/FIX/journal bytes are identical
  and no serde `rc` feature is pulled — the one owned `String` is still
  materialised only at the serialize seam.
- The underlying ticker is interned once at registration as an `Arc<str>` on
  each `QuotableInstrument` and cloned (a refcount bump) into each per-leg
  `RestingQuote`, replacing the per-leg `String` allocations the old
  `underlying.to_string()` calls made in the quote loop every tick. (The
  `update_price` ENTRY path's own two `underlying.to_string()` calls — the
  prices-map key and the `PriceUpdated` event — are NOT removed by this pass;
  see "What remains" below.)
- `requote_symbol` gathers an underlying's contracts into a **reused
  per-engine scratch buffer** of `Arc<QuotableInstrument>` clones instead of
  deep-cloning the whole `Vec<QuotableInstrument>` (with its owned `Symbol` /
  underlying / persona-name) each tick — eliminating that per-tick `Vec` +
  per-contract deep copy while still releasing the `instruments` read lock
  before the quote loop (no lock across a sink enqueue / broadcast, rule 8).

**What remains — HYPOTHESISED contributors, not a measured attribution.** The
`alloc_profile` counter is **process-wide** and no call-stack profiler was run,
so the breakdown below is a **source-reading hypothesis** about where the
residual 232 allocs/op most likely come from — NOT a per-call-site measurement.
It is not evidence that these are the dominant costs or that venue plumbing has
reached any "floor"; attributing the 232 to concrete call sites needs the
call-stack-profiler follow-up (#138). Read the list as candidates to
investigate, not as measured shares. In particular, `update_price` itself
(`src/market_maker/engine.rs`) **still visibly allocates two owned underlying
`String`s per tick** — one for the prices-map `insert` key and one for the
`MarketMakerEvent::PriceUpdated { symbol }` broadcast — so the entry path is
demonstrably NOT at a wire-safe allocation floor; those two are a known,
removable venue-plumbing residual (a follow-up, not claimed already-minimal).
The likely larger contributors, by source inspection:

- the **`optionstratlib` Black-Scholes evaluation** — 10 real
  `Quoter::generate_quote` calls per tick, each building an
  `optionstratlib::Options` (which owns a `String` underlying its API forces
  us to allocate, rebuilt per valuation because spot/strike/style differ) and
  running the Decimal-heavy `black_scholes` kernel. Pricing/Greeks are
  mandated to go through `optionstratlib` (CLAUDE.md), so this is a **named
  upstream-bound cost**, not venue plumbing to optimise here; and
- the `AccountId` / `VenueOrderId` owned-`String` clones on the emitted
  commands (the reserved market-maker account tag on 4 commands/instrument,
  plus the minted leg-id clones the two tracking maps hold). These id
  newtypes are `#[serde(transparent)]` `String`-backed DTOs in `src/models.rs`;
  interning them to `Arc<str>` the way `Symbol` was done would require either
  the serde `rc` feature or a hand-rolled `Serialize`/`Deserialize` +
  `ToSchema` on that DTO surface — a wire/schema change out of scope for this
  in-plumbing pass and gated by `#122`'s own "if the wire form would change,
  don't do it." Named as a follow-up, not silently absorbed.

The zero-steady-state-allocation DESIGN TARGET therefore remains **open** for
this path, now at a smaller, MEASURED gap (232 allocs/op, down from 343 — the
number is measured; the per-call-site breakdown above is not). The remainder is
NOT claimed to be at any "wire-safe floor": `update_price` still allocates two
owned underlying strings per tick (above), and the split between the pricing
kernel, the DTO id representation, and that entry-path residual is a hypothesis
pending the #138 call-stack-attribution follow-up, not a measured attribution.

**Method and what this does / does not prove.** `alloc_profile.rs` itself is a
**process-wide** allocation-pressure profile of the measured loop (every
allocation on any thread during the window), not a call-stack-scoped
instrumentation of `handle`/`submit` alone. As of `#126` that call-stack view
exists as a **separate** bench: `benches/alloc_dhat.rs` swaps the global
allocator to `dhat::Alloc` (dev-only, behind the OFF-by-default `dhat-heap`
feature) and attributes each allocation to its call site — the breakdown table
above is its output. Run it with
`RUSTFLAGS="-C debuginfo=1" cargo bench --bench alloc_dhat --features dhat-heap`.
The two benches agree on the total (~195 allocs/op), so `alloc_profile.rs`
remains the fast, dependency-light regression signal and `alloc_dhat.rs` the
attribution tool when the signal moves. What the pair proves is the
failed-target finding stated above: the steady-state turn is measurably far
from the zero-allocation DESIGN TARGET, at ~180–205 allocations per submitted
command, not the `0` the target names — and the dominant term is now
**attributed** (upstream `Hash32::to_hex`, ~57 %), not merely "structurally
plausible." The two `tokio`-driven sections (direct vs async) sit close enough
and swap ordering run to run that no reliable direction between them is
claimed. **This non-zero, attributed number is exactly the regression-signal
baseline docs/07 §4 asks for** — a future PR that changes it materially
(either direction) without an explanation is the signal to re-run
`alloc_dhat.rs` and see which call site moved.

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
- **HP-4 (market-maker requote) — now measured (`#050`, §12)**, no longer an
  omission: out of scope for `#020` (the persona-driven requote path landed
  `#47`, v0.5); `#050` adds the real `MarketMakerEngine::update_price`
  closed-/open-loop quantiles, the allocation profile's third section (§6),
  and the requote-isolation assertion (`tests/requote_isolation.rs`) — see
  §12 for the numbers, the 2-vs-4-worker scheduler-contention disclosure, and
  the isolation tolerance rationale.
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
  (`#035`), `benches/mm_requote_hdr.rs` (`#050`), `benches/alloc_profile.rs`,
  `benches/alloc_dhat.rs` (`#126` — the `dhat` call-stack-attributed heap
  profiler behind the OFF-by-default `dhat-heap` feature; the attribution tool
  §6's Root cause block uses), `benches/criterion_match_cost.rs` — the eight
  registered `[[bench]]` targets (`harness = false`), `Cargo.toml`.
- `benches/support/` — the reusable `bench-hdr` harness: `hdr.rs` (the
  `hdrhistogram` quantile report — unit-tested via `tests/bench_harness.rs`),
  `workload.rs` (the seeded, deterministic command-stream builder; `#050`
  adds `jitter_stream`, the price-tick generator `mm_requote_hdr` and
  `alloc_profile`'s third section share), `timing.rs` (the paired
  `TimingExecutor`/`TimingJournal` instrumentation seams, reused unchanged by
  `hp5_durable_append` against the durable journal), `openloop.rs` (the
  coordinated-omission-corrected load generator; `#043` adds
  `run_open_loop_pure` alongside the original `ActorHandle`-shaped
  `run_open_loop`, reused unchanged by `#050`), `fix_fixtures.rs` (`#043` —
  the fixed, golden-shaped `NewOrderSingle (D)` / `ExecutionReport (8)`
  fixtures HP-3 measures), `mm_workload.rs` (`#050` — the shared 10-contract
  persona-bound `MarketMakerEngine` fixture and `CountingSink`, reused by
  `mm_requote_hdr`, `alloc_profile`'s third section, and
  `tests/requote_isolation.rs` so the three never independently reconstruct,
  and possibly drift from, the same requote shape).
- `tests/bench_harness.rs` — 7 unit tests: the original 5 proving the
  histogram/quantile plumbing itself is correct against known distributions
  (uniform, constant, bimodal, empty, and a `report`-return-value consistency
  check), plus 2 added by `#043` proving the HP-3 `D`/`8` fixtures decode to
  themselves (never a silent reject-path measurement).
- `tests/requote_isolation.rs` (`#050`) — the requote-isolation assertion: a
  continuous, concurrent, real persona-driven requote sharing a client's own
  underlying actor mailbox must not inflate the client's HP-1-style p99
  beyond a documented, disclosed tolerance factor — see §12.3.
- `tests/docker_smoke.rs` (#027) — the Docker e2e smoke test that measures §9's
  cold-bring-up number and proves the one-order REST → WS-fill round-trip
  against the real container.
- `src/db/journal.rs` (`PgVenueJournal`, #029), `src/exchange/journal.rs`
  (`InMemoryVenueJournal`, `check_record_size`) — the two journal
  implementations §3.6 and §5 measure; neither changed in `#035` (a pure
  measurement issue, no `src/` change).
- `tests/load.rs` (`#054`, §14) — the v1.0 stability soak: flat memory, no
  sequence gaps, clean shutdown drains in-flight orders, restart-from-journal
  determinism, `#[ignore]` + `SOAK=1`-gated (never on the fast CI gate).
  Reuses `tests/conformance/` for the REST driver and `benches/support/hdr.rs`
  for the throughput/latency and latency-draw-fidelity quantile reports —
  neither reimplemented. `Makefile`'s `soak` target runs it (`--release`).

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
`0.3.1`, `ironfix-tagvalue` `0.3.1`, `ironfix-dictionary` `0.3.1`,
`ironfix-transport` `0.3.1`, `tokio-util` `0.7.18`, `bytes` `1.12.1`.

> **Re-measured on `ironfix` 0.3.1 (#140).** The numbers below were re-run after
> the 0.3.1 bump that retired the venue's redundant `BodyLength`/`CheckSum`
> pre-decode guards (the checks now live in the checked upstream decoder, which
> already ran them). Removing venue-side prechecks only *reduces* work on this
> path, so the decode `p99`/`p99.9` tail is if anything **tighter** than the
> 0.3.0 grounding (decode `p99` 875–916 ns vs the prior 1 084–2 251 ns); no
> regression. Setting the actual numeric HP-3 budget from this data remains
> #107's scope.

### 11.1 Closed-loop, 5 000 warmup + 100 000 measured ops (discarded warmup)

Three real, independent `cargo bench --bench hp3_fix_parse` runs on this
machine, identical configuration, disclosed side by side rather than
collapsed into one (the same "show the variance, don't hide it" convention
§3.1/§3.6 use):

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `hp3_decode_d_closed_loop` p50 | 750 ns | 750 ns | 750 ns |
| `hp3_decode_d_closed_loop` p99 | 875 ns | 916 ns | 875 ns |
| `hp3_decode_d_closed_loop` p99.9 | 1 000 ns | 1 000 ns | 1 000 ns |
| `hp3_decode_d_closed_loop` p99.99 | 8 879 ns | 4 711 ns | 4 251 ns |
| `hp3_decode_d_closed_loop` min / max | 666 / 42 047 ns | 666 / 17 423 ns | 666 / 20 543 ns |
| `hp3_encode_8_closed_loop` p50 | 458 ns | 458 ns | 458 ns |
| `hp3_encode_8_closed_loop` p99 | 542 ns | 542 ns | 542 ns |
| `hp3_encode_8_closed_loop` p99.9 | 625 ns | 625 ns | 625 ns |
| `hp3_encode_8_closed_loop` p99.99 | 2 543 ns | 2 833 ns | 2 791 ns |
| `hp3_encode_8_closed_loop` min / max | 375 / 14 167 ns | 375 / 14 375 ns | 375 / 14 751 ns |

**Interpretation — DESIGN TARGET grounding, not yet a stated number.**
docs/07 §3-HP3 has, until now, deliberately carried NO numeric budget for
HP-3 ("Budget stated once the FIX wire dialect is pinned … the bench lands
with v0.4, not before, so the number is grounded in the real message
schema"). This is that grounding measurement: across three independent runs,
decode `p50` is **sub-microsecond** (750 ns) with a sub-microsecond
`p99`/`p99.9` tail (875–1 000 ns), while encode is **sub-microsecond through
`p99.9`** (458–625 ns) — both one to two orders of magnitude inside even a
generous "sub-millisecond" reading, and
decode is consistently ~1.6× the cost of encode (a `FieldBag::collect` +
per-tag UTF-8/parse pass on untrusted bytes is real work the encoder's
straight-line field-write does not do). `p99.99` is the one quantile that
moves meaningfully run to run (decode: 4 251 ns – 8 879 ns; encode:
2 543 ns – 2 833 ns) — at 100 000 samples this quantile is resolved by roughly
the 10 slowest samples, so a single OS-scheduler preemption on this shared,
un-pinned developer laptop (background process, GC-style pause, whatever) can
move it by an order of magnitude without the underlying decode/encode code
doing anything different; this is disclosed exactly as HP-1's own p99.99
run-to-run variance is (§3.1, §3.5). **The numeric HP-3 DESIGN TARGET is now
stated in `docs/07-performance-budgets.md` §3-HP3, grounded in this data
(`#107`): decode p99 ≤ 5 µs and encode p99 ≤ 2 µs on dev-laptop-class
hardware** — comfortable headroom (roughly 2–4×) over the measured decode
`p99`/`p99.9` of 1.08–2.54 µs and encode of 0.58–0.75 µs, deliberately loose so
it flags a real order-of-magnitude regression on the untrusted parse path
without churning on the disclosed `p99.99` scheduler noise. This mirrors how
#020 refined HP-1's target only once real quantiles existed; measuring set the
grounding, `#107` set the target.

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
| `hp3_decode_d_open_loop_sojourn` p50 | 12 711 ns | 14 007 ns | 14 215 ns |
| `hp3_decode_d_open_loop_sojourn` p99 | 52 095 ns | 35 103 ns | 42 879 ns |
| `hp3_decode_d_open_loop_sojourn` p99.9 | 111 359 ns | 71 423 ns | 108 543 ns |
| `hp3_decode_d_open_loop_sojourn` p99.99 | 153 471 ns | 484 863 ns | 1 407 999 ns |
| `hp3_encode_8_open_loop_sojourn` p50 | 12 423 ns | 13 007 ns | 13 463 ns |
| `hp3_encode_8_open_loop_sojourn` p99 | 43 903 ns | 43 839 ns | 32 175 ns |
| `hp3_encode_8_open_loop_sojourn` p99.9 | 530 431 ns | 85 055 ns | 77 759 ns |
| `hp3_encode_8_open_loop_sojourn` p99.99 | 2 605 055 ns | 591 871 ns | 97 215 ns |

**Interpretation — an honest, disclosed harness-overhead effect, not a
decode/encode regression.** The open-loop sojourn p50 (~12.4–14.2 µs across
both spans) is **~17–30× the closed-loop p50** (§11.1: 750 ns decode,
458 ns encode) — a MUCH larger gap than HP-1's own open-loop section saw
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
single-sample artifact at 3 000 samples (encode run 1: 2.61 ms driven by one
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

## 12. HP-4 — market-maker requote, and the requote-isolation assertion (`#050`)

`benches/mm_requote_hdr.rs`: an underlying price update
([`MarketMakerEngine::update_price`](src/market_maker/engine.rs)) →
`requote_symbol` → the persona-driven edge calc (`Quoter::generate_quote`
inside `update_quote`, `#47`) → the generated `VenueCommand`s handed to a
`CommandSink`. `update_price` is the engine's only **public** entry point onto
this pipeline (`requote_symbol` / `update_quote` are private to
`src/market_maker/engine.rs`), so every number below times a REAL call to it —
never a stand-in for the `#47` persona-driven requote path. Registered chain
(`benches/support/mm_workload.rs::chain_symbols`): 5 strikes × {call, put} = 10
instruments, each bound to a shared persona, so a steady-state requote tick
enqueues up to 4 × 10 = 40 commands (20 cancels + 20 fresh adds; the first
tick is 20 adds only).

Two sections, mirroring `alloc_profile.rs`'s "direct vs round-trip" shape:
**engine-only** (`support::mm_workload::CountingSink` — no channel, no actor,
no `tokio` at all: the PURE requote-compute cost) and **mailbox-wired** (the
REAL `fauxchange::market_maker::ActorCommandSink`, wired to a REAL spawned
actor: the same computation plus a real bounded-channel `try_send`). Because
`update_price` never awaits the actor's own turn (the sink's `enqueue` is
`try_send`, non-blocking, fire-and-forget — `src/market_maker/sink.rs`'s
documented "off the client path"), matching (`MatchingExecutor::execute`)
never runs inside either timed span — it happens later, asynchronously, on
the actor's own task, off this bench entirely. **This is the structural
reason match time stays separated from venue overhead here**: there is no
fused number to decompose, because the production wiring itself decouples the
two, not a bench-side approximation. The mailbox-wired section's sink and
actor-mailbox capacity are sized (`total_ops × 4 × n_instruments + margin`) so
this run's total generated command count cannot exceed either — a simple
arithmetic guarantee of zero drops regardless of how fast the forwarder
happens to drain, isolating the enqueue's own added cost from the actor's
downstream processing rate (a different question the isolation assertion,
§12.3, exists to answer).

Run conditions are identical to §1 (same host, same toolchain, no Docker/
Postgres needed — this bench is pure in-process CPU work), same pinned
upstream crate versions as §1/§3.6, `hdrhistogram`/`criterion` `7.5.4`/`0.8.2`.

### 12.1 Closed-loop, 1 000 warmup + 5 000 measured ops (discarded warmup)

Three real, independent `cargo bench --bench mm_requote_hdr` runs on this
machine, identical configuration, disclosed side by side (the same "show the
variance, don't hide it" convention §3.1/§3.6/§11.1 use):

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `hp4_requote_engine_only_closed_loop` p50 | 115 519 ns | 117 503 ns | 120 767 ns |
| `hp4_requote_engine_only_closed_loop` p99 | 136 319 ns | 138 239 ns | 137 599 ns |
| `hp4_requote_engine_only_closed_loop` p99.9 | 149 759 ns | 145 663 ns | 145 407 ns |
| `hp4_requote_engine_only_closed_loop` p99.99 | 165 887 ns | 255 999 ns | 171 775 ns |
| `hp4_requote_engine_only_closed_loop` min / max | 95 104 / 165 887 ns | 95 360 / 255 999 ns | 97 152 / 171 775 ns |
| `hp4_requote_mailbox_closed_loop` p50 | 122 367 ns | 121 599 ns | 120 959 ns |
| `hp4_requote_mailbox_closed_loop` p99 | 142 719 ns | 142 591 ns | 142 079 ns |
| `hp4_requote_mailbox_closed_loop` p99.9 | 165 887 ns | 162 175 ns | 150 911 ns |
| `hp4_requote_mailbox_closed_loop` p99.99 | 187 007 ns | 175 103 ns | 180 863 ns |
| `hp4_requote_mailbox_closed_loop` min / max | 95 360 / 187 007 ns | 95 872 / 175 103 ns | 96 000 / 180 863 ns |

**Interpretation — DESIGN TARGET grounding, not yet a stated numeric budget.**
docs/07 §3-HP4 (mirroring HP-3's own precedent before `#043`) carried no
numeric budget prior to this measurement. Both sections land at p50
~116–123 µs and p99 ~136–143 µs across a full **10-contract chain requote**
(not one instrument) — comfortably inside even a strict "sub-millisecond"
reading, with real headroom. The mailbox-wired section is consistently ~5–7 µs
slower at p50 than engine-only (~5–6%) — a small, real, and expected delta:
`ActorCommandSink::enqueue`'s `try_send` onto a real (if drop-free-sized)
bounded channel is genuinely more work than `CountingSink`'s bare atomic
increment, but it is a small fraction of the requote's own compute cost (10
`Quoter::generate_quote` calls, each running a real `optionstratlib`
Black-Scholes evaluation, dominates both numbers). **Stating the actual
numeric HP-4 budget in `docs/07-performance-budgets.md` §3-HP4 is an
`architect` follow-up against this grounding data** — outside this bench's own
scope (measure and report, not set the design-doc target), the same precedent
`#043` set for HP-3.

### 12.2 Open-loop, coordinated-omission corrected, 3 000 ops at a ~2 ms intended interval

`support::openloop::run_open_loop_pure` — the same generator HP-3 uses for its
`decode`/`encode` spans (`update_price` has no bounded-mailbox/rejection
concept of its own; that concept lives downstream, inside the `CommandSink`).

**A disclosed tuning finding: 2 workers vs 4.** This bench's mailbox-wired
sections run a REAL `ActorCommandSink` forwarder + a REAL actor continuously
draining a (deliberately oversized, tens-of-thousands-of-commands) backlog in
the background, long after the open-loop dispatch window's own sends finish.
At `worker_threads(2)` (HP-1/HP-3's own default), this background drain
measurably starved the open-loop dispatch tasks for CPU — a real, reproduced
effect, not noise:

| | 2 workers, run 1 | 2 workers, run 2 |
|---|---|---|
| `hp4_requote_mailbox_open_loop_sojourn` p50 | 438 783 ns | 483 583 ns |
| `hp4_requote_mailbox_open_loop_sojourn` p99 | 1 671 167 ns | 1 709 055 ns |
| `hp4_requote_mailbox_open_loop_sojourn` p99.9 | 1 878 015 ns | 1 952 767 ns |
| `hp4_requote_engine_only_open_loop_sojourn` p50 (same run, for contrast) | 138 623 ns | 139 135 ns |
| `hp4_requote_engine_only_open_loop_sojourn` p99 (same run, for contrast) | 161 279 ns | 161 919 ns |

A 3–4× scheduler-contention effect from an **unrelated background task** (the
forwarder+actor still draining backlog), not the enqueue cost this section
exists to isolate. Raising the runtime to `worker_threads(4)` gives the
background drain room without starving the measured section — the numbers
below are all at 4 workers (see `benches/mm_requote_hdr.rs`'s doc comment for
the same disclosure, kept next to the code):

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `hp4_requote_engine_only_open_loop_sojourn` p50 | 142 463 ns | 142 719 ns | 142 591 ns |
| `hp4_requote_engine_only_open_loop_sojourn` p99 | 225 919 ns | 162 559 ns | 166 399 ns |
| `hp4_requote_engine_only_open_loop_sojourn` p99.9 | 880 127 ns | 217 855 ns | 231 679 ns |
| `hp4_requote_engine_only_open_loop_sojourn` p99.99 | 4 923 391 ns | 745 983 ns | 501 247 ns |
| `hp4_requote_mailbox_open_loop_sojourn` p50 | 149 119 ns | 148 863 ns | 148 607 ns |
| `hp4_requote_mailbox_open_loop_sojourn` p99 | 186 495 ns | 182 015 ns | 185 599 ns |
| `hp4_requote_mailbox_open_loop_sojourn` p99.9 | 214 143 ns | 213 887 ns | 214 271 ns |
| `hp4_requote_mailbox_open_loop_sojourn` p99.99 | 219 391 ns | 231 807 ns | 229 247 ns |

**Interpretation.** At 4 workers, both sections' p50 (~143–149 µs) sit close
to their closed-loop counterparts (§12.1: ~116–123 µs) — the open-loop
dispatch overhead (task spawn + schedule, mirroring HP-1's/HP-3's own
disclosed open-loop-vs-closed-loop gap) accounts for the difference, not
queueing (0 rejections are possible here — `update_price` has no rejection
path of its own). The **engine-only** section's p99.9/p99.99 are the more
volatile of the two, run to run (880 µs / 4.9 ms in run 1 vs 218–232 µs /
501–746 µs in runs 2–3) — at 3 000 samples this quantile is resolved by
roughly the 3 slowest samples, so a single OS-scheduler preemption on this
shared, un-pinned developer laptop can move it by an order of magnitude
without the underlying `update_price` call doing anything different, the same
disclosed pattern HP-1 (§3.5) and HP-3 (§11.2) both name at comparable sample
counts. The **mailbox-wired** section is, by contrast, tightly reproducible
across all three runs even at p99.99 (219–231 µs) — plausibly because the
real actor+forwarder machinery running alongside it keeps the runtime's
scheduler more uniformly busy (less idle-to-burst variance) than the
engine-only section's otherwise-idle 3 remaining workers; this is an
observation, not a measured causal claim.

### 12.3 The requote-isolation assertion (`tests/requote_isolation.rs`) — the v0.5 acceptance criterion that matters most

Proves a **continuous, concurrent, real** persona-driven market-maker requote
— sharing the SAME underlying's actor mailbox as a client's own orders, the
realistic (harder) case, not an easier cross-underlying setup — does not
inflate a client `AddOrder`'s HP-1-style p99 beyond a documented, bounded
tolerance. Method: two fresh `AppState`s (never sharing journal depth, the
same "fresh instance per measurement" convention `hp1_order_path.rs`'s own
open-loop section uses), each hosting one underlying (`BTC`), a 4 096-entry
mailbox (matching `hp1_order_path.rs`'s own bench convention, wider than the
venue's `DEFAULT_MAILBOX_CAPACITY = 1 024`): **quiet** (500 warmup + 3 000
measured client `AddOrder`/`CancelOrder` commands via `AppState::submit`, no
MM activity) vs **concurrent** (the IDENTICAL client workload, run while a
background task drives the SAME 10-contract persona-bound chain through
`update_price` every 20 ms — a realistic fast-moving-underlying cadence, not
an artificial flood — each tick's ≤40 commands routed through the REAL
`ActorCommandSink` onto the client's own actor).

Five real, independent `cargo test --test requote_isolation --release --
--nocapture` runs on this machine:

| Run | quiet p50 | quiet p99 | quiet p99.9 | concurrent p50 | concurrent p99 | concurrent p99.9 |
|---|---|---|---|---|---|---|
| 1 | 25 631 ns | 49 695 ns | 91 263 ns | 25 423 ns | 48 255 ns | 81 471 ns |
| 2 | 25 423 ns | 48 095 ns | 70 847 ns | 25 599 ns | 48 095 ns | 63 135 ns |
| 3 | 25 599 ns | 51 295 ns | 106 751 ns | 25 631 ns | 49 919 ns | 83 711 ns |
| 4 | 25 807 ns | 50 047 ns | 83 455 ns | 25 919 ns | 50 431 ns | 72 255 ns |
| 5 | 24 879 ns | 48 191 ns | 58 751 ns | 25 503 ns | 49 695 ns | 107 711 ns |

**Result: no measurable inflation at this configuration.** Across all 5 runs
the concurrent p99 is statistically indistinguishable from — sometimes even
below — the quiet p99 (ratio 0.96×–1.03×); the concurrent condition's p99.9
is noisier (as expected — a smaller quantile at 3 000 samples) but shows no
systematic direction either. **The test asserts `concurrent.p99 ≤ max(quiet.p99, 200 µs) × 6`**
(`TOLERANCE_FACTOR` + `FLOOR_NS`, `tests/requote_isolation.rs`). Be precise
about what that bound actually is: because the observed quiet p99 (~50 µs)
sits **below** the 200 µs floor, the floor dominates and the effective bound
is `200 µs × 6 = 1.2 ms` — roughly **24× the observed ~50 µs concurrent p99**,
not 6×. That is deliberately loose, and the threshold is NOT the isolation
evidence: the assertion's job is to catch **unbounded** inflation (a
stalled/starved client dragged toward the millisecond scale), not to police
ordinary FIFO-mailbox-sharing queueing, which is an expected structural
consequence of the single-writer actor (a client `AddOrder` and a concurrent
MM pair genuinely share one mailbox when they target the same underlying). The
floor exists so a near-zero quiet p99 can't make the ratio spuriously tight.
The real isolation evidence is the measured **~1.0× ratio across 5/5 runs**
(above) plus the 1 ms-cadence sensitivity diagnostic (below); the wide
threshold only backstops a catastrophic stall on this noisy, un-pinned laptop
(§3.1: a ~13% p99 swing on HP-1 with ZERO code change) without flaking (a stalled/starved client, not
ordinary FIFO-mailbox-sharing queueing — the venue's single-writer actor
means a client `AddOrder` and a concurrent MM `CancelOrder`/`AddOrder` pair
genuinely share ONE FIFO mailbox when they target the same underlying, so
SOME added queueing from shared traffic is an expected, structural
consequence of the architecture, not a bug the tolerance needs to reject at
1.0×). **A diagnostic-only run at a 10× more aggressive tick cadence (1 ms,
not committed)** confirms the test is not vacuously easy to pass: concurrent
p99 rose to ~76 µs against a quiet ~51 µs (~1.5×) — a real, meaningfully
measurable, but still well-bounded effect, evidence this assertion is
genuinely sensitive to load rather than trivially always green.

### 12.4 What this section does and does not prove

- **Proves**: `MarketMakerEngine::update_price` (the real, persona-driven,
  `#47` requote pipeline) requotes a realistic 10-contract chain at p50
  ~116–123 µs / p99 ~136–143 µs, comfortably inside a sub-millisecond reading,
  on this host, with match time structurally excluded (not merely subtracted)
  from the span; and that a continuous, concurrent, realistic-cadence requote
  sharing a client's own underlying mailbox produces no measurable client
  HP-1 p99 inflation at 5/5 real runs, asserted against a documented bound of
  `max(quiet.p99, 200 µs) × 6` — floor-dominated at the current ~50 µs quiet
  p99, so an effective ~1.2 ms ≈ 24× backstop against unbounded inflation, not
  a 6× regression policer (see §12.3; the ~1.0× measured ratio is the evidence,
  the threshold is only a catastrophe backstop).
- **Does not prove**: a production SLA (one un-pinned developer laptop, §1's
  own disclosed limitation); a stated HP-4 numeric budget in
  `docs/07-performance-budgets.md` (an `architect` follow-up against this
  data, §12.1); isolation under an arbitrarily aggressive requote cadence
  (§12.3's 1 ms diagnostic shows the effect is real and grows with load, just
  not yet at the 20 ms cadence this assertion commits to); or a
  call-stack-attributed allocation breakdown for the 232 allocs/op the
  allocation profile's third section reports (§6 — reduced from 343 by `#122`;
  still a process-wide count, not a per-call-site attribution. The `#126` `dhat`
  bench `benches/alloc_dhat.rs` now makes such a breakdown possible — it is
  applied to §6's actor-turn sections 1/2, not yet to this requote section;
  pointing it at the requote path is a small, tracked follow-up).
- **CI regression gate**: not armed by this change — `#050` is scope-limited
  to landing the measured baseline and the isolation assertion (a `cargo
  test`, so it runs as a normal, always-on correctness check, not a
  budget-breaching *bench* gate); the CI `bench-regression` gate over the
  `bench-hdr` quantiles arms before v1.0 (#053,
  [07 §6](docs/07-performance-budgets.md#6-ci-regression-gate)), same as
  every other hot path in this document.

## 13. CI regression-gate ceilings, re-verification, and the dry-run (`#053`)

`#053` arms `.github/workflows/bench-regression.yml`: a `bench-regression`
job (every push, every PR to `main`/`release/**`) plus a
`bench-regression-nightly` job (`schedule` + `workflow_dispatch`, full
default sample counts). Both run the SAME gate,
[`scripts/bench_regression_gate.py`](scripts/bench_regression_gate.py),
against the SAME documented ceiling table — see that script's module doc for
the full per-series numbers; this section records the derivation, the
re-verification runs the ceilings are grounded in, an honestly-disclosed
divergence this re-verification surfaced, and the synthetic-regression
dry-run proving the gate actually fails a real regression.

### 13.1 Why a ceiling, not a same-machine p99 delta

Every number in §§1-12 above was measured on ONE developer's Apple M4 Max
laptop (§1: "not a tuned bench rig," un-pinned). `fauxchange` has **no
self-hosted CI runner** — every job in every workflow in this repo
(`.github/workflows/ci.yml`, and this one) runs on GitHub-hosted
`ubuntu-24.04`: shared, no CPU-governor control, no guarantee of the same
physical host between runs. Comparing a CI run's measured p99 directly to
this laptop's measured p99 with a tight tolerance would be apples-to-oranges
— either spuriously failing on ordinary cross-machine noise, or (loosened
enough to avoid that) becoming meaningless. Two of the three approaches the
`#053` task considered were therefore rejected, explicitly, rather than
silently:

- **(a) Pin a self-hosted/fixed runner class.** Infeasible — this repo has no
  self-hosted runner today, and adding one is a paid-infrastructure decision
  outside `#053`'s scope (and `devops`'s "confirm first" list for paid CI-minute
  expansions applies equally to standing up new infrastructure).
- **(c) A first CI-runner-established baseline artifact.** Rejected: a first
  CI-runner baseline would itself be measured on the same noisy, shared,
  non-reproducible hardware this gate exists to be honest about — it does not
  solve the problem, it relocates it. `#053` is also explicitly not the place
  to *establish* a new baseline (that was `#035`/`#043`/`#050`'s job).
- **(b) A generous, documented absolute ceiling — CHOSEN.** Every gated
  series is compared against a ceiling derived from the **worst disclosed
  measured p99/p99.9** for that series across every `BENCH.md` run (§§3-12)
  *and* this section's own re-verification runs (§13.2), multiplied by a
  stated margin: **10x** once a series' measured latency is already at or
  above ~100 µs, or a **1 ms floor** for series still at low-microsecond
  scale (HP-3's decode/encode, whose measured p99 sits 400-2000x inside that
  floor — genuinely "an order of magnitude inside," not a tight bound dressed
  up as generous). This is measured-to-a-documented-ceiling, explicitly
  labelled a provisional DESIGN TARGET where no formal numeric budget exists
  yet in [docs/07-performance-budgets.md](docs/07-performance-budgets.md)
  (HP-3, HP-4) — **never** a same-machine p99 comparison presented as such.

**HP-1's own ceiling is deliberately wide for a second, disclosed reason.**
[Issue #91](https://github.com/joaquinbejar/fauxchange/issues/91) (the
in-memory journal's O(journal-depth) append-tail cost, §3's own diagnosed
driver of "just inside the ceiling, then past it") was named in §3.6 as
"scheduled to land before #53 arms the CI bench-regression gate over HP-1" —
**it has not landed as of this gate's arming.** A tight ceiling at the
sub-millisecond DESIGN TARGET would therefore be **born red**: BENCH.md's own
committed baseline already shows p99.9/p99.99 past 1 ms at full journal depth
(§3.1, §3.6) on the REFERENCE laptop, before any CI-runner slowdown is even
considered. The chosen ceiling (15 ms p99 / 25 ms p99.9) is generous enough
to stay green against that already-disclosed, tracked, unresolved issue while
still failing on a genuine multi-x regression — see §13.4 for proof it does.

### 13.2 Re-verification runs (2026-07-19, immediately before arming the gate)

Same machine, same toolchain, same OS, same `Cargo.lock` as §1 (`rustc
1.97.0`, macOS 26.5.2, Darwin 25.5.0, Apple M4 Max — re-confirmed via `uname
-a` / `sw_vers` / `rustc --version` before this run) — no dependency or
`src/`/`benches/` code changed since the `#050` baseline (`git diff
71df09f..HEAD -- src/ benches/` is empty for every file these benches
exercise; confirmed before drawing any conclusion below).

| Bench | Config | Flagship p99 | Flagship p99.9 |
|---|---|---|---|
| HP-1 (`hp1_full_turn_closed_loop`) | `HP1_WARMUP_OPS=2000 HP1_MEASURED_OPS=20000` (reduced — journal depth ~22k, not the full-scale ~105k §3.1 uses) | 220,031 ns | 303,615 ns |
| HP-2 (flatness verdict) | `HP2_WARMUP_OPS=1000 HP2_MEASURED_OPS=10000` | worst \|Δp99\| vs N=1: **13.3%** | PASS (tolerance 15%) |
| HP-3 (`hp3_decode_d_closed_loop` / `hp3_encode_8_closed_loop`) | full default (`5000`/`100000`) | 1,125 ns / 584 ns | 1,250 ns / 1,375 ns |
| HP-4 (`hp4_requote_engine_only_closed_loop` / `hp4_requote_mailbox_closed_loop`) | full default (`1000`/`5000`) | 160,767 ns / 156,671 ns | 216,447 ns / 195,583 ns |
| HP-5 (`hp5_persistent_full_turn_closed_loop`) | `HP5_WARMUP_OPS=50 HP5_MEASURED_OPS=300` (reduced; real ephemeral `postgres:18-alpine` via `testcontainers`) | 800,767 ns | 977,407 ns |

**Interpretation.** HP-2/HP-3/HP-4/HP-5 all land within the same order of
magnitude as their §4/§11/§12/§5 committed figures — no unexplained
divergence, the mechanism and magnitude both still hold. HP-1's reduced-scale
number (p99 220 µs at ~22k records) is consistent with §3.4's small-N
reference (p99 33 µs at ~2.2k records) and far below the full-scale §3.1
figure (p99 932 µs-1.5 ms at ~105k records) — exactly the journal-depth
dependence §3 already diagnosed, reconfirmed, not contradicted.

### 13.3 A disclosed divergence — now RESOLVED (`#126`): §6's stale, pre-`#34` allocation baseline, carried over unrefreshed

> **RESOLVED 2026-07-22 (`#126`).** When `#053` armed the gate this divergence
> was real and unexplained; the root cause is now found and §6's baseline
> refreshed — see §6's **Root cause** block. In one line: §6's 60–80 allocs/op
> was measured at `ab756ab` (pre-`#34`, when `InMemoryVenueJournal::append` did
> no serialization) and carried over unchanged; `#34`'s `check_record_size`
> (`serde_json::to_string` per append, serializing the `owner: Hash32` through
> the upstream per-byte `Hash32::to_hex`) added ~111 allocs/op afterward and was
> never re-measured. Today's ~180–205 is the honest steady-state; the 60–80 was
> pre-`#34` code. The §13.2 conclusion that `git diff … -- benches/*` was empty
> was correct but **scoped too narrowly** — it did not diff `src/exchange/`, so
> it missed `#34`'s append-path change (the actual cause). The runs and gate
> rationale below are kept as the honest record of the state when the gate was
> armed; the interpretation is corrected inline.

This was the one `#053` re-verification result that did **not** land where §6's
committed baseline said it should, and it was reported honestly rather than
quietly reconciled or overwritten.

Five independent `cargo bench --bench alloc_profile` runs today (default
`5000`/`50000` ops except where noted), same machine/toolchain/`Cargo.lock`
as §13.2:

| Run | `UnderlyingActor::handle` direct (allocs/op) | `ActorHandle::submit` (allocs/op) | `MarketMakerEngine::update_price` (allocs/op) |
|---|---|---|---|
| 1 (default) | 180.355 | 193.426 | 343.000 |
| 2 (default) | 197.098 | — | — |
| 3 (default) | 197.877 | — | — |
| 4 (default) | 202.160 | — | — |
| 5 (`ALLOC_WARMUP_OPS=100000`, larger warmup) | 189.745 | — | — |
| 6 (default) | 197.487 | 199.656 | 343.000 |
| 7 (smoke-scale, `2000`/`10000`) | 181.489 | 193.775 | 343.000 |

**§6's committed baseline (recorded 2026-07-18, three runs): direct
62.577/79.710/77.374, submit 61.630/79.153/82.657.** Today's seven runs
(direct: 180.355-202.160, tightly clustered around ~190; submit:
193.426-199.656) sit **roughly 2.3-2.6x above §6's own highest disclosed
figure**, with NO code, dependency, or `Cargo.lock` change between the two
measurement sessions (`git diff 71df09f..HEAD -- benches/alloc_profile.rs
benches/support/workload.rs` shows only `#050`'s purely-additive section-3
insertion, verified in §13.2). A larger warmup (run 5) did not converge the
number down toward §6's figure, ruling out "insufficient warmup / still
mid-growth" as the explanation. The `MarketMakerEngine::update_price` section
is, by contrast, **exactly reproducible**: `343.000` allocs/op on every one
of these seven runs, matching §6's own three historical runs exactly — ten
total measurements, zero variance, on the SAME machine as the two sections
above that show a real, unexplained ~2.3-2.6x shift.

**Root cause (corrected 2026-07-22, `#126`).** The candidate causes this
paragraph originally listed as "unattributed" — a `DashMap` hasher-spread
effect larger than anticipated, a `libmalloc` memory-pressure interaction,
"a subtlety this investigation did not find" — were all **wrong or moot**. The
actual cause is a **stale carried-over baseline over a genuine post-baseline
allocation increase**, proven with `dhat` (`benches/alloc_dhat.rs`, added by
`#126`) and a scaling sweep (§6's Root cause block):

- The one false premise was "**NO code change between the two measurement
  sessions**." That was inferred from `git diff … -- benches/alloc_profile.rs
  benches/support/workload.rs` — a diff scoped to the **bench** files only. It
  did **not** diff `src/exchange/`. §6's 60–80 was measured at `ab756ab`
  (topologically the oldest bench commit, pre-`#34`), and `#34` (`9e5a537`)
  later added `check_record_size` to the append path. So the order-path code
  DID change between §6's measurement and `#053`'s re-verification — the diff
  just did not look where the change was.
- The `DashMap`-hasher hypothesis was right about the *shape* of the true
  ±6 % run-to-run band (180.4–203.3) but not the *magnitude* of the divergence;
  the ~150 % gap is the pre-`#34`-vs-current code difference, not hasher noise.
- `libmalloc` memory pressure is ruled out: `dhat` (a call-stack allocation
  counter, not a latency tool) independently reproduces ~195 allocs/op, and
  allocation *count* is a pure function of program logic + hasher seed — the
  count did not change because of system state, it changed because `#34`'s
  serialization runs on every append now.

Attribution: ~111 of the ~190 allocs/op (~57 %) is upstream
`pricelevel::Hash32::to_hex` (one `format!("{:02x}")` per byte, ×32),
reached through `check_record_size`'s `serde_json::to_string` of each journal
record's `owner: Hash32`. **[Issue #126](https://github.com/joaquinbejar/fauxchange/issues/126)
is resolved by this finding**; `architect`'s `#053` review correctly called it
a ship-with-follow-up, and the follow-up (`#165`) is now a *targeted* alloc
reduction with a known offending call site, not an open mystery.

**Why the gate's ceiling uses the freshly-observed numbers, not §6's stale
figure.** A "no regression over the committed §6 baseline" gate taken
literally would be **born red today**, on this exact machine, with zero code
change — the same "born red" problem §13.1 names for HP-1 and #91. The
allocation ceilings in `scripts/bench_regression_gate.py`
(`ALLOC_CEILINGS_PER_OP`) are therefore set from THIS section's freshly
re-verified numbers with real margin (450 allocs/op for the direct section,
~2.2x the highest of the seven fresh runs; 500 for the submit section,
similarly), so the gate is honest about current, reproducible reality rather
than gating against a number that does not reproduce on this exact host today.
These ceilings are grounded in the true ~180–205 steady-state, so `#126`'s
resolution requires **no ceiling change** — only the rationale strings in
`scripts/bench_regression_gate.py` are updated to drop the "pending-#126"
caveat now that the baseline is refreshed and root-caused. **§6's committed
table above has now been CORRECTED** (2026-07-22): the earlier
`77.374`/`82.657` figures were pre-`#34` code carried over stale, and §6 now
records the re-measured ~180–205 cluster with the full reconciliation — this
paragraph's original "left UNCHANGED … neither is known yet" no longer holds.
The `MarketMakerEngine::update_price` ceiling (343) stays tight, matching that
section's genuine, ten-run, zero-variance reproducibility.

> **`#122` update (2026-07-22).** The seven-run table above records the
> pre-`#122` state (MM section `343.000`, correct for 2026-07-19). `#122` has
> since reduced the `MarketMakerEngine::update_price` section to an equally
> reproducible **`232.000` allocs/op / `3 513.3` bytes/op** (see §6's `#122`
> note for the mechanism and the honest re-scope of the remainder). This stays
> comfortably under the existing `350` allocs/op gate ceiling, so
> `scripts/bench_regression_gate.py` remains green with more margin and this
> change does not require re-arming it. Tightening that ceiling toward the new
> `232` baseline is a `devops`/`architect` follow-up (a ceiling that already
> passes is not a regression), not part of `#122`.

### 13.4 The synthetic-regression dry-run

Proving the gate actually fails, per the milestone's acceptance criterion —
never asserted without evidence:

1. **A real, injected latency regression.** A single `std::thread::sleep(Duration::from_millis(20))`
   was added, temporarily, inside `benches/hp1_order_path.rs`'s closed-loop
   measurement loop (never `src/` — the venue code itself was never touched),
   clearly commented as a `#053` dry-run injection. A real
   `cargo bench --bench hp1_order_path` run (`HP1_WARMUP_OPS=50
   HP1_MEASURED_OPS=200`) against the modified binary measured
   `hp1_full_turn_closed_loop` p99 = **25,706,495 ns (25.7 ms)**, p99.9 =
   **26,214,399 ns (26.2 ms)** — both comfortably past the 15 ms / 25 ms
   ceiling. `python3 scripts/bench_regression_gate.py` against this REAL
   (not fabricated) output printed:
   ```
   FAIL — 2 violation(s):
     - 'hp1_full_turn_closed_loop' p99 25,706,495 ns exceeds the documented ceiling 15,000,000 ns
     - 'hp1_full_turn_closed_loop' p99.9 26,214,399 ns exceeds the documented ceiling 25,000,000 ns
   ```
   exit status **1**. The injection was then reverted (`git diff --stat --
   benches/hp1_order_path.rs` is empty after the revert — confirmed, never
   committed).
2. **Synthetic latency/allocation/flatness breaches (parser + comparator
   coverage).** A hand-built, clearly-synthetic log (never presented as a
   real bench run) with an inflated `hp1_full_turn_closed_loop` p99/p99.9, an
   inflated `UnderlyingActor::handle` allocs/op, a non-flat `hp2_fanout_n1000`
   p99, and several gated series simply OMITTED (simulating a bench crash)
   was fed to the same script, producing 11 distinct violations covering
   every branch of the gate logic (latency ceiling, alloc ceiling, missing
   -gated-series, and fan-out flatness) — exit status **1**.
3. **A clean baseline passes.** The REAL, un-injected §13.2 logs (HP-1
   through HP-5 plus `alloc_profile`) were run through the same script:
   `PASS — every gated series is within its documented ceiling.`, exit status
   **0**.

### 13.5 Noise margin and the baseline-update procedure

- **The noise margin is the ceiling's own multiplier**, not a separate knob:
  10x the worst disclosed measured p99/p99.9 for series already above ~100 µs,
  or a 1 ms floor for series still at low-microsecond scale (§13.1). This is
  wide enough to absorb (a) the M4-Max-laptop-vs-GitHub-hosted-runner
  hardware gap, (b) #91's own disclosed, unresolved HP-1 tail regression, and
  (c) §13.3's disclosed, unresolved allocation-count divergence — while still
  failing a genuine multi-x regression (§13.4 proves this against a REAL 20ms
  injection, not merely a fabricated one).
- **A legitimate budget change is a reviewed `BENCH.md` commit, never a
  silent gate edit.** If a future PR intentionally changes a hot path's
  performance characteristics (a deliberate trade-off, a new dependency, an
  accepted regression with a documented reason), the correct procedure is:
  1. Re-run the affected `bench-hdr` bench(es) for real, paste the output
     into a new dated `BENCH.md` entry (never edit the historical §§3-13
     tables in place — add, disclose, interpret, per this repo's existing
     convention throughout §§3-12).
  2. Write an interpretation block: what moved, why (grounded in code
     actually read, not "probably jitter"), and whether the new ceiling
     still holds or needs a reviewed change.
  3. If the ceiling itself needs to move, edit
     `scripts/bench_regression_gate.py`'s `LATENCY_CEILINGS_NS` /
     `ALLOC_CEILINGS_PER_OP` tables in the SAME PR as the `BENCH.md` entry,
     with the module doc comment updated to point at the new dated section —
     never a bare number change with no `BENCH.md` paper trail. A reviewer
     rejects a ceiling-only diff with no accompanying `BENCH.md` commit.
- **What this gate does NOT prove**: a production SLA on GitHub-hosted
  runners (the ceilings are deliberately generous, not tuned); that #91 or
  §13.3's divergence (#126) is resolved (both remain open, named follow-ups);
  or that the smoke-scale per-push job reaches the same journal depth /
  sample count the ceilings' margin was sized against (§13.2's reduced-scale
  HP-1 number is far below its own ceiling for a different reason than
  "healthy" — see the nightly-job rationale in
  `.github/workflows/bench-regression.yml`'s header comment).

### 13.6 HP-2 fan-out flatness is gated at nightly full sample, report-only at per-PR smoke sample

Architect review (#053) flagged this as the one gate-design point that
mattered most to fix before landing: §13.2's own re-verification measured
worst |Δp99| across the N sweep = **13.3% at 10,000 measured ops** — only
**1.7 percentage points** under the 15% `FANOUT_FLATNESS_TOLERANCE_PCT`
tolerance (`benches/hp2_ws_fanout.rs`, reused unchanged by
`scripts/bench_regression_gate.py`). The per-PR `bench-regression` job runs
HP-2 at a smaller, faster `HP2_MEASURED_OPS=3000` — fewer tail samples per N,
so the SAME underlying noise that produced 13.3% at 10,000 ops could plausibly
cross 15% at 3,000 ops on a PR that changed nothing about fan-out at all. A
gate that fails an unrelated PR on ordinary sampling noise gets overridden or
disabled by frustrated reviewers — which defeats the gate more thoroughly
than not gating that one check at that one sample scale.

**The fix, chosen from the options architect named:** gate flatness ONLY in
the `bench-regression-nightly` job (full default `HP2_MEASURED_OPS=30000`,
the SAME sample scale §4's own flatness finding was measured at — 3.7%
worst |Δp99|, well inside tolerance). The per-PR `bench-regression` job still
PARSES and PRINTS the flatness percentage every run (never silently dropped —
visible in both the job log and the uploaded artifact, under "reported, NOT
gated"), it just does not fail the build on a breach at that sample scale.
Mechanism: `scripts/bench_regression_gate.py` reads the
`BENCH_REGRESSION_GATE_FLATNESS` environment variable (`"1"` = gate, anything
else = report-only, default report-only) — `bench-regression.yml` sets it
explicitly to `"0"` in the smoke job and `"1"` in the nightly job, so the
distinction is a visible, reviewable, one-line diff in the workflow file, not
a silent loosening buried in the script.

The two OTHER options architect offered (raise the smoke job's
`HP2_MEASURED_OPS` until flatness is stable at PR scale; or widen the
smoke-scale tolerance specifically) were considered and set aside: raising
`HP2_MEASURED_OPS` enough to reliably clear the 1.7-point margin would erode
most of the per-PR job's speed advantage over the nightly job for exactly the
one series least likely to need per-PR granularity (a fan-out regression is
architecturally a whole-class-of-change issue — `WsFanOut`/
`OrderbookSubscriptionManager` — not a narrow one-line diff a fast per-PR
signal is uniquely suited to catch minutes sooner); a SEPARATE, WIDER
smoke-scale tolerance constant would work but adds a second tolerance number
to keep synchronized with the bench's own `FLATNESS_TOLERANCE_PCT` and BENCH.md
§4's interpretation, for a check that is, by construction, only ever
authoritative at full sample anyway. Gating once, at the sample scale the
15% tolerance was actually calibrated against, is the more honest of the
three.

## 14. Stability soak — flat memory, no sequence gaps, clean shutdown, restart-from-journal (`#054`, v1.0)

`tests/load.rs` (`#[ignore]` + `SOAK=1` — never on the fast CI gate,
[docs/TESTING.md §8](docs/TESTING.md#8-load--soak)) drives a bounded,
sustained order-flow window through the real REST router (`tests/conformance/`
— the module `src/conformance/harness.rs`'s own doc comment names as its
"library-side, production-grade sibling"; the milestone's named
`src/conformance/harness.rs`/`VenueServer` is `mod harness;`, private to
`fauxchange::conformance`, unreachable from an external `tests/*.rs` crate —
see the test file's own module docs for the full disclosure) and asserts the
four v1.0 stability properties. This is a stability/duration check, not a
throughput ceiling measurement — peak matching throughput stays HP-1's job
(§3).

### 14.1 Run conditions

| Item | Value |
|---|---|
| Machine | Apple M4 Max developer laptop (macOS, Darwin 25.5.0, `arm64`) — same class as §1, not a tuned bench rig |
| Build | `cargo test` (debug/`unoptimized + debuginfo`) — the EXACT documented acceptance-criterion invocation, `SOAK=1 cargo test --test load -- --ignored` (no `--release`) |
| Invocation | `SOAK=1 cargo test --test load --all-features -- --ignored --nocapture` (`make soak` runs the `--release` variant for a faster operator loop; both pass) |
| Window (`SOAK_SECS`) | `60` s (default) |
| Target rate (`SOAK_RATE`) | `40.0` rounds/sec (80 orders/sec) — deliberately modest, see rationale below and the test file's own module docs |
| Fixture | `BTC-20240329-50000-C`, `trader-1` (maker, GTC sell) / `trader-2` (taker, market buy), `tests/conformance/mod.rs::venue(AMPLE_RATE_LIMIT)` |
| RSS read mechanism | `ps -o rss= -p <pid>` (POSIX; verified on this Darwin host) — see §14.2's disclosure |

### 14.2 The four properties — real measured results

A real `SOAK=1 cargo test --test load --all-features -- --ignored --nocapture`
run, the exact documented default invocation, passed clean in `61.10s`
(re-verification run, after the §14.6 Property-3 fix):

| Property | Result |
|---|---|
| 1. Flat RSS | Early-window median **28 480 KB**, late-window median **36 752 KB** (Δ = 8 272 KB), documented margin **20 480 KB** (`max(20% relative, 20 MB absolute)`) — **within margin, PASSED**. Journal footprint lower bound at window end: 8 680 records × 280 B (`size_of::<JournalRecord>`) ≈ 2 373 KB — the disclosed, EXPECTED, volume-proportional component (`InMemoryVenueJournal` retains every record for the process lifetime by design; not a leak), a small fraction of both the observed Δ and the margin. |
| 2. No sequence gaps | `underlying_sequence`: **4 340** distinct values, `0..=4339` contiguous, **zero gaps** (read from the live `AppState::journal_snapshot`). `instrument_sequence` (WS `orderbook_delta`, `BTC-20240329-50000-C`): **4 340** messages observed, strictly consecutive, **zero duplicates, zero gaps, zero broadcast-lag skips**. |
| 3. Clean shutdown drains in-flight orders | A dedicated actor (`spawn_matching_actor`, bypassing `AppState` — see §14.6 for why) took a 60-submission concurrent burst against a deliberately small 4-slot bounded mailbox: **5/60 accepted, 55 rate-limited (fail-fast, not lost), 0 orphaned**. Every `ActorHandle` clone was dropped, the actor's own `JoinHandle` was GENUINELY AWAITED to completion (real proof the `run()` loop drained and exited, not an inference), and only then was the SURVIVING `SharedJournal` (an `Arc<Mutex<...>>` clone held independently of the actor's lifetime) read back to confirm every accepted receipt's `underlying_sequence` has a committed `VenueEvent`. (`SOAK_SECS=15`/`60` re-runs both landed 5/60 accepted, 0 lost — the accept/reject SPLIT is a scheduling artifact of the deliberately tiny 4-slot mailbox racing 60 concurrent submitters, not something this property asserts a fixed ratio on.) |
| 4. Restart-from-journal determinism | **4 340** exported events re-executed through `fauxchange::simulation::replay_bundle` (recovery-as-re-execution, ADR-0006) to values EQUAL to the stored oracle (positive case) — with the live venue already dropped before the replay call. A corrupted stored event at `underlying_sequence 0` correctly HALTED recovery with the typed `ReplayError::JournalCorruption { underlying: "BTC", sequence: 0 }` (negative case) — never a silent divergent resume. |

### 14.3 Throughput + latency (real measurements, `bench-hdr`/`hdrhistogram`)

REST round-trip latency (maker sell + taker market-buy, `benches/support/hdr.rs`
reused verbatim), over 4 340 samples across the 60 s window:

| Quantile | Value |
|---|---|
| p50 | 3 993 599 ns |
| p99 | 9 502 719 ns |
| p99.9 | 12 140 543 ns |
| p99.99 | 20 922 367 ns |
| min / max | 1 833 984 ns / 20 922 367 ns |

2 170 rounds completed (4 340 commands) — **36.2 rounds/sec achieved** against
a 40.0/sec target (an `axum::Router` `tower::ServiceExt::oneshot` dispatch
through the real auth/handler/actor stack per call, debug build — NOT HP-1's
dedicated hot-path measurement; the gap to target is expected debug-build +
`oneshot`-dispatch overhead, not a regression signal this soak asserts
against, and the run-to-run tail variance vs the first recorded run — e.g.
p99.99 20.9ms here vs 33.3ms originally — is this shared, un-pinned
developer laptop's own ordinary scheduler noise, §1's own disclosed
characteristic, not a regression). A `--release` re-run at the same 60 s
window/40 rounds-per-sec target showed the same four properties holding (all
four PASSED), confirming the result is not a debug-build artifact.

### 14.4 Injected-latency fidelity — honest disclosure

`src/microstructure/latency.rs`'s own module docs are explicit: the **live
gateway-edge application** of a drawn `LatencyOffset` onto real request
arrival order is deferred to
[#111](https://github.com/joaquinbejar/fauxchange/issues/111) — today
`LatencyConfig` is a config + seeded-draw surface only, not yet wired onto
live traffic. So §14.3's REST latency above carries **zero** injected delay.
What this soak measures instead is the seeded draw's OWN fidelity against its
configured distribution — the only latency mechanism that exists today —
2 000 samples per model:

| Model | Configured | Observed (p50 / min / max) | Result |
|---|---|---|---|
| `Fixed{us:2000}` | exact 2 000 µs every draw | 2 000 895 / 1 999 872 / 2 000 895 ns | Exact at the source; the reported spread is `hdrhistogram`'s own 3-sig-fig bucket resolution (≤ 0.06%), not draw jitter — PASSED within a disclosed 0.5% tolerance |
| `Uniform{min:1000,max:5000}` | band `[1 000, 5 000]` µs | p50 2 973 695 / min 999 936 / max 5 001 215 ns | Within the configured band (± the same bucket-resolution artifact at the edges); p50 near the analytic midpoint — PASSED |
| `Lognormal{median:1500,sigma:0.5}` | median 1 500 µs | p50 1 494 015 ns | Within a disclosed 50% tolerance of the configured median (heavy-tailed, 2 000 samples) — PASSED |

### 14.5 Interpretation

All four v1.0 stability properties held over the default documented window,
measured both at debug build (the literal acceptance-criterion invocation)
and `--release` (`make soak`), and at several window sizes (10 s / 15 s / 20 s
/ 60 s) during iteration. The soak is genuinely exercising sustained flow, not
a single request: 2 170-2 916 rounds (4 340-8 696 commands) depending on the
window, through the real REST gateway, auth middleware, and the sequenced
actor path, with concurrent RSS sampling and live WS broadcast observation
running the whole time.

**Platform limitation, disclosed as designed:** the RSS read shells out to
the POSIX `ps -o rss= -p <pid>` utility rather than `/proc/self/status`
(Linux-only) or `getrusage`'s `ru_maxrss` (a monotonic peak, structurally
unusable for a flatness trend) via a new `libc` dependency this crate does
not otherwise need. `ps` is present on both macOS and Linux CI runners; a
host with neither (a `scratch`/`distroless` container, Windows) degrades to
zero RSS samples and the test prints a `WARNING:` line rather than failing
the whole soak on a missing tool — this did not occur on this run (120 real
samples collected over the 60 s window).

**DESIGN TARGETs, not achieved SLOs:** the `max(20% relative, 20 MB
absolute)` RSS flatness margin and the soak's own throughput/latency numbers
are this soak's own measured evidence for [docs/07-performance-budgets.md
§4](docs/07-performance-budgets.md#4-throughput-scaling-and-isolation-budgets)'s
"flat memory under sustained order flow" DESIGN TARGET — met on this run, on
this host, at this volume; re-measure (never re-estimate) if the sustained
volume, the journal's retention policy, or the mailbox capacity change
materially. Peak matching throughput remains HP-1's (§3) DESIGN TARGET, not
this soak's.

### 14.6 Architect review fix — Property 3 now genuinely exercises the drain

Architect review flagged that the first cut of Property 3 overstated what it
tested: it kept a second `Arc<AppState>` clone (`verifier`) alive across the
whole burst + assertion sequence purely to read the journal back afterward —
which meant the actor's mailbox never actually reached zero senders during
the test, so the "clean shutdown drains in-flight orders" title, and a
comment claiming "the mailbox only closes once every one of them has
resolved," were not literally exercised (`verifier` kept it open past that
point).

**Investigated first, per the review's own instruction, before picking a
fix.** `AppState` itself has **no awaitable drain hook**:
`AppState::new` spawns each per-underlying actor via
`spawn_matching_actor_with_registry_and_index` and immediately `drop(join)`s
the returned `JoinHandle` (`src/state.rs`) — the task is detached by
construction, and nothing in `AppState`'s public surface can await its
completion. But the lower-level primitive `AppState` itself calls,
`spawn_matching_actor` (`fauxchange::exchange`, already `pub`, already used
directly by several other tests — `tests/order_path.rs`,
`tests/simulation.rs`), DOES return `(ActorHandle, JoinHandle<()>)` — a real,
awaitable completion signal exists one layer below `AppState`.

**Fix taken: the PREFERRED path — genuinely exercise the drain**, not a
retitle. `run_shutdown_drain_check` now builds its own actor directly on
`spawn_matching_actor`, over a test-local `SharedJournal`
(`tests/load.rs`) — a `VenueJournal` implementation whose storage is an
`Arc<Mutex<Vec<JournalRecord>>>`, so a clone taken BEFORE the journal is
moved into the actor **survives** the actor/handle/task, unlike the
actor-owned `InMemoryVenueJournal`. The check fires the 60-submission burst
through cloned `ActorHandle`s, drops every handle (including its own),
awaits every submission to a definitive `Ok(Receipt)` / `Err(RateLimited)`,
THEN genuinely **awaits the actor's own `JoinHandle`** — proof the `run()`
receive loop actually drained its backlog and returned — and only after that
real completion signal reads the surviving `SharedJournal` to confirm every
accepted receipt's event is durably present. The title is now literally
true. §14.2's Property 3 row and this document's §14 numbers were
re-measured (not carried over) against the fixed code.

Two cosmetic nits from the same review were also applied:
`collect_orderbook_deltas`'s `Instant::saturating_duration_since` now carries
a one-line note distinguishing it from the overflow-hiding integer/`Decimal`
arithmetic `rules/global_rules.md` and this file's own `bounded` helper are
about (a monotonic-clock timeout-budget clamp, the same idiom
`src/conformance/harness.rs` already uses, is a different thing); and
`capture_mid_run_bundle`'s doc comment now clarifies "mid-run" means "the
venue was never stopped/drained before this capture," not "while the load
loop was still actively looping" (that loop has already returned by the time
this function runs — the venue's own continuous serving state, proven by the
post-export `CancelOrder` liveness probe, is what "mid-run" refers to).
