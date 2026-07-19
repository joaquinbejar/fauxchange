#!/usr/bin/env python3
"""bench_regression_gate.py — the #053 `bench-regression` CI pass/fail gate.

Parses the plain-text `bench-hdr` output the `benches/*.rs` binaries already
print (`benches/support/hdr.rs::report`, `benches/alloc_profile.rs`'s
`report_window`) and compares the measured quantiles / allocation counts
against a **generous, documented ceiling** — never a same-machine p99
comparison against the M4-Max-laptop-recorded `BENCH.md` baseline.

Why a ceiling, not a laptop-vs-runner delta (docs/07-performance-budgets.md
§6, BENCH.md's new "CI regression-gate ceilings" section): `BENCH.md`'s
committed HP-1..HP-5 baselines were measured on one developer's Apple M4 Max
laptop, un-pinned, disclosed in BENCH.md §1. GitHub-hosted runners are
different, generally slower, shared, noisier hardware with no CPU-governor
control and no guarantee of the same physical host between runs. Comparing a
CI-runner's measured p99 directly to the laptop's measured p99 with a tight
tolerance would be apples-to-oranges — it would either spuriously fail on
ordinary cross-machine noise or (if loosened enough to avoid that) become
meaningless. Every ceiling below is derived from the WORST disclosed
measured p99/p99.9 across every BENCH.md run (and this gate's own
`#053` re-verification run, `BENCH.md` §13) for that series, multiplied by a
documented margin (10x for series already at or above ~100 us, a 1 ms floor
for series still at low-microsecond scale) — generous enough to absorb
cross-machine noise and the known, still-open HP-1 append-tail issue
(#91, not yet landed when this gate was armed) without being vacuous: it
still fails a genuine multi-x regression. See `BENCH.md` §13 for the full
derivation and the noise-margin / baseline-update procedure.

This script is deliberately dependency-free (Python 3 stdlib only) so no new
Cargo or pip dependency is needed to arm the gate — `devops` does not add
Rust dependencies, and a CI-only Python script needs none either.

Usage:
    python3 scripts/bench_regression_gate.py <bench-log-file> [<bench-log-file> ...]

Environment:
    BENCH_REGRESSION_GATE_FLATNESS=1  Gate on the HP-2 fan-out flatness sweep
        (fail the build if it breaches tolerance). Unset/`0` (the default,
        used by the reduced-sample per-PR `bench-regression` job): the
        flatness percentage is still COMPUTED and PRINTED every run, but a
        breach does not fail the build. BENCH.md §13.2 measured worst
        |delta p99| = 13.3% at 10,000 ops — only 1.7 percentage points under
        the 15% tolerance — so gating this at the smaller per-PR sample size
        (3,000 ops, fewer tail samples) risked a spurious >15% on an
        unrelated PR (a gate that cries wolf gets overridden, which is worse
        than not gating it at all at that scale). The `bench-regression-nightly`
        job (full default sample, matching BENCH.md's own §4 methodology)
        sets this to `1` and genuinely gates flatness. See BENCH.md §13.6.

Exit status: 0 if every gated series (latency ceilings, allocation ceilings,
and — only when `BENCH_REGRESSION_GATE_FLATNESS=1` — HP-2 fan-out flatness)
is within bounds; 1 on any breach (including a gated series that never
appears in the provided logs — a silent bench crash or renamed report string
must never pass the gate vacuously).
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# The ceiling table — see BENCH.md §13 for the derivation of every number.
# ---------------------------------------------------------------------------

# Latency ceilings, in nanoseconds, keyed by the exact `report()` name each
# bench prints (`--- <name> ---`). Only series listed here are GATED; every
# other parsed series (upstream match cost, write-ahead append sub-spans,
# open-loop sojourn times) is still printed in the summary — "reported, not
# gated" — so match time stays visibly separate from venue overhead
# (docs/07 §7) without silently disappearing from the artifact.
LATENCY_CEILINGS_NS: dict[str, dict[str, int]] = {
    # HP-1 flagship, in-memory. Worst disclosed measured (BENCH.md §3.1/§3.6,
    # post-#34, journal depth ~105k): p99 1,498,111 ns, p99.9 2,174,975 ns.
    # #91 (the O(depth) append-tail fix) has NOT landed as of this gate's
    # arming (#053) — the ceiling is deliberately generous enough that the
    # gate is not "born red" on that already-disclosed, tracked issue.
    "hp1_full_turn_closed_loop": {"p99_ns": 15_000_000, "p999_ns": 25_000_000},
    # HP-2 fan-out, all four swept N. Worst disclosed post-#34 p99 229,503 ns
    # (N=1000), p99.9 325,375 ns (N=1000). The PRIMARY HP-2 gate is the
    # flatness check below; this is a defense-in-depth absolute sanity bound
    # (catches "flat but uniformly terrible", which flatness alone would not).
    "hp2_fanout_n1": {"p99_ns": 5_000_000, "p999_ns": 6_000_000},
    "hp2_fanout_n10": {"p99_ns": 5_000_000, "p999_ns": 6_000_000},
    "hp2_fanout_n100": {"p99_ns": 5_000_000, "p999_ns": 6_000_000},
    "hp2_fanout_n1000": {"p99_ns": 5_000_000, "p999_ns": 6_000_000},
    # HP-3 decode/encode. Worst disclosed closed-loop p99 2,251 ns (decode) /
    # 625 ns (encode) — low-single-digit microseconds. 10x would be a few
    # tens of microseconds, too tight given CI-runner noise at this scale;
    # the 1 ms floor dominates, which is still 400-2000x the measured value
    # (genuinely "sits an order of magnitude inside", per docs/07 §5).
    "hp3_decode_d_closed_loop": {"p99_ns": 1_000_000, "p999_ns": 1_000_000},
    "hp3_encode_8_closed_loop": {"p99_ns": 1_000_000, "p999_ns": 1_000_000},
    # HP-4 requote (10-contract chain), both closed-loop sections. Worst
    # disclosed + this gate's own #053 re-run p99 ~160,767 ns, p99.9
    # ~216,447 ns.
    "hp4_requote_engine_only_closed_loop": {"p99_ns": 2_000_000, "p999_ns": 3_000_000},
    "hp4_requote_mailbox_closed_loop": {"p99_ns": 2_000_000, "p999_ns": 3_000_000},
    # HP-5 durable, measured-fused persistent-mode full turn. Worst disclosed
    # p99 1,041,919 ns, p99.9 4,165,631 ns (BENCH.md §5.1 run 1 — the
    # "disclosed tail instability" run). The p99.9 margin is wider than the
    # other series' because BENCH.md §5.1 explicitly could not rule out
    # genuine Postgres/Docker scheduling variance as the tail's cause, and a
    # GitHub-hosted runner's Docker daemon is plausibly noisier still.
    "hp5_persistent_full_turn_closed_loop": {"p99_ns": 15_000_000, "p999_ns": 45_000_000},
}

# Allocation ceilings (allocs/op = allocations + reallocations, per op),
# keyed by a substring match against the `[alloc-profile] <label>: N measured
# ops` line `benches/alloc_profile.rs::report_window` prints for that
# section. See BENCH.md §13 for why these are NOT zero (the milestone spec's
# "zero steady-state allocation" wording; the measured common actor turn is
# real, non-zero, and disclosed as such in BENCH.md §6 and §13).
#
# Precision matters here, per architect review (#053): the two `tokio`-driven
# sections below (`UnderlyingActor::handle` direct, `ActorHandle::submit`)
# get a COARSE MULTI-X ALLOC-REGRESSION CATCHER, not a tight no-regression
# bound — the ceiling sits ~2.2-2.5x above the freshly re-verified baseline
# (BENCH.md §13.3 / #126: the baseline itself is currently UNSTABLE, a
# disclosed ~2.3-2.6x divergence from the previously committed §6 figure with
# no code change), so a real but modest regression (e.g. 1.5x, to ~300-500
# allocs/op) would still pass. It genuinely catches an order-of-magnitude
# blowup, nothing finer, until #126 resolves which baseline is the honest
# reference. The THIRD section below (`MarketMakerEngine::update_price`) is
# different in kind: it is exactly reproducible (zero disclosed variance
# across every run, historical and #053's own re-verification alike), so its
# ceiling genuinely IS a tight no-regression bound — any deviation there is a
# real signal, not noise.
ALLOC_CEILINGS_PER_OP: dict[str, float] = {
    # `UnderlyingActor::handle` direct — the exact "append -> match -> append
    # -> enqueue" common actor turn docs/07 §4 names. BENCH.md §6's committed
    # baseline (77.374, range 62.577-82.657 across 3 disclosed runs); this
    # gate's own #053 re-verification measured a NEW, higher, reproducible
    # cluster (180.355-202.160 across 5 fresh runs on the identical machine/
    # code/Cargo.lock — see BENCH.md §13.3 / #126's disclosed, unresolved
    # divergence). COARSE catcher, not tight no-regression: ~2.2x the higher,
    # freshly-observed cluster (~202) — still fails a genuine multi-x
    # regression, would NOT catch a 1.5x-2x one.
    "UnderlyingActor::handle (direct": 450.0,
    # `ActorHandle::submit` round-trip (async mailbox + oneshot reply) — the
    # production gateway-facing API, expected to allocate a bit more than the
    # direct section (a fresh oneshot channel + mpsc send slot per call).
    # Baseline 82.657 (committed) / ~189.7-199.7 (#053 re-verification).
    # COARSE catcher, same caveat as above: ~2.5x the freshly-observed
    # cluster (~200).
    "ActorHandle::submit (async": 500.0,
    # `MarketMakerEngine::update_price` steady-state requote (HP-4, #050) —
    # UNLIKE the two sections above, this section is exactly reproducible:
    # 343.000 allocs/op with ZERO disclosed variance across every historical
    # run AND this gate's own #053 re-verification runs. A TIGHT ceiling
    # (~2% margin) is legitimate and higher-value here: any deviation is a
    # real signal, not noise absorption.
    "MarketMakerEngine::update_price": 350.0,
}

# HP-2 fan-out flatness tolerance — MUST match `FLATNESS_TOLERANCE_PCT` in
# `benches/hp2_ws_fanout.rs` (the bench's own printed verdict uses the same
# constant; this gate recomputes it independently from the parsed quantiles
# rather than trusting the printed PASS/WARN string, so a future change to
# the bench's print statements can never silently defeat the gate).
FANOUT_FLATNESS_TOLERANCE_PCT = 15.0
FANOUT_SERIES_IN_N_ORDER = [
    "hp2_fanout_n1",
    "hp2_fanout_n10",
    "hp2_fanout_n100",
    "hp2_fanout_n1000",
]


# ---------------------------------------------------------------------------
# Parsing — `benches/support/hdr.rs::report` and `benches/alloc_profile.rs`'s
# `report_window` are the two plain-text shapes every bench prints.
# ---------------------------------------------------------------------------


@dataclass
class Quantiles:
    samples: int
    p50_ns: int
    p99_ns: int
    p999_ns: int
    p9999_ns: int
    min_ns: int
    max_ns: int


@dataclass
class AllocStats:
    allocs_per_op: float
    bytes_per_op: float


@dataclass
class ParseResult:
    quantiles: dict[str, Quantiles] = field(default_factory=dict)
    alloc: dict[str, AllocStats] = field(default_factory=dict)


_REPORT_HEADER = re.compile(r"^--- (\S+) ---$")
_SAMPLES = re.compile(r"^\s*samples : (\d+)$")
_P50 = re.compile(r"^\s*p50\s+: (\d+) ns$")
_P99 = re.compile(r"^\s*p99\s+: (\d+) ns$")
_P999 = re.compile(r"^\s*p99\.9\s+: (\d+) ns$")
_P9999 = re.compile(r"^\s*p99\.99\s+: (\d+) ns$")
_MIN = re.compile(r"^\s*min\s+: (\d+) ns$")
_MAX = re.compile(r"^\s*max\s+: (\d+) ns$")

_ALLOC_HEADER = re.compile(r"^\[alloc-profile\] (.+): \d+ measured ops$")
_ALLOCS_PER_OP = re.compile(r"^\s*allocs/op\s+: ([\d.]+)$")
_BYTES_PER_OP = re.compile(r"^\s*bytes_alloc/op\s+: ([\d.]+)$")


def parse_log(text: str, result: ParseResult, source: str) -> None:
    """Parses one bench log's stdout text, merging into `result`.

    A duplicate series name across files (e.g. the same bench run twice) is
    allowed — the later file's reading wins — but is announced on stderr so
    it is never silently ambiguous which run a gate decision came from.
    """
    lines = text.splitlines()
    i = 0
    n = len(lines)
    while i < n:
        header = _REPORT_HEADER.match(lines[i])
        if header:
            name = header.group(1)
            fields: dict[str, int] = {}
            j = i + 1
            # The report block is exactly 7 fixed lines, in this order
            # (`Quantiles`'s `Display` impl, benches/support/hdr.rs).
            patterns = [
                ("samples", _SAMPLES),
                ("p50_ns", _P50),
                ("p99_ns", _P99),
                ("p999_ns", _P999),
                ("p9999_ns", _P9999),
                ("min_ns", _MIN),
                ("max_ns", _MAX),
            ]
            ok = True
            for key, pat in patterns:
                if j >= n:
                    ok = False
                    break
                m = pat.match(lines[j])
                if not m:
                    ok = False
                    break
                fields[key] = int(m.group(1))
                j += 1
            if ok:
                if name in result.quantiles:
                    print(
                        f"warning: series '{name}' reported more than once "
                        f"across the provided logs (last one, from {source}, wins)",
                        file=sys.stderr,
                    )
                result.quantiles[name] = Quantiles(
                    samples=fields["samples"],
                    p50_ns=fields["p50_ns"],
                    p99_ns=fields["p99_ns"],
                    p999_ns=fields["p999_ns"],
                    p9999_ns=fields["p9999_ns"],
                    min_ns=fields["min_ns"],
                    max_ns=fields["max_ns"],
                )
                i = j
                continue
        alloc_header = _ALLOC_HEADER.match(lines[i])
        if alloc_header:
            label = alloc_header.group(1)
            allocs_per_op = None
            bytes_per_op = None
            # `report_window` prints 8 lines after the header; allocs/op and
            # bytes_alloc/op are the last two, in a fixed position, but this
            # scans a small bounded window rather than assuming an exact
            # offset, so an unrelated formatting tweak upstream cannot
            # silently break parsing.
            for j in range(i + 1, min(i + 12, n)):
                m1 = _ALLOCS_PER_OP.match(lines[j])
                if m1:
                    allocs_per_op = float(m1.group(1))
                m2 = _BYTES_PER_OP.match(lines[j])
                if m2:
                    bytes_per_op = float(m2.group(1))
                if allocs_per_op is not None and bytes_per_op is not None:
                    break
            if allocs_per_op is not None and bytes_per_op is not None:
                if label in result.alloc:
                    print(
                        f"warning: alloc section '{label}' reported more than "
                        f"once across the provided logs (last one, from "
                        f"{source}, wins)",
                        file=sys.stderr,
                    )
                result.alloc[label] = AllocStats(
                    allocs_per_op=allocs_per_op, bytes_per_op=bytes_per_op
                )
        i += 1


# ---------------------------------------------------------------------------
# Gate evaluation
# ---------------------------------------------------------------------------


@dataclass
class Violation:
    detail: str


def check_latency_ceilings(result: ParseResult) -> list[Violation]:
    violations: list[Violation] = []
    for name, ceilings in LATENCY_CEILINGS_NS.items():
        q = result.quantiles.get(name)
        if q is None:
            violations.append(
                Violation(
                    f"GATED series '{name}' was not found in the bench output "
                    "(bench crashed, was renamed, or its report string "
                    "changed — a missing gated series is a gate failure, "
                    "never a silent pass)"
                )
            )
            continue
        p99_ceiling = ceilings["p99_ns"]
        p999_ceiling = ceilings["p999_ns"]
        if q.p99_ns > p99_ceiling:
            violations.append(
                Violation(
                    f"'{name}' p99 {q.p99_ns:,} ns exceeds the documented "
                    f"ceiling {p99_ceiling:,} ns"
                )
            )
        if q.p999_ns > p999_ceiling:
            violations.append(
                Violation(
                    f"'{name}' p99.9 {q.p999_ns:,} ns exceeds the documented "
                    f"ceiling {p999_ceiling:,} ns"
                )
            )
    return violations


def check_alloc_ceilings(result: ParseResult) -> list[Violation]:
    violations: list[Violation] = []
    for needle, ceiling in ALLOC_CEILINGS_PER_OP.items():
        match = next((v for k, v in result.alloc.items() if needle in k), None)
        if match is None:
            violations.append(
                Violation(
                    f"GATED alloc section matching '{needle}' was not found "
                    "in the alloc_profile output (bench crashed, label "
                    "changed, or section reordered — never a silent pass)"
                )
            )
            continue
        if match.allocs_per_op > ceiling:
            violations.append(
                Violation(
                    f"alloc section '{needle}' measured {match.allocs_per_op:.3f} "
                    f"allocs/op, exceeding the documented ceiling "
                    f"{ceiling:.3f} allocs/op (see ALLOC_CEILINGS_PER_OP's "
                    "comment for whether this is a tight no-regression bound "
                    "or a coarse multi-x catcher for this specific series)"
                )
            )
    return violations


def check_fanout_flatness(
    result: ParseResult, gate: bool
) -> tuple[list[Violation], float | None]:
    """Computes the HP-2 fan-out flatness verdict.

    Always PARSES and returns the worst |p99 delta| percentage so it can be
    printed regardless of `gate`. Only turns a tolerance breach into a
    `Violation` (build-failing) when `gate` is `True` — see this module's
    docstring / `BENCH_REGRESSION_GATE_FLATNESS` for why the per-PR smoke job
    runs with `gate=False` (report-only) while the nightly full-sample job
    runs with `gate=True` (BENCH.md §13.6).
    """
    violations: list[Violation] = []
    values: dict[str, int] = {}
    for name in FANOUT_SERIES_IN_N_ORDER:
        q = result.quantiles.get(name)
        if q is None:
            # A missing hp2_fanout_n* series is ALSO one of the
            # LATENCY_CEILINGS_NS gated series, so `check_latency_ceilings`
            # already reports it as a violation unconditionally (both smoke
            # and nightly) — no need to duplicate that failure here. Flatness
            # simply cannot be computed without every N; report that plainly
            # via the `None` return rather than a second violation.
            return violations, None
        values[name] = q.p99_ns
    baseline = values[FANOUT_SERIES_IN_N_ORDER[0]]
    worst_pct = 0.0
    for name in FANOUT_SERIES_IN_N_ORDER[1:]:
        delta = values[name] - baseline
        pct = 0.0 if baseline == 0 else 100.0 * delta / baseline
        worst_pct = max(worst_pct, abs(pct))
    if worst_pct > FANOUT_FLATNESS_TOLERANCE_PCT and gate:
        violations.append(
            Violation(
                f"HP-2 fan-out flatness: worst |p99 delta| across the N sweep "
                f"was {worst_pct:.1f}%, exceeding the {FANOUT_FLATNESS_TOLERANCE_PCT:.0f}% "
                "tolerance (docs/07 §4 DESIGN TARGET: HP-1 p99 must stay flat in N)"
            )
        )
    return violations, worst_pct


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_summary(result: ParseResult) -> None:
    print("=== bench-regression: parsed series ===")
    gated_names = set(LATENCY_CEILINGS_NS.keys())
    print("\n-- Gated latency series --")
    for name in sorted(gated_names):
        q = result.quantiles.get(name)
        if q is None:
            print(f"  {name}: MISSING")
            continue
        print(
            f"  {name}: p50={q.p50_ns:,}ns p99={q.p99_ns:,}ns "
            f"p99.9={q.p999_ns:,}ns p99.99={q.p9999_ns:,}ns "
            f"(n={q.samples})"
        )
    reported_only = sorted(set(result.quantiles.keys()) - gated_names)
    if reported_only:
        print(
            "\n-- Reported, NOT gated (match cost / append sub-spans / "
            "open-loop sojourn — docs/07 §7 excludes upstream match time "
            "from the venue-overhead budget) --"
        )
        for name in reported_only:
            q = result.quantiles[name]
            print(
                f"  {name}: p50={q.p50_ns:,}ns p99={q.p99_ns:,}ns "
                f"p99.9={q.p999_ns:,}ns p99.99={q.p9999_ns:,}ns "
                f"(n={q.samples})"
            )
    print(
        "\n-- Gated allocation series (the two tokio-driven sections are a "
        "COARSE multi-x catcher over an unstable baseline, #126 — not a "
        "tight no-regression bound; the MarketMakerEngine::update_price "
        "section IS a tight bound, it is exactly reproducible) --"
    )
    for needle, ceiling in ALLOC_CEILINGS_PER_OP.items():
        match = next((v for k, v in result.alloc.items() if needle in k), None)
        if match is None:
            print(f"  {needle}: MISSING")
        else:
            print(
                f"  {needle}: {match.allocs_per_op:.3f} allocs/op "
                f"(ceiling {ceiling:.3f}), {match.bytes_per_op:.1f} bytes/op"
            )


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <bench-log-file> [<bench-log-file> ...]", file=sys.stderr)
        return 2

    gate_flatness = os.environ.get("BENCH_REGRESSION_GATE_FLATNESS", "0") == "1"

    result = ParseResult()
    for path in argv[1:]:
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                text = f.read()
        except OSError as e:
            print(f"error: could not read '{path}': {e}", file=sys.stderr)
            return 2
        parse_log(text, result, source=path)

    print_summary(result)

    violations: list[Violation] = []
    violations += check_latency_ceilings(result)
    violations += check_alloc_ceilings(result)
    fanout_violations, worst_pct = check_fanout_flatness(result, gate=gate_flatness)
    violations += fanout_violations

    print("\n=== bench-regression: verdict ===")
    if worst_pct is not None:
        mode = "GATED" if gate_flatness else "reported only, NOT gated at this sample scale — BENCH.md §13.6"
        print(
            f"HP-2 fan-out flatness: worst |p99 delta| across N = "
            f"{worst_pct:.1f}% (tolerance {FANOUT_FLATNESS_TOLERANCE_PCT:.0f}%) [{mode}]"
        )
    else:
        print("HP-2 fan-out flatness: could not be computed (a swept-N series was missing)")

    if violations:
        print(f"FAIL — {len(violations)} violation(s):")
        for v in violations:
            print(f"  - {v.detail}")
        return 1

    print("PASS — every gated series is within its documented ceiling.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
