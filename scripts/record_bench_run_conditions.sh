#!/usr/bin/env bash
# scripts/record_bench_run_conditions.sh — the #053 `bench-regression` gate's
# run-conditions artifact (docs/07-performance-budgets.md §5: "Run conditions
# recorded ... machine class, CPU governor, toolchain, commit, upstream crate
# commits, N (for HP-2), and journal mode").
#
# Usage: scripts/record_bench_run_conditions.sh <smoke|full>
#
# Printed to stdout; the calling CI step redirects it into the uploaded
# artifact alongside the raw bench logs, so a reader can never separate a
# pasted quantile table from *how* it was produced (the same principle
# `benches/support/mod.rs::print_run_conditions` applies inside each bench
# binary itself, at a narrower scope — this script is the CI-job-level
# superset docs/07 §5 asks for: CPU model, governor, toolchain version, git
# commit, and the pinned upstream crate versions a running binary cannot
# reliably self-report on every platform).
set -euo pipefail

MODE="${1:-smoke}"

echo "=== bench-regression run conditions (#053) — mode: ${MODE} ==="
echo
echo "-- Machine class --"
if [ -n "${RUNNER_NAME:-}" ]; then
  echo "  GitHub Actions runner: ${RUNNER_NAME} (${RUNNER_OS:-unknown}/${RUNNER_ARCH:-unknown})"
  echo "  Runner class: GitHub-hosted (ubuntu-24.04), SHARED — not a dedicated/pinned"
  echo "    bench rig; no CPU-governor control, no guarantee of the same physical"
  echo "    host between runs. This is WHY the gate compares against a documented"
  echo "    generous absolute ceiling (BENCH.md §13), never a same-machine p99"
  echo "    delta against the M4-Max-laptop-recorded BENCH.md baseline."
else
  echo "  Not running under GitHub Actions (RUNNER_NAME unset) — local invocation."
fi
echo "  uname -a: $(uname -a)"
if command -v nproc >/dev/null 2>&1; then
  echo "  logical CPUs (nproc): $(nproc)"
fi
if [ -r /proc/cpuinfo ]; then
  # `|| true`: some CPU architectures' /proc/cpuinfo has no "model name"
  # field (e.g. non-x86) — degrade to "<unavailable>" rather than aborting
  # this step under `set -euo pipefail`.
  cpu_model="$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')" || true
  echo "  CPU model: ${cpu_model:-<unavailable>}"
fi
echo "  CPU governor / pinning: not controllable on a GitHub-hosted runner (no"
echo "    cpufreq governor exposed, no taskset pinning) — disclosed, not hidden,"
echo "    same convention BENCH.md §1 uses for the un-pinned developer laptop."
echo
echo "-- Toolchain --"
rustc --version
cargo --version
echo
echo "-- Commit --"
echo "  $(git rev-parse HEAD 2>/dev/null || echo 'unknown (no git checkout?)')"
echo
echo "-- Pinned upstream crates (from Cargo.lock) --"
for crate in option-chain-orderbook orderbook-rs pricelevel optionstratlib \
             ironfix-core ironfix-tagvalue ironfix-dictionary ironfix-transport \
             hdrhistogram criterion; do
  # `|| true` guards every stage of this pipeline: under `set -euo pipefail`,
  # a crate absent from Cargo.lock (grep finds nothing -> exits 1) would
  # otherwise abort this WHOLE run-conditions step before any bench even
  # runs. A missing/renamed crate degrades to a recorded "<absent>" instead.
  version="$(grep -A1 "^name = \"${crate}\"$" Cargo.lock 2>/dev/null | grep '^version' | head -1 | cut -d'"' -f2)" || true
  echo "  ${crate}: ${version:-<absent>}"
done
echo
echo "-- Bench configuration --"
echo "  HP-2 N sweep: 1, 10, 100, 1000 (fixed, benches/hp2_ws_fanout.rs)"
echo "  Journal mode: in-memory (InMemoryVenueJournal) for HP-1/HP-2/alloc_profile;"
echo "    durable (PgVenueJournal against a real ephemeral postgres:18-alpine via"
echo "    testcontainers) for HP-5 — never mocked, matching BENCH.md's own"
echo "    convention (§1)."
if [ "${MODE}" = "smoke" ]; then
  echo "  Sample scale: REDUCED (\"smoke\") — every *_WARMUP_OPS/*_MEASURED_OPS env"
  echo "    var below is set below BENCH.md's own default op counts so this job"
  echo "    stays fast on every push/PR. Absolute ceilings (not relative deltas)"
  echo "    mean this is still a REAL gate at reduced scale, just a less"
  echo "    discriminating one than the nightly full-scale run for HP-1's own"
  echo "    journal-depth-dependent tail (BENCH.md §3.4) — see BENCH.md §13."
else
  echo "  Sample scale: FULL — every *_WARMUP_OPS/*_MEASURED_OPS is left at its"
  echo "    bench-file default, matching BENCH.md's own documented methodology"
  echo "    exactly (the same op counts as the committed HP-1..HP-5 baselines)."
fi
