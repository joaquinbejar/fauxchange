# fauxchange — developer entrypoints.
#
# `make pre-push` is the canonical "ready to push" target; it mirrors the
# binding Pre-Submission Checklist (rules/global_rules.md) and the CI job
# set (.github/workflows/ci.yml, docs/TESTING.md §11) so local and CI never
# diverge. Every non-file target below is declared `.PHONY` and safe to
# re-run.
#
# Invoke `make pre-push` WITHOUT `-j` — its prerequisites
# (fix fmt lint-fix test check-spanish release readme doc) are listed in the
# order they must run and GNU Make preserves that order for a serial
# (non-parallel) invocation.

SHELL := /bin/bash
CARGO ?= cargo
LOGLEVEL ?= WARN

.DEFAULT_GOAL := help

.PHONY: all help build release run run-seeded check test test-conformance \
	docker-smoke soak fmt fmt-check lint lint-fix fix clean doc readme coverage \
	coverage-html audit deny check-spanish pre-push publish \
	bench-regression bench-regression-full

all: help ## Alias for `help`

help: ## List the common targets
	@echo "fauxchange — developer entrypoints"
	@echo ""
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| grep -v '^workflow-%' \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  workflow-<job-id>  Run a .github/workflows/*.yml job locally via act"
	@echo "                     (fmt, clippy, test, build-release, doctests, msrv,"
	@echo "                     golden, determinism, parity, fuzz, migrations, cargo-audit,"
	@echo "                     cargo-deny, image-build, docker-smoke, bench-regression,"
	@echo "                     bench-regression-nightly)"

# --- Build / run -------------------------------------------------------

build: ## cargo build (debug, all features)
	$(CARGO) build --all-features

release: ## cargo build --release (zero warnings; RUSTFLAGS=-D warnings)
	RUSTFLAGS="-D warnings" $(CARGO) build --release

check: ## cargo check --all-features (fast type-check)
	$(CARGO) check --all-features

run: ## Boot the venue locally (REST gateway on FAUXCHANGE_REST_ADDR, default 127.0.0.1:8080)
	FAUXCHANGE_DEV=1 $(CARGO) run

run-seeded: ## Boot the venue with seeded instruments for manual testing (BTC, ETH)
	FAUXCHANGE_DEV=1 FAUXCHANGE_UNDERLYINGS=BTC,ETH $(CARGO) run

# --- Format / lint -------------------------------------------------------

fmt: ## Apply rustfmt
	$(CARGO) fmt --all

fmt-check: ## Check formatting without writing — mirrors CI job `fmt`
	$(CARGO) fmt --all --check

lint: ## clippy, all targets/features, warnings deny — mirrors CI job `clippy`
	$(CARGO) clippy --all-targets --all-features -- -D warnings

lint-fix: ## clippy --fix (auto-apply clippy suggestions), warnings deny
	$(CARGO) clippy --all-targets --all-features --fix --allow-dirty --allow-staged -- -D warnings

fix: ## cargo fix (auto-apply compiler-suggested fixes)
	$(CARGO) fix --all-targets --all-features --allow-dirty --allow-staged

doc: ## Missing-docs gate on every `pub` item
	$(CARGO) clippy --all-features -- -W missing-docs

check-spanish: ## Fail if Spanish-only diacritics appear in src/ or tests/ (basic heuristic; rules/global_rules.md); excludes a quoted single accented char fed to .repeat(...) — a deliberate multi-byte-vs-byte-length test literal (src/gateway/fix/limits.rs, src/conformance/report.rs), never Spanish prose
	@matches="$$(grep -rn \
		-e 'ñ' -e 'Ñ' -e '¿' -e '¡' \
		-e 'á' -e 'é' -e 'í' -e 'ó' -e 'ú' \
		-e 'Á' -e 'É' -e 'Í' -e 'Ó' -e 'Ú' \
		--include='*.rs' src tests 2>/dev/null \
		| grep -vE '"[^"]*"[[:space:]]*\.repeat\(' || true)"; \
	if [ -n "$$matches" ]; then \
		echo "$$matches"; \
		echo "check-spanish: Spanish characters found above — rules/global_rules.md" >&2; \
		echo "  requires English-only code/comments. Fix and re-run." >&2; \
		exit 1; \
	else \
		echo "check-spanish: OK — no Spanish diacritics found in src/ tests/ (excluding deliberate multi-byte test literals piped through .repeat())."; \
	fi

# --- Test ------------------------------------------------------------------

test: ## Unit + integration + doctest suite — mirrors CI job `test`
	LOGLEVEL=$(LOGLEVEL) $(CARGO) test --all-features

test-conformance: ## The tests/ protocol conformance + e2e suite (golden/determinism/parity/rest/ws/...)
	LOGLEVEL=$(LOGLEVEL) $(CARGO) test --tests --all-features

docker-smoke: ## Docker e2e smoke (DOCKER=1): compose up -> health -> order -> WS fill -> clean shutdown, < 30s cold budget (builds the image first)
	docker compose -f docker/docker-compose.yml build
	DOCKER=1 $(CARGO) test --test docker_smoke --all-features -- --nocapture

soak: ## Stability soak (SOAK=1, a few minutes): flat memory, no sequence gaps, clean shutdown, restart-from-journal (#54) — operator-run before a release cut, never on the fast CI gate
	SOAK=1 $(CARGO) test --release --test load -- --ignored --nocapture

# --- Supply-chain gate (docs/08-threat-model.md §8) -------------------------

audit: ## cargo audit — RustSec advisory-DB scan (config: .cargo/audit.toml)
	$(CARGO) audit

deny: ## cargo deny check — license/ban/duplicate/source policy (config: deny.toml)
	$(CARGO) deny check

# --- Docs / coverage / release ----------------------------------------------

readme: ## Regenerate README.md from src/lib.rs module docs via cargo-readme
	$(CARGO) readme > README.md

coverage: ## cargo tarpaulin — text + lcov coverage summary
	$(CARGO) tarpaulin --all-features --workspace --timeout 120 --out Stdout --out Lcov

coverage-html: ## cargo tarpaulin — HTML report under target/tarpaulin
	$(CARGO) tarpaulin --all-features --workspace --timeout 120 --out Html --output-dir target/tarpaulin

clean: ## cargo clean
	$(CARGO) clean

# --- Performance regression gate (#053, docs/07-performance-budgets.md §6) --

bench-regression: ## Reduced-sample hot-path benches + the bench-regression gate — mirrors CI job `bench-regression` (HP-2 flatness gated at the 100% PR-path tolerance, above the observed CI-runner noise floor; BENCH.md §13.6)
	@mkdir -p target/bench-out
	scripts/record_bench_run_conditions.sh smoke | tee target/bench-out/run-conditions.txt
	HP1_WARMUP_OPS=1000 HP1_MEASURED_OPS=5000 HP1_OPEN_LOOP_OPS=200 $(CARGO) bench --bench hp1_order_path | tee target/bench-out/hp1.log
	HP2_WARMUP_OPS=500 HP2_MEASURED_OPS=3000 $(CARGO) bench --bench hp2_ws_fanout | tee target/bench-out/hp2.log
	HP3_WARMUP_OPS=1000 HP3_MEASURED_OPS=10000 HP3_OPEN_LOOP_OPS=200 $(CARGO) bench --bench hp3_fix_parse | tee target/bench-out/hp3.log
	HP4_WARMUP_OPS=500 HP4_MEASURED_OPS=2000 HP4_OPEN_LOOP_OPS=200 $(CARGO) bench --bench mm_requote_hdr | tee target/bench-out/hp4.log
	HP5_WARMUP_OPS=30 HP5_MEASURED_OPS=150 HP5_OPEN_LOOP_OPS=20 $(CARGO) bench --bench hp5_durable_append | tee target/bench-out/hp5.log
	ALLOC_WARMUP_OPS=2000 ALLOC_MEASURED_OPS=10000 ALLOC_MM_WARMUP_OPS=500 ALLOC_MM_MEASURED_OPS=2000 $(CARGO) bench --bench alloc_profile | tee target/bench-out/alloc.log
	BENCH_REGRESSION_GATE_FLATNESS=1 BENCH_FANOUT_FLATNESS_TOLERANCE_PCT=100 python3 scripts/bench_regression_gate.py target/bench-out/*.log

bench-regression-full: ## Full default-sample hot-path benches + the bench-regression gate — mirrors CI job `bench-regression-nightly` (HP-2 flatness genuinely gated; needs local Docker for HP-5)
	@mkdir -p target/bench-out
	scripts/record_bench_run_conditions.sh full | tee target/bench-out/run-conditions.txt
	$(CARGO) bench --bench hp1_order_path | tee target/bench-out/hp1.log
	$(CARGO) bench --bench hp2_ws_fanout | tee target/bench-out/hp2.log
	$(CARGO) bench --bench hp3_fix_parse | tee target/bench-out/hp3.log
	$(CARGO) bench --bench mm_requote_hdr | tee target/bench-out/hp4.log
	$(CARGO) bench --bench hp5_durable_append | tee target/bench-out/hp5.log
	$(CARGO) bench --bench alloc_profile | tee target/bench-out/alloc.log
	BENCH_REGRESSION_GATE_FLATNESS=1 python3 scripts/bench_regression_gate.py target/bench-out/*.log

pre-push: fix fmt lint-fix test check-spanish release readme doc ## The canonical ready-to-push gate — mirrors every binding pre-submission item (run without -j)
	@echo "pre-push: done — re-check 'git status' (readme may have staged README.md)."

publish: ## Publish fauxchange to crates.io (CARGO_REGISTRY_TOKEN); CONFIRM with the user first
	@test -n "$$CARGO_REGISTRY_TOKEN" || (echo "publish: CARGO_REGISTRY_TOKEN is not set" >&2 && exit 1)
	$(MAKE) readme
	$(CARGO) package --all-features
	$(CARGO) publish

# --- CI parity (act) --------------------------------------------------------
#
# `make workflow-<job-id>` runs the matching job from ANY workflow file under
# .github/workflows/ (ci.yml, bench-regression.yml, ...) locally via `act`
# (https://github.com/nektos/act) — pointed at the directory, not one named
# file, so job ids stay a single stable contract across every workflow file
# in this repo. Job ids: fmt, clippy, test, build-release, doctests, msrv,
# golden, determinism, parity, fuzz, migrations, cargo-audit, cargo-deny,
# image-build, docker-smoke, bench-regression, bench-regression-nightly.
# Example: `make workflow-clippy`, `make workflow-bench-regression`.
workflow-%:
	act push -j $* --workflows .github/workflows
