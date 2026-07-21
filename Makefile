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
	docker-smoke fmt fmt-check lint lint-fix fix clean doc readme coverage \
	coverage-html audit deny check-spanish pre-push publish

all: help ## Alias for `help`

help: ## List the common targets
	@echo "fauxchange — developer entrypoints"
	@echo ""
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| grep -v '^workflow-%' \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  workflow-<job-id>  Run a .github/workflows/ci.yml job locally via act"
	@echo "                     (fmt, clippy, test, build-release, doctests, msrv,"
	@echo "                     golden, determinism, parity, fuzz, migrations, cargo-audit,"
	@echo "                     cargo-deny, image-build, docker-smoke)"

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

check-spanish: ## Fail if Spanish-only diacritics appear in src/ or tests/ (basic heuristic; rules/global_rules.md)
	@if grep -rn \
		-e 'ñ' -e 'Ñ' -e '¿' -e '¡' \
		-e 'á' -e 'é' -e 'í' -e 'ó' -e 'ú' \
		-e 'Á' -e 'É' -e 'Í' -e 'Ó' -e 'Ú' \
		--include='*.rs' src tests 2>/dev/null; then \
		echo "check-spanish: Spanish characters found above — rules/global_rules.md" >&2; \
		echo "  requires English-only code/comments. Fix and re-run." >&2; \
		exit 1; \
	else \
		echo "check-spanish: OK — no Spanish diacritics found in src/ tests/."; \
	fi

# --- Test ------------------------------------------------------------------

test: ## Unit + integration + doctest suite — mirrors CI job `test`
	LOGLEVEL=$(LOGLEVEL) $(CARGO) test --all-features

test-conformance: ## The tests/ protocol conformance + e2e suite (golden/determinism/parity/rest/ws/...)
	LOGLEVEL=$(LOGLEVEL) $(CARGO) test --tests --all-features

docker-smoke: ## Docker e2e smoke (DOCKER=1): compose up -> health -> order -> WS fill -> clean shutdown, < 30s cold budget (builds the image first)
	docker compose -f docker/docker-compose.yml build
	DOCKER=1 $(CARGO) test --test docker_smoke --all-features -- --nocapture

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

pre-push: fix fmt lint-fix test check-spanish release readme doc ## The canonical ready-to-push gate — mirrors every binding pre-submission item (run without -j)
	@echo "pre-push: done — re-check 'git status' (readme may have staged README.md)."

publish: ## Publish fauxchange to crates.io (CARGO_REGISTRY_TOKEN); CONFIRM with the user first
	@test -n "$$CARGO_REGISTRY_TOKEN" || (echo "publish: CARGO_REGISTRY_TOKEN is not set" >&2 && exit 1)
	$(MAKE) readme
	$(CARGO) package --all-features
	$(CARGO) publish

# --- CI parity (act) --------------------------------------------------------
#
# `make workflow-<job-id>` runs the matching job from
# .github/workflows/ci.yml locally via `act` (https://github.com/nektos/act).
# Job ids are the stable contract: fmt, clippy, test, build-release,
# doctests, msrv, golden, determinism, parity, fuzz, cargo-audit, cargo-deny.
# Example: `make workflow-clippy`, `make workflow-cargo-deny`.
workflow-%:
	act push -j $* --workflows .github/workflows/ci.yml
