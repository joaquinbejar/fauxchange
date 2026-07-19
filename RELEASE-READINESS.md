# fauxchange release readiness — 1.0 promotion HELD

> **What this is.** The one-off pre-1.0 acceptance pass
> (`docs/RELEASE-PROCESS.md` §11, `docs/SEMVER.md`, issue #55), run against
> the v1.0 milestone's four gate PRs. It is a **release-readiness record**,
> not a cut: this pass built and proved the release **machinery** (the
> version bump mechanics, the `CHANGELOG.md` lock mechanics, the
> tag-triggered publish workflow, the `[package]` metadata) and confirmed
> the four v1.0 gates are green — but it deliberately **does not promote
> the crate to `1.0.0`**. `docs/` is gitignored (local-only by design — see
> the `CHANGELOG.md` preamble), so this file is the **committed** mirror of
> the readiness evidence; the fuller design-doc narrative lives in
> `docs/RELEASE-PROCESS.md` / `docs/SEMVER.md` / `docs/ROADMAP.md` locally.

- **Crate version:** unchanged at `0.0.1` (still the name-reservation
  placeholder — deliberately NOT bumped by this pass).
- **`CHANGELOG.md`:** unchanged — `[Unreleased]` stays `[Unreleased]`; no
  dated release section was added.
- **What *is* new and kept:** `.github/workflows/release.yml` (the
  tag-triggered publish machinery, version-agnostic), a real `Cargo.toml`
  `[package].include` bug fix, and a `Makefile` `check-spanish` false-positive
  fix. See §4–§6.

## Why the promotion is held (both reasons, stated plainly)

1. **The "one quarter" surface-stability criterion cannot be met on a repo
   this young.** `docs/ROADMAP.md` / `docs/RELEASE-PROCESS.md` §11 gate the
   1.0 promotion on each external surface having "shipped without a
   breaking change for one quarter." This repository's entire history spans
   **2026-07-12 to 2026-07-19 — eight days** (`git log --format=%ad
   --date=short | sort -u`), and no surface has ever shipped as a real
   `0.x` release — `0.0.1` is a crates.io name-reservation placeholder with
   no code. There is nothing to measure a quarter of stability *against*
   yet. Every surface is genuinely additive-only so far (§2), which is
   necessary but not sufficient for the promotion the ROADMAP describes.
2. **The whole PR stack is still unmerged.** All four v1.0 gate PRs
   (#124/#125/#127/#128) and this release-readiness PR sit open, stacked,
   on top of `main`. "Every v1.0 milestone issue closed" — an explicit
   acceptance criterion in `milestones/v1.0-stability/055-v1-acceptance-
   release.md` — cannot be true before a merge happens.

Neither of these is a code defect. Both are named here so the promotion
decision is made deliberately, by the human, with the facts in hand — not
inferred from a version number this pass declined to bump.

## 0. Known finding — a fix is already in flight (not done by this pass)

**A genuine inconsistency between `docs/SEMVER.md`'s replay-portability
promise and the current implementation, found by this pass, being fixed by
`matching-expert` as a separate follow-up commit on this branch — not
resolved here.**

- **The promise** (`docs/SEMVER.md` "v1.0 commitments"): *"A replay journal
  recorded by `v1.x` replays on any later `v1.y` — the journal `schema` tag
  is the consumer's **primary** version pin, the crate version
  **secondary**."*
- **The implementation found during this pass** (`src/simulation/manifest.rs`
  `DependencyVersions::first_mismatch`, called from
  `src/simulation/replay.rs` `verify_bundle_versions`): compares **three**
  fields for **exact string equality** — `envelope_schema` (the schema tag
  — fine, matches the promise), but also `fauxchange`
  (`env!("CARGO_PKG_VERSION")`) and `optionstratlib`'s version. A mismatch
  on **any** of the three refuses replay — the crate version is not
  treated as secondary, it is an equally hard gate.
- **Directly reproduced.** Bumping `Cargo.toml` to `1.0.0` during this pass
  (since reverted — see below) broke two committed adversarial-corpus
  fixtures that were recorded under the crate's `0.0.1` build: both failed
  with `Err(VersionMismatch { kind: "fauxchange", expected: "1.0.0", found:
  "0.0.1" })` **before** the scenario each fixture was written to exercise
  was ever reached. That is live, reproduced proof that a bundle/journal
  recorded on one `fauxchange` build does not replay on a different one —
  including, as implemented, a mere **patch** bump within `v1.x`.
- **Disposition: fixing, not just documenting.** The coordinator has
  assigned this to `matching-expert`: loosen the load gate to the
  schema-tag (+ same-major) rule the promise actually describes, as a
  separate follow-up commit on `stack/55-v1-release`. This pass's two
  affected fixtures (`tests/adversarial/bundle/newer_journal_schema.json`,
  `tests/adversarial/bundle/tampered_event.json`) were **reverted to their
  original `0.0.1` form** rather than left regenerated at a version this
  pass no longer promotes to — they load fine against a `0.0.1` binary as
  they always did. `matching-expert` will add a new, dedicated positive
  cross-version replay test rather than relying on these two adversarial
  fixtures to carry that assertion incidentally.
- **Why this matters, independent of the version number:** `replay_bundle`/
  `recover_from_records` is the same algorithm the #054 stability soak used
  for "restart-from-journal determinism," and the one a future automatic
  boot-time journal-resume (tracked, not-yet-built, `#85`) will be built
  on. Whatever version fauxchange eventually promotes to, an operator who
  patch-upgrades a running container and tries to resume its existing
  durable journal should not get a refused resume — that is the whole
  point of the gate this finding is about.
- **Not mine to fix** — `src/` is out of DevOps's scope and this is a
  determinism-semantics design decision, correctly routed to
  `matching-expert`.

## 1. ROADMAP v1.0 gate acceptance pass

The v1.0 milestone (`docs/ROADMAP.md#v10--stability--conformance-suite`) is
four gate issues plus this readiness pass. Each is evidenced below from the
actual PR and a live re-check of its CI run (not merely restated from the
PR description) — `gh pr view` / `gh pr checks` against
`joaquinbejar/fauxchange` on 2026-07-19. **All four gates are green; none
of the four PRs are merged yet** (see "why the promotion is held," above).

### #51 — Packaged `fauxchange conformance` harness across REST, WS, FIX

PR #124 (`stack/51-conformance-harness` → `main`, **open**, not yet merged).

- CI: all 15 distinct jobs green (`fmt`, `clippy`, `test`, `build-release`,
  `doctests`, `msrv`, `golden`, `determinism`, `parity`, `fuzz`,
  `migrations`, `cargo-audit`, `cargo-deny`, `image-build`, `docker-smoke`),
  each reporting `SUCCESS` across both recorded check-suite runs (30/30
  individual check entries).
- `./fauxchange conformance`: initially 26/26 green per the PR description;
  a follow-up review round (architect + `api-security-auditor`, run after
  the account's weekly sub-agent limit reset — see below) added one more
  case (`control.ws_live_permission_gate`, driving a real `/ws` socket
  end-to-end) and landed at **27/27 green**, all four fixes from the
  security pass applied and pushed (bounded `redact()` on Bearer/JWT
  substrings, typed REST-failure projection instead of raw-body echo, a
  guarded FIX event-count index, checked/bounded `BodyLength` arithmetic in
  `split_frames`).
- Review: the PR body itself discloses the architect + `api-security-auditor`
  passes were **initially deferred** (the account hit its weekly usage
  limit). This was **not silently skipped** — a follow-up comment on the PR
  (2026-07-19T06:41Z) confirms both ran once the limit reset: **architect
  PASS**; security **"conditionally yes" with four items, all fixed and
  pushed** (see above). Flagged here for transparency since the PR
  description alone (without the follow-up comment) would understate the
  review state.
- Coverage: order-entry parity REST ≡ FIX, observation parity REST/WS/FIX,
  control parity REST/WS (FIX has no control message), the FIX
  session/order/market-data script plus every `docs/03` §8 reject row with a
  redacted `Text (58)`, and REST/WS conformance (OpenAPI shape, tokenless
  `/health`, permission gating, subscribe → snapshot → sequenced deltas with
  monotonic `instrument_sequence`).

### #52 — REST/WS JSON decoder fuzz targets, full adversarial corpus, final SECURITY.md

PR #125 (`stack/52-json-fuzz-security` → `main`, **open**, stacked on #124).

- CI: all 15 jobs green, 30/30 check entries, same as #124.
- `api-security-auditor` (the enforcement owner for this gate, CLAUDE.md Key
  Decisions / ADR-0008): **clean PASS — closes the v1.0 security gate**
  (PR body, verified verbatim) — zero P1/P2/P3. All four `cargo fuzz`
  targets (`fix_decode` from #42, plus new `rest_json_decode` /
  `ws_frame_decode` / `journal_bundle_decode`) drive the real production
  decode paths under their real ceilings (`MAX_REQUEST_BODY_BYTES`,
  `MAX_WS_FRAME_BYTES`, `MAX_JOURNAL_RECORD_BYTES` / `MAX_BUNDLE_BYTES` —
  confirmed present in `src/exchange/journal.rs` and the router/WS modules),
  no panic path, corpus committed, zero `src/` change (the fuzz targets
  prove existing validation, they don't add new validation).
- `SECURITY.md`'s supported-versions table carries the final v1.0 policy
  (verified against the file: `latest published 1.x minor` gets security
  fixes; `0.x` carries none) — matches the CHANGELOG `### Security` entry.
  This table describes the policy for **when** `v1.0.0` ships, not a claim
  that it has.

### #53 — Arm the CI performance regression gate

PR #127 (`stack/53-regression-gate` → `main`, **open**, stacked on #125).

- CI: `fmt`/`clippy`/`test`/`build-release`/`doctests`/`msrv`/`golden`/
  `determinism`/`parity`/`fuzz`/`migrations`/`cargo-audit`/`cargo-deny`
  green; **`bench-regression` SUCCESS** (the new gate job itself, verified
  live); `bench-regression-nightly` correctly `SKIPPED` on a push/PR event
  (it is `schedule` + `workflow_dispatch`-only by design).
- `architect`: **clean PASS — closes the v1.0 performance gate** (PR body,
  verified verbatim). The gate compares measured `bench-hdr` p99/p99.9
  against documented absolute ceilings (never a same-machine delta against
  the disclosed un-pinned laptop baseline — this repo has no self-hosted
  runner) and was proven to actually fail: a real injected 20 ms
  `std::thread::sleep` in `benches/hp1_order_path.rs` (never `src/`) tripped
  it (p99 25.7 ms > 15 ms ceiling), then was reverted before commit.
- Disclosed, non-blocking follow-up: **#126**, an unresolved ~2.3–2.6×
  divergence between this re-verification's allocation counts and the
  previously committed `BENCH.md` §6 figures on the same
  machine/code/`Cargo.lock` — named for `matching-expert`/`architect` with a
  real profiler, not silently reconciled. It does not gate the readiness
  verdict: the ceiling already covers both clusters and is labelled DESIGN
  TARGET, not an SLO.

### #54 — Stability soak: flat memory, no sequence gaps, clean shutdown, restart-from-journal

PR #128 (`stack/54-stability-soak` → `main`, **open**, stacked on #127).

- CI: all 15 jobs green (`fmt`, `clippy`, `test`, `build-release`,
  `doctests`, `msrv`, `golden`, `determinism`, `parity`, `fuzz`,
  `migrations`, `cargo-audit`, `cargo-deny`, `image-build`, `docker-smoke`),
  plus `bench-regression` `SUCCESS` and `bench-regression-nightly`
  correctly `SKIPPED` — confirmed by polling `gh pr checks 128` to
  completion (an earlier check at the start of this pass caught several
  jobs still `pending`; re-polled to a final, fully green state before
  citing it here).
- `architect`: **clean PASS — "the v1.0 stability gate is honestly closed"**
  (PR body, verified verbatim); one 🟠 (Property-3 overclaim — the drain
  assertion needed to genuinely await the actor's `JoinHandle`, not infer
  completion) was fixed before the PASS.
- Real measured soak (`SOAK=1 cargo test --release --test load -- --ignored`,
  three runs, ~61 s each, all four properties held every run):
  - **Flat memory:** RSS 27.6 MB → 36.4 MB (Δ 8.7 MB, well inside the
    `max(20%, 20 MB)` margin) — the delta is the in-memory journal's
    expected retained-record growth, not a leak (`BENCH.md` §14.2).
  - **No sequence gaps:** `underlying_sequence` 0..=4,347 (4,348 values, no
    gaps); `instrument_sequence` 4,347 strictly consecutive deltas.
  - **Clean shutdown drain:** a 60-order concurrent burst at a 4-slot
    mailbox — 5 accepted, 55 rate-limited, **0 lost**; the actor's own
    `JoinHandle` genuinely awaited to completion (not inferred from the
    submitters' results).
  - **Restart-from-journal determinism:** 4,348 events re-executed against
    the real `replay_bundle`/`recover_from_records` oracle, ordered-value
    equality held; the negative case — a corrupted stored event — halted
    recovery with the typed `JournalCorruption { underlying: BTC, sequence: 0 }`
    naming the exact point, never a silent divergent resume. (This test runs
    within one build throughout — it does not exercise cross-version
    replay; see §0 for why that distinction matters.)
  - Disclosed: live gateway-edge latency injection is deferred to #111 — the
    soak measures the seeded `LatencyConfig::draw` fidelity, not live-request
    latency, and says so.

### Overall gate verdict

- Conformance + parity suites: **green across REST, WS, and FIX** (#51).
- Security gate: **green**, `api-security-auditor` clean PASS (#52).
- Performance regression gate: **armed and proven to fail on a real
  regression**, `architect` clean PASS (#53).
- Stability soak: **green on the real measured run**, `architect` clean
  PASS (#54), CI fully green.
- No milestone PR has fabricated a number, skipped a check to go green, or
  silently downgraded a gate — every disclosed gap above (#124's initially
  deferred review then resolved, #126's alloc divergence, #111's deferred
  live-latency injection, §0's replay-version finding now routed to
  `matching-expert`) is a **named, tracked** follow-up, not a hidden one.
- **All four gates pass. The promotion is still held** — for the two
  reasons stated above, neither of which is "a gate failed."

## 2. SemVer surface-stability verification (`docs/SEMVER.md`)

Each row of the `docs/SEMVER.md` public-surface table, checked against its
named source of truth and the full `CHANGELOG.md` history
(2026-07-12 → 2026-07-19):

| Surface | Source of truth | Additive so far | Evidence |
|---|---|---|---|
| REST endpoints | `src/gateway/rest/` + `03-protocol-surfaces.md` | Yes | `CHANGELOG.md` carries only `### Added`/`### Security` entries across the entire history — zero `### Changed`/`### Removed` (breaking) entries; #51's conformance harness asserts every route against its own `#[utoipa::path]` shape |
| REST DTO shapes | `src/models.rs` | Yes | Same — every DTO addition is additive; `#[serde(deny_unknown_fields)]` on request DTOs means a field addition to a *request* is opt-in only when defaulted, never silently breaking |
| OpenAPI document | `utoipa` `ApiDoc` in `src/main.rs` | Yes | Registered/asserted by the conformance harness (#51) |
| WebSocket protocol | `src/gateway/ws/`, `WsMessage` | Yes | `WsMessage` uses internally-adjacent tagging (`#[serde(tag = "type", content = "data")]`, confirmed in `src/models.rs`); every variant added since inception, none removed/reshaped per the CHANGELOG |
| FIX 4.4 message set + session rules | `src/gateway/fix/` | Yes | `BeginString = FIX.4.4` pinned (`docs/specs/fix-dialect.md`, enforced in `src/gateway/fix/error.rs`'s `BeginStringMismatch`); confirmed message set present in `src/gateway/fix/`: `Logon(A)`/`Heartbeat(0)`/`TestRequest(1)`/`ResendRequest(2)`/`SequenceReset(4)`/`Logout(5)`, `NewOrderSingle(D)`/`OrderCancelRequest(F)`/`OrderCancelReplaceRequest(G)`/`OrderCancelReject(9)`/`ExecutionReport(8)`, `OrderMassCancelRequest`/`Report`, `MarketDataRequest(V)`/`SnapshotFullRefresh(W)`/`IncrementalRefresh(X)`/`RequestReject(Y)`, `Reject(3)`/`BusinessMessageReject(j)` — additive only across #37–#41, #52 |
| Error envelope | `src/error.rs` | Yes | HTTP-status + FIX-reject mapping; no removed variant in the CHANGELOG |
| Configuration file + env vars | `src/config.rs` + `06-deployment.md` | Yes | `.env.example` documents every env var by layer; new keys landed with defaults across v0.2–v0.5, none removed |
| Container / compose contract | `docker/` | Yes | Service names (`fauxchange`, `postgres`), ports (`8080`, `9878`, `9090`, `5432`), and volumes unchanged since #25/#26; `image-build` CI job asserts `docker compose config` on both the default and `persistent` profiles every push |
| Replay journal format | `src/exchange/` (journal wiring) | **Schema tag: yes. Cross-version replay: NO — see §0, fix in flight.** | `VENUE_ENVELOPE_SCHEMA` confirmed present and checked on load; #54's soak proves a real restart-from-journal round-trip *within one build only*. §0 reproduces, directly, that a bundle recorded under one crate version is refused by a different crate version — `matching-expert` is fixing this as a follow-up commit, not resolved as of this pass. |
| Seed-data / scenario file format | `src/config.rs` + `05-microstructure-config.md` | Yes | `seeds/default.toml` + `seeds/README.md` unchanged in shape since #24, extended additively through v0.5 |
| Database schema | `migrations/*.sql` | Yes | Six numbered, timestamp-prefixed migrations; none contain `DROP`/destructive `ALTER`/`RENAME` (grepped directly) |

**Additive-so-far is necessary but not sufficient for the SemVer.md
promotion.** Every surface above (bar the §0 replay gate, being fixed) has
never taken a breaking change — genuinely good evidence for whenever the
promotion happens. But `docs/SEMVER.md`'s actual gate is "shipped without a
breaking change for **one quarter**," and this repository has existed for
**eight days**, with no surface ever having shipped a real `0.x`. "Additive
so far" and "stable for one quarter" are different claims; this pass
verifies the first and does not — cannot yet — claim the second. That is
reason (1) the promotion is held (see above).

## 3. Pre-release checks (`docs/RELEASE-PROCESS.md` §1)

Run as a **readiness** check, not a cut checklist — the crate version was
not bumped, so several of these are "verified the mechanism works," not
"verified the release is ready to cut":

- [x] `make pre-push` (fmt / clippy / test / release build / doc) — see §7
      below for the actual run output.
- [ ] Every issue in the `v1.0` milestone closed — **not yet, and not
      expected to be by this pass**: `gh issue list --repo
      joaquinbejar/fauxchange --milestone v1.0 --state open` returns all
      five (#51–#55) open, because this repo's convention closes an issue
      only when its PR merges, and none of the four gate PRs
      (#124/#125/#127/#128) nor this readiness PR have merged yet. Reason
      (2) the promotion is held (see above).
- [x] `[Unreleased]` in `CHANGELOG.md` is non-empty (2,280+ lines of
      `### Added`/`### Security` entries) — **left exactly as-is**, not
      locked into a dated release section, because there is no release
      being cut yet.
- [x] `docker compose up` reaches a serving, seeded venue — evidenced by the
      `docker-smoke` CI job, confirmed green on all of #124/#125/#127/#128.
      A fresh local `make docker-smoke` was not re-run for this specific
      commit given the size of the multi-stage release build; the CI
      evidence above is the cited basis. Flagged, not hidden.
- [x] No `TODO`/`FIXME` mentioning a target version in tracked files
      (spot-checked; none found).
- [x] SEMVER.md rules hold against the previous tag (`v0.0.1`, itself the
      only tag that exists) for every **wire/API** surface — see §2.
- [ ] SEMVER.md rules hold for the **replay cross-version** commitment —
      **does not hold as implemented yet** — see §0, fix in flight, not
      silently checked off.
- [ ] "Shipped without a breaking change for one quarter" — **cannot be
      true of an eight-day-old repository**; not checked off, not silently
      assumed. See "why the promotion is held" above.

## 4. What this pass changed (and deliberately reverted)

- **`Cargo.toml` `[package].version` — unchanged, `0.0.1`.** This pass
  bumped it to `1.0.0` to exercise the real release mechanics (§6 found two
  genuine packaging bugs doing exactly that), then **reverted the bump**
  once the promotion was held — the bugs found and fixed along the way
  (§4 include-list fix, below) do not depend on which version ships.
- **`Cargo.lock` — unchanged**, `cargo update -p fauxchange` re-run after
  reverting `Cargo.toml` so the lockfile's own `fauxchange` entry matches
  `0.0.1` again (verified: `git diff HEAD~1 -- Cargo.lock` is empty).
- **`CHANGELOG.md` — unchanged.** `[Unreleased]` was never locked into a
  dated section; no fresh empty `[Unreleased]` was inserted on top of one,
  because there is no locked section to sit above.
- **Two adversarial-corpus fixtures — reverted to their original `0.0.1`
  form** (`tests/adversarial/bundle/newer_journal_schema.json`,
  `tests/adversarial/bundle/tampered_event.json`). They were regenerated
  once, while the crate was briefly at `1.0.0` during this pass, to make
  §0's finding directly reproducible; reverted so they load cleanly again
  against the actual `0.0.1` binary, exactly as they did before this pass.
  `matching-expert`'s follow-up adds a **new**, dedicated cross-version
  replay test rather than repurposing these two.
- **`Cargo.toml`'s `[package].include` — fixed, and kept.** This is a real
  bug, independent of the version question:
  - **Unanchored patterns leaked local-only files.** The prior
    `include = ["src/**/*", "Cargo.toml", "README.md", "LICENSE"]` used
    gitignore-style basename matching for the last three entries (no `/`),
    so `cargo package --list` (run for real) proved it packaged
    `docs/README.md`, `docs/specs/README.md`, all five
    `milestones/*/README.md`, `seeds/README.md`, and
    `tests/adversarial/README.md` — every one of which is meant to be
    local-only governance material (`docs/`/`milestones/` are `.gitignore`d
    outright). Fixed by anchoring: `["src/**/*", "/Cargo.toml",
    "/README.md", "/LICENSE"]`.
  - **`migrations/` and `.sqlx/` were never in `include` at all.** With the
    anchoring fixed, `cargo publish --dry-run` (the real verification
    build, not `--list`) failed to *compile* the repackaged tarball: 15
    errors, culminating in `error canonicalizing migration directory ...
    No such file or directory` from `sqlx::migrate!("./migrations")` — a
    **compile-time** macro. Both `migrations/` (6 tracked `.sql` files) and
    `.sqlx/` (11 tracked offline query-cache `.json` files) are real,
    committed, non-secret, production-necessary source, just missing from
    the publish allowlist. Fixed by adding `/migrations/**/*` and
    `/.sqlx/**/*` to `include`, each with an inline comment naming why.
  - **Consequence if this had shipped unfixed:** whenever `fauxchange` does
    promote and publish, `cargo install fauxchange` would have failed to
    compile on the very first published version — an immutable, `cargo
    publish`-can't-be-undone mistake requiring a same-day yank-and-reissue.
    Found and fixed now, independent of which version eventually ships;
    see §6 for the re-verified clean dry-run.
- **`Makefile`'s `check-spanish` target — narrowed, and kept.** Was
  flagging `"é".repeat(N)` in `src/gateway/fix/limits.rs` /
  `src/conformance/report.rs` (deliberate multi-byte-vs-byte-length test
  literals, English test code, not Spanish prose) as a violation — a
  pre-existing false positive `ci.yml` never catches (it doesn't run
  `check-spanish`), unrelated to the version question. Fixed by excluding
  the `"<content>".repeat(` idiom from the diacritics grep; does not
  weaken the check against real Spanish prose elsewhere.
- **`README.md` — regenerated, and kept.** `make readme`'s output changed
  by exactly one paragraph (the `[`conformance`]` module doc `src/lib.rs`
  already carried, which a stale `README.md` had never picked up) — a
  doc-sync fix, unrelated to the version question.
- **`.github/workflows/release.yml` — new, and kept.** See §5. It tags
  every artifact FROM `Cargo.toml`'s `[package].version` at cut time, so it
  needed no changes when the version bump was reverted — it is
  version-agnostic by construction.

## 5. Release automation (`.github/workflows/release.yml`)

**Did not exist before this pass** — verified by directory listing
(`.github/workflows/` held only `ci.yml` and `bench-regression.yml`)
before this change; **created** per issue #55's instruction to create it
if missing. **Kept** despite the promotion being held — it is exactly the
mechanism the human will trigger once a target version is chosen, and it
required no version-specific changes to remain correct.

- Trigger: `on: push: tags: ["v*"]` — tag-only, never a branch push (kept
  disjoint from `ci.yml`'s `push: branches: ["**"]`, so this workflow never
  fires on an ordinary commit).
- `verify` job (gates both publish jobs): re-runs the binding Pre-Submission
  Checklist (`fmt --check`, `clippy -D warnings`, `test --all-features`,
  `build --release`) on the tagged commit itself (never trusts a separate
  `ci.yml` run blindly), asserts the pushed tag's version matches
  `Cargo.toml`'s `[package].version`, runs `cargo package --list` (the file
  set), and — **not merely `--list`** — `cargo publish --dry-run` (rebuilds
  the packaged tarball in isolation and compiles it; no
  `CARGO_REGISTRY_TOKEN` needed, it never uploads). This distinction is not
  academic — see §6: `--list` alone would NOT have caught the real
  publish-blocking bug this pass found.
- `publish-crate`: `cargo publish` gated on `CARGO_REGISTRY_TOKEN` (repo
  secret, read via `env:`, never echoed to a log or interpolated into a
  shell command that could land in `set -x` output).
- `publish-image`: builds + pushes **both** runtime targets to
  `ghcr.io/joaquinbejar/fauxchange` — `runtime-slim` (the compose-pinned
  default) tagged `X.Y.Z` and `latest`, `runtime-distroless` tagged
  `X.Y.Z-distroless` (never `latest` — a distinct, non-default variant keeps
  its own tag namespace) — using the ambient `GITHUB_TOKEN`
  (`packages: write`, scoped to this job only). Re-runs
  `docker/scan-image-secrets.sh` against each image immediately before its
  `docker push` (docs/08 §7's no-baked-secrets gate, not merely the
  `image-build` CI job's earlier pass on a differently-tagged local build).
- `github-release`: waits on both publish jobs, extracts the matching
  `CHANGELOG.md` section verbatim as release notes (the same `awk` idiom
  `docs/RELEASE-PROCESS.md` §6c documents), and runs `gh release create`
  under `contents: write`.
- Pinning: `actions/checkout@v7`, `dtolnay/rust-toolchain@v1`,
  `Swatinem/rust-cache@v2.9.1` — the same pins already used by `ci.yml` /
  `bench-regression.yml`; no `docker/*-action`, no `softprops/action-gh-release`
  — the runner's preinstalled `docker`/`gh` CLIs are used directly (mirrors
  `ci.yml`'s own `image-build` job, which already documents this
  "no extra action to pin for a plain `docker build`" precedent). No image
  tag or action ref is `@master`/`@latest`.
- Validated **statically only** — no tag was pushed, no publish ran:
  - `actionlint` (with `shellcheck` wired in): clean, zero findings.
  - Parsed with Python's `yaml.safe_load`: valid YAML, 4 jobs
    (`verify`, `publish-crate`, `publish-image`, `github-release`).
  - `cargo publish --dry-run` and `cargo package --list` — see §6.

## 6. `cargo publish --dry-run` + metadata

**Two real, publish-blocking `[package].include` bugs were found and fixed
here (§4) — this is not a clean pass.** Both were found by actually running
the commands this issue requires, not by inspection, and both were
re-verified clean at the crate's actual, un-promoted `0.0.1` version.

### 6.1 Re-verified clean, at `0.0.1`

```
$ cargo package --list --allow-dirty
Cargo.toml.orig
.cargo_vcs_info.json
Cargo.lock
Cargo.toml
LICENSE
README.md
src/**/*   (every src/*.rs file, ~95 files)
migrations/*.sql   (6 files)
.sqlx/query-*.json (11 files)

$ cargo publish --dry-run --allow-dirty
    Updating crates.io index
    Packaged 116 files, 2.6MiB (637.7KiB compressed)
   Verifying fauxchange v0.0.1 (/Users/joaquin/Repos/Local/fauxchange)
   Compiling fauxchange v0.0.1 (/Users/joaquin/Repos/Local/fauxchange/target/package/fauxchange-0.0.1)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 21.82s
   Uploading fauxchange v0.0.1 (/Users/joaquin/Repos/Local/fauxchange)
warning: aborting upload due to dry run
```

(`--allow-dirty` is expected and correct here — this dry run was captured
*before* the amended commit; the real `release.yml` `verify` job runs
against a clean, tagged, committed checkout and needs no such flag. Cargo's
own "dirty" file discovery also lists gitignored `docs/`/`milestones/`
README stubs as "not committed" even though `git status` itself shows them
as ignored/clean — a cargo-vs-git-ignore-scope quirk, harmless here since
none of those paths are in `include` post-fix and so are never packaged
regardless.) `tests/`/`benches/` are correctly excluded from the package
(cargo prints an informational `warning: ignoring test ...` /
`warning: ignoring benchmark ...` per file — expected; a published binary
crate's consumers do not need the test suite or the bench harness).

**Note for whoever cuts the eventual release:** `fauxchange v0.0.1` is
*already published* on crates.io (the original name-reservation placeholder
— `CHANGELOG.md`'s `[0.0.1] - 2026-07-12` entry). `cargo publish` is
irreversible per version — this dry run above **cannot itself be turned
into the real publish**; the human bumps `Cargo.toml` to whichever target
version is actually chosen (`docs/RELEASE-PROCESS.md` §2) before running
`cargo publish` for real. This dry run only proves the packaging mechanics
(§4's two fixes) are sound at whatever version ships.

### 6.2 `[package]` metadata — otherwise complete

- `description`, `license = "MIT"` (+ `LICENSE` present), `repository` /
  `homepage` (both `https://github.com/joaquinbejar/fauxchange`),
  `readme = "README.md"` (regenerated via `make readme`, part of
  `make pre-push`), `authors`, `keywords` / `categories` (within crates.io's
  5-keyword / well-known-category limits) — all already correct; nothing
  else had to be added.

## 7. `make pre-push`

**First attempt failed — twice — both times for reasons the (since-reverted)
version bump exposed, both fixed independent of the version question; the
final run, at the crate's real `0.0.1`, is clean end to end
(`EXIT_CODE=0`).**

1. **`test` failed first**, while the crate was briefly at `1.0.0`, with two
   `tests/adversarial.rs` panics: `expected a SchemaRefused reject, got
   Err(VersionMismatch { kind: "fauxchange", expected: "1.0.0", found:
   "0.0.1" })` and the equivalent for the `JournalCorruption` case. This
   *is* §0's finding, reproduced directly — see §0. Both fixtures are now
   reverted to their original `0.0.1` form (§4), so this does not reproduce
   at the crate's actual version.
2. **`check-spanish` failed second** (after the fixture issue), flagging
   `"é".repeat(1_000)` / `"é".repeat(MAX_DETAIL_LEN)` in
   `src/gateway/fix/limits.rs` / `src/conformance/report.rs` — pre-existing
   English test code using a multi-byte character as deliberate test
   payload, a false positive in the Makefile heuristic never caught before
   because `ci.yml` does not run this check. Fixed (§4) — kept regardless
   of the version question.
3. **Final run, clean throughout**, at the crate's real `0.0.1`
   (`LOGLEVEL=WARN`, all-features, `RUSTFLAGS=-D warnings` on the release
   build):
   - `fix` (`cargo fix --all-targets --all-features`) — clean.
   - `fmt` — clean.
   - `lint-fix` (`cargo clippy --all-targets --all-features --fix -- -D
     warnings`) — clean.
   - `test` (`cargo test --all-features`) — clean; 856 unit tests + every
     integration-test binary (`actor`, `adversarial` — including the two
     restored fixtures — `auth`, `conformance_harness`, `determinism`,
     `dos_controls`, `fix_acceptor`, `fix_adversarial`, `fix_session`,
     `golden`, `golden_fix`, `integration`, `market_maker`, `order_path`,
     `parity`, `property`, `requote_isolation`, `rest`,
     `scenario_failure_modes`, `security`, `seed`, `simulation`, `snapshot`,
     `state`, `stores`, `subscription`, `ws`) + 28 doctests, all green.
   - `check-spanish` — clean (now correctly excludes the `.repeat()` test
     idiom).
   - `release` (`cargo build --release`, `RUSTFLAGS=-D warnings`) — clean,
     zero warnings.
   - `readme` (`cargo readme > README.md`) — regenerated; the only diff vs.
     the pre-pass file is the `[`conformance`]` module-doc paragraph
     `src/lib.rs` already carries but a stale `README.md` had never picked
     up (a 5-line addition, nothing removed).
   - `doc` (`cargo clippy --all-features -- -W missing-docs`) — clean.
   - Exit code `0`; `pre-push: done` printed.

**Observation for `architect`, out of DevOps scope (not touched — `src/` is
off-limits to this PR):** `src/lib.rs`'s `//! ## Status` block still reads
"Under active design and early implementation ... see `docs/ROADMAP.md` for
the delivery plan starting at v0.1.0," which `make readme` propagates
verbatim into `README.md`. Given the promotion is deliberately held, this
line is arguably still accurate as written for now — flagged for
`architect` to revisit whenever the actual promotion is cut, not silently
edited here.

## 8. Remaining for the human

This PR prepares release **readiness**; it does **not** bump the version,
lock the changelog, publish, tag, or merge anything. In order, still to do:

0. **Land `matching-expert`'s §0 fix** — the schema-tag (+ same-major)
   replay gate — as the next commit on `stack/55-v1-release`.
1. **Review and merge the stack bottom-up:** #124 (issue #51) → #125 (#52) →
   #127 (#53) → #128 (#54) → this readiness PR (#55, once §0's fix lands)
   → `main`. Every PR is individually CI-green (confirmed for all four,
   including #128, which finished green during this pass).
2. **Decide the target version and promote `Cargo.toml`.** This pass
   deliberately left the crate at `0.0.1` — the human chooses the actual
   version (`1.0.0`, once the "one quarter" gate is genuinely met; an
   earlier `0.x`; or something else) and runs
   `docs/RELEASE-PROCESS.md` §2–§3 (`sed` the version, `cargo update -p
   fauxchange`, lock `CHANGELOG.md`'s `[Unreleased]` into a dated section)
   at that time. **`fauxchange v0.0.1` is already published on crates.io**
   — `cargo publish` is irreversible per version, so whatever version is
   chosen must be a version that has never been published before.
3. **Provision `CARGO_REGISTRY_TOKEN`** as a repository secret before the
   first tag push — `gh secret list --repo joaquinbejar/fauxchange`
   currently returns **no secrets configured**, so `release.yml`'s
   `publish-crate` job would fail today if a tag were pushed. `GITHUB_TOKEN`
   for `ghcr.io` + the GitHub Release needs no separate provisioning (the
   workflow's own ambient token, scoped by the job `permissions:` blocks).
4. **Push the branch, then the annotated tag matching the chosen version**
   (never the reverse — `docs/RELEASE-PROCESS.md` §5): `git push origin
   main` then `git tag -a v<X.Y.Z> -m "..."` / `git push origin v<X.Y.Z>`.
   The tag push triggers `.github/workflows/release.yml`, which — using
   repo secrets — publishes the crate to crates.io, both image variants to
   `ghcr.io`, and the GitHub Release.
5. **Post-release sanity** (`docs/RELEASE-PROCESS.md` §7): `cargo install
   fauxchange --version <X.Y.Z>` from a clean cache; `docker run
   ghcr.io/joaquinbejar/fauxchange:<X.Y.Z> --version` reports `<X.Y.Z>`;
   `docker compose up` against the tagged image reaches a serving venue.

None of steps 1–5 above were performed by this PR — no version was bumped,
no tag was pushed, no `cargo publish` (real or otherwise beyond
`--dry-run`) ran, no Docker image was pushed to any registry, and nothing
was merged.
