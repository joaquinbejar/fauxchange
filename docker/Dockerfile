# docker/Dockerfile — the fauxchange exchange-in-a-box: ONE crate, ONE
# binary, ONE container image (docs/06-deployment.md §2) — NOT the Backend's
# build-per-service compose, and NOT the simulator's ClickHouse/Redis/Mongo
# stack. Multi-stage: a pinned Rust builder produces the single `fauxchange`
# release binary, copied into a slim runtime base.
#
# Scope note (#26, hardening on top of #25's working image): TWO runtime
# targets share the one `builder` stage —
#
#   runtime-slim        (DEFAULT target — the last stage in this file, so a
#                        plain `docker build` / `docker compose build` with no
#                        `--target` still resolves here, matching #25) —
#                        `debian:bookworm-slim` + `curl` (the HEALTHCHECK
#                        below), non-root.
#   runtime-distroless   `gcr.io/distroless/cc-debian12:nonroot`, PINNED by
#                        digest — no shell, no package manager, no curl. Build
#                        it explicitly:
#                          docker build -f docker/Dockerfile \
#                            --target runtime-distroless -t fauxchange:distroless .
#                        It has NO `HEALTHCHECK` (there is no shell/curl to
#                        run one against `/health` from inside the container —
#                        an honest tradeoff, not an oversight; see the stage
#                        comment below) — an orchestrator's own HTTP probe
#                        (Kubernetes `httpGet`, a sidecar, `docker compose`'s
#                        OWN healthcheck stanza pointed at a *different*
#                        checker container) covers it instead.
#
# Both targets run as the SAME unprivileged uid/gid (65532, the conventional
# distroless "nonroot" id) so the run posture — non-root, read-only rootfs,
# dropped capabilities (docker-compose.yml) — is identical across variants;
# only the base image (and therefore the presence of a shell) differs.
#
# Build:   docker build -f docker/Dockerfile -t fauxchange:local .
#          (build context is the REPO ROOT — see .dockerignore; targets
#          `runtime-slim` by default, `--target runtime-distroless` for the
#          minimal variant)
# Run:     docker run --rm -p 8080:8080 -p 9878:9878 fauxchange:local
#          NOTE (honest gap, see docker-compose.yml): `main.rs` always signs
#          with the embedded JwtAuth::dev() keypair, gated by FAUXCHANGE_DEV
#          (src/auth.rs `release_gated`); it does not yet load a real RS256
#          key pair from mounted paths (tracked; a real deployment mounts a
#          production key pair and does NOT set FAUXCHANGE_DEV — the
#          `DevKeyRefused` release gate this file's non-root/no-secrets
#          hardening backs is #26). Until then a bare `docker run` with no
#          FAUXCHANGE_DEV set makes the process EXIT at startup
#          (`DevKeyRefused`) before the venue serves — the container never
#          becomes healthy — so pass `-e FAUXCHANGE_DEV=1` for local use,
#          exactly as docker-compose.yml does for the one-command dev
#          distribution.
# Compose: docker compose -f docker/docker-compose.yml up
#          (compose builds `runtime-slim` explicitly — `target:` in the
#          service's `build:` block, docker-compose.yml)

# =============================================================================
# Stage 1: builder
# =============================================================================
# Pinned to the EXACT toolchain in rust-toolchain.toml — bump both together,
# deliberately (rust-toolchain.toml's own header explains the MSRV floor).
FROM rust:1.97.0-bookworm AS builder

# Offline sqlx: the release build resolves the compile-time `sqlx::query!` /
# `sqlx::migrate!` macros from the COMMITTED `.sqlx/` cache, never a live DB
# connection at build time — matches CI's `SQLX_OFFLINE: "true"`
# (.github/workflows/ci.yml). Fail on warnings, mirroring the Pre-Submission
# Checklist's `cargo build --release` (zero warnings) gate.
ENV SQLX_OFFLINE=true \
    RUSTFLAGS="-D warnings" \
    CARGO_TERM_COLOR=always

WORKDIR /build

# Copy only what `cargo build --release` needs to resolve the manifest and
# compile the single `fauxchange` binary:
#   - Cargo.toml / Cargo.lock     — the dependency graph (locked).
#   - .sqlx/                      — the offline query-checking cache (SQLX_OFFLINE).
#   - migrations/                 — `sqlx::migrate!("./migrations")` (src/db/pool.rs)
#                                    embeds these at COMPILE time; the runtime
#                                    stages below never read this directory again.
#   - benches/                    — Cargo.toml's `[[bench]]` entries declare
#                                    explicit paths Cargo resolves when parsing
#                                    the manifest, even though a plain
#                                    `cargo build --release` (no `--benches`)
#                                    never compiles them.
#   - README.md / LICENSE         — referenced by Cargo.toml package metadata.
#   - src/                        — the crate source.
COPY Cargo.toml Cargo.lock ./
COPY .sqlx ./.sqlx
COPY migrations ./migrations
COPY benches ./benches
COPY README.md LICENSE ./
COPY src ./src

RUN cargo build --release --locked --bin fauxchange && \
    strip target/release/fauxchange

# =============================================================================
# Stage 2: runtime-distroless — minimal, no shell / package manager (#26)
# =============================================================================
# `gcr.io/distroless/cc-debian12:nonroot`, PINNED BY DIGEST (never `latest`;
# the `:nonroot` TAG is kept alongside the digest purely for human
# readability — the digest is what actually resolves the build). Verified
# locally (docker buildx imagetools inspect) as the manifest-LIST digest, so
# the correct per-platform image still resolves on amd64/arm64 CI/dev hosts.
#
# `cc-debian12` (not `static-debian12` / `base-debian12`) is the variant that
# ships glibc + `libgcc_s.so.1` + `libstdc++` — verified against the built
# binary's actual dynamic dependencies (`ldd target/release/fauxchange`):
# `libgcc_s.so.1`, `libm.so.6`, `libc.so.6`, `ld-linux*.so` — exactly what
# `cc-debian12` provides and what motivated debian-slim (not alpine/musl) for
# the builder in the first place (`ring`, see the builder/runtime-slim
# comments). It ALSO ships `/etc/ssl/certs/ca-certificates.crt`
# (`SSL_CERT_FILE` is set in its own image config) — sqlx's `tls-rustls-ring`
# outbound TLS to Postgres needs a trust store, same as runtime-slim's
# `ca-certificates` package.
#
# The `:nonroot` variant runs as uid/gid 65532 BY DEFAULT (its own image
# config sets `USER 65532`) — no `useradd`/`groupadd` is possible here (no
# shell, no package manager), so this hardening rides the base image's own
# non-root default instead of a Dockerfile `RUN`. `--chown=65532:65532` on
# every `COPY` below makes that explicit rather than relying on the default
# alone.
#
# HONEST GAP: no `HEALTHCHECK`. The #25 healthcheck runs `curl` inside the
# container; this base has neither `curl` nor a shell for `CMD-SHELL`, and
# adding a `--healthcheck` CLI subcommand to the binary is a `src/` change
# out of #26's `docker/` + `.github/workflows/` scope. The venue's own
# `docker-compose.yml` therefore builds `runtime-slim` (not this stage) so
# the one-command distribution keeps a working healthcheck; an operator who
# deploys THIS stage supplies their own liveness/readiness probe (Kubernetes
# `httpGet` against `/health`, an external checker) instead of a
# container-native `HEALTHCHECK`.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:66aa873a4a14fb164aa01296058efd8253744606d72715e45acface073359faa AS runtime-distroless

WORKDIR /app

# The single binary.
COPY --from=builder --chown=65532:65532 /build/target/release/fauxchange /usr/local/bin/fauxchange

# The default scenario seed manifest — see the identical note on
# `runtime-slim` below (same file, same rationale, same NOT-a-secret status).
COPY --chown=65532:65532 seeds/default.toml /app/seeds/default.toml

# Ports (docs/06 §5) — identical across both runtime targets, see runtime-slim.
EXPOSE 8080 9878 9090

# Already the base image's default (`USER 65532` baked into the `:nonroot`
# tag's own config) — restated explicitly so it survives a future FROM swap.
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/fauxchange"]
CMD []

# =============================================================================
# Stage 3: runtime-slim — DEFAULT target (curl HEALTHCHECK, debian-slim)
# =============================================================================
# debian:bookworm-slim, deliberately NOT alpine: the crate depends on `ring`
# (via `jsonwebtoken`'s RS256 backend and sqlx's `tls-rustls-ring` feature,
# Cargo.toml), whose musl cross-build/runtime story on alpine is a documented
# source of friction; glibc (debian-slim) avoids it without verification work.
#
# This is the LAST stage in the file — the implicit default when `docker
# build` / `docker compose build` is run with no `--target` — so the existing
# #25 one-command distribution (docker-compose.yml) and the CI `image-build`
# job's default `docker build` keep resolving here unchanged; only an
# explicit `--target runtime-distroless` reaches the minimal variant above.
FROM debian:bookworm-slim AS runtime-slim

# `ca-certificates` — any outbound TLS the binary makes (sqlx's rustls stack
# when DATABASE_URL is set, or a future outbound call) needs a trust store.
# `curl` — the HEALTHCHECK below; the slim base ships neither by default.
#
# Non-root (#26): a fixed uid/gid 65532 — the same id the distroless
# `:nonroot` tag uses by convention above, so both runtime variants present an
# IDENTICAL uid/gid to an orchestrator's PodSecurityContext / compose `user:`
# override, whichever base is deployed. `useradd` prints a benign RANGE
# warning ("outside of the UID_MIN/UID_MAX range" — verified locally) because
# 65532 falls above Debian's default 1000-60000 allocation window; the
# explicit `--uid` still WINS and the account is created at exactly 65532
# (verified: `docker run --entrypoint id` reports `uid=65532(fauxchange)`) —
# not a build failure, just `useradd` sanity-checking its default range.
# `--no-create-home` + `--shell /usr/sbin/nologin` — this is a service
# account, never an interactive login.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 fauxchange \
    && useradd --uid 65532 --gid fauxchange --no-create-home \
       --shell /usr/sbin/nologin fauxchange

WORKDIR /app

# The single binary.
COPY --from=builder --chown=65532:65532 /build/target/release/fauxchange /usr/local/bin/fauxchange

# The default scenario seed manifest (docs/06 §7, #24 `src/seed.rs`) — the
# `--config` target for the venue's own bounded SEEDING PHASE, which applies
# it IN-PROCESS at boot before flipping to serving. Baked in so the image is
# self-contained (a `docker run` with `--config /app/seeds/default.toml`
# needs no host mount); docker-compose.yml passes that flag by default so
# `docker compose up` yields a seeded venue with no extra step. NOT a secret:
# the FIX password inside is a clearly-labelled dev fixture (seeds/default.toml
# header), hashed with Argon2id and dropped at provisioning — never logged.
# `docker/scan-image-secrets.sh` (#26) whitelists exactly this one known
# fixture string and fails on anything else that looks like a secret.
COPY --chown=65532:65532 seeds/default.toml /app/seeds/default.toml

# Ports (docs/06 §5):
#   8080 — REST + WebSocket. LIVE today (src/gateway/rest/serve).
#   9878 — FIX 4.4. RESERVED — the acceptor is v0.4 (src/gateway/fix is a stub
#          today); nothing binds this port yet. Exposed now so the topology is
#          documented and stable once it lands.
#   9090 — Prometheus metrics. RESERVED — no metrics server exists yet
#          (docs/06 §9, observability milestone); exposed for the same
#          reason. When it lands it MUST bind `127.0.0.1` by default (#26
#          hardening assertion — docker-compose.yml already loopback-publishes
#          this port, docs/06 §5).
EXPOSE 8080 9878 9090

# Drop root for everything from here on — the process itself, and anything a
# future `RUN` in this stage would do (there is none after this point).
USER 65532:65532

# `GET /health` is auth- and rate-limit-exempt and answers unconditionally
# (src/gateway/rest/meta.rs) — the container health probe.
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/health || exit 1

# Config is injected at RUN TIME ONLY — env vars (DATABASE_URL,
# AUTH_BOOTSTRAP_SECRET, FAUXCHANGE_*) or a mounted/baked `--config` file, per
# `docker-compose.yml` / the operator's own compose override. Nothing above
# bakes a secret or a deployment-specific choice into a layer
# (`docker/scan-image-secrets.sh` asserts this, #26).
ENTRYPOINT ["/usr/local/bin/fauxchange"]
CMD []
