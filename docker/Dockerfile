# docker/Dockerfile — the fauxchange exchange-in-a-box: ONE crate, ONE
# binary, ONE container image (docs/06-deployment.md §2) — NOT the Backend's
# build-per-service compose, and NOT the simulator's ClickHouse/Redis/Mongo
# stack. Multi-stage: a pinned Rust builder produces the single `fauxchange`
# release binary, copied into a slim runtime base.
#
# Scope note (#25): this is the WORKING image — non-root user, distroless
# variant, read-only rootfs, dropped capabilities, and the dev-key
# release-refusal hardening test are #26, layered on top of this file without
# reshaping it.
#
# Build:   docker build -f docker/Dockerfile -t fauxchange:local .
#          (build context is the REPO ROOT — see .dockerignore)
# Run:     docker run --rm -p 8080:8080 -p 9878:9878 fauxchange:local
#          NOTE (honest gap, see docker-compose.yml): `main.rs` always signs
#          with the embedded JwtAuth::dev() keypair, gated by FAUXCHANGE_DEV
#          (src/auth.rs `release_gated`); it does not yet load a real RS256
#          key pair from mounted paths (tracked; lands with #26's hardening
#          posture). Until then a bare `docker run` with no FAUXCHANGE_DEV
#          set makes the process EXIT at startup (`DevKeyRefused`) before the
#          venue serves — the container never becomes healthy — so pass
#          `-e FAUXCHANGE_DEV=1` for local use, exactly as docker-compose.yml
#          does for the one-command dev distribution.
# Compose: docker compose -f docker/docker-compose.yml up

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
#                                    stage below never reads this directory again.
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
# Stage 2: runtime
# =============================================================================
# debian:bookworm-slim, deliberately NOT alpine: the crate depends on `ring`
# (via `jsonwebtoken`'s RS256 backend and sqlx's `tls-rustls-ring` feature,
# Cargo.toml), whose musl cross-build/runtime story on alpine is a documented
# source of friction; glibc (debian-slim) avoids it without verification work.
# Non-root / distroless / read-only-rootfs hardening is #26 on top of this.
FROM debian:bookworm-slim AS runtime

# `ca-certificates` — any outbound TLS the binary makes (sqlx's rustls stack
# when DATABASE_URL is set, or a future outbound call) needs a trust store.
# `curl` — the HEALTHCHECK below; the slim base ships neither by default.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# The single binary.
COPY --from=builder /build/target/release/fauxchange /usr/local/bin/fauxchange

# The default scenario seed manifest (docs/06 §7, #24 `src/seed.rs`) — the
# `--config` target for the venue's own bounded SEEDING PHASE, which applies
# it IN-PROCESS at boot before flipping to serving. Baked in so the image is
# self-contained (a `docker run` with `--config /app/seeds/default.toml`
# needs no host mount); docker-compose.yml passes that flag by default so
# `docker compose up` yields a seeded venue with no extra step. NOT a secret:
# the FIX password inside is a clearly-labelled dev fixture (seeds/default.toml
# header), hashed with Argon2id and dropped at provisioning — never logged.
COPY seeds/default.toml /app/seeds/default.toml

# Ports (docs/06 §5):
#   8080 — REST + WebSocket. LIVE today (src/gateway/rest/serve).
#   9878 — FIX 4.4. RESERVED — the acceptor is v0.4 (src/gateway/fix is a stub
#          today); nothing binds this port yet. Exposed now so the topology is
#          documented and stable once it lands.
#   9090 — Prometheus metrics. RESERVED — no metrics server exists yet
#          (docs/06 §9, observability milestone); exposed for the same reason.
EXPOSE 8080 9878 9090

# `GET /health` is auth- and rate-limit-exempt and answers unconditionally
# (src/gateway/rest/meta.rs) — the container health probe.
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/health || exit 1

# Config is injected at RUN TIME ONLY — env vars (DATABASE_URL,
# AUTH_BOOTSTRAP_SECRET, FAUXCHANGE_*) or a mounted/baked `--config` file, per
# `docker-compose.yml` / the operator's own compose override. Nothing above
# bakes a secret or a deployment-specific choice into a layer.
ENTRYPOINT ["/usr/local/bin/fauxchange"]
CMD []
