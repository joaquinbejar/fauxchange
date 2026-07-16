//! Bootstrap entry point for the `fauxchange` binary.
//!
//! This wires the minimum honest bootstrap of the REST gateway: load the layered
//! venue [`Config`](fauxchange::config::Config) (defaults → file → environment →
//! CLI), install a `tracing` subscriber, log the **effective config once at boot
//! with secrets redacted**, build the shared [`AppState`](fauxchange::state::AppState)
//! from that config, then serve the router
//! ([`fauxchange::gateway::rest::serve`]) with the rate-limit sweeper and the
//! real-socket-peer connect-info. The fuller bootstrap sequence — structured/JSON
//! log output (observability #06), the seed manifest (#024), the durable DB pool
//! (#023), and the WS/FIX gateways + background tasks — lands with the modules
//! that own it; this file grows with them.
//!
//! **Security posture.** The embedded dev JWT keypair is refused in a released
//! image unless dev mode is set (`FAUXCHANGE_DEV=1`), via the
//! [`JwtAuth::release_gated`](fauxchange::auth::JwtAuth::release_gated) gate — so
//! a published image never runs on the well-known dev keys by default. Token
//! issuance additionally requires `AUTH_BOOTSTRAP_SECRET` (config `[auth]`) and a
//! provisioned account (operator config). The bootstrap secret and the
//! `DATABASE_URL` are wrapped in [`Secret`](fauxchange::config::Secret) and never
//! logged.
//!
//! Configuration (layered defaults → `--config <file>` → env → CLI flags):
//! - `[server]` `FAUXCHANGE_HTTP_ADDR` / `--http-addr` — REST/WS bind (default `0.0.0.0:8080`).
//! - `[fix]` `FAUXCHANGE_FIX_ADDR` / `--fix-addr` — FIX bind (default `0.0.0.0:9878`).
//! - `[persistence]` `DATABASE_URL` / `--database-url` — unset ⇒ in-memory (#023 consumes it).
//! - `[clock]` `FAUXCHANGE_CLOCK` / `--clock` — `realtime` | `accelerated` | `stepped` (#28).
//! - `[determinism]` `FAUXCHANGE_SEED` / `--seed` — one run-level seed → run lineage.
//! - `[auth]` `AUTH_BOOTSTRAP_SECRET` — gates `POST /api/v1/auth/token`.
//! - `[logging]` `FAUXCHANGE_LOG_FORMAT` / `--log-format` — `json` | `pretty` (JSON emission #06).
//! - `FAUXCHANGE_UNDERLYINGS` — comma-separated underlyings (default `BTC,ETH`;
//!   the declarative `[instruments.*]` seed is #024).
//! - `FAUXCHANGE_DEV` — `1`/`true` admits the dev JWT keypair for local use.

use fauxchange::auth::{DevMode, JwtAuth};
use fauxchange::config::Config;
use fauxchange::db::{DatabasePool, DbPoolConfig};
use fauxchange::gateway::rest;
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load + validate the layered config BEFORE anything else — a bad address,
    // clock, seed, unknown key, or unknown flag fails the process fast here.
    let config = Config::load()?;

    // Install the `tracing` subscriber next — without one every event is dropped.
    // Filter by `RUST_LOG`, defaulting to `info` for the crate. The `[logging]`
    // format is carried through and logged below; the subscriber that emits true
    // structured JSON is the observability milestone's (#06 §9).
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // The effective config, logged once at boot with secrets redacted, so a run
    // is self-describing (docs/06 §4). `render_effective` never emits the
    // bootstrap secret or the DATABASE_URL.
    tracing::info!(
        effective = %config.render_effective(),
        backend = %config.persistence.backend().as_str(),
        clock = %config.clock.mode.as_str(),
        log_format = %config.logging.format.as_str(),
        "effective venue config at boot"
    );

    // Underlyings are seeded from the env for now; the declarative
    // `[instruments.*]` manifest lands with the seed (#024).
    let underlyings: Vec<String> = std::env::var("FAUXCHANGE_UNDERLYINGS")
        .unwrap_or_else(|_| "BTC,ETH".to_string())
        .split(',')
        .map(|ticker| ticker.trim().to_string())
        .filter(|ticker| !ticker.is_empty())
        .collect();

    // Dev keypair, refused in a released image unless dev mode is set.
    let jwt = JwtAuth::dev()?.release_gated(DevMode::from_env())?;
    let mut auth = AuthConfig::with_jwt(jwt);
    // Token issuance gate: an unset `AUTH_BOOTSTRAP_SECRET` disables it. The
    // config wraps it in `Secret`; expose the plaintext only here, for the gate.
    if let Some(secret) = config.auth.bootstrap_secret_value() {
        auth = auth.with_bootstrap_secret(secret);
    }

    // Optional durable persistence (#023): when `DATABASE_URL` is set, open the
    // `PgPool` and run `sqlx::migrate!("./migrations")` at boot; unset ⇒ fully
    // in-memory. The URL is exposed from the `Secret` at this one legitimate
    // consumer and is NEVER logged. Pool size + slow-acquire threshold come from
    // config, not hard-coded. "Both modes serve" — the venue starts either way.
    let db = match config.persistence.connection_url() {
        Some(url) => {
            let pool_config = DbPoolConfig::from_persistence(&config.persistence);
            let pool = DatabasePool::connect_and_migrate(url, pool_config).await?;
            tracing::info!("durable persistence enabled; migrations applied at boot");
            Some(pool)
        }
        None => {
            tracing::info!("no DATABASE_URL; running fully in-memory persistence");
            None
        }
    };

    // The run lineage is derived from the seed, so ids namespace per seed.
    let app_config = AppStateConfig::new(underlyings)
        .with_lineage(config.determinism.lineage_id())
        .with_auth(auth)
        .with_db(db);
    let state = AppState::new(app_config)?;
    tracing::info!(
        underlyings = state.underlying_count(),
        durable = state.is_persistent(),
        "AppState assembled"
    );

    rest::serve(state, config.server.http_addr).await?;
    Ok(())
}
