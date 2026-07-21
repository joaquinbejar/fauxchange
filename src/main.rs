//! Bootstrap entry point for the `fauxchange` binary.
//!
//! This wires the minimum honest bootstrap of the REST gateway (#013): install a
//! `tracing` subscriber, build the shared [`AppState`](fauxchange::state::AppState),
//! then serve the router ([`fauxchange::gateway::rest::serve`]) with the
//! rate-limit sweeper and the real-socket-peer connect-info. The fuller bootstrap
//! sequence — declarative venue config, structured/JSON log output, account
//! provisioning, and the WS/FIX gateways + background tasks — lands with the
//! modules that own it (`config` #022, observability #06, WS #014, FIX v0.4);
//! this file grows with them.
//!
//! **Security posture.** The embedded dev JWT keypair is refused in a released
//! image unless dev mode is set (`FAUXCHANGE_DEV=1`), via the
//! [`JwtAuth::release_gated`](fauxchange::auth::JwtAuth::release_gated) gate — so
//! a published image never runs on the well-known dev keys by default. Token
//! issuance additionally requires `AUTH_BOOTSTRAP_SECRET` and a provisioned
//! account (operator config).
//!
//! Environment:
//! - `FAUXCHANGE_REST_ADDR` — REST bind address (default `127.0.0.1:8080`).
//! - `FAUXCHANGE_UNDERLYINGS` — comma-separated underlyings (default `BTC,ETH`).
//! - `FAUXCHANGE_DEV` — `1`/`true` admits the dev JWT keypair for local use.
//! - `AUTH_BOOTSTRAP_SECRET` — gates `POST /api/v1/auth/token`.

use std::net::SocketAddr;

use fauxchange::auth::{DevMode, JwtAuth};
use fauxchange::gateway::rest;
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install the `tracing` subscriber FIRST — without one every event is
    // dropped. Filter by `RUST_LOG`, defaulting to `info` for the crate.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let addr: SocketAddr = std::env::var("FAUXCHANGE_REST_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()?;

    let underlyings: Vec<String> = std::env::var("FAUXCHANGE_UNDERLYINGS")
        .unwrap_or_else(|_| "BTC,ETH".to_string())
        .split(',')
        .map(|ticker| ticker.trim().to_string())
        .filter(|ticker| !ticker.is_empty())
        .collect();

    // Dev keypair, refused in a released image unless dev mode is set.
    let jwt = JwtAuth::dev()?.release_gated(DevMode::from_env())?;
    let mut auth = AuthConfig::with_jwt(jwt);
    // Token issuance gate: unset or empty `AUTH_BOOTSTRAP_SECRET` disables it.
    if let Ok(secret) = std::env::var("AUTH_BOOTSTRAP_SECRET")
        && !secret.is_empty()
    {
        auth = auth.with_bootstrap_secret(secret);
    }

    let state = AppState::new(AppStateConfig::new(underlyings).with_auth(auth))?;
    tracing::info!(underlyings = state.underlying_count(), "AppState assembled");

    rest::serve(state, addr).await?;
    Ok(())
}
