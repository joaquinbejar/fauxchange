//! Bootstrap entry point for the `fauxchange` binary.
//!
//! This wires the minimum honest bootstrap of the REST gateway: load the layered
//! venue [`Config`](fauxchange::config::Config) (defaults → file → environment →
//! CLI), install a `tracing` subscriber, log the **effective config once at boot
//! with secrets redacted**, build the shared [`AppState`](fauxchange::state::AppState)
//! from that config, then serve the router
//! ([`fauxchange::gateway::rest::serve`]) with the rate-limit sweeper and the
//! real-socket-peer connect-info. As of #024 it also runs the bounded **seeding
//! phase**: the venue is assembled in the seeding phase, the scenario manifest
//! ([`Config::seed`](fauxchange::config::Config)) is applied in fixed order
//! ([`fauxchange::seed::apply_seed_phase`]), and the venue flips to serving before
//! it binds. As of #037 it also spawns the **FIX 4.4 acceptor** when `[fix]
//! enabled` is set (the raw-TCP accept loop over IronFix's `FixCodec`, with a
//! logging stub at the dispatch seam the #038 session FSM replaces); the acceptor
//! is disabled by default and drained on shutdown. The fuller bootstrap sequence —
//! structured/JSON log output (observability #06) and the remaining background
//! tasks — lands with the modules that own it; this file grows with them.
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
//! - `FAUXCHANGE_UNDERLYINGS` — comma-separated underlyings (default `BTC,ETH`),
//!   the fallback when the config file declares no `[instruments.*]` seed.
//! - `[accounts.*]` / `[instruments.*]` / `[market_maker.*]` (config file only) —
//!   the scenario seed manifest (#024) applied by the bounded seeding phase.
//! - `FAUXCHANGE_DEV` — `1`/`true` admits the dev JWT keypair for local use.

use std::sync::Arc;

use fauxchange::auth::{DevMode, JwtAuth};
use fauxchange::config::Config;
use fauxchange::db::{DatabasePool, DbPoolConfig};
use fauxchange::gateway::fix::{
    FixAcceptor, FixAcceptorConfig, FixSessionStore, InMemoryFixSessionStore, SessionConfig,
    VenueFixSessionFactory,
};
use fauxchange::gateway::rest;
use fauxchange::seed;
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Subcommand dispatch: the `conformance` verb runs the packaged cross-surface
    // parity + conformance harness (#051) instead of serving, so a consumer can
    // gate a CI run on the venue's protocol correctness with ONE command. It is
    // intercepted before `Config::load` (which is the serve path's layered CLI).
    if std::env::args().nth(1).as_deref() == Some("conformance") {
        return run_conformance(std::env::args().skip(2)).await;
    }

    serve().await
}

/// The default path: load the layered venue config, install tracing, seed the
/// venue, and serve the REST/WS (and optional FIX) gateways.
async fn serve() -> Result<(), Box<dyn std::error::Error>> {
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

    // The scenario seed manifest (#024) rode through the layered loader onto
    // `config.seed`. When it declares instruments they define the hosted
    // underlyings; otherwise the venue falls back to the `FAUXCHANGE_UNDERLYINGS`
    // env list (an empty-manifest, no-seeded-instruments run).
    let manifest = &config.seed;
    let underlyings: Vec<String> = if manifest.is_empty() {
        std::env::var("FAUXCHANGE_UNDERLYINGS")
            .unwrap_or_else(|_| "BTC,ETH".to_string())
            .split(',')
            .map(|ticker| ticker.trim().to_string())
            .filter(|ticker| !ticker.is_empty())
            .collect()
    } else {
        manifest.underlyings()
    };

    // Dev keypair, refused in a released image unless dev mode is set.
    let jwt = JwtAuth::dev()?.release_gated(DevMode::from_env())?;
    // The per-tier venue rate-limit budgets (#046) — resolved and validated by the
    // config loader; wired into the venue-clock rate limiter so throttling is
    // configurable per deployment and replays deterministically.
    let mut auth =
        AuthConfig::with_jwt(jwt).with_rate_limit_budgets(config.rate_limits.to_budgets());
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

    // The run lineage is derived from the seed, so ids namespace per seed. The
    // venue starts in the bounded SEEDING phase (`with_serving(false)`): the seed
    // manifest is applied before any traffic, then the venue flips to serving.
    // Accounts are provisioned by the seeding phase (not at construction), so the
    // registry starts empty here.
    //
    // SECURITY: extract everything still needed AFTER seeding into owned locals so
    // the `config` — whose seed manifest holds the plaintext `[accounts.*]` FIX
    // passwords — can be dropped PROMPTLY once the seeding phase has hashed them
    // into the registry, rather than living for the whole process lifetime (it was
    // previously read again at `rest::serve` at the very end).
    let http_addr = config.server.http_addr;
    // `FixConfig` is `Copy`, so the FIX gateway settings are lifted into an owned
    // local BEFORE `config` is dropped (below) — the acceptor is spawned after the
    // venue flips to serving.
    let fix_config = config.fix;
    let lineage = config.determinism.lineage_id();
    let assets = seed::asset_configs(manifest);
    let manifest_summary = manifest.summary(); // secret-free counts only
    // The venue clock (#028): map the `[clock]` mode + knobs onto the runtime
    // clock config, pinning its virtual epoch to the price-walk epoch, and record
    // the run seed in the run manifest. The chosen mode drives `venue_ts`, the
    // simulator cadence, and the rate limiter.
    let venue_clock = config
        .clock
        .to_venue_clock_config(fauxchange::simulation::DEFAULT_CLOCK_START_MS);
    let seed = config.determinism.seed;
    // The resolved, validated venue microstructure (#044) — extracted into an owned
    // local (like `assets` / `lineage`) so it survives the prompt `drop(config)`
    // after seeding. Applied to every book at creation and recorded in the run
    // manifest fingerprint.
    let microstructure = config.microstructure.clone();
    let app_config = AppStateConfig::new(underlyings)
        .with_lineage(lineage)
        .with_clock(venue_clock)
        .with_seed(seed)
        .with_auth(auth)
        .with_assets(assets)
        .with_db(db)
        .with_serving(false)
        .with_microstructure(microstructure);
    let state = AppState::new(app_config)?;

    // The bounded seeding phase: apply the manifest in fixed order (accounts,
    // instruments, opening prices, personas) BEFORE the venue serves. This is the
    // LAST use of `manifest` (a borrow of `config`).
    let report = seed::apply_seed_phase(&state, manifest).await?;
    // The plaintext FIX passwords are now hashed into the registry; drop the
    // config (and its plaintext copy) immediately rather than hold it to `serve`.
    drop(config);
    state.begin_serving();
    tracing::info!(
        underlyings = state.underlying_count(),
        durable = state.is_persistent(),
        seed = %report.summary(),
        manifest = %manifest_summary,
        "AppState assembled and seeded; venue is serving"
    );

    // The venue clock-cadence driver (#028; self-review fix #112): spawn the owned
    // background task that advances the shared venue clock off the sequenced path,
    // so `venue_ts` progresses and the rate-limit window rolls for the whole life of
    // the process. Realtime / accelerated only — a stepped clock advances via
    // explicit `Clock` commands and spawns nothing (the driver returns `None`). The
    // `Weak`-backed task also self-terminates when the last `Arc<AppState>` drops.
    let clock_driver = fauxchange::state::spawn_clock_cadence_driver(&state);

    // The FIX 4.4 gateway (#038): spawn the acceptor ONLY when `[fix].enabled`, so
    // a released image never opens a raw-TCP port by default. The acceptor reaches
    // auth / rate-limit / the account registry / the venue clock through
    // `Arc<AppState>` (the `VenueFixSessionFactory` seam that replaced the #037
    // stub); the gateway depends on `AppState`, never the reverse. Its bounded
    // connection cap, per-session mailbox, and max-frame-length caps are the
    // validated `[fix]` DoS controls, and the durable account-keyed session store
    // resumes MsgSeqNum numbering across reconnects. A `watch` shutdown signal
    // drains the in-flight sessions when the REST server returns (process shutdown).
    let fix_shutdown = if fix_config.enabled {
        let acceptor = FixAcceptor::bind(FixAcceptorConfig::from_config(&fix_config)).await?;
        let addr = acceptor.local_addr();
        // The account-keyed session store: in-memory by default (a future PG
        // backend slots in behind the same `FixSessionStore` swap seam, exactly as
        // the durable venue journal does, when `DATABASE_URL` is set).
        let session_store: Arc<dyn FixSessionStore> = Arc::new(InMemoryFixSessionStore::new());
        let factory = Arc::new(VenueFixSessionFactory::new(
            Arc::clone(&state),
            session_store,
            SessionConfig::from_config(&fix_config),
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(acceptor.serve(factory, shutdown_rx));
        tracing::info!(%addr, "FIX 4.4 gateway enabled (session FSM + logon auth)");
        Some(shutdown_tx)
    } else {
        tracing::info!("FIX 4.4 gateway disabled ([fix] enabled = false)");
        None
    };

    let result = rest::serve(state, http_addr).await;

    // The REST server drained: stop the clock driver promptly. It also exits on its
    // dropped `Weak`, but the explicit abort gives immediate, deterministic shutdown.
    if let Some(driver) = clock_driver {
        driver.abort();
    }

    // The REST server returned (shutdown / listener error): signal the FIX acceptor
    // to stop accepting and drain its in-flight sessions.
    if let Some(shutdown_tx) = fix_shutdown {
        let _ = shutdown_tx.send(true);
    }
    result?;
    Ok(())
}

/// The `fauxchange conformance` subcommand: runs the packaged cross-surface
/// parity and conformance harness over ephemeral in-process venues, prints the
/// machine-readable JSON report to stdout (optionally also to `--out <path>`),
/// and exits **non-zero on any surface failure** so a CI can gate on the
/// process code.
///
/// Flags (`--flag value` / `--flag=value`): `--out <path>` writes the JSON report
/// to a file in addition to stdout. An unknown flag fails fast (the same
/// deny-unknown discipline the serve path applies). The report never carries a
/// secret, a JWT, or a credential — failure detail is redacted at the source.
async fn run_conformance(
    args: impl Iterator<Item = String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // A minimal, hand-rolled flag parse (no CLI crate promoted to a runtime dep),
    // matching `src/config.rs`'s `--flag value` / `--flag=value` convention.
    let mut out_path: Option<String> = None;
    let mut iter = args;
    while let Some(arg) = iter.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) => (flag.to_string(), Some(value.to_string())),
            None => (arg, None),
        };
        match flag.as_str() {
            "--out" => {
                let value = match inline {
                    Some(value) => value,
                    None => iter
                        .next()
                        .ok_or("`--out` requires a path value".to_string())?,
                };
                out_path = Some(value);
            }
            other => return Err(format!("unknown conformance flag: {other}").into()),
        }
    }

    // The subcommand installs its own tracing subscriber (the serve path's is not
    // reached here); the harness logs its per-run summary through it.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let report = fauxchange::conformance::run().await;
    let json = serde_json::to_string_pretty(&report)?;

    // The machine-readable report is the subcommand's DATA-PLANE output: an explicit
    // stdout write (not `println!`, which the "tracing only" rule forbids in `src/`),
    // so a CI can parse it. The human-facing summary + logs go to STDERR via
    // `tracing`, so the two streams never interleave.
    {
        use std::io::Write as _;
        writeln!(std::io::stdout().lock(), "{json}")
            .map_err(|e| format!("write conformance report to stdout: {e}"))?;
    }
    if let Some(path) = &out_path {
        std::fs::write(path, format!("{json}\n"))?;
        tracing::info!(path = %path, "conformance report written");
    }

    let failed = report.failed_surfaces();
    if report.passed() {
        tracing::info!(
            cases = report.totals.cases,
            "conformance green across REST, WS, and FIX"
        );
    } else {
        tracing::error!(
            cases = report.totals.cases,
            failed = report.totals.failed,
            ?failed,
            "conformance FAILED — see the report for the failing cases"
        );
    }

    std::process::exit(report.exit_code());
}
