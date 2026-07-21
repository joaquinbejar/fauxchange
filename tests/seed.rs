//! Integration tests for the #024 scenario seed + bounded seeding phase.
//!
//! Bring up an ephemeral venue with a seed manifest, apply the bounded seeding
//! phase, and assert: the chain/prices/personas/accounts are present; a re-seed is
//! idempotent (and a conflicting re-seed is a typed error); a post-serving
//! hierarchy create is refused (manifest input); and a seeded account's bootstrap
//! token mints for the NAMED account and authenticates over JWT. Driven through the
//! **public** surface — `fauxchange::seed`, `AppState`, and the REST router.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::Value;
use tower::ServiceExt;

use fauxchange::auth::{AccountStore, AuthError};
use fauxchange::config::{ConfigError, SeedManifest};
use fauxchange::gateway::rest::create_router;
use fauxchange::models::{AccountId, Permission};
use fauxchange::seed::{self, SeedError};
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};

const SECRET: &str = "op-secret";

/// A `MakeWriter` that appends every formatted `tracing` event into a shared
/// buffer, so a test can scan everything the seeding phase logged (mirrors the
/// `tests/security.rs` capture harness).
#[derive(Clone)]
struct CaptureBuffer(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CaptureBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(mut guard) = self.0.lock() {
            guard.extend_from_slice(buf);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureBuffer {
    type Writer = CaptureBuffer;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// A representative scenario: two underlyings on absolute `DateTime` expiries with
/// a strike ladder, opening prices, a default persona, and Read + Trade + Admin
/// accounts (the Admin lets the post-serving create test reach the manifest
/// refusal past the `Admin` permission gate). The expiries sit after the seeded
/// virtual clock so the market maker quotes (and vivifies) the chain.
const SEED: &str = r#"
[market_maker]
default_persona = "balanced"

[market_maker.personas.balanced]
spread_multiplier = 1.0
size_scalar = 1.0
directional_skew = 0.0

[instruments.BTC]
opening_price_cents = 5000000
expirations = ["20261231"]
strikes = [45000, 50000, 55000]

[instruments.ETH]
opening_price_cents = 300000
expirations = ["20261231"]
strikes = [2500, 3000]
styles = ["call"]

[accounts.market-admin]
permissions = ["admin"]

[accounts.market-reader]
permissions = ["read"]

[accounts.market-taker]
permissions = ["read", "trade"]
fix_username = "TAKER1"
fix_password = "dev-taker-secret"
"#;

fn manifest() -> SeedManifest {
    SeedManifest::from_toml_str(SEED).expect("the seed manifest must parse and validate")
}

/// Builds an ephemeral venue in the bounded **seeding** phase (not yet serving),
/// hosting the manifest's underlyings with the bootstrap secret set.
fn seeding_venue() -> Arc<AppState> {
    let manifest = manifest();
    let auth = AuthConfig::dev()
        .expect("dev auth must build")
        .with_bootstrap_secret(SECRET);
    let config = AppStateConfig::new(manifest.underlyings())
        .with_auth(auth)
        .with_assets(seed::asset_configs(&manifest))
        .with_serving(false);
    AppState::new(config).expect("AppState must build")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs()
}

fn build_request(method: &str, uri: &str, bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    builder.body(Body::empty()).expect("request must build")
}

async fn send(state: &Arc<AppState>, request: Request<Body>) -> (StatusCode, Value) {
    let router: Router = create_router(Arc::clone(state));
    let response = router.oneshot(request).await.expect("router is infallible");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

// ============================================================================
// The default scenario populates the chain, prices, personas, and accounts
// ============================================================================

#[tokio::test]
async fn test_seed_default_scenario_populates_chain_prices_personas_accounts() {
    let manifest = manifest();
    let state = seeding_venue();
    assert!(
        !state.is_serving(),
        "the venue starts in the bounded seeding phase"
    );

    let report = seed::apply_seed_phase(&state, &manifest)
        .await
        .expect("the seed manifest applies");
    assert_eq!(report.accounts_provisioned, 3);
    assert_eq!(report.underlyings_seeded, 2);
    // BTC: 3 strikes × 1 expiry × 2 styles = 6; ETH: 2 × 1 × 1 (call) = 2.
    assert_eq!(report.contracts_registered, 8);

    // Opening prices are present (in cents).
    assert_eq!(state.market_maker().get_price("BTC"), Some(5_000_000));
    assert_eq!(state.market_maker().get_price("ETH"), Some(300_000));

    // Personas registered the full chain per underlying.
    assert_eq!(state.market_maker().registered_count("BTC"), 6);
    assert_eq!(state.market_maker().registered_count("ETH"), 2);

    // Accounts are present with their seeded permissions and FIX credential.
    let reader = state
        .accounts()
        .account(&AccountId::new("market-reader"))
        .expect("the reader account is provisioned");
    assert_eq!(reader.permissions, vec![Permission::Read]);
    let taker = state
        .accounts()
        .account(&AccountId::new("market-taker"))
        .expect("the taker account is provisioned");
    assert_eq!(taker.credentials.fix_username.as_deref(), Some("TAKER1"));

    // The chain vivified into the shared symbol index (the hierarchy is present).
    // Every seeded contract is present; the index count is >= the seeded contracts
    // because upstream a strike node carries both its call and put book.
    let symbols: std::collections::HashSet<String> =
        state.symbol_index().symbols().into_iter().collect();
    for set in manifest.instruments() {
        for contract in &set.contracts {
            assert!(
                symbols.contains(contract.as_str()),
                "seeded contract {} is missing from the symbol index",
                contract.as_str()
            );
        }
    }
    assert!(symbols.contains("BTC-20261231-50000-C"));
    assert!(
        symbols.len() >= report.contracts_registered,
        "the full seeded chain vivified (got {} symbols)",
        symbols.len()
    );

    // The venue is still seeding until the explicit flip.
    assert!(!state.is_serving());
    state.begin_serving();
    assert!(state.is_serving());
}

// ============================================================================
// Idempotent re-seed
// ============================================================================

#[tokio::test]
async fn test_seed_reapply_is_idempotent() {
    let manifest = manifest();
    let state = seeding_venue();
    seed::apply_seed_phase(&state, &manifest)
        .await
        .expect("first seed");
    let accounts_before = state.accounts().account_count();
    let symbols_before = state.symbol_index().symbols().len();

    // Re-applying the SAME manifest is a no-op — no duplicate accounts or leaves.
    let report = seed::apply_seed_phase(&state, &manifest)
        .await
        .expect("re-seed is idempotent");
    assert_eq!(report.accounts_provisioned, 0, "no new accounts on re-seed");
    assert_eq!(report.accounts_unchanged, 3, "existing accounts are no-ops");
    assert_eq!(state.accounts().account_count(), accounts_before);
    assert_eq!(
        state.symbol_index().symbols().len(),
        symbols_before,
        "no duplicate vivification"
    );
    assert_eq!(
        state.market_maker().registered_count("BTC"),
        6,
        "register_instrument is idempotent"
    );
}

// ============================================================================
// A conflicting re-seed is a typed error
// ============================================================================

#[tokio::test]
async fn test_seed_conflicting_reapply_is_typed_error() {
    let state = seeding_venue();
    seed::apply_seed_phase(&state, &manifest())
        .await
        .expect("first seed");

    // The same underlying at a DIFFERENT opening price is a conflicting spec.
    let conflicting = SeedManifest::from_toml_str(
        "[instruments.BTC]\n\
         opening_price_cents = 9999999\n\
         expirations = [\"20261231\"]\n\
         strikes = [50000]\n",
    )
    .expect("the conflicting manifest still parses");
    match seed::apply_seed_phase(&state, &conflicting).await {
        Err(SeedError::InstrumentPriceConflict { underlying, .. }) => {
            assert_eq!(underlying, "BTC")
        }
        other => panic!("expected InstrumentPriceConflict, got {other:?}"),
    }
}

// ============================================================================
// After the flip to serving, a runtime hierarchy create is refused
// ============================================================================

#[tokio::test]
async fn test_post_serving_hierarchy_create_is_refused() {
    let state = seeding_venue();
    seed::apply_seed_phase(&state, &manifest())
        .await
        .expect("seed");
    state.begin_serving();

    let bearer = state
        .mint_token(&AccountId::new("market-admin"), SECRET, now_secs(), 3_600)
        .expect("admin token mints");
    let (status, body) = send(
        &state,
        build_request("POST", "/api/v1/underlyings/SOL", Some(&bearer)),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["message"].as_str().unwrap_or("").contains("manifest"),
        "the refusal must name the manifest-input reason: {body}"
    );
}

// ============================================================================
// A seeded account's bootstrap token mints for the NAMED account + authenticates
// ============================================================================

#[tokio::test]
async fn test_seeded_account_bootstrap_token_mints_and_authenticates() {
    let state = seeding_venue();
    seed::apply_seed_phase(&state, &manifest())
        .await
        .expect("seed");
    state.begin_serving();

    // Mint for the NAMED, seeded account (not a fabricated fresh subject).
    let token = state
        .mint_token(&AccountId::new("market-reader"), SECRET, now_secs(), 3_600)
        .expect("minting for a seeded account succeeds");
    let claims = state
        .auth()
        .jwt()
        .verify_token(&token)
        .expect("the minted token verifies");
    assert_eq!(claims.sub, AccountId::new("market-reader"));
    assert_eq!(claims.permissions, vec![Permission::Read]);

    // An unseeded account cannot mint — no fabricated subject or permissions.
    assert!(
        state
            .mint_token(&AccountId::new("ghost"), SECRET, now_secs(), 3_600)
            .is_err(),
        "minting for an unseeded account must fail"
    );
}

// ============================================================================
// A Days-relative expiry in a seed is rejected at load; DateTime is accepted
// ============================================================================

#[test]
fn test_seed_days_expiry_is_rejected_at_load() {
    let document = "[instruments.BTC]\n\
         opening_price_cents = 5000000\n\
         expirations = [\"30\"]\n\
         strikes = [50000]\n";
    match SeedManifest::from_toml_str(document) {
        Err(ConfigError::SeedDaysExpiry { underlying, value }) => {
            assert_eq!(underlying, "BTC");
            assert_eq!(value, "30");
        }
        other => panic!("expected SeedDaysExpiry, got {other:?}"),
    }
}

// ============================================================================
// SECURITY: the seeding phase never logs a seeded FIX password (P1 regression)
// ============================================================================

#[tokio::test]
async fn test_seed_phase_never_logs_a_credential() {
    const MARKER: &str = "FIX-PASSWORD-MARKER-DoNotLog-024";

    // A thread-local capture subscriber over the current-thread test runtime, so
    // every event — including from the seeding phase's spawned forwarder tasks
    // (polled on this thread) — is captured for the flow.
    let buffer = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CaptureBuffer(Arc::clone(&buffer)))
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let document = format!(
        "[instruments.BTC]\n\
         opening_price_cents = 5000000\n\
         expirations = [\"20261231\"]\n\
         strikes = [50000]\n\
         \n\
         [accounts.taker]\n\
         permissions = [\"trade\"]\n\
         fix_username = \"T1\"\n\
         fix_password = \"{MARKER}\"\n"
    );
    let manifest = SeedManifest::from_toml_str(&document).expect("the seed manifest parses");
    let auth = AuthConfig::dev()
        .expect("dev auth")
        .with_bootstrap_secret(SECRET);
    let state = AppState::new(
        AppStateConfig::new(manifest.underlyings())
            .with_auth(auth)
            .with_assets(seed::asset_configs(&manifest))
            .with_serving(false),
    )
    .expect("AppState builds");

    seed::apply_seed_phase(&state, &manifest)
        .await
        .expect("the seed applies");
    state.begin_serving();
    // Also emit the summary the boot path logs, to exercise that log line.
    tracing::info!(manifest = %manifest.summary(), "seeded (test)");

    let captured = {
        let guard = buffer.lock().expect("capture buffer lock");
        String::from_utf8_lossy(&guard).into_owned()
    };
    // POSITIVE proof the harness captured something (else the assertion is vacuous).
    assert!(
        !captured.is_empty(),
        "the capture harness must have captured a seeding log event"
    );
    assert!(
        !captured.contains(MARKER),
        "a seeded FIX password leaked into the captured log:\n{captured}"
    );
}

// ============================================================================
// SECURITY: the reserved market-maker identity cannot be seeded (#12/#21 guard)
// ============================================================================

#[tokio::test]
async fn test_seeding_reserved_market_maker_id_is_rejected() {
    // Seeding the reserved market-maker account id must be refused on the seed
    // provisioning path with the typed sentinel error.
    let document = "[accounts.\"@market-maker\"]\npermissions = [\"trade\"]\n";
    let manifest = SeedManifest::from_toml_str(document).expect("parses");
    let auth = AuthConfig::dev()
        .expect("dev auth")
        .with_bootstrap_secret(SECRET);
    let state = AppState::new(
        AppStateConfig::new(["BTC"])
            .with_auth(auth)
            .with_serving(false),
    )
    .expect("state");
    match seed::apply_seed_phase(&state, &manifest).await {
        Err(SeedError::Account(AuthError::Provisioning(label))) => {
            assert!(label.contains("market-maker"), "label: {label}")
        }
        other => panic!("expected the reserved MM id to be rejected, got {other:?}"),
    }
}

#[tokio::test]
async fn test_seeding_reserved_market_maker_owner_is_rejected() {
    // owner = 0xEE * 32 == MARKET_MAKER_OWNER — the STP sentinel — is refused too.
    let owner = "ee".repeat(32);
    let document = format!("[accounts.impostor]\npermissions = [\"trade\"]\nowner = \"{owner}\"\n");
    let manifest = SeedManifest::from_toml_str(&document).expect("parses");
    let auth = AuthConfig::dev()
        .expect("dev auth")
        .with_bootstrap_secret(SECRET);
    let state = AppState::new(
        AppStateConfig::new(["BTC"])
            .with_auth(auth)
            .with_serving(false),
    )
    .expect("state");
    match seed::apply_seed_phase(&state, &manifest).await {
        Err(SeedError::Account(AuthError::Provisioning(label))) => {
            assert!(label.contains("market-maker"), "label: {label}")
        }
        other => panic!("expected the reserved MM owner to be rejected, got {other:?}"),
    }
}
