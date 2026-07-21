//! Boot-time journal recovery integration tests (#85) — resume a durable venue on
//! restart.
//!
//! ## Gating (the main suite stays green WITHOUT Docker)
//!
//! Every real-Postgres test here is `#[ignore]`d, so the default `cargo test` never
//! starts a container — the main suite is green on a machine with no Docker. They
//! run ONLY under `cargo test --test recovery -- --ignored` (the CI `migrations`
//! job, where Docker is available), matching the EXISTING `tests/db.rs` pattern.
//! The Docker-FREE half of the guarantee — the recovery reducer's determinism
//! oracle (record → restart → continue) and its typed corruption / newer-schema
//! refusals naming `(underlying, sequence)` — lives in `tests/determinism.rs`
//! (`test_recovery_then_continue_matches_a_never_restarted_run`,
//! `test_recover_into_halts_on_corruption_naming_underlying_and_sequence`,
//! `test_recover_into_refuses_a_newer_than_binary_schema`).
//!
//! The real Postgres is an EPHEMERAL `postgres:18-alpine` container via
//! `testcontainers` — never a mocked DB (rules SQL & Persistence). A
//! `flavor = "multi_thread"` runtime is required by the durable journal's
//! sync→async bridge (`block_in_place`).
//!
//! ## What it proves
//!
//! - **Resume**: boot → trade → kill → re-boot the SAME `DATABASE_URL` resumes the
//!   venue — same rehydrated `lineage_id`, the `underlying_sequence` continues (no
//!   reset, no `Conflict` on the first post-restart command), the book is
//!   reconstructed by re-execution (a post-restart taker fills against a recovered
//!   maker), and the executions / positions folds match the pre-kill run.
//! - **Fail-stop**: a re-boot whose venue config drifts from the recorded stream
//!   (a different fee schedule ⇒ re-execution diverges from the stored event)
//!   **refuses to serve** with a typed `AppStateError::Recovery` naming the
//!   `(underlying, sequence)` — never a silent fresh start over durable history.

use std::collections::BTreeMap;
use std::sync::Arc;

use fauxchange::db::{DatabasePool, DbPoolConfig};
use fauxchange::exchange::{
    Cents, ExecutionsStore, Hash32, JournalError, LineageId, PositionsStore, STPMode, Side, Symbol,
    TimeInForce, VenueCommand,
};
use fauxchange::microstructure::{FeeConfig, FileMicrostructure, MicrostructureConfig};
use fauxchange::state::{AppState, AppStateConfig, AppStateError};
use fauxchange::{AccountId, OrderType, VenueOrderId};

const UNDERLYING: &str = "BTC";
const CALL: &str = "BTC-20240329-50000-C";

// ============================================================================
// Fixtures
// ============================================================================

fn sym(raw: &str) -> Symbol {
    match Symbol::parse(raw) {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
    }
}

/// A GTC limit `AddOrder` onto the venue's BTC book — a gateway stand-in.
#[allow(clippy::too_many_arguments)]
fn add(
    symbol: &str,
    order_id: &str,
    account: &str,
    owner: u8,
    side: Side,
    price: u64,
    quantity: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(symbol),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new(account),
        owner: Hash32([owner; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

/// A resolved microstructure carrying an explicit fee schedule — used to prove that
/// a config drift on reboot is caught by the recovery integrity oracle.
fn fee_microstructure(maker_bps: i32, taker_bps: i32) -> MicrostructureConfig {
    let file = FileMicrostructure {
        fees: Some(FeeConfig {
            maker_bps,
            taker_bps,
        }),
        ..FileMicrostructure::default()
    };
    match MicrostructureConfig::resolve(&file, &BTreeMap::new()) {
        Ok(micro) => micro,
        Err(error) => panic!("microstructure must resolve: {error}"),
    }
}

/// Boots an `AppState` on the durable pool with the given run lineage +
/// microstructure, hosting only `BTC`. Serving is on so submits flow immediately.
fn boot(pool: &DatabasePool, lineage: &str, micro: MicrostructureConfig) -> Arc<AppState> {
    let config = AppStateConfig::new([UNDERLYING])
        .with_lineage(LineageId::new(lineage))
        .with_db(Some(pool.clone()))
        .with_serving(true)
        .with_microstructure(micro);
    match AppState::new(config) {
        Ok(state) => state,
        Err(error) => panic!("durable AppState boot must succeed: {error}"),
    }
}

/// Starts an ephemeral, pinned `postgres:18-alpine` and opens + migrates the pool —
/// the SAME setup `tests/db.rs` uses. Returns the container (kept alive by the
/// caller) and the durable pool.
async fn start_pg() -> (
    testcontainers_modules::testcontainers::ContainerAsync<
        testcontainers_modules::postgres::Postgres,
    >,
    DatabasePool,
) {
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::{ImageExt, runners::AsyncRunner};

    let container = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .expect("start postgres:18-alpine container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool_config = DbPoolConfig {
        max_connections: 5,
        slow_acquire: std::time::Duration::from_millis(500),
    };
    let db = DatabasePool::connect_and_migrate(&url, pool_config)
        .await
        .expect("open pool and run migrations");
    (container, db)
}

// ============================================================================
// Resume (Docker-gated, #[ignore])
// ============================================================================

/// Boot → trade → kill → re-boot the SAME durable pool: the venue RESUMES. The
/// rehydrated `lineage_id` is the recorded one (not the differing config lineage the
/// reboot supplied), the `underlying_sequence` continues past the pre-kill tail (no
/// reset, no `Conflict`), the reconstructed book lets a post-restart taker fill
/// against a recovered maker, and the executions / positions folds match.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_boot_trade_kill_reboot_resumes_at_continued_sequence() {
    let (container, db) = start_pg().await;

    // ---- BOOT 1: trade a crossing pair (a fill), then "kill" the venue. --------
    let state1 = boot(&db, "resume-run", MicrostructureConfig::default());
    let maker = state1
        .submit(add(CALL, "mk-0", "maker", 0x11, Side::Sell, 50_000, 2))
        .await
        .expect("boot-1 maker submits");
    assert_eq!(
        maker.underlying_sequence.get(),
        0,
        "the maker sequences at 0"
    );
    let taker = state1
        .submit(add(CALL, "tk-1", "taker", 0x22, Side::Buy, 50_000, 2))
        .await
        .expect("boot-1 taker submits");
    assert_eq!(
        taker.underlying_sequence.get(),
        1,
        "the taker sequences at 1"
    );
    // The crossing pair produced a fill → two linked execution legs.
    assert_eq!(
        state1.executions().len(),
        2,
        "boot-1 recorded both fill legs"
    );
    let snap1 = state1
        .journal_snapshot(UNDERLYING)
        .await
        .expect("boot-1 journal snapshot");
    assert_eq!(snap1.last_sequence.map(|s| s.get()), Some(1));
    // Kill: dropping the only `Arc<AppState>` closes every actor mailbox. The
    // durable writes committed synchronously before each submit returned.
    drop(state1);

    // ---- RE-BOOT the SAME pool, with a DIFFERENT config lineage. ---------------
    // Recovery must rehydrate the RECORDED lineage, not adopt this one.
    let state2 = boot(
        &db,
        "different-config-lineage",
        MicrostructureConfig::default(),
    );

    // The venue resumed: BTC is recovered, its lineage is the recorded one.
    assert!(
        state2.is_recovered(UNDERLYING),
        "BTC resumed from the non-empty durable journal"
    );
    assert_eq!(state2.recovered_underlyings(), vec![UNDERLYING]);
    assert_eq!(
        state2.lineage_id().as_str(),
        "resume-run",
        "the venue rehydrated the recorded run lineage, not the reboot's config lineage"
    );

    // The sequence continues (no reset) and the executions / positions folds were
    // rebuilt from the recovered events.
    let snap2 = state2
        .journal_snapshot(UNDERLYING)
        .await
        .expect("boot-2 journal snapshot");
    assert_eq!(
        snap2.last_sequence.map(|s| s.get()),
        Some(1),
        "recovery leaves the underlying at the last journaled sequence"
    );
    assert_eq!(
        state2.executions().len(),
        2,
        "the executions fold was rebuilt from the recovered events"
    );
    let call = sym(CALL);
    let maker_pos = state2
        .positions()
        .get(&AccountId::new("maker"), &call, None)
        .expect("maker position get")
        .expect("a rebuilt maker position");
    let taker_pos = state2
        .positions()
        .get(&AccountId::new("taker"), &call, None)
        .expect("taker position get")
        .expect("a rebuilt taker position");
    assert_eq!(maker_pos.net_quantity, -2, "recovered maker sold 2");
    assert_eq!(taker_pos.net_quantity, 2, "recovered taker bought 2");

    // ---- The venue ACCEPTS the next command at the CONTINUED sequence. ---------
    // A post-restart maker rests on the reconstructed book; a taker crosses it —
    // proving the book was reconstructed AND the stream continues without a Conflict.
    let resumed_maker = state2
        .submit(add(CALL, "mk-2", "maker", 0x11, Side::Sell, 50_000, 1))
        .await
        .expect("post-restart maker submits (continues, no Conflict)");
    assert_eq!(
        resumed_maker.underlying_sequence.get(),
        2,
        "the resumed underlying continues at last + 1, never resetting to 0"
    );
    let resumed_taker = state2
        .submit(add(CALL, "tk-3", "taker", 0x22, Side::Buy, 50_000, 1))
        .await
        .expect("post-restart taker submits");
    assert_eq!(resumed_taker.underlying_sequence.get(), 3);
    assert_eq!(
        state2.executions().len(),
        4,
        "the post-restart fill appended two more legs onto the rebuilt fold"
    );

    drop(container);
}

/// A re-boot whose venue config DRIFTS from the recorded stream (a different fee
/// schedule) **refuses to serve**: re-executing the recorded crossing under the new
/// fees derives a different fill fee than the stored event, so the recovery
/// integrity oracle halts with a typed `AppStateError::Recovery` naming the
/// `(underlying, sequence)` — never a silent fresh start over durable history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_boot_recovery_refuses_to_serve_on_config_drift_naming_underlying_and_sequence() {
    let (container, db) = start_pg().await;

    // BOOT 1: record a crossing fill under a taker-fee schedule.
    let state1 = boot(&db, "drift-run", fee_microstructure(-10, 35));
    state1
        .submit(add(CALL, "mk-0", "maker", 0x11, Side::Sell, 50_000, 2))
        .await
        .expect("boot-1 maker submits");
    state1
        .submit(add(CALL, "tk-1", "taker", 0x22, Side::Buy, 50_000, 2))
        .await
        .expect("boot-1 taker submits (fills)");
    assert_eq!(state1.executions().len(), 2);
    drop(state1);

    // RE-BOOT the SAME pool with a DIFFERENT fee schedule → the re-executed fill's
    // fee diverges from the stored event at the crossing sequence (1).
    let config = AppStateConfig::new([UNDERLYING])
        .with_lineage(LineageId::new("drift-run"))
        .with_db(Some(db.clone()))
        .with_serving(true)
        .with_microstructure(fee_microstructure(-10, 0));
    match AppState::new(config) {
        Err(AppStateError::Recovery {
            underlying,
            source: JournalError::Corruption { sequence, .. },
        }) => {
            assert_eq!(
                underlying, UNDERLYING,
                "the refusal names the exact underlying"
            );
            assert_eq!(
                sequence.get(),
                1,
                "the refusal names the exact sequence where re-execution diverged"
            );
        }
        Ok(_) => panic!("a config drift over a non-empty journal must refuse to serve, not resume"),
        Err(other) => panic!("expected a fail-stop AppStateError::Recovery, got {other:?}"),
    }

    drop(container);
}
