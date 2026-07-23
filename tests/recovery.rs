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

use fauxchange::config::SeedManifest;
use fauxchange::db::{DatabasePool, DbPoolConfig};
use fauxchange::exchange::{
    Cents, EventTimestamp, ExecutionsStore, Hash32, JournalError, LineageId, PositionsStore,
    STPMode, Side, Symbol, TimeInForce, VenueCommand,
};
use fauxchange::market_maker::MarketMakerEvent;
use fauxchange::microstructure::{FeeConfig, FileMicrostructure, MicrostructureConfig};
use fauxchange::seed;
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

// ============================================================================
// Market-maker quoting reconciliation after boot-time recovery (#148)
// ============================================================================

/// The market-maker opening reference price the seed manifest sets, in cents.
const MM_OPENING: u64 = 5_000_000;
/// A later reference price journaled as a `SimStep` in boot 1, in cents — distinct
/// from `MM_OPENING`, so the recovered reference proves it is read from the durable
/// journal (the last `SimStep`), not re-derived from the manifest opening price.
const MM_MOVED: u64 = 5_100_000;
/// A generous taker limit (well above the maker's ATM call premium, inside the
/// `[1, 100_000_000]`-cent default price band) that lifts the maker's resting ask.
const TAKER_LIFT: u64 = 10_000_000;
/// The manifest's 50000-strike call on the seeded `20261231` expiry — the contract
/// the maker actually quotes (distinct from the file-scope `CALL`, which sits on a
/// different expiry the market-maker manifest never seeds).
const MM_CALL: &str = "BTC-20261231-50000-C";

/// A seed manifest with a `balanced` persona and one BTC chain on an absolute
/// `DateTime` expiry after the venue clock (so the maker quotes and vivifies it). No
/// accounts, so `apply_seed_phase` provisions none.
const MM_SEED: &str = r#"
[market_maker]
default_persona = "balanced"

[market_maker.personas.balanced]
spread_multiplier = 1.0
size_scalar = 1.0
directional_skew = 0.0

[instruments.BTC]
opening_price_cents = 5000000
expirations = ["20261231"]
strikes = [50000, 55000]
"#;

fn mm_manifest() -> SeedManifest {
    SeedManifest::from_toml_str(MM_SEED).expect("the market-maker seed manifest parses")
}

/// Boots a durable `AppState` in the bounded **seeding** phase (not yet serving),
/// hosting the manifest's underlyings with its price-seam assets wired — the shape
/// `main.rs` uses before `apply_seed_phase`.
fn seeding_boot(pool: &DatabasePool, lineage: &str, manifest: &SeedManifest) -> Arc<AppState> {
    let config = AppStateConfig::new(manifest.underlyings())
        .with_lineage(LineageId::new(lineage))
        .with_db(Some(pool.clone()))
        .with_assets(seed::asset_configs(manifest))
        .with_serving(false);
    match AppState::new(config) {
        Ok(state) => state,
        Err(error) => panic!("durable seeding AppState boot must succeed: {error}"),
    }
}

/// A journaled `SimStep` reference-price override on `underlying` — the sequenced
/// producer `POST /api/v1/prices` wraps, awaited so it is durable before the kill.
fn sim_step(underlying: &str, price: u64, now_ms: u64) -> VenueCommand {
    VenueCommand::SimStep {
        now_ms: EventTimestamp::new(now_ms),
        underlying: underlying.to_string(),
        price: Cents::new(price),
        bid: None,
        ask: None,
    }
}

/// Boot → seed → trade → kill → re-boot the SAME durable pool: the recovered
/// underlying's market maker is reconciled (#148) so it is **not** quote-silent.
///
/// It proves the three halves of the fix, without journaling any duplicate record on
/// the resumed stream:
///
/// 1. **Non-journaling reference restore.** After `AppState::new` recovers BTC — but
///    *before* the seed phase — the maker's reference price is the last journaled
///    `SimStep` price (`MM_MOVED`), restored in-memory from the recovered stream, not
///    a default/zero and not the manifest opening.
/// 2. **Seed step 3 kept, step 4 skipped.** Re-running `apply_seed_phase` re-runs the
///    in-memory persona/contract registration (`registered_count > 0`) for the
///    recovered underlying but journals **nothing** (the `underlying_sequence` is
///    unchanged across the phase — no duplicate `SimStep` onto the resumed stream),
///    and does not trip an `InstrumentPriceConflict` on the moved reference.
/// 3. **The maker quotes.** A live requote at the resumed reference produces two-sided
///    `QuoteUpdated` events on the recovered chain — the maker is not silent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_boot_seed_trade_kill_reboot_recovered_market_maker_quotes() {
    let (container, db) = start_pg().await;
    let manifest = mm_manifest();

    // ---- BOOT 1: seed → the maker quotes → move the reference → a trade. --------
    let state1 = seeding_boot(&db, "mm-resume-run", &manifest);
    seed::apply_seed_phase(&state1, &manifest)
        .await
        .expect("boot-1 seed manifest applies");
    state1.begin_serving();

    // The seed registered + priced + quoted the full chain (BTC: 2 strikes × 2 styles).
    assert_eq!(state1.market_maker().registered_count(UNDERLYING), 4);
    assert_eq!(
        state1.market_maker().get_price(UNDERLYING),
        Some(MM_OPENING),
        "the fresh underlying seeded its opening reference price"
    );

    // Move the reference with an awaited, journaled `SimStep`, so the RESUMED reference
    // (the last `SimStep`) is `MM_MOVED`, not the manifest opening — this both proves
    // the restore reads the journal and exercises the recovered conflict-skip.
    let now_ms = state1.clock().now_ms().get();
    state1
        .submit(sim_step(UNDERLYING, MM_MOVED, now_ms))
        .await
        .expect("boot-1 reference-move SimStep journals");

    // A trade: a taker lifts the maker's resting ask on the call → a fill (two legs).
    state1
        .submit(add(
            MM_CALL,
            "tk-1",
            "taker",
            0x22,
            Side::Buy,
            TAKER_LIFT,
            1,
        ))
        .await
        .expect("boot-1 taker submits");
    assert!(
        state1.executions().len() >= 2,
        "boot-1 recorded a trade (a fill against the maker)"
    );

    // Kill: dropping the only `Arc<AppState>` closes the actor mailbox; the durable
    // writes committed synchronously before each awaited submit returned.
    drop(state1);

    // ---- RE-BOOT the SAME pool (seeding phase again, a DIFFERENT config lineage). -
    let state2 = seeding_boot(&db, "mm-reboot-lineage", &manifest);
    assert!(
        state2.is_recovered(UNDERLYING),
        "BTC resumed from the non-empty durable journal"
    );

    // (1) The maker reference was restored — NON-journaled — from the last recovered
    // `SimStep` at boot, BEFORE the seed phase runs.
    assert_eq!(
        state2.market_maker().get_price(UNDERLYING),
        Some(MM_MOVED),
        "the recovered maker reference is the resumed price, not a default/zero or the manifest opening"
    );
    // The recovered underlying carries NO in-memory contract registration yet (recovery
    // rebuilds the book, not the live-only maker state) — the seed phase supplies it.
    assert_eq!(
        state2.market_maker().registered_count(UNDERLYING),
        0,
        "recovery does not rebuild the live-only maker contract registration"
    );

    // The durable sequence the recovered stream sits at, BEFORE the re-seed. Boot
    // recovery reconciliation (the reference restore) is in-memory and journals nothing.
    let seq_before_seed = state2
        .journal_snapshot(UNDERLYING)
        .await
        .expect("boot-2 pre-seed snapshot")
        .last_sequence;

    // (2) Re-running the seed phase re-runs step 3 (in-memory registration) for the
    // recovered underlying and SKIPS step 4 (the journaled opening `SimStep`) — and
    // does NOT trip an `InstrumentPriceConflict` on the moved reference.
    seed::apply_seed_phase(&state2, &manifest)
        .await
        .expect("boot-2 re-seed applies (recover wins; no conflict on the moved reference)");
    state2.begin_serving();
    assert_eq!(
        state2.market_maker().registered_count(UNDERLYING),
        4,
        "the recovered underlying got its in-memory persona/contract registration (step 3)"
    );

    // The recovered re-seed journaled NOTHING — no duplicate `SimStep` (or any record)
    // onto the resumed stream: the sequence is unchanged across the whole seed phase.
    let seq_after_seed = state2
        .journal_snapshot(UNDERLYING)
        .await
        .expect("boot-2 post-seed snapshot")
        .last_sequence;
    assert_eq!(
        seq_after_seed, seq_before_seed,
        "boot-recovery reconciliation + the recovered re-seed appended no journal record"
    );

    // (3) The maker is NOT quote-silent: a live requote at the resumed reference
    // produces two-sided quotes on every recovered contract.
    let mut events = state2.market_maker().subscribe();
    state2
        .market_maker()
        .set_venue_now_ms(state2.clock().now_ms().get());
    state2.market_maker().update_price(UNDERLYING, MM_MOVED);
    let mut quotes = 0usize;
    while let Ok(event) = events.try_recv() {
        if let MarketMakerEvent::QuoteUpdated {
            bid_price,
            ask_price,
            ..
        } = event
        {
            assert!(
                ask_price.get() > bid_price.get() && bid_price.get() >= 1,
                "a recovered quote is a well-formed two-sided quote"
            );
            quotes += 1;
        }
    }
    assert_eq!(
        quotes, 4,
        "the recovered underlying's maker quotes every contract around the resumed reference price"
    );

    drop(container);
}
