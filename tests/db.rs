//! Integration tests for the optional durable persistence layer (#023).
//!
//! ## Gating (the main suite stays green WITHOUT Docker)
//!
//! The real-Postgres test [`test_pg_and_in_memory_executions_parity`] is
//! `#[ignore]`d, so the default `cargo test` never starts a container — the main
//! suite is green on a machine with no Docker. It runs ONLY under
//! `cargo test --test db -- --ignored` (the CI `migrations` job, where Docker is
//! available). The DB-less test [`test_dbless_path_serves_in_memory_backend`] runs
//! always (no Docker) and asserts the in-memory path serves.
//!
//! The real Postgres is an EPHEMERAL `postgres:18-alpine` container via
//! `testcontainers` — never a mocked DB (rules SQL & Persistence).
//!
//! ## What it proves
//!
//! The durable [`PgExecutionsStore`] and the [`InMemoryExecutionsStore`] serve
//! **identical** reads behind the **same** #008 `ExecutionsStore` contract:
//! records written through the trait read back equal (same records, same journal
//! order), the account-scoped `get`/`list`/underlying-filter/limit agree, and the
//! `(execution_id, liquidity)` upsert is idempotent — "backend parity behind one
//! contract, chosen at boot; gateways never know which".

use std::sync::Arc;

use fauxchange::db::{DatabasePool, DbPoolConfig, select_executions_store};
use fauxchange::exchange::{
    Cents, EventTimestamp, ExecutionFilter, ExecutionsStore, SequenceNumber, SignedCents, Symbol,
};
use fauxchange::models::{
    AccountId, ExecutionId, ExecutionRecord, LiquidityFlag, Side, VenueOrderId,
};

// ============================================================================
// Fixtures
// ============================================================================

/// Builds the two linked legs (maker + taker) of one match at `sequence`, sharing
/// one execution id, each with its own account / side / fee.
fn match_legs(
    sequence: u64,
    underlying: &str,
    instrument: &str,
    price: u64,
    quantity: u64,
    maker_account: &str,
    taker_account: &str,
) -> [ExecutionRecord; 2] {
    let execution_id = ExecutionId::new(format!("exec-{underlying}-{sequence}"));
    let instrument_sym = match Symbol::parse(instrument) {
        Ok(symbol) => symbol,
        Err(error) => panic!("fixture instrument {instrument} failed to parse: {error:?}"),
    };
    let base = ExecutionRecord {
        execution_id: execution_id.clone(),
        order_id: VenueOrderId::new(format!("ord-{sequence}")),
        account: AccountId::new(maker_account),
        symbol: underlying.to_string(),
        instrument: instrument_sym.clone(),
        side: Side::Sell,
        liquidity: LiquidityFlag::Maker,
        quantity,
        price_cents: Cents::new(price),
        fee_cents: SignedCents::new(-10),
        theo_value_cents: Cents::new(price),
        edge_cents: SignedCents::new(0),
        underlying_sequence: SequenceNumber::new(sequence),
        latency_us: 0,
        executed_at: EventTimestamp::new(1_700_000_000_000 + sequence),
    };
    let maker = base.clone();
    let taker = ExecutionRecord {
        order_id: VenueOrderId::new(format!("ord-{sequence}-t")),
        account: AccountId::new(taker_account),
        side: Side::Buy,
        liquidity: LiquidityFlag::Taker,
        fee_cents: SignedCents::new(15),
        ..base
    };
    [maker, taker]
}

/// The deterministic fixture flow: three matches across two accounts and two
/// underlyings — six legs, in journal order.
fn fixture_legs() -> Vec<ExecutionRecord> {
    let mut legs = Vec::new();
    // Match 1 (seq 1, BTC call): maker alice (sell), taker bob (buy).
    legs.extend(match_legs(
        1,
        "BTC",
        "BTC-20240329-50000-C",
        50_000,
        2,
        "alice",
        "bob",
    ));
    // Match 2 (seq 2, BTC call): maker bob (sell), taker alice (buy).
    legs.extend(match_legs(
        2,
        "BTC",
        "BTC-20240329-50000-C",
        50_100,
        1,
        "bob",
        "alice",
    ));
    // Match 3 (seq 3, ETH call): maker alice (sell), taker bob (buy).
    legs.extend(match_legs(
        3,
        "ETH",
        "ETH-20240329-3000-C",
        3_000,
        5,
        "alice",
        "bob",
    ));
    legs
}

/// Records every leg into `store` through the #008 trait, in fixture order.
fn record_all(store: &dyn ExecutionsStore, legs: &[ExecutionRecord]) {
    for leg in legs {
        store
            .record(leg.clone())
            .unwrap_or_else(|error| panic!("record failed: {error}"));
    }
}

/// Asserts the two backends serve identical reads through the trait.
fn assert_backends_agree(a: &dyn ExecutionsStore, b: &dyn ExecutionsStore) {
    assert_eq!(a.len(), b.len(), "leg counts must match");

    for account in ["alice", "bob"] {
        let account = AccountId::new(account);

        // Unfiltered account list (journal order).
        let la = a
            .list(&account, &ExecutionFilter::default())
            .unwrap_or_else(|e| panic!("list failed: {e}"));
        let lb = b
            .list(&account, &ExecutionFilter::default())
            .unwrap_or_else(|e| panic!("list failed: {e}"));
        assert_eq!(la, lb, "unfiltered list must match for {account:?}");

        // Underlying filter.
        let filter = ExecutionFilter {
            underlying: Some("BTC".to_string()),
            limit: None,
        };
        let fa = a
            .list(&account, &filter)
            .unwrap_or_else(|e| panic!("list failed: {e}"));
        let fb = b
            .list(&account, &filter)
            .unwrap_or_else(|e| panic!("list failed: {e}"));
        assert_eq!(fa, fb, "BTC-filtered list must match for {account:?}");
        assert!(
            fa.iter().all(|record| record.symbol == "BTC"),
            "the underlying filter must only keep BTC legs"
        );

        // Limit.
        let limited = ExecutionFilter {
            underlying: None,
            limit: Some(1),
        };
        let lima = a
            .list(&account, &limited)
            .unwrap_or_else(|e| panic!("list failed: {e}"));
        let limb = b
            .list(&account, &limited)
            .unwrap_or_else(|e| panic!("list failed: {e}"));
        assert_eq!(lima, limb, "limited list must match for {account:?}");
        assert!(lima.len() <= 1, "the limit must be applied");

        // get() for every fixture execution id (taker-first for self-trades).
        for sequence in [1_u64, 2, 3] {
            for underlying in ["BTC", "ETH"] {
                let execution_id = ExecutionId::new(format!("exec-{underlying}-{sequence}"));
                let ga = a
                    .get(&execution_id, &account)
                    .unwrap_or_else(|e| panic!("get failed: {e}"));
                let gb = b
                    .get(&execution_id, &account)
                    .unwrap_or_else(|e| panic!("get failed: {e}"));
                assert_eq!(ga, gb, "get must match for {account:?} / {execution_id:?}");
            }
        }
    }
}

// ============================================================================
// DB-less path (always runs, NO Docker)
// ============================================================================

/// The DB-less path serves: `select_executions_store(None)` yields a working
/// in-memory backend behind the #008 contract. Runs in the default `cargo test`
/// suite without Docker.
#[test]
fn test_dbless_path_serves_in_memory_backend() {
    let store = select_executions_store(None).expect("in-memory selection is infallible");
    let legs = fixture_legs();
    record_all(store.as_ref(), &legs);

    assert_eq!(store.len(), 6, "six legs recorded");
    let alice = AccountId::new("alice");
    let alice_legs = store
        .list(&alice, &ExecutionFilter::default())
        .expect("list");
    // alice: maker of match 1, taker of match 2, maker of match 3 — journal order.
    assert_eq!(alice_legs.len(), 3);
    assert_eq!(alice_legs[0].underlying_sequence, SequenceNumber::new(1));
    assert_eq!(alice_legs[1].underlying_sequence, SequenceNumber::new(2));
    assert_eq!(alice_legs[2].underlying_sequence, SequenceNumber::new(3));
}

// ============================================================================
// Durable Postgres parity (Docker-gated, #[ignore])
// ============================================================================

/// Backend parity behind one contract, against a REAL ephemeral `postgres:18-alpine`.
///
/// `#[ignore]` + `flavor = "multi_thread"`: the default suite skips it (no
/// Docker); the CI `migrations` job runs it with `-- --ignored`. The multi-thread
/// runtime is required by the durable store's sync→async bridge (`block_in_place`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_pg_and_in_memory_executions_parity() {
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::{ImageExt, runners::AsyncRunner};

    // Ephemeral, PINNED postgres:18-alpine (never a mocked DB).
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

    // Open the pool + run migrations at boot (pool knobs are config, not hard-coded).
    let pool_config = DbPoolConfig {
        max_connections: 5,
        slow_acquire: std::time::Duration::from_millis(500),
    };
    let db = DatabasePool::connect_and_migrate(&url, pool_config)
        .await
        .expect("open pool and run migrations");

    // Both backends behind the SAME #008 contract, chosen at boot.
    let pg: Arc<dyn ExecutionsStore> =
        select_executions_store(Some(&db)).expect("select durable backend");
    let memory: Arc<dyn ExecutionsStore> =
        select_executions_store(None).expect("select in-memory backend");

    let legs = fixture_legs();
    record_all(pg.as_ref(), &legs);
    record_all(memory.as_ref(), &legs);

    // The persisted records match the in-memory backend's, read-for-read.
    assert_backends_agree(pg.as_ref(), memory.as_ref());
    assert_eq!(pg.len(), 6, "the durable store recorded six legs");

    // The (execution_id, liquidity) upsert is idempotent: a re-record of the same
    // legs leaves the count and the reads unchanged (matching the in-memory store).
    record_all(pg.as_ref(), &legs);
    assert_eq!(pg.len(), 6, "a re-record must not duplicate legs");
    assert_backends_agree(pg.as_ref(), memory.as_ref());

    // Container is dropped here, stopping and removing it.
    drop(container);
}
