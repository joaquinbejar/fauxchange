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
use fauxchange::gateway::fix::{
    FixSessionStore, ResetTrigger, SequenceResetEvent, SessionCounters, SessionKey, StoredOutbound,
    select_fix_session_store,
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

// ============================================================================
// Durable FIX session store (#095): restart-durability + in-memory parity
// ============================================================================

/// The FIX session key the durable-store tests exercise — one authenticated
/// account bound to a `(SenderCompID, TargetCompID)` tuple (ADR-0010).
fn fix_key() -> SessionKey {
    SessionKey::new(AccountId::new("acct-fix-1"), "CLIENT", "FAUXCHANGE")
}

/// The read-only observations a scenario yields — the trait's entire observable
/// surface (counters, resend range reads, reset audit). Both backends must agree.
#[derive(Debug, PartialEq, Eq)]
struct SessionObservations {
    counters_after_save: SessionCounters,
    range_all: Vec<StoredOutbound>,
    range_2_3: Vec<StoredOutbound>,
    range_from_5: Vec<StoredOutbound>,
    counters_after_reset: SessionCounters,
    resets: Vec<SequenceResetEvent>,
    range_1_1_after_reuse: Vec<StoredOutbound>,
    counters_after_atomic: SessionCounters,
    range_after_atomic: Vec<StoredOutbound>,
}

/// Drives one fixed sequence of `FixSessionStore` trait calls against `store` and
/// captures every observable read. The sequence covers: counter save/load, an
/// ascending-seq resend log, bounded range reads (closed, open-ended `0`, and a
/// past-the-end empty read), a `LogonReset` audit + counter reset, and a
/// **re-used seq after the reset** (the append-not-dedup edge — both backends
/// append and order by `(seq, id)`, so a re-sent seq 1 co-exists with the pre-reset
/// seq 1, oldest first).
fn run_fix_session_scenario(store: &dyn FixSessionStore, key: &SessionKey) -> SessionObservations {
    let unwrap = |label: &str, r: Result<(), fauxchange::gateway::fix::SessionStoreError>| {
        r.unwrap_or_else(|e| panic!("{label} failed: {e}"));
    };

    // A fresh key reads as a default (1/1) session.
    let fresh = store.load_counters(key).expect("load fresh");
    assert_eq!(fresh, SessionCounters::default(), "a fresh key is 1/1");

    unwrap(
        "save_counters",
        store.save_counters(
            key,
            SessionCounters {
                next_sender_seq: 10,
                next_target_seq: 20,
            },
        ),
    );
    let counters_after_save = store.load_counters(key).expect("load after save");

    unwrap("store_outbound 1", store.store_outbound(key, 1, b"aaa"));
    unwrap("store_outbound 2", store.store_outbound(key, 2, b"bbbb"));
    unwrap("store_outbound 3", store.store_outbound(key, 3, b"cc"));

    let range_all = store.outbound_range(key, 0, 0).expect("range all");
    let range_2_3 = store.outbound_range(key, 2, 3).expect("range 2..3");
    let range_from_5 = store.outbound_range(key, 5, 0).expect("range from 5");

    // A `LogonReset` (ResetSeqNumFlag=Y): both counters back to 1, audited within
    // this key only.
    unwrap(
        "record_reset",
        store.record_reset(
            key,
            SequenceResetEvent {
                at_ms: 1_700_000_000_123,
                trigger: ResetTrigger::LogonReset,
                old_next_sender_seq: 10,
                old_next_target_seq: 20,
                new_next_sender_seq: 1,
                new_next_target_seq: 1,
            },
            SessionCounters::default(),
        ),
    );
    let counters_after_reset = store.load_counters(key).expect("load after reset");
    let resets = store.reset_events(key).expect("reset events");

    // A re-used seq 1 after the reset: append (no dedup), oldest-first ordering.
    unwrap(
        "store_outbound reuse",
        store.store_outbound(key, 1, b"post-reset"),
    );
    let range_1_1_after_reuse = store.outbound_range(key, 1, 1).expect("range 1..1");

    // The atomic emit primitive (#149 finding 1B): store a frame at seq 2 AND advance
    // the OUTBOUND counter to 3 in one op — the frame is present and next_sender is 3,
    // while next_target is left UNTOUCHED (still 1 from the reset; the inbound advance
    // is deferred to the post-effect persist). Both backends must agree bit-for-bit.
    unwrap(
        "store_outbound_and_advance",
        store.store_outbound_and_advance(key, 2, b"atomic", 3),
    );
    let counters_after_atomic = store.load_counters(key).expect("load after atomic");
    let range_after_atomic = store.outbound_range(key, 2, 2).expect("range 2..2 atomic");

    SessionObservations {
        counters_after_save,
        range_all,
        range_2_3,
        range_from_5,
        counters_after_reset,
        resets,
        range_1_1_after_reuse,
        counters_after_atomic,
        range_after_atomic,
    }
}

/// The durable FIX session state (sequence counters + resend log + reset audit)
/// survives a **simulated process restart**: written through the durable store on
/// one `PgPool`, the state is read back — identical — from a *fresh* pool opened
/// against the same database, and numbering resumes from the persisted counters.
///
/// `#[ignore]` + `flavor = "multi_thread"`: the default suite skips it (no Docker);
/// the CI `migrations` job runs it with `-- --ignored`. The multi-thread runtime is
/// required by the durable store's sync→async bridge (`block_in_place`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_pg_fix_session_survives_process_restart() {
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
    let key = fix_key();

    // ---- "First process": open + migrate, write the session state, then drop the
    // store AND the pool (simulating the process exiting). ----
    {
        let db = DatabasePool::connect_and_migrate(&url, pool_config)
            .await
            .expect("open pool and run migrations");
        let store = select_fix_session_store(Some(&db)).expect("select durable fix session store");

        store
            .save_counters(
                &key,
                SessionCounters {
                    next_sender_seq: 42,
                    next_target_seq: 77,
                },
            )
            .expect("save counters");
        store.store_outbound(&key, 1, b"frame-one").expect("out 1");
        store.store_outbound(&key, 2, b"frame-two").expect("out 2");
        store
            .store_outbound(&key, 3, b"frame-three")
            .expect("out 3");
        store
            .record_reset(
                &key,
                SequenceResetEvent {
                    at_ms: 1_700_000_000_000,
                    trigger: ResetTrigger::SequenceReset,
                    old_next_sender_seq: 42,
                    old_next_target_seq: 77,
                    new_next_sender_seq: 42,
                    new_next_target_seq: 90,
                },
                SessionCounters {
                    next_sender_seq: 42,
                    next_target_seq: 90,
                },
            )
            .expect("record reset");

        drop(store);
        drop(db);
    }

    // ---- "Second process": a FRESH pool against the SAME database resumes the
    // persisted session state, byte-for-byte and counter-for-counter. ----
    let db2 = DatabasePool::connect(&url, pool_config)
        .await
        .expect("reopen pool after restart");
    let store2 = select_fix_session_store(Some(&db2)).expect("reselect durable store");

    // Counters survived (the post-reset counters).
    let counters = store2.load_counters(&key).expect("load after restart");
    assert_eq!(
        counters,
        SessionCounters {
            next_sender_seq: 42,
            next_target_seq: 90,
        },
        "counters resume from the persisted post-reset state"
    );

    // The resend log survived, byte-exact and in order.
    let frames = store2
        .outbound_range(&key, 0, 0)
        .expect("range after restart");
    assert_eq!(
        frames,
        vec![
            StoredOutbound {
                seq: 1,
                frame: b"frame-one".to_vec()
            },
            StoredOutbound {
                seq: 2,
                frame: b"frame-two".to_vec()
            },
            StoredOutbound {
                seq: 3,
                frame: b"frame-three".to_vec()
            },
        ],
        "the resend log survives a restart, byte-exact and ordered"
    );

    // The reset audit survived.
    let resets = store2.reset_events(&key).expect("resets after restart");
    assert_eq!(resets.len(), 1, "the reset audit survives a restart");
    assert_eq!(resets[0].trigger, ResetTrigger::SequenceReset);
    assert_eq!(resets[0].new_next_target_seq, 90);

    // Numbering RESUMES: a subsequent save advances from the resumed counter and
    // reads back on the fresh pool.
    store2
        .save_counters(
            &key,
            SessionCounters {
                next_sender_seq: 43,
                next_target_seq: 91,
            },
        )
        .expect("resume save");
    assert_eq!(
        store2.load_counters(&key).expect("load resumed"),
        SessionCounters {
            next_sender_seq: 43,
            next_target_seq: 91,
        },
        "numbering resumes from the persisted state"
    );

    drop(container);
}

/// In-memory / durable-Postgres **behavioral parity**: the SAME sequence of
/// `FixSessionStore` trait calls yields the SAME observable sequence / resend /
/// reset behavior on both backends — "one contract, two backends, chosen at boot".
///
/// `#[ignore]` + `flavor = "multi_thread"` (same gating + runtime requirement as
/// the restart test).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_pg_and_in_memory_fix_session_parity() {
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

    // Both backends behind the SAME trait, chosen at boot.
    let pg = select_fix_session_store(Some(&db)).expect("select durable backend");
    let memory = select_fix_session_store(None).expect("select in-memory backend");

    let key = fix_key();
    let pg_obs = run_fix_session_scenario(pg.as_ref(), &key);
    let mem_obs = run_fix_session_scenario(memory.as_ref(), &key);

    assert_eq!(
        pg_obs, mem_obs,
        "the durable and in-memory backends must be observably identical"
    );

    // Spot-check the load-bearing shape (so a regression names WHICH facet drifted).
    assert_eq!(
        pg_obs.counters_after_save,
        SessionCounters {
            next_sender_seq: 10,
            next_target_seq: 20,
        }
    );
    assert_eq!(pg_obs.range_all.len(), 3, "three frames stored");
    assert_eq!(pg_obs.range_2_3.len(), 2, "closed range keeps [2,3]");
    assert!(
        pg_obs.range_from_5.is_empty(),
        "past-the-end range is empty"
    );
    assert_eq!(
        pg_obs.counters_after_reset,
        SessionCounters::default(),
        "a LogonReset returns both counters to 1"
    );
    assert_eq!(pg_obs.resets.len(), 1, "one reset audited");
    assert_eq!(
        pg_obs.range_1_1_after_reuse,
        vec![
            StoredOutbound {
                seq: 1,
                frame: b"aaa".to_vec()
            },
            StoredOutbound {
                seq: 1,
                frame: b"post-reset".to_vec()
            },
        ],
        "a re-used seq appends (no dedup) and reads oldest-first on both backends"
    );
    // The atomic emit primitive (#149 finding 1B) is faithful on both backends: the
    // frame lands AND only the outbound counter advances (inbound left deferred).
    assert_eq!(
        pg_obs.counters_after_atomic,
        SessionCounters {
            next_sender_seq: 3,
            next_target_seq: 1,
        },
        "store_outbound_and_advance moves ONLY next_sender; next_target is deferred"
    );
    assert_eq!(
        pg_obs.range_after_atomic,
        vec![
            // The pre-reset `store_outbound(2, "bbbb")` frame still resides at seq 2
            // (append semantics, oldest-first), then the atomically-stored frame.
            StoredOutbound {
                seq: 2,
                frame: b"bbbb".to_vec()
            },
            StoredOutbound {
                seq: 2,
                frame: b"atomic".to_vec()
            },
        ],
        "the atomically-stored frame is present at its seq on both backends"
    );

    drop(container);
}

/// The durable key-space bound is serialized across concurrent **new**-key inserts
/// (#095 finding 2): with the registry pre-filled to exactly `MAX_SESSION_KEYS - 1`,
/// firing several concurrent first-logons for DISTINCT new keys admits EXACTLY ONE
/// (reaching the ceiling); the rest are refused with `KeyspaceFull`, and the durable
/// count never exceeds `MAX_SESSION_KEYS`. Without the transaction-scoped advisory
/// lock the concurrent inserts would each observe `count < ceiling` under
/// `READ COMMITTED` and all insert, overshooting the ceiling.
///
/// `#[ignore]` + `flavor = "multi_thread"` (same gating + runtime requirement as the
/// other durable FIX-session tests).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_pg_fix_keyspace_bound_serialized_across_concurrent_new_keys() {
    use fauxchange::gateway::fix::{PgFixSessionStore, SessionStoreError};
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::{ImageExt, runners::AsyncRunner};

    // The keyspace ceiling the durable store enforces (mirrors the in-memory bound).
    const MAX_SESSION_KEYS: i64 = 65_536;

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
        max_connections: 8,
        slow_acquire: std::time::Duration::from_millis(500),
    };
    let db = DatabasePool::connect_and_migrate(&url, pool_config)
        .await
        .expect("open pool and run migrations");

    // Pre-fill the registry to EXACTLY ceiling - 1 distinct keys in one fast bulk
    // insert (raw SQL, test-only) — so the next NEW key sits precisely at the boundary.
    sqlx::query(
        "INSERT INTO fix_session_counters \
         (account_id, sender_comp_id, target_comp_id, next_sender_seq, next_target_seq) \
         SELECT 'fill-' || g::text, 'S', 'T', 1, 1 FROM generate_series(1, $1) g",
    )
    .bind(MAX_SESSION_KEYS - 1)
    .execute(db.pool())
    .await
    .expect("bulk-fill the registry to ceiling - 1");

    // Fire N concurrent first-logons for DISTINCT new keys against the boundary.
    let store = Arc::new(PgFixSessionStore::new(&db).expect("durable store"));
    let attempts = 8_usize;
    let mut handles = Vec::with_capacity(attempts);
    for i in 0..attempts {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let key = SessionKey::new(AccountId::new(format!("race-{i}")), "CLIENT", "FAUXCHANGE");
            store.save_counters(&key, SessionCounters::default())
        }));
    }

    let mut ok = 0_usize;
    let mut full = 0_usize;
    for handle in handles {
        match handle.await.expect("join") {
            Ok(()) => ok += 1,
            Err(SessionStoreError::KeyspaceFull { .. }) => full += 1,
            Err(other) => panic!("unexpected store error: {other:?}"),
        }
    }

    assert_eq!(
        ok, 1,
        "exactly one concurrent new key is admitted at the boundary"
    );
    assert_eq!(
        full,
        attempts - 1,
        "every other concurrent new key is refused KeyspaceFull"
    );

    // The durable count never exceeded the ceiling — the advisory-locked registry
    // update serialized the count-check-and-insert.
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM fix_session_counters")
        .fetch_one(db.pool())
        .await
        .expect("count rows");
    assert_eq!(
        count, MAX_SESSION_KEYS,
        "the durable keyspace is exactly at the ceiling, never overshot"
    );

    drop(container);
}
