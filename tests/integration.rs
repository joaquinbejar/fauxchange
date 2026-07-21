//! #028 integration: the price-walk cadence, the market maker it drives, and the
//! sequenced-path `venue_ts` all run off the **one** injected venue clock — an
//! accelerated run advances the cadence at the configured multiplier
//! ([04 §2](../docs/04-market-data-and-replay.md#2-synthetic-price-generation),
//! [04 §5](../docs/04-market-data-and-replay.md#5-clock-control)).
//!
//! The clock is driven with **controlled wall instants** ([`SimClock::track_wall`]),
//! so these assertions are deterministic rather than racing the real wall clock;
//! the live cadence loop reads `SystemTime` in the off-path driver
//! ([`SimClock::tick`]), which is exercised by the clock unit tests.

use std::sync::Arc;

use fauxchange::exchange::{Cents, EventTimestamp, VenueCommand};
use fauxchange::models::{AccountId, VenueOrderId};
use fauxchange::simulation::{
    AssetConfig, PriceUpdate, SessionConfig, VenueClockConfig, WalkTypeConfig, synthesize_chain,
};
use fauxchange::state::{AppState, AppStateConfig};
use tokio::sync::broadcast;

// Durable-journal recovery integration (#029) — a REAL ephemeral
// `postgres:18-alpine` via `testcontainers`, never a mocked DB (rules SQL &
// Persistence). These are `#[ignore]`d so the default `cargo test` stays green
// without Docker; the CI `migrations` job runs them with `-- --ignored`.
use fauxchange::OrderType;
use fauxchange::db::{DatabasePool, DbError, DbPoolConfig, PgVenueJournal};
use fauxchange::exchange::{
    ActorConfig, ExecutionsStore, FixedClock, Hash32, InMemoryExecutionsStore,
    InMemoryPositionsStore, JournalError, JournalHeader, JournalRecord, LineageId, MarkPriceBook,
    MatchingExecutor, PositionsStore, RecordKind, SequenceNumber, Side, StoreFanOut, Symbol,
    TimeInForce, TopOfBook, UnderlyingActor, VenueJournal, recover,
};

const UNDERLYING: &str = "BTC";
const SYMBOL: &str = "BTC-20240329-50000-C";
const MULTIPLIER: u32 = 60;
const START_MS: u64 = 1_000_000_000_000;

/// An `AppState` over one walked underlying on an **accelerated** venue clock — the
/// one clock the actors, the simulator, and the rate limiter share.
fn accelerated_state() -> Arc<AppState> {
    let config = AppStateConfig::new([UNDERLYING])
        .with_clock(VenueClockConfig::accelerated(START_MS, MULTIPLIER))
        .with_assets(vec![AssetConfig::new(
            UNDERLYING,
            Cents::new(5_000_000),
            0.20,
            WalkTypeConfig::GeometricBrownian,
        )]);
    match AppState::new(config) {
        Ok(state) => state,
        Err(e) => panic!("AppState with dev auth must build: {e}"),
    }
}

/// The `now_ms` of the next broadcast price update.
fn recv_now_ms(rx: &mut broadcast::Receiver<PriceUpdate>) -> u64 {
    match rx.try_recv() {
        Ok(update) => update.now_ms.get(),
        Err(e) => panic!("expected a broadcast price update: {e:?}"),
    }
}

/// A BTC cancel — the cheapest command that returns a receipt carrying `venue_ts`.
fn cancel() -> VenueCommand {
    VenueCommand::CancelOrder {
        symbol: match fauxchange::exchange::Symbol::parse(SYMBOL) {
            Ok(symbol) => symbol,
            Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
        },
        order_id: VenueOrderId::new("order-1"),
        account: AccountId::new("acct-1"),
    }
}

#[tokio::test]
async fn test_accelerated_clock_advances_price_walk_cadence_at_multiplier() {
    let state = accelerated_state();
    let sim = state.simulator();
    let mut prices = sim.subscribe();

    // The price-walk cadence runs off the injected clock: drive it with controlled
    // wall instants and step the walk — each SimStep is stamped from the SAME clock,
    // advancing at the multiplier.
    state.clock().track_wall(10_000); // anchor at the epoch
    sim.step_once();
    let first = recv_now_ms(&mut prices);
    assert_eq!(
        first, START_MS,
        "the first emit is at the anchored virtual epoch"
    );

    state.clock().track_wall(10_100); // +100 ms of wall time
    sim.step_once();
    let second = recv_now_ms(&mut prices);
    // 100 ms of wall time × 60 = 6_000 ms of virtual time advanced.
    assert_eq!(
        second,
        START_MS + 100 * u64::from(MULTIPLIER),
        "the accelerated cadence advanced at the configured multiplier"
    );
    assert_eq!(second - first, 6_000);
}

#[tokio::test]
async fn test_sequenced_venue_ts_and_price_walk_share_the_one_injected_clock() {
    let state = accelerated_state();

    // Advance the injected clock off the sequenced path (accelerated wall-tracking).
    state.clock().track_wall(20_000); // anchor
    state.clock().track_wall(20_500); // +500 ms wall × 60 = 30_000 virtual ms
    let expected = START_MS + 500 * u64::from(MULTIPLIER);
    assert_eq!(state.clock().now_ms().get(), expected);

    // A sequenced order's venue_ts is stamped from that SAME clock — so the actor's
    // venue_ts and the price-walk's now_ms are one injected clock, not two.
    let receipt = match state.submit(cancel()).await {
        Ok(receipt) => receipt,
        Err(e) => panic!("cancel must route to the BTC actor: {e}"),
    };
    assert_eq!(
        receipt.venue_ts,
        EventTimestamp::new(expected),
        "venue_ts is stamped from the same injected clock the price walk reads"
    );

    // And a walk step emitted after the advance carries the identical instant.
    let sim = state.simulator();
    let mut prices = sim.subscribe();
    sim.step_once();
    assert_eq!(
        recv_now_ms(&mut prices),
        expected,
        "the SimStep now_ms matches the advanced venue clock"
    );
}

// ============================================================================
// #031: stepped synthetic sessions — end to end (in-memory, no Docker)
// ============================================================================

/// The deterministic virtual epoch the session clock and expiry share.
const SESSION_START_MS: u64 = 1_735_689_600_000;
/// One virtual minute per stepped advance.
const SESSION_STEP_MS: u64 = 60_000;

/// A stepped-clock venue hosting `BTC` as the session's walked underlying.
fn session_config() -> SessionConfig {
    SessionConfig::new(
        "BTC",
        Cents::new(5_000_000), // $50,000
        30.0,
        0.20,
        WalkTypeConfig::GeometricBrownian,
    )
    .with_strike_interval(500)
    .with_chain_size(5)
    .with_smile_curve(0.5)
}

fn session_state(config: &SessionConfig) -> Arc<AppState> {
    let app_config = AppStateConfig::new(["BTC"])
        .with_lineage(LineageId::new("run-1"))
        .with_clock(VenueClockConfig::stepped(SESSION_START_MS, SESSION_STEP_MS))
        .with_assets(vec![config.to_asset_config()]);
    match AppState::new(app_config) {
        Ok(state) => state,
        Err(e) => panic!("session AppState must build: {e}"),
    }
}

/// Bounded wait for the async requote forwarders to drain, so the venue journal is
/// stable before it is snapshotted (the market-maker's requotes are journaled
/// off-thread). Polls until the highest journaled sequence holds steady across two
/// consecutive reads, or the window elapses.
async fn settle_journal(state: &Arc<AppState>, underlying: &str) {
    let mut last = None;
    let mut stable = 0;
    for _ in 0..400 {
        let snapshot = state
            .journal_snapshot(underlying)
            .await
            .expect("journal snapshot");
        let seq = snapshot.last_sequence;
        if seq == last {
            stable += 1;
            if stable >= 3 {
                return;
            }
        } else {
            stable = 0;
            last = seq;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn test_stepped_session_synthesises_seeds_and_advances() {
    let config = session_config();
    let state = session_state(&config);

    // Chain synthesis: expirations × strikes with the smile-shaped IVs.
    let chain = synthesize_chain(&config, SESSION_START_MS).expect("synthesise the chain");
    assert_eq!(chain.strikes.len(), 5);

    // Materialise onto the live venue: register each leaf with its IV + seed the
    // opening price; the maker's requotes vivify the leaf books.
    let contracts = state
        .materialize_session(&chain)
        .await
        .expect("materialise the session chain");
    assert_eq!(contracts, 10, "5 strikes × (call + put) registered");

    // The chain is live: every synthesised contract vivified onto the shared index.
    let present: std::collections::HashSet<String> =
        state.symbol_index().symbols().into_iter().collect();
    for strike in &chain.strikes {
        assert!(present.contains(strike.call.as_str()), "call leaf vivified");
        assert!(present.contains(strike.put.as_str()), "put leaf vivified");
    }

    // Step the session a few times on the stepped clock: each step advances the
    // venue clock and walks the price, journaled and driving the maker's requotes.
    for _ in 0..3 {
        let advance = state.step_session().await;
        assert!(!advance.is_partial(), "the clock fan-out is complete");
    }
    settle_journal(&state, "BTC").await;

    // The venue clock advanced by exactly three stepped intervals.
    assert_eq!(
        state.clock().now_ms(),
        EventTimestamp::new(SESSION_START_MS + 3 * SESSION_STEP_MS),
        "three stepped advances moved the venue clock deterministically"
    );
}

#[tokio::test]
async fn test_stepped_session_replays_from_the_journal() {
    // The session's journaled SimStep / Clock commands and the maker's derived
    // requote AddOrders replay from the journal IDENTICALLY through the #030 driver
    // (its `Ok` proves every re-derived event equalled the stored one), with the
    // live requote engine muted by construction (the offline driver never invokes
    // it, so no cascading duplicate requotes are generated).
    let config = session_config();
    let state = session_state(&config);
    let chain = synthesize_chain(&config, SESSION_START_MS).expect("synthesise");
    state
        .materialize_session(&chain)
        .await
        .expect("materialise");
    for _ in 0..3 {
        state.step_session().await;
    }
    settle_journal(&state, "BTC").await;

    // Export the venue journal and replay it offline into a fresh registry.
    let bundle = state
        .export_bundle()
        .await
        .expect("export the session bundle");
    let report = state
        .replay_bundle(&bundle)
        .await
        .expect("replay the session");

    let replay = report.underlying("BTC").expect("BTC replay");
    // No cascade: the reconstructed event count equals the journaled command count
    // (the driver replays the journaled requotes, never re-deriving them).
    let journaled_commands = state
        .journal_snapshot("BTC")
        .await
        .expect("snapshot")
        .records
        .iter()
        .filter(|record| record.kind() == RecordKind::Command)
        .count();
    assert_eq!(
        replay.events.len(),
        journaled_commands,
        "replay reproduces exactly the journaled commands — no cascading requote"
    );
    assert!(
        replay.events.len() >= 4,
        "the session journaled a non-trivial stream"
    );

    // The reconstructed top-of-book for the ATM call matches the live venue's — the
    // maker's synthetic liquidity is reproduced from the journal.
    let atm = chain
        .strikes
        .iter()
        .find(|s| s.strike == 50_000)
        .expect("ATM strike");
    let live = state
        .journal_snapshot("BTC")
        .await
        .expect("snapshot")
        .records
        .iter()
        .filter(|r| r.kind() == RecordKind::Command)
        .count();
    assert_eq!(live, journaled_commands);
    // The reconstructed book has resting maker liquidity on the ATM call.
    let top = replay.top_of_book(&atm.call);
    assert!(
        top.best_bid.is_some() || top.best_ask.is_some(),
        "the ATM call carries reconstructed maker liquidity"
    );
}

#[tokio::test]
async fn test_stepped_session_client_order_matches_synthetic_liquidity() {
    // After chain synthesis the venue is LIVE: a client order matches against the
    // maker's synthesised liquidity and fills.
    let config = session_config();
    let state = session_state(&config);
    let chain = synthesize_chain(&config, SESSION_START_MS).expect("synthesise");
    state
        .materialize_session(&chain)
        .await
        .expect("materialise");
    settle_journal(&state, "BTC").await;

    let atm = chain
        .strikes
        .iter()
        .find(|s| s.strike == 50_000)
        .expect("ATM strike");

    // An aggressive client BUY crosses the maker's resting ask on the ATM call.
    let client_buy = VenueCommand::AddOrder {
        symbol: atm.call.clone(),
        order_id: VenueOrderId::new("client-1"),
        account: AccountId::new("client"),
        owner: Hash32([0x42; 32]),
        client_order_id: None,
        side: Side::Buy,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(100_000_000)), // far above theo → crosses any ask
        quantity: 1,
        time_in_force: TimeInForce::Gtc,
        stp_mode: fauxchange::exchange::STPMode::None,
    };
    state.submit(client_buy).await.expect("client buy routes");

    // The client filled against the maker's synthetic liquidity.
    use fauxchange::exchange::ExecutionFilter;
    let fills = state
        .executions()
        .list(&AccountId::new("client"), &ExecutionFilter::default())
        .expect("client executions");
    assert!(
        !fills.is_empty(),
        "the client order matched the maker's synthetic liquidity and filled"
    );
    assert_eq!(fills[0].instrument, atm.call, "filled on the ATM call");
    assert!(fills[0].quantity >= 1, "at least one contract filled");
}

// ============================================================================
// Durable journal recovery (#029) — testcontainers postgres:18-alpine
// ============================================================================

const JOURNAL_UNDERLYING: &str = "BTC";
const JOURNAL_CALL: &str = "BTC-20240329-50000-C";
const JOURNAL_CLOCK: FixedClock = FixedClock::new(EventTimestamp::new(1_700_000_000_000));

/// The concrete durable-order-path actor the integration tests drive directly
/// (synchronously, under the single writer) — the real [`MatchingExecutor`] writing
/// through a durable [`PgVenueJournal`], with the #008 [`StoreFanOut`] so
/// snapshot/restore is available.
type DurableActor = UnderlyingActor<
    PgVenueJournal,
    MatchingExecutor,
    StoreFanOut<InMemoryExecutionsStore, InMemoryPositionsStore>,
    FixedClock,
>;

/// Starts an ephemeral `postgres:18-alpine`, opens the pool, and runs the embedded
/// migrations (so the journal schema exists). Never a mocked DB.
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
    let db = DatabasePool::connect_and_migrate(
        &url,
        DbPoolConfig {
            max_connections: 5,
            slow_acquire: std::time::Duration::from_millis(500),
        },
    )
    .await
    .expect("open pool and run migrations");
    (container, db)
}

fn journal_sym() -> Symbol {
    match Symbol::parse(JOURNAL_CALL) {
        Ok(symbol) => symbol,
        Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
    }
}

/// A resting/crossing limit add whose venue order id is minted from the id grammar
/// for the sequence it will be assigned (submissions are serial, so `sequence`
/// matches).
fn journal_add(
    lineage: &LineageId,
    sequence: u64,
    side: Side,
    price: u64,
    quantity: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: journal_sym(),
        order_id: lineage.venue_order_id(JOURNAL_UNDERLYING, SequenceNumber::new(sequence), 0),
        account: AccountId::new(format!("acct-{sequence}")),
        owner: Hash32([sequence as u8; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: fauxchange::exchange::STPMode::None,
    }
}

/// Builds the direct-drive durable actor over `db` for `JOURNAL_UNDERLYING`, keyed
/// on `lineage`.
fn durable_actor(db: &DatabasePool, lineage: &LineageId) -> DurableActor {
    let journal = PgVenueJournal::open(db, JOURNAL_UNDERLYING, JournalHeader::new(lineage.clone()))
        .expect("open durable journal");
    let executor = MatchingExecutor::new(JOURNAL_UNDERLYING);
    let fan_out = StoreFanOut::new(
        Arc::new(InMemoryExecutionsStore::new()),
        Arc::new(InMemoryPositionsStore::new()),
        Arc::new(MarkPriceBook::new()),
    );
    let config = ActorConfig::new(JOURNAL_UNDERLYING, lineage.clone(), 64);
    UnderlyingActor::new(config, journal, executor, fan_out, JOURNAL_CLOCK)
}

/// The stored `VenueEvent`s in the durable journal, in `N` order.
fn stored_events(journal: &PgVenueJournal) -> Vec<fauxchange::exchange::VenueEvent> {
    let records = match journal.read_from(SequenceNumber::START) {
        Ok(records) => records,
        Err(e) => panic!("durable read_from failed: {e}"),
    };
    records
        .into_iter()
        .filter_map(|record| match record {
            JournalRecord::Event(event) => Some(event),
            _ => None,
        })
        .collect()
}

/// Append a session's journal → restart → replay from sequence 0 into a FRESH
/// registry → reconstruct identical book state. Recovery is re-execution with the
/// stored event as the oracle: `recover` returning `Ok` PROVES every re-derived
/// event equalled the durably-stored one (a mismatch would halt with
/// `JournalCorruption`), and the reconstructed top-of-book is asserted concretely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_durable_journal_recovers_book_by_reexecution() {
    let (container, db) = start_pg().await;
    let lineage = LineageId::new("run-1");

    // A deterministic crossing session: rest a call ask (3 @ 50000) and a bid
    // (2 @ 49900), then a marketable buy (1 @ 50000) crosses one unit of the ask.
    {
        let mut actor = durable_actor(&db, &lineage);
        for command in [
            journal_add(&lineage, 0, Side::Sell, 50_000, 3),
            journal_add(&lineage, 1, Side::Buy, 49_900, 2),
            journal_add(&lineage, 2, Side::Buy, 50_000, 1),
        ] {
            actor.handle(command).expect("durable turn commits");
        }
        // Actor (and its journal handle) dropped here — a clean "shutdown".
    }

    // Restart: a FRESH durable handle over the SAME Postgres, then recover.
    let restarted = PgVenueJournal::open_for_recovery(&db, JOURNAL_UNDERLYING)
        .expect("reopen durable journal for recovery");
    let durable_events = stored_events(&restarted);

    let recovered = match recover(&restarted, JOURNAL_UNDERLYING) {
        Ok(recovered) => recovered,
        Err(e) => panic!("recovery of a clean durable journal must not halt: {e:?}"),
    };

    // Recovery re-derived exactly the durably-stored event stream.
    assert_eq!(
        recovered.events, durable_events,
        "recovery re-executes the durable journal to events equal to the stored ones"
    );
    assert_eq!(recovered.last_sequence, Some(SequenceNumber::new(2)));

    // And the reconstructed book matches the session's known resting state: the
    // ask has 2 of 3 left (one unit crossed), the bid rests at 49_900.
    let top = recovered.executor.top_of_book(&journal_sym());
    assert_eq!(
        top,
        TopOfBook {
            best_bid: Some(Cents::new(49_900)),
            best_ask: Some(Cents::new(50_000)),
            bid_depth: 2,
            ask_depth: 2,
        },
        "the durable journal reconstructs identical book state"
    );

    drop(container);
}

/// `underlying_sequence` CONTINUES across a snapshot epoch — the `SnapshotRestored`
/// marker opens the new epoch at the NEXT `N` (never a reset), durably persisted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_durable_journal_sequence_continues_across_snapshot_epoch() {
    let (container, db) = start_pg().await;
    let lineage = LineageId::new("run-1");

    {
        let mut actor = durable_actor(&db, &lineage);
        // Three pre-restore commands: sequences 0, 1, 2.
        for seq in 0..3 {
            actor
                .handle(journal_add(&lineage, seq, Side::Sell, 50_000 + seq, 1))
                .expect("pre-restore turn commits");
        }
        // Capture + restore: the epoch marker opens at the CONTINUED sequence 3.
        let snapshot = actor.capture("snap-1", "fp-1");
        let receipt = actor
            .restore(&snapshot, "fp-1")
            .expect("restore over the consistent cut");
        assert_eq!(
            receipt.underlying_sequence,
            SequenceNumber::new(3),
            "the epoch marker opens at the continued sequence 3, not a reset to 0"
        );
        // One post-restore command continues past the marker at sequence 4.
        actor
            .handle(journal_add(&lineage, 4, Side::Sell, 51_000, 1))
            .expect("post-restore turn commits");
    }

    // Restart and read the durable stream back.
    let restarted =
        PgVenueJournal::open_for_recovery(&db, JOURNAL_UNDERLYING).expect("reopen durable journal");
    let records = restarted
        .read_from(SequenceNumber::START)
        .expect("durable read_from");

    // The sequence never reset: the highest sequence present is 4.
    assert_eq!(restarted.last_sequence(), Some(SequenceNumber::new(4)));

    // Exactly one epoch marker, at the CONTINUED sequence 3.
    let epochs: Vec<&JournalRecord> = records
        .iter()
        .filter(|record| record.kind() == RecordKind::Epoch)
        .collect();
    assert_eq!(epochs.len(), 1, "one snapshot epoch marker");
    assert_eq!(
        epochs[0].sequence(),
        SequenceNumber::new(3),
        "the epoch marker sits at the continued sequence, never a reset"
    );

    // The post-restore command continues past the marker at sequence 4.
    let post_epoch_commands: Vec<u64> = records
        .iter()
        .filter(|record| record.kind() == RecordKind::Command)
        .map(|record| record.sequence().get())
        .filter(|&seq| seq > 3)
        .collect();
    assert_eq!(
        post_epoch_commands,
        vec![4],
        "the post-restore command continues the sequence past the epoch marker"
    );

    drop(container);
}

/// `(underlying, N, kind)` is unique; an idempotent re-append of the IDENTICAL
/// record is a NO-OP, and a differing payload at the same key is a `Conflict`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_journal_reappend_is_noop() {
    let (container, db) = start_pg().await;
    let lineage = LineageId::new("run-1");
    let mut journal =
        PgVenueJournal::open(&db, JOURNAL_UNDERLYING, JournalHeader::new(lineage.clone()))
            .expect("open durable journal");

    let command = journal_add(&lineage, 0, Side::Sell, 50_000, 1);
    let record = JournalRecord::command(SequenceNumber::new(0), EventTimestamp::new(1), command);

    // First append commits; the identical re-append is an idempotent no-op.
    journal
        .append(record.clone())
        .expect("first append commits");
    journal
        .append(record.clone())
        .expect("identical re-append is a no-op");
    let all = journal
        .read_from(SequenceNumber::START)
        .expect("durable read_from");
    assert_eq!(all.len(), 1, "a re-append must not duplicate the record");

    // A DIFFERENT payload at the same (underlying, N, kind) is an integrity error.
    let conflicting = JournalRecord::command(
        SequenceNumber::new(0),
        EventTimestamp::new(999),
        journal_add(&lineage, 0, Side::Buy, 40_000, 7),
    );
    match journal.append(conflicting) {
        Err(JournalError::Conflict { sequence, kind }) => {
            assert_eq!(sequence, SequenceNumber::new(0));
            assert_eq!(kind, RecordKind::Command);
        }
        other => panic!("expected a Conflict at the same key, got {other:?}"),
    }

    drop(container);
}

/// Recovery REFUSES a journal whose envelope schema is newer than the binary
/// understands, with the typed `JournalError::SchemaTooNew` on the DURABLE path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_journal_refuses_newer_schema() {
    let (container, db) = start_pg().await;

    // Persist a header written by a hypothetical LATER binary (schema `venue.v2`),
    // via a runtime-checked parameterised query (no macro, so no offline data).
    sqlx::query(
        "INSERT INTO journal_headers (underlying, lineage_id, schema_version) VALUES ($1, $2, $3)",
    )
    .bind(JOURNAL_UNDERLYING)
    .bind("run-1")
    .bind("venue.v2")
    .execute(db.pool())
    .await
    .expect("seed a newer-schema header");

    let journal =
        PgVenueJournal::open_for_recovery(&db, JOURNAL_UNDERLYING).expect("reopen durable journal");
    match recover(&journal, JOURNAL_UNDERLYING) {
        Err(JournalError::SchemaTooNew { found }) => assert_eq!(found, "venue.v2"),
        other => panic!("expected a SchemaTooNew refusal on the durable path, got {other:?}"),
    }

    drop(container);
}

/// `open` REFUSES to open a durable stream under a FRESH run lineage over a
/// PRE-EXISTING stream: it reads the persisted header back and compares it, so a
/// lineage disagreement is the typed `DbError::HeaderMismatch` (never a silently
/// cached foreign header that would corrupt replay/recovery identity), while a
/// re-open under the SAME lineage succeeds (#112, #84).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_open_refuses_fresh_lineage_over_existing_stream() {
    let (container, db) = start_pg().await;

    // First open persists lineage `run-1`'s header for the stream and caches it.
    let first = PgVenueJournal::open(
        &db,
        JOURNAL_UNDERLYING,
        JournalHeader::new(LineageId::new("run-1")),
    )
    .expect("first open persists the run-1 header");
    assert_eq!(first.header().lineage_id, LineageId::new("run-1"));

    // A restart under a FRESH lineage over the SAME durable stream is refused: the
    // stored records belong to `run-1`, so caching `run-2` would silently corrupt
    // replay identity. The header is read back and the mismatch is typed.
    match PgVenueJournal::open(
        &db,
        JOURNAL_UNDERLYING,
        JournalHeader::new(LineageId::new("run-2")),
    ) {
        Err(DbError::HeaderMismatch { stored, supplied }) => {
            assert!(
                stored.contains("run-1"),
                "the refusal names the persisted lineage: {stored}"
            );
            assert!(
                supplied.contains("run-2"),
                "the refusal names the fresh lineage: {supplied}"
            );
        }
        other => panic!("expected a HeaderMismatch refusal on a fresh lineage, got {other:?}"),
    }

    // Re-opening under the SAME lineage is fine (idempotent; the header matches).
    let reopened = PgVenueJournal::open(
        &db,
        JOURNAL_UNDERLYING,
        JournalHeader::new(LineageId::new("run-1")),
    )
    .expect("re-open under the same lineage succeeds");
    assert_eq!(reopened.header().lineage_id, LineageId::new("run-1"));

    drop(container);
}

/// The live-write `open` path (not just recovery) ALSO refuses a stream whose STORED
/// envelope schema disagrees with this binary's — the same read-back/compare guard,
/// so a newer-schema stream is never opened under a stale-schema header (#112, #84).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_open_refuses_foreign_schema_over_existing_stream() {
    let (container, db) = start_pg().await;

    // Seed a header written by a hypothetical LATER binary (schema `venue.v2`), via a
    // runtime-checked parameterised query (no macro, so no offline data).
    sqlx::query(
        "INSERT INTO journal_headers (underlying, lineage_id, schema_version) VALUES ($1, $2, $3)",
    )
    .bind(JOURNAL_UNDERLYING)
    .bind("run-1")
    .bind("venue.v2")
    .execute(db.pool())
    .await
    .expect("seed a newer-schema header");

    // Opening the live write path under the current `venue.v1` schema is refused.
    match PgVenueJournal::open(
        &db,
        JOURNAL_UNDERLYING,
        JournalHeader::new(LineageId::new("run-1")),
    ) {
        Err(DbError::HeaderMismatch { stored, supplied }) => {
            assert!(
                stored.contains("venue.v2"),
                "the refusal names the persisted schema: {stored}"
            );
            assert!(
                supplied.contains("venue.v1"),
                "the refusal names this binary's schema: {supplied}"
            );
        }
        other => panic!("expected a HeaderMismatch on a schema disagreement, got {other:?}"),
    }

    drop(container);
}

// ============================================================================
// Replay driver end-to-end (#030) — testcontainers postgres:18-alpine
// ============================================================================

/// A limit add onto the `AppState` BTC book, minting the venue order id from the id
/// grammar at its sequence.
fn app_add(
    lineage: &LineageId,
    sequence: u64,
    account: &str,
    owner: u8,
    side: Side,
    price: u64,
    quantity: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: journal_sym(),
        order_id: lineage.venue_order_id(JOURNAL_UNDERLYING, SequenceNumber::new(sequence), 0),
        account: AccountId::new(account),
        owner: Hash32([owner; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: fauxchange::exchange::STPMode::None,
    }
}

/// Record a scenario into a **durable** venue (the sequenced order path REST enters,
/// journaled through Postgres), export the durable journal as a portable scenario
/// bundle, replay the bundle into a **fresh** registry offline, and assert the
/// reconstructed executions store + positions fold match the live venue's — the
/// persistent-path oracle end to end (#030).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run in the CI migrations job with `-- --ignored`"]
async fn test_record_over_durable_venue_then_replay_bundle_matches_goldens() {
    use fauxchange::exchange::ExecutionFilter;

    let (container, db) = start_pg().await;

    // A durable venue hosting BTC — every committed command is journaled to Postgres.
    let state = AppState::new(
        AppStateConfig::new([JOURNAL_UNDERLYING])
            .with_lineage(LineageId::new("run-1"))
            .with_db(Some(db)),
    )
    .expect("durable AppState builds");
    let lineage = LineageId::new("run-1");

    // Record a crossing scenario onto the sequenced path (the same path REST enters):
    // a resting maker sell (3) and a bid (2), then a marketable buy (1) crosses.
    for command in [
        app_add(&lineage, 0, "maker", 0x11, Side::Sell, 50_000, 3),
        app_add(&lineage, 1, "bidder", 0x33, Side::Buy, 49_900, 2),
        app_add(&lineage, 2, "taker", 0x22, Side::Buy, 50_000, 1),
    ] {
        state.submit(command).await.expect("durable submit commits");
    }

    // Export the DURABLE journal as a portable bundle (its version set is pinned).
    let bundle = state
        .export_bundle()
        .await
        .expect("export the recorded scenario");
    assert!(bundle.is_current_schema());
    assert!(bundle.manifest.versions.matches_current());

    // Replay the bundle OFFLINE into a fresh registry (no durable venue involved).
    let report = state
        .replay_bundle(&bundle)
        .await
        .expect("replay the bundle");

    // The reconstructed top-of-book matches the recorded end state (2 of the ask
    // left after 1 crossed; the bid rests at 49_900).
    let replay = report.underlying(JOURNAL_UNDERLYING).expect("BTC replay");
    assert_eq!(
        replay.top_of_book(&journal_sym()),
        TopOfBook {
            best_bid: Some(Cents::new(49_900)),
            best_ask: Some(Cents::new(50_000)),
            bid_depth: 2,
            ask_depth: 2,
        },
        "the replayed bundle reconstructs identical book state"
    );

    // The reconstructed executions store + positions fold match the LIVE goldens.
    for account in ["maker", "taker"] {
        let account = AccountId::new(account);
        let golden = state
            .executions()
            .list(&account, &ExecutionFilter::default())
            .expect("live executions list");
        let reconstructed = report
            .executions
            .list(&account, &ExecutionFilter::default())
            .expect("reconstructed executions list");
        assert!(!golden.is_empty(), "{account:?} has a recorded fill leg");
        assert_eq!(
            reconstructed, golden,
            "the reconstructed executions store matches the live golden for {account:?}"
        );

        // Positions fold (mark-free — the journaled fold, not the live mark).
        let golden_pos = state
            .positions()
            .get(&account, &journal_sym(), None)
            .expect("live positions get");
        let reconstructed_pos = report
            .positions
            .get(&account, &journal_sym(), None)
            .expect("reconstructed positions get");
        assert_eq!(
            reconstructed_pos, golden_pos,
            "the reconstructed positions fold matches the live golden for {account:?}"
        );
    }

    drop(container);
}
