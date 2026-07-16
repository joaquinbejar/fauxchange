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
use fauxchange::simulation::{AssetConfig, PriceUpdate, VenueClockConfig, WalkTypeConfig};
use fauxchange::state::{AppState, AppStateConfig};
use tokio::sync::broadcast;

// Durable-journal recovery integration (#029) — a REAL ephemeral
// `postgres:18-alpine` via `testcontainers`, never a mocked DB (rules SQL &
// Persistence). These are `#[ignore]`d so the default `cargo test` stays green
// without Docker; the CI `migrations` job runs them with `-- --ignored`.
use fauxchange::OrderType;
use fauxchange::db::{DatabasePool, DbPoolConfig, PgVenueJournal};
use fauxchange::exchange::{
    ActorConfig, FixedClock, Hash32, InMemoryExecutionsStore, InMemoryPositionsStore, JournalError,
    JournalHeader, JournalRecord, LineageId, MarkPriceBook, MatchingExecutor, RecordKind,
    SequenceNumber, Side, StoreFanOut, Symbol, TimeInForce, TopOfBook, UnderlyingActor,
    VenueJournal, recover,
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
