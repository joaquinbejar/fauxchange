//! Integration + determinism tests for the market maker on the sequenced path
//! ([015](../milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md)).
//!
//! - A price update → requote produces MM-tagged `AddOrder` `VenueCommand`s that
//!   route onto the sequenced order path.
//! - Replaying the resulting command stream (the "journal") through the upstream
//!   matching executor reproduces **identical** fills and top-of-book — the
//!   determinism oracle covers generated liquidity, and the requote is journaled,
//!   not re-derived.
//! - A crossing client order fills the maker's resting quote, attributed to the
//!   venue-reserved market-maker account.
//! - The real `AppState` wiring routes a live requote onto the per-underlying
//!   actor and journals it.

use std::sync::{Arc, Mutex};

use fauxchange::exchange::{
    Cents, CommandExecutor, EventTimestamp, ExecutionContext, JournalRecord, LineageId,
    MatchingExecutor, STPMode, SequenceNumber, Side, Symbol, TimeInForce, TopOfBook, VenueCommand,
    VenueOutcome, is_market_maker_account, is_market_maker_command, market_maker_account,
};
use fauxchange::market_maker::{CommandSink, MarketMakerEngine, Quoter};
use fauxchange::models::{AccountId, OrderType, VenueOrderId};
use fauxchange::state::{AppState, AppStateConfig};

const UNDERLYING: &str = "BTC";
const CALL: &str = "BTC-20351231-50000-C";
const TS: EventTimestamp = EventTimestamp::new(1_700_000_000_000);
/// 2025-01-01T00:00:00Z in ms — well before the 2035 expiry.
const VENUE_NOW_MS: u64 = 1_735_689_600_000;

/// A [`CommandSink`] that records the commands routed to it, in order.
#[derive(Default)]
struct CollectingSink {
    commands: Mutex<Vec<VenueCommand>>,
}

impl CollectingSink {
    fn take(&self) -> Vec<VenueCommand> {
        std::mem::take(&mut self.commands.lock().expect("sink lock"))
    }
}

impl CommandSink for CollectingSink {
    fn enqueue(&self, command: VenueCommand) {
        self.commands.lock().expect("sink lock").push(command);
    }
}

fn sym(raw: &str) -> Symbol {
    Symbol::parse(raw).expect("valid fixture symbol")
}

/// Builds an engine over a collecting sink, with the venue clock set.
fn engine() -> (MarketMakerEngine, Arc<CollectingSink>) {
    let sink = Arc::new(CollectingSink::default());
    let engine = MarketMakerEngine::new(sink.clone(), LineageId::new("run-1"), Quoter::default());
    engine.set_venue_now_ms(VENUE_NOW_MS);
    (engine, sink)
}

/// A client limit order (a non-market-maker account).
fn client_add(order_id: &str, side: Side, price: u64, quantity: u64) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(CALL),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new("alice"),
        owner: fauxchange::exchange::Hash32([0x11; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

/// Replays a command stream through a fresh executor, returning every outcome and
/// the final top-of-book — the determinism-oracle read surface.
fn replay(commands: &[VenueCommand]) -> (Vec<VenueOutcome>, TopOfBook) {
    let lineage = LineageId::new("run-1");
    let mut executor = MatchingExecutor::new(UNDERLYING);
    let mut outcomes = Vec::with_capacity(commands.len());
    for (index, command) in commands.iter().enumerate() {
        let outcome = executor.execute(ExecutionContext {
            underlying: UNDERLYING,
            lineage_id: &lineage,
            sequence: SequenceNumber::new(index as u64),
            venue_ts: TS,
            command,
        });
        outcomes.push(outcome);
    }
    (outcomes, executor.top_of_book(&sym(CALL)))
}

#[test]
fn test_requote_produces_market_maker_add_orders() {
    let (engine, sink) = engine();
    engine.register_instrument(&sym(CALL));
    engine.update_price(UNDERLYING, 5_000_000);

    let commands = sink.take();
    assert_eq!(commands.len(), 2, "first requote adds a bid and an ask");
    for command in &commands {
        match command {
            VenueCommand::AddOrder {
                symbol, account, ..
            } => {
                assert_eq!(symbol, &sym(CALL));
                assert!(
                    is_market_maker_account(account),
                    "every requote order carries the reserved MM account"
                );
            }
            other => panic!("expected an AddOrder, got {other:?}"),
        }
    }
}

/// #032: the pricer resolves time-to-expiry from the instrument's **absolute
/// `ExpirationDate::DateTime`** against the **venue clock**, never the wall clock.
/// Two requotes at the **same `venue_now_ms`** produce byte-identical limit prices
/// even though `Utc::now()` advances between the two calls — proving the clock-free
/// `DateTime − venue_now → days` seam is a pure function of the injected clock, the
/// precondition the replay oracle rests on.
#[test]
fn test_requote_prices_are_identical_under_a_fixed_venue_clock() {
    fn limit_prices(commands: &[VenueCommand]) -> Vec<(Side, Cents)> {
        commands
            .iter()
            .filter_map(|command| match command {
                VenueCommand::AddOrder {
                    side, limit_price, ..
                } => limit_price.map(|price| (*side, price)),
                _ => None,
            })
            .collect()
    }

    let (engine, sink) = engine();
    engine.register_instrument(&sym(CALL));

    engine.update_price(UNDERLYING, 5_000_000);
    let first = limit_prices(&sink.take());
    assert_eq!(first.len(), 2, "a requote quotes a bid and an ask");

    // A second requote at the identical venue clock — wall time has advanced since
    // the first call, the injected venue clock has not.
    engine.update_price(UNDERLYING, 5_000_000);
    let second = limit_prices(&sink.take());

    assert_eq!(
        first, second,
        "the pricer is a pure function of the venue clock — no wall-clock drift between requotes"
    );
}

#[test]
fn test_requote_commands_replay_to_a_crossing_fill_attributed_to_the_market_maker() {
    let (engine, sink) = engine();
    engine.register_instrument(&sym(CALL));
    engine.update_price(UNDERLYING, 5_000_000);
    let mut commands = sink.take();

    // The maker's ask price is the price a client buy will cross.
    let ask_price = commands
        .iter()
        .find_map(|c| match c {
            VenueCommand::AddOrder {
                side: Side::Sell,
                limit_price: Some(price),
                ..
            } => Some(*price),
            _ => None,
        })
        .expect("the requote placed an ask leg");

    // Append a client buy that crosses the resting maker ask.
    commands.push(client_add("client-1", Side::Buy, ask_price.get(), 1));

    let (outcomes, _top) = replay(&commands);
    // The last outcome (the client buy) captured a two-leg fill: maker = the MM.
    let last = outcomes.last().expect("an outcome for the client order");
    let fills = match last {
        VenueOutcome::Added { fills, .. } => fills,
        other => panic!("expected the client add to fill against the maker, got {other:?}"),
    };
    assert!(!fills.is_empty(), "the crossing client order filled");
    let maker = fills
        .iter()
        .find(|f| f.liquidity == fauxchange::LiquidityFlag::Maker)
        .expect("a maker leg");
    assert_eq!(
        maker.account,
        market_maker_account(),
        "the resting maker leg attributes to the venue-reserved market-maker account"
    );
}

#[test]
fn test_journaled_requotes_replay_to_identical_fills_and_top_of_book() {
    let (engine, sink) = engine();
    engine.register_instrument(&sym(CALL));
    engine.update_price(UNDERLYING, 5_000_000);
    let mut commands = sink.take();

    let ask_price = commands
        .iter()
        .find_map(|c| match c {
            VenueCommand::AddOrder {
                side: Side::Sell,
                limit_price: Some(price),
                ..
            } => Some(*price),
            _ => None,
        })
        .expect("an ask leg");
    commands.push(client_add("client-1", Side::Buy, ask_price.get(), 1));

    // The command stream (the journal content) actually contains the MM requote
    // adds — the determinism oracle covers generated liquidity, asserted from the
    // journal, not re-derived on replay.
    let mm_adds = commands
        .iter()
        .filter(|c| is_market_maker_command(c))
        .count();
    assert_eq!(mm_adds, 2, "the journal contains the two MM requote adds");

    // Replaying the SAME journal twice reproduces identical fills and top-of-book.
    let (outcomes_a, top_a) = replay(&commands);
    let (outcomes_b, top_b) = replay(&commands);
    assert_eq!(outcomes_a, outcomes_b, "identical fills on replay");
    assert_eq!(top_a, top_b, "identical top-of-book on replay");
}

#[tokio::test]
async fn test_appstate_wiring_routes_a_live_requote_onto_the_actor_and_journals_it() {
    let state = AppState::new(AppStateConfig::new([UNDERLYING])).expect("AppState with dev auth");
    let engine = state.market_maker();
    engine.set_venue_now_ms(VENUE_NOW_MS);
    engine.register_instrument(&sym(CALL));

    // A live price update requotes through the real ActorCommandSink → forwarder
    // → per-underlying actor → write-ahead journal.
    engine.update_price(UNDERLYING, 5_000_000);

    // The forwarder submits asynchronously; poll the journal (bounded) until the
    // MM requote adds are journaled.
    let mut mm_commands = 0;
    for _ in 0..200 {
        let snapshot = state
            .journal_snapshot(UNDERLYING)
            .await
            .expect("journal snapshot");
        mm_commands = snapshot
            .records
            .iter()
            .filter_map(|record| match record {
                JournalRecord::Command(journaled) => Some(&journaled.command),
                _ => None,
            })
            .filter(|command| is_market_maker_command(command))
            .count();
        if mm_commands >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        mm_commands >= 2,
        "the live requote's MM add orders reached the sequenced journal (found {mm_commands})"
    );
}
