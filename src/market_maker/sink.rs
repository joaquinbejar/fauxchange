//! The seam by which the market maker reaches the **sequenced order path**
//! ([015](../../milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md),
//! [02 §4](../../docs/02-matching-architecture.md)).
//!
//! ## Requotes are journaled orders, never a side channel
//!
//! A generated quote never touches a book directly. The engine builds an
//! [`AddOrder`](VenueCommand::AddOrder) / [`CancelOrder`](VenueCommand::CancelOrder)
//! [`VenueCommand`] and hands it to a [`CommandSink`], which routes it onto the
//! per-underlying single-writer actor exactly like a client order — so generated
//! liquidity is journaled and replayable, part of the determinism oracle rather
//! than an unreproducible background process. A direct book call would be a
//! review blocker.
//!
//! Every requote command carries the venue-reserved market-maker identity
//! ([`crate::exchange::market_maker_account`] / [`crate::exchange::MARKET_MAKER_OWNER`])
//! so fills attribute to the maker and the WS subscription manager can suppress
//! the requote's `orderbook_delta`. That marker is a venue-wide contract consumed
//! by both this domain and the WS service, so it lives in [`crate::exchange`]
//! beside the [`VenueCommand`] it tags ([02 §6](../../docs/02-matching-architecture.md)).
//!
//! ## The requote is admitted against the venue price band (#109)
//!
//! Before the forwarder hands a requote to its actor it runs the **same**
//! [`check_price_band`] the gateway order seam ([`crate::state::AppState::submit`])
//! runs — never a second, forked band check. A requote whose limit price falls
//! outside the underlying's `[min_price_cents, max_price_cents]` band is **dropped**
//! (never posted, never journaled), logged at `debug`; a cancel (no limit price)
//! and the in-band side (a separate command) pass through untouched, so the maker
//! keeps quoting the side that fits. The decision is a pure function of the resolved
//! config and the price — no wall-clock, RNG, or map-iteration order — so a quote
//! dropped on a live run is dropped identically on replay: the journal simply never
//! contains it, and re-execution reproduces the same stream
//! ([05 §4.1](../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable)).

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::exchange::{ActorHandle, SymbolParser, VenueCommand, check_price_band};
use crate::microstructure::MicrostructureConfig;

/// The sink a market maker routes its generated commands into — the sequenced
/// order path.
///
/// `enqueue` is **non-blocking and fire-and-forget**: it hands the command off
/// without awaiting its receipt, so the requote loop never sits on a client's
/// order path and a slow actor turn never inflates a client `AddOrder`'s latency
/// (rule 8). Ordering of the commands handed to a single sink is preserved.
pub trait CommandSink: Send + Sync {
    /// Routes `command` onto the sequenced order path for its underlying,
    /// enqueuing it without awaiting the receipt. Never blocks.
    fn enqueue(&self, command: VenueCommand);
}

/// The default bounded capacity of the [`ActorCommandSink`] forwarding channel —
/// a DoS control, never unbounded. A full channel drops the requote command with
/// a `WARN` (best-effort generated liquidity) rather than blocking the loop.
pub const DEFAULT_SINK_CAPACITY: usize = 4_096;

/// The production [`CommandSink`]: routes each requote command to the right
/// per-underlying [`ActorHandle`] through a single ordered forwarder task, off
/// the requote loop's thread.
///
/// `enqueue` performs a non-blocking `try_send` onto a bounded channel; a
/// dedicated forwarder task drains it **in order** and `submit`s each command to
/// the underlying's actor, dropping the receipt. Because the forwarder is its own
/// task, a slow actor turn delays only the maker's own liquidity, never a client.
///
/// Ordering note: the forwarder awaits each `submit` receipt before the next
/// command, so intra-requote order (cancel-before-add) is preserved. A
/// fire-and-forget *ordered* enqueue on [`ActorHandle`] (owned by the matching
/// subsystem) would remove the receipt round-trip; the forwarder is the interim
/// entry point until that lands.
pub struct ActorCommandSink {
    tx: mpsc::Sender<VenueCommand>,
}

impl ActorCommandSink {
    /// Builds the sink over the venue's per-underlying actor handles and spawns
    /// its forwarder task, admitting each requote against `microstructure`'s price
    /// band (#109). Must be called within a `tokio` runtime.
    #[must_use]
    pub fn new(
        handles: HashMap<Arc<str>, ActorHandle>,
        microstructure: Arc<MicrostructureConfig>,
    ) -> Arc<Self> {
        Self::with_capacity(handles, microstructure, DEFAULT_SINK_CAPACITY)
    }

    /// Builds the sink with an explicit bounded forwarder capacity.
    #[must_use]
    pub fn with_capacity(
        handles: HashMap<Arc<str>, ActorHandle>,
        microstructure: Arc<MicrostructureConfig>,
        capacity: usize,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let forwarder = Forwarder {
            handles,
            rx,
            microstructure,
        };
        tokio::spawn(forwarder.run());
        Arc::new(Self { tx })
    }
}

impl CommandSink for ActorCommandSink {
    fn enqueue(&self, command: VenueCommand) {
        match self.tx.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    "market-maker command sink is full; dropping a requote command (backpressure)"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("market-maker command sink is closed; dropping a requote command");
            }
        }
    }
}

/// The forwarder task: drains the bounded channel in order and submits each
/// command onto its underlying's actor, dropping the receipt.
struct Forwarder {
    handles: HashMap<Arc<str>, ActorHandle>,
    rx: mpsc::Receiver<VenueCommand>,
    /// The resolved venue microstructure — the source of the price band each
    /// requote is admitted against before it reaches a leaf (#109).
    microstructure: Arc<MicrostructureConfig>,
}

impl Forwarder {
    async fn run(mut self) {
        while let Some(command) = self.rx.recv().await {
            let Some(underlying) = routable_underlying(&command) else {
                tracing::warn!("market-maker command carries no routable underlying; dropping");
                continue;
            };
            let Some(handle) = self.handles.get(underlying.as_str()) else {
                tracing::warn!(
                    underlying = %underlying,
                    "market-maker command routes to an unhosted underlying; dropping"
                );
                continue;
            };
            // Admit the requote against the venue-owned price band using the SAME
            // `check_price_band` the gateway submit seam runs (#109) — never a
            // second band check. A band-violating quote is DROPPED before `submit`
            // (never posted, never journaled); a cancel and the in-band side (a
            // separate command) carry no violation and pass. The drop is a pure
            // function of config + price, so it is identical on a live run and on
            // replay: the out-of-band quote is simply never in the journal.
            if let Err(error) = check_price_band(&self.microstructure, &command) {
                tracing::debug!(
                    underlying = %underlying,
                    error = %error,
                    "market-maker requote is outside the venue price band; dropping (not posting)"
                );
                continue;
            }
            if let Err(error) = handle.submit(command).await {
                // Best-effort: a rejected requote (full mailbox, sealed underlying)
                // is dropped; the next price tick requotes.
                tracing::debug!(error = %error, "market-maker requote command not accepted");
            }
        }
        tracing::debug!("market-maker command forwarder stopped");
    }
}

/// The underlying a requote command routes to (parsed from its target symbol).
/// The market maker only ever emits `AddOrder` / `CancelOrder` / `Replace`.
#[must_use]
fn routable_underlying(command: &VenueCommand) -> Option<String> {
    let symbol = match command {
        VenueCommand::AddOrder { symbol, .. }
        | VenueCommand::CancelOrder { symbol, .. }
        | VenueCommand::Replace { symbol, .. } => symbol,
        _ => return None,
    };
    SymbolParser::parse(symbol.as_str())
        .ok()
        .map(|parsed| parsed.underlying().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{
        ActorConfig, Cents, EventTimestamp, FixedClock, InMemoryVenueJournal, JournalHeader,
        JournalRecord, LineageId, MARKET_MAKER_OWNER, MatchingExecutor, NoopFanOut, STPMode, Side,
        Symbol, TimeInForce, market_maker_account, spawn_underlying_actor,
    };
    use crate::models::{OrderType, VenueOrderId};

    const BTC_CALL: &str = "BTC-20351231-50000-C";

    fn sym(raw: &str) -> Symbol {
        Symbol::parse(raw).expect("valid fixture symbol")
    }

    fn mm_add(symbol: &str) -> VenueCommand {
        mm_add_priced(symbol, "mm-1", Side::Buy, 100)
    }

    /// A market-maker `AddOrder` at `price` cents on `side` — the shape the engine
    /// enqueues per requote leg (bid and ask are separate commands).
    fn mm_add_priced(symbol: &str, order_id: &str, side: Side, price: u64) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym(symbol),
            order_id: VenueOrderId::new(order_id),
            account: market_maker_account(),
            owner: MARKET_MAKER_OWNER,
            client_order_id: None,
            side,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity: 1,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        }
    }

    #[test]
    fn test_routable_underlying_parses_the_symbol() {
        assert_eq!(
            routable_underlying(&mm_add("BTC-20240329-50000-C")),
            Some("BTC".to_string())
        );
        assert_eq!(
            routable_underlying(&VenueCommand::Clock {
                now_ms: EventTimestamp::new(1),
            }),
            None
        );
    }

    /// The limit prices of every `AddOrder` **command** journaled onto `handle`'s
    /// stream — the write-ahead record is appended before matching, so a command
    /// that reached the actor appears here whether or not the leaf accepted it. A
    /// requote dropped at the sink is absent entirely.
    async fn journaled_add_prices(handle: &ActorHandle) -> Vec<u64> {
        let snapshot = handle.snapshot().await.expect("journal snapshot");
        snapshot
            .records
            .iter()
            .filter_map(|record| match record {
                JournalRecord::Command(command) => match &command.command {
                    VenueCommand::AddOrder {
                        limit_price: Some(price),
                        ..
                    } => Some(price.get()),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    /// A band-violating requote (an ask above `max_price_cents`) is DROPPED at the
    /// sink and never reaches the leaf — its command is never journaled — while the
    /// in-band bid on the same requote still posts (#109). Proven against a real
    /// single-underlying actor: the sink forwards onto it, and only the admitted
    /// command lands in its write-ahead journal.
    #[tokio::test]
    async fn test_out_of_band_requote_is_dropped_and_in_band_side_still_posts() {
        let lineage = LineageId::new("run-mm-band");
        let (handle, _join) = spawn_underlying_actor(
            ActorConfig::new("BTC", lineage.clone(), 64),
            InMemoryVenueJournal::new(JournalHeader::new(lineage)),
            MatchingExecutor::new("BTC"),
            NoopFanOut,
            FixedClock::new(EventTimestamp::new(1_700_000_000_000)),
        );
        let mut handles: HashMap<Arc<str>, ActorHandle> = HashMap::new();
        handles.insert(Arc::from("BTC"), handle.clone());

        // The baseline band caps at 100_000_000 cents. The sink admits each requote
        // against that SAME band the gateway submit seam uses.
        let sink = ActorCommandSink::new(handles, Arc::new(MicrostructureConfig::default()));
        // A requote's two legs, as the engine emits them: an in-band bid and an
        // out-of-band ask (above the cap).
        sink.enqueue(mm_add_priced(BTC_CALL, "mm-bid", Side::Buy, 50_000));
        sink.enqueue(mm_add_priced(BTC_CALL, "mm-ask", Side::Sell, 200_000_000));

        // Drain the forwarder, then read the journal: only the in-band bid was
        // submitted and journaled; the out-of-band ask was dropped at the sink and
        // never reached the leaf.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            journaled_add_prices(&handle).await,
            vec![50_000],
            "the in-band bid posts; the out-of-band ask is dropped before the leaf"
        );
    }

    /// Determinism: the SAME requote command + config yields the SAME admit/drop
    /// decision on a live run and on a replay — the band is a pure function of
    /// config + price, with no clock/RNG/iteration-order input (#109). Two fresh
    /// actors driven with the identical command stream journal the identical
    /// admitted commands.
    #[tokio::test]
    async fn test_band_drop_is_identical_across_two_runs() {
        async fn run() -> Vec<u64> {
            let lineage = LineageId::new("run-mm-det");
            let (handle, _join) = spawn_underlying_actor(
                ActorConfig::new("BTC", lineage.clone(), 64),
                InMemoryVenueJournal::new(JournalHeader::new(lineage)),
                MatchingExecutor::new("BTC"),
                NoopFanOut,
                FixedClock::new(EventTimestamp::new(1_700_000_000_000)),
            );
            let mut handles: HashMap<Arc<str>, ActorHandle> = HashMap::new();
            handles.insert(Arc::from("BTC"), handle.clone());
            let sink = ActorCommandSink::new(handles, Arc::new(MicrostructureConfig::default()));
            // In-band, out-of-band, in-band — the middle command must drop on both.
            sink.enqueue(mm_add_priced(BTC_CALL, "a", Side::Buy, 50_000));
            sink.enqueue(mm_add_priced(BTC_CALL, "b", Side::Sell, 200_000_000));
            sink.enqueue(mm_add_priced(BTC_CALL, "c", Side::Buy, 60_000));
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            journaled_add_prices(&handle).await
        }

        let live = run().await;
        let replay = run().await;
        assert_eq!(
            live, replay,
            "the band decision is identical live and on replay"
        );
        assert_eq!(
            live,
            vec![50_000, 60_000],
            "both in-band legs post in order; the out-of-band leg is dropped on both runs"
        );
    }
}
