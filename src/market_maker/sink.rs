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

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::exchange::{ActorHandle, SymbolParser, VenueCommand};

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
    /// its forwarder task. Must be called within a `tokio` runtime.
    #[must_use]
    pub fn new(handles: HashMap<Arc<str>, ActorHandle>) -> Arc<Self> {
        Self::with_capacity(handles, DEFAULT_SINK_CAPACITY)
    }

    /// Builds the sink with an explicit bounded forwarder capacity.
    #[must_use]
    pub fn with_capacity(handles: HashMap<Arc<str>, ActorHandle>, capacity: usize) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let forwarder = Forwarder { handles, rx };
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
        Cents, EventTimestamp, MARKET_MAKER_OWNER, STPMode, Side, Symbol, TimeInForce,
        market_maker_account,
    };
    use crate::models::{OrderType, VenueOrderId};

    fn sym(raw: &str) -> Symbol {
        Symbol::parse(raw).expect("valid fixture symbol")
    }

    fn mm_add(symbol: &str) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym(symbol),
            order_id: VenueOrderId::new("mm-1"),
            account: market_maker_account(),
            owner: MARKET_MAKER_OWNER,
            client_order_id: None,
            side: Side::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(100)),
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
}
