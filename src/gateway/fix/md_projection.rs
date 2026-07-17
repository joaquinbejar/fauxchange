//! Projecting the WebSocket market-data messages onto the FIX market-data
//! surface (#040): `MarketDataSnapshotFullRefresh (W)` is the `orderbook_snapshot`
//! twin and `MarketDataIncrementalRefresh (X)` is the `orderbook_delta` twin.
//!
//! ## Observation parity by construction
//!
//! The projection is a **pure** function of the same [`WsMessage`] the WS surface
//! already produces: a `W` carries the snapshot's `instrument_sequence` as
//! `RptSeq (83)`, and an `X` carries the delta's `instrument_sequence` as
//! `RptSeq (83)` with the identical resulting-quantity semantics (`quantity == 0`
//! = level removed). Because both surfaces read the **same** producer, the FIX
//! `RptSeq` equals the WS `sequence` for the same book by construction — there is
//! no parallel market-data path to drift ([03 §5.4](../../../docs/03-protocol-surfaces.md#54-market-data),
//! [fix-dialect §2.3](../../../docs/specs/fix-dialect.md#23-market-data-subscription-surfaces-03-54)).
//!
//! ## Sequence namespaces stay distinct
//!
//! `RptSeq (83)` is the per-instrument market-data sequence; the session
//! `MsgSeqNum (34)` the emitted frames also carry is a **separate** namespace. A
//! `RptSeq` gap recovers by a fresh `MarketDataRequest (V)` (a new snapshot),
//! never by `ResendRequest (2)` — this module never conflates the two.

use super::enums::{MdEntryType, MdUpdateAction};
use super::marketdata::{IncrementalEntry, SnapshotEntry};
use crate::models::{BookSide, PriceLevelChange, PriceLevelData, WsMessage};

/// Which book sides a FIX market-data subscription asked for, derived from the
/// `MDEntryType (269)` group (`0` = Bid, `1` = Offer). A `2` = Trade entry type is
/// admitted at decode but selects neither book side (the trade-tape projection over
/// FIX MD is deferred — a `V` that asks for **no** book side is rejected with `Y`,
/// not silently served).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestedSides {
    /// The request asked for the bid side (`MDEntryType = 0`).
    pub bids: bool,
    /// The request asked for the offer side (`MDEntryType = 1`).
    pub asks: bool,
}

impl RequestedSides {
    /// Resolves the requested book sides from a decoded `MDEntryType (269)` group.
    /// A `Trade` entry type is exhaustively matched and contributes no book side.
    #[must_use]
    pub fn from_entry_types(entry_types: &[MdEntryType]) -> Self {
        let mut sides = Self {
            bids: false,
            asks: false,
        };
        for entry_type in entry_types {
            match entry_type {
                MdEntryType::Bid => sides.bids = true,
                MdEntryType::Offer => sides.asks = true,
                MdEntryType::Trade => {}
            }
        }
        sides
    }

    /// Whether at least one book side was requested (a `V` with none is rejected).
    #[must_use]
    pub const fn any(self) -> bool {
        self.bids || self.asks
    }

    /// Whether `side`'s levels belong in the projection.
    #[must_use]
    const fn includes(self, side: BookSide) -> bool {
        match side {
            BookSide::Bid => self.bids,
            BookSide::Ask => self.asks,
        }
    }
}

/// The `MDEntryType (269)` for a book side: a bid is `0`, an ask is an offer `1`.
#[must_use]
const fn entry_type_for(side: BookSide) -> MdEntryType {
    match side {
        BookSide::Bid => MdEntryType::Bid,
        BookSide::Ask => MdEntryType::Offer,
    }
}

/// Projects the levels of a WS `orderbook_snapshot` into the `NoMDEntries (268)`
/// group of a FIX `W`, filtered to the requested sides. Bid levels render as
/// `MDEntryType = 0`, ask levels as `MDEntryType = 1`; `MDEntryPx (270)` is the
/// level price in cents (rendered decimal at the wire seam) and `MDEntrySize (271)`
/// the resting quantity.
#[must_use]
pub fn snapshot_entries(
    bids: &[PriceLevelData],
    asks: &[PriceLevelData],
    sides: RequestedSides,
) -> Vec<SnapshotEntry> {
    let mut entries = Vec::new();
    if sides.bids {
        for level in bids {
            entries.push(SnapshotEntry {
                entry_type: MdEntryType::Bid,
                price: level.price,
                size: level.quantity,
            });
        }
    }
    if sides.asks {
        for level in asks {
            entries.push(SnapshotEntry {
                entry_type: MdEntryType::Offer,
                price: level.price,
                size: level.quantity,
            });
        }
    }
    entries
}

/// Projects the changes of a WS `orderbook_delta` into the `NoMDEntries (268)`
/// group of a FIX `X`, filtered to the requested sides. Resulting-quantity
/// semantics carry over unchanged: a change to quantity `0` is `MDUpdateAction =
/// Delete` (the level was removed), any other resulting quantity is `Change`. The
/// WS surface does not distinguish a brand-new level from a resized one — its
/// deltas are pure resulting-quantity — so `Change` is the faithful, parity-
/// preserving action for every non-zero level.
#[must_use]
pub fn incremental_entries(
    symbol: &crate::exchange::Symbol,
    changes: &[PriceLevelChange],
    sides: RequestedSides,
) -> Vec<IncrementalEntry> {
    let mut entries = Vec::new();
    for change in changes {
        if !sides.includes(change.side) {
            continue;
        }
        let update_action = if change.quantity == 0 {
            MdUpdateAction::Delete
        } else {
            MdUpdateAction::Change
        };
        entries.push(IncrementalEntry {
            update_action,
            entry_type: entry_type_for(change.side),
            symbol: symbol.clone(),
            price: change.price,
            size: change.quantity,
        });
    }
    entries
}

/// The `(instrument_sequence, entries)` of a WS `orderbook_snapshot`, or `None`
/// for any other message — the parity seam a test drives on the exact
/// [`WsMessage`] the manager produced.
#[must_use]
pub fn snapshot_projection(
    ws: &WsMessage,
    sides: RequestedSides,
) -> Option<(u64, Vec<SnapshotEntry>)> {
    match ws {
        WsMessage::OrderbookSnapshot {
            sequence,
            bids,
            asks,
            ..
        } => Some((*sequence, snapshot_entries(bids, asks, sides))),
        _ => None,
    }
}

/// The `(instrument_sequence, entries)` of a WS `orderbook_delta`, or `None` for
/// any other message — the parity seam a test drives on the exact [`WsMessage`]
/// the manager produced. Proves `RptSeq (83)` equals the WS `sequence` and the
/// resulting quantities match.
#[must_use]
pub fn incremental_projection(
    ws: &WsMessage,
    sides: RequestedSides,
) -> Option<(u64, Vec<IncrementalEntry>)> {
    match ws {
        WsMessage::OrderbookDelta {
            symbol,
            sequence,
            changes,
        } => Some((*sequence, incremental_entries(symbol, changes, sides))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{Cents, Symbol};

    const ALL: RequestedSides = RequestedSides {
        bids: true,
        asks: true,
    };

    fn sym() -> Symbol {
        Symbol::parse("BTC-20240329-50000-C").expect("symbol")
    }

    #[test]
    fn test_requested_sides_from_entry_types_maps_bid_and_offer_ignoring_trade() {
        let sides = RequestedSides::from_entry_types(&[
            MdEntryType::Bid,
            MdEntryType::Offer,
            MdEntryType::Trade,
        ]);
        assert!(sides.bids);
        assert!(sides.asks);
        assert!(sides.any());
    }

    #[test]
    fn test_requested_sides_trade_only_selects_no_book_side() {
        let sides = RequestedSides::from_entry_types(&[MdEntryType::Trade]);
        assert!(!sides.bids);
        assert!(!sides.asks);
        assert!(!sides.any());
    }

    #[test]
    fn test_snapshot_entries_render_bids_as_bid_and_asks_as_offer() {
        let bids = vec![PriceLevelData {
            price: Cents::new(49_995),
            quantity: 10,
        }];
        let asks = vec![PriceLevelData {
            price: Cents::new(50_005),
            quantity: 7,
        }];
        let entries = snapshot_entries(&bids, &asks, ALL);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_type, MdEntryType::Bid);
        assert_eq!(entries[0].price, Cents::new(49_995));
        assert_eq!(entries[0].size, 10);
        assert_eq!(entries[1].entry_type, MdEntryType::Offer);
        assert_eq!(entries[1].price, Cents::new(50_005));
        assert_eq!(entries[1].size, 7);
    }

    #[test]
    fn test_snapshot_entries_filter_to_requested_side() {
        let bids = vec![PriceLevelData {
            price: Cents::new(49_995),
            quantity: 10,
        }];
        let asks = vec![PriceLevelData {
            price: Cents::new(50_005),
            quantity: 7,
        }];
        let bids_only = snapshot_entries(
            &bids,
            &asks,
            RequestedSides {
                bids: true,
                asks: false,
            },
        );
        assert_eq!(bids_only.len(), 1);
        assert_eq!(bids_only[0].entry_type, MdEntryType::Bid);
    }

    #[test]
    fn test_incremental_entries_zero_quantity_is_delete_else_change() {
        let changes = vec![
            PriceLevelChange {
                side: BookSide::Bid,
                price: Cents::new(49_995),
                quantity: 4,
            },
            PriceLevelChange {
                side: BookSide::Ask,
                price: Cents::new(50_005),
                quantity: 0,
            },
        ];
        let entries = incremental_entries(&sym(), &changes, ALL);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].update_action, MdUpdateAction::Change);
        assert_eq!(entries[0].entry_type, MdEntryType::Bid);
        assert_eq!(entries[0].size, 4);
        assert_eq!(entries[1].update_action, MdUpdateAction::Delete);
        assert_eq!(entries[1].entry_type, MdEntryType::Offer);
        assert_eq!(entries[1].size, 0);
    }

    #[test]
    fn test_incremental_projection_carries_the_ws_sequence_as_rpt_seq() {
        let ws = WsMessage::OrderbookDelta {
            symbol: sym(),
            sequence: 43,
            changes: vec![PriceLevelChange {
                side: BookSide::Bid,
                price: Cents::new(49_995),
                quantity: 4,
            }],
        };
        let (sequence, entries) = incremental_projection(&ws, ALL).expect("delta projects");
        assert_eq!(sequence, 43, "RptSeq must equal the WS instrument_sequence");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].size, 4);
    }

    #[test]
    fn test_snapshot_projection_none_for_non_snapshot() {
        let ws = WsMessage::OrderbookDelta {
            symbol: sym(),
            sequence: 1,
            changes: Vec::new(),
        };
        assert!(snapshot_projection(&ws, ALL).is_none());
    }
}
