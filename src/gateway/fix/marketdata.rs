//! Market-data messages: `MarketDataRequest (V)`,
//! `MarketDataSnapshotFullRefresh (W)`, `MarketDataIncrementalRefresh (X)`,
//! `MarketDataRequestReject (Y)`
//! ([fix-dialect §2.3](../../../docs/specs/fix-dialect.md#23-market-data-subscription-surfaces-03-54)).
//!
//! `V` carries the required `NoMDEntryTypes (267)` and `NoRelatedSym (146)`
//! groups; `W` is the `orderbook_snapshot` twin (`NoMDEntries (268)`); `X` is the
//! `orderbook_delta` twin with resulting-quantity semantics (`MDEntrySize (271)`
//! of `0` = level removed). `RptSeq (83)` is the per-instrument sequence — the
//! same monotonic sequence the WS surface uses.

use super::FixBody;
use super::codec::{FieldBag, FieldWriter, tags};
use super::enums::{MdEntryType, MdUpdateAction, SubscriptionRequestType};
use super::error::{FixDecodeError, FixEncodeError};
use super::header::StandardHeader;
use super::price::{parse_decimal_to_cents, render_cents_to_decimal};
use crate::exchange::{Cents, SequenceNumber, Symbol};

/// Decodes a required `Price`-typed group field into [`Cents`].
fn decode_group_price(entry: &FieldBag<'_>, tag: u16) -> Result<Cents, FixDecodeError> {
    let raw = entry.req_str(tag)?;
    parse_decimal_to_cents(raw).map_err(|e| FixDecodeError::price(tag, e))
}

/// `MarketDataRequest (V)` — subscribe/unsubscribe to `orderbook`/`trades`/
/// `quotes` for one or more symbols and entry types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataRequest {
    /// Standard header.
    pub header: StandardHeader,
    /// `MDReqID (262)`.
    pub md_req_id: String,
    /// `SubscriptionRequestType (263)`.
    pub subscription_request_type: SubscriptionRequestType,
    /// `MarketDepth (264)`.
    pub market_depth: u32,
    /// `NoMDEntryTypes (267)` + `MDEntryType (269)` — the entry types requested.
    pub entry_types: Vec<MdEntryType>,
    /// `NoRelatedSym (146)` + `Symbol (55)` — the symbols requested.
    pub symbols: Vec<Symbol>,
}

impl FixBody for MarketDataRequest {
    const MSG_TYPE: &'static str = "V";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let entry_type_entries = fields.required_group(
            tags::NO_MD_ENTRY_TYPES,
            tags::MD_ENTRY_TYPE,
            &[tags::MD_ENTRY_TYPE],
        )?;
        let mut entry_types = Vec::with_capacity(entry_type_entries.len());
        for entry in &entry_type_entries {
            entry_types.push(MdEntryType::from_fix(entry.req_str(tags::MD_ENTRY_TYPE)?)?);
        }

        let symbol_entries =
            fields.required_group(tags::NO_RELATED_SYM, tags::SYMBOL, &[tags::SYMBOL])?;
        let mut symbols = Vec::with_capacity(symbol_entries.len());
        for entry in &symbol_entries {
            symbols.push(
                Symbol::parse(entry.req_str(tags::SYMBOL)?)
                    .map_err(|e| FixDecodeError::from_symbol_error(&e))?,
            );
        }

        Ok(Self {
            header,
            md_req_id: fields.req_str(tags::MD_REQ_ID)?.to_string(),
            subscription_request_type: SubscriptionRequestType::from_fix(
                fields.req_str(tags::SUBSCRIPTION_REQUEST_TYPE)?,
            )?,
            market_depth: fields.req_u32(tags::MARKET_DEPTH)?,
            entry_types,
            symbols,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::MD_REQ_ID, &self.md_req_id);
        writer.str(
            tags::SUBSCRIPTION_REQUEST_TYPE,
            self.subscription_request_type.to_fix(),
        );
        writer.u64(tags::MARKET_DEPTH, u64::from(self.market_depth));
        writer.u64(tags::NO_MD_ENTRY_TYPES, self.entry_types.len() as u64);
        for entry_type in &self.entry_types {
            writer.str(tags::MD_ENTRY_TYPE, entry_type.to_fix());
        }
        writer.u64(tags::NO_RELATED_SYM, self.symbols.len() as u64);
        for symbol in &self.symbols {
            writer.str(tags::SYMBOL, symbol.as_str());
        }
        writer.finish()
    }
}

/// One entry of a `MarketDataSnapshotFullRefresh (W)` — a book level or a trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    /// `MDEntryType (269)`.
    pub entry_type: MdEntryType,
    /// `MDEntryPx (270)` — cents.
    pub price: Cents,
    /// `MDEntrySize (271)`.
    pub size: u64,
}

/// `MarketDataSnapshotFullRefresh (W)` — the `orderbook_snapshot` twin. Outbound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataSnapshotFullRefresh {
    /// Standard header.
    pub header: StandardHeader,
    /// `MDReqID (262)`.
    pub md_req_id: String,
    /// `Symbol (55)`.
    pub symbol: Symbol,
    /// `RptSeq (83)` — the per-instrument sequence.
    pub rpt_seq: SequenceNumber,
    /// `NoMDEntries (268)` — the snapshot entries.
    pub entries: Vec<SnapshotEntry>,
}

impl FixBody for MarketDataSnapshotFullRefresh {
    const MSG_TYPE: &'static str = "W";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let entry_bags = fields.group(
            tags::NO_MD_ENTRIES,
            tags::MD_ENTRY_TYPE,
            &[tags::MD_ENTRY_TYPE, tags::MD_ENTRY_PX, tags::MD_ENTRY_SIZE],
        )?;
        let mut entries = Vec::with_capacity(entry_bags.len());
        for entry in &entry_bags {
            entries.push(SnapshotEntry {
                entry_type: MdEntryType::from_fix(entry.req_str(tags::MD_ENTRY_TYPE)?)?,
                price: decode_group_price(entry, tags::MD_ENTRY_PX)?,
                size: entry.req_u64(tags::MD_ENTRY_SIZE)?,
            });
        }
        Ok(Self {
            header,
            md_req_id: fields.req_str(tags::MD_REQ_ID)?.to_string(),
            symbol: Symbol::parse(fields.req_str(tags::SYMBOL)?)
                .map_err(|e| FixDecodeError::from_symbol_error(&e))?,
            rpt_seq: SequenceNumber::new(fields.req_u64(tags::RPT_SEQ)?),
            entries,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::MD_REQ_ID, &self.md_req_id);
        writer.str(tags::SYMBOL, self.symbol.as_str());
        writer.u64(tags::RPT_SEQ, self.rpt_seq.get());
        writer.u64(tags::NO_MD_ENTRIES, self.entries.len() as u64);
        for entry in &self.entries {
            writer.str(tags::MD_ENTRY_TYPE, entry.entry_type.to_fix());
            writer.str(tags::MD_ENTRY_PX, &render_cents_to_decimal(entry.price));
            writer.u64(tags::MD_ENTRY_SIZE, entry.size);
        }
        writer.finish()
    }
}

/// One entry of a `MarketDataIncrementalRefresh (X)` — an ordered book delta with
/// resulting-quantity semantics (`size = 0` means the level was removed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalEntry {
    /// `MDUpdateAction (279)`.
    pub update_action: MdUpdateAction,
    /// `MDEntryType (269)`.
    pub entry_type: MdEntryType,
    /// `Symbol (55)`.
    pub symbol: Symbol,
    /// `MDEntryPx (270)` — cents.
    pub price: Cents,
    /// `MDEntrySize (271)` — the resulting quantity (`0` = level removed).
    pub size: u64,
}

/// `MarketDataIncrementalRefresh (X)` — the `orderbook_delta` twin. Outbound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataIncrementalRefresh {
    /// Standard header.
    pub header: StandardHeader,
    /// `MDReqID (262)`.
    pub md_req_id: String,
    /// `RptSeq (83)` — the per-instrument sequence.
    pub rpt_seq: SequenceNumber,
    /// `NoMDEntries (268)` — the ordered deltas.
    pub entries: Vec<IncrementalEntry>,
}

impl FixBody for MarketDataIncrementalRefresh {
    const MSG_TYPE: &'static str = "X";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let entry_bags = fields.group(
            tags::NO_MD_ENTRIES,
            tags::MD_UPDATE_ACTION,
            &[
                tags::MD_UPDATE_ACTION,
                tags::MD_ENTRY_TYPE,
                tags::SYMBOL,
                tags::MD_ENTRY_PX,
                tags::MD_ENTRY_SIZE,
            ],
        )?;
        let mut entries = Vec::with_capacity(entry_bags.len());
        for entry in &entry_bags {
            entries.push(IncrementalEntry {
                update_action: MdUpdateAction::from_fix(entry.req_str(tags::MD_UPDATE_ACTION)?)?,
                entry_type: MdEntryType::from_fix(entry.req_str(tags::MD_ENTRY_TYPE)?)?,
                symbol: Symbol::parse(entry.req_str(tags::SYMBOL)?)
                    .map_err(|e| FixDecodeError::from_symbol_error(&e))?,
                price: decode_group_price(entry, tags::MD_ENTRY_PX)?,
                size: entry.req_u64(tags::MD_ENTRY_SIZE)?,
            });
        }
        Ok(Self {
            header,
            md_req_id: fields.req_str(tags::MD_REQ_ID)?.to_string(),
            rpt_seq: SequenceNumber::new(fields.req_u64(tags::RPT_SEQ)?),
            entries,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::MD_REQ_ID, &self.md_req_id);
        writer.u64(tags::RPT_SEQ, self.rpt_seq.get());
        writer.u64(tags::NO_MD_ENTRIES, self.entries.len() as u64);
        for entry in &self.entries {
            writer.str(tags::MD_UPDATE_ACTION, entry.update_action.to_fix());
            writer.str(tags::MD_ENTRY_TYPE, entry.entry_type.to_fix());
            writer.str(tags::SYMBOL, entry.symbol.as_str());
            writer.str(tags::MD_ENTRY_PX, &render_cents_to_decimal(entry.price));
            writer.u64(tags::MD_ENTRY_SIZE, entry.size);
        }
        writer.finish()
    }
}

/// `MarketDataRequestReject (Y)` — an unsupported/invalid market-data request.
/// Outbound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataRequestReject {
    /// Standard header.
    pub header: StandardHeader,
    /// `MDReqID (262)`.
    pub md_req_id: String,
    /// `MDReqRejReason (281)`.
    pub md_req_rej_reason: u16,
    /// `Text (58)` — a redacted reason.
    pub text: Option<String>,
}

impl FixBody for MarketDataRequestReject {
    const MSG_TYPE: &'static str = "Y";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        Ok(Self {
            header,
            md_req_id: fields.req_str(tags::MD_REQ_ID)?.to_string(),
            md_req_rej_reason: fields.req_u16(tags::MD_REQ_REJ_REASON)?,
            text: fields.opt_str(tags::TEXT)?.map(str::to_string),
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::MD_REQ_ID, &self.md_req_id);
        writer.u16(tags::MD_REQ_REJ_REASON, self.md_req_rej_reason);
        writer.opt_str(tags::TEXT, self.text.as_deref());
        writer.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::header::UtcTimestamp;
    use super::super::{DecodedMessage, decode};
    use super::*;
    use ironfix_core::types::{CompId, SeqNum};

    fn header() -> StandardHeader {
        StandardHeader::new(
            CompId::new("CLIENT").expect("comp id"),
            CompId::new("FAUXCHANGE").expect("comp id"),
            SeqNum::new(4),
            UtcTimestamp::parse(52, "20240329-12:00:00.000").expect("ts"),
        )
    }

    fn sym() -> Symbol {
        Symbol::parse("BTC-20240329-50000-C").expect("symbol")
    }

    #[test]
    fn test_market_data_request_round_trips_both_groups() {
        let request = MarketDataRequest {
            header: header(),
            md_req_id: "MDR-1".to_string(),
            subscription_request_type: SubscriptionRequestType::SnapshotPlusUpdates,
            market_depth: 0,
            entry_types: vec![MdEntryType::Bid, MdEntryType::Offer, MdEntryType::Trade],
            symbols: vec![sym()],
        };
        let bytes = request.encode().expect("test encode");
        match decode(&bytes) {
            Ok(DecodedMessage::MarketDataRequest(back)) => assert_eq!(back, request),
            other => panic!("expected MarketDataRequest, got {other:?}"),
        }
    }

    #[test]
    fn test_market_data_request_empty_entry_types_group_is_rejected() {
        // NoMDEntryTypes=0 is a required-non-empty group.
        let mut writer = FieldWriter::new(MarketDataRequest::MSG_TYPE);
        header().encode(&mut writer);
        writer.str(tags::MD_REQ_ID, "MDR-1");
        writer.str(tags::SUBSCRIPTION_REQUEST_TYPE, "1");
        writer.u64(tags::MARKET_DEPTH, 0);
        writer.u64(tags::NO_MD_ENTRY_TYPES, 0);
        writer.u64(tags::NO_RELATED_SYM, 1);
        writer.str(tags::SYMBOL, "BTC-20240329-50000-C");
        let bytes = writer.finish().expect("test finish");
        match decode(&bytes) {
            Err(FixDecodeError::EmptyGroup { count_tag }) => {
                assert_eq!(count_tag, tags::NO_MD_ENTRY_TYPES);
            }
            other => panic!("expected EmptyGroup(267), got {other:?}"),
        }
    }

    #[test]
    fn test_market_data_request_group_count_mismatch_is_rejected() {
        // Declare 3 entry types but supply 2.
        let mut writer = FieldWriter::new(MarketDataRequest::MSG_TYPE);
        header().encode(&mut writer);
        writer.str(tags::MD_REQ_ID, "MDR-1");
        writer.str(tags::SUBSCRIPTION_REQUEST_TYPE, "1");
        writer.u64(tags::MARKET_DEPTH, 0);
        writer.u64(tags::NO_MD_ENTRY_TYPES, 3);
        writer.str(tags::MD_ENTRY_TYPE, "0");
        writer.str(tags::MD_ENTRY_TYPE, "1");
        writer.u64(tags::NO_RELATED_SYM, 1);
        writer.str(tags::SYMBOL, "BTC-20240329-50000-C");
        let bytes = writer.finish().expect("test finish");
        match decode(&bytes) {
            Err(FixDecodeError::GroupCountMismatch {
                count_tag,
                declared,
                decoded,
            }) => {
                assert_eq!(count_tag, tags::NO_MD_ENTRY_TYPES);
                assert_eq!(declared, 3);
                assert_eq!(decoded, 2);
            }
            other => panic!("expected GroupCountMismatch(267), got {other:?}"),
        }
    }

    #[test]
    fn test_snapshot_round_trips_with_book_levels() {
        let snapshot = MarketDataSnapshotFullRefresh {
            header: header(),
            md_req_id: "MDR-1".to_string(),
            symbol: sym(),
            rpt_seq: SequenceNumber::new(42),
            entries: vec![
                SnapshotEntry {
                    entry_type: MdEntryType::Bid,
                    price: Cents::new(49995),
                    size: 10,
                },
                SnapshotEntry {
                    entry_type: MdEntryType::Offer,
                    price: Cents::new(50005),
                    size: 7,
                },
            ],
        };
        let bytes = snapshot.encode().expect("test encode");
        match decode(&bytes) {
            Ok(DecodedMessage::MarketDataSnapshotFullRefresh(back)) => assert_eq!(back, snapshot),
            other => panic!("expected snapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_snapshot_empty_book_round_trips() {
        let snapshot = MarketDataSnapshotFullRefresh {
            header: header(),
            md_req_id: "MDR-1".to_string(),
            symbol: sym(),
            rpt_seq: SequenceNumber::new(1),
            entries: Vec::new(),
        };
        let bytes = snapshot.encode().expect("test encode");
        match decode(&bytes) {
            Ok(DecodedMessage::MarketDataSnapshotFullRefresh(back)) => assert_eq!(back, snapshot),
            other => panic!("expected snapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_incremental_refresh_round_trips_with_delete_semantics() {
        let refresh = MarketDataIncrementalRefresh {
            header: header(),
            md_req_id: "MDR-1".to_string(),
            rpt_seq: SequenceNumber::new(43),
            entries: vec![
                IncrementalEntry {
                    update_action: MdUpdateAction::Change,
                    entry_type: MdEntryType::Bid,
                    symbol: sym(),
                    price: Cents::new(49995),
                    size: 4,
                },
                IncrementalEntry {
                    // A delete carries the resulting quantity 0.
                    update_action: MdUpdateAction::Delete,
                    entry_type: MdEntryType::Offer,
                    symbol: sym(),
                    price: Cents::new(50005),
                    size: 0,
                },
            ],
        };
        let bytes = refresh.encode().expect("test encode");
        match decode(&bytes) {
            Ok(DecodedMessage::MarketDataIncrementalRefresh(back)) => assert_eq!(back, refresh),
            other => panic!("expected incremental refresh, got {other:?}"),
        }
    }

    #[test]
    fn test_market_data_request_reject_round_trips() {
        let reject = MarketDataRequestReject {
            header: header(),
            md_req_id: "MDR-1".to_string(),
            md_req_rej_reason: 0,
            text: Some("unsupported subscription".to_string()),
        };
        let bytes = reject.encode().expect("test encode");
        match decode(&bytes) {
            Ok(DecodedMessage::MarketDataRequestReject(back)) => assert_eq!(back, reject),
            other => panic!("expected reject, got {other:?}"),
        }
    }
}
