//! Order-entry messages: `NewOrderSingle (D)`, `OrderCancelRequest (F)`,
//! `OrderCancelReplaceRequest (G)`, `OrderMassCancelRequest (q)`,
//! `OrderStatusRequest (H)`
//! ([fix-dialect §2.2](../../../docs/specs/fix-dialect.md#22-order-entry-and-execution)).
//!
//! Conditional requiredness is enforced as a typed error, never a silent
//! default: `Price (44)` must be present for a `Limit` order, `ExpireTime (126)`
//! for a `GTD` order, `Symbol (55)` for a per-security mass cancel, and an
//! `OrderStatusRequest` needs one of `OrderID (37)` / `ClOrdID (11)`. Prices
//! decode scale-only into [`Cents`]; the off-tick tick check runs once the
//! instrument is resolved (#039).

use super::FixBody;
use super::codec::{FieldBag, FieldWriter, tags};
use super::enums::{MassCancelRequestType, OrdType, OrderSide, TimeInForce};
use super::error::{FixDecodeError, FixEncodeError};
use super::header::{StandardHeader, UtcTimestamp};
use super::price::{parse_decimal_to_cents, render_cents_to_decimal};
use crate::exchange::{Cents, Symbol};
use crate::models::{AccountId, ClientOrderId, VenueOrderId};

/// Decodes a `Symbol (55)` value into the canonical [`Symbol`] type.
fn decode_symbol(value: &str) -> Result<Symbol, FixDecodeError> {
    Symbol::parse(value).map_err(|e| FixDecodeError::from_symbol_error(&e))
}

/// Decodes an optional `Price`-typed tag scale-only into [`Cents`].
fn decode_optional_price(fields: &FieldBag<'_>, tag: u16) -> Result<Option<Cents>, FixDecodeError> {
    match fields.opt_str(tag)? {
        Some(raw) => parse_decimal_to_cents(raw)
            .map(Some)
            .map_err(|e| FixDecodeError::price(tag, e)),
        None => Ok(None),
    }
}

/// `NewOrderSingle (D)` — a new order. `Price (44)` is required for a `Limit`
/// order; `ExpireTime (126)` for a `GTD` order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOrderSingle {
    /// Standard header.
    pub header: StandardHeader,
    /// `ClOrdID (11)` — the account-scoped idempotency key.
    pub cl_ord_id: ClientOrderId,
    /// `Account (1)` — absent, or equal to the authenticated account (the
    /// binding check is the session layer's, [ADR-0010](../../../docs/adr/0010-fix-session-account-binding.md)).
    pub account: Option<AccountId>,
    /// `Symbol (55)` — the canonical contract symbol.
    pub symbol: Symbol,
    /// `Side (54)`.
    pub side: OrderSide,
    /// `TransactTime (60)`.
    pub transact_time: UtcTimestamp,
    /// `OrdType (40)`.
    pub ord_type: OrdType,
    /// `Price (44)` — cents (required for `Limit`).
    pub price: Option<Cents>,
    /// `OrderQty (38)` — integer contract count.
    pub order_qty: u64,
    /// `TimeInForce (59)` — defaults to GTC when absent.
    pub time_in_force: TimeInForce,
    /// `ExpireTime (126)` — required for `GTD`.
    pub expire_time: Option<UtcTimestamp>,
}

impl FixBody for NewOrderSingle {
    const MSG_TYPE: &'static str = "D";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let ord_type = OrdType::from_fix(fields.req_str(tags::ORD_TYPE)?)?;
        let price = decode_optional_price(fields, tags::PRICE)?;
        if ord_type == OrdType::Limit && price.is_none() {
            return Err(FixDecodeError::MissingConditionalField {
                tag: tags::PRICE,
                condition: "OrdType=2 (Limit)",
            });
        }

        let time_in_force = TimeInForce::from_fix_or_default(fields.opt_str(tags::TIME_IN_FORCE)?)?;
        let expire_time = match fields.opt_str(tags::EXPIRE_TIME)? {
            Some(raw) => Some(UtcTimestamp::parse(tags::EXPIRE_TIME, raw)?),
            None => None,
        };
        if time_in_force == TimeInForce::Gtd && expire_time.is_none() {
            return Err(FixDecodeError::MissingConditionalField {
                tag: tags::EXPIRE_TIME,
                condition: "TimeInForce=6 (GTD)",
            });
        }

        Ok(Self {
            header,
            cl_ord_id: ClientOrderId::new(fields.req_str(tags::CL_ORD_ID)?),
            account: fields.opt_str(tags::ACCOUNT)?.map(AccountId::new),
            symbol: decode_symbol(fields.req_str(tags::SYMBOL)?)?,
            side: OrderSide::from_fix(fields.req_str(tags::SIDE)?)?,
            transact_time: UtcTimestamp::parse(
                tags::TRANSACT_TIME,
                fields.req_str(tags::TRANSACT_TIME)?,
            )?,
            ord_type,
            price,
            order_qty: fields.req_u64(tags::ORDER_QTY)?,
            time_in_force,
            expire_time,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::CL_ORD_ID, self.cl_ord_id.as_str());
        writer.opt_str(tags::ACCOUNT, self.account.as_ref().map(AccountId::as_str));
        writer.str(tags::SYMBOL, self.symbol.as_str());
        writer.str(tags::SIDE, self.side.to_fix());
        writer.str(tags::TRANSACT_TIME, self.transact_time.as_str());
        writer.str(tags::ORD_TYPE, self.ord_type.to_fix());
        if let Some(price) = self.price {
            writer.str(tags::PRICE, &render_cents_to_decimal(price));
        }
        writer.u64(tags::ORDER_QTY, self.order_qty);
        writer.str(tags::TIME_IN_FORCE, self.time_in_force.to_fix());
        writer.opt_str(
            tags::EXPIRE_TIME,
            self.expire_time.as_ref().map(UtcTimestamp::as_str),
        );
        writer.finish()
    }
}

/// `OrderCancelRequest (F)` — cancel a resting order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCancelRequest {
    /// Standard header.
    pub header: StandardHeader,
    /// `OrigClOrdID (41)` — the order being cancelled.
    pub orig_cl_ord_id: ClientOrderId,
    /// `ClOrdID (11)` — the cancel request's own id.
    pub cl_ord_id: ClientOrderId,
    /// `Symbol (55)`.
    pub symbol: Symbol,
    /// `Side (54)`.
    pub side: OrderSide,
}

impl FixBody for OrderCancelRequest {
    const MSG_TYPE: &'static str = "F";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        Ok(Self {
            header,
            orig_cl_ord_id: ClientOrderId::new(fields.req_str(tags::ORIG_CL_ORD_ID)?),
            cl_ord_id: ClientOrderId::new(fields.req_str(tags::CL_ORD_ID)?),
            symbol: decode_symbol(fields.req_str(tags::SYMBOL)?)?,
            side: OrderSide::from_fix(fields.req_str(tags::SIDE)?)?,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::ORIG_CL_ORD_ID, self.orig_cl_ord_id.as_str());
        writer.str(tags::CL_ORD_ID, self.cl_ord_id.as_str());
        writer.str(tags::SYMBOL, self.symbol.as_str());
        writer.str(tags::SIDE, self.side.to_fix());
        writer.finish()
    }
}

/// `OrderCancelReplaceRequest (G)` — non-atomic replace ([ADR-0006](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
/// `Price (44)` is required for a `Limit` replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCancelReplaceRequest {
    /// Standard header.
    pub header: StandardHeader,
    /// `OrigClOrdID (41)` — the order being replaced.
    pub orig_cl_ord_id: ClientOrderId,
    /// `ClOrdID (11)` — the replacement's own id.
    pub cl_ord_id: ClientOrderId,
    /// `Symbol (55)`.
    pub symbol: Symbol,
    /// `Side (54)`.
    pub side: OrderSide,
    /// `OrdType (40)`.
    pub ord_type: OrdType,
    /// `Price (44)` — cents (required for `Limit`).
    pub price: Option<Cents>,
    /// `OrderQty (38)`.
    pub order_qty: u64,
}

impl FixBody for OrderCancelReplaceRequest {
    const MSG_TYPE: &'static str = "G";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let ord_type = OrdType::from_fix(fields.req_str(tags::ORD_TYPE)?)?;
        let price = decode_optional_price(fields, tags::PRICE)?;
        if ord_type == OrdType::Limit && price.is_none() {
            return Err(FixDecodeError::MissingConditionalField {
                tag: tags::PRICE,
                condition: "OrdType=2 (Limit)",
            });
        }
        Ok(Self {
            header,
            orig_cl_ord_id: ClientOrderId::new(fields.req_str(tags::ORIG_CL_ORD_ID)?),
            cl_ord_id: ClientOrderId::new(fields.req_str(tags::CL_ORD_ID)?),
            symbol: decode_symbol(fields.req_str(tags::SYMBOL)?)?,
            side: OrderSide::from_fix(fields.req_str(tags::SIDE)?)?,
            ord_type,
            price,
            order_qty: fields.req_u64(tags::ORDER_QTY)?,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::ORIG_CL_ORD_ID, self.orig_cl_ord_id.as_str());
        writer.str(tags::CL_ORD_ID, self.cl_ord_id.as_str());
        writer.str(tags::SYMBOL, self.symbol.as_str());
        writer.str(tags::SIDE, self.side.to_fix());
        writer.str(tags::ORD_TYPE, self.ord_type.to_fix());
        if let Some(price) = self.price {
            writer.str(tags::PRICE, &render_cents_to_decimal(price));
        }
        writer.u64(tags::ORDER_QTY, self.order_qty);
        writer.finish()
    }
}

/// `OrderMassCancelRequest (q)` — cancel a scope of orders. `Symbol (55)` is
/// required when the scope is per-security.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderMassCancelRequest {
    /// Standard header.
    pub header: StandardHeader,
    /// `ClOrdID (11)`.
    pub cl_ord_id: ClientOrderId,
    /// `MassCancelRequestType (530)`.
    pub mass_cancel_request_type: MassCancelRequestType,
    /// `Symbol (55)` — required for a per-security scope.
    pub symbol: Option<Symbol>,
}

impl FixBody for OrderMassCancelRequest {
    const MSG_TYPE: &'static str = "q";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let mass_cancel_request_type =
            MassCancelRequestType::from_fix(fields.req_str(tags::MASS_CANCEL_REQUEST_TYPE)?)?;
        let symbol = match fields.opt_str(tags::SYMBOL)? {
            Some(raw) => Some(decode_symbol(raw)?),
            None => None,
        };
        if mass_cancel_request_type == MassCancelRequestType::Security && symbol.is_none() {
            return Err(FixDecodeError::MissingConditionalField {
                tag: tags::SYMBOL,
                condition: "MassCancelRequestType=1 (per security)",
            });
        }
        Ok(Self {
            header,
            cl_ord_id: ClientOrderId::new(fields.req_str(tags::CL_ORD_ID)?),
            mass_cancel_request_type,
            symbol,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::CL_ORD_ID, self.cl_ord_id.as_str());
        writer.str(
            tags::MASS_CANCEL_REQUEST_TYPE,
            self.mass_cancel_request_type.to_fix(),
        );
        writer.opt_str(tags::SYMBOL, self.symbol.as_ref().map(Symbol::as_str));
        writer.finish()
    }
}

/// `OrderStatusRequest (H)` — query an order's status. One of `OrderID (37)` /
/// `ClOrdID (11)` is required.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderStatusRequest {
    /// Standard header.
    pub header: StandardHeader,
    /// `OrderID (37)` — the venue order id.
    pub order_id: Option<VenueOrderId>,
    /// `ClOrdID (11)` — the client order id.
    pub cl_ord_id: Option<ClientOrderId>,
    /// `Symbol (55)`.
    pub symbol: Symbol,
}

impl FixBody for OrderStatusRequest {
    const MSG_TYPE: &'static str = "H";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let order_id = fields.opt_str(tags::ORDER_ID)?.map(VenueOrderId::new);
        let cl_ord_id = fields.opt_str(tags::CL_ORD_ID)?.map(ClientOrderId::new);
        if order_id.is_none() && cl_ord_id.is_none() {
            return Err(FixDecodeError::MissingRequiredChoice {
                first: tags::ORDER_ID,
                second: tags::CL_ORD_ID,
            });
        }
        Ok(Self {
            header,
            order_id,
            cl_ord_id,
            symbol: decode_symbol(fields.req_str(tags::SYMBOL)?)?,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.opt_str(
            tags::ORDER_ID,
            self.order_id.as_ref().map(VenueOrderId::as_str),
        );
        writer.opt_str(
            tags::CL_ORD_ID,
            self.cl_ord_id.as_ref().map(ClientOrderId::as_str),
        );
        writer.str(tags::SYMBOL, self.symbol.as_str());
        writer.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{DecodedMessage, decode};
    use super::*;
    use ironfix_core::types::{CompId, SeqNum};

    fn header() -> StandardHeader {
        StandardHeader::new(
            CompId::new("CLIENT").expect("comp id"),
            CompId::new("FAUXCHANGE").expect("comp id"),
            SeqNum::new(2),
            UtcTimestamp::parse(52, "20240329-12:00:00.000").expect("ts"),
        )
    }

    fn sym() -> Symbol {
        Symbol::parse("BTC-20240329-50000-C").expect("symbol")
    }

    fn limit_order() -> NewOrderSingle {
        NewOrderSingle {
            header: header(),
            cl_ord_id: ClientOrderId::new("CLIENT-1"),
            account: Some(AccountId::new("acct-1")),
            symbol: sym(),
            side: OrderSide::Buy,
            transact_time: UtcTimestamp::parse(60, "20240329-12:00:00.000").expect("ts"),
            ord_type: OrdType::Limit,
            price: Some(Cents::new(50005)),
            order_qty: 3,
            time_in_force: TimeInForce::Gtc,
            expire_time: None,
        }
    }

    #[test]
    fn test_new_order_single_limit_round_trips() {
        let order = limit_order();
        let bytes = order.encode().expect("test encode");
        match decode(&bytes) {
            Ok(DecodedMessage::NewOrderSingle(back)) => assert_eq!(back, order),
            other => panic!("expected NewOrderSingle, got {other:?}"),
        }
    }

    #[test]
    fn test_new_order_single_price_is_the_seam_cents() {
        // The decimal 44=500.05 becomes exactly 50005 cents — REST/FIX parity.
        let order = limit_order();
        let bytes = order.encode().expect("test encode");
        let wire = String::from_utf8(bytes.clone()).expect("utf8");
        assert!(wire.contains("\u{1}44=500.05\u{1}"), "wire: {wire}");
        match decode(&bytes) {
            Ok(DecodedMessage::NewOrderSingle(back)) => {
                assert_eq!(back.price, Some(Cents::new(50005)));
            }
            other => panic!("expected NewOrderSingle, got {other:?}"),
        }
    }

    #[test]
    fn test_new_order_single_limit_without_price_is_typed_error() {
        let mut order = limit_order();
        order.price = None;
        let bytes = order.encode().expect("test encode");
        match decode(&bytes) {
            Err(FixDecodeError::MissingConditionalField { tag, .. }) => {
                assert_eq!(tag, tags::PRICE);
            }
            other => panic!("expected MissingConditionalField(44), got {other:?}"),
        }
    }

    #[test]
    fn test_new_order_single_gtd_without_expire_time_is_typed_error() {
        let mut order = limit_order();
        order.time_in_force = TimeInForce::Gtd;
        order.expire_time = None;
        let bytes = order.encode().expect("test encode");
        match decode(&bytes) {
            Err(FixDecodeError::MissingConditionalField { tag, .. }) => {
                assert_eq!(tag, tags::EXPIRE_TIME);
            }
            other => panic!("expected MissingConditionalField(126), got {other:?}"),
        }
    }

    #[test]
    fn test_new_order_single_market_order_needs_no_price() {
        let order = NewOrderSingle {
            ord_type: OrdType::Market,
            price: None,
            ..limit_order()
        };
        let bytes = order.encode().expect("test encode");
        match decode(&bytes) {
            Ok(DecodedMessage::NewOrderSingle(back)) => assert_eq!(back, order),
            other => panic!("expected NewOrderSingle, got {other:?}"),
        }
    }

    #[test]
    fn test_new_order_single_unknown_side_is_typed_reject() {
        let mut writer = FieldWriter::new(NewOrderSingle::MSG_TYPE);
        header().encode(&mut writer);
        writer.str(tags::CL_ORD_ID, "CLIENT-1");
        writer.str(tags::SYMBOL, "BTC-20240329-50000-C");
        writer.str(tags::SIDE, "9"); // not admitted
        writer.str(tags::TRANSACT_TIME, "20240329-12:00:00.000");
        writer.str(tags::ORD_TYPE, "2");
        writer.str(tags::PRICE, "500.05");
        writer.u64(tags::ORDER_QTY, 3);
        let bytes = writer.finish().expect("test finish");
        match decode(&bytes) {
            Err(FixDecodeError::ValueIsIncorrect { tag, value }) => {
                assert_eq!(tag, tags::SIDE);
                assert_eq!(value, "9");
            }
            other => panic!("expected ValueIsIncorrect(54), got {other:?}"),
        }
    }

    #[test]
    fn test_new_order_single_malformed_symbol_is_typed_reject() {
        let mut writer = FieldWriter::new(NewOrderSingle::MSG_TYPE);
        header().encode(&mut writer);
        writer.str(tags::CL_ORD_ID, "CLIENT-1");
        writer.str(tags::SYMBOL, "NOT-A-SYMBOL");
        writer.str(tags::SIDE, "1");
        writer.str(tags::TRANSACT_TIME, "20240329-12:00:00.000");
        writer.str(tags::ORD_TYPE, "2");
        writer.str(tags::PRICE, "500.05");
        writer.u64(tags::ORDER_QTY, 3);
        let bytes = writer.finish().expect("test finish");
        assert!(matches!(
            decode(&bytes),
            Err(FixDecodeError::MalformedSymbol { .. })
        ));
    }

    #[test]
    fn test_new_order_single_off_scale_price_is_typed_reject() {
        let mut writer = FieldWriter::new(NewOrderSingle::MSG_TYPE);
        header().encode(&mut writer);
        writer.str(tags::CL_ORD_ID, "CLIENT-1");
        writer.str(tags::SYMBOL, "BTC-20240329-50000-C");
        writer.str(tags::SIDE, "1");
        writer.str(tags::TRANSACT_TIME, "20240329-12:00:00.000");
        writer.str(tags::ORD_TYPE, "2");
        writer.str(tags::PRICE, "500.055"); // sub-cent
        writer.u64(tags::ORDER_QTY, 3);
        let bytes = writer.finish().expect("test finish");
        match decode(&bytes) {
            Err(FixDecodeError::Price { tag, .. }) => assert_eq!(tag, tags::PRICE),
            other => panic!("expected Price error on tag 44, got {other:?}"),
        }
    }

    #[test]
    fn test_cancel_and_replace_and_mass_and_status_round_trip() {
        let msgs = [
            DecodedMessage::OrderCancelRequest(OrderCancelRequest {
                header: header(),
                orig_cl_ord_id: ClientOrderId::new("CLIENT-1"),
                cl_ord_id: ClientOrderId::new("CLIENT-2"),
                symbol: sym(),
                side: OrderSide::Buy,
            }),
            DecodedMessage::OrderCancelReplaceRequest(OrderCancelReplaceRequest {
                header: header(),
                orig_cl_ord_id: ClientOrderId::new("CLIENT-1"),
                cl_ord_id: ClientOrderId::new("CLIENT-3"),
                symbol: sym(),
                side: OrderSide::Buy,
                ord_type: OrdType::Limit,
                price: Some(Cents::new(50100)),
                order_qty: 5,
            }),
            DecodedMessage::OrderMassCancelRequest(OrderMassCancelRequest {
                header: header(),
                cl_ord_id: ClientOrderId::new("CLIENT-4"),
                mass_cancel_request_type: MassCancelRequestType::Security,
                symbol: Some(sym()),
            }),
            DecodedMessage::OrderMassCancelRequest(OrderMassCancelRequest {
                header: header(),
                cl_ord_id: ClientOrderId::new("CLIENT-5"),
                mass_cancel_request_type: MassCancelRequestType::All,
                symbol: None,
            }),
            DecodedMessage::OrderStatusRequest(OrderStatusRequest {
                header: header(),
                order_id: Some(VenueOrderId::new("run-1:BTC:7:0")),
                cl_ord_id: None,
                symbol: sym(),
            }),
        ];
        for msg in msgs {
            let bytes = msg.encode().expect("test encode");
            match decode(&bytes) {
                Ok(back) => assert_eq!(back, msg),
                Err(e) => panic!("round trip failed for {msg:?}: {e:?}"),
            }
        }
    }

    #[test]
    fn test_mass_cancel_security_scope_without_symbol_is_typed_error() {
        let mut writer = FieldWriter::new(OrderMassCancelRequest::MSG_TYPE);
        header().encode(&mut writer);
        writer.str(tags::CL_ORD_ID, "CLIENT-4");
        writer.str(tags::MASS_CANCEL_REQUEST_TYPE, "1"); // per-security, needs symbol
        let bytes = writer.finish().expect("test finish");
        match decode(&bytes) {
            Err(FixDecodeError::MissingConditionalField { tag, .. }) => {
                assert_eq!(tag, tags::SYMBOL);
            }
            other => panic!("expected MissingConditionalField(55), got {other:?}"),
        }
    }

    #[test]
    fn test_order_status_request_without_either_id_is_typed_error() {
        let mut writer = FieldWriter::new(OrderStatusRequest::MSG_TYPE);
        header().encode(&mut writer);
        writer.str(tags::SYMBOL, "BTC-20240329-50000-C");
        let bytes = writer.finish().expect("test finish");
        match decode(&bytes) {
            Err(FixDecodeError::MissingRequiredChoice { first, second }) => {
                assert_eq!(first, tags::ORDER_ID);
                assert_eq!(second, tags::CL_ORD_ID);
            }
            other => panic!("expected MissingRequiredChoice(37/11), got {other:?}"),
        }
    }
}
