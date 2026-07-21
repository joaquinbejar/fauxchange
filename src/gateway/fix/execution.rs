//! Execution and reject reports: `ExecutionReport (8)`, `OrderCancelReject (9)`,
//! `OrderMassCancelReport (r)`
//! ([fix-dialect §2.2](../../../docs/specs/fix-dialect.md#22-order-entry-and-execution)).
//!
//! `OrderID (37)` / `ExecID (17)` are the §6.1 composite ids; `SecondaryExecID
//! (527)` is the `underlying_sequence` cross-surface join key
//! ([fix-dialect §4](../../../docs/specs/fix-dialect.md#4-identifiers-correlation-and-idempotency)).
//! Per-leg fees ride `Commission (12)` + `CommType (13)=3` (a maker leg can be a
//! rebate, so the amount is signed), and the mass-cancel report's affected ids
//! ride the **ordered** `NoAffectedOrders (534)` group ([ADR-0009](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).

use super::FixBody;
use super::codec::{FieldBag, FieldWriter, tags};
use super::enums::{
    CommType, CxlRejResponseTo, ExecType, LastLiquidityInd, MassCancelResponse, OrdStatus,
    OrderSide,
};
use super::error::FixDecodeError;
use super::header::StandardHeader;
use super::price::{
    parse_decimal_to_cents, parse_signed_decimal_to_cents, render_cents_to_decimal,
    render_signed_cents_to_decimal,
};
use crate::exchange::{Cents, SequenceNumber, SignedCents, Symbol};
use crate::models::{ClientOrderId, ExecutionId, VenueOrderId};

/// Decodes an optional non-negative `Price`-typed tag into [`Cents`].
fn decode_optional_price(fields: &FieldBag<'_>, tag: u16) -> Result<Option<Cents>, FixDecodeError> {
    match fields.opt_str(tag)? {
        Some(raw) => parse_decimal_to_cents(raw)
            .map(Some)
            .map_err(|e| FixDecodeError::price(tag, e)),
        None => Ok(None),
    }
}

/// `ExecutionReport (8)` — one leg of a fill, a `New`/`Canceled`/`Replaced`/
/// `Expired`/`Rejected` transition. Outbound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionReport {
    /// Standard header.
    pub header: StandardHeader,
    /// `OrderID (37)` — the venue composite order id.
    pub order_id: VenueOrderId,
    /// `ExecID (17)` — the venue composite execution id.
    pub exec_id: ExecutionId,
    /// `ExecType (150)`.
    pub exec_type: ExecType,
    /// `OrdStatus (39)`.
    pub ord_status: OrdStatus,
    /// `Symbol (55)`.
    pub symbol: Symbol,
    /// `Side (54)`.
    pub side: OrderSide,
    /// `LeavesQty (151)`.
    pub leaves_qty: u64,
    /// `CumQty (14)`.
    pub cum_qty: u64,
    /// `LastQty (32)` — the quantity of this fill leg.
    pub last_qty: Option<u64>,
    /// `LastPx (31)` — the price of this fill leg, cents.
    pub last_px: Option<Cents>,
    /// `Price (44)` — the order's limit price, cents.
    pub price: Option<Cents>,
    /// `SecondaryExecID (527)` — the `underlying_sequence` join key.
    pub secondary_exec_id: SequenceNumber,
    /// `Commission (12)` — the per-leg fee (signed; a maker rebate is negative).
    pub commission: Option<SignedCents>,
    /// `CommType (13)` — `3` (absolute) when a `Commission` is present.
    pub comm_type: Option<CommType>,
    /// `LastLiquidityInd (851)` — maker or taker.
    pub last_liquidity_ind: Option<LastLiquidityInd>,
    /// `OrdRejReason (103)` — present on a `Rejected` report.
    pub ord_rej_reason: Option<u16>,
    /// `Text (58)` — a redacted reason.
    pub text: Option<String>,
}

impl FixBody for ExecutionReport {
    const MSG_TYPE: &'static str = "8";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let commission = match fields.opt_str(tags::COMMISSION)? {
            Some(raw) => Some(
                parse_signed_decimal_to_cents(raw)
                    .map_err(|e| FixDecodeError::price(tags::COMMISSION, e))?,
            ),
            None => None,
        };
        let comm_type = match fields.opt_str(tags::COMM_TYPE)? {
            Some(raw) => Some(CommType::from_fix(raw)?),
            None => None,
        };
        // `Commission (12)` + `CommType (13)` are a JOINT conditional
        // ([fix-dialect §2.2](../../../docs/specs/fix-dialect.md#22-order-entry-and-execution)):
        // a per-leg fee is present as the pair or not at all. Exactly one present
        // is a mis-conditioned `C` tag — a typed reject, never a silent default.
        match (commission.is_some(), comm_type.is_some()) {
            (true, false) => {
                return Err(FixDecodeError::MissingConditionalField {
                    tag: tags::COMM_TYPE,
                    condition: "Commission (12) is present",
                });
            }
            (false, true) => {
                return Err(FixDecodeError::MissingConditionalField {
                    tag: tags::COMMISSION,
                    condition: "CommType (13) is present",
                });
            }
            (true, true) | (false, false) => {}
        }
        let last_liquidity_ind = match fields.opt_str(tags::LAST_LIQUIDITY_IND)? {
            Some(raw) => Some(LastLiquidityInd::from_fix(raw)?),
            None => None,
        };
        Ok(Self {
            header,
            order_id: VenueOrderId::new(fields.req_str(tags::ORDER_ID)?),
            exec_id: ExecutionId::new(fields.req_str(tags::EXEC_ID)?),
            exec_type: ExecType::from_fix(fields.req_str(tags::EXEC_TYPE)?)?,
            ord_status: OrdStatus::from_fix(fields.req_str(tags::ORD_STATUS)?)?,
            symbol: Symbol::parse(fields.req_str(tags::SYMBOL)?)
                .map_err(|e| FixDecodeError::from_symbol_error(&e))?,
            side: OrderSide::from_fix(fields.req_str(tags::SIDE)?)?,
            leaves_qty: fields.req_u64(tags::LEAVES_QTY)?,
            cum_qty: fields.req_u64(tags::CUM_QTY)?,
            last_qty: fields.opt_u64(tags::LAST_QTY)?,
            last_px: decode_optional_price(fields, tags::LAST_PX)?,
            price: decode_optional_price(fields, tags::PRICE)?,
            secondary_exec_id: SequenceNumber::new(fields.req_u64(tags::SECONDARY_EXEC_ID)?),
            commission,
            comm_type,
            last_liquidity_ind,
            ord_rej_reason: fields.opt_u16(tags::ORD_REJ_REASON)?,
            text: fields.opt_str(tags::TEXT)?.map(str::to_string),
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::ORDER_ID, self.order_id.as_str());
        writer.str(tags::EXEC_ID, self.exec_id.as_str());
        writer.str(tags::EXEC_TYPE, self.exec_type.to_fix());
        writer.str(tags::ORD_STATUS, self.ord_status.to_fix());
        writer.str(tags::SYMBOL, self.symbol.as_str());
        writer.str(tags::SIDE, self.side.to_fix());
        writer.u64(tags::LEAVES_QTY, self.leaves_qty);
        writer.u64(tags::CUM_QTY, self.cum_qty);
        writer.opt_u64(tags::LAST_QTY, self.last_qty);
        if let Some(last_px) = self.last_px {
            writer.str(tags::LAST_PX, &render_cents_to_decimal(last_px));
        }
        if let Some(price) = self.price {
            writer.str(tags::PRICE, &render_cents_to_decimal(price));
        }
        writer.u64(tags::SECONDARY_EXEC_ID, self.secondary_exec_id.get());
        if let Some(commission) = self.commission {
            writer.str(
                tags::COMMISSION,
                &render_signed_cents_to_decimal(commission),
            );
        }
        writer.opt_str(tags::COMM_TYPE, self.comm_type.map(CommType::to_fix));
        writer.opt_str(
            tags::LAST_LIQUIDITY_IND,
            self.last_liquidity_ind.map(LastLiquidityInd::to_fix),
        );
        writer.opt_u16(tags::ORD_REJ_REASON, self.ord_rej_reason);
        writer.opt_str(tags::TEXT, self.text.as_deref());
        writer.finish()
    }
}

/// `OrderCancelReject (9)` — a rejected cancel/replace. Outbound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCancelReject {
    /// Standard header.
    pub header: StandardHeader,
    /// `OrderID (37)`.
    pub order_id: VenueOrderId,
    /// `ClOrdID (11)`.
    pub cl_ord_id: ClientOrderId,
    /// `OrigClOrdID (41)`.
    pub orig_cl_ord_id: ClientOrderId,
    /// `OrdStatus (39)`.
    pub ord_status: OrdStatus,
    /// `CxlRejResponseTo (434)`.
    pub cxl_rej_response_to: CxlRejResponseTo,
    /// `CxlRejReason (102)`.
    pub cxl_rej_reason: u16,
    /// `Text (58)` — a redacted reason.
    pub text: Option<String>,
}

impl FixBody for OrderCancelReject {
    const MSG_TYPE: &'static str = "9";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        Ok(Self {
            header,
            order_id: VenueOrderId::new(fields.req_str(tags::ORDER_ID)?),
            cl_ord_id: ClientOrderId::new(fields.req_str(tags::CL_ORD_ID)?),
            orig_cl_ord_id: ClientOrderId::new(fields.req_str(tags::ORIG_CL_ORD_ID)?),
            ord_status: OrdStatus::from_fix(fields.req_str(tags::ORD_STATUS)?)?,
            cxl_rej_response_to: CxlRejResponseTo::from_fix(
                fields.req_str(tags::CXL_REJ_RESPONSE_TO)?,
            )?,
            cxl_rej_reason: fields.req_u16(tags::CXL_REJ_REASON)?,
            text: fields.opt_str(tags::TEXT)?.map(str::to_string),
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::ORDER_ID, self.order_id.as_str());
        writer.str(tags::CL_ORD_ID, self.cl_ord_id.as_str());
        writer.str(tags::ORIG_CL_ORD_ID, self.orig_cl_ord_id.as_str());
        writer.str(tags::ORD_STATUS, self.ord_status.to_fix());
        writer.str(tags::CXL_REJ_RESPONSE_TO, self.cxl_rej_response_to.to_fix());
        writer.u16(tags::CXL_REJ_REASON, self.cxl_rej_reason);
        writer.opt_str(tags::TEXT, self.text.as_deref());
        writer.finish()
    }
}

/// `OrderMassCancelReport (r)` — the outcome of a mass cancel with its ordered
/// affected ids. Outbound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderMassCancelReport {
    /// Standard header.
    pub header: StandardHeader,
    /// `MassCancelResponse (531)`.
    pub mass_cancel_response: MassCancelResponse,
    /// `TotalAffectedOrders (533)`.
    pub total_affected_orders: u32,
    /// `NoAffectedOrders (534)` + `AffectedOrderID (535)` — the ordered affected
    /// order ids ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
    pub affected_orders: Vec<VenueOrderId>,
}

impl FixBody for OrderMassCancelReport {
    const MSG_TYPE: &'static str = "r";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let entries = fields.group(
            tags::NO_AFFECTED_ORDERS,
            tags::AFFECTED_ORDER_ID,
            &[tags::AFFECTED_ORDER_ID],
        )?;
        let mut affected_orders = Vec::with_capacity(entries.len());
        for entry in &entries {
            affected_orders.push(VenueOrderId::new(entry.req_str(tags::AFFECTED_ORDER_ID)?));
        }
        Ok(Self {
            header,
            mass_cancel_response: MassCancelResponse::from_fix(
                fields.req_str(tags::MASS_CANCEL_RESPONSE)?,
            )?,
            total_affected_orders: fields.req_u32(tags::TOTAL_AFFECTED_ORDERS)?,
            affected_orders,
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(
            tags::MASS_CANCEL_RESPONSE,
            self.mass_cancel_response.to_fix(),
        );
        writer.u64(
            tags::TOTAL_AFFECTED_ORDERS,
            u64::from(self.total_affected_orders),
        );
        writer.u64(tags::NO_AFFECTED_ORDERS, self.affected_orders.len() as u64);
        for affected in &self.affected_orders {
            writer.str(tags::AFFECTED_ORDER_ID, affected.as_str());
        }
        writer.finish()
    }
}

/// `BusinessMessageReject (j)` — a well-formed application message the venue
/// understood but cannot business-process (an application `MsgType` with no
/// handler). Outbound; the order-level rejects (`8`/`9`) are preferred for
/// `D`/`F`/`G`, so `j` is the reject of last resort for an unsupported
/// application message ([03 §8](../../../docs/03-protocol-surfaces.md#8-error-mapping-across-surfaces)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessMessageReject {
    /// Standard header.
    pub header: StandardHeader,
    /// `RefSeqNum (45)` — the `MsgSeqNum` of the rejected message.
    pub ref_seq_num: SequenceNumber,
    /// `RefMsgType (372)` — the `MsgType (35)` of the rejected message.
    pub ref_msg_type: String,
    /// `BusinessRejectReason (380)`.
    pub business_reject_reason: u16,
    /// `Text (58)` — a redacted reason.
    pub text: Option<String>,
}

impl FixBody for BusinessMessageReject {
    const MSG_TYPE: &'static str = "j";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        Ok(Self {
            header,
            ref_seq_num: SequenceNumber::new(fields.req_u64(tags::REF_SEQ_NUM)?),
            ref_msg_type: fields.req_str(tags::REF_MSG_TYPE)?.to_string(),
            business_reject_reason: fields.req_u16(tags::BUSINESS_REJECT_REASON)?,
            text: fields.opt_str(tags::TEXT)?.map(str::to_string),
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.u64(tags::REF_SEQ_NUM, self.ref_seq_num.get());
        writer.str(tags::REF_MSG_TYPE, &self.ref_msg_type);
        writer.u16(tags::BUSINESS_REJECT_REASON, self.business_reject_reason);
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
            CompId::new("FAUXCHANGE").expect("comp id"),
            CompId::new("CLIENT").expect("comp id"),
            SeqNum::new(3),
            UtcTimestamp::parse(52, "20240329-12:00:00.000").expect("ts"),
        )
    }

    fn sym() -> Symbol {
        Symbol::parse("BTC-20240329-50000-C").expect("symbol")
    }

    fn filled_report() -> ExecutionReport {
        ExecutionReport {
            header: header(),
            order_id: VenueOrderId::new("run-1:BTC:7:0"),
            exec_id: ExecutionId::new("run-1:BTC:7:0"),
            exec_type: ExecType::Trade,
            ord_status: OrdStatus::Filled,
            symbol: sym(),
            side: OrderSide::Buy,
            leaves_qty: 0,
            cum_qty: 3,
            last_qty: Some(3),
            last_px: Some(Cents::new(50005)),
            price: Some(Cents::new(50005)),
            secondary_exec_id: SequenceNumber::new(7),
            commission: Some(SignedCents::new(-10)),
            comm_type: Some(CommType::Absolute),
            last_liquidity_ind: Some(LastLiquidityInd::Maker),
            ord_rej_reason: None,
            text: None,
        }
    }

    #[test]
    fn test_execution_report_trade_round_trips_with_signed_fee() {
        let report = filled_report();
        let bytes = report.encode();
        let wire = String::from_utf8(bytes.clone()).expect("utf8");
        // Composite ids, the underlying_sequence join key, and the signed rebate.
        assert!(wire.contains("\u{1}37=run-1:BTC:7:0\u{1}"), "{wire}");
        assert!(wire.contains("\u{1}527=7\u{1}"), "{wire}");
        assert!(wire.contains("\u{1}12=-0.10\u{1}"), "{wire}");
        match decode(&bytes) {
            Ok(DecodedMessage::ExecutionReport(back)) => assert_eq!(back, report),
            other => panic!("expected ExecutionReport, got {other:?}"),
        }
    }

    #[test]
    fn test_execution_report_rejected_carries_ord_rej_reason() {
        let report = ExecutionReport {
            exec_type: ExecType::Rejected,
            ord_status: OrdStatus::Rejected,
            leaves_qty: 0,
            cum_qty: 0,
            last_qty: None,
            last_px: None,
            commission: None,
            comm_type: None,
            last_liquidity_ind: None,
            ord_rej_reason: Some(1),
            text: Some("unknown symbol".to_string()),
            ..filled_report()
        };
        let bytes = report.encode();
        match decode(&bytes) {
            Ok(DecodedMessage::ExecutionReport(back)) => assert_eq!(back, report),
            other => panic!("expected ExecutionReport, got {other:?}"),
        }
    }

    /// Writes the required `ExecutionReport` fields plus whatever fee tags the
    /// caller adds, so a mis-paired `Commission`/`CommType` can be hand-built.
    fn report_frame_with_fee_tags(add_fee: impl FnOnce(&mut FieldWriter)) -> Vec<u8> {
        let mut writer = FieldWriter::new(ExecutionReport::MSG_TYPE);
        header().encode(&mut writer);
        writer.str(tags::ORDER_ID, "run-1:BTC:7:0");
        writer.str(tags::EXEC_ID, "run-1:BTC:7:0");
        writer.str(tags::EXEC_TYPE, "F");
        writer.str(tags::ORD_STATUS, "2");
        writer.str(tags::SYMBOL, "BTC-20240329-50000-C");
        writer.str(tags::SIDE, "1");
        writer.u64(tags::LEAVES_QTY, 0);
        writer.u64(tags::CUM_QTY, 3);
        writer.u64(tags::SECONDARY_EXEC_ID, 7);
        add_fee(&mut writer);
        writer.finish()
    }

    #[test]
    fn test_execution_report_commission_without_comm_type_is_typed_error() {
        // Commission (12) present without CommType (13) — the joint conditional
        // is violated, so it is a typed reject, not a silent default.
        let bytes = report_frame_with_fee_tags(|w| w.str(tags::COMMISSION, "-0.10"));
        match decode(&bytes) {
            Err(FixDecodeError::MissingConditionalField { tag, .. }) => {
                assert_eq!(tag, tags::COMM_TYPE);
            }
            other => panic!("expected MissingConditionalField(13), got {other:?}"),
        }
    }

    #[test]
    fn test_execution_report_comm_type_without_commission_is_typed_error() {
        // CommType (13) present without Commission (12) — the mirror violation.
        let bytes = report_frame_with_fee_tags(|w| w.str(tags::COMM_TYPE, "3"));
        match decode(&bytes) {
            Err(FixDecodeError::MissingConditionalField { tag, .. }) => {
                assert_eq!(tag, tags::COMMISSION);
            }
            other => panic!("expected MissingConditionalField(12), got {other:?}"),
        }
    }

    #[test]
    fn test_execution_report_no_fee_pair_decodes() {
        // Neither fee tag present is valid (a fill with no per-leg fee reported).
        let bytes = report_frame_with_fee_tags(|_| {});
        match decode(&bytes) {
            Ok(DecodedMessage::ExecutionReport(back)) => {
                assert!(back.commission.is_none());
                assert!(back.comm_type.is_none());
            }
            other => panic!("expected ExecutionReport, got {other:?}"),
        }
    }

    #[test]
    fn test_order_cancel_reject_round_trips() {
        let reject = OrderCancelReject {
            header: header(),
            order_id: VenueOrderId::new("run-1:BTC:7:0"),
            cl_ord_id: ClientOrderId::new("CLIENT-2"),
            orig_cl_ord_id: ClientOrderId::new("CLIENT-1"),
            ord_status: OrdStatus::Canceled,
            cxl_rej_response_to: CxlRejResponseTo::OrderCancelRequest,
            cxl_rej_reason: 1,
            text: Some("unknown order".to_string()),
        };
        let bytes = reject.encode();
        match decode(&bytes) {
            Ok(DecodedMessage::OrderCancelReject(back)) => assert_eq!(back, reject),
            other => panic!("expected OrderCancelReject, got {other:?}"),
        }
    }

    #[test]
    fn test_mass_cancel_report_preserves_affected_id_order() {
        let report = OrderMassCancelReport {
            header: header(),
            mass_cancel_response: MassCancelResponse::All,
            total_affected_orders: 3,
            affected_orders: vec![
                VenueOrderId::new("run-1:BTC:7:0"),
                VenueOrderId::new("run-1:BTC:8:0"),
                VenueOrderId::new("run-1:BTC:9:0"),
            ],
        };
        let bytes = report.encode();
        match decode(&bytes) {
            Ok(DecodedMessage::OrderMassCancelReport(back)) => {
                assert_eq!(back, report);
                // The ordered affected-id list is preserved exactly.
                assert_eq!(back.affected_orders[0].as_str(), "run-1:BTC:7:0");
                assert_eq!(back.affected_orders[2].as_str(), "run-1:BTC:9:0");
            }
            other => panic!("expected OrderMassCancelReport, got {other:?}"),
        }
    }

    #[test]
    fn test_business_message_reject_round_trips() {
        let reject = BusinessMessageReject {
            header: header(),
            ref_seq_num: SequenceNumber::new(42),
            ref_msg_type: "AE".to_string(),
            business_reject_reason: 3,
            text: Some("unsupported message type".to_string()),
        };
        let bytes = reject.encode();
        match decode(&bytes) {
            Ok(DecodedMessage::BusinessMessageReject(back)) => assert_eq!(back, reject),
            other => panic!("expected BusinessMessageReject, got {other:?}"),
        }
    }

    #[test]
    fn test_mass_cancel_report_empty_group_round_trips() {
        let report = OrderMassCancelReport {
            header: header(),
            mass_cancel_response: MassCancelResponse::Rejected,
            total_affected_orders: 0,
            affected_orders: Vec::new(),
        };
        let bytes = report.encode();
        match decode(&bytes) {
            Ok(DecodedMessage::OrderMassCancelReport(back)) => assert_eq!(back, report),
            other => panic!("expected OrderMassCancelReport, got {other:?}"),
        }
    }
}
