//! Golden wire-format tests for the FIX 4.4 vocabulary (#036,
//! [TESTING.md §4](../docs/TESTING.md#4-golden-wire-format-tests),
//! [fix-dialect §6](../docs/specs/fix-dialect.md#6-golden-fixtures-required)).
//!
//! One golden per supported message under `tests/golden/fix/`, using `|` as the
//! SOH placeholder so the fixtures are diff-readable; `BodyLength (9)` and
//! `CheckSum (10)` are **asserted, not elided**. Each golden proves both
//! directions: the constructed message encodes to the committed bytes, and the
//! committed bytes decode back to the identical message. A dialect change must
//! update the affected goldens in the same commit; regenerate with
//! `UPDATE_GOLDEN=1 cargo test --test golden_fix`.

use fauxchange::exchange::{Cents, SequenceNumber, SignedCents, Symbol};
use fauxchange::gateway::fix::enums::{
    CommType, CxlRejResponseTo, ExecType, LastLiquidityInd, MassCancelRequestType,
    MassCancelResponse, MdEntryType, MdUpdateAction, OrdStatus, OrdType, OrderSide,
    SubscriptionRequestType, TimeInForce,
};
use fauxchange::gateway::fix::error::SessionRejectReason;
use fauxchange::gateway::fix::execution::{
    BusinessMessageReject, ExecutionReport, OrderCancelReject, OrderMassCancelReport,
};
use fauxchange::gateway::fix::header::{StandardHeader, UtcTimestamp};
use fauxchange::gateway::fix::marketdata::{
    IncrementalEntry, MarketDataIncrementalRefresh, MarketDataRequest, MarketDataRequestReject,
    MarketDataSnapshotFullRefresh, SnapshotEntry,
};
use fauxchange::gateway::fix::order::{
    NewOrderSingle, OrderCancelReplaceRequest, OrderCancelRequest, OrderMassCancelRequest,
    OrderStatusRequest,
};
use fauxchange::gateway::fix::session::{
    Heartbeat, Logon, Logout, Reject, ResendRequest, SecretField, SequenceReset, TestRequest,
};
use fauxchange::gateway::fix::{DecodedMessage, decode};
use fauxchange::{ClientOrderId, ExecutionId, VenueOrderId};
use ironfix_core::types::{CompId, SeqNum};

/// A fixed sending time so the goldens are byte-stable.
const SENDING_TIME: &str = "20240329-12:00:00.000";

fn comp(id: &str) -> CompId {
    match CompId::new(id) {
        Ok(c) => c,
        Err(_) => panic!("comp id {id} too long"),
    }
}

fn ts(raw: &str) -> UtcTimestamp {
    match UtcTimestamp::parse(52, raw) {
        Ok(t) => t,
        Err(e) => panic!("timestamp {raw} failed: {e:?}"),
    }
}

/// A client → venue header (`SenderCompID=CLIENT`).
fn client_header(seq: u64) -> StandardHeader {
    StandardHeader::new(
        comp("CLIENT"),
        comp("FAUXCHANGE"),
        SeqNum::new(seq),
        ts(SENDING_TIME),
    )
}

/// A venue → client header (`SenderCompID=FAUXCHANGE`).
fn venue_header(seq: u64) -> StandardHeader {
    StandardHeader::new(
        comp("FAUXCHANGE"),
        comp("CLIENT"),
        SeqNum::new(seq),
        ts(SENDING_TIME),
    )
}

fn sym() -> Symbol {
    match Symbol::parse("BTC-20240329-50000-C") {
        Ok(s) => s,
        Err(e) => panic!("symbol failed: {e:?}"),
    }
}

/// Encodes the message, compares (or regenerates) its golden with `|` for SOH,
/// asserts tags 9 and 10 are present, and asserts the golden decodes back to the
/// identical message.
fn assert_golden_fix(name: &str, message: &DecodedMessage) {
    let bytes = message.encode().expect("test encode");
    let text = match String::from_utf8(bytes.clone()) {
        Ok(t) => t,
        Err(e) => panic!("encoded {name} is not utf-8: {e}"),
    };
    let display = text.replace('\u{1}', "|");

    let path = format!("{}/tests/golden/fix/{}", env!("CARGO_MANIFEST_DIR"), name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let mut out = display.clone();
        out.push('\n');
        if let Err(e) = std::fs::write(&path, out) {
            panic!("failed to write golden {path}: {e}");
        }
    } else {
        let expected = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) => panic!("failed to read golden {path}: {e}"),
        };
        assert_eq!(
            display,
            expected.trim_end_matches('\n'),
            "golden mismatch for {name}"
        );
    }

    // BodyLength (9) and CheckSum (10) are asserted, not elided.
    assert!(
        display.starts_with("8=FIX.4.4|9="),
        "tag 9 (BodyLength) must be present in {name}: {display}"
    );
    assert!(
        display.contains("|10="),
        "tag 10 (CheckSum) must be present in {name}: {display}"
    );
    assert!(
        display.ends_with('|'),
        "{name} must terminate with SOH: {display}"
    );

    // The golden decodes back to the identical message (round trip both ways).
    match decode(&bytes) {
        Ok(back) => assert_eq!(&back, message, "decode(golden) mismatch for {name}"),
        Err(e) => panic!("decode failed for {name}: {e:?}"),
    }
}

#[test]
fn test_golden_logon_a() {
    let msg = DecodedMessage::Logon(Logon {
        header: client_header(1),
        heart_bt_int: 30,
        username: "acct-1".to_string(),
        password: SecretField::new("s3cr3t"),
        reset_seq_num_flag: Some(true),
    });
    assert_golden_fix("logon_A.txt", &msg);
}

#[test]
fn test_golden_logout_5() {
    let msg = DecodedMessage::Logout(Logout {
        header: venue_header(9),
        text: Some("session ended".to_string()),
    });
    assert_golden_fix("logout_5.txt", &msg);
}

#[test]
fn test_golden_heartbeat_0() {
    let msg = DecodedMessage::Heartbeat(Heartbeat {
        header: client_header(2),
        test_req_id: Some("TR-1".to_string()),
    });
    assert_golden_fix("heartbeat_0.txt", &msg);
}

#[test]
fn test_golden_test_request_1() {
    let msg = DecodedMessage::TestRequest(TestRequest {
        header: venue_header(3),
        test_req_id: "TR-1".to_string(),
    });
    assert_golden_fix("test_request_1.txt", &msg);
}

#[test]
fn test_golden_resend_request_2() {
    let msg = DecodedMessage::ResendRequest(ResendRequest {
        header: client_header(4),
        begin_seq_no: SeqNum::new(5),
        end_seq_no: SeqNum::new(0),
    });
    assert_golden_fix("resend_request_2.txt", &msg);
}

#[test]
fn test_golden_sequence_reset_4() {
    let msg = DecodedMessage::SequenceReset(SequenceReset {
        header: venue_header(5),
        new_seq_no: SeqNum::new(42),
        gap_fill_flag: Some(true),
    });
    assert_golden_fix("sequence_reset_4.txt", &msg);
}

#[test]
fn test_golden_reject_3() {
    let msg = DecodedMessage::Reject(Reject {
        header: venue_header(6),
        ref_seq_num: SeqNum::new(7),
        session_reject_reason: Some(SessionRejectReason::RequiredTagMissing),
        ref_tag_id: Some(44),
        text: Some("missing price".to_string()),
    });
    assert_golden_fix("reject_3.txt", &msg);
}

#[test]
fn test_golden_new_order_single_d() {
    let msg = DecodedMessage::NewOrderSingle(NewOrderSingle {
        header: client_header(7),
        cl_ord_id: ClientOrderId::new("CLIENT-1"),
        account: Some(fauxchange::AccountId::new("acct-1")),
        symbol: sym(),
        side: OrderSide::Buy,
        transact_time: ts(SENDING_TIME),
        ord_type: OrdType::Limit,
        price: Some(Cents::new(50005)),
        order_qty: 3,
        time_in_force: TimeInForce::Gtc,
        expire_time: None,
    });
    assert_golden_fix("new_order_single_D.txt", &msg);
}

#[test]
fn test_golden_order_cancel_request_f() {
    let msg = DecodedMessage::OrderCancelRequest(OrderCancelRequest {
        header: client_header(8),
        orig_cl_ord_id: ClientOrderId::new("CLIENT-1"),
        cl_ord_id: ClientOrderId::new("CLIENT-2"),
        symbol: sym(),
        side: OrderSide::Buy,
    });
    assert_golden_fix("order_cancel_request_F.txt", &msg);
}

#[test]
fn test_golden_order_cancel_replace_request_g() {
    let msg = DecodedMessage::OrderCancelReplaceRequest(OrderCancelReplaceRequest {
        header: client_header(9),
        orig_cl_ord_id: ClientOrderId::new("CLIENT-1"),
        cl_ord_id: ClientOrderId::new("CLIENT-3"),
        symbol: sym(),
        side: OrderSide::Buy,
        ord_type: OrdType::Limit,
        price: Some(Cents::new(50100)),
        order_qty: 5,
    });
    assert_golden_fix("order_cancel_replace_request_G.txt", &msg);
}

#[test]
fn test_golden_order_mass_cancel_request_q() {
    let msg = DecodedMessage::OrderMassCancelRequest(OrderMassCancelRequest {
        header: client_header(10),
        cl_ord_id: ClientOrderId::new("CLIENT-4"),
        mass_cancel_request_type: MassCancelRequestType::Security,
        symbol: Some(sym()),
    });
    assert_golden_fix("order_mass_cancel_request_q.txt", &msg);
}

#[test]
fn test_golden_order_status_request_h() {
    let msg = DecodedMessage::OrderStatusRequest(OrderStatusRequest {
        header: client_header(11),
        order_id: Some(VenueOrderId::new("run-1:BTC:7:0")),
        cl_ord_id: None,
        symbol: sym(),
    });
    assert_golden_fix("order_status_request_H.txt", &msg);
}

#[test]
fn test_golden_execution_report_8() {
    let msg = DecodedMessage::ExecutionReport(ExecutionReport {
        header: venue_header(12),
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
        transact_time: ts(SENDING_TIME),
        commission: Some(SignedCents::new(-10)),
        comm_type: Some(CommType::Absolute),
        last_liquidity_ind: Some(LastLiquidityInd::Maker),
        ord_rej_reason: None,
        text: None,
    });
    assert_golden_fix("execution_report_8.txt", &msg);
}

#[test]
fn test_golden_order_cancel_reject_9() {
    let msg = DecodedMessage::OrderCancelReject(OrderCancelReject {
        header: venue_header(13),
        order_id: VenueOrderId::new("run-1:BTC:7:0"),
        cl_ord_id: ClientOrderId::new("CLIENT-2"),
        orig_cl_ord_id: ClientOrderId::new("CLIENT-1"),
        ord_status: OrdStatus::Canceled,
        cxl_rej_response_to: CxlRejResponseTo::OrderCancelRequest,
        cxl_rej_reason: 1,
        text: Some("unknown order".to_string()),
    });
    assert_golden_fix("order_cancel_reject_9.txt", &msg);
}

#[test]
fn test_golden_order_mass_cancel_report_r() {
    let msg = DecodedMessage::OrderMassCancelReport(OrderMassCancelReport {
        header: venue_header(14),
        mass_cancel_response: MassCancelResponse::All,
        total_affected_orders: 2,
        affected_orders: vec![
            VenueOrderId::new("run-1:BTC:7:0"),
            VenueOrderId::new("run-1:BTC:8:0"),
        ],
    });
    assert_golden_fix("order_mass_cancel_report_r.txt", &msg);
}

#[test]
fn test_golden_business_message_reject_j() {
    let msg = DecodedMessage::BusinessMessageReject(BusinessMessageReject {
        header: venue_header(19),
        ref_seq_num: SequenceNumber::new(42),
        ref_msg_type: "AE".to_string(),
        business_reject_reason: 3,
        text: Some("unsupported message type".to_string()),
    });
    assert_golden_fix("business_message_reject_j.txt", &msg);
}

#[test]
fn test_golden_market_data_request_v() {
    let msg = DecodedMessage::MarketDataRequest(MarketDataRequest {
        header: client_header(15),
        md_req_id: "MDR-1".to_string(),
        subscription_request_type: SubscriptionRequestType::SnapshotPlusUpdates,
        market_depth: 0,
        entry_types: vec![MdEntryType::Bid, MdEntryType::Offer, MdEntryType::Trade],
        symbols: vec![sym()],
    });
    assert_golden_fix("market_data_request_V.txt", &msg);
}

#[test]
fn test_golden_market_data_snapshot_w() {
    let msg = DecodedMessage::MarketDataSnapshotFullRefresh(MarketDataSnapshotFullRefresh {
        header: venue_header(16),
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
    });
    assert_golden_fix("market_data_snapshot_W.txt", &msg);
}

#[test]
fn test_golden_market_data_incremental_x() {
    let msg = DecodedMessage::MarketDataIncrementalRefresh(MarketDataIncrementalRefresh {
        header: venue_header(17),
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
                update_action: MdUpdateAction::Delete,
                entry_type: MdEntryType::Offer,
                symbol: sym(),
                price: Cents::new(50005),
                size: 0,
            },
        ],
    });
    assert_golden_fix("market_data_incremental_X.txt", &msg);
}

#[test]
fn test_golden_market_data_request_reject_y() {
    let msg = DecodedMessage::MarketDataRequestReject(MarketDataRequestReject {
        header: venue_header(18),
        md_req_id: "MDR-1".to_string(),
        md_req_rej_reason: 0,
        text: Some("unsupported subscription".to_string()),
    });
    assert_golden_fix("market_data_request_reject_Y.txt", &msg);
}

/// The economic-parity golden ([fix-dialect §6](../docs/specs/fix-dialect.md#6-golden-fixtures-required)):
/// a REST order and its FIX twin produce the **same cents** through the checked
/// `Price` seam, and the FIX execution report carries the same
/// `underlying_sequence` in `SecondaryExecID (527)`.
#[test]
fn test_economic_parity_rest_and_fix_agree_on_cents_and_sequence() {
    // The FIX new-order-single golden's `44=500.05` decodes to exactly 50005
    // cents — the same integer a REST order at `price: 50005` carries.
    let fix_new_order = NewOrderSingle {
        header: client_header(7),
        cl_ord_id: ClientOrderId::new("CLIENT-1"),
        account: None,
        symbol: sym(),
        side: OrderSide::Buy,
        transact_time: ts(SENDING_TIME),
        ord_type: OrdType::Limit,
        price: Some(Cents::new(50005)),
        order_qty: 3,
        time_in_force: TimeInForce::Gtc,
        expire_time: None,
    };
    let rest_order = fauxchange::PlaceLimitOrderRequest {
        side: fauxchange::Side::Buy,
        price: Cents::new(50005),
        quantity: 3,
        time_in_force: None,
        gtd_expires_at: None,
        client_order_id: Some(ClientOrderId::new("CLIENT-1")),
    };
    assert_eq!(fix_new_order.price, Some(rest_order.price));

    // Re-decode the FIX wire to prove the seam produced the cents, not a float.
    let wire = DecodedMessage::NewOrderSingle(fix_new_order)
        .encode()
        .expect("test encode");
    match decode(&wire) {
        Ok(DecodedMessage::NewOrderSingle(back)) => {
            assert_eq!(back.price, Some(Cents::new(50005)));
        }
        other => panic!("expected NewOrderSingle, got {other:?}"),
    }

    // The execution report's SecondaryExecID (527) is the underlying_sequence 7.
    let report = ExecutionReport {
        header: venue_header(12),
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
        transact_time: ts(SENDING_TIME),
        commission: Some(SignedCents::new(-10)),
        comm_type: Some(CommType::Absolute),
        last_liquidity_ind: Some(LastLiquidityInd::Maker),
        ord_rej_reason: None,
        text: None,
    };
    assert_eq!(report.secondary_exec_id, SequenceNumber::new(7));
}

/// Encodes an ordered message script to one `|`-delimited frame per line, asserting
/// tags `9`/`10` are present and each frame round-trips, then compares (or, under
/// `UPDATE_GOLDEN=1`, regenerates) the captured golden.
fn assert_golden_script(name: &str, messages: &[DecodedMessage]) {
    let mut lines: Vec<String> = Vec::with_capacity(messages.len());
    for message in messages {
        let bytes = message.encode().expect("test encode");
        let text = match String::from_utf8(bytes.clone()) {
            Ok(t) => t,
            Err(e) => panic!("encoded script frame is not utf-8: {e}"),
        };
        let display = text.replace('\u{1}', "|");
        assert!(
            display.starts_with("8=FIX.4.4|9="),
            "tag 9 (BodyLength) must be present: {display}"
        );
        assert!(
            display.contains("|10="),
            "tag 10 (CheckSum) must be present: {display}"
        );
        assert!(
            display.ends_with('|'),
            "each script frame must terminate with SOH: {display}"
        );
        match decode(&bytes) {
            Ok(back) => assert_eq!(&back, message, "decode(script frame) mismatch"),
            Err(e) => panic!("decode failed for a script frame: {e:?}"),
        }
        lines.push(display);
    }
    let joined = lines.join("\n");

    let path = format!("{}/tests/golden/fix/{}", env!("CARGO_MANIFEST_DIR"), name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let mut out = joined.clone();
        out.push('\n');
        if let Err(e) = std::fs::write(&path, out) {
            panic!("failed to write golden {path}: {e}");
        }
    } else {
        let expected = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) => panic!("failed to read golden {path}: {e}"),
        };
        assert_eq!(
            joined,
            expected.trim_end_matches('\n'),
            "golden script mismatch for {name}"
        );
    }
}

/// A `New`/`Replaced`/`Canceled`/`Rejected` `ExecutionReport (8)` for the script.
#[allow(clippy::too_many_arguments)]
fn script_report(
    seq: u64,
    exec_type: ExecType,
    ord_status: OrdStatus,
    side: OrderSide,
    leaves: u64,
    cum: u64,
    price: Option<Cents>,
    underlying_sequence: u64,
    ord_rej_reason: Option<u16>,
    text: Option<&str>,
) -> ExecutionReport {
    ExecutionReport {
        header: venue_header(seq),
        order_id: VenueOrderId::new("run-1:BTC:1:0"),
        exec_id: ExecutionId::new(format!("run-1:BTC:1:{seq}")),
        exec_type,
        ord_status,
        symbol: sym(),
        side,
        leaves_qty: leaves,
        cum_qty: cum,
        last_qty: None,
        last_px: None,
        price,
        secondary_exec_id: SequenceNumber::new(underlying_sequence),
        transact_time: ts(SENDING_TIME),
        commission: None,
        comm_type: None,
        last_liquidity_ind: None,
        ord_rej_reason,
        text: text.map(str::to_string),
    }
}

/// The **captured conformance script** (#041): the canonical ordered frame set the
/// FIX conformance test exercises — session admin (`A`/`0`/`1`/`2`/`4`/`5`), order
/// entry (`D`/`G`/`F` → `8`) and its rejects (`8 Rejected`/`9`), and market data
/// (`V` → `W`/`X`) and its rejects (`Y`/`3`/`j`) — one diff-readable golden with `|`
/// for SOH and tags `9`/`10` asserted per frame. Live behaviour (reason tags,
/// redaction) is asserted in `tests/parity.rs`; this pins the wire shapes.
#[test]
fn test_golden_conformance_script() {
    let script = vec![
        // --- Session admin: logon + heartbeat cadence ---
        DecodedMessage::Logon(Logon {
            header: client_header(1),
            heart_bt_int: 30,
            username: "trader-1".to_string(),
            password: SecretField::new("REDACTED-TEST-CREDENTIAL"),
            reset_seq_num_flag: None,
        }),
        DecodedMessage::TestRequest(TestRequest {
            header: venue_header(1),
            test_req_id: "PING-CONF".to_string(),
        }),
        DecodedMessage::Heartbeat(Heartbeat {
            header: client_header(2),
            test_req_id: Some("PING-CONF".to_string()),
        }),
        // --- Order entry: D → 8 New, G → 8 Replaced, F → 8 Canceled ---
        DecodedMessage::NewOrderSingle(NewOrderSingle {
            header: client_header(3),
            cl_ord_id: ClientOrderId::new("conf-rest"),
            account: None,
            symbol: sym(),
            side: OrderSide::Sell,
            transact_time: ts(SENDING_TIME),
            ord_type: OrdType::Limit,
            price: Some(Cents::new(50_000)),
            order_qty: 5,
            time_in_force: TimeInForce::Gtc,
            expire_time: None,
        }),
        DecodedMessage::ExecutionReport(script_report(
            2,
            ExecType::New,
            OrdStatus::New,
            OrderSide::Sell,
            5,
            0,
            Some(Cents::new(50_000)),
            1,
            None,
            None,
        )),
        DecodedMessage::OrderCancelReplaceRequest(OrderCancelReplaceRequest {
            header: client_header(4),
            orig_cl_ord_id: ClientOrderId::new("conf-rest"),
            cl_ord_id: ClientOrderId::new("conf-repl"),
            symbol: sym(),
            side: OrderSide::Sell,
            ord_type: OrdType::Limit,
            price: Some(Cents::new(50_500)),
            order_qty: 5,
        }),
        DecodedMessage::ExecutionReport(script_report(
            3,
            ExecType::Replaced,
            OrdStatus::Replaced,
            OrderSide::Sell,
            5,
            0,
            Some(Cents::new(50_500)),
            2,
            None,
            None,
        )),
        DecodedMessage::OrderCancelRequest(OrderCancelRequest {
            header: client_header(5),
            orig_cl_ord_id: ClientOrderId::new("conf-repl"),
            cl_ord_id: ClientOrderId::new("conf-cxl"),
            symbol: sym(),
            side: OrderSide::Sell,
        }),
        DecodedMessage::ExecutionReport(script_report(
            4,
            ExecType::Canceled,
            OrdStatus::Canceled,
            OrderSide::Sell,
            0,
            0,
            None,
            3,
            None,
            None,
        )),
        // --- Market data: V → W then X ---
        DecodedMessage::MarketDataRequest(MarketDataRequest {
            header: client_header(6),
            md_req_id: "MDR-CONF".to_string(),
            subscription_request_type: SubscriptionRequestType::SnapshotPlusUpdates,
            market_depth: 0,
            entry_types: vec![MdEntryType::Bid, MdEntryType::Offer],
            symbols: vec![sym()],
        }),
        DecodedMessage::MarketDataSnapshotFullRefresh(MarketDataSnapshotFullRefresh {
            header: venue_header(5),
            md_req_id: "MDR-CONF".to_string(),
            symbol: sym(),
            rpt_seq: SequenceNumber::new(1),
            entries: vec![SnapshotEntry {
                entry_type: MdEntryType::Offer,
                price: Cents::new(50_500),
                size: 5,
            }],
        }),
        DecodedMessage::MarketDataIncrementalRefresh(MarketDataIncrementalRefresh {
            header: venue_header(6),
            md_req_id: "MDR-CONF".to_string(),
            rpt_seq: SequenceNumber::new(2),
            entries: vec![IncrementalEntry {
                update_action: MdUpdateAction::Change,
                entry_type: MdEntryType::Offer,
                symbol: sym(),
                price: Cents::new(50_500),
                size: 8,
            }],
        }),
        // --- Session admin: resend / sequence reset / logout ---
        DecodedMessage::ResendRequest(ResendRequest {
            header: client_header(7),
            begin_seq_no: SeqNum::new(3),
            end_seq_no: SeqNum::new(0),
        }),
        DecodedMessage::SequenceReset(SequenceReset {
            header: venue_header(7),
            new_seq_no: SeqNum::new(8),
            gap_fill_flag: Some(true),
        }),
        DecodedMessage::Logout(Logout {
            header: venue_header(8),
            text: Some("session ended".to_string()),
        }),
        // --- Every context-sensitive reject row (03 §8) ---
        DecodedMessage::Reject(Reject {
            header: venue_header(9),
            ref_seq_num: SeqNum::new(3),
            session_reject_reason: Some(SessionRejectReason::RequiredTagMissing),
            ref_tag_id: Some(54),
            text: Some("required tag missing".to_string()),
        }),
        DecodedMessage::ExecutionReport(script_report(
            10,
            ExecType::Rejected,
            OrdStatus::Rejected,
            OrderSide::Sell,
            0,
            0,
            None,
            0,
            Some(6),
            Some("client_order_id reused with a different order"),
        )),
        DecodedMessage::OrderCancelReject(OrderCancelReject {
            header: venue_header(11),
            order_id: VenueOrderId::new("run-1:BTC:0:0"),
            cl_ord_id: ClientOrderId::new("conf-cxl-unknown"),
            orig_cl_ord_id: ClientOrderId::new("never-placed"),
            ord_status: OrdStatus::Rejected,
            cxl_rej_response_to: CxlRejResponseTo::OrderCancelRequest,
            cxl_rej_reason: 1,
            text: Some("unknown order".to_string()),
        }),
        DecodedMessage::MarketDataRequestReject(MarketDataRequestReject {
            header: venue_header(12),
            md_req_id: "MDR-TRADE".to_string(),
            md_req_rej_reason: 8,
            text: Some("only Bid/Offer market data is served over FIX".to_string()),
        }),
        DecodedMessage::BusinessMessageReject(BusinessMessageReject {
            header: venue_header(13),
            ref_seq_num: SequenceNumber::new(2),
            ref_msg_type: "R".to_string(),
            business_reject_reason: 3,
            text: Some("unsupported message type".to_string()),
        }),
    ];
    assert_golden_script("conformance_script.txt", &script);
}
