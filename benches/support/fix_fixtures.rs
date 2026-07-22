//! Fixed, valid FIX fixtures for the HP-3 parse/encode bench
//! ([043](../../milestones/v0.4-fix-gateway/043-fix-parse-encode-budget.md),
//! [07 §3-HP3](../../docs/07-performance-budgets.md#3-latency-budgets-design-targets)).
//!
//! Deliberately the SAME `NewOrderSingle (D)` / `ExecutionReport (8)` shapes
//! that `tests/golden_fix.rs` golden-tests (`tests/golden/fix/new_order_single_D.txt`
//! / `tests/golden/fix/execution_report_8.txt`) — docs/07 §3-HP3 asks the perf
//! bench to share fixtures with the pinned dialect (#036) rather than invent a
//! parallel shape that could silently drift from what the wire actually
//! carries. To make "no drift" *enforced* rather than merely intended, the
//! decode span consumes the committed golden bytes **directly** (`include_str!`,
//! `|` converted back to SOH) and both fixtures **assert** byte-for-byte
//! equality against their goldens off the timed path, so a dialect change the
//! fixtures miss fails loudly instead of silently measuring a stale shape.
//! Built once, outside any measured loop: construction cost (string allocation,
//! header building) must never pollute the decode/encode histograms, mirroring
//! `workload.rs`'s "build outside the loop" convention.

use fauxchange::exchange::{Cents, SequenceNumber, SignedCents, Symbol};
use fauxchange::gateway::fix::enums::{
    CommType, ExecType, LastLiquidityInd, OrdStatus, OrdType, OrderSide, TimeInForce,
};
use fauxchange::gateway::fix::execution::ExecutionReport;
use fauxchange::gateway::fix::header::{StandardHeader, UtcTimestamp};
use fauxchange::gateway::fix::order::NewOrderSingle;
use fauxchange::gateway::fix::{DecodedMessage, FixBody, decode};
use fauxchange::{AccountId, ClientOrderId, ExecutionId, VenueOrderId};
use ironfix_core::types::{CompId, SeqNum};

/// A fixed sending time — matches `tests/golden_fix.rs`'s own `SENDING_TIME`
/// so this fixture is byte-identical to the golden's construction.
const SENDING_TIME: &str = "20240329-12:00:00.000";

fn comp(id: &str) -> CompId {
    match CompId::new(id) {
        Some(c) => c,
        None => panic!("HP-3 fixture comp id {id} failed to construct"),
    }
}

fn ts() -> UtcTimestamp {
    match UtcTimestamp::parse(52, SENDING_TIME) {
        Ok(t) => t,
        Err(e) => panic!("HP-3 fixture sending time failed to parse: {e:?}"),
    }
}

fn symbol() -> Symbol {
    match Symbol::parse("BTC-20240329-50000-C") {
        Ok(s) => s,
        Err(e) => panic!("HP-3 fixture symbol failed to parse: {e:?}"),
    }
}

/// The exact `NewOrderSingle (D)` shape golden-tested by
/// `tests/golden_fix.rs::test_golden_new_order_single_d` /
/// `tests/golden/fix/new_order_single_D.txt` — a limit buy, `GTC`, with an
/// `Account (1)`.
#[must_use]
pub fn new_order_single_fixture() -> NewOrderSingle {
    NewOrderSingle {
        header: StandardHeader::new(comp("CLIENT"), comp("FAUXCHANGE"), SeqNum::new(7), ts()),
        cl_ord_id: ClientOrderId::new("CLIENT-1"),
        account: Some(AccountId::new("acct-1")),
        symbol: symbol(),
        side: OrderSide::Buy,
        transact_time: ts(),
        ord_type: OrdType::Limit,
        price: Some(Cents::new(50005)),
        order_qty: 3,
        time_in_force: TimeInForce::Gtc,
        expire_time: None,
    }
}

/// Converts a committed FIX golden's diff-readable form (`|` standing in for the
/// SOH byte, exactly one trailing newline — the shape `tests/golden_fix.rs`
/// writes with `UPDATE_GOLDEN=1`) back to the raw wire bytes it pins.
fn golden_frame(display: &str) -> Vec<u8> {
    display
        .trim_end_matches('\n')
        .replace('|', "\u{1}")
        .into_bytes()
}

/// The committed `NewOrderSingle (D)` golden (`tests/golden/fix/new_order_single_D.txt`)
/// as raw wire bytes — the HP-3 decode span's fixed input, consumed **directly**
/// from the #036 golden so the bench measures the exact bytes the pinned dialect
/// carries, never a parallel construction that could silently drift. Built once
/// outside any measured loop.
///
/// # Panics
///
/// Panics if the reconstructed [`new_order_single_fixture`] no longer encodes
/// byte-for-byte to this golden (a drift guard, off the timed path — a dialect
/// change the fixture missed fails loudly here), or if the golden bytes fail to
/// decode back to a `NewOrderSingle` (which would let the bench silently measure
/// a reject path). `tests/bench_harness.rs` asserts both properties under
/// `cargo test`, independently of this guard.
#[must_use]
pub fn new_order_single_frame() -> Vec<u8> {
    let golden = golden_frame(include_str!(
        "../../tests/golden/fix/new_order_single_D.txt"
    ));
    let reconstructed = FixBody::encode(&new_order_single_fixture());
    assert_eq!(
        reconstructed, golden,
        "HP-3 D fixture drifted from tests/golden/fix/new_order_single_D.txt; \
         regenerate the golden (UPDATE_GOLDEN=1 cargo test --test golden_fix) and \
         re-check new_order_single_fixture"
    );
    match decode(&golden) {
        Ok(DecodedMessage::NewOrderSingle(_)) => {}
        other => panic!("HP-3 D golden does not decode to NewOrderSingle: {other:?}"),
    }
    golden
}

/// The exact `ExecutionReport (8)` shape golden-tested by
/// `tests/golden_fix.rs::test_golden_execution_report_8` /
/// `tests/golden/fix/execution_report_8.txt` — a fully-filled `Trade` leg with
/// a maker commission, the shape HP-3's encode span renders on every fill.
#[must_use]
pub fn execution_report_fixture() -> ExecutionReport {
    ExecutionReport {
        header: StandardHeader::new(comp("FAUXCHANGE"), comp("CLIENT"), SeqNum::new(12), ts()),
        order_id: VenueOrderId::new("run-1:BTC:7:0"),
        exec_id: ExecutionId::new("run-1:BTC:7:0"),
        exec_type: ExecType::Trade,
        ord_status: OrdStatus::Filled,
        symbol: symbol(),
        side: OrderSide::Buy,
        leaves_qty: 0,
        cum_qty: 3,
        last_qty: Some(3),
        last_px: Some(Cents::new(50005)),
        price: Some(Cents::new(50005)),
        secondary_exec_id: SequenceNumber::new(7),
        transact_time: ts(),
        commission: Some(SignedCents::new(-10)),
        comm_type: Some(CommType::Absolute),
        last_liquidity_ind: Some(LastLiquidityInd::Maker),
        ord_rej_reason: None,
        text: None,
    }
}

/// The committed `ExecutionReport (8)` golden (`tests/golden/fix/execution_report_8.txt`)
/// as raw wire bytes — the exact frame HP-3's encode span must render. The encode
/// span's input is the typed [`execution_report_fixture`] (you cannot "encode a
/// byte string"), so the bench asserts, off the timed path, that
/// `FixBody::encode(&execution_report_fixture())` equals these bytes — a dialect
/// change the fixture missed fails loudly there instead of measuring a stale
/// shape. `tests/bench_harness.rs` asserts the same equality under `cargo test`.
///
/// # Panics
///
/// Panics if the golden bytes fail to decode back to an `ExecutionReport` — the
/// pinned encode target must itself be a valid `8`.
#[must_use]
pub fn execution_report_golden_frame() -> Vec<u8> {
    let golden = golden_frame(include_str!(
        "../../tests/golden/fix/execution_report_8.txt"
    ));
    match decode(&golden) {
        Ok(DecodedMessage::ExecutionReport(_)) => {}
        other => panic!("HP-3 8 golden does not decode to ExecutionReport: {other:?}"),
    }
    golden
}
