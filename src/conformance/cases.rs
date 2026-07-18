//! The **conformance / parity cases** — the report-producing suites the packaged
//! `fauxchange conformance` run executes
//! ([051](../../milestones/v1.0-stability/051-conformance-harness.md)).
//!
//! Each `run_*` suite records one [`super::report::CaseReport`] per case; a case
//! is a `async fn() -> CaseOutcome` that returns `Ok(())` on a pass or a redacted
//! reason on a fail — it never panics on wire data. The suites mirror the frozen
//! `tests/parity.rs` + `tests/conformance/` assertions (#018/#041): they package
//! the *existing* contract, they do not re-derive it.
//!
//! - [`run_order_entry_parity`] — REST ≡ FIX for place / partial-fill /
//!   cancel-replace / STP outcome / per-leg fees / rejection / same-payload retry.
//! - [`run_observation_parity`] — one committed fill across REST/WS/FIX join keys,
//!   the anonymised WS `fill`, and FIX `W`/`X` ≡ WS market data.
//! - [`run_control_parity`] — REST ≡ WS control knobs + the *no FIX control* rule.
//! - [`run_fix_conformance`] — session admin + order + market data + **every**
//!   reject row of the [03 §8](../../docs/03-protocol-surfaces.md#8-error-mapping-across-surfaces)
//!   matrix, each with a redacted `Text (58)`.
//! - [`run_rest_ws_conformance`] — OpenAPI route shape, `/health` tokenless,
//!   permission gating, and WS snapshot→delta sequencing / laggard re-snapshot.

use serde_json::{Value, json};

use crate::error::{VenueError, WsErrorCode};
use crate::exchange::{
    CancelReason, CancelledLeg, Cents, EventTimestamp, Hash32, STPMode, SequenceNumber, Side,
    Symbol, TimeInForce, VenueCommand, VenueEvent, VenueOutcome,
};
use crate::models::{AccountId, OrderType, Permission, VenueOrderId, WsMessage};
use crate::subscription::OrderbookSubscriptionManager;

use super::harness::{
    ADMIN, CALL, CONTRACT, FixClient, READER, Step, TRADER1, TRADER2, UNDERLYING, VenueServer,
    WsClient, any_msg_type, attempt_logon, drive_fix_orders, drive_rest_orders, field, find_msg,
    find_report, http, journaled_events, msg_type, ws_find_type,
};
use super::parity::{
    drain, execution_record_join_keys, find_taker_fill, fix_report_projection, normalize_event,
    streams_parity, ws_fill_data, ws_fill_join_keys,
};
use super::report::{CaseOutcome, SuiteRecorder, SuiteReport, Surface};

// ============================================================================
// Small event fixtures (built directly, no server — for outcome / market-data
// projections where the arrival surface is irrelevant).
// ============================================================================

fn sym() -> Result<Symbol, String> {
    Symbol::parse(CALL).map_err(|e| format!("fixture symbol failed to parse: {e:?}"))
}

/// A resting limit add (no fills) — a committed `VenueEvent`.
fn resting_add(
    symbol: Symbol,
    sequence: u64,
    order_id: &str,
    side: Side,
    price: u64,
    qty: u64,
) -> VenueEvent {
    let command = VenueCommand::AddOrder {
        symbol,
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new("acct"),
        owner: Hash32([1; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity: qty,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    };
    VenueEvent::new(
        SequenceNumber::new(sequence),
        EventTimestamp::new(1_700_000_000_000),
        command,
        VenueOutcome::Added {
            fills: vec![],
            resting_quantity: qty,
            stp_cancelled: vec![],
        },
    )
}

/// An `AddOrder` whose STP-configured book cancels one resting leg, parameterised
/// by the aggressor / resting ids so two per-surface events differ only in the
/// stripped ids.
fn stp_event(symbol: Symbol, aggressor: &str, resting: &str) -> VenueEvent {
    VenueEvent::new(
        SequenceNumber::new(9),
        EventTimestamp::new(1_700_000_000_000),
        VenueCommand::AddOrder {
            symbol,
            order_id: VenueOrderId::new(aggressor),
            account: AccountId::new("trader-1"),
            owner: Hash32([0x22; 32]),
            client_order_id: None,
            side: Side::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 2,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::CancelMaker,
        },
        VenueOutcome::Added {
            fills: vec![],
            resting_quantity: 0,
            stp_cancelled: vec![CancelledLeg {
                order_id: VenueOrderId::new(resting),
                owner: Hash32([0x22; 32]),
                reason: CancelReason::SelfTradePrevention,
            }],
        },
    )
}

fn require(condition: bool, detail: impl Into<String>) -> CaseOutcome {
    if condition {
        Ok(())
    } else {
        Err(detail.into())
    }
}

fn rest_fix() -> Vec<Surface> {
    vec![Surface::Rest, Surface::Fix]
}

// ============================================================================
// 1. Order-entry parity (REST ≡ FIX)
// ============================================================================

/// Runs the order-entry parity suite (REST ≡ FIX, one fresh venue per surface).
pub async fn run_order_entry_parity() -> SuiteReport {
    let mut r = SuiteRecorder::new("order_entry_parity");
    r.record(
        "order_entry.place",
        "a single resting place journals an identical normalized stream on REST and FIX",
        rest_fix(),
        case_place().await,
    );
    r.record(
        "order_entry.partial_fill",
        "a partial fill (maker rests, taker crosses) normalizes equal on REST and FIX",
        rest_fix(),
        case_partial_fill().await,
    );
    r.record(
        "order_entry.cancel_replace",
        "the place→cancel→re-place idiom journals an identical stream on REST and FIX",
        rest_fix(),
        case_cancel_replace().await,
    );
    r.record(
        "order_entry.stp_rejection",
        "the STP-cancelled outcome normalizes identically across surfaces",
        rest_fix(),
        case_stp_outcome().await,
    );
    r.record(
        "order_entry.per_leg_fees",
        "the signed per-leg fees of one crossing agree verbatim across REST and FIX",
        rest_fix(),
        case_per_leg_fees().await,
    );
    r.record(
        "order_entry.rejection",
        "a Read-permission order is refused on both surfaces and journals nothing",
        rest_fix(),
        case_rejection().await,
    );
    r.record(
        "order_entry.same_payload_retry",
        "a same-payload retry opens one order and returns the stored terminal on both",
        rest_fix(),
        case_same_payload_retry().await,
    );
    r.finish()
}

async fn case_place() -> CaseOutcome {
    let rest = VenueServer::start().await.map_err(|e| e.to_string())?;
    let fix = VenueServer::start().await.map_err(|e| e.to_string())?;
    let steps = [Step::Place {
        account: "trader-1",
        side: "sell",
        price: 50_000,
        qty: 5,
        tif: None,
    }];
    let rest_events = drive_rest_orders(&rest, &steps).await?;
    let fix_events = drive_fix_orders(&fix, &steps).await?;
    require(rest_events.len() == 1, "expected exactly one REST event")?;
    require(fix_events.len() == 1, "expected exactly one FIX event")?;
    streams_parity("rest", &rest_events, "fix", &fix_events)?;
    require(
        rest_events[0].underlying_sequence == fix_events[0].underlying_sequence,
        "underlying_sequence must be identical raw (compared verbatim)",
    )
}

async fn case_partial_fill() -> CaseOutcome {
    let rest = VenueServer::start().await.map_err(|e| e.to_string())?;
    let fix = VenueServer::start().await.map_err(|e| e.to_string())?;
    let steps = crossing_steps();
    let rest_events = drive_rest_orders(&rest, &steps).await?;
    let fix_events = drive_fix_orders(&fix, &steps).await?;
    require(rest_events.len() == 2, "expected place + crossing on REST")?;
    require(fix_events.len() == 2, "expected place + crossing on FIX")?;
    let fills = match &fix_events[1].outcome {
        VenueOutcome::Added { fills, .. } if !fills.is_empty() => fills,
        _ => return Err("the FIX crossing must carry fills".to_string()),
    };
    require(fills.len() == 2, "one match must be two linked legs")?;
    streams_parity("rest", &rest_events, "fix", &fix_events)
}

async fn case_cancel_replace() -> CaseOutcome {
    let rest = VenueServer::start().await.map_err(|e| e.to_string())?;
    let fix = VenueServer::start().await.map_err(|e| e.to_string())?;
    let steps = [
        Step::Place {
            account: "trader-1",
            side: "sell",
            price: 50_000,
            qty: 4,
            tif: None,
        },
        Step::Cancel {
            account: "trader-1",
            target: 0,
        },
        Step::Place {
            account: "trader-1",
            side: "sell",
            price: 50_500,
            qty: 4,
            tif: None,
        },
    ];
    let rest_events = drive_rest_orders(&rest, &steps).await?;
    let fix_events = drive_fix_orders(&fix, &steps).await?;
    require(
        rest_events.len() == 3,
        "place + cancel + re-place = 3 REST events",
    )?;
    require(
        fix_events.len() == 3,
        "place + cancel + re-place = 3 FIX events",
    )?;
    require(
        matches!(fix_events[1].command, VenueCommand::CancelOrder { .. }),
        "the middle FIX command must be a CancelOrder",
    )?;
    streams_parity("rest", &rest_events, "fix", &fix_events)
}

async fn case_stp_outcome() -> CaseOutcome {
    // A LIVE STP rejection is not wire-expressible at v1: neither the REST place DTO
    // nor a FIX `D` carries an STP mode (per-account STP is venue config), so an
    // STP-mode order is identically inexpressible. The packaged assertion is that the
    // STP-cancelled OUTCOME normalizes identically across surfaces.
    let symbol = sym()?;
    let rest_like = stp_event(symbol.clone(), "rest-aggressor", "rest-resting");
    let fix_like = stp_event(symbol, "fix-aggressor", "fix-resting");
    let na = normalize_event(&rest_like)?;
    let nb = normalize_event(&fix_like)?;
    require(na == nb, "the STP-cancelled outcome must normalize equal")?;
    let ra = serde_json::to_value(&rest_like).map_err(|e| e.to_string())?;
    let rb = serde_json::to_value(&fix_like).map_err(|e| e.to_string())?;
    require(
        ra != rb,
        "raw, the two events must differ in the stripped ids",
    )
}

async fn case_per_leg_fees() -> CaseOutcome {
    let rest = VenueServer::start().await.map_err(|e| e.to_string())?;
    let fix = VenueServer::start().await.map_err(|e| e.to_string())?;
    let steps = crossing_steps();
    let rest_events = drive_rest_orders(&rest, &steps).await?;
    let fix_events = drive_fix_orders(&fix, &steps).await?;
    let rest_fills = match &rest_events.get(1).map(|e| &e.outcome) {
        Some(VenueOutcome::Added { fills, .. }) => fills.clone(),
        _ => return Err("the REST crossing must carry fills".to_string()),
    };
    let fix_fills = match &fix_events.get(1).map(|e| &e.outcome) {
        Some(VenueOutcome::Added { fills, .. }) => fills.clone(),
        _ => return Err("the FIX crossing must carry fills".to_string()),
    };
    let rest_fees: Vec<_> = rest_fills.iter().map(|f| f.fee).collect();
    let fix_fees: Vec<_> = fix_fills.iter().map(|f| f.fee).collect();
    require(
        rest_fees == fix_fees,
        "the per-leg fees must agree verbatim across REST and FIX",
    )
}

async fn case_rejection() -> CaseOutcome {
    let rest = VenueServer::start().await.map_err(|e| e.to_string())?;
    let fix = VenueServer::start().await.map_err(|e| e.to_string())?;

    // REST: a Read account is refused with 403 forbidden and journals nothing.
    let reader = rest.token("reader-1")?;
    let reply = http(
        rest.rest_addr(),
        "POST",
        &format!("{CONTRACT}/orders"),
        Some(&reader),
        Some(json!({ "side": "buy", "price": 50_000, "quantity": 1 })),
    )
    .await?;
    require(
        reply.status == 403,
        "REST must refuse a Read order with 403",
    )?;
    require(
        reply.body["code"] == json!("forbidden"),
        "REST reject must carry the forbidden code",
    )?;
    let rest_events = journaled_events(rest.state(), UNDERLYING).await?;
    require(
        rest_events.is_empty(),
        "a REST-rejected order must journal no command",
    )?;

    // FIX: a Read account is refused with an ExecutionReport(8) Rejected, not a
    // session Reject(3), and journals nothing.
    let mut reader_fix = FixClient::logon(fix.fix_addr(), READER).await?;
    let reply = reader_fix.place_limit("rej-1", "1", 50_000, 1, "1").await?;
    require(
        !any_msg_type(&reply, "3"),
        "a Read FIX order rejection must never be a session Reject(3)",
    )?;
    let rejected = find_report(&reply, "8")
        .ok_or_else(|| "a Read FIX order must be an ExecutionReport(8) Rejected".to_string())?;
    require(
        field(rejected, "39").as_deref() == Some("8"),
        "OrdStatus must be Rejected",
    )?;
    let fix_events = journaled_events(fix.state(), UNDERLYING).await?;
    require(
        fix_events.is_empty(),
        "a FIX-rejected order must journal no command",
    )?;

    streams_parity("rest", &rest_events, "fix", &fix_events)
}

async fn case_same_payload_retry() -> CaseOutcome {
    let rest = VenueServer::start().await.map_err(|e| e.to_string())?;
    let fix = VenueServer::start().await.map_err(|e| e.to_string())?;

    // REST: a byte-identical retry (shared idempotency key) returns the stored
    // terminal — journaled as a post-journal no-op replay (two events).
    let trader = rest.token("trader-1")?;
    let body = json!({
        "side": "sell", "price": 50_000, "quantity": 3, "client_order_id": "idem-key-1"
    });
    for attempt in 0..2 {
        let reply = http(
            rest.rest_addr(),
            "POST",
            &format!("{CONTRACT}/orders"),
            Some(&trader),
            Some(body.clone()),
        )
        .await?;
        require(
            reply.status == 200,
            format!("REST retry #{attempt} must be accepted"),
        )?;
    }
    let rest_events = journaled_events(rest.state(), UNDERLYING).await?;

    // FIX: the same ClOrdID twice (new MsgSeqNum each) dedups before the sequencer —
    // one journaled event.
    let mut trader_fix = FixClient::logon(fix.fix_addr(), TRADER1).await?;
    let _ = trader_fix
        .place_limit("idem-key-1", "2", 50_000, 3, "1")
        .await?;
    let _ = trader_fix
        .place_limit("idem-key-1", "2", 50_000, 3, "1")
        .await?;
    let fix_events = journaled_events(fix.state(), UNDERLYING).await?;

    require(
        rest_events.len() == 2,
        "REST journals original + deduped-replay retry",
    )?;
    require(
        rest_events[1].outcome == rest_events[0].outcome,
        "the REST retry must replay the stored terminal result",
    )?;
    require(
        fix_events.len() == 1,
        "FIX dedups the retry before the sequencer",
    )?;
    require(
        rest_events[0].outcome == fix_events[0].outcome,
        "the one opened order must be identical across REST and FIX",
    )
}

fn crossing_steps() -> [Step; 2] {
    [
        Step::Place {
            account: "trader-1",
            side: "sell",
            price: 50_000,
            qty: 5,
            tif: None,
        },
        Step::Place {
            account: "trader-2",
            side: "buy",
            price: 50_000,
            qty: 2,
            tif: None,
        },
    ]
}

// ============================================================================
// 2. Observation parity (REST/WS/FIX)
// ============================================================================

/// Runs the observation-parity suite (one committed fill, three projections).
pub async fn run_observation_parity() -> SuiteReport {
    let mut r = SuiteRecorder::new("observation_parity");
    r.record(
        "observation.one_fill_rest_ws_fix",
        "one committed fill renders identical join keys on REST, WS, and FIX",
        vec![Surface::Rest, Surface::Ws, Surface::Fix],
        case_one_fill_all_surfaces().await,
    );
    r.record(
        "observation.ws_fill_anonymised",
        "the WS fill omits account/fee while the REST ExecutionRecord carries them",
        vec![Surface::Rest, Surface::Ws],
        case_ws_fill_anonymised().await,
    );
    r.record(
        "observation.market_data_wx_matches_ws",
        "FIX W/X share the WS instrument_sequence and resulting-quantity semantics",
        vec![Surface::Ws, Surface::Fix],
        case_market_data_projection(),
    );
    r.finish()
}

async fn drive_fix_crossing(server: &VenueServer) -> Result<(), String> {
    let mut maker = FixClient::logon(server.fix_addr(), TRADER1).await?;
    let _ = maker.place_limit("obs-maker", "2", 50_000, 5, "1").await?;
    let mut taker = FixClient::logon(server.fix_addr(), TRADER2).await?;
    let reports = taker.place_limit("obs-taker", "1", 50_000, 5, "1").await?;
    // Drain until the taker's Trade report lands (New + Trade may arrive separately).
    let mut reports = reports;
    for _ in 0..5 {
        if reports.iter().any(|f| fix_report_projection(f).is_some()) {
            break;
        }
        reports.extend(taker.drain().await);
    }
    if reports
        .iter()
        .find_map(|f| fix_report_projection(f))
        .is_none()
    {
        return Err("the FIX crossing produced no taker Trade report".to_string());
    }
    Ok(())
}

async fn case_one_fill_all_surfaces() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let state = server.state();
    let mut rx = state.subscriptions().subscribe();

    let mut maker = FixClient::logon(server.fix_addr(), TRADER1).await?;
    let _ = maker.place_limit("obs-maker", "2", 50_000, 5, "1").await?;
    let mut taker = FixClient::logon(server.fix_addr(), TRADER2).await?;
    let mut reports = taker.place_limit("obs-taker", "1", 50_000, 5, "1").await?;
    for _ in 0..5 {
        if reports.iter().any(|f| fix_report_projection(f).is_some()) {
            break;
        }
        reports.extend(taker.drain().await);
    }
    let fix_keys = reports
        .iter()
        .find_map(|f| fix_report_projection(f))
        .ok_or_else(|| "no taker FIX Trade report".to_string())?;

    let messages = drain(&mut rx);
    let taker_fill = find_taker_fill(&messages).ok_or_else(|| "no taker WS fill".to_string())?;
    let (ws_keys, ws_ts) =
        ws_fill_join_keys(&taker_fill).ok_or_else(|| "no WS join keys".to_string())?;

    let taker_token = server.token("trader-2")?;
    let uri = format!("/api/v1/executions/{}", ws_keys.execution_id);
    let record = http(server.rest_addr(), "GET", &uri, Some(&taker_token), None).await?;
    require(record.status == 200, "the taker ExecutionRecord must read")?;
    let (rest_keys, rest_ts) =
        execution_record_join_keys(&record.body).ok_or_else(|| "no REST join keys".to_string())?;

    // REST ≡ WS on every key including venue_ts.
    require(ws_keys == rest_keys, "REST and WS join keys must match")?;
    require(ws_ts == rest_ts, "REST and WS venue_ts must match")?;
    // FIX carries every join key except venue_ts (no venue-timestamp tag).
    require(fix_keys == ws_keys, "FIX and WS join keys must match")?;

    // Sanity: the driven values.
    require(
        rest_keys.underlying_sequence == 1,
        "underlying_sequence == 1",
    )?;
    require(rest_keys.price == 50_000, "price == 50000 cents")?;
    require(rest_keys.quantity == 5, "quantity == 5")?;
    require(rest_keys.side == "buy", "taker side == buy")?;
    require(rest_keys.liquidity == "taker", "taker liquidity == taker")
}

async fn case_ws_fill_anonymised() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let state = server.state();
    let mut rx = state.subscriptions().subscribe();
    drive_fix_crossing(&server).await?;

    let messages = drain(&mut rx);
    let taker_fill = find_taker_fill(&messages).ok_or_else(|| "no taker WS fill".to_string())?;
    let data = ws_fill_data(&taker_fill).ok_or_else(|| "no WS fill data".to_string())?;
    require(data.get("account").is_none(), "WS fill must omit account")?;
    require(data.get("fee").is_none(), "WS fill must omit fee")?;
    for key in [
        "execution_id",
        "underlying_sequence",
        "venue_ts",
        "liquidity",
    ] {
        require(data.get(key).is_some(), format!("WS fill must carry {key}"))?;
    }

    let execution_id = data
        .get("execution_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "WS fill missing execution_id".to_string())?;
    let taker_token = server.token("trader-2")?;
    let record = http(
        server.rest_addr(),
        "GET",
        &format!("/api/v1/executions/{execution_id}"),
        Some(&taker_token),
        None,
    )
    .await?;
    require(record.status == 200, "the ExecutionRecord must read")?;
    require(
        record.body["account"] == json!("trader-2"),
        "the REST record must carry the account",
    )?;
    require(
        record.body.get("fee_cents").is_some(),
        "the REST record must carry the fee",
    )
}

fn case_market_data_projection() -> CaseOutcome {
    use crate::gateway::fix::md_projection::{self, RequestedSides};

    const BOTH: RequestedSides = RequestedSides {
        bids: true,
        asks: true,
    };
    let symbol = sym()?;
    let manager = OrderbookSubscriptionManager::with_capacity(64);
    let mut rx = manager.subscribe();
    manager.on_committed_event(&resting_add(symbol.clone(), 1, "r1", Side::Sell, 50_100, 8));
    manager.on_committed_event(&resting_add(symbol.clone(), 2, "r2", Side::Sell, 50_100, 4));

    let mut projected = Vec::new();
    while let Ok(message) = rx.try_recv() {
        if let Some((rpt_seq, entries)) = md_projection::incremental_projection(&message, BOTH) {
            let WsMessage::OrderbookDelta {
                sequence, changes, ..
            } = &message
            else {
                continue;
            };
            require(
                rpt_seq == *sequence,
                "RptSeq(83) must equal the WS sequence",
            )?;
            let entry = entries
                .first()
                .ok_or_else(|| "an X delta must carry an entry".to_string())?;
            let change = changes
                .first()
                .ok_or_else(|| "a WS delta must carry a change".to_string())?;
            require(
                entry.size == change.quantity,
                "MDEntrySize must equal the WS resulting quantity",
            )?;
            projected.push(rpt_seq);
        }
    }
    require(projected.len() == 2, "two user rests → two X deltas")?;
    require(
        projected[1] > projected[0],
        "RptSeq must be strictly increasing per instrument",
    )?;

    let snapshot = manager.orderbook_snapshot(&symbol, None);
    let (w_seq, w_entries) = md_projection::snapshot_projection(&snapshot, BOTH)
        .ok_or_else(|| "a W projection must exist".to_string())?;
    let WsMessage::OrderbookSnapshot {
        sequence: ws_seq,
        asks,
        ..
    } = &snapshot
    else {
        return Err("orderbook_snapshot must return a snapshot".to_string());
    };
    require(
        w_seq == *ws_seq,
        "W RptSeq must equal the WS snapshot sequence",
    )?;
    require(w_entries.len() == asks.len(), "one W entry per ask level")?;
    let folded = w_entries
        .first()
        .ok_or_else(|| "the W snapshot must carry the folded ask".to_string())?;
    require(folded.size == 12, "the folded resulting total must be 12")
}

// ============================================================================
// 3. Control parity (REST ≡ WS) + the no-FIX-control rule
// ============================================================================

/// Runs the control-parity suite (REST ≡ WS; control has no FIX message).
pub async fn run_control_parity() -> SuiteReport {
    let mut r = SuiteRecorder::new("control_parity");
    r.record(
        "control.rest_ws_same_knob",
        "REST controls and the WS-equivalent commands drive the engine to the same config",
        vec![Surface::Rest, Surface::Ws],
        case_control_same_knob().await,
    );
    r.record(
        "control.permission_gate",
        "a control needs Admin on REST and the WS Forbidden rendering is non-terminal",
        vec![Surface::Rest, Surface::Ws],
        case_control_permission().await,
    );
    r.record(
        "control.ws_live_permission_gate",
        "a REAL /ws control frame is applied for an Admin token and rejected by the gateway for a Trade token",
        vec![Surface::Ws],
        case_ws_live_control().await,
    );
    r.record(
        "control.no_fix_control_message",
        "no FIX message changes a control knob — the control plane is REST/WS only",
        vec![Surface::Fix],
        case_no_fix_control().await,
    );
    r.finish()
}

async fn case_control_same_knob() -> CaseOutcome {
    let rest = VenueServer::start().await.map_err(|e| e.to_string())?;
    let ws = VenueServer::start().await.map_err(|e| e.to_string())?;
    let admin = rest.token("admin-1")?;

    // Drive the controls over the live REST routes.
    let steps: [(&str, Value); 4] = [
        (
            "/api/v1/controls/parameters",
            json!({ "spread_multiplier": 2.5, "size_scalar": 0.4, "directional_skew": -0.3 }),
        ),
        (
            "/api/v1/controls/parameters",
            json!({ "spread_multiplier": 1.5 }),
        ),
        ("/api/v1/controls/kill-switch", json!({ "enabled": false })),
        ("/api/v1/controls/enable", json!({ "enabled": true })),
    ];
    for (path, body) in &steps {
        let reply = http(
            rest.rest_addr(),
            "POST",
            path,
            Some(&admin),
            Some(body.clone()),
        )
        .await?;
        require(
            reply.status == 200,
            format!("REST control {path} must be accepted, got {}", reply.status),
        )?;
    }

    // Submit the SAME sequenced commands the WS actions build.
    let commands = [
        control(Some(2.5), Some(0.4), Some(-0.3), None),
        control(Some(1.5), None, None, None),
        control(None, None, None, Some(false)),
        control(None, None, None, Some(true)),
    ];
    for command in commands {
        ws.state()
            .submit(command)
            .await
            .map_err(|e| format!("WS-equivalent control submit: {e}"))?;
    }

    require(
        rest.state().market_maker().get_config() == ws.state().market_maker().get_config(),
        "REST and WS controls must drive the engine to the identical config",
    )?;
    let config = rest.state().market_maker().get_config();
    require(config.enabled, "the final enable control must take effect")?;
    require(
        (config.spread_multiplier - 1.5).abs() < f64::EPSILON,
        "the spread control must take effect",
    )
}

async fn case_control_permission() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let trader = server.token("trader-1")?;
    let reply = http(
        server.rest_addr(),
        "POST",
        "/api/v1/controls/enable",
        Some(&trader),
        Some(json!({ "enabled": true })),
    )
    .await?;
    require(
        reply.status == 403,
        "a Trade token must be forbidden control",
    )?;
    require(
        reply.body["code"] == json!("forbidden"),
        "the reject must carry the forbidden code",
    )?;
    let ws_error = VenueError::Forbidden(Permission::Admin).ws_error(None);
    require(
        ws_error.code == WsErrorCode::Forbidden,
        "the WS rendering must be a Forbidden code",
    )?;
    require(
        !ws_error.terminal,
        "the WS command error must be non-terminal",
    )
}

async fn case_ws_live_control() -> CaseOutcome {
    // Drive REAL masked control frames through the live `/ws` gateway over a socket,
    // so the WS surface's own message parse + Permission::Admin gate are exercised
    // end-to-end (not bypassed via AppState) — the auth-bypass class this harness
    // must catch.
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let state = server.state();
    require(
        state.market_maker().get_config().enabled,
        "the venue must start enabled",
    )?;

    // (a) An Admin token's `kill` over the live socket is applied: the gateway
    // replies with a `config` (enabled:false) and the engine flips.
    let admin = server.token("admin-1")?;
    let mut admin_ws = WsClient::connect(server.rest_addr(), &admin).await?;
    let reply = admin_ws.send_control(r#"{"action":"kill"}"#).await?;
    let config = ws_find_type(&reply, "config")
        .ok_or_else(|| "an Admin kill must return a WS config".to_string())?;
    require(
        config["enabled"] == json!(false),
        "the Admin kill config must report enabled:false",
    )?;
    require(
        !state.market_maker().get_config().enabled,
        "the Admin kill must be applied to the engine",
    )?;

    // Re-enable (still Admin) so the negative test can detect a NON-applied kill.
    let reenable = admin_ws.send_control(r#"{"action":"enable"}"#).await?;
    require(
        ws_find_type(&reenable, "config").is_some(),
        "the Admin enable must return a WS config",
    )?;
    require(
        state.market_maker().get_config().enabled,
        "the venue must be re-enabled for the negative test",
    )?;

    // (b) A Trade token's `kill` over the live socket is rejected BY THE GATEWAY:
    // a typed WS error envelope (forbidden), and the engine is untouched.
    let trader = server.token("trader-1")?;
    let mut trader_ws = WsClient::connect(server.rest_addr(), &trader).await?;
    let reply = trader_ws.send_control(r#"{"action":"kill"}"#).await?;
    require(
        ws_find_type(&reply, "config").is_none(),
        "a Trade kill must NOT return an applied config",
    )?;
    let error = ws_find_type(&reply, "error")
        .ok_or_else(|| "a Trade kill must return a typed WS error envelope".to_string())?;
    require(
        error["code"] == json!("forbidden"),
        "the WS control rejection must carry the forbidden code",
    )?;
    require(
        error["terminal"] == json!(false),
        "a forbidden control is a non-terminal command error (socket stays open)",
    )?;
    require(
        state.market_maker().get_config().enabled,
        "the Trade kill must NOT be applied to the engine",
    )
}

async fn case_no_fix_control() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let before = server.state().market_maker().get_config();
    let mut client = FixClient::logon(server.fix_addr(), TRADER1).await?;
    // The closest a FIX client can get to a "control" is an application message the
    // venue has no handler for; it is a BusinessMessageReject(j), never a control.
    let reply = client.unsupported().await?;
    require(
        any_msg_type(&reply, "j"),
        "an unsupported app message must be a BusinessMessageReject(j)",
    )?;
    let after = server.state().market_maker().get_config();
    require(
        before == after,
        "no FIX message may change a control knob (control is REST/WS only)",
    )
}

fn control(
    spread_multiplier: Option<f64>,
    size_scalar: Option<f64>,
    directional_skew: Option<f64>,
    enabled: Option<bool>,
) -> VenueCommand {
    VenueCommand::MarketMakerControl {
        spread_multiplier,
        size_scalar,
        directional_skew,
        enabled,
    }
}

// ============================================================================
// 4. FIX conformance — session admin + order + market data + every reject row
// ============================================================================

/// Runs the FIX conformance suite: the happy-path script plus every reject row of
/// the [03 §8](../../docs/03-protocol-surfaces.md#8-error-mapping-across-surfaces)
/// error matrix, each with a redacted `Text (58)`.
pub async fn run_fix_conformance() -> SuiteReport {
    let mut r = SuiteRecorder::new("fix_conformance");
    let fix = vec![Surface::Fix];
    r.record(
        "fix.session_admin_order_market_data",
        "A / 0 / 1 / 2 / 5 admin + D / G / F → 8 + V → W happy path",
        fix.clone(),
        case_fix_happy_path().await,
    );
    r.record(
        "fix.sequence_reset",
        "a SequenceReset(4) gap-fill advances the inbound expectation and the session continues",
        fix.clone(),
        case_fix_sequence_reset().await,
    );
    r.record(
        "fix.reject_3_malformed_frame",
        "a D missing Side(54) is a session Reject(3) with SessionRejectReason + RefTagID",
        fix.clone(),
        case_fix_reject_3().await,
    );
    r.record(
        "fix.reject_8_conflicting_clordid",
        "a conflicting ClOrdID reuse is an ExecutionReport(8) Rejected with OrdRejReason=6",
        fix.clone(),
        case_fix_reject_8().await,
    );
    r.record(
        "fix.reject_9_cancel_unknown",
        "a cancel of an unknown order is an OrderCancelReject(9) with CxlRejReason=1",
        fix.clone(),
        case_fix_reject_9().await,
    );
    r.record(
        "fix.reject_y_unsupported_market_data",
        "a trade-only V is a MarketDataRequestReject(Y) with MDReqRejReason=8, redacted Text(58)",
        fix.clone(),
        case_fix_reject_y().await,
    );
    r.record(
        "fix.reject_j_unsupported_application",
        "an unsupported app MsgType is a BusinessMessageReject(j) with BusinessRejectReason + RefMsgType",
        fix.clone(),
        case_fix_reject_j().await,
    );
    r.record(
        "fix.logout_5_credential_failure",
        "a bad-credential logon is refused with Logout(5) and never echoes the credential",
        fix,
        case_fix_logout_5().await,
    );
    r.finish()
}

async fn collect_reports(
    client: &mut FixClient,
    mut frames: Vec<Vec<u8>>,
    exec_type: &str,
) -> Vec<Vec<u8>> {
    for _ in 0..5 {
        if frames.iter().any(|f| {
            msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some(exec_type)
        }) {
            break;
        }
        frames.extend(client.drain().await);
    }
    frames
}

async fn case_fix_happy_path() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let addr = server.fix_addr();

    // Logon (A): a raw logon so the ack fields are asserted credential-free.
    let logon = attempt_logon(addr, ADMIN.sender, ADMIN.user, ADMIN.pw).await?;
    let ack = find_msg(&logon, "A").ok_or_else(|| "Logon(A) must be acked".to_string())?;
    require(
        field(ack, "108").as_deref() == Some("30"),
        "the Logon(A) ack must echo HeartBtInt",
    )?;
    require(
        field(ack, "553").is_none() && field(ack, "554").is_none(),
        "the Logon(A) ack must carry no credential",
    )?;

    // TRADER1: TestRequest(1) → Heartbeat(0), D → 8 New, G → 8 Replaced, F → 8 Canceled.
    let mut trader = FixClient::logon(addr, TRADER1).await?;
    let hb = trader.test_request("PING-CONF").await?;
    let hb0 =
        find_msg(&hb, "0").ok_or_else(|| "TestRequest(1) must yield Heartbeat(0)".to_string())?;
    require(
        field(hb0, "112").as_deref() == Some("PING-CONF"),
        "the Heartbeat(0) must echo the TestReqID",
    )?;

    let d = trader.place_limit("conf-new", "2", 50_000, 5, "1").await?;
    let new = find_msg(&d, "8").ok_or_else(|| "D must yield an ExecutionReport(8)".to_string())?;
    require(field(new, "150").as_deref() == Some("0"), "ExecType New")?;
    require(field(new, "39").as_deref() == Some("0"), "OrdStatus New")?;

    let g = trader
        .replace("conf-new", "conf-repl", "2", 50_500, 5)
        .await?;
    let g = collect_reports(&mut trader, g, "5").await;
    let replaced = find_report(&g, "5").ok_or_else(|| "G must yield an 8 Replaced".to_string())?;
    require(
        field(replaced, "39").as_deref() == Some("5"),
        "OrdStatus Replaced",
    )?;

    let f = trader.cancel("conf-repl", "conf-cxl", "2").await?;
    let canceled =
        find_msg(&f, "8").ok_or_else(|| "F must yield an ExecutionReport(8)".to_string())?;
    require(
        field(canceled, "150").as_deref() == Some("4"),
        "ExecType Canceled",
    )?;

    // READER: market data V (Bid+Offer) → W.
    let mut reader = FixClient::logon(addr, READER).await?;
    let v = reader.market_data("MDR-CONF", &["0", "1"]).await?;
    let w = find_msg(&v, "W").ok_or_else(|| "V must yield a W snapshot".to_string())?;
    require(
        field(w, "262").as_deref() == Some("MDR-CONF"),
        "W must echo MDReqID",
    )?;
    require(field(w, "83").is_some(), "W must carry RptSeq(83)")?;

    // Session admin (2): a deliberate inbound gap on a dedicated TRADER2 session.
    let mut gapper = FixClient::logon(addr, TRADER2).await?;
    let gap_reply = gapper.send_out_of_order().await?;
    require(
        any_msg_type(&gap_reply, "2"),
        "an inbound MsgSeqNum gap must yield a ResendRequest(2)",
    )?;

    // Session admin (5): a clean client Logout is acked with a Logout(5).
    let logout_reply = trader.logout().await?;
    require(
        any_msg_type(&logout_reply, "5"),
        "a client Logout(5) must be acked with a Logout(5)",
    )
}

async fn case_fix_sequence_reset() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let mut client = FixClient::logon(server.fix_addr(), TRADER2).await?;
    // A GapFill SequenceReset(4) administratively advances the inbound expectation.
    let reply = client.sequence_reset_gap_fill(60).await?;
    require(
        !any_msg_type(&reply, "3"),
        "a valid SequenceReset(4) must not be a session Reject(3)",
    )?;
    // The session continues at the advanced sequence: a TestRequest is answered with
    // a Heartbeat(0), never another ResendRequest(2) (the gap was filled).
    let hb = client.test_request("PING-SR").await?;
    require(
        !any_msg_type(&hb, "2"),
        "the gap was filled, so no ResendRequest(2) is expected",
    )?;
    let hb0 = find_msg(&hb, "0")
        .ok_or_else(|| "the post-reset TestRequest must yield a Heartbeat(0)".to_string())?;
    require(
        field(hb0, "112").as_deref() == Some("PING-SR"),
        "the Heartbeat(0) must echo the TestReqID",
    )
}

async fn case_fix_reject_3() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let mut client = FixClient::logon(server.fix_addr(), TRADER1).await?;
    let reply = client.order_missing_side("conf-bad").await?;
    let r3 = find_msg(&reply, "3")
        .ok_or_else(|| "a D missing Side(54) must be a session Reject(3)".to_string())?;
    require(
        field(r3, "373").is_some(),
        "Reject(3) must carry a SessionRejectReason(373)",
    )?;
    require(
        field(r3, "371").as_deref() == Some("54"),
        "RefTagID(371) must point at the missing Side(54)",
    )
}

async fn case_fix_reject_8() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let mut client = FixClient::logon(server.fix_addr(), TRADER1).await?;
    let _ = client
        .place_limit("conf-reuse", "2", 40_000, 3, "1")
        .await?;
    let conflict = client
        .place_limit("conf-reuse", "2", 40_000, 7, "1")
        .await?;
    require(
        !any_msg_type(&conflict, "3"),
        "an idempotency conflict must never be a session Reject(3)",
    )?;
    let rejected = find_report(&conflict, "8")
        .ok_or_else(|| "a conflicting ClOrdID reuse must be an 8 Rejected".to_string())?;
    require(
        field(rejected, "103").as_deref() == Some("6"),
        "OrdRejReason must be Duplicate Order",
    )
}

async fn case_fix_reject_9() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let mut client = FixClient::logon(server.fix_addr(), TRADER1).await?;
    let reply = client
        .cancel("never-placed", "conf-cxl-unknown", "1")
        .await?;
    require(
        !any_msg_type(&reply, "3"),
        "a cancel failure must never be a session Reject(3)",
    )?;
    let r9 = find_msg(&reply, "9").ok_or_else(|| {
        "a cancel of an unknown order must be an OrderCancelReject(9)".to_string()
    })?;
    require(
        field(r9, "102").as_deref() == Some("1"),
        "CxlRejReason must be Unknown order",
    )?;
    require(
        field(r9, "434").as_deref() == Some("1"),
        "CxlRejResponseTo must be Order Cancel Request",
    )?;
    require(
        field(r9, "41").as_deref() == Some("never-placed"),
        "the OrigClOrdID must be echoed",
    )
}

async fn case_fix_reject_y() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let mut client = FixClient::logon(server.fix_addr(), READER).await?;
    let reply = client.market_data("MDR-TRADE", &["2"]).await?;
    require(
        !any_msg_type(&reply, "3"),
        "a MD reject must never be a bare Reject(3)",
    )?;
    let y = find_msg(&reply, "Y")
        .ok_or_else(|| "a trade-only V must be a MarketDataRequestReject(Y)".to_string())?;
    require(
        field(y, "281").as_deref() == Some("8"),
        "MDReqRejReason must be Unsupported MDEntryType",
    )?;
    if let Some(text) = field(y, "58") {
        require(
            !text.contains("panic") && !text.contains("src/") && text.len() < 200,
            "the Text(58) must be a safe, redacted reason",
        )?;
    }
    Ok(())
}

async fn case_fix_reject_j() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let mut client = FixClient::logon(server.fix_addr(), TRADER1).await?;
    let reply = client.unsupported().await?;
    let j = find_msg(&reply, "j").ok_or_else(|| {
        "an unsupported app MsgType must be a BusinessMessageReject(j)".to_string()
    })?;
    require(
        field(j, "380").as_deref() == Some("3"),
        "BusinessRejectReason must be Unsupported Message Type",
    )?;
    require(
        field(j, "372").as_deref() == Some("R"),
        "RefMsgType must be echoed",
    )
}

async fn case_fix_logout_5() -> CaseOutcome {
    const BAD_USER: &str = "ghost-nonexistent-user";
    const BAD_PW: &str = "totally-wrong-secret-DoNotLog";
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let reply = attempt_logon(server.fix_addr(), "GHOSTCLIENT", BAD_USER, BAD_PW).await?;
    require(
        any_msg_type(&reply, "5"),
        "a bad-credential logon must be refused with a Logout(5)",
    )?;
    for frame in &reply {
        let text = String::from_utf8_lossy(frame);
        require(
            !text.contains(BAD_PW),
            "the presented password must never appear in a reply frame",
        )?;
    }
    Ok(())
}

// ============================================================================
// 5. REST/WS conformance — OpenAPI shape, auth exemption, permission gating, WS
// ============================================================================

/// Runs the REST/WS conformance suite.
pub async fn run_rest_ws_conformance() -> SuiteReport {
    let mut r = SuiteRecorder::new("rest_ws_conformance");
    r.record(
        "rest.openapi_route_shape",
        "every documented REST route is served with its OpenAPI shape + bearer scheme",
        vec![Surface::Rest],
        case_openapi_shape().await,
    );
    r.record(
        "rest.health_tokenless",
        "the auth-exempt /health is reachable without a token",
        vec![Surface::Rest],
        case_health_tokenless().await,
    );
    r.record(
        "rest.mutating_requires_permission",
        "a mutating route rejects a missing token (401) and an insufficient one (403)",
        vec![Surface::Rest],
        case_mutating_permission().await,
    );
    r.record(
        "ws.subscribe_snapshot_then_sequenced_deltas",
        "deltas carry a strictly-increasing instrument_sequence and resulting-quantity",
        vec![Surface::Ws],
        case_ws_sequenced_deltas(),
    );
    r.record(
        "ws.laggard_recovers_by_fresh_snapshot",
        "a laggard lags rather than stalls and recovers by a fresh snapshot, not a resend",
        vec![Surface::Ws],
        case_ws_laggard_resnapshot(),
    );
    r.finish()
}

async fn case_openapi_shape() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let doc = http(
        server.rest_addr(),
        "GET",
        "/api-docs/openapi.json",
        None,
        None,
    )
    .await?;
    require(doc.status == 200, "the OpenAPI doc must serve")?;
    let paths = doc
        .body
        .get("paths")
        .and_then(Value::as_object)
        .ok_or_else(|| "the OpenAPI doc must carry a paths object".to_string())?;
    for (path, methods) in rest_route_inventory() {
        let entry = paths
            .get(&path)
            .ok_or_else(|| format!("documented route {path} missing from OpenAPI doc"))?;
        for method in methods {
            require(
                entry.get(method).is_some(),
                format!("route {path} must document the {method} operation"),
            )?;
        }
    }
    require(
        doc.body["components"]["securitySchemes"]["bearer_jwt"].is_object(),
        "the bearer_jwt security scheme must be registered",
    )
}

async fn case_health_tokenless() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let reply = http(server.rest_addr(), "GET", "/health", None, None).await?;
    require(
        reply.status == 200,
        format!("/health must be reachable tokenless, got {}", reply.status),
    )
}

async fn case_mutating_permission() -> CaseOutcome {
    let server = VenueServer::start().await.map_err(|e| e.to_string())?;
    let uri = format!("{CONTRACT}/orders");
    let body = json!({ "side": "buy", "price": 50_000, "quantity": 1 });

    // No token → 401.
    let no_token = http(server.rest_addr(), "POST", &uri, None, Some(body.clone())).await?;
    require(
        no_token.status == 401,
        format!(
            "a mutating route must reject a missing token with 401, got {}",
            no_token.status
        ),
    )?;

    // Read token → 403 (insufficient permission).
    let reader = server.token("reader-1")?;
    let insufficient = http(server.rest_addr(), "POST", &uri, Some(&reader), Some(body)).await?;
    require(
        insufficient.status == 403,
        format!(
            "a mutating route must reject a Read token with 403, got {}",
            insufficient.status
        ),
    )
}

fn case_ws_sequenced_deltas() -> CaseOutcome {
    let symbol = sym()?;
    let manager = OrderbookSubscriptionManager::with_capacity(64);
    let mut rx = manager.subscribe();
    // Two sells at the same ask level: resulting totals 8 then 12.
    manager.on_committed_event(&resting_add(symbol.clone(), 1, "r1", Side::Sell, 50_100, 8));
    manager.on_committed_event(&resting_add(symbol.clone(), 2, "r2", Side::Sell, 50_100, 4));

    let mut deltas: Vec<(u64, u64)> = Vec::new();
    while let Ok(message) = rx.try_recv() {
        let value = serde_json::to_value(&message).map_err(|e| e.to_string())?;
        if value.get("type").and_then(Value::as_str) != Some("orderbook_delta") {
            continue;
        }
        let sequence = value["data"]["sequence"]
            .as_u64()
            .ok_or_else(|| "a delta must carry a sequence".to_string())?;
        let change = value["data"]["changes"]
            .get(0)
            .ok_or_else(|| "a delta must carry a change".to_string())?;
        require(
            change["side"] == json!("ask"),
            "the touched side must be ask",
        )?;
        require(
            change["price"] == json!(50_100),
            "the touched price must be 50100",
        )?;
        let quantity = change["quantity"]
            .as_u64()
            .ok_or_else(|| "a change must carry a resulting quantity".to_string())?;
        deltas.push((sequence, quantity));
    }
    require(deltas.len() == 2, "each user rest emits one delta")?;
    require(
        deltas[1].0 > deltas[0].0,
        "instrument_sequence must strictly increase",
    )?;
    require(deltas[0].1 == 8, "first delta shows the resulting total 8")?;
    require(
        deltas[1].1 == 12,
        "second delta shows the resulting total 12",
    )?;

    match manager.orderbook_snapshot(&symbol, None) {
        WsMessage::OrderbookSnapshot { asks, sequence, .. } => {
            require(
                sequence == deltas[1].0,
                "the snapshot baselines at the last seq",
            )?;
            let ask = asks
                .first()
                .ok_or_else(|| "the snapshot must carry the folded ask".to_string())?;
            require(ask.quantity == 12, "the folded resulting total must be 12")
        }
        _ => Err("expected a fresh snapshot".to_string()),
    }
}

fn case_ws_laggard_resnapshot() -> CaseOutcome {
    use tokio::sync::broadcast::error::TryRecvError;
    let symbol = sym()?;
    let manager = OrderbookSubscriptionManager::with_capacity(2);
    let mut rx = manager.subscribe();
    for i in 0..6u64 {
        manager.on_committed_event(&resting_add(
            symbol.clone(),
            i + 1,
            &format!("m{i}"),
            Side::Sell,
            50_000 + i,
            1,
        ));
    }
    let mut lagged = false;
    loop {
        match rx.try_recv() {
            Ok(_) => {}
            Err(TryRecvError::Lagged(_)) => {
                lagged = true;
                break;
            }
            Err(_) => break,
        }
    }
    require(lagged, "a slow consumer must lag on a bounded broadcast")?;

    match manager.orderbook_snapshot(&symbol, None) {
        WsMessage::OrderbookSnapshot { asks, sequence, .. } => {
            require(
                asks.len() == 6,
                "the fresh snapshot must have every folded level",
            )?;
            require(
                sequence == 6,
                "the snapshot must re-baseline at the current seq",
            )
        }
        _ => Err("expected a fresh snapshot".to_string()),
    }
}

/// The documented REST route inventory (`(path, methods)` with `{param}`
/// placeholders) — the OpenAPI shape contract.
fn rest_route_inventory() -> Vec<(String, Vec<&'static str>)> {
    let mut routes: Vec<(&str, Vec<&str>)> = vec![
        ("/health", vec!["get"]),
        ("/api/v1/stats", vec!["get"]),
        ("/api/v1/auth/token", vec!["post"]),
        ("/api/v1/controls", vec!["get"]),
        ("/api/v1/controls/kill-switch", vec!["post"]),
        ("/api/v1/controls/enable", vec!["post"]),
        ("/api/v1/controls/parameters", vec!["post"]),
        ("/api/v1/controls/instruments", vec!["get"]),
        ("/api/v1/controls/instrument/{symbol}/toggle", vec!["post"]),
        ("/api/v1/replay/record", vec!["get", "post"]),
        ("/api/v1/replay/export", vec!["get"]),
        ("/api/v1/replay/bundle", vec!["post"]),
        ("/api/v1/prices", vec!["get", "post"]),
        ("/api/v1/prices/{symbol}", vec!["get"]),
        ("/api/v1/underlyings", vec!["get"]),
        (
            "/api/v1/underlyings/{underlying}",
            vec!["get", "post", "delete"],
        ),
        ("/api/v1/underlyings/{underlying}/expirations", vec!["get"]),
        (
            "/api/v1/underlyings/{underlying}/expirations/{expiration}",
            vec!["get", "post"],
        ),
        (
            "/api/v1/underlyings/{underlying}/volatility-surface",
            vec!["get"],
        ),
        (
            "/api/v1/underlyings/{underlying}/expirations/{expiration}/chain",
            vec!["get"],
        ),
        (
            "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes",
            vec!["get"],
        ),
        (
            "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}",
            vec!["get", "post"],
        ),
        ("/api/v1/orders", vec!["get"]),
        ("/api/v1/orders/bulk", vec!["post", "delete"]),
        ("/api/v1/orders/cancel-all", vec!["delete"]),
        ("/api/v1/orders/{order_id}", vec!["get"]),
        ("/api/v1/positions", vec!["get"]),
        ("/api/v1/positions/{symbol}", vec!["get"]),
        ("/api/v1/executions", vec!["get"]),
        ("/api/v1/executions/{execution_id}", vec!["get"]),
        ("/api/v1/admin/snapshot", vec!["post"]),
        ("/api/v1/admin/snapshots", vec!["get"]),
        ("/api/v1/admin/snapshots/{snapshot_id}", vec!["get"]),
        (
            "/api/v1/admin/snapshots/{snapshot_id}/restore",
            vec!["post"],
        ),
    ];
    let contract: Vec<(&str, Vec<&str>)> = vec![
        ("", vec!["get"]),
        ("/orders", vec!["post"]),
        ("/orders/market", vec!["post"]),
        ("/orders/{order_id}", vec!["delete", "patch"]),
        ("/quote", vec!["get"]),
        ("/greeks", vec!["get"]),
        ("/snapshot", vec!["get"]),
        ("/last-trade", vec!["get"]),
        ("/ohlc", vec!["get"]),
        ("/metrics", vec!["get"]),
    ];
    const CONTRACT_TEMPLATE: &str = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}";
    let mut out: Vec<(String, Vec<&'static str>)> = routes
        .drain(..)
        .map(|(path, methods)| (path.to_string(), methods))
        .collect();
    for (suffix, methods) in contract {
        out.push((format!("{CONTRACT_TEMPLATE}{suffix}"), methods));
    }
    out
}
