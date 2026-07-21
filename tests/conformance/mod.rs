//! Reusable **conformance / parity helpers** — the shared substrate the v0.1
//! REST/WS parity suite ([`tests/parity.rs`]) is built on, and the seam the v0.4
//! FIX order-entry parity arm (#041) and the v1.0 packaged conformance harness
//! (#051) extend rather than rewrite
//! ([018](../../milestones/v0.1-backend-core/018-parity-fixtures-rest-ws.md),
//! [03 §7](../../docs/03-protocol-surfaces.md#7-protocol-parity-guarantees),
//! [TESTING.md §6](../../docs/TESTING.md#6-conformance--parity-rest--ws--fix)).
//!
//! ## What lives here (protocol-agnostic on purpose)
//!
//! - **The normalization rule** ([`normalize_event`] / [`normalize_stream`] /
//!   [`assert_streams_parity`]): the documented rule that makes order-entry
//!   streams comparable across surfaces. Protocol-only fields — the transport
//!   timestamp (`venue_ts`) and the per-surface order-id / `ClOrdID` mapping
//!   placeholders (`order_id` / `new_order_id` / `client_order_id`) — are
//!   normalized away; the venue identifiers that MUST stay equal —
//!   `underlying_sequence`, `execution_id`, fills, and resting-book state — are
//!   compared **verbatim**.
//! - **The per-surface fresh-venue topology** ([`venue`]): one identically-seeded
//!   fresh venue per surface (same lineage `fauxchange`, same fixed venue clock),
//!   so submitting the same logical order to each surface is a real parity test
//!   (submitting twice to *one* live actor cannot show parity — the second sees
//!   mutated state).
//! - **The cross-surface join-key projection** ([`FillJoinKeys`]): the four join
//!   keys plus price/quantity/side, extracted identically from a REST
//!   `ExecutionRecord`, a WS `fill`, and (at #041) a FIX `ExecutionReport (8)`.
//! - **A REST HTTP harness** (`tower::ServiceExt::oneshot` over the live router)
//!   and a small **order-entry scenario driver** ([`drive_rest_orders`]) that
//!   returns the journaled `VenueEvent` stream for a scenario.
//!
//! The FIX arm plugs in by adding a `drive_fix_orders` returning the same
//! `Vec<VenueEvent>` and a `fix_execution_report_join_keys` returning the same
//! [`FillJoinKeys`], then calling the *same* [`assert_streams_parity`] /
//! [`FillJoinKeys`] comparators — no change to this module.
//!
//! Some items are consumed by the v0.1 suite and some are staged for #041/#051,
//! so the module allows dead code (it is a shared helper crate module, not a
//! leaf test).

#![allow(dead_code)]

/// The FIX order-entry parity arm (#041): the live-acceptor harness, the FIX test
/// client, [`fix::drive_fix_orders`], and the FIX fill projection — the seam this
/// module's doc reserved, added without changing the normalization rule above.
pub mod fix;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tower::ServiceExt;

use fauxchange::auth::AccountProvision;
use fauxchange::exchange::{
    Cents, Hash32, JournalRecord, STPMode, Side, Symbol, TimeInForce, VenueCommand, VenueEvent,
};
use fauxchange::gateway::rest::create_router;
use fauxchange::models::{AccountId, OrderType, Permission, VenueOrderId, WsMessage};
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};

// ============================================================================
// Constants and the per-surface fresh venue
// ============================================================================

/// The bootstrap secret that gates token issuance on the fresh venues.
pub const SECRET: &str = "op-secret";

/// The single underlying every parity fixture hosts.
pub const UNDERLYING: &str = "BTC";

/// The canonical fixture contract symbol.
pub const CALL: &str = "BTC-20240329-50000-C";

/// The per-contract REST path prefix for the fixture contract (`BTC` call, exp
/// `20240329`, strike `50000`).
pub const CONTRACT: &str =
    "/api/v1/underlyings/BTC/expirations/20240329/strikes/50000/options/call";

/// A generous rate-limit budget for a multi-request fixture — high enough that
/// throttling never masks a reachability / parity assertion.
pub const AMPLE_RATE_LIMIT: u32 = 100_000;

/// Builds one **identically-seeded fresh venue** hosting `BTC` with the standard
/// four-account tier set (`admin-1`/`trader-1`/`trader-2`/`reader-1`), the
/// bootstrap secret, and `limit` requests/window. Every call yields a distinct
/// venue with the **same** run lineage (`fauxchange`) and the same fixed venue
/// clock, so two venues are a valid per-surface parity pair.
#[must_use]
pub fn venue(limit: u32) -> Arc<AppState> {
    let accounts = vec![
        AccountProvision::new(
            AccountId::new("admin-1"),
            Hash32([1; 32]),
            vec![Permission::Admin],
        ),
        AccountProvision::new(
            AccountId::new("trader-1"),
            Hash32([2; 32]),
            vec![Permission::Trade],
        ),
        AccountProvision::new(
            AccountId::new("trader-2"),
            Hash32([3; 32]),
            vec![Permission::Trade],
        ),
        AccountProvision::new(
            AccountId::new("reader-1"),
            Hash32([4; 32]),
            vec![Permission::Read],
        ),
    ];
    let auth = match AuthConfig::dev() {
        Ok(auth) => auth
            .with_bootstrap_secret(SECRET)
            .with_accounts(accounts)
            .with_rate_limit(limit),
        Err(error) => panic!("dev auth must build: {error}"),
    };
    match AppState::new(AppStateConfig::new([UNDERLYING]).with_auth(auth)) {
        Ok(state) => state,
        Err(error) => panic!("AppState must build: {error}"),
    }
}

/// Wall-clock seconds for token minting (the credential plane is wall-clock, not
/// the venue clock).
#[must_use]
pub fn now_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(e) => panic!("system clock before epoch: {e}"),
    }
}

/// Mints a JWT for `account` through the bootstrap-gated path.
#[must_use]
pub fn token(state: &Arc<AppState>, account: &str) -> String {
    match state.mint_token(&AccountId::new(account), SECRET, now_secs(), 3_600) {
        Ok(token) => token,
        Err(error) => panic!("minting must succeed for {account}: {error}"),
    }
}

/// Parses a canonical symbol for a fixture (never `unwrap`).
#[must_use]
pub fn sym(raw: &str) -> Symbol {
    match Symbol::parse(raw) {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
    }
}

// ============================================================================
// REST HTTP harness (oneshot against the live router)
// ============================================================================

/// Builds a `Request` with an optional bearer token and JSON body.
#[must_use]
pub fn build_request(
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Option<Value>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            match serde_json::to_vec(&value) {
                Ok(bytes) => Body::from(bytes),
                Err(e) => panic!("serialising the request body must succeed: {e}"),
            }
        }
        None => Body::empty(),
    };
    match builder.body(body) {
        Ok(request) => request,
        Err(e) => panic!("building the request must succeed: {e}"),
    }
}

/// Sends one request through a fresh clone of the router and returns
/// `(status, body_json)`; a missing / empty body decodes to [`Value::Null`].
pub async fn send(state: &Arc<AppState>, request: Request<Body>) -> (StatusCode, Value) {
    let router: Router = create_router(Arc::clone(state));
    let response = match router.oneshot(request).await {
        Ok(response) => response,
        Err(e) => panic!("router must be infallible: {e}"),
    };
    let status = response.status();
    let bytes = match to_bytes(response.into_body(), usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => panic!("reading the body must succeed: {e}"),
    };
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

// ============================================================================
// The journaled event stream
// ============================================================================

/// Reads the ordered committed `VenueEvent` stream for `underlying` from its
/// actor journal — the artifact the order-entry parity oracle compares.
pub async fn journaled_events(state: &Arc<AppState>, underlying: &str) -> Vec<VenueEvent> {
    let snapshot = match state.journal_snapshot(underlying).await {
        Ok(snapshot) => snapshot,
        Err(e) => panic!("journal snapshot for {underlying} must succeed: {e}"),
    };
    snapshot
        .records
        .into_iter()
        .filter_map(|record| match record {
            JournalRecord::Event(event) => Some(event),
            _ => None,
        })
        .collect()
}

// ============================================================================
// The normalization rule (protocol-only stripped, venue identity verbatim)
// ============================================================================

/// The object keys that carry a **per-surface order-id / `ClOrdID` mapping
/// placeholder**, normalized away before a cross-surface stream comparison.
///
/// The venue order id is minted per surface today (the REST gateway uses a
/// monotonic `g`-counter, `src/gateway/rest/support.rs`; a FIX `ClOrdID` is the
/// client's own token), so it is a mapping placeholder, exactly the
/// "`ClOrdID` echoes" the parity rule strips
/// ([03 §7](../../docs/03-protocol-surfaces.md#7-protocol-parity-guarantees)).
/// `execution_id` is deliberately **not** here: it is derived from the run
/// lineage + `underlying_sequence` and is a compared-verbatim join key.
pub const STRIPPED_KEYS: &[&str] = &["order_id", "new_order_id", "client_order_id"];

/// The transport-timestamp key normalized to a canonical value (it is not a
/// venue-identity field; across two surface runs it may differ under latency
/// injection).
pub const TRANSPORT_TS_KEY: &str = "venue_ts";

/// The canonical value every stripped key is rewritten to.
pub const NORMALIZED_PLACEHOLDER: &str = "<normalized>";

/// The canonical value the transport timestamp is rewritten to.
pub const NORMALIZED_TS: u64 = 0;

/// Recursively rewrites the protocol-only fields of a serialized `VenueEvent`:
/// every [`STRIPPED_KEYS`] value becomes [`NORMALIZED_PLACEHOLDER`], the
/// [`TRANSPORT_TS_KEY`] becomes [`NORMALIZED_TS`], and every other value —
/// `underlying_sequence`, `execution_id`, fills, resting-book state — is left
/// **verbatim**.
fn canonicalize(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if STRIPPED_KEYS.contains(&key.as_str()) {
                    *child = Value::String(NORMALIZED_PLACEHOLDER.to_string());
                } else if key == TRANSPORT_TS_KEY {
                    *child = Value::Number(NORMALIZED_TS.into());
                } else {
                    canonicalize(child);
                }
            }
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                canonicalize(item);
            }
        }
        _ => {}
    }
}

/// Normalizes one committed `VenueEvent` to its comparable JSON projection.
#[must_use]
pub fn normalize_event(event: &VenueEvent) -> Value {
    let mut value = match serde_json::to_value(event) {
        Ok(value) => value,
        Err(e) => panic!("serialising a VenueEvent must succeed: {e}"),
    };
    canonicalize(&mut value);
    value
}

/// Normalizes an ordered event stream.
#[must_use]
pub fn normalize_stream(events: &[VenueEvent]) -> Vec<Value> {
    events.iter().map(normalize_event).collect()
}

/// The reusable cross-surface parity comparator: asserts two surfaces' committed
/// `VenueEvent` streams are equal **after normalization** (order-entry parity).
/// #041 calls this with `(rest_events, fix_events)` unchanged.
pub fn assert_streams_parity(
    label_a: &str,
    events_a: &[VenueEvent],
    label_b: &str,
    events_b: &[VenueEvent],
) {
    let a = normalize_stream(events_a);
    let b = normalize_stream(events_b);
    assert_eq!(
        a.len(),
        b.len(),
        "{label_a} and {label_b} must journal the same number of events"
    );
    for (index, (ea, eb)) in a.iter().zip(b.iter()).enumerate() {
        assert_eq!(
            ea, eb,
            "normalized event #{index} must agree across {label_a} and {label_b}"
        );
    }
}

/// Collects every value stored under `key` anywhere in the JSON tree (used by
/// the normalizer unit tests to prove a key was / was not rewritten).
pub fn collect_values_for_key<'a>(value: &'a Value, key: &str, out: &mut Vec<&'a Value>) {
    match value {
        Value::Object(map) => {
            for (k, child) in map {
                if k == key {
                    out.push(child);
                }
                collect_values_for_key(child, key, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_values_for_key(item, key, out);
            }
        }
        _ => {}
    }
}

/// A convenience over [`collect_values_for_key`] returning owned clones.
#[must_use]
pub fn values_for_key(value: &Value, key: &str) -> Vec<Value> {
    let mut refs = Vec::new();
    collect_values_for_key(value, key, &mut refs);
    refs.into_iter().cloned().collect()
}

// ============================================================================
// Cross-surface join keys (observation parity)
// ============================================================================

/// The surface-independent projection of one fill leg — the four documented
/// **join keys** (`execution_id`, `liquidity`, `underlying_sequence`,
/// `venue_ts`) plus `price` / `quantity` / `side`
/// ([01 §7](../../docs/01-domain-model.md#7-fills-executions-and-execution-reports)).
///
/// A REST `ExecutionRecord`, a WS `fill`, and (at #041) a FIX
/// `ExecutionReport (8)` each project to this same value; parity is exact
/// equality of the projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillJoinKeys {
    pub execution_id: String,
    pub liquidity: String,
    pub underlying_sequence: u64,
    pub venue_ts: u64,
    pub side: String,
    pub quantity: u64,
    pub price: u64,
}

fn json_str(value: &Value, key: &str) -> Option<String> {
    value.get(key)?.as_str().map(str::to_string)
}

fn json_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key)?.as_u64()
}

/// The `data` object of a WS `fill` message, or `None` for any other message.
#[must_use]
pub fn ws_fill_data(message: &WsMessage) -> Option<Value> {
    let value = serde_json::to_value(message).ok()?;
    if value.get("type")?.as_str()? != "fill" {
        return None;
    }
    value.get("data").cloned()
}

/// Extracts the join keys from a WS `fill` message (the public anonymised
/// projection), or `None` if `message` is not a `fill`.
#[must_use]
pub fn ws_fill_join_keys(message: &WsMessage) -> Option<FillJoinKeys> {
    let data = ws_fill_data(message)?;
    Some(FillJoinKeys {
        execution_id: json_str(&data, "execution_id")?,
        liquidity: json_str(&data, "liquidity")?,
        underlying_sequence: json_u64(&data, "underlying_sequence")?,
        venue_ts: json_u64(&data, "venue_ts")?,
        side: json_str(&data, "side")?,
        quantity: json_u64(&data, "quantity")?,
        price: json_u64(&data, "price")?,
    })
}

/// Extracts the join keys from a REST `ExecutionRecord` JSON body (the
/// account-scoped projection). The REST field names differ from the WS ones
/// (`executed_at` / `price_cents`), which is exactly what the projection
/// normalizes over.
#[must_use]
pub fn execution_record_join_keys(record: &Value) -> Option<FillJoinKeys> {
    Some(FillJoinKeys {
        execution_id: json_str(record, "execution_id")?,
        liquidity: json_str(record, "liquidity")?,
        underlying_sequence: json_u64(record, "underlying_sequence")?,
        venue_ts: json_u64(record, "executed_at")?,
        side: json_str(record, "side")?,
        quantity: json_u64(record, "quantity")?,
        price: json_u64(record, "price_cents")?,
    })
}

// ============================================================================
// WS broadcast draining + committed-event fixtures
// ============================================================================

/// Drains every currently-buffered message from a broadcast receiver.
#[must_use]
pub fn drain(rx: &mut broadcast::Receiver<WsMessage>) -> Vec<WsMessage> {
    let mut out = Vec::new();
    while let Ok(message) = rx.try_recv() {
        out.push(message);
    }
    out
}

/// A limit `AddOrder` on the fixture contract, as a raw sequenced command (used
/// to produce one committed fill for observation / market-data parity, where the
/// arrival surface is irrelevant — only the fill rendering is under test).
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn add_order(
    order_id: &str,
    account: &str,
    owner_byte: u8,
    side: Side,
    price: u64,
    quantity: u64,
    time_in_force: TimeInForce,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(CALL),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new(account),
        owner: Hash32([owner_byte; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force,
        stp_mode: STPMode::None,
    }
}

// ============================================================================
// REST order-entry scenario driver (the per-surface driver #041 mirrors)
// ============================================================================

/// One logical order-entry step of a parity scenario. Kept protocol-agnostic so
/// the FIX arm (#041) can drive the *same* scenario over `D`/`F` messages; WS is
/// never an order-entry surface, so it never drives these.
#[derive(Debug, Clone)]
pub enum Step {
    /// Place a resting / crossing limit order on the fixture contract.
    Place {
        account: &'static str,
        side: &'static str,
        price: u64,
        qty: u64,
        tif: Option<&'static str>,
    },
    /// Cancel the order placed by a prior [`Step::Place`] at index `target`.
    Cancel {
        account: &'static str,
        target: usize,
    },
}

/// Drives a scenario over the **live REST surface** of `state` (real router,
/// real auth, real sequenced path), returning the per-step venue order ids (the
/// gateway-minted placeholders, `None` for a [`Step::Cancel`] step). Read the
/// resulting committed stream with [`journaled_events`].
pub async fn drive_rest_orders(state: &Arc<AppState>, steps: &[Step]) -> Vec<Option<String>> {
    let mut tokens: HashMap<&'static str, String> = HashMap::new();
    let mut ids: Vec<Option<String>> = Vec::with_capacity(steps.len());
    // Iterate by owned value so the field bindings are plain values (not the
    // double references match-ergonomics would give over `&[Step]`).
    for step in steps.iter().cloned() {
        match step {
            Step::Place {
                account,
                side,
                price,
                qty,
                tif,
            } => {
                let bearer = tokens
                    .entry(account)
                    .or_insert_with(|| token(state, account))
                    .clone();
                let mut body = json!({ "side": side, "price": price, "quantity": qty });
                if let Some(tif) = tif {
                    body["time_in_force"] = json!(tif);
                }
                let uri = format!("{CONTRACT}/orders");
                let (status, response) = send(
                    state,
                    build_request("POST", &uri, Some(&bearer), Some(body)),
                )
                .await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "REST place must be accepted, got {status}: {response}"
                );
                ids.push(response["order_id"].as_str().map(str::to_string));
            }
            Step::Cancel { account, target } => {
                let bearer = tokens
                    .entry(account)
                    .or_insert_with(|| token(state, account))
                    .clone();
                let order_id = match ids.get(target).and_then(Clone::clone) {
                    Some(id) => id,
                    None => panic!("Cancel target #{target} did not place an order"),
                };
                let uri = format!("{CONTRACT}/orders/{order_id}");
                let (status, response) =
                    send(state, build_request("DELETE", &uri, Some(&bearer), None)).await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "REST cancel must be accepted, got {status}: {response}"
                );
                ids.push(None);
            }
        }
    }
    ids
}
